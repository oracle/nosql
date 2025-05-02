/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.rep.login;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.RepNodeService.KVStoreCreator;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.LoginSession;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.security.login.SessionManager;
import oracle.kv.impl.security.login.TokenResolver;
import oracle.kv.impl.test.TestIOHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepGroupMap;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.RateLimitingLogger;

/**
 * KVSessionManager provides an implementation of SessionManager that stores
 * session state information in the KVStore as well as an implementation of
 * TokenResolver that resolves persistent tokens.
 * <p>
 * All session data is stored in an internal keyspace below the root key
 * ///sess/.  Each session has 2 possible KV entries that should be present.
 *
 *   ///sess/<sessionid>/-/data  - the actual session data
 *
 *   ///sess/<sessionid>/-/expire/<expire time> - when the session expires.
 *        This is used as a poor-man's secondary index to allow cleanup
 *        of expired entries to be performed with a key-only scan.
 */
public class KVSessionManager implements SessionManager, TokenResolver {

    /**
     * The standard size of a session id value.  Code should not rely on this
     * value remaining constant.
     */
    public static final int SESSION_ID_BYTES = 16;

    /**
     * The common key component at the top of the hidden keyspace.
     */
    public static final String INTERNAL_SESSION_KEY = "sess";

    /**
     * The minor component indicating that this is a session entry.
     */
    public static final String INTERNAL_SESSION_DATA_KEY = "data";

    /**
     * The minor component indicating when this session will expire.
     */
    public static final String INTERNAL_SESSION_EXPIRE_KEY = "expire";

    /**
     * The retry times when update session subject failed.
     */
    private static final int UPDATE_SUBJECT_MAX_RETRIES = 5;

    /**
     * The default iteration batch size.
     */
    private static final int ITERATE_BATCH_SIZE = 100;

    /**
     * The max number of faults to track for the purposes of log rate limiting.
     */
    private static final int LIMIT_FAULTS = 20;

    /**
     * 5 min in millis
     */
    private static final int FIVE_MINUTE_MS = 300 * 1000;

    /**
     * Used to generate session ids.
     */
    private static final SecureRandom secureRandom = new SecureRandom();

    /**
     * The requestDispatcher for the RepNode.
     */
    private final RequestDispatcher dispatcher;

    /**
     * The RepNodeParams for the RepNode.
     */
    private final RepNodeParams rnParams;

    /* A prefix to add to generated ids */
    private final byte[] idPrefix;

    /* The size of the generated portion of a session id value */
    private final int nSIDRandomBytes;

    /* A UserVerifier instance to allow checking of user login validity */
    private final UserVerifier userVerifier;

    private final Logger logger;

    /*
     * Encapsulates the above logger to limit the rate of log messages
     * associated with a specific fault.
     */
    private volatile RateLimitingLogger<String> rateLimitingLogger;

    /* The configured timemout values, etc, all in milliseconds */
    private final long sessLookupRequestTimeoutMs;
    private final long sessLookupConsistencyLimitMs;
    private final long sessLookupConsistencyTimeoutMs;
    private final long sessLogoutRequestTimeoutMs;
    private final long sessWriteTimeoutMs;

    /*
     * An internal kvstore interface object, initialized in start().
     * This handle supports serialization of the hidden keyspace, but does not
     * allow close() operations.
     */
    private volatile KVStore kvstore;

    /* KVStoreCreator is used to get the handle of KVStore */
    private final KVStoreCreator creator;

    /*
     * An estimate of the number of shards in the store. This is used to
     * help us guess the number of re-tries we should attempt when creating
     * sessions.  Initialized in start().
     */
    private int nShardsEstimate;

    /**
     * A timer that schedules session cleanup work, to be run periodically.
     */
    private Timer sessCleanupTimer;

    /**
     * TestIOHook to inject IOException in lookupKVSession to test jira
     * KVSTORE-1832: NullPointerException in
     * KVSessionManager.updateKVSessionExpire
     */
    public static volatile TestIOHook<Void> LOOKUP_FAULT_HOOK;

    /**
     * Creates a KVSessionManager.  It will not be operational until the
     * start() method is called.
     *
     * @param dispatcher the request dispatcher for the rep node
     * @param rnp the RepNodeParams for the owning RepNode
     * @param idPrefix a prefix that the caller supplies that is prepended
     *    to session id values
     * @param nSIDRandomBytes the number of randomly generated bytes in a
     *    session id
     * @param logger a logger instance
     * @param creator for creating the KVStore handle
     */
    public KVSessionManager(RequestDispatcher dispatcher,
                            RepNodeParams rnp,
                            byte[] idPrefix,
                            int nSIDRandomBytes,
                            UserVerifier userVerifier,
                            Logger logger,
                            KVStoreCreator creator) {

        this.dispatcher = dispatcher;
        this.rnParams = rnp;
        this.idPrefix = Arrays.copyOf(idPrefix, idPrefix.length);
        this.nSIDRandomBytes = nSIDRandomBytes;
        this.userVerifier = userVerifier;
        this.logger = logger;
        this.creator = creator;
        this.rateLimitingLogger = new RateLimitingLogger<String>(
            FIVE_MINUTE_MS, LIMIT_FAULTS, logger);

        this.sessLookupRequestTimeoutMs =
            getParamMillis(ParameterState.RN_SESS_LOOKUP_REQUEST_TIMEOUT);
        this.sessLookupConsistencyLimitMs =
            getParamMillis(ParameterState.RN_SESS_LOOKUP_CONSISTENCY_LIMIT);
        this.sessLookupConsistencyTimeoutMs =
            getParamMillis(ParameterState.RN_SESS_LOOKUP_CONSISTENCY_TIMEOUT);
        this.sessLogoutRequestTimeoutMs =
            getParamMillis(ParameterState.RN_SESS_LOGOUT_REQUEST_TIMEOUT);
        this.sessWriteTimeoutMs =
            getParamMillis(ParameterState.RN_SESS_REQUEST_TIMEOUT);
    }

    /**
     * For access by RepNodeService.
     * Starts the KVSessionManager.  The dispatcher must have topology
     * initialized before this can be called.
     */
    public void start() {
        /* Attempt to initialize the KVStore object. */
        initializeKVStore();
    }

    /**
     * Disables the KVSessionManager by closine the kvstore, if open, and
     * cancelling any session maintenace activity.
     */
    public void stop() {
        disableKVStore();
    }

    /**
     * Test whether the session manager is ready.  The KVStore can't be created
     * until a topology is available.
     */
    public boolean isReady() {
        return kvstore != null || initializeKVStore();
    }

    /**
     * Creates a new Session.
     * @throws SessionAccessException if KVStore is not yet ready for access
     * or if a FaultException occurs while accessing the store.
     */
    @Override
    public LoginSession createSession(
        Subject subject, String clientHost, long expireTime)
        throws SessionAccessException {

        if (!isReady()) {
            throw new SessionAccessException("KVStore is not yet ready");
        }

        Exception cause = null;

        for (int attempt = 1; attempt <= nShardsEstimate; attempt++) {
            /* Generate a new id value.  */
            final byte[] randomBytes = new byte[nSIDRandomBytes];
            final byte[] idBytes = new byte[nSIDRandomBytes + idPrefix.length];
            secureRandom.nextBytes(randomBytes);
            System.arraycopy(idPrefix, 0, idBytes, 0, idPrefix.length);
            System.arraycopy(randomBytes, 0, idBytes, idPrefix.length,
                             nSIDRandomBytes);

            final LoginSession.Id id = new LoginSession.Id(idBytes);
            final LoginSession session =
                new LoginSession(id, subject, clientHost, true);
            session.setExpireTime(expireTime);

            try {
                createKVSession(session);
                return session;
            } catch (SessionConflictException cce) {
                /* by some quirk, that session is already in use - try again */
                logger.info("Encountered a SessionConflictException");
                cause = new FaultException(cce, true);
            } catch (KVSecurityException kse) {
                /*
                 * After retries in request dispatcher, if there is still a
                 * KVSecurityException, wrap as SessionAccessException and
                 * re-throw, so upper wrapper FailoverSessionManager can
                 * fall back to in-memory access model.
                 */
                cause = kse;
            } catch (FaultException fe) {
                /*
                 * Some sort of problem - try again with a different id and
                 * hopefully a different RN
                 */
                cause = fe;
            }
        }

        if (cause != null) {
            throw new SessionAccessException(cause, true /* isReturnSignal */);
        }

        /*
         * Shouldn't ever get here.  As long as nShardsEstimate is >= 1,
         * cause will either be assigned non-null or we will have returned.
         */
        throw new IllegalStateException(
            "failed to create session, but without cause");
    }

    /**
     * Look up a Session by SessionId.
     * @return the login session if found, or else null
     */
    @Override
    public LoginSession lookupSession(LoginSession.Id sessionId)
        throws SessionAccessException {

        if (!isReady()) {
            throw new SessionAccessException("Persistent access not available");
        }

        try {
            final KVSession session = lookupKVSession(sessionId, false);
            if (session == null) {
                return null;
            }
            return session.makeLoginSession();
        } catch (FaultException fe) {
            throw new SessionAccessException(fe, true /* isReturnSignal */);
        } catch (KVSecurityException kse) {
            /* wrap as SAE to re-throw. the caller are supposed to handle SAE */
            throw new SessionAccessException(kse, true /* isReturnSignal */);
        }
    }

    /**
     * Update the expiration time for the specified session.
     */
    @Override
    public LoginSession updateSessionExpiration(LoginSession.Id sessionId,
                                                long newExpireTime)
        throws SessionAccessException {

        if (!isReady()) {
            throw new SessionAccessException("Persistent access not available");
        }

        try {
            return updateKVSessionExpire(sessionId, newExpireTime);
        } catch (FaultException fe) {
            logger.info("Failed to update the session: " + fe.getMessage());
            throw new SessionAccessException(fe, true /* isReturnSignal */);
        } catch (KVSecurityException kse) {
            logger.info("Failed to update the session: " + kse);

            /* wrap as SAE to re-throw. the caller are supposed to handle SAE */
            throw new SessionAccessException(kse, true /* isReturnSignal */);
        }
    }

    @Override
    public List<LoginSession.Id> lookupSessionByUser(String user) {
        if (!isReady()) {
            throw new SessionAccessException("Persistent access not available");
        }

        try {
            return lookupKVSessionByUser(user);
        } catch (FaultException fe) {
            logger.info("Failed to look up user sessions: " + fe.getMessage());

            throw new SessionAccessException(fe, true /* isReturnSignal */);
        }  catch (KVSecurityException kse) {
            logger.info("Failed to look up user sessions: " + kse);

            /* wrap as SAE to re-throw. the caller are supposed to handle SAE */
            throw new SessionAccessException(kse, true /* isReturnSignal */);
        }
    }

    @Override
    public void updateSessionSubject(LoginSession.Id sessionId, Subject subject)
        throws SessionAccessException, IllegalArgumentException {

        if (!isReady()) {
            throw new SessionAccessException("Persistent access not available");
        }

        /* Only master node update session subject */
        if (!isMaster()) {
            return;
        }
        try {
            updateKVSessionSubject(sessionId, subject);
        } catch (FaultException fe) {
            logger.info("Failed to update the session subject: " +
                fe.getMessage());
            throw new SessionAccessException(fe, true /* isReturnSignal */);
        } catch (KVSecurityException kse) {
            logger.info("Failed to update the session subject: " + kse);

            /* wrap as SAE to re-throw. the caller are supposed to handle SAE */
            throw new SessionAccessException(kse, true /* isReturnSignal */);
        }
    }

    /**
     * Log out the specified session.
     */
    @Override
    public void logoutSession(LoginSession.Id sessionId)
        throws SessionAccessException {

        if (!isReady()) {
            throw new SessionAccessException("Persistent access not available");
        }

        try {
            logoutKVSession(sessionId);
        } catch (FaultException fe) {
            logger.info("Failed to log out session: " + fe.getMessage());

            throw new SessionAccessException(fe, true /* isReturnSignal */);
        } catch (KVSecurityException kse) {
            logger.info("Failed to update the session subject: " + kse);

            /* wrap as SAE to re-throw. the caller are supposed to handle SAE */
            throw new SessionAccessException(kse, true /* isReturnSignal */);
        }
    }

    /**
     * Resolves a persistent LoginToken.
     */
    @Override
    public Subject resolve(LoginToken token)
        throws SessionAccessException {

        if (!isReady()) {
            logger.fine("KVSessionManager: unable to resolve tokens before " +
                        "topology information is available");
            throw new SessionAccessException("Persistent access not available");
        }

        logger.fine("KVSessionManager: attempt to resolve " + token);

        final SessionId sid = token.getSessionId();
        if (sid.getIdValueScope() != IdScope.PERSISTENT) {
            logger.info("KVSessionManager: unable to resolve " +
                        "non-persistent tokens");
            throw new UnsupportedOperationException(
                "KVSessionManager: Attempt to resolve non-persistent token");
        }

        final KVSession kvSession;
        try {
            kvSession = lookupKVSession(new LoginSession.Id(sid.getIdValue()),
                                        false);
        } catch (FaultException fe) {
            rateLimitingLogger.log(
                 sid.toString(), Level.INFO,
                "KVSessionManager: exception while attempting to " +
                "access session info for token resolve: " +
                fe.getMessage());
            throw new SessionAccessException(fe, true /* isReturnSignal */);
        } catch (KVSecurityException kse) {
            rateLimitingLogger.log(
                sid.toString(), Level.INFO,
                "KVSessionManager: exception while attempting to " +
                "access session info for token resolve: " + kse);
            throw new SessionAccessException(kse, true /* isReturnSignal */);
        }

        if (kvSession == null) {
            rateLimitingLogger.log(
                sid.toString(), Level.INFO,
                "KVSessionManager: session does not exist");
            return null;
        }

        final LoginSession session = kvSession.makeLoginSession();
        if (session.isExpired()) {
            rateLimitingLogger.log(
                sid.toString(), Level.INFO,
                "KVSessionManager: session is expired");
            return null;
        }

        return userVerifier.verifyUser(session.getSubject());
    }

    /**
     * Return true if current node is the master of replication group.
     */
    private boolean isMaster() {
        final RepNodeId rnId = (RepNodeId)dispatcher.getDispatcherId();
        final RepGroupStateTable rgst = dispatcher.getRepGroupStateTable();
        final RepNodeState master =
            rgst.getGroupState(new RepGroupId(rnId.getGroupId())).getMaster();

        if (master.getRepNodeId().equals(rnId)) {
            return true;
        }
        return false;
    }

    /**
     * Starts the KVSessionManager.  The dispatcher must have topology
     * initialized before this can be called.
     */
    private synchronized boolean initializeKVStore() {
        /*
         * Callers normally check that kvstore is non-null before calling this,
         * but the kvstore variable may become non-null by the time they enter
         * this method.
         */
        if (kvstore != null) {
            return true;
        }

        /* Return false when kvstore is not ready */
        kvstore = creator.getKVStore();
        if (kvstore == null) {
            return false;
        }

        scheduleSessionCleanup();
        this.nShardsEstimate = estimateNShards(dispatcher);
        return true;
    }

    /**
     * Disables the KVSessionManager by closing the kvstore.
     */
    private void disableKVStore() {
        if (sessCleanupTimer != null) {
            try {
                sessCleanupTimer.cancel();
            } catch (IllegalStateException iae) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE: ON */
        }
    }

    /*
     * Private persistent representation methods.
     */

    /**
     * Create a persistent login session entry
     */
    private void createKVSession(LoginSession session)
        throws SessionConflictException, FaultException {

        /* construct a serialized session representation */
        final byte[] sessionData = serializeSession(new KVSession(session));

        /* Create the major key components for the session entries */
        final List<String> majorKey = prepareMajorKey(session.getId());

        final OperationFactory operationFactory =
            kvstore.getOperationFactory();

        /* Create an operation that will store the session data */
        final Key dataKey = makeDataKey(majorKey);
        final Operation putData =
            operationFactory.createPutIfAbsent(
                dataKey,
                Value.createValue(sessionData),
                ReturnValueVersion.Choice.NONE,
                true /* abort if unsuccessful */);

        /* Create an operation that will store the session expire entry */
        final Key expireKey = makeExpireKey(majorKey, session.getExpireTime());
        final Operation putExpire =
            operationFactory.createPutIfAbsent(
                expireKey,
                Value.EMPTY_VALUE,
                ReturnValueVersion.Choice.NONE,
                true /* abort if unsuccessful */);

        /*
         * Store the entries.  Be prepared for the unlikely possibility that
         * the session id is already claimed, and for the usual possible
         * faults.
         */
        final List<Operation> ops = new ArrayList<Operation>();
        ops.add(putData);
        ops.add(putExpire);

        try {
            kvstore.execute(ops, null, sessWriteTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (OperationExecutionException oee) {

            dumpKVSessionKeys("Attempting to create session",
                              session.getId().getValue());

            throw new SessionConflictException("confict with existing session");
        }
    }

    /*
     * Lookup a persistent login session entry
     * @throw FaultException if an error occurs as a result of the access
     */
    private KVSession lookupKVSession(LoginSession.Id sessionId,
                                      boolean requireMaster)
        throws FaultException {

        /* Create the major key components for the session entries */
        final List<String> majorKey = prepareMajorKey(sessionId);
        final Key dataKey = makeDataKey(majorKey);

        Consistency opConsistency;
        if (requireMaster) {
            opConsistency = Consistency.ABSOLUTE;
        } else {

            /*
             * Allow replicas to be modestly out-of-date
             */
            opConsistency = new Consistency.Time(sessLookupConsistencyLimitMs,
                                                 TimeUnit.MILLISECONDS,
                                                 sessLookupConsistencyTimeoutMs,
                                                 TimeUnit.MILLISECONDS);
        }

        /* Retrieve the session data */
        final ValueVersion dataVV =
            kvstore.get(dataKey,
                        opConsistency,
                        sessLookupRequestTimeoutMs,
                        TimeUnit.MILLISECONDS);

        if (dataVV == null) {
            /* non-existent - presumably previously deleted*/

            dumpKVSessionKeys("Attempting to lookup session",
                              sessionId.getValue());

            return null;
        }

        final byte[] sessionData = dataVV.getValue().getValue();

        final KVSession kvSession;
        try {
            kvSession = KVSession.fromByteArray(sessionData);
            // inject IOException to test jira KVSTORE-1832
            TestHookExecute.doIOHookIfSet(LOOKUP_FAULT_HOOK, null);
        } catch (IOException ioe) {
            logger.info("IO exception deserializing KVSession: " + ioe);
            return null;
        }

        return kvSession;
    }

    /*
     * Lookup persistent login session ids of given user.
     * @throw FaultException if an error occurs as a result of the access
     */
    private List<LoginSession.Id> lookupKVSessionByUser(String userName) {
        final List<LoginSession.Id> ids = new ArrayList<LoginSession.Id>();
        try {
            final Iterator<KeyValueVersion> iter =
                kvstore.storeIterator(Direction.UNORDERED,
                                      ITERATE_BATCH_SIZE, /* batch size*/
                                      makeSessionParentKey(),
                                      null /* subRange*/,
                                      Depth.DESCENDANTS_ONLY,
                                      Consistency.ABSOLUTE,
                                      10, /* timeout */
                                      TimeUnit.SECONDS);

            while (iter.hasNext()) {
                final KeyValueVersion sessData = iter.next();
                final Key sessKey = sessData.getKey();
                final List<String> minorPath = sessKey.getMinorPath();

                if (minorPath.size() == 1 &&
                    minorPath.get(0).equals(INTERNAL_SESSION_DATA_KEY)) {
                    final KVSession kvSession = KVSession.fromByteArray(
                        sessData.getValue().getValue());

                    if (userName.equals(kvSession.getUserName())) {
                        ids.add(new LoginSession.Id(kvSession.getSessionId()));
                    }
                }
            }
        } catch (IOException ioe) {
            logger.info("IO exception deserializing KVSession: " + ioe);
            throw new FaultException(ioe, true);
        } catch (KVSecurityException kvse) {
            logger.info("Security error during session lookup by user: " + kvse);
            throw new SessionAccessException(kvse, true /* isReturnSignal */);
        }
        return ids;
    }

    /*
     * Update a persistent login session entry with a new version.
     * @throw FaultException if an error occurs as a result of the access
     */
    private LoginSession updateKVSessionExpire(LoginSession.Id sessionId,
                                               long newExpireTime)
        throws FaultException {

        KVSession session = lookupKVSession(sessionId, true);

        /**
         * Fix for KVSTORE-1832: NullPointerException in
         * KVSessionManager.updateKVSessionExpire
         * session may already cleaned so return null
         */
        if (session == null) {
            return null;
        }
        final long initialExpireTime = session.getSessionExpire();

        if (newExpireTime == initialExpireTime) {
            return session.makeLoginSession();
        }
        session.setSessionExpire(newExpireTime);
        session = updateKVSession(sessionId, session, initialExpireTime);

        if (session != null) {
            return session.makeLoginSession();
        }
        return null;
    }

    /*
     * Update a persistent login session entry with a new subject
     * @throw FaultException if an error occurs as a result of the access
     */
    private void updateKVSessionSubject(LoginSession.Id sessionId,
                                        Subject subject)
        throws FaultException {

        KVSession session = lookupKVSession(sessionId, true);

        /* session may already cleaned */
        if (session == null) {
            return;
        }
        final String[] newRoles = ExecutionContext.getSubjectRoles(subject);

        /*
         * Session subject update may fail because of concurrent expire time
         * update and session logout. Retry only for the first case.
         */
        int attempts = 0;
        while (session != null &&
               !checkRolesEquals(newRoles, session.getUserRoles())) {

            if (attempts > UPDATE_SUBJECT_MAX_RETRIES) {
                throw new SessionAccessException(
                    "Failed to update session subject");
            }
            attempts++;
            session.setUserRoles(newRoles);
            session = updateKVSession(
                sessionId, session, session.getSessionExpire());
        }
    }

    /*
     * Compare actual role arrays with expected by checking if they contain
     * the same elements.
     */
    private boolean checkRolesEquals(String[] expected, String[] actual) {
        if (expected.length != actual.length) {
            return false;
        }
        final List<String> roleList = Arrays.asList(expected);
        for (final String role : actual) {
            if (!roleList.contains(role)) {
                return false;
            }
        }
        return true;
    }

    /*
     * Update persistent login session entry. If the current expire time of
     * session differs from given initial expire time, update session expire
     * time entry accordingly.
     */
    private KVSession updateKVSession(LoginSession.Id sessionId,
                                      KVSession session,
                                      long initialExpireTime)
        throws FaultException {

        /* construct a serialized session representation */
        final byte[] sessionData = serializeSession(session);
        final long currentExpireTime = session.getSessionExpire();

        /* Create the major key components for the session entries */
        final List<String> majorKey = prepareMajorKey(session.getSessionId());
        final OperationFactory operationFactory = kvstore.getOperationFactory();

        /*
         * Create an operation that will update the existing data, which must
         * already exist.
         */
        final Key dataKey = makeDataKey(majorKey);
        final Operation putData =
            operationFactory.createPutIfPresent(
                dataKey,
                Value.createValue(sessionData),
                ReturnValueVersion.Choice.NONE,
                true /* abort if unsuccessful */);
        final List<Operation> ops = new ArrayList<Operation>();
        ops.add(putData);

        if (initialExpireTime != currentExpireTime) {
            /*
             * Create an operation that will store the new session expire
             * entry if expire time need to be updated.
             */
            final Operation putExpire =
                operationFactory.createPutIfAbsent(
                    makeExpireKey(majorKey, currentExpireTime),
                    Value.EMPTY_VALUE,
                    ReturnValueVersion.Choice.NONE,
                    true /* abort if unsuccessful */);

            /*
             * Create an operation that will delete the old session expire
             * entry
             */
            final Operation deleteExpire =
                operationFactory.createDelete(
                    makeExpireKey(majorKey, initialExpireTime),
                    ReturnValueVersion.Choice.NONE,
                    true /* abort if unsuccessful */);
            ops.add(deleteExpire);
            ops.add(putExpire);
        }

        try {
            kvstore.execute(ops, null, sessWriteTimeoutMs, TimeUnit.MILLISECONDS);
            return session;
        } catch (OperationExecutionException oee) {

            final String errorMsg = (initialExpireTime != currentExpireTime) ?
                "Attempting to change expire time from " +
                initialExpireTime + " to " + currentExpireTime :
                "Attempting to update session subject";
            dumpKVSessionKeys(errorMsg, session.getSessionId());
        }

        /*
         * There is a race condition where a concurrent update could
         * occur between the time we read the entry and the time that we
         * update it. Re-read the entry, and if it exists and the expire time
         * differs from the original expire time, return the new session.
         */
        session = lookupKVSession(sessionId, true);
        if (session != null &&
            session.getSessionExpire() != initialExpireTime) {
            return session;
        }

        return null;
    }

    /**
     * Log out a persistent login session entry.
     */
    private void logoutKVSession(LoginSession.Id sessionId)
        throws FaultException {

        /* Create the major key components for the session entries */
        final List<String> majorKey = prepareMajorKey(sessionId);
        final Key fullMajorKey = makeMajorKey(majorKey);

        /* final int deleteCount = */
        kvstore.multiDelete(fullMajorKey,
                            null, /* subRange */
                            Depth.DESCENDANTS_ONLY,
                            Durability.COMMIT_SYNC,
                            sessLogoutRequestTimeoutMs,
                            TimeUnit.MILLISECONDS);
    }

    /*
     * Session maintenance support
     */

    /**
     * Schedules a persistent session cleanup task.  The kvstore instance
     * must be initialized.
     */
    private void scheduleSessionCleanup() {
        if (kvstore == null) {
            return;
        }

        /* Cancel any outstanding timer */
        if (sessCleanupTimer != null) {
            sessCleanupTimer.cancel();
            sessCleanupTimer = null;
        }

        final long now = System.currentTimeMillis();

        /*
         * Target session cleanup to occur once per hour, on average,
         * across the entire store.  Our refresh period is scaled by the number
         * of RepNodes in the system, with our initial run chosen as a
         * random fraction of that period.
         */
        final int nRNs = dispatcher.getTopologyManager().getTopology().
            getSortedRepNodes().size();
        final long cleanupPeriod = nRNs * 3600 * 1000L;
        final long cleanupTime = now +
            (long) (new Random().nextDouble() * cleanupPeriod);

        this.sessCleanupTimer = new Timer(true /* isDaemon */);
        final TimerTask cleanupTask =
            new TimerTask() {
                @Override
                public void run() {
                    purgeExpiredKVSessions();
                }
            };

        this.sessCleanupTimer.schedule(cleanupTask, (cleanupTime - now),
                                       cleanupPeriod);

        logger.info("session cleanup task scheduled to run in " +
                    ((cleanupTime - now) / 1000L) +
                    " seconds, with period of " +
                    (cleanupPeriod / 1000L) + " seconds");
    }

    /**
     * Deletes persistent sessions that have passed their expiration time.
     */
    private void purgeExpiredKVSessions() {

        /*
         * Allow a substantial lag to find expired sessions.  We adjust the
         * expiration threshold so that even though the data could be stale,
         * the session would already have expired at the point of consistency
         * so it is safe to delete the session, and if it already has been
         * deleted, that's harmless, except that it may result in an
         * informational log message.
         */
        final long expireLagSecs = 60;
        final long timeSkewAllowSecs = 60;
        final long now = System.currentTimeMillis();
        final long expireThresh =
            now - (expireLagSecs + timeSkewAllowSecs) * 1000L;
        final long consistencyTimeoutSecs = 0;

        final Direction direction = Direction.UNORDERED;
        final int batchSize = ITERATE_BATCH_SIZE;
        final KeyRange subRange = null;
        final Depth depth = Depth.DESCENDANTS_ONLY;
        final Consistency consistency =
            new Consistency.Time(expireLagSecs,
                                 TimeUnit.SECONDS,
                                 consistencyTimeoutSecs,
                                 TimeUnit.SECONDS);
        final long timeout = 10;
        final TimeUnit timeoutUnit = TimeUnit.SECONDS;

        try {
            final Iterator<Key> keyIter =
                kvstore.storeKeysIterator(direction, batchSize,
                                          makeSessionParentKey(),
                                          subRange, depth, consistency,
                                          timeout, timeoutUnit);

            while (keyIter.hasNext()) {
                final Key sessKey = keyIter.next();
                final List<String> minorPath = sessKey.getMinorPath();
                final List<String> majorPath = sessKey.getMajorPath();

                if (minorPath.size() == 2 &&
                    minorPath.get(0).equals(INTERNAL_SESSION_EXPIRE_KEY)) {

                    /*
                     * This is the expire minor key.  Extract the expire
                     * time from the key to determine if the key is to be
                     * expired.
                     */
                    try {
                        final long expireTime =
                            decodeExpireTime(minorPath.get(1));

                        /*
                         * Note: no allowance for expireTime==0 is included
                         * here. Persistent sessions MUST have an expiration or
                         * they could accumulate indefinitely.
                         */
                        if (expireTime < expireThresh) {
                            logger.fine("Deleting expired session");
                            deleteKVSession(Key.createKey(majorPath));
                        }
                    } catch (NumberFormatException nfe) {
                        logger.info("Error parsing session expire time: " +
                                    nfe);
                    }
                }
            }
        } catch (KVSecurityException kvse) {
            logger.info("Security error during session cleanup: " + kvse);
        } catch (FaultException fe) {
            logger.info("Fault during session cleanup: " + fe.getMessage());
        }
    }

    private void deleteKVSession(Key sessionParentKey) {
        try {
            final int deleteCount =
                kvstore.multiDelete(sessionParentKey,
                                    null, /* subRange */
                                    Depth.DESCENDANTS_ONLY,
                                    Durability.COMMIT_SYNC,
                                    10,
                                    TimeUnit.SECONDS);
            if (deleteCount < 1) {

                /*
                 * It's remotely possible that this was caused by concurrent
                 * access.
                 */
                logger.info("No KV entries deleted as part of session " +
                            "deletion");
            }
        } catch (KVSecurityException kvse) {
            throw new SessionAccessException(kvse, true /* isReturnSignal */);
        } catch (FaultException fe) {
            logger.info("Error encountered while deleting session: " +
                fe.getMessage());
        }
    }

    /**
     * A debugging helper.  Logs an informational messsage along with
     * about the minor keys in the database that are associated with the
     * session.  The actual session id is not printed, as that is
     * sensitive information.
     */
    private void dumpKVSessionKeys(String msg, byte[] id) {

        try {
            final StringBuilder sb = new StringBuilder();

            final Key majorKey = makeMajorKey(prepareMajorKey(id));
            boolean first = true;
            for (Key key : kvstore.multiGetKeys(majorKey,
                                            (KeyRange) null,
                                            Depth.DESCENDANTS_ONLY)) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }

                sb.append(key.getMinorPath());
            }

            rateLimitingLogger.log(
                 majorKey.toString(), Level.INFO,
                "KVSessionManager: " + msg + " keys found were " +
                sb.toString());
        } catch (Exception e) {
            logger.info("KVSessionManager: encountered exception " + e +
                        " while attempting to diagnose: " + msg);
        }
    }

    /**
     * Serialize a KVSession object to create a byte array. Returns the
     * serialized representation if successful.  If an error occurs during
     * the serialization process, an exception is logged and a FaultException
     * is thrown.
     */
    private byte[] serializeSession(KVSession session)
        throws FaultException {

        try {
            return session.toByteArray();
        } catch (IOException ioe) {

            /*
             * There's little reason for this to happen, but handle it
             * reasonably.
             */
            logger.info("IO error serializing session: " + ioe);
            throw new FaultException(ioe, true);
        }
    }

    /*
     * A collection of utility functions related to session access.
     */

    private static List<String> prepareMajorKey(LoginSession.Id sessionId) {
        return prepareMajorKey(sessionId.getValue());
    }

    private static List<String> prepareMajorKey(byte[] sessionId) {

        /* Create the major key components for the session entries */
        final List<String> majorKey = new ArrayList<String>();
        majorKey.add("");
        majorKey.add("");
        majorKey.add(INTERNAL_SESSION_KEY);
        majorKey.add(encodeId(sessionId));

        return majorKey;
    }

    private static String encodeId(byte[] id) {
        final StringBuilder sb = new StringBuilder();
        for (int x : id) {
            x = x & 0xff;
            sb.append(Integer.toHexString(x));
        }
        return sb.toString();
    }

    private static Key makeMajorKey(List<String> majorKeyList) {
        return Key.createKey(majorKeyList);
    }

    private static Key makeDataKey(List<String> majorKey) {
        final List<String> dataMinorKey = new ArrayList<String>();
        dataMinorKey.add(INTERNAL_SESSION_DATA_KEY);
        return Key.createKey(majorKey, dataMinorKey);
    }

    private static Key makeSessionParentKey() {
        final List<String> majorKeyPath = new ArrayList<String>();
        majorKeyPath.add("");
        majorKeyPath.add("");
        majorKeyPath.add(INTERNAL_SESSION_KEY);
        return Key.createKey(majorKeyPath);
    }

    private static Key makeExpireKey(List<String> majorKey, long expireTime) {
        final String expireString = encodeExpireTime(expireTime);
        final List<String> expireMinorKey = new ArrayList<String>();
        expireMinorKey.add(INTERNAL_SESSION_EXPIRE_KEY);
        expireMinorKey.add(expireString);
        return Key.createKey(majorKey, expireMinorKey);
    }

    private static String encodeExpireTime(long expireTime) {
        return Long.toHexString(expireTime);
    }

    private static long decodeExpireTime(String expireTimeString)
        throws NumberFormatException {

        return Long.valueOf(expireTimeString, 16);
    }

    /**
     * Generate an estimate of the number of shards in the store
     * This never returns a number less than 1.
     */
    private static int estimateNShards(RequestDispatcher dispatcher) {
        final TopologyManager topoMgr = dispatcher.getTopologyManager();
        if (topoMgr == null) {
            return 1;
        }

        final Topology topo = topoMgr.getTopology();
        if (topo == null) {
            return 1;
        }

        final RepGroupMap rgMap = topo.getRepGroupMap();
        if (rgMap.size() <= 1) {
            return 1;
        }
        return rgMap.size();
    }

    private long getParamMillis(String param) {
        return ParameterUtils.getDurationMillis(rnParams.getMap(), param);
    }

    @SuppressWarnings("serial")
    private static class SessionConflictException extends Exception {
        private SessionConflictException(String msg) {
            super(msg);
        }
    }
}
