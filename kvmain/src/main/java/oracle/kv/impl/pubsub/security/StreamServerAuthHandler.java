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

package oracle.kv.impl.pubsub.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.util.RateLimitingLogger;

import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.subscription.StreamAuthenticator;

/**
 * Object represents an authenticator that is running at the server side and
 * used by by feeder to authenticate the subscriber.
 */
public class StreamServerAuthHandler implements StreamAuthenticator {

    /**
     * Null channel id for configured the authenticator passed down to JE.
     * During handshake, each data channel will create a clone authenticator
     * with its own channel id
     */
    private static final String NULL_CHANNEL_ID = "NullChannelId";
    /**
     * Rate limiting logger max objects
     */
    private static final int RL_MAX_OBJS = 1024;
    /**
     * Rate limiting logger sample interval in ms
     */
    private static final int RL_INTV_MS = 10 * 1000;
    /**
     * Max attempts to retry in authentication
     */
    private static final int MAX_AUTH_ATTEMPTS = 128;
    /**
     * Sleep time in ms before retry in identify requester
     */
    private static final long RETRY_SLEEP_MS = 200;

    /* access checker from host rn */
    private final AccessChecker accessChecker;

    /* auth context built from token */
    private volatile AuthContext authCtx;

    /* subscribed table id strings, null if all tables subscribed */
    private volatile String[] tableIdStr;

    /* last time of security check */
    private volatile long lastCheckTime;

    /* logger */
    private final Logger logger;
    /**
     * rate limiting logger
     */
    private final RateLimitingLogger<String> rl;
    /**
     * Channel associated with the authenticator mainly used for logging,
     * useful in presence of multiple streams
     */
    private final String channelId;

    private StreamServerAuthHandler(String channelId,
                                    AccessChecker accessChecker,
                                    Logger logger) {

        if (accessChecker == null) {
            throw new IllegalArgumentException("Null access checker");
        }

        this.channelId = channelId;
        this.accessChecker = accessChecker;
        this.logger = logger;
        authCtx = null;
        tableIdStr = null;
        lastCheckTime = 0;
        rl = new RateLimitingLogger<>(RL_INTV_MS, RL_MAX_OBJS, logger);
    }

    @Override
    public String toString() {
        return "StreamServerAuthHandler of channelId=" + channelId +
               ", tableIdStr=" + Arrays.toString(tableIdStr) ;
    }

    /**
     * Gets an instance of server side authentication handler
     *
     * @param ac      access checker
     * @param logger  logger
     *
     * @return an instance of server side authentication handler
     *
     * @throws IllegalArgumentException if access checker is null
     */
    public static StreamServerAuthHandler getAuthHandler(
        AccessChecker ac, Logger logger) throws IllegalArgumentException {
        return new StreamServerAuthHandler(NULL_CHANNEL_ID, ac, logger);
    }

    @Override
    public StreamAuthenticator getInstance(DataChannel channel) {
        final StreamServerAuthHandler ret =
            new StreamServerAuthHandler(channel.getChannelId(),
                                        accessChecker,
                                        logger);
        logger.info(lm("Create a new instance of StreamServerAuthHandler, " +
                       "channel id=" + ret.getChannelId()));
        return ret;
    }

    @Override
    public String getChannelId() {
        return channelId;
    }

    /**
     * Creates auth context from login token
     *
     * @param token login token in byte array
     */
    @Override
    public void setToken(byte[] token) {
        authCtx = new AuthContext(LoginToken.fromByteArray(token));
        logger.fine(() -> lm("Token set and auth context created"));
    }

    /**
     * Sets subscribed table id strings
     *
     * @param ids subscribed table id strings, null if all tables subscribed.
     */
    @Override
    public void setTableIds(String[] ids) {
        tableIdStr = ids;
        /* make it INFO level for diagnosis purpose */
        logger.info(lm("Set subscribed table (id)=" +
                       ((ids == null || ids.length == 0) ?
                           " all tables." : Arrays.toString(ids))));
    }

    /**
     * Authenticate login token. If no token has been set in the
     * authenticator, returns false.
     *
     * @return true if token is authenticated, false otherwise.
     */
    @Override
    public boolean authenticate() {

        lastCheckTime = System.currentTimeMillis();

        logger.fine(() -> lm("Authentication starts"));

        if (authCtx == null) {
            /*
             * A protection in the case caller incorrectly call authenticate()
             * without an auth ctx. For example, in a secure store, feeder
             * which servers a replica does not have auth ctx initialized
             * in handshake since it relies on channel authentication. Also,
             * authenticate() should never be called for a non-secure store.
             */
            logger.warning(lm("Fail to authenticate because auth context is " +
                              "unavailable"));
            return false;
        }

        //TODO: do we really need check expiration, any api to do that?
        if (isTokenExpired(authCtx)) {
            logger.warning(lm("Fail to authenticate because token has " +
                              "expired"));
            return false;
        }

        return identifyRequestorWithRetry();
    }

    /**
     * Authenticates requester with retry
     * @return true if successfully authenticated, false if fail to
     * authenticate or cannot perform authentication after max number of
     * attempts.
     */
    private boolean identifyRequestorWithRetry() {
        boolean succ = false;
        boolean sleepBeforeRetry = false;
        int attempts = 0;
        final long ts = System.currentTimeMillis();
        while (attempts < MAX_AUTH_ATTEMPTS) {
            try {
                if (sleepBeforeRetry) {
                    Thread.sleep(RETRY_SLEEP_MS);
                }
                attempts++;
                if (accessChecker.identifyRequestor(authCtx) != null) {
                    final int att = attempts;
                    logger.fine(() -> lm("Authentication succeeded " +
                                         ", attempts=" + att +
                                         ", elapsed ms=" +
                                         (System.currentTimeMillis() - ts)));
                    succ = true;
                } else {
                    logger.warning(lm("Authentication failed because " +
                                      "requester cannot be identified" +
                                      ", attempts=" + attempts +
                                      ", elapsed ms=" +
                                      (System.currentTimeMillis() - ts)));
                }
                break;
            } catch (SessionAccessException sae) {
                rl.log("Timeout", Level.INFO,
                       lm("Authentication failure due to" +
                          " exception=" + sae + ", attempts=" + attempts +
                          ", will retry after sleep for ms=" + RETRY_SLEEP_MS));
                sleepBeforeRetry = true;
            } catch (InterruptedException ie) {
                logger.warning(lm("Interrupted in sleeping before trying to " +
                                  "identify requester, exception=" + ie +
                                  ", attempts=" + attempts));
                break;
            }
        }

        if (attempts >= MAX_AUTH_ATTEMPTS && !succ) {
            logger.warning(lm("Authentication failed after maximum" +
                              " attempts=" + MAX_AUTH_ATTEMPTS));
        }
        return succ;
    }

    /**
     * Checks if token is valid and the owner has enough privilege to stream
     * updates from subscribed tables
     *
     * @return true if token owner has enough privilege, false otherwise.
     */
    @Override
    public boolean checkAccess() {

        /*
         * If no auth context has been created, check fails
         *
         * For a replica in secure store, which does not create any auth ctx
         * in service handshake, we shall never reach here because the data
         * channel is trusted. Check feeder and feeder replica sync-up in JE
         * for details.
         *
         * For all other cases, if we reach here without a valid auth ctx,
         * something wrong already happened and we shall fail access check.
         */
        if (authCtx == null) {
            logger.warning(lm("Check access failed because auth context is " +
                              "not initialized"));
            return false;
        }

        lastCheckTime = System.currentTimeMillis();

        logger.fine(() -> lm("Security check starts"));

        /* authenticate token */
        if (!authenticate()) {
            logger.warning(lm("Security check failed in authentication."));
            return false;
        }

        final SubscriptionOpsCtx opCtx = new SubscriptionOpsCtx(tableIdStr);
        try {
            /*
             * Calling ExecutionContext.create has checked if execution ctx
             * has all privileges, and raise exception if it fails.
             */
            ExecutionContext.create(accessChecker, authCtx, opCtx);
            logger.finest(() -> lm("Privilege check passed for=" +
                                   opCtx.describe()));
            return true;
        } catch (AuthenticationRequiredException | UnauthorizedException |
            SessionAccessException exp) {
            logger.warning(lm("Check access failed, error=" + exp));
            return false;
        }
    }

    /**
     * Gets the time stamp of last security check in milliseconds
     *
     * @return the time stamp of last security check, or 0 if no check has
     * been made.
     */
    @Override
    public long getLastCheckTimeMs() {
        return lastCheckTime;
    }

    private static boolean isTokenExpired(AuthContext ac) {
        return (ac.getLoginToken().getExpireTime() <=
                System.currentTimeMillis());
    }

    private String lm(String msg) {
        return "[StreamServerAuth][ChannelId=" + channelId + "] " + msg;
    }

    /**
     * Subscription operation context
     */
    static class SubscriptionOpsCtx implements OperationContext {

        private final String[] ids;
        SubscriptionOpsCtx(String[] tableIds) {
            ids = tableIds;
        }

        @Override
        public String describe() {
            return "Subscription of" + ((ids == null || ids.length == 0) ?
                " all user tables" : " tables IDs=" + Arrays.toString(ids));
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            /* required read privileges for all subscribed tables */
            final List<KVStorePrivilege> privileges = new ArrayList<>();
            if (ids == null || ids.length == 0) {
                /* read all tables */
                privileges.add(SystemPrivilege.READ_ANY_TABLE);
            } else {
                /* privilege to read each table */
                for (String idStr : ids) {
                    final long tid = TableImpl.createIdFromIdStr(idStr);
                    privileges.add(new TablePrivilege.ReadTable(tid));
                }
            }
            return privileges;
        }
    }
}
