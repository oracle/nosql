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

import java.util.List;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginSession;
import oracle.kv.impl.security.login.SessionManager;

/**
 * FailoverSessionManager uses a KVSessionManager as its primary storage
 * location for session storage, but will fail over to an in-memory
 * session manager if the KVSessionManager encounters faults.
 */
public class FailoverSessionManager implements SessionManager {

    public static final byte[] MEMORY_PREFIX = { 0 };
    public static final byte[] PERSISTENT_PREFIX = { 0x01 };

    private final Logger logger;

    private final KVSessionManager kvSessMgr;

    private final SessionManager tableSessMgr;

    /**
     * Creates a FailoverSessionManager.
     */
    public FailoverSessionManager(KVSessionManager kvSessMgr,
                                  SessionManager tableSessMgr,
                                  Logger logger) {

        this.kvSessMgr = kvSessMgr;
        this.tableSessMgr = tableSessMgr;
        this.logger = logger;
    }

    /**
     * Creates a new Session.
     */
    @Override
    public LoginSession createSession(
        Subject subject, String clientHost, long expireTime)
    {

        try {
            final LoginSession session =
                kvSessMgr.createSession(subject, clientHost, expireTime);

            /*
             * The persistent session manager might fail to create the
             * session.  If so, we fail over to the in-memory session
             * manager.
             */
            if (session != null) {
                return session;
            }
            logger.info("Persistent session manager failed to create " +
                        "a new session.");
        } catch (SessionAccessException sae) {

            /*
             * Some sort of failure on persistent access.  We'll fall
             * back to in-memory access.
             */
            logger.info("Persistent session manager encountered " +
                        " exception: " + sae);
        }

        return tableSessMgr.createSession(subject, clientHost, expireTime);
    }

    /**
     * Look up a Session by SessionId.
     * @return the login session if found, or else null
     */
    @Override
    public LoginSession lookupSession(LoginSession.Id sessionId)
        throws SessionAccessException {

        if (sessionId.beginsWith(PERSISTENT_PREFIX)) {
            return kvSessMgr.lookupSession(sessionId);
        }
        return tableSessMgr.lookupSession(sessionId);
    }

    /**
     * Log out the specified session.
     */
    @Override
    public LoginSession updateSessionExpiration(LoginSession.Id sessionId,
                                                long newExpireTime)
        throws SessionAccessException {

        if (sessionId.beginsWith(PERSISTENT_PREFIX)) {
            return kvSessMgr.updateSessionExpiration(sessionId,
                                                     newExpireTime);
        }
        return tableSessMgr.updateSessionExpiration(sessionId, newExpireTime);
    }

    /**
     * Log out the specified session.
     */
    @Override
    public void logoutSession(LoginSession.Id sessionId)
        throws SessionAccessException {

        if (sessionId.beginsWith(PERSISTENT_PREFIX)) {
            kvSessMgr.logoutSession(sessionId);
        } else {
            tableSessMgr.logoutSession(sessionId);
        }
    }

    @Override
    public List<LoginSession.Id> lookupSessionByUser(String userName) {
        try {
            return kvSessMgr.lookupSessionByUser(userName);
        } catch (SessionAccessException sae) {

            /*
             * Some sort of failure on persistent access.  We'll fall
             * back to in-memory access.
             */
            logger.info("Persistent session manager encountered " +
                        " exception: " + sae);
        }
        return tableSessMgr.lookupSessionByUser(userName);
    }

    @Override
    public void updateSessionSubject(LoginSession.Id sessionId, Subject subject)
        throws SessionAccessException, IllegalArgumentException {

        if (sessionId.beginsWith(PERSISTENT_PREFIX)) {
            kvSessMgr.updateSessionSubject(sessionId, subject);
        } else {
            tableSessMgr.updateSessionSubject(sessionId, subject);
        }
    }
}
