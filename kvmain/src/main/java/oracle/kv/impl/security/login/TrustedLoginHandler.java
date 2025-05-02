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

package oracle.kv.impl.security.login;

import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.security.util.SubjectUtils;
import oracle.kv.impl.topo.ResourceId;

/**
 * Provides login capabilities for infrastructure components.
 * This serves as the implementation layer behind TrustedLoginImpl.
 */
public class TrustedLoginHandler {

    /**
     * The number of login sessions that this can manage, but default.
     * This is used in the case of an unregistered SNA, so this is probably
     * total overkill.
     */
    private static final int CAPACITY = 10000;

    private static final int SESSION_ID_RANDOM_BYTES = 16;

    /* Our session manager */
    private final LoginTable sessMgr;

    /* The identifier for the owning component */
    private final ResourceId ownerId;

    /* If true, the ownerId can only be interpreted locally */
    private final boolean localId;

    /*
     * The lifetime for newly created sessions, in ms.  The value 0L means
     * that sessions do not expire.
     */
    private volatile long sessionLifetime;

    /**
     * Constructor for use by the unregistered SNA.
     */
    public TrustedLoginHandler(ResourceId ownerId, boolean localId) {
        this(ownerId, localId, 0L, CAPACITY);
    }

    /**
     * Common constructor for use by the registered SNA and the unregistered
     * SNA constructor.
     */
    public TrustedLoginHandler(ResourceId ownerId,
                               boolean localId,
                               long sessionLifeTime,
                               int sessionLimit) {
        this.ownerId = ownerId;
        this.localId = localId;
        this.sessionLifetime = sessionLifeTime;
        this.sessMgr =
            new LoginTable(sessionLimit,
                           new byte[0],
                           SESSION_ID_RANDOM_BYTES);
    }

    /**
     * Obtain a login token that identifies the caller as an infrastructure
     * component when accessing the RMI interfaces of this component.
     *
     * @return a login result
     */
    LoginResult loginInternal(String clientHost) {

        final long expireTime = (sessionLifetime == 0L) ? 0L :
            System.currentTimeMillis() + sessionLifetime;
        final LoginSession session =
            sessMgr.createSession(makeInternalSubject(), clientHost,
                                  expireTime);

        return new LoginResult(
            new LoginToken(new SessionId(
                               session.getId().getValue(),
                               localId ? IdScope.LOCAL : IdScope.STORE,
                               ownerId),
                           session.getExpireTime()));
    }

    /**
     * Check an existing LoginToken for validity.  This is intended for use
     * with locally generated tokens.
     *
     * @return a Subject describing the user, or null if not valid
     */
    Subject validateLoginToken(LoginToken loginToken, Logger logger) {

        if (loginToken == null) {
            logMessage(logger, Level.INFO,
                       "Passing an invalid internal login token");
            return null;
        }

        final LoginSession session =
            sessMgr.lookupSession(new LoginSession.Id(
                                      loginToken.getSessionId().getIdValue()));
        if (session == null) {
            logMessage(logger, Level.INFO,
                "Failed to find the internal session for login token " +
                loginToken.hashId());
            return null;
        }
        if (session.isExpired()) {
            logMessage(logger, Level.FINE, "Internal login token " +
                loginToken.hashId() + " is expired");
            return null;
        }
        return session.getSubject();
    }

    /**
     * Log out the login token.  The LoginToken will no longer be usable for
     * accessing secure interfaces.
     *
     * @throws AuthenticationRequiredException if the login token is not valid,
     *         or is already logged out.
     */
    void logout(LoginToken loginToken) {

        if (loginToken == null) {
            throw new AuthenticationRequiredException(
                "LoginToken is null",
                true /* isReturnSignal */);
        }

        final LoginSession session =
            sessMgr.lookupSession(new LoginSession.Id(
                                      loginToken.getSessionId().getIdValue()));
        if (session == null || session.isExpired()) {
            throw new AuthenticationRequiredException(
                "session is not valid",
                true /* isReturnSignal */);
        }
        sessMgr.logoutSession(session.getId());
    }

    /**
     * Update the session limits.
     */
    public boolean updateSessionLimit(final int newLimit) {
        return sessMgr.updateSessionLimit(newLimit);
    }

    /**
     * Update the session lifetime.
     */
    public boolean updateSessionLifetime(final long newLifetime) {
        if (newLifetime == sessionLifetime) {
            return false;
        }
        sessionLifetime = newLifetime;
        return true;
    }

    private Subject makeInternalSubject() {
        return SubjectUtils.createSubject(
            Collections.singleton(KVStoreRolePrincipal.INTERNAL));
    }

    private void logMessage(Logger logger, Level level, String message) {
        if (logger != null) {
            logger.log(level, message);
        }
    }
}
