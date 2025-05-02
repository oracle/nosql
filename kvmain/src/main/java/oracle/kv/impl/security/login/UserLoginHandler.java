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

import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.security.PasswordExpiredException;
import oracle.kv.impl.security.PasswordRenewResult;
import oracle.kv.impl.security.PasswordRenewer;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.security.login.UserLoginCallbackHandler.UserSessionInfo;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMDChange;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElementType;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.server.LoggerUtils.KVAuditInfo;
import oracle.kv.impl.util.server.LoggerUtils.SecurityLevel;

/**
 * Provides logic for authentication of logins against a user database.
 */

public class UserLoginHandler {

    /**
     * Number of bytes in new session id values.
     */
    public static final int SESSION_ID_RANDOM_BYTES = 16;

    /* Default account lockout checking interval in seconds */
    private static final int DEF_ACCT_ERR_LCK_INT = 10 * 60;

    /* Default account lockout checking threshold count */
    private static final int DEF_ACCT_ERR_LCK_CNT = 10;

    /* Default account lockout period in seconds */
    private static final int DEF_ACCT_ERR_LCK_TMO = 10 * 60;

    /* Identifies the component that is hosting this instance */
    private volatile ResourceId ownerId;

    /*
     * If true, indicates that the ownerId is locally generated, and is not
     * resolvable by other storage nodes.
     */
    private volatile boolean localOwnerId;

    /* The underlying session manager object */
    private final SessionManager sessMgr;

    /* An object for translating user identities into Subject descriptions */
    private final UserVerifier userVerifier;

    private final PasswordRenewer passwordRenewer;

    /*
     * The configured lifetime value for newly created sessions, in ms.
     * See SessionManager for a description of session lifetime.
     */
    private volatile long sessionLifetime;

    /*
     * The configuration setting indicating whether a client may request that
     * their session be extended.
     */
    private volatile boolean allowExtension;

    protected volatile Logger logger;

    /*
     * Tracks login errors, and prevents login when an excessive number of
     * login attempts have failed.
     */
    private volatile LoginErrorTracker errorTracker;

    /**
     * Configuration for the login handler.
     */
    public static class LoginConfig {
        /* session lifetime in ms */
        private long sessionLifetime;
        /* whether to allow sessions to be extended */
        private boolean allowExtension;
        /* account error lockout interval in ms */
        private long acctErrLockoutInt;
        /* account error threshold for lockout */
        private int acctErrLockoutCnt;
        /* account error lockout timeout in ms */
        private long acctErrLockoutTMO;

        public LoginConfig() {
        }

        public LoginConfig setSessionLifetime(long lifetimeMs) {
            this.sessionLifetime = lifetimeMs;
            return this;
        }

        public LoginConfig setAllowExtension(boolean allowExtend) {
            this.allowExtension = allowExtend;
            return this;
        }

        public LoginConfig setAcctErrLockoutInt(long lockoutIntervalMs) {
            this.acctErrLockoutInt = lockoutIntervalMs;
            return this;
        }

        public LoginConfig setAcctErrLockoutCnt(int lockoutCount) {
            this.acctErrLockoutCnt = lockoutCount;
            return this;
        }

        public LoginConfig setAcctErrLockoutTMO(long lockoutTMOMs) {
            this.acctErrLockoutTMO = lockoutTMOMs;
            return this;
        }

        @Override
        public LoginConfig clone() {
            final LoginConfig dup = new LoginConfig();

            dup.sessionLifetime = sessionLifetime;
            dup.allowExtension = allowExtension;
            dup.acctErrLockoutInt = acctErrLockoutInt;
            dup.acctErrLockoutCnt = acctErrLockoutCnt;
            dup.acctErrLockoutTMO = acctErrLockoutTMO;
            return dup;
        }

        /**
         * Try to populate a LoginConfig using the values in the specified
         * GlobalParams object. If the GlobalParams is null, a LoginConfig with
         * all zero values is returned.
         *
         * @param gp GlobalParam object
         */
        public static LoginConfig buildLoginConfig(final GlobalParams gp) {
            final LoginConfig config = new LoginConfig();
            if (gp != null) {
                final long sessionLifetimeInMillis =
                    gp.getSessionTimeoutUnit().toMillis(gp.getSessionTimeout());
                final int acctErrLockoutTMOInSeconds = (int)
                    gp.getAcctErrLockoutTimeoutUnit().toMillis(
                        gp.getAcctErrLockoutTimeout());
                final int acctErrLockoutIntInSeconds = (int)
                    gp.getAcctErrLockoutThrIntUnit().toMillis(
                        gp.getAcctErrLockoutThrInt());

                config.setAcctErrLockoutCnt(gp.getAcctErrLockoutThrCount()).
                       setSessionLifetime(sessionLifetimeInMillis).
                       setAllowExtension(gp.getSessionExtendAllow()).
                       setAcctErrLockoutTMO(acctErrLockoutTMOInSeconds).
                       setAcctErrLockoutInt(acctErrLockoutIntInSeconds);
            }
            return config;
        }
    }

    public UserLoginHandler(ResourceId ownerId,
                            boolean localOwnerId,
                            UserVerifier userVerifier,
                            PasswordRenewer passwordRenewer,
                            SessionManager sessionManager,
                            LoginConfig loginConfig,
                            Logger logger) {
        this.logger = logger;
        this.ownerId = ownerId;
        this.localOwnerId = localOwnerId;
        this.sessMgr = sessionManager;
        this.userVerifier = userVerifier;
        this.passwordRenewer = passwordRenewer;
        this.sessionLifetime = loginConfig.sessionLifetime;
        this.allowExtension = loginConfig.allowExtension;
        this.errorTracker = makeErrorTracker(loginConfig);
    }

    private LoginErrorTracker makeErrorTracker(final LoginConfig loginConfig) {
        return
            new LoginErrorTracker(
                (loginConfig.acctErrLockoutInt == 0 ?
                 DEF_ACCT_ERR_LCK_INT : loginConfig.acctErrLockoutInt),
                (loginConfig.acctErrLockoutCnt == 0 ?
                 DEF_ACCT_ERR_LCK_CNT : loginConfig.acctErrLockoutCnt),
                (loginConfig.acctErrLockoutTMO == 0 ?
                 DEF_ACCT_ERR_LCK_TMO : loginConfig.acctErrLockoutTMO),
                logger);
    }

    /**
     * Log a user into the database.
     * @param creds the credential object used for login.  The actual type
     *        of the object will vary depending on login mechanism.
     * @param clientHost the host from which the client request was received,
     *        if this is a forwarded login request.
     * @return a LoginResult
     * @throws AuthenticationFailureException if the LoginCredentials are
     *        not valid
     */
    public LoginResult login(LoginCredentials creds, String clientHost)
         throws AuthenticationFailureException {

        if (creds instanceof ProxyCredentials) {
            /* ProxyCredentials can be used only through proxyLogin() */
            throw new AuthenticationFailureException (
                "Invalid use of ProxyCredentials.");
        }

        return loginInternal(creds, clientHost);
    }

    /**
     * Only used to log a user whose password is expired into the database
     * after renewing the password, and will fail in any other case.  This is
     * used to give a change for users whose passwords have been expired to
     * renew their passwords.
     *
     * @param oldCreds the credential object used for login.
     * @param clientHost the host from which the client request was received,
     *        if this is a forwarded login request.
     * @return a LoginResult
     * @throws AuthenticationFailureException if the old LoginCredentials are
     *         not valid
     */
    public LoginResult renewPasswordLogin(LoginCredentials oldCreds,
                                          char[] newPassword,
                                          String clientHost)
        throws AuthenticationFailureException {

        if (passwordRenewer == null) {
            throw new AuthenticationFailureException(
                new UnsupportedOperationException("Could not renew password"));
        }

        if (oldCreds instanceof ProxyCredentials) {
            /* ProxyCredentials can be used only through proxyLogin() */
            throw new AuthenticationFailureException (
                "Invalid use of ProxyCredentials.");
        }

        /* Verify old credentials are valid but expired */
        if (oldCreds == null) {
            throw new AuthenticationFailureException (
                "No credentials provided.");
        }

        if (errorTracker.isAccountLocked(oldCreds.getUsername(), clientHost)) {
            throw new AuthenticationFailureException (
                "User account is locked.");
        }
        final String userName = oldCreds.getUsername();

        String renewMessage = null;
        try {
            userVerifier.verifyUser(oldCreds, null);
        } catch (PasswordExpiredException e) {
            final PasswordRenewResult result =
                passwordRenewer.renewPassword(userName, newPassword);
            renewMessage = result.getMessage();
            if (result.isSuccess()) {
                final PasswordCredentials newCreds =
                    new PasswordCredentials(userName, newPassword);
                return loginInternal(newCreds, clientHost);
            }
        }

        throw new AuthenticationFailureException("Renew password failed" +
            (renewMessage != null ? ": " + renewMessage : ""));
    }

    /**
     * Log a user into the database, without password checking.
     *
     * @param creds the credential object used for login, which identifies.
     *        the user to be logged in.
     * @param clientHost the host from which the client request was received,
     *        if this is a forwarded login request.
     * @return a LoginResult
     * @throws AuthenticationFailureException if the LoginCredentials are
     *         not valid
     */
    public LoginResult proxyLogin(ProxyCredentials creds, String clientHost)
         throws AuthenticationFailureException {

        return loginInternal(creds, clientHost);
    }

    /**
     * Request that a login token be replaced with a new token that has a later
     * expiration than that of the original token.  Depending on system policy,
     * this might not be allowed.  This is controlled by the
     * sessionTokenExpiration security configuration parameter.
     * TODO: link to discussion of system policy
     *
     * @return null if the LoginToken is not valid or if session extension is
     *          not allowed - otherwise, return a new LoginToken.  The
     *          returned LoginToken will use the same session id as the
     *          original token, but the client should use the new LoginToken
     *          for subsequent requests.
     * @throws SessionAccessException if an operational failure prevents
     *   completion of the operation
     */
    public LoginToken requestSessionExtension(LoginToken loginToken)
        throws SessionAccessException {

        if (loginToken == null) {
            return null;
        }

        if (!allowExtension) {
            logger.fine("Session extend not allowed");
            return null;
        }

        final LoginSession session =
            sessMgr.lookupSession(
                new LoginSession.Id(
                    loginToken.getSessionId().getIdValue()));


        if (session == null || session.isExpired()) {
            logger.info("Session " + loginToken.getSessionId().hashId() +
                        ": extend failed due to expiration");

            return null;
        }

        long newExpire;
        if (sessionLifetime == 0L) {
            newExpire = 0L;
        } else {
            newExpire = System.currentTimeMillis() + sessionLifetime;
        }

        final LoginSession newSession =
            sessMgr.updateSessionExpiration(session.getId(), newExpire);

        if (newSession == null) {
            logger.info("Session " + session.getId().hashId() +
                        ": update failed");
            return null;
        }

        final SessionId sid = (newSession.isPersistent()) ?
            new SessionId(newSession.getId().getValue()) :
            new SessionId(newSession.getId().getValue(),
                          getScope(), ownerId);

        logger.fine(() -> "Extended session " + session.getId().hashId() +
                          " to new session " + newSession.getId().hashId() +
                          " with expiration " + newExpire + " ms");
        return new LoginToken(sid, newSession.getExpireTime());
    }

    /**
     * Check an existing LoginToken for validity.
     * @return a Subject describing the user, or null if not valid
     * @throws SessionAccessException
     */
    public Subject validateLoginToken(LoginToken loginToken)
        throws SessionAccessException {

        if (loginToken == null) {
            return null;
        }

        final SessionId sessId = loginToken.getSessionId();
        final LoginSession session =
            sessMgr.lookupSession(new LoginSession.Id(sessId.getIdValue()));

        if (session == null) {
            logger.info("Failed to find the session with " + ownerId +
                        " login token " + loginToken.hashId());
            return null;
        }
        if (session.isExpired()) {
            logger.info(
                "User login token " + loginToken.hashId() + " is expired");
            return null;
        }

        return userVerifier.verifyUser(session.getSubject());
    }

    /**
     * Log out the login token.  The LoginToken will no longer be usable for
     * accessing secure object interfaces.  If the session is already logged
     * out, this is treated as a a successful operation.  If the LoginToken
     * is not recognized, this may be because it was logged out earlier and
     * flushed from memory, and so this case will also be treated as successful.
     * If the LoginToken is recognized, but the session
     * cannot be modified because the session-containing shard is not
     * writable,
     */
    public void logout(LoginToken loginToken)
        throws AuthenticationRequiredException, SessionAccessException {

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

    public void updateSessionSubject(SecurityMDChange mdChange)
        throws SessionAccessException {

        if (mdChange.getElementType() == SecurityElementType.KVSTOREUSER) {
            final KVStoreUser user = (KVStoreUser)mdChange.getElement();
            final Subject userSubject = user.makeKVSubject();

            for (LoginSession.Id sessionId :
                 sessMgr.lookupSessionByUser(user.getName())) {
                sessMgr.updateSessionSubject(sessionId, userSubject);
            }
        }
    }

    protected LoginToken createLoginSession(Subject subject,
                                            String clientHost) {
        return createLoginSession(subject, clientHost, -1);
    }

    protected LoginToken createLoginSession(Subject subject,
                                            String clientHost,
                                            long expireTime) {
        if (expireTime == -1) {
            expireTime =
                (sessionLifetime != 0L) ?
                (System.currentTimeMillis() + sessionLifetime) :
                0;
        }

        final LoginSession session =
            sessMgr.createSession(subject, clientHost, expireTime);

        final SessionId sid = (session.isPersistent()) ?
            new SessionId(session.getId().getValue()) :
            new SessionId(session.getId().getValue(), getScope(), ownerId);
        return new LoginToken(sid, session.getExpireTime());
    }

    protected IdScope getScope() {
        return localOwnerId ? IdScope.LOCAL : IdScope.STORE;
    }

    /**
     * Update the global session and login config.
     */
    public void updateConfig(final LoginConfig config) {
        this.sessionLifetime = config.sessionLifetime;
        this.allowExtension = config.allowExtension;
        this.errorTracker = makeErrorTracker(config);
    }

    /**
     * Log a user into the database.
     *
     * @param creds the credential object used for login.  The actual type
     *        of the object will vary depending on login mechanism.
     * @param clientHost the host from which the client request was received,
     *        if this is a forwarded login request.
     * @return a LoginResult
     * @throws AuthenticationFailureException if the LoginCredentials are
     *        not valid
     */
    private LoginResult loginInternal(LoginCredentials creds, String clientHost)
         throws AuthenticationFailureException {

         Subject subject = null;

         if (creds == null) {
             throw new AuthenticationFailureException (
                 "No credentials provided.");
         }

         if (errorTracker.isAccountLocked(creds.getUsername(), clientHost)) {
             throw new AuthenticationFailureException (
                 "User account is locked.");
         }
         final String userName = creds.getUsername();
         final UserLoginCallbackHandler loginHandler =
             new UserLoginCallbackHandler(logger);

         try {
             subject = userVerifier.verifyUser(creds, loginHandler);
         } catch (PasswordExpiredException e) {
             handleLoginFailure(userName, clientHost, e);
         }

         if (subject == null) {
             handleLoginFailure(userName, clientHost, null);
         }
         errorTracker.noteLoginSuccess(userName, clientHost);
         logger.log(SecurityLevel.SEC_INFO,
                    KVAuditInfo.success(userName, clientHost, "LOGIN"));
         final UserSessionInfo sessInfo = loginHandler.getUserSessionInfo();

         /*
          * Initialized with -1, if no expire time acquired from underlying
          * authenticator, will use system session lifetime to create session.
          */
         long sessionExpireTime = -1;
         if (sessInfo != null) {
             sessionExpireTime = sessInfo.getExpireTime();
         }
         final LoginToken loginToken =
             createLoginSession(subject, clientHost, sessionExpireTime);

         LoginResult loginResult = loginHandler.getLoginResult();
         if (loginResult == null) {
             loginResult = new LoginResult();
         }

         return loginResult.setLoginToken(loginToken);
     }

    private void handleLoginFailure(String userName,
                                    String clientHost,
                                    PasswordExpiredException e)
        throws AuthenticationFailureException {

        errorTracker.noteLoginError(userName, clientHost);
        logger.log(SecurityLevel.SEC_WARNING,
                   KVAuditInfo.failure(userName, clientHost,
                                       "LOGIN", "Login Failed"));
        if (e != null) {
            throw e;
        }
        throw new AuthenticationFailureException ("Authentication failed");
    }

    /**
     * Changes the ownerId for this handler.  When changed, it is assumed
     * that the id is no longer a local id.  This is used when an Admin is
     * deployed.
     */
    public void updateOwner(final ResourceId id) {
        this.ownerId = id;
        this.localOwnerId = false;
    }

    public ResourceId getOwnerId() {
        return this.ownerId;
    }

    /**
     * Reset login handler logger.
     */
    public void resetLogger(Logger newLogger) {
        this.logger = newLogger;
    }
}
