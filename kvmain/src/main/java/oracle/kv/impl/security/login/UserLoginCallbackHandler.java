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

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;

/**
 * Callback handler implementation that passed to underlying user login
 * authenticators. Currently only support LoggingCallback and
 * LoginResultCallback.
 */
public class UserLoginCallbackHandler {

    /* Login result produced by user authenticator */
    private LoginResult loginResult;

    /* Session information produced by user authenticator */
    private UserSessionInfo sessInfo;

    private final Logger logger;

    public UserLoginCallbackHandler(Logger logger) {
        this.loginResult = null;
        this.sessInfo = null;
        this.logger = logger;
    }

    public void handle(Callback callback) {
        if (callback instanceof LoginResultCallback) {
            final LoginResultCallback ck = (LoginResultCallback) callback;
            loginResult = ck.getLoginResult();
        } else if (callback instanceof LoggingCallback) {
            final LoggingCallback ck = (LoggingCallback) callback;
            logger.log(ck.getLevel(), ck.getMessage());
        }  else if (callback instanceof UserSessionCallback) {
            final UserSessionCallback ck = (UserSessionCallback) callback;
            sessInfo = ck.getSessionInfo();
        } else {
            logger.warning("Handle unsupported callback: " +
                           callback.getClass().getName());
        }
    }

    /**
     * Get login result. May return null if underlying authenticator does not
     * produce a login result.
     */
    public LoginResult getLoginResult() {
        return loginResult;
    }

    /**
     * Get user session information. May return null if underlying authenticator
     * does not produce session information.
     */
    public UserSessionInfo getUserSessionInfo() {
        return sessInfo;
    }

    /**
     * Login result callback. It is used for underlying authenticator to
     * rebuild the login results.
     */
    public interface LoginResultCallback extends Callback {
        public LoginResult getLoginResult();
    }

    /**
     * User session callback. It is used to allow the underlying authenticator
     * to supply information about the associated user session.
     */
    public interface UserSessionCallback extends Callback {
        public UserSessionInfo getSessionInfo();
    }

    /**
     * Logging callback. It is used for underlying authenticator logging.
     */
    public static class LoggingCallback implements Callback {

        private Level level;

        private String message;

        public LoggingCallback() {
            level = null;
            message = null;
        }

        public LoggingCallback(Level level, String message) {
            this.level = level;
            this.message = message;
        }

        public LoggingCallback setLevel(Level level) {
            this.level = level;
            return this;
        }

        public LoggingCallback setMessage(String message) {
            this.message = message;
            return this;
        }

        public Level getLevel() {
            return level;
        }

        public String getMessage() {
            return message;
        }
    }

    /**
     * The user subject and expiration time associated with a login.
     */
    public static class UserSessionInfo {
        private final Subject subject;
        private final long expireTime;

        public UserSessionInfo(Subject subj, long expireTime) {
            this.subject = subj;
            this.expireTime = expireTime;
        }

        public Subject getSubject() {
            return this.subject;
        }

        public long getExpireTime() {
            return this.expireTime;
        }
    }
}
