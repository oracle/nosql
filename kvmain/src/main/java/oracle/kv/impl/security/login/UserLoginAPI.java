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

import java.rmi.RemoteException;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.util.registry.RemoteAPI;

/**
 * An RMI interface that provides user login capabilities for the KVStore
 * database.  KVUserLogin is implemented and exported by SNA, RepNode and Admin
 * components in a storage node with an InterfaceType of LOGIN.
 */

public final class UserLoginAPI extends RemoteAPI {

    /* Null value that will be filled in by proxyRemote */
    private static final AuthContext NULL_CTX = null;

    private UserLogin proxyRemote;

    private UserLoginAPI(UserLogin remote,
                         LoginHandle loginHdl,
                         short serialVersion)
        throws RemoteException {

        super(remote);
        this.proxyRemote = (loginHdl != null) ?
            ContextProxy.create(remote, loginHdl, serialVersion) :
            remote;
    }

    public static UserLoginAPI wrap(UserLogin remote)
        throws RemoteException {

        return wrap(remote, null);
    }

    public static UserLoginAPI wrap(UserLogin remote, LoginHandle loginHdl)
        throws RemoteException {

        return new UserLoginAPI(remote, loginHdl, remote.getSerialVersion());
    }

    /** Returns the underlying remote proxy. */
    public UserLogin getProxy() {
        return proxyRemote;
    }

    /**
     * Log a user into the database.  A login may be persistent, in which
     * case it is can be used on any rep node, and does not depend upon the
     * node that served the token being up for others to make use of the token,
     * or it can be non-persistent, in which case it remains valid only while
     * the serving component is up.  For the non-persistent case, it can be
     * either local or non-local.  In the case of local tokens, it is valid
     * only for use on the SN that performed the login. Non-local tokens may
     * be used on SNs other than the one that originated it.  Note that a
     * non-persistent token is quicker to process and requires fewer resources,
     * but is less resilient in the case of a component restart and less
     * efficient and reliable for logging out if the login token is used with
     * multiple components.  If the login is authenticated but the
     * login cannot be made persistent, the login is treated as successful
     * and the returned login token is noted as non-persistent.
     *
     * @param creds the credential object used for login.  The actual type
     *        of the object will vary depending on login mechanism.
     * @return a LoginResult
     * @throws AuthenticationFailureException if the LoginCredentials are
     *         not valid
     */
    public LoginResult login(LoginCredentials creds)
        throws AuthenticationFailureException, RemoteException {

        return proxyRemote.login(creds, getSerialVersion());
    }

    /**
     * @see UserLogin#proxyLogin(ProxyCredentials, AuthContext, short)
     */
    public LoginResult proxyLogin(ProxyCredentials creds)
        throws AuthenticationFailureException,
               AuthenticationRequiredException,
               UnauthorizedException,
               SessionAccessException,
               RemoteException {

        return proxyRemote.proxyLogin(creds, NULL_CTX, getSerialVersion());
    }

    /**
     * @see UserLogin#renewPasswordLogin(PasswordCredentials, char[], short)
     */
    public LoginResult renewPasswordLogin(PasswordCredentials oldCreds,
                                          char[] newPassword)
        throws AuthenticationFailureException, RemoteException {

        return proxyRemote.renewPasswordLogin(oldCreds, newPassword,
                                              getSerialVersion());
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
     */
    public LoginToken requestSessionExtension(LoginToken loginToken)
        throws SessionAccessException, RemoteException {

        return proxyRemote.requestSessionExtension(loginToken,
                                                   getSerialVersion());
    }

    /**
     * Check an existing LoginToken for validity.
     * @return a Subject describing the user, or null if not valid
     * @throws SessionAccessException if unable to access the session
     *   information associated with the token
     */
    public Subject validateLoginToken(LoginToken loginToken)
        throws SessionAccessException, RemoteException {

        return proxyRemote.validateLoginToken(loginToken,
                                              NULL_CTX, getSerialVersion());
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
        throws AuthenticationRequiredException, SessionAccessException,
               RemoteException {

        proxyRemote.logout(loginToken, getSerialVersion());
    }
}
