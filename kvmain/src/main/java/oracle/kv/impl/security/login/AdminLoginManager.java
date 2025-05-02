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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Logger;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.security.kerberos.KerberosLoginHelper;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * AdminLoginManager is an implementation of LoginManager that is intended to
 * manage a login to a KVStore Admin.  Much of the implementation is shared
 * with RepNodeLoginManager through the common UserLoginManager base class.
 */

public class AdminLoginManager extends UserLoginManager {

    private final Logger logger;

    /**
     * Creates an instance with no login handle.  Before using it, it should
     * be initialized.  This can be done with a call to initialize or with
     * a call to bootstrap.
     */
    public AdminLoginManager(String username,
                             boolean autoRenew,
                             Logger logger) {
        super(username, autoRenew);
        this.logger = logger;
    }

    /**
     * Initializes the login manager for use.
     *
     * @param token a LoginToken for use in accessing the admin interface
     * @param registryHost the host from which the token originated
     * @param registryPort the port from which the token originated
     */
    public void initialize(LoginToken token,
                           String registryHost,
                           int registryPort) {
        init(new AdminLoginHandle(token, registryHost, registryPort));
    }

    /**
     * Performs the initial login.
     *
     * @return true if we were able to authenticate to the admin, or false
     * if a communication problem prevented it.
     * @throws AuthenticationFailureException if the login credentials are
     * not valid
     */
    public boolean bootstrap(String registryHost,
                             int registryPort,
                             LoginCredentials loginCreds)
        throws AuthenticationFailureException {

        return bootstrap(new String[] { registryHost + ":" + registryPort },
                         loginCreds);
    }

    /**
     * Performs the initial login.
     *
     * @return true if we were able to authenticate to the admin, or false
     * if a communication problem prevented it.
     * @throws AuthenticationFailureException if the login credentials are
     * not valid
     * @throws IllegalArgumentException if the registryHostPorts array is
     * null or if any of the contained Strings are null, or do not contain
     * a valid host:port string.
     */
    public boolean bootstrap(String[] registryHostPorts,
                             LoginCredentials loginCreds)
        throws AuthenticationFailureException {

        final HostPort[] hostPorts = HostPort.parse(registryHostPorts);

        for (HostPort hostPort : hostPorts) {

            final String registryHost = hostPort.hostname();
            final int registryPort = hostPort.port();

            try {
                final UserLoginAPI loginAPI =
                    RegistryUtils.getAdminLogin(registryHost, registryPort,
                                                (LoginManager) null, logger);

                final LoginResult login;
                if (loginCreds instanceof KerberosClientCreds) {
                    login = KerberosLoginHelper.kerberosLogin(
                        loginAPI, (KerberosClientCreds)loginCreds,
                        hostPort.hostname());
                } else {
                    login = loginAPI.login(loginCreds);
                }

                if (login.getLoginToken() == null) {
                    continue;
                }

                final LoginHandle loginHandle =
                    new AdminLoginHandle(login.getLoginToken(),
                                         registryHost, registryPort);
                init(loginHandle);
                return true;
            } catch (NotBoundException nbe) {
                /* Try the next option, if any */
                continue;
            } catch (RemoteException re) {
                /* Try the next option, if any */
                continue;
            }
        }

        return false;
    }

    /**
     * Performs renew password login.
     *
     * @param oldCreds old credentials which expires
     * @param newPassword password for renewal
     *
     * @return true if we are able to renew password and successfully
     * authenticate to the admin.
     * @throws AuthenticationFailureException if renew password failed or old
     * credentials passed in is not valid.
     * 
     */
    public boolean renewPassword(String registryHost,
                                 int registryPort,
                                 PasswordCredentials oldCreds,
                                 char[] newPassword)
        throws AuthenticationFailureException {

        try {
            final UserLoginAPI loginAPI =
                RegistryUtils.getAdminLogin(registryHost, registryPort,
                                            (LoginManager) null, logger);

            final LoginResult login =
                loginAPI.renewPasswordLogin(oldCreds, newPassword);

            if (login.getLoginToken() != null) {
                final LoginHandle loginHandle =
                        new AdminLoginHandle(login.getLoginToken(),
                                             registryHost, registryPort);
                init(loginHandle);
                return true;
            }
        } catch (NotBoundException nbe) {
            return false;
        } catch (RemoteException re) {
            return false;
        }

        return false;
    }

    /**
     * AdminLoginHandle is intended specifically for use within the
     * AdminLoginManager class.
     */
    private final class AdminLoginHandle
        extends AbstractUserLoginHandle {

        private final String hostname;
        private final int registryPort;

        private AdminLoginHandle(LoginToken loginToken,
                                 String hostname,
                                 int registryPort) {
            super(loginToken);
            this.hostname = hostname;
            this.registryPort = registryPort;
        }

        @Override
        protected UserLoginAPI getLoginAPI()
            throws RemoteException {

            try {
                return RegistryUtils.getAdminLogin(hostname, registryPort,
                                                   (LoginManager) null,
                                                   logger);
            } catch (NotBoundException nbe) {
                throw new RemoteException(
                    "login interface not bound", nbe);
            }
        }

        /**
         * Report whether this login handle supports authentication to the
         * specified type of resource.
         */
        @Override
        public boolean isUsable(ResourceType rtype) {
            return rtype.equals(ResourceType.ADMIN);
        }
    }
}
