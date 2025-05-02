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
package oracle.kv.impl.admin;

import java.net.SocketException;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.security.KVStorePrincipal;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.LoginTable;
import oracle.kv.impl.security.login.UserLoginHandler;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.security.util.SubjectUtils;
import oracle.kv.impl.topo.AdminId;

/**
 * Provides logic for authentication of logins against a user database within
 * the KVStore admin.
 */

public class AdminLoginHandler extends UserLoginHandler {

    /* The AdminService for which we are handling logins */
    @SuppressWarnings("unused")
    private final AdminService adminService;

    /* The UserVerifier implementation */
    private final AdminUserVerifier userVerifier;

    private final LoginTable sessionMgr;

    /**
     * Create an AdminLoginHandler for the service.
     */
    public static AdminLoginHandler create(AdminService adminService) {

        final AdminUserVerifier adminVerifier =
            new AdminUserVerifier(adminService);
        final AdminPasswordRenewer passwordRenewer =
            new AdminPasswordRenewer(adminService);
        final AdminParams ap = adminService.getParams().getAdminParams();
        Admin admin = adminService.getAdmin();
        AdminId adminId = null;
        if (admin != null) {
            adminId = ap.getAdminId();
        } else {

            /*
             * Make up an id that is distinct from valid ids.  The constructor
             * uses the null admin test to note that the adminId value is
             * not resolvable.
             */
            adminId = new AdminId(-1);
        }

        /* Populated loginConfig from GlobalParameters */
        final GlobalParams gp = adminService.getParams().getGlobalParams();
        final LoginConfig loginConfig = LoginConfig.buildLoginConfig(gp);

        final LoginTable sessMgr =
            new LoginTable(ap.getSessionLimit(),
                           new byte[0],
                           SESSION_ID_RANDOM_BYTES);

        return new AdminLoginHandler(adminId, adminService, adminVerifier,
                                     passwordRenewer, sessMgr, loginConfig);
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
    @Override
    public LoginResult login(LoginCredentials creds,
                             String clientHost)
        throws AuthenticationFailureException {

        if (!userVerifier.userDataExists()) {
            /* No user login data yet.  Allow logins on loopback addrs */

            if (creds != null) {
                throw new AuthenticationFailureException(
                    "No user data exists, only anonymous login is allowed.");
            }
            return tryAnonymousLogin(creds, clientHost);
        }

        if (creds == null) {
            throw new AuthenticationFailureException("Authentication failed");
        }
        return super.login(creds, clientHost);
     }

    /**
     * Check whether the login should be allowed as an anonymous login.
     * This is only allowed when login requests are made from the local
     * machine with no user login data available.
     * <p>
     * The approach taken for determining that the user is local is to compare
     * the IP address reported by RMI for the source of the connection with the
     * addresses configured for network interfaces on this machine.  In theory,
     * an attacker could construct packets to make it look as though the client
     * were local, but in practice it is very difficult to take advantage of
     * this for a TCP connection.
     * <p>
     * An alternate approach could be to bind the login interface only on the
     * loopback network interface initially and to rebind on a wildcard address
     * once anonymous logins are no longer permitted, but the logistics of
     * rebinding in a timely fashion make this approach less reliable, so for
     * now we will accept the slightly less secure option.
     */
    LoginResult tryAnonymousLogin(
        @SuppressWarnings("unused") LoginCredentials creds,
        String clientHost)
        throws AuthenticationFailureException {

        boolean isLocal = false;
        try {
            isLocal = SecurityUtils.isLocalHost(clientHost);
        } catch (SocketException se) {
            logger.info("Encountered exception while checking whether " +
                        clientHost + " is local: " + se);
        } catch (SecurityException se) {
            /* Shouldn't be able to happen */
            logger.info("Encountered exception while checking whether " +
                        clientHost + " is local: " + se);
        }

        if (!isLocal) {
            logger.info("anonymous client login from " + clientHost +
                        ": host is not a local address");
            throw new AuthenticationFailureException(
                "Anonymous login allowed only from local host");
        }

        /* Allow it */
        return new LoginResult(
            createLoginSession(makeAdminSubject(), clientHost));
    }

    /**
     * Internal constructor
     */
    private AdminLoginHandler(AdminId ownerId,
                              AdminService adminService,
                              AdminUserVerifier adminVerifier,
                              AdminPasswordRenewer passwordRenewer,
                              LoginTable sessionManager,
                              LoginConfig loginConfig) {
        super(ownerId,
              (adminService.getAdmin() == null), /* localOwnerId */
              adminVerifier, passwordRenewer, sessionManager, loginConfig,
              adminService.getLogger());
        this.adminService = adminService;
        this.userVerifier = adminVerifier;
        this.sessionMgr = sessionManager;
    }

    /**
     * Create a Subject that has the Admin role, but no user role.
     * This is used to support local anonymous logins.
     */
    private Subject makeAdminSubject() {

        Set<KVStorePrincipal> adminPrincipals = new HashSet<>();
        adminPrincipals.add(KVStoreRolePrincipal.SYSADMIN);
        adminPrincipals.add(KVStoreRolePrincipal.PUBLIC);
        return SubjectUtils.createSubject(adminPrincipals);
    }

    /**
     * Update the session limit
     */
    public boolean updateSessionLimit(final int newLimit) {
        return sessionMgr.updateSessionLimit(newLimit);
    }
}
