/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static oracle.kv.impl.async.StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.security.Principal;
import java.util.Set;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.LoginCredentials;
import oracle.kv.LoginCredentials.LoginCredentialsType;
import oracle.kv.LoginCredentialsTypeFinders;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.TestProcessFaultHandler;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.PasswordExpiredException;
import oracle.kv.impl.security.PasswordRenewResult;
import oracle.kv.impl.security.PasswordRenewer;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.ScaffoldUserVerifier;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.security.login.UserLoginHandler.LoginConfig;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Test;

/**
 * Test the UserLogin interface and surrounding classes.
 */
public class UserLoginTest extends LoginTestBase {

    private static final int N_ID_BYTES = 16;

    public static final String ADMIN_USER_NAME = "admin";
    public static final char[] ADMIN_USER_PASSWORD =
        "NoSql00__hello".toCharArray();

    private static final String USER_LOGIN = "UserLogin";
    private int registryPort = 0;
    private Registry registry;
    private ListenHandle registryHandle;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        registryPort =
            new FreePortLocator("localHost", 5050, 5100).next();

        registry = TestUtils.createRegistry(registryPort);
        if (AsyncRegistryUtils.serverUseAsync) {
            registryHandle = TestUtils.createServiceRegistry(registryPort);
        }
    }

    @Override
    public void tearDown()
        throws Exception {

        if (registry != null) {
            TestUtils.destroyRegistry(registry);
        }
        if (registryHandle != null) {
            registryHandle.shutdown(true);
        }
        super.tearDown();
    }

    @Test
    public void testLogin()
        throws Exception {

        /* Setup */
        final ULFH ulfh = new ULFH(logger);
        final AdminId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier,
                                 null /* password renewer */,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);
        final UserLoginImpl uli = new UserLoginImpl(ulfh, ulh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind("localhost", registryPort, USER_LOGIN,
                                 rid, export(uli), DEFAULT_CSF, DEFAULT_SSF,
                                 USER_LOGIN_TYPE_FAMILY,
                                 () -> new AsyncUserLoginResponder(
                                     uli, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final UserLoginAPI ulAPI =
            RegistryUtils.getUserLogin(null /* storeName */, "localhost",
                                       registryPort, USER_LOGIN,
                                       ResourceType.ADMIN, null /* loginMgr */,
                                       null /* clientId */,
                                       Protocols.getDefault(),
                                       false /* ignoreWrongType */,
                                       logger);

        /* login() testing */

        /* successful login */
        final LoginCredentials goodCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    ADMIN_USER_PASSWORD);
        final LoginResult lr = ulAPI.login(goodCreds);
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());

        /* bad user and password */
        try {
            final LoginCredentials badCreds =
                new PasswordCredentials("unknown", "nopass".toCharArray());
            ulAPI.login(badCreds);
            fail("expected exception");
        } catch (AuthenticationFailureException fe) {
            assertTrue(true); /* expected */
        } catch (Exception e) {
            fail("unexpected exception: " + e);
        }

        /* proxy credentials not allowed */
        try {
            final LoginCredentials badCreds =
                new ProxyCredentials(new KVStoreUserPrincipal(ADMIN_USER_NAME));
            ulAPI.login(badCreds);
            fail("expected exception");
        } catch (AuthenticationFailureException fe) {
            assertTrue(true); /* expected */
        } catch (Exception e) {
            fail("unexpected exception: " + e);
        }

        try {
            final LoginCredentials unknownCreds = new SimpleCredentials();
            ulAPI.login(unknownCreds);
            fail("expected exception");
        } catch (AuthenticationFailureException fe) {
            assertTrue(true); /* expected */
        }
    }

    @Test
    public void testRenewPasswordLogin() {
        renewPasswordTest(-1);
        renewPasswordTest(1000);
    }

    private void renewPasswordTest(long pwdLifeTime) {
        /* Setup */
        final ResourceId rid = new AdminId(1);
        final ScaffoldUserVerifier verifier = new ScaffoldUserVerifier(
            ADMIN_USER_NAME, ADMIN_USER_PASSWORD, pwdLifeTime);
        final long lockoutInterval = 1000;
        final int lockoutCount = 3;
        final long lockoutTimeout = 2000;
        final LoginConfig lc = new LoginConfig().
            setAcctErrLockoutInt(lockoutInterval).
            setAcctErrLockoutCnt(lockoutCount).
            setAcctErrLockoutTMO(lockoutTimeout);
        final PasswordRenewer renewer = new PasswordRenewer() {

            @Override
            public PasswordRenewResult renewPassword(String userName,
                                                     char[] newPassword) {
                verifier.renewPassword(userName, newPassword);
                return new PasswordRenewResult(true, null);
            }
        };
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier,
                                 renewer,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 lc, logger);

        /* login() testing */
        final PasswordCredentials expiredCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    ADMIN_USER_PASSWORD);
        LoginResult lr = null;

        /*
         * Now wait a while to allow the password to expire
         */
        try {
            if (pwdLifeTime > 0) {
                Thread.sleep(pwdLifeTime + 100);
            }
        } catch (InterruptedException ie) {
            fail("didn't expect to be interrupted");
        }

        try {
            ulh.login(expiredCreds, "localhost");
            fail("expected exception");
        } catch (PasswordExpiredException e) {
            assertThat("password expire", e.getMessage(),
                       containsString("expired"));
        }

        /* The renew password login should succeed */
        final char[] NEW_PASSWORD = "NoSql00__world".toCharArray();
        lr = ulh.renewPasswordLogin(expiredCreds, NEW_PASSWORD, "localhost");
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());
    }

    @Test
    public void testProxyLogin()
        throws RemoteException, NotBoundException {

        /* Setup */
        final ULFH ulfh = new ULFH(logger);
        final AdminId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier,
                                 null /* password renewer */,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);
        final UserLoginImpl uli = new UserLoginImpl(ulfh, ulh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind("localhost", registryPort, USER_LOGIN,
                                 rid, export(uli), DEFAULT_CSF, DEFAULT_SSF,
                                 USER_LOGIN_TYPE_FAMILY,
                                 () -> new AsyncUserLoginResponder(
                                     uli, DEFAULT_THREAD_POOL, logger),
                                 logger));


        final LoginToken dummyToken =
            new LoginToken(new SessionId(new byte[0]), 0L);

        /* successful proxy login */
        final UserLoginAPI ulAPI =
            RegistryUtils.getUserLogin(null /* storeName */, "localhost",
                                       registryPort, USER_LOGIN,
                                       new BasicLoginHandle(dummyToken),
                                       null /* clientId */,
                                       Protocols.getDefault(),
                                       false /* ignoreWrongType */,
                                       logger);
        final ProxyCredentials goodCreds =
            new ProxyCredentials(new KVStoreUserPrincipal(ADMIN_USER_NAME));
        final LoginResult lr = ulAPI.proxyLogin(goodCreds);
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());

        /* Invalid user */
        try {
            final ProxyCredentials badCreds =
                new ProxyCredentials(new KVStoreUserPrincipal("unknown"));
            ulAPI.proxyLogin(badCreds);
            fail("expected exception");
        } catch (AuthenticationFailureException fe) {
            assertTrue(true); /* expected */
        } catch (Exception e) {
            fail("unexpected exception: " + e);
        }
    }

    @Test
    public void testLogout()
        throws RemoteException, NotBoundException {

        /* Setup */
        final ULFH ulfh = new ULFH(logger);
        final AdminId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier,
                                 null /* password renewer */,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);
        final UserLoginImpl uli = new UserLoginImpl(ulfh, ulh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind("localhost", registryPort, USER_LOGIN,
                                 rid, export(uli), DEFAULT_CSF, DEFAULT_SSF,
                                 USER_LOGIN_TYPE_FAMILY,
                                 () -> new AsyncUserLoginResponder(
                                     uli, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final UserLoginAPI ulAPI =
            RegistryUtils.getUserLogin(null /* storeName */, "localhost",
                                       registryPort, USER_LOGIN,
                                       ResourceType.ADMIN, null /* loginMgr */,
                                       null /* clientId */,
                                       Protocols.getDefault(),
                                       false /* ignoreWrongType */,
                                       logger);

        /* testing */
        final LoginCredentials goodCreds =
            new PasswordCredentials(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final LoginResult lr = ulAPI.login(goodCreds);

        /* Logout a bogus token */
        try {
            final byte[] sid = new byte[20];
            for (int i = 0; i < sid.length; i++) {
                sid[i] = (byte) i;
            }
            final long et = 1234567890123456L;
            final LoginToken lt =
                new LoginToken(new SessionId(sid, IdScope.LOCAL, rid), et);

            ulAPI.logout(lt);
            fail("expected exception");
        } catch (AuthenticationRequiredException are) /* CHECKSTYLE:OFF */{
            /* expected */
        } /* CHECKSTYLE:ON */ catch (Exception e) {
            fail("wrong exception type: " + e);
        }

        /* Logout a good token */
        ulAPI.logout(lr.getLoginToken());

        /* Re-logout the previously good token */
        try {
            ulAPI.logout(lr.getLoginToken());
            fail("expected exception");
        } catch (AuthenticationRequiredException are) /* CHECKSTYLE:OFF */{
            /* expected */
        } /* CHECKSTYLE:ON */ catch (Exception e) {
            fail("wrong exception type: " + e);
        }
    }

    @Test
    public void testValidateLoginToken()
        throws RemoteException, NotBoundException {

        /* Setup */
        final ULFH ulfh = new ULFH(logger);
        final AdminId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier,
                                 null /*password renewer */,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);
        final UserLoginImpl uli = new UserLoginImpl(ulfh, ulh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind("localhost", registryPort, USER_LOGIN,
                                 rid, export(uli), DEFAULT_CSF, DEFAULT_SSF,
                                 USER_LOGIN_TYPE_FAMILY,
                                 () -> new AsyncUserLoginResponder(
                                     uli, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final UserLoginAPI ulAPI =
            RegistryUtils.getUserLogin(null /* storeName */, "localhost",
                                       registryPort, USER_LOGIN,
                                       ResourceType.ADMIN, null /* loginMgr */,
                                       null /* clientId */,
                                       Protocols.getDefault(),
                                       false /* ignoreWrongType */,
                                       logger);

        /* testing */
        /* Validate a bogus token */
        final byte[] sid = new byte[20];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) i;
        }
        final long et = 1234567890123456L;
        final ResourceId snrid = new StorageNodeId(17);
        final LoginToken lt =
            new LoginToken(new SessionId(sid, IdScope.LOCAL, snrid), et);

        final Subject badSubj = ulAPI.validateLoginToken(lt);
        assertNull(badSubj);

        /* Validate a good token */
        final LoginCredentials goodCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    ADMIN_USER_PASSWORD);
        final LoginResult lr = ulAPI.login(goodCreds);

        final Subject subject =
            ulAPI.validateLoginToken(lr.getLoginToken());
        assertNotNull(subject);
        final Set<Principal> principals = subject.getPrincipals();
        assertNotNull(principals);
        assertTrue(principals.contains(
                       KVStoreRolePrincipal.PUBLIC));
        assertFalse(principals.contains(
                        KVStoreRolePrincipal.INTERNAL));

        /* Now, log out the session and try again */
        ulAPI.logout(lr.getLoginToken());

        /* Validate the logged-out token */
        final Subject loSubj = ulAPI.validateLoginToken(lr.getLoginToken());
        assertNull(loSubj);
    }

    @Test
    public void testRequestSessionExtensionAllowed()
        throws RemoteException, NotBoundException {

        /* Setup */
        final ULFH ulfh = new ULFH(logger);
        final AdminId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        /* Configure with non-infinite lifetime and extension allowed */
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier,
                                 null /* password renewer */,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig().
                                 setSessionLifetime(3600 * 1000L).
                                 setAllowExtension(true),
                                 logger);
        final UserLoginImpl uli = new UserLoginImpl(ulfh, ulh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind("localhost", registryPort, USER_LOGIN,
                                 rid, export(uli), DEFAULT_CSF, DEFAULT_SSF,
                                 USER_LOGIN_TYPE_FAMILY,
                                 () -> new AsyncUserLoginResponder(
                                     uli, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final UserLoginAPI ulAPI =
            RegistryUtils.getUserLogin(null /* storeName */, "localhost",
                                       registryPort, USER_LOGIN,
                                       ResourceType.ADMIN, null /* loginMgr */,
                                       null /* clientId */,
                                       Protocols.getDefault(),
                                       false /* ignoreWrongType */,
                                       logger);

        final LoginCredentials goodCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    ADMIN_USER_PASSWORD);
        final LoginResult lr = ulAPI.login(goodCreds);
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());

        final byte[] sid = new byte[20];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) i;
        }
        final long et = 1234567890123456L;
        final LoginToken lt =
            new LoginToken(new SessionId(sid, IdScope.LOCAL, rid), et);

        final LoginToken newToken1 = ulAPI.requestSessionExtension(lt);
        assertNull(newToken1);

        /* extend a good token */
        /* Sleep briefly to allow extension to mean something */
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        final LoginToken newToken2 =
            ulAPI.requestSessionExtension(lr.getLoginToken());
        assertNotNull(newToken2);
        assertTrue(newToken2.getExpireTime() >
                   lr.getLoginToken().getExpireTime());
    }

    private static class SimpleCredentialsType
            implements LoginCredentialsType {
        private static final int INT_VALUE = 42;
        private static SimpleCredentialsType TYPE =
            new SimpleCredentialsType();
        static {
            LoginCredentialsTypeFinders.addFinder(
                SimpleCredentialsType::getType);
        }
        static SimpleCredentialsType getType(int intValue) {
            return (intValue == INT_VALUE) ? TYPE : null;
        }
        @Override
        public int getIntValue() {
            return INT_VALUE;
        }
        @Override
        public LoginCredentials readLoginCredentials(DataInput in, short sv) {
            return new SimpleCredentials();
        }
    }

    static class SimpleCredentials
        implements LoginCredentials, Serializable {
        private static final long serialVersionUID = 1L;
        @Override
        public void writeFastExternal(DataOutput out, short sv) { }
        @Override
        public LoginCredentialsType getLoginCredentialsType() {
            return SimpleCredentialsType.TYPE;
        }
        @Override
        public String getUsername() {
            return "nobody";
        }
    }

    static class ULFH extends TestProcessFaultHandler {
        ULFH(Logger logger) {
            super(logger, ProcessExitCode.RESTART);
        }

        @Override
        public void queueShutdownInternal(Throwable th, ProcessExitCode pec) {
            fail("queueShutdownInternal called");
        }
    }

    private static class BasicLoginHandle extends LoginHandle {
        BasicLoginHandle(LoginToken token) {
            super(token);
        }


        @Override
        public LoginToken renewToken(LoginToken currToken) {
            /* not trying very hard */
            return null;
        }

        @Override
        public void logoutToken() {
            /* no action taken */
        }

        @Override
        public boolean isUsable(ResourceType rtype) {
            return true;
        }
    }
}
