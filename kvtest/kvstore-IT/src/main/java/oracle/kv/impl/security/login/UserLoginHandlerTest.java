/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;
import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.LoginCredentials;
import oracle.kv.LoginCredentials.LoginCredentialsType;
import oracle.kv.LoginCredentialsTypeFinders;
import oracle.kv.PasswordCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.ScaffoldUserVerifier;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.security.login.UserLoginHandler.LoginConfig;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

import org.junit.Test;

/**
 * Test the UserLoginHandler class.
 */
public class UserLoginHandlerTest extends TestBase {

    private static final int N_ID_BYTES = 16;

    public static final String ADMIN_USER_NAME = "admin";
    public static final String ADMIN_USER_PASSWORD_STR = "NoSql00__hello";
    public static final char[] ADMIN_USER_PASSWORD =
        "NoSql00__hello".toCharArray();

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    @Test
    public void testLogin() {

        /* Setup */
        final ResourceId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier, null /* pwdRenewer */,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);

        /* login() testing */

        final LoginCredentials goodCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    ADMIN_USER_PASSWORD);
        final LoginResult lr =
            ulh.login(goodCreds, "localhost");
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());

        try {
            final LoginCredentials badCreds =
                new PasswordCredentials("unknown", "nopass".toCharArray());
            ulh.login(badCreds, "localhost");
            fail("expected exception");
        } catch (AuthenticationFailureException fe) {
            assertTrue(true); /* expected */
        } catch (Exception e) {
            fail("unexpected exception: " + e);
        }

        try {
            final LoginCredentials unknownCreds = new SimpleCredentials();
            ulh.login(unknownCreds, "localhost");
            fail("expected exception");
        } catch (AuthenticationFailureException fe) {
            assertTrue(true); /* expected */
        } catch (Exception e) {
            fail("unexpected exception: " + e);
        }
    }

    @Test
    public void testLoginErrors() {

        /* Setup */
        final ResourceId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final long lockoutInterval = 1000;
        final int lockoutCount = 3;
        final long lockoutTimeout = 2000;
        final LoginConfig lc = new LoginConfig().
            setAcctErrLockoutInt(lockoutInterval).
            setAcctErrLockoutCnt(lockoutCount).
            setAcctErrLockoutTMO(lockoutTimeout);
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier,  null, /* pwdRenewer */
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 lc, logger);

        /* login() testing */

        final LoginCredentials goodCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    ADMIN_USER_PASSWORD);
        final LoginCredentials badCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    (ADMIN_USER_PASSWORD_STR + "XXX").
                                    toCharArray());

        /* The good creds should succeed */
        final LoginResult lr = ulh.login(
            goodCreds, "localhost");
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());

        /* run through series of failed login attempts */
        for (int iter = 0; iter < lockoutCount; iter++) {
            try {
                ulh.login(badCreds, "localhost");
                fail("expected exception");
            } catch (AuthenticationFailureException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        /*
         * At this point we expect that the account should be locked out, so
         * the good creds should fail.  Try multiple times
         */
        for (int iter = 0; iter < 3; iter++) {
            try {
                ulh.login(goodCreds, "localhost");
                fail("expected exception");
            } catch (AuthenticationFailureException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        /*
         * Now wait a while to allow the lockout to expire
         */
        try {
            Thread.sleep(lockoutTimeout + 1000);
        } catch (InterruptedException ie) {
            fail("didn't expect to be interrupted");
        }

        /* And now the login should again succeed */
        final LoginResult lr2 = ulh.login(
            goodCreds, "localhost");
        assertNotNull(lr2);
        assertNotNull(lr2.getLoginToken());

        /* Now check that login succeses clear errors */
        for (int iter = 0; iter < 10 * lockoutCount; iter++) {
            /* The bad creds should fail */
            try {
                ulh.login(badCreds, "localhost");
                fail("expected exception");
            } catch (AuthenticationFailureException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */

            /* The good creds should succeed */
            final LoginResult lr3 = ulh.login(
                goodCreds, "localhost");
            assertNotNull(lr3);
            assertNotNull(lr3.getLoginToken());
        }
    }

    @Test
    public void testLogout() {

        /* Setup */
        final ResourceId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier, null, /* pwdRenewer */
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);

        /* testing */
        final LoginCredentials goodCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    ADMIN_USER_PASSWORD);
        final LoginResult lr =
            ulh.login(goodCreds, "localhost");

        /* Logout a bogus token */
        try {
            final byte[] sid = new byte[20];
            for (int i = 0; i < sid.length; i++) {
                sid[i] = (byte) i;
            }
            final long et = 1234567890123456L;
            final LoginToken lt =
                new LoginToken(new SessionId(sid, IdScope.LOCAL, rid), et);

            ulh.logout(lt);
            fail("expected exception");
        } catch (AuthenticationRequiredException are) /* CHECKSTYLE:OFF */{
            /* expected */
        } /* CHECKSTYLE:ON */ catch (Exception e) {
            fail("wrong exception type: " + e);
        }

        /* Logout a good token */
        ulh.logout(lr.getLoginToken());

        /* Re-logout the previously good token */
        try {
            ulh.logout(lr.getLoginToken());
            fail("expected exception");
        } catch (AuthenticationRequiredException are) /* CHECKSTYLE:OFF */{
            /* expected */
        } /* CHECKSTYLE:ON */ catch (Exception e) {
            fail("wrong exception type: " + e);
        }
    }

    @Test
    public void testValidateLoginToken() {

        /* Setup */
        final ResourceId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier, null, /* pwdRenewer */
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);

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

        Subject subject = ulh.validateLoginToken(lt);
        assertNull(subject);

        /* Validate a good token */
        final LoginCredentials goodCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    ADMIN_USER_PASSWORD);
        final LoginResult lr = ulh.login(
            goodCreds, "localhost");

        subject = ulh.validateLoginToken(lr.getLoginToken());
        assertNotNull(subject);
        final Set<Principal> principals = subject.getPrincipals();
        assertNotNull(principals);
        assertTrue(principals.contains(
                       KVStoreRolePrincipal.PUBLIC));
        assertFalse(principals.contains(
                        KVStoreRolePrincipal.INTERNAL));

        /* Now, log out the session and try again */
        ulh.logout(lr.getLoginToken());

        /* Validate the logged-out token */
        subject = ulh.validateLoginToken(lr.getLoginToken());
        assertNull(subject);
    }

    @Test
    public void testRequestSessionExtensionAllowed() {

        /* Setup */
        final ResourceId rid = new AdminId(1);
        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        /* Configure with non-infinite lifetime and extension allowed */
        final UserLoginHandler ulh =
            new UserLoginHandler(rid, true, /* localId */
                                 verifier, null, /* pwdRenewer */
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig().
                                 setSessionLifetime(3600 * 1000L).
                                 setAllowExtension(true), logger);

        final LoginCredentials goodCreds =
            new PasswordCredentials(ADMIN_USER_NAME,
                                    ADMIN_USER_PASSWORD);
        final LoginResult lr = ulh.login(
            goodCreds, "localhost");
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());

        final byte[] sid = new byte[20];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) i;
        }
        final long et = 1234567890123456L;
        final LoginToken lt =
            new LoginToken(new SessionId(sid, IdScope.LOCAL, rid), et);

        final LoginToken newToken1 = ulh.requestSessionExtension(lt);
        assertNull(newToken1);

        /* extend a good token */
        /* Sleep briefly to allow extension to mean something */
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        final LoginToken newToken2 =
            ulh.requestSessionExtension(lr.getLoginToken());
        assertNotNull(newToken2);
        assertTrue(newToken2.getExpireTime() >
                   lr.getLoginToken().getExpireTime());
    }

    private static class SimpleCredentialsType
            implements LoginCredentialsType {
        private static final int INT_VALUE = 89;
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
        implements LoginCredentials, Serializable
    {
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

}
