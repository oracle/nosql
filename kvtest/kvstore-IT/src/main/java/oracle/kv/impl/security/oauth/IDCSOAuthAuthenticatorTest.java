/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.oauth;

import static oracle.kv.impl.security.oauth.IDCSOAuthTestUtils.OAUTH_KEY_STORE;
import static oracle.kv.impl.security.oauth.IDCSOAuthTestUtils.OAUTH_KS_ALIAS_DEF;
import static oracle.kv.impl.security.oauth.IDCSOAuthTestUtils.OAUTH_PW_NAME;
import static oracle.kv.impl.security.oauth.IDCSOAuthTestUtils.OAUTH_TRUST_STORE;
import static oracle.kv.impl.security.oauth.IDCSOAuthTestUtils.OAUTH_TS_ALIAS_DEF;
import static oracle.kv.impl.security.oauth.IDCSOAuthTestUtils.OAUTH_VFY_ALG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Level;

import javax.security.auth.Subject;

import org.junit.Test;

import oracle.kv.IDCSOAuthCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.SecureTestBase.ErrorMsgCallbackHandler;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;
import oracle.kv.impl.security.login.UserLoginCallbackHandler.UserSessionInfo;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource;
import oracle.kv.impl.util.TestUtils;

public class IDCSOAuthAuthenticatorTest extends TestBase {
    private static final String SECURITY_DIR = "securityx";
    private static final String AUDIENCE = "testaudience";
    private File secDir;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        secDir = new File(TestUtils.getTestDir(), SECURITY_DIR);
        secDir.mkdir();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        clearTestDirectory();
    }

    /*
     * A sanity check against access token acquired from IDCS server
     * using tenant public key provided by IDCS.
     */
    @Test
    public void testIDCSAccessToken()
        throws Exception {

        TestUtils.copyFile(IDCSOAuthTestUtils.IDCS_TRUST_STORE,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        TestUtils.copyFile(IDCSOAuthTestUtils.OAUTH_PW_NAME,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        SecurityParams sp = IDCSOAuthTestUtils.makeSecurityParams(secDir,
            IDCSOAuthTestUtils.IDCS_TRUST_STORE,
            IDCSOAuthTestUtils.OAUTH_PW_NAME);
        GlobalParams gp = IDCSOAuthTestUtils.makeGlobalParams(
            IDCSOAuthTestUtils.IDCS_AUDIENCE,
            IDCSOAuthTestUtils.IDCS_PUBLIC_KEY,
            IDCSOAuthTestUtils.OAUTH_VFY_ALG);
        IDCSOAuthAuthenticator authen = new IDCSOAuthAuthenticator(sp, gp);
        IDCSOAuthCredentials creds =
            new IDCSOAuthCredentials(IDCSOAuthTestUtils.IDCS_ACCESS_TOKEN);
        UserLoginCallbackHandler handler = new UserLoginCallbackHandler(logger);
        assertTrue(authen.authenticate(creds, handler));
        UserSessionInfo sessInfo = handler.getUserSessionInfo();
        assertNotNull(sessInfo);
        Subject subj = sessInfo.getSubject();
        assertNotNull(subj);
        ExecutionContext.subjectHasRole(subj, "readonly");
        ExecutionContext.subjectHasRole(subj, "dbadmin");
    }

    @Test
    public void testBasic()
        throws Exception {

        TestUtils.copyFile(OAUTH_KEY_STORE,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        TestUtils.copyFile(OAUTH_TRUST_STORE,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        TestUtils.copyFile(OAUTH_PW_NAME,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        SecurityParams sp = IDCSOAuthTestUtils.
            makeSecurityParams(secDir, OAUTH_TRUST_STORE, OAUTH_PW_NAME);
        GlobalParams gp = IDCSOAuthTestUtils.
            makeGlobalParams(AUDIENCE, OAUTH_TS_ALIAS_DEF, OAUTH_VFY_ALG);
        String keystorePath = secDir + File.separator + OAUTH_KEY_STORE;
        KeyStorePasswordSource pwdSrc = KeyStorePasswordSource.create(sp);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Date date = dateFormat.parse("2080/01/01");
        char[] ksPwd = pwdSrc.getPassword();
        String accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "/read",
            date,
            OAUTH_KS_ALIAS_DEF,
            keystorePath,
            ksPwd);
        IDCSOAuthAuthenticator authen = new IDCSOAuthAuthenticator(sp, gp);
        IDCSOAuthCredentials creds = new IDCSOAuthCredentials(accessToken);
        UserLoginCallbackHandler handler = new UserLoginCallbackHandler(logger);
        assertTrue(authen.authenticate(creds, handler));
        UserSessionInfo sessInfo = handler.getUserSessionInfo();
        assertNotNull(sessInfo);
        Subject subj = sessInfo.getSubject();
        assertNotNull(subj);
        ExecutionContext.subjectHasRole(subj, "readonly");
        long expireTime = sessInfo.getExpireTime();
        assertEquals(expireTime, date.getTime());
    }

    @Test
    public void testErrorAccessTokens()
        throws Exception {

        TestUtils.copyFile(OAUTH_KEY_STORE,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        TestUtils.copyFile(OAUTH_TRUST_STORE,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        TestUtils.copyFile(OAUTH_PW_NAME,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        SecurityParams sp = IDCSOAuthTestUtils.
            makeSecurityParams(secDir, OAUTH_TRUST_STORE, OAUTH_PW_NAME);
        GlobalParams gp = IDCSOAuthTestUtils.
            makeGlobalParams(AUDIENCE, OAUTH_TS_ALIAS_DEF, OAUTH_VFY_ALG);
        String keystorePath =
            secDir + File.separator + IDCSOAuthTestUtils.OAUTH_KEY_STORE;
        KeyStorePasswordSource pwdSrc = KeyStorePasswordSource.create(sp);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        char[] ksPwd = pwdSrc.getPassword();

        IDCSOAuthAuthenticator authen = new IDCSOAuthAuthenticator(sp, gp);
        ErrorMsgCallbackHandler handler = new ErrorMsgCallbackHandler();

        /* Test authenticate without callback handler */
        try {
            authen.authenticate(new IDCSOAuthCredentials(""), null);
            fail("invalid callback handler");
        } catch (IllegalArgumentException iae) {
            assertThat("invliad callback handler",
                       iae.getMessage(),
                       containsString("requires callback handler"));
        }

        /* Test authenticate non-oauth credentials */
        assertFalse(authen.authenticate(new PasswordCredentials(
            "test", "NoSql00__1234".toCharArray()), handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("Not OAuth credentials",
                   handler.getErrorMsg(),
                   containsString("Not OAuth credentials"));

        /* Test authenticate null oAuth credentials */
        assertFalse(authen.authenticate(new IDCSOAuthCredentials(null), handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("no access token",
                   handler.getErrorMsg(),
                   containsString("no access token"));

        /* Test authenticate empty oAuth credentials */
        assertFalse(authen.authenticate(new IDCSOAuthCredentials(""), handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("no access token",
                   handler.getErrorMsg(),
                   containsString("no access token"));

        /* token with invalid number of audiences */
        String accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList("invalid_audience1", "invalid_audience2"),
            "/read",
            dateFormat.parse("2080/01/01"),
            OAUTH_KS_ALIAS_DEF,
            keystorePath,
            ksPwd);
        IDCSOAuthCredentials creds = new IDCSOAuthCredentials(accessToken);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("invalid number of audiences specified",
                    handler.getErrorMsg(),
                    containsString("invalid number 2 of audiences specified"));

        /* token with invalid audience */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList("invalid_audience1"),
            "/read",
            dateFormat.parse("2080/01/01"),
            OAUTH_KS_ALIAS_DEF,
            keystorePath,
            ksPwd);
        creds = new IDCSOAuthCredentials(accessToken);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("failed to find match audience",
                   handler.getErrorMsg(),
                   containsString("failed to find match audience"));

        /* expired token */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "/read",
            dateFormat.parse("1900/01/01"),
            IDCSOAuthTestUtils.OAUTH_KS_ALIAS_DEF,
            keystorePath,
            ksPwd);
        creds = new IDCSOAuthCredentials(accessToken);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("access token is expired",
                   handler.getErrorMsg(),
                   containsString("access token is expired"));

        /* token without scope claim */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            null,
            dateFormat.parse("2080/01/01"),
            OAUTH_KS_ALIAS_DEF,
            keystorePath,
            ksPwd);
        creds = new IDCSOAuthCredentials(accessToken);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("no scope claim in access token",
                   handler.getErrorMsg(),
                   containsString("no scope claim in access token"));

        /* token without valid scope */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "sysadmin",
            dateFormat.parse("2080/01/01"),
            OAUTH_KS_ALIAS_DEF,
            keystorePath,
            ksPwd);
        creds = new IDCSOAuthCredentials(accessToken);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("failed to find valid scope",
                   handler.getErrorMsg(),
                   containsString("failed to find valid scope"));

        /* token without valid scope */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "/tables.ddl /sysadmin",
            dateFormat.parse("2080/01/01"),
            OAUTH_KS_ALIAS_DEF,
            keystorePath,
            ksPwd);
        creds = new IDCSOAuthCredentials(accessToken);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("failed to find valid scope",
                   handler.getErrorMsg(),
                   containsString("failed to find valid scope"));

        /* token signed by other key */
        accessToken = IDCSOAuthTestUtils.buildAccessToken(
            Arrays.asList(AUDIENCE),
            "/tables.ddl",
            dateFormat.parse("2080/01/01"),
            IDCSOAuthTestUtils.WRONG_TS_ALIAS_DEF,
            keystorePath,
            ksPwd);
        creds = new IDCSOAuthCredentials(accessToken);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("signature verification failed",
                   handler.getErrorMsg(),
                   containsString("signature verification failed"));
    }

    @Test
    public void testErrorInitialize()
        throws Exception {

        TestUtils.copyFile(IDCSOAuthTestUtils.OAUTH_TRUST_STORE,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        TestUtils.copyFile(IDCSOAuthTestUtils.OAUTH_PW_NAME,
                           IDCSOAuthTestUtils.getTestOAuthDir(),
                           secDir);
        IDCSOAuthCredentials creds = new IDCSOAuthCredentials("abc");

        /* initialized without audience */
        SecurityParams sp = IDCSOAuthTestUtils.
            makeSecurityParams(secDir, OAUTH_TRUST_STORE, OAUTH_PW_NAME);
        GlobalParams gp = IDCSOAuthTestUtils.
            makeGlobalParams(null, OAUTH_TS_ALIAS_DEF, OAUTH_VFY_ALG);
        IDCSOAuthAuthenticator authen = new IDCSOAuthAuthenticator(sp, gp);
        ErrorMsgCallbackHandler handler = new ErrorMsgCallbackHandler();
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("not initialized",
                   handler.getErrorMsg(),
                   containsString("is not initialized or is disabled"));

        /* initialized with empty audience */
        sp = IDCSOAuthTestUtils.
            makeSecurityParams(secDir, OAUTH_TRUST_STORE, OAUTH_PW_NAME);
        gp = IDCSOAuthTestUtils.
            makeGlobalParams("", OAUTH_TS_ALIAS_DEF, OAUTH_VFY_ALG);
        authen = new IDCSOAuthAuthenticator(sp, gp);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("not initialized",
                   handler.getErrorMsg(),
                   containsString("is not initialized or is disabled"));

        /* initialized with unsupported verifying algorithm */
        sp = IDCSOAuthTestUtils.
            makeSecurityParams(secDir, OAUTH_TRUST_STORE, OAUTH_PW_NAME);
        gp = IDCSOAuthTestUtils.
            makeGlobalParams(AUDIENCE, OAUTH_TS_ALIAS_DEF, "ES256");
        authen = new IDCSOAuthAuthenticator(sp, gp);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("unsupported algorithm",
                   handler.getErrorMsg(),
                   containsString("is not the supported algorithm"));

        /* initialized with non-existent password store */
        sp = IDCSOAuthTestUtils.
            makeSecurityParams(secDir, OAUTH_TRUST_STORE, "non-existent");
        gp = IDCSOAuthTestUtils.
            makeGlobalParams(AUDIENCE, OAUTH_TS_ALIAS_DEF, OAUTH_VFY_ALG);
        authen = new IDCSOAuthAuthenticator(sp, gp);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("no password source",
                   handler.getErrorMsg(),
                   containsString("Unable to access"));

        /* initialized with non-existent certificate */
        sp = IDCSOAuthTestUtils.
            makeSecurityParams(secDir, OAUTH_TRUST_STORE, OAUTH_PW_NAME);
        gp = IDCSOAuthTestUtils.
            makeGlobalParams(AUDIENCE, "non-existent", OAUTH_VFY_ALG);
        authen = new IDCSOAuthAuthenticator(sp, gp);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("could not find certificate",
                   handler.getErrorMsg(),
                   containsString("Could not find certificate"));

        /* initialized with non-existent truststore */
        sp = IDCSOAuthTestUtils.
            makeSecurityParams(secDir, "non-existent", OAUTH_PW_NAME);
        gp = IDCSOAuthTestUtils.
            makeGlobalParams(AUDIENCE, OAUTH_TS_ALIAS_DEF, OAUTH_VFY_ALG);
        authen = new IDCSOAuthAuthenticator(sp, gp);
        assertFalse(authen.authenticate(creds, handler));
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("could not find certificate store",
                   handler.getErrorMsg(),
                   containsString("Unable to locate"));
    }
}
