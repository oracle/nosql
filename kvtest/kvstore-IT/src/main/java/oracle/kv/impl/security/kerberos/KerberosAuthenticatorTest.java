/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.kerberos;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.logging.Level;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.kerberos.KerberosConfig.ClientKrbConfiguration;
import oracle.kv.impl.security.login.KerberosInternalCredentials;
import oracle.kv.impl.security.login.KerberosLoginResult;
import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;
import oracle.kv.impl.util.TestUtils;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.junit.Test;

public class KerberosAuthenticatorTest extends KerberosTestBase {

    private static final String SERVICE_PRINCIPAL = "oraclenosql/localhost";
    private static final String USER_PRINCIPAL = "krbuser";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        startKdc();
    }

    @Override
    public void tearDown() throws Exception {
        stopKdc();
        KerberosContext.resetContext();
    }

    @Test
    public void testBasic() throws Exception {
        final File secDir = new File(TestUtils.getTestDir(), "security");
        assertTrue(secDir.mkdir());
        final File keytab = new File(secDir, "store.keytab");
        addPrincipal(keytab, SERVICE_PRINCIPAL, USER_PRINCIPAL);

        final UserLoginCallbackHandler handler =
           new UserLoginCallbackHandler(logger);
        final SecurityParams secParams =
            makeSecurityParams(secDir, keytab, "localhost" /* instance name */);
        final KerberosAuthenticator authenticator =
            new KerberosAuthenticator(secParams);

        /* JAAS login using specified configuration */
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
        final Subject subject = new Subject();
        final Configuration config =
            new ClientKrbConfiguration(USER_PRINCIPAL,
                                       null /* credential cache */,
                                       keytab.getAbsolutePath(),
                                       true /* doNotPrompt */);
        final LoginContext loginContext =
            new LoginContext(ClientKrbConfiguration.CLIENT_KERBEROS_CONFIG,
                             subject, null, config);
        loginContext.login();

        final GSSManager manager = GSSManager.getInstance();
        final GSSName serviceName = manager.createName(
            SERVICE_PRINCIPAL, KerberosConfig.getKrbPrincNameType());
        final GSSContext context = manager.createContext(
            serviceName,
            KerberosConfig.getKerberosMethOid(),
            null, /* GSSCredential */
            GSSContext.DEFAULT_LIFETIME);
        byte[] ticket = KerberosContext.runWithContext(
            subject,
            () -> {
                /* GSS-API login */
                byte[] token = new byte[0];
                context.requestMutualAuth(true);
                return context.initSecContext(token, 0, token.length);
            });
        final KerberosInternalCredentials creds =
            new KerberosInternalCredentials(USER_PRINCIPAL, ticket);
        assertTrue(authenticator.authenticate(creds, handler));
        final LoginResult loginResult = handler.getLoginResult();
        assertTrue(loginResult instanceof KerberosLoginResult);
        final byte[] mutualAuthen =
            ((KerberosLoginResult) loginResult).getMutualAuthToken();
        context.initSecContext(mutualAuthen, 0, mutualAuthen.length);
        assertTrue(context.isEstablished());
    }

    @Test
    public void testErrorLogging() throws Exception {
        final File secDir = new File(TestUtils.getTestDir(), "security");
        assertTrue(secDir.mkdir());
        final File keytab = new File(secDir, "store.keytab");
        addPrincipal(keytab, SERVICE_PRINCIPAL, USER_PRINCIPAL);

        /* Test initialize Authenticator with invalid security parameters */
        final File nonExistKeytab = new File("nonExist");
        SecurityParams secParams = makeSecurityParams(
            secDir, nonExistKeytab, "localhost" /* instance name */);
        KerberosAuthenticator authenticator =
            new KerberosAuthenticator(secParams);
        final ErrorMsgCallbackHandler handler =
            new ErrorMsgCallbackHandler();
        authenticator.authenticate(new KerberosInternalCredentials(), handler);
        assertEquals(Level.WARNING, handler.getLevel());
        assertThat("Service principal login failed",
                   handler.getErrorMsg(),
                   containsString("Kerberos Service login failed"));

        secParams = makeSecurityParams(secDir, keytab, "localhost");
        authenticator = new KerberosAuthenticator(secParams);

        /* Test authenticate without callback handler */
        try {
            authenticator.authenticate(new KerberosInternalCredentials(), null);
            fail("invalid callback handler");
        } catch (IllegalArgumentException iae) {
            assertThat("invliad callback handler",
                       iae.getMessage(),
                       containsString("requires callback handler"));
        }

        /* Test authenticate non-Kerberos credentials */
        authenticator.authenticate(
            new PasswordCredentials("test",
                "NoSql00__1234".toCharArray()), handler);
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("Not a kerberos credentials",
                   handler.getErrorMsg(),
                   containsString("Not Kerberos credentials"));

        /* Test authenticate empty Kerberos credentials */
        authenticator.authenticate(new KerberosInternalCredentials(), handler);
        assertEquals(Level.INFO, handler.getLevel());
        assertThat("init token is not valid",
                   handler.getErrorMsg(),
                   containsString("init token is not valid"));
    }
}
