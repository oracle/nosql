/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.unwrapException;
import static oracle.kv.impl.async.MethodCallUtils.checkFastSerializeResponse;
import static oracle.kv.impl.async.MethodCallUtils.fastSerializeResponse;
import static oracle.kv.impl.async.MethodCallUtils.serializeMethodCall;
import static oracle.kv.util.TestUtils.assertEqualClasses;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.concurrent.CompletableFuture;

import javax.security.auth.Subject;

import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.util.SubjectUtils;
import oracle.kv.impl.util.SerialVersion;

import org.junit.Test;

/** Test AsyncUserLogin. */
public class AsyncUserLoginTest extends TestBase {

    private static final PasswordCredentials samplePasswordCreds =
        new PasswordCredentials("myuser", new char[] { 'p', 'w' });

    private static final LoginToken sampleToken =
        new LoginToken(new SessionId(new byte[] { '1', '2' }), 33);

    private static final LoginToken sampleToken2 =
        new LoginToken(new SessionId(new byte[] { '3', '4' }), 55);

    private static final KVStoreUserPrincipal samplePrincipal =
        new KVStoreUserPrincipal("user");

    private static final LoginResult sampleLoginResult = new LoginResult();

    private static final Subject sampleSubject =
        SubjectUtils.createSubject(singleton(samplePrincipal));

    /* Tests */

    @Test
    public void testSerializeGetSerialVersionCall() throws Exception {
        final AsyncUserLogin.GetSerialVersionCall getSerialVersion =
            serializeMethodCall(new AsyncUserLogin.GetSerialVersionCall());
        getSerialVersion.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncUserLogin() {
                @Override
                public CompletableFuture<Short>
                    getSerialVersion(short sv, long tm)
                {
                    return completedFuture(SerialVersion.CURRENT);
                }
            })
            .thenAccept(result -> assertSame(SerialVersion.CURRENT, result))
            .get();
        checkFastSerializeResponse(getSerialVersion, (short) 3);
    }

    @Test
    public void testSerializeLoginCall() throws Exception {
        for (final LoginCredentials testCreds :
                 asList(samplePasswordCreds, null)) {
            final AsyncUserLogin.LoginCall login =
                serializeMethodCall(new AsyncUserLogin.LoginCall(testCreds));
            login.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncUserLogin() {
                    @Override
                    public CompletableFuture<LoginResult>
                        login(short sv, LoginCredentials creds, long tm)
                    {
                        assertEqualClasses(testCreds, creds);
                        return completedFuture(null);
                    }
                })
                .thenAccept(result -> assertSame(null, result))
                .get();
            fastSerializeResponse(login, new LoginResult());
            fastSerializeResponse(login, null);
        }
    }

    @Test
    public void testSerializeProxyLoginCall() throws Exception {
        for (final ProxyCredentials testCreds :
            new ProxyCredentials[]{new ProxyCredentials(samplePrincipal), null}) {
            final AsyncUserLogin.ProxyLoginCall proxyLogin =
                serializeMethodCall(new AsyncUserLogin.ProxyLoginCall(
                                        testCreds, null /* authContext */));
            proxyLogin.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncUserLogin() {
                    @Override
                    public CompletableFuture<LoginResult>
                        proxyLogin(short sv, ProxyCredentials creds,
                                   AuthContext authContext, long tm)
                    {
                        assertEqualClasses(testCreds, creds);
                        assertEquals(null, authContext);
                        return completedFuture(sampleLoginResult);
                    }
                })
                .thenAccept(result -> assertSame(sampleLoginResult, result))
                .get();
            fastSerializeResponse(proxyLogin, sampleLoginResult);
            fastSerializeResponse(proxyLogin, null);
        }
    }

    @Test
    public void testSerializeRenewPasswordLoginCall() throws Exception {
        for (final PasswordCredentials testCreds :
                 asList(samplePasswordCreds, null)) {
            for (final char[] testPass :
                     asList(new char[] { 'n', 'e', 'w' }, null)) {
                final AsyncUserLogin.RenewPasswordLoginCall
                    renewPasswordLogin = serializeMethodCall(
                        new AsyncUserLogin.RenewPasswordLoginCall(testCreds,
                                                                  testPass));
                renewPasswordLogin.callService(
                    SerialVersion.CURRENT, 10,
                    new AbstractAsyncUserLogin() {
                        @Override
                        public CompletableFuture<LoginResult>
                            renewPasswordLogin(short sv,
                                               PasswordCredentials creds,
                                               char[] newPassword, long tm)
                        {
                            assertEqualClasses(testCreds, creds);
                            assertArrayEquals(testPass, newPassword);
                            return completedFuture(sampleLoginResult);
                        }
                    })
                    .thenAccept(result -> assertSame(sampleLoginResult, result))
                    .get();
                fastSerializeResponse(renewPasswordLogin, sampleLoginResult);
                fastSerializeResponse(renewPasswordLogin, null);
            }
        }
    }

    @Test
    public void testSerializeRequestSessionExtensionCall() throws Exception {
        for (final LoginToken testToken : asList(sampleToken, null)) {
            final AsyncUserLogin.RequestSessionExtensionCall
                requestSessionExtension = serializeMethodCall(
                    new AsyncUserLogin.RequestSessionExtensionCall(testToken));
            requestSessionExtension.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncUserLogin() {
                    @Override
                    public CompletableFuture<LoginToken>
                        requestSessionExtension(short sv, LoginToken loginToken,
                                                long tm) {
                        assertEquals(testToken, loginToken);
                        return completedFuture(sampleToken2);
                    }
                })
                .thenAccept(result -> assertSame(sampleToken2, result))
                .get();
            fastSerializeResponse(requestSessionExtension, testToken);
        }
    }

    @Test
    public void testSerializeValidateLoginTokenCall() throws Exception {
        for (final LoginToken testToken : asList(sampleToken, null)) {
            final AsyncUserLogin.ValidateLoginTokenCall
                validateLoginToken = serializeMethodCall(
                    new AsyncUserLogin.ValidateLoginTokenCall(
                        testToken, null /* authContext */));
            validateLoginToken.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncUserLogin() {
                    @Override
                    public CompletableFuture<Subject>
                        validateLoginToken(short sv, LoginToken loginToken,
                                           AuthContext authContext, long tm) {
                        assertEquals(testToken, loginToken);
                        assertEquals(null, authContext);
                        return completedFuture(sampleSubject);
                    }
                })
                .thenAccept(result -> assertSame(sampleSubject, result))
                .get();
            fastSerializeResponse(validateLoginToken, null);
            fastSerializeResponse(validateLoginToken, sampleSubject);
        }
    }

    @Test
    public void testSerializeLogoutCall() throws Exception {
        for (final LoginToken testToken : asList(sampleToken, null)) {
            final AsyncUserLogin.LogoutCall logout =
                serializeMethodCall(new AsyncUserLogin.LogoutCall(testToken));
            logout.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncUserLogin() {
                    @Override
                    public CompletableFuture<Void>
                        logout(short sv, LoginToken loginToken, long tm) {
                        assertEquals(testToken, loginToken);
                        return completedFuture(null);
                    }
                })
                .thenAccept(result -> assertSame(null, result))
                .get();
            logout.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncUserLogin() {
                    @Override
                    public CompletableFuture<Void>
                        logout(short sv, LoginToken loginToken, long tm) {
                        assertEquals(testToken, loginToken);
                        return failedFuture(new NumberFormatException("abc"));
                    }
                })
                .handle(
                    unwrapException(
                        (result, exp) -> {
                            assertEquals(null, result);
                            checkException(exp, NumberFormatException.class,
                                           "abc");
                            return null;
                        }))
                .get();
            fastSerializeResponse(logout, null);
        }
    }

    /* Other methods and classes */

    static class AbstractAsyncUserLogin implements AsyncUserLogin {
        @Override
        public CompletableFuture<Short> getSerialVersion(short sv, long t) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<LoginResult>
            login(short sv, LoginCredentials c, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<LoginResult>
            proxyLogin(short sv, ProxyCredentials c, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<LoginResult>
            renewPasswordLogin(short sv, PasswordCredentials oc, char[] np,
                               long tm) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<LoginToken>
            requestSessionExtension(short sv, LoginToken lt, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Subject>
            validateLoginToken(short sv, LoginToken lt, AuthContext a,
                               long tm) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Void> logout(short sv, LoginToken lt,
                                              long tm) {
            throw new AssertionError();
        }
    }
}
