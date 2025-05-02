/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.async.AsyncVersionedRemote;
import oracle.kv.impl.security.login.AsyncUserLogin;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.sna.SNAFaultException;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;

/**
 * Test the ContextProxy class.
 */
public class ContextProxyTest extends TestBase {

    public static final int DO_THROW_ISE = 1;
    public static final int DO_CHECK_AUTH = 2;
    public static final int DO_OTHER = 3;
    public static final int DO_THROW_SAE = 4;
    public static final int DO_THROW_IFE_SAE = 5;

    @Test
    public void testBasic() throws Exception {
        testBasic(new TestIFImpl());
        testBasic(new TestIFAsyncImpl());
    }

    private void testBasic(TestIF testIf) throws Exception {

        /*
         * goodToken should allow this to succeed without retries
         */
        final LoginHandle lh = testIf.makeLoginHandle(testIf.getGoodToken());
        final TestIF proxyTestIf = testIf.createProxy(lh);

        proxyTestIf.doit(DO_CHECK_AUTH, null, (short) 0);
        /* No renews should be required */
        assertEquals(0, testIf.getARECount());
    }

    @Test
    public void testRenew() throws Exception {
        testRenew(new TestIFImpl());
        testRenew(new TestIFAsyncImpl());
    }

    private void testRenew(final TestIF testIf) throws Exception {

        /*
         * newableToken should allow this to succeeding, but will cause
         * retries to occur
         */
        final LoginHandle lh =
            testIf.makeLoginHandle(testIf.getRenewableToken());
        final TestIF proxyTestIf = testIf.createProxy(lh);

        proxyTestIf.doit(DO_CHECK_AUTH, null, (short) 0);
        /* There should be 1 renew attempt */
        assertEquals(1, testIf.getARECount());
        assertEquals(1, testIf.getRenewCount());
    }

    @Test
    public void testCantRenew() throws Exception {
        testCantRenew(new TestIFImpl());
        testCantRenew(new TestIFAsyncImpl());
    }

    private void testCantRenew(final TestIF testIf) throws Exception {

        /*
         * badToken should prevent this from succeeding, but will cause
         * retries to occur
         */
        final LoginHandle lh = testIf.makeLoginHandle(testIf.getBadToken());
        final TestIF proxyTestIf = testIf.createProxy(lh);

        try {
            proxyTestIf.doit(DO_CHECK_AUTH, null, (short) 0);
            fail("expected exception");
        } catch (AuthenticationRequiredException are) {
            /* only 1 attempt at renewal */
            assertEquals(1, testIf.getARECount());
            assertEquals(1, testIf.getRenewCount());
        }
    }

    @Test
    public void testRenewSAE()
        throws Exception {

        testRenewSAE(new TestIFImpl());
        testRenewSAE(new TestIFAsyncImpl());
    }

    private void testRenewSAE(final TestIF testIf) throws Exception {
        /*
         * newableToken should allow this to succeeding, but will cause
         * retries to occur
         */
        LoginHandle lh = testIf.makeLoginHandle(
            testIf.getRenewableToken(),
            new SessionAccessException("bad renew"));
        TestIF proxyTestIf = testIf.createProxy(lh);

        try {
            proxyTestIf.doit(DO_CHECK_AUTH, null, (short) 0);
            fail("expected exception");
        } catch (RemoteException re) {
            assertTrue(re.getCause() instanceof SessionAccessException);
        } catch (Exception e) {
            fail("wrong exception type: " + e);
        }

        lh = testIf.makeLoginHandle(testIf.getRenewableToken(),
            new SNAFaultException(new SessionAccessException("bad renew")));
        try {
            proxyTestIf.doit(DO_CHECK_AUTH, null, (short) 0);
            fail("expected exception");
        } catch (RemoteException re) {
            assertTrue(re.getCause() instanceof SessionAccessException);
        } catch (Exception e) {
            fail("wrong exception type: " + e);
        }
    }

    @Test
    public void testExceptions() throws Exception {
        testExceptions(new TestIFImpl());
        testExceptions(new TestIFAsyncImpl());
    }

    private void testExceptions(final TestIF testIf) throws Exception {

        final LoginHandle lh = null;
        final TestIF proxyTestIf = testIf.createProxy(lh);

        try {
            proxyTestIf.doit(DO_THROW_ISE, null, (short) 0);
            fail("expected exception");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        } catch (Exception e) {
            fail("wrong exception type: " + e);
        }

        try {
            proxyTestIf.doit(DO_CHECK_AUTH, null, (short) 0);
            fail("expected exception");
        } catch (AuthenticationRequiredException afe) {
            assertEquals(1, testIf.getARECount());
        } catch (Exception e) {
            fail("wrong exception type: " + e);
        }

        try {
            proxyTestIf.doit(DO_THROW_SAE, null, (short) 0);
            fail("expected exception");
        } catch (RemoteException re) {
            assertTrue(re.getCause() instanceof SessionAccessException);
        } catch (Exception e) {
            fail("wrong exception type: " + e);
        }

        try {
            proxyTestIf.doit(DO_THROW_IFE_SAE, null, (short) 0);
            fail("expected exception");
        } catch (RemoteException re) {
            assertTrue(re.getCause() instanceof SessionAccessException);
        } catch (Exception e) {
            fail("wrong exception type: " + e);
        }
    }

    @Test
    public void testLoginSAE()
        throws Exception {

        FaultUserLogin ful = new FaultUserLogin();
        UserLogin ul = ContextProxy.create(ful, null, SerialVersion.MINIMUM);
        ful.setFault(DO_THROW_SAE);
        checkExpectSAE(ul);
        ful.setFault(DO_THROW_IFE_SAE);
        checkExpectSAE(ul);

        FaultAsyncUserLogin afl = new FaultAsyncUserLogin(ful);
        AsyncUserLogin aul = ContextProxy.createAsync(
            afl, null, SerialVersion.MINIMUM);
        afl.setFault(DO_THROW_SAE);
        checkExpectSAE(aul);
        afl.setFault(DO_THROW_IFE_SAE);
        checkExpectSAE(aul);
    }

    private void checkExpectSAE(UserLogin login) {
        try {
            login.validateLoginToken(null, null, (short) 0);
            fail("expected exception");
        } catch (SessionAccessException re) {
            assertTrue(true); /* ignore */
        } catch (Exception e) {
            fail("wrong exception type: " + e);
        }
    }

    private void checkExpectSAE(AsyncUserLogin login) {
        try {
            login.validateLoginToken((short)0, null, null, 0).get();
            fail("expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof SessionAccessException);
        } catch (Throwable t) {
            throw new RuntimeException("Unexpected exception: " + t, t);
        }
    }

    public interface TestIF extends Remote {
        int getARECount();
        int getRenewCount();
        LoginToken getGoodToken();
        LoginToken getRenewableToken();
        LoginToken getBadToken();

        LoginHandle makeLoginHandle(LoginToken token);

        LoginHandle makeLoginHandle(LoginToken token, RuntimeException e);

        TestIF createProxy(LoginHandle lh);

        void doit(int val, AuthContext ac, short sv) throws RemoteException;
    }

    public static class TestIFImpl implements TestIF {

        private int areCount = 0;
        private int renewCount = 0;

        /* only the identities of these tokens matter for this test */
        private final LoginToken goodToken = new LoginToken(null, 0);
        private final LoginToken renewableToken = new LoginToken(null, 0);
        private final LoginToken badToken = new LoginToken(null, 0);

        /**
         * Used to allow verification of retries.
         */
        @Override
        public int getARECount() {
            return areCount;
        }

        @Override
        public int getRenewCount() {
            return renewCount;
        }

        @Override
        public LoginToken getGoodToken() { return goodToken; }

        @Override
        public LoginToken getRenewableToken() { return renewableToken; }

        @Override
        public LoginToken getBadToken() { return badToken; }

        @Override
        public LoginHandle makeLoginHandle(LoginToken token) {
            return new TestLoginHandle(token, null);
        }

        @Override
        public LoginHandle makeLoginHandle(LoginToken token,
                                           RuntimeException e) {
            return new TestLoginHandle(token, e);
        }

        @Override
        public TestIF createProxy(LoginHandle lh) {
            return ContextProxy.create(this, lh, SerialVersion.MINIMUM);
        }

        @Override
        public void doit(int val, AuthContext ac, short sv) {
            if (DO_THROW_ISE == val) {
                throw new IllegalStateException("bad state");
            } else if (DO_THROW_SAE == val) {
                throw new SessionAccessException("bad session");
            } else if (DO_THROW_IFE_SAE == val) {
                throw new SNAFaultException(
                    new SessionAccessException("bad session in fault"));
            } else if (DO_CHECK_AUTH == val) {
                if (ac == null || ac.getLoginToken() != goodToken) {
                    areCount++;
                    throw new AuthenticationRequiredException(
                        "auth req",
                        false /* isReturnSignal */);
                }
            }
        }

        private final class TestLoginHandle extends LoginHandle {

            private RuntimeException fault;
            private TestLoginHandle(LoginToken loginToken, RuntimeException e) {
                super(loginToken);
                this.fault = e;
            }

            @Override
            public LoginToken renewToken(LoginToken currToken) {
                if (fault != null) {
                    throw fault;
                }
                renewCount++;
                if (currToken == renewableToken) {
                    updateLoginToken(currToken, goodToken);
                }
                return getLoginToken();
            }

            @Override
            public void logoutToken() {
                updateLoginToken(getLoginToken(), null);
            }

            @Override
            public boolean isUsable(ResourceType rtype) {
                return true;
            }
        }
    }

    /** An equivalent async interface */
    public interface AsyncTestIF extends AsyncVersionedRemote {
        TestIF getSync();

        CompletableFuture<Void> doit(short serialVersion,
                                     int val,
                                     AuthContext authContext,
                                     long timeoutMillis);
    }

    /** Implement the async interface by using sync implementation */
    static class AsyncTestIFImpl implements AsyncTestIF {
        private final TestIFImpl sync = new TestIFImpl();
        @Override
        public TestIF getSync() { return sync; }
        @Override
        public CompletableFuture<Short> getSerialVersion(short sv, long tm) {
            return completedFuture(SerialVersion.MINIMUM);
        }
        @Override
        public CompletableFuture<Void>
            doit(short serialVersion, int val, AuthContext authContext,
                 long timeoutMillis) {
            try {
                sync.doit(val, authContext, serialVersion);
                return completedFuture(null);
            } catch (Throwable t) {
                return failedFuture(t);
            }
        }
    }

    /**
     * Implement the standard synchronous interface by indirecting to an async
     * one. That way we can test the context proxy for an async implementation
     * using the same tests.
     */
    static class TestIFAsyncImpl implements TestIF {
        final AsyncTestIF async;

        TestIFAsyncImpl() {
            this(new AsyncTestIFImpl());
        }

        TestIFAsyncImpl(AsyncTestIF async) {
            this.async = async;
        }

        @Override
        public int getARECount() { return getSync().getARECount(); }
        @Override
        public int getRenewCount() { return getSync().getRenewCount(); }
        @Override
        public LoginToken getGoodToken() { return getSync().getGoodToken(); }
        @Override
        public LoginToken getRenewableToken() {
            return getSync().getRenewableToken();
        }
        @Override
        public LoginToken getBadToken() { return getSync().getBadToken(); }
        @Override
        public LoginHandle makeLoginHandle(LoginToken lt) {
            return getSync().makeLoginHandle(lt);
        }
        @Override
        public LoginHandle makeLoginHandle(LoginToken lt, RuntimeException e) {
            return getSync().makeLoginHandle(lt, e);
        }

        private TestIF getSync() { return async.getSync(); }

        /**
         * Wrap a TestIFAsyncImpl object around a a proxy for the underlying
         * async implementation, since it is that proxied async implementation
         * that we are trying to test.
         */
        @Override
        public TestIF createProxy(LoginHandle lh) {
            return new TestIFAsyncImpl(
                ContextProxy.createAsync(async, lh, SerialVersion.MINIMUM));
        }

        @Override
        public void doit(int val, AuthContext ac, short sv)
            throws RemoteException {

            try {
                async.doit(sv, val, ac, 10).get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                } else if (e.getCause() instanceof RemoteException) {
                    throw (RemoteException) e.getCause();
                }
                throw new RuntimeException("Unexpected exception: " + e, e);
            } catch (Throwable t) {
                throw new RuntimeException("Unexpected exception: " + t, t);
            }
        }
    }

    static class FaultUserLogin
        extends VersionedRemoteImpl implements UserLogin {

        private int fault;

        FaultUserLogin setFault(int fault) {
            this.fault = fault;
            return this;
        }

        @Override
        public LoginToken requestSessionExtension(LoginToken lt, short sv) {
            throw new AssertionError();
        }

        @Override
        public LoginResult login(LoginCredentials lc, short sv)
            throws AuthenticationFailureException, RemoteException {
            throw new AssertionError();
        }

        @Override
        public LoginResult proxyLogin(ProxyCredentials pc, AuthContext ac,
                                      short sv)
            throws AuthenticationFailureException,
            AuthenticationRequiredException, RemoteException {
            throw new AssertionError();
        }

        @Override
        public LoginResult
            renewPasswordLogin(PasswordCredentials pc, char[] np, short sv)
            throws AuthenticationFailureException, RemoteException {
            throw new AssertionError();
        }

        @Override
        public Subject
            validateLoginToken(LoginToken lt, AuthContext ac, short sv)
            throws SessionAccessException, RemoteException {

            if (DO_THROW_SAE == fault) {
                throw new SessionAccessException("bad session");
            } else if (DO_THROW_IFE_SAE == fault) {
                throw new SNAFaultException(
                    new SessionAccessException("bad session in fault"));
            }
            throw new IllegalArgumentException("no fault specified");
        }

        @Override
        public void logout(LoginToken lt, short sv)
            throws RemoteException, AuthenticationRequiredException,
            SessionAccessException {
            throw new AssertionError();
        }
    }

    static class FaultAsyncUserLogin implements AsyncUserLogin {

        private FaultUserLogin delegate;
        FaultAsyncUserLogin(FaultUserLogin delegate) {
            this.delegate = delegate;
        }

        FaultAsyncUserLogin setFault(int fault) {
            delegate.setFault(fault);
            return this;
        }

        @Override
        public @NonNull CompletableFuture<Short>
            getSerialVersion(short sv, long tm) {
            throw new AssertionError();
        }

        @Override
        public CompletableFuture<LoginResult>
            login(short sv, LoginCredentials lc, long tm) {
            throw new AssertionError();
        }

        @Override
        public CompletableFuture<LoginResult>
            proxyLogin(short sv, ProxyCredentials pc, AuthContext ac, long tm) {
            throw new AssertionError();
        }

        @Override
        public CompletableFuture<LoginResult>
            renewPasswordLogin(short sv, PasswordCredentials pc, char[] np,
                               long tm) {
            throw new AssertionError();
        }

        @Override
        public CompletableFuture<LoginToken>
            requestSessionExtension(short sv, LoginToken lt, long tm) {
            throw new AssertionError();
        }

        @Override
        public CompletableFuture<Subject>
            validateLoginToken(short sv, LoginToken lt, AuthContext ac,
                               long tm) {
            try {
                delegate.validateLoginToken(lt, ac, sv);
                return failedFuture(new IllegalArgumentException("no fault"));
            } catch (Throwable t) {
                return failedFuture(t);
            }
        }

        @Override
        public CompletableFuture<Void>
            logout(short sv, LoginToken lt, long tm) {
            throw new AssertionError();
        }
    }
}
