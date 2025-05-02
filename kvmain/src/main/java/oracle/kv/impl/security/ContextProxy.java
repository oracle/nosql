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
package oracle.kv.impl.security;

import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.impl.async.AsyncVersionedRemote;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.security.login.AsyncUserLogin;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.AbstractInvocationHandler;
import oracle.kv.impl.util.SerialVersion;

/**
 * Provide a proxy for an object that implements one or more Remote or
 * AsyncVersionedRemote interfaces, supplying security credentials as needed.
 * The proxy uses the supplied LoginHandle to create AuthContext objects for
 * methods that require them, as well as providing automated retry capability
 * in the event that a login token needs to be refreshed.
 *
 * @param <P> the proxy interface type
 * @param <T> the base remote interface type
 */
public abstract class ContextProxy<P extends T, T>
        extends AbstractInvocationHandler {

    /**
     * The number of handle attempts to allow. Must be greater than 0 in order
     * to enable handle renewal.
     */
    private static final int MAX_RENEW_ATTEMPTS = 1;

    /**
     * The number of retries due to SessionAccessException to allow. Must be
     * greater than 0 in order to enable retries.
     */
    private static final int MAX_SAE_RETRIES = 5;

    /*
     * A common error message specified in RemoteException when it's used to
     * wrap a SessionAccessException thrown by remote API call
     */
    private static final String SAE_ERROR_MSG = "Inaccessible session";

    /** The security-enabled object that be will be proxying for. */
    final P proxyTo;

    /** Set of methods that need AuthContext handling. */
    private final Set<Method> authMethods;

    /** The LoginHandle to use for acquiring LoginTokens. */
    final LoginHandle loginHdl;

    /*
     * Test hook will be invoked before RemoteProxy invoke method and skip
     * AuthenticationRequiredException and SessionAccessException retries if set
     */
    public static volatile TestHook<Integer> beforeInvokeNoAuthRetry;

    private ContextProxy(P proxyTo,
                         LoginHandle loginHdl,
                         int serialVersion,
                         Class<T> iface) {
        if (serialVersion < SerialVersion.MINIMUM) {
            throw new IllegalArgumentException("Serial version " +
                                               serialVersion +
                                               " is not supported");
        }
        this.proxyTo = proxyTo;
        this.loginHdl = loginHdl;

        /* The compile-time check that proxyTo is of type P is good enough */
        @SuppressWarnings("unchecked")
        final Class<? extends P> proxyToClass =
            (Class<? extends P>) proxyTo.getClass();

        authMethods = ProxyUtils.findInterfaceMethods(proxyToClass, iface);
        authMethods.removeIf(method -> !isAuthMethod(method));
    }

    /**
     * Returns true if the method requires handling of AuthContext parameters,
     * including retries. For both Remote and AsyncVersionedRemote methods, the
     * AuthContext parameter is always second from the end.
     */
    private static boolean isAuthMethod(Method method) {
        final Class<?>[] paramTypes = method.getParameterTypes();
        return (paramTypes.length >= 2) &&
            (paramTypes[paramTypes.length - 2] == AuthContext.class);
    }

    /**
     * Call the method directly if it does not require a login token, and
     * otherwise call invokeAuth.
     */
    @Override
    protected final Object invokeNonObject(Method method, Object[] args)
        throws Exception {

        if (!authMethods.contains(method)) {

            /*
             * This doesn't have the right signature, so don't do any automatic
             * handling
             */
            return invokeMethod(proxyTo, method, args);
        }

        return invokeAuth(method, args);
    }

    /** Invoke method with AuthContext handling. */
    abstract Object invokeAuth(Method method, Object[] args)
        throws Exception;

    /**
     * Create a proxy object for a Remote object.
     *
     * @param proxyTo the Remote object for which a context proxy is to be
     *        created
     * @param loginHdl an object that manages login token information for
     *        the caller
     * @param serialVersion the SerialVersion level at which the method
     *        invocations should take place
     *
     * @return a proxy instance
     */
    public static <T extends Remote> T create(T proxyTo,
                                              LoginHandle loginHdl,
                                              int serialVersion) {
        final ContextProxy<T, Remote> proxy =
            new RemoteProxy<T>(proxyTo, loginHdl, serialVersion);
        /* Make Eclipse happy */
        final Class<Remote> remoteClass = Remote.class;
        return createProxy(proxy, remoteClass);
    }

    /**
     * Create a proxy object for a AsyncVersionedRemote object.
     *
     * @param proxyTo the AsyncVersionedRemote object for which a context proxy
     *        is to be created
     * @param loginHdl an object that manages login token information for
     *        the caller
     * @param serialVersion the SerialVersion level at which the method
     *        invocations should take place
     *
     * @return a proxy instance
     */
    public static <T extends AsyncVersionedRemote>
        T createAsync(T proxyTo, LoginHandle loginHdl, int serialVersion)
    {
        final ContextProxy<T, AsyncVersionedRemote> proxy =
            new AsyncProxy<T>(proxyTo, loginHdl, serialVersion);
        /* Make Eclipse happy */
        final Class<AsyncVersionedRemote> asyncVersionedRemoteClass =
            AsyncVersionedRemote.class;
        return createProxy(proxy, asyncVersionedRemoteClass);
    }

    private static <P extends T, T> P createProxy(
        ContextProxy<P, T> proxyHandler, Class<T> iface)
    {
        final P proxyTo = proxyHandler.proxyTo;

        /* The compile-time check that proxyTo is of type P is good enough */
        @SuppressWarnings("unchecked")
        final Class<? extends P> proxyToClass =
            (Class<? extends P>) proxyTo.getClass();

        /*
         * Create a dynamic proxy instance that implements all of the supplied
         * interfaces
         */
        final Class<?>[] interfaces =
            ProxyUtils.findInterfaces(proxyToClass, iface)
            .toArray(new Class<?>[0]);

        /* No compile time checking is available for Proxy.newProxyInstance */
        @SuppressWarnings("unchecked")
        final P proxy = (P) Proxy.newProxyInstance(
            proxyToClass.getClassLoader(), interfaces, proxyHandler);
        return proxy;
    }

    /**
     * Translate exceptions as needed. The UserLogin and AsyncUserLogin
     * interfaces are expected to throw SessionAccessException, including if it
     * is the cause of an InternalFaultException. For all other interfaces, we
     * wrap the SessionAccessException in a RemoteException so that the caller
     * will retry in the same way as a network exception. [KVSTORE-1459]
     */
    private static <T> Exception getThrowException(T proxyTo,
                                                   RuntimeException exp){
        final boolean isLoginInterface =
            (proxyTo instanceof UserLogin ||
             proxyTo instanceof AsyncUserLogin);

        try {
            throw exp;
        } catch (SessionAccessException sae) {
            if (isLoginInterface) {
                return sae;
            }
            return new RemoteException(SAE_ERROR_MSG, sae);
        } catch (InternalFaultException ife) {
            if (SessionAccessException.class.getName().equals(
                ife.getFaultClassName())) {

                /*
                 * InternalFaultException may be thrown when remote
                 * service has issue finding the session. Replace it
                 * with a SessionAccessException to make it easier
                 * for caller to handle. It will be re-throw if this
                 * is a login interface, or wrap and throw as a
                 * RemoteException otherwise.
                 */
                final SessionAccessException sae =
                    new SessionAccessException(ife.toString());
                if (isLoginInterface) {
                    return sae;
                }
                return new RemoteException(SAE_ERROR_MSG, sae);
            }
            return ife;
        }
    }

    /**
     * An invocation handler for Remote interfaces with synchronous retries.
     */
    private static class RemoteProxy<P extends Remote>
            extends ContextProxy<P, Remote> {

        RemoteProxy(P proxyTo, LoginHandle loginHdl, int serialVersion) {
            super(proxyTo, loginHdl, serialVersion, Remote.class);
        }

        @Override
        Object invokeAuth(Method method, Object[] args)
            throws Exception {

            final AuthContext initialAuthContext =
                (AuthContext) args[args.length - 2];
            final int maxRenewAttempts =
                (loginHdl == null || initialAuthContext != null) ?
                0 :
                MAX_RENEW_ATTEMPTS;

            int renews = 0;
            int saeRetries = 0;

            while (true) {
                LoginToken token = null;
                if (initialAuthContext == null && loginHdl != null) {
                    token = loginHdl.getLoginToken();
                    if (token != null) {
                        args[args.length - 2] = new AuthContext(token);
                    }
                }

                try {
                    assert TestHookExecute.doHookIfSet(
                        beforeInvokeNoAuthRetry, null);
                    try {
                        return invokeMethod(proxyTo, method, args);
                    } catch (SessionAccessException sae) {
                        if (sae.getIsReturnSignal() ||
                            saeRetries++ >= MAX_SAE_RETRIES) {
                            throw sae;
                        }
                        saeRetries++;
                    } catch (AuthenticationRequiredException are) {
                        if (are.getIsReturnSignal() ||
                            renews++ >= maxRenewAttempts) {
                            throw are;
                        }

                        /*
                         * When login service is unavailable, renewToken may
                         * fail. Callers of Remote API should be aware of
                         * SessionAccessException thrown here in this case.
                         */
                        final LoginToken newToken = loginHdl.renewToken(token);
                        if (newToken == null || newToken == token) {
                            throw are;
                        }
                    }
                } catch (SessionAccessException | InternalFaultException e) {
                    throw getThrowException(proxyTo, e);
                }
            }
        }
    }

    /**
     * An invocation handler for AsyncVersionedRemote interfaces with
     * asynchronous retries and returning a CompletableFuture.
     */
    private static class AsyncProxy<P extends AsyncVersionedRemote>
            extends ContextProxy<P, AsyncVersionedRemote> {

        AsyncProxy(P proxyTo, LoginHandle loginHdl, int serialVersion) {
            super(proxyTo, loginHdl, serialVersion,
                  AsyncVersionedRemote.class);
        }

        @Override
        Object invokeAuth(Method method, Object[] args) {
            return new CallContext(method, args).call();
        }

        /**
         * Stores information about an async call to be passed to completion
         * handlers, to support retries.
         */
        private class CallContext {
            final Method method;
            final Object[] args;
            final AuthContext initialAuthContext;
            final int maxRenewAttempts;
            final CompletableFuture<Object> future;
            volatile int renews;
            volatile int saeRetries;
            volatile LoginToken token;
            CallContext(Method method, Object[] args) {
                this.method = method;
                this.args = args;
                initialAuthContext = (AuthContext) args[args.length - 2];
                maxRenewAttempts =
                    (loginHdl == null || initialAuthContext != null) ?
                    0 :
                    MAX_RENEW_ATTEMPTS;
                future = new CompletableFuture<>();
            }

            @Override
            public String toString() {
                return super.toString() + "[" +
                    "method=" + method +
                    " args=" + Arrays.toString(args) + "]";
            }

            CompletableFuture<Object> call() {
                try {
                    if (initialAuthContext == null && loginHdl != null) {
                        token = loginHdl.getLoginToken();
                        if (token != null) {
                            args[args.length - 2] = new AuthContext(token);
                        }
                    }
                    ((CompletableFuture<?>) invokeMethod(proxyTo, method,
                                                         args))
                        /* Complete asynchronously to avoid stack overflow */
                        .whenCompleteAsync(
                            unwrapExceptionVoid(this::whenComplete));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
                return future;
            }

            private void whenComplete(Object result, Throwable exception) {
                if (exception == null) {
                    future.complete(result);
                    return;
                }
                try {
                    try {
                        throw exception;
                    } catch (SessionAccessException sae) {
                        if (sae.getIsReturnSignal() ||
                            saeRetries++ >= MAX_SAE_RETRIES) {
                            throw sae;
                        }
                        saeRetries++;
                        call();
                    } catch (AuthenticationRequiredException are) {
                        if (are.getIsReturnSignal() ||
                            renews++ >= maxRenewAttempts) {
                            throw are;
                        }
                        final LoginToken newToken = loginHdl.renewToken(token);
                        if (newToken == null || newToken == token) {
                            throw are;
                        }
                        call();
                    }
                } catch (SessionAccessException | InternalFaultException e) {
                    future.completeExceptionally(getThrowException(proxyTo, e));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            }
        }
    }
}
