/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.registry;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.StandardDialogTypeFamily.SERVICE_REGISTRY_DIALOG_TYPE;
import static oracle.kv.impl.security.ssl.SSLConfig.CLIENT_AUTHENTICATOR;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_FILE;
import static oracle.kv.impl.security.ssl.SSLConfig.SERVER_HOST_VERIFIER;
import static oracle.kv.impl.security.ssl.SSLConfig.TRUSTSTORE_FILE;
import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.ListenerConfigBuilder;
import oracle.kv.impl.async.ListenerPortRange;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.registry.ClearClientSocketFactory;
import oracle.kv.impl.util.registry.ClientSocketFactory;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/** Test {@link ServiceRegistryAPI} and related classes. */
@RunWith(FilterableParameterized.class)
public class ServiceRegistryTest extends UncaughtExceptionTestBase {

    private static final long TIMEOUT = 5000;

    private static final DialogTypeFamily dialogTypeFamily =
        new DialogTypeFamily() {
            { DialogType.registerTypeFamily(this); }
            @Override
            public int getFamilyId() { return 42; }
            @Override
            public String getFamilyName() { return "DialogTypeFamily"; }
            @Override
            public String toString() { return "DialogTypeFamily42"; }
    };

    private static final DialogType dialogType =
        new DialogType(dialogTypeFamily, 1);

    private final boolean secure;
    private @Nullable EndpointGroup endpointGroup;
    private @Nullable ListenerConfig listenerConfig;

    public ServiceRegistryTest(boolean secure) {
        this.secure = secure;
    }

    @Parameters(name="secure={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        endpointGroup = new NioEndpointGroup(logger, 2);
        listenerConfig = new ListenerConfigBuilder()
            .portRange(new ListenerPortRange(6000, 26000))
            .endpointConfigBuilder(createEndpointConfigBuilder(true))
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (endpointGroup != null) {
            endpointGroup.shutdown(true);
        }
    }

    /* -- Tests -- */

    @Test
    public void testSimple() throws Exception {
        final ServiceRegistry server = new ServiceRegistryImpl(logger);
        EndpointGroup.ListenHandle listenHandle = getEndpointGroup().listen(
            getListenerConfig(),
            SERVICE_REGISTRY_DIALOG_TYPE.getDialogTypeId(),
            () -> new ServiceRegistryResponder(server, logger));
        NetworkAddress localAddress =
            listenHandle.getLocalAddress().toNetworkAddress();
        CreatorEndpoint endpoint = getEndpointGroup().getCreatorEndpoint(
            "perfName",
            new InetNetworkAddress("localhost", localAddress.getPort()),
            InetNetworkAddress.ANY_LOCAL_ADDRESS,
            createEndpointConfigBuilder(false).build());
        final CompletableFuture<ServiceRegistryAPI> apiFuture =
            ServiceRegistryAPI.wrap(endpoint, null /* clientId */, TIMEOUT,
                                    DEFAULT_REGISTRY_READ_TIMEOUT, logger);
        final ServiceRegistryAPI api =
            checkNull("api", apiFuture.get(TIMEOUT, MILLISECONDS));

        assertEquals(Collections.emptyList(), list(api));
        assertEquals(null, lookup(api, "foo"));

        unbind(api, "foo");

        final ClientSocketFactory clientSocketFactory =
            new ClearClientSocketFactory("bar", 1, 2, null /* clientId */);
        final ServiceEndpoint serviceEndpoint = new ServiceEndpoint(
            new InetNetworkAddress("localhost", 0), dialogType,
            clientSocketFactory);

        bind(api, "foo", serviceEndpoint);
        assertEquals(serviceEndpoint, lookup(api, "foo"));
        assertEquals(Collections.singletonList("foo"), list(api));

        final ServiceEndpoint serviceEndpoint2 = new ServiceEndpoint(
            new InetNetworkAddress("localhost", 37), dialogType,
            clientSocketFactory);

        bind(api, "foo", serviceEndpoint2);
        assertEquals(serviceEndpoint2, lookup(api, "foo"));

        unbind(api, "foo");
        assertEquals(Collections.emptyList(), list(api));
        assertEquals(null, lookup(api, "foo"));
    }

    private ListenerConfig getListenerConfig() {
        return checkNull("listenerConfig", listenerConfig);
    }

    private EndpointGroup getEndpointGroup() {
        return checkNull("endpointGroup", endpointGroup);
    }

    @Test
    public void testThrow() throws Exception {
        final String msg = "Test exception message";
        final ServiceRegistry server = new ServiceRegistryImpl(logger) {
            @Override
            public CompletableFuture<List<String>> list(short serialVersion,
                                                        long timeout) {
                /*
                 * Pick an unusual exception type, so we're sure we're getting
                 * the right one.
                 */
                return failedFuture(new NumberFormatException(msg));
            }
        };
        EndpointGroup.ListenHandle listenHandle = getEndpointGroup().listen(
            getListenerConfig(),
            SERVICE_REGISTRY_DIALOG_TYPE.getDialogTypeId(),
            () -> new ServiceRegistryResponder(server, logger));

        NetworkAddress localAddress =
            listenHandle.getLocalAddress().toNetworkAddress();
        CreatorEndpoint endpoint = getEndpointGroup().getCreatorEndpoint(
            "perfName",
            new InetNetworkAddress("localhost", localAddress.getPort()),
            InetNetworkAddress.ANY_LOCAL_ADDRESS,
            createEndpointConfigBuilder(false).build());
        final CompletableFuture<ServiceRegistryAPI> apiFuture =
            ServiceRegistryAPI.wrap(endpoint, null /* clientId */, TIMEOUT,
                                    DEFAULT_REGISTRY_READ_TIMEOUT, logger);
        final ServiceRegistryAPI api =
            checkNull("api", apiFuture.get(TIMEOUT, MILLISECONDS));

        try {
            list(api);
            fail("Expected NumberFormatException");
        } catch (NumberFormatException e) {
            assertEquals(msg, e.getMessage());
        }
    }

    @Test
    public void testWrongDialogTypeFamily() throws Exception {
        final ServiceRegistry server = new ServiceRegistryImpl(logger);
        EndpointGroup.ListenHandle listenHandle = getEndpointGroup().listen(
            getListenerConfig(),
            SERVICE_REGISTRY_DIALOG_TYPE.getDialogTypeId(),
            () -> new ServiceRegistryResponder(server, logger));
        NetworkAddress localAddress =
            listenHandle.getLocalAddress().toNetworkAddress();
        CreatorEndpoint endpoint = getEndpointGroup().getCreatorEndpoint(
            "perfName",
            new InetNetworkAddress("localhost", localAddress.getPort()),
            InetNetworkAddress.ANY_LOCAL_ADDRESS,
            createEndpointConfigBuilder(false).build());
        final CompletableFuture<ServiceRegistryAPI> apiFuture =
            ServiceRegistryAPI.wrap(endpoint, null /* clientId */, TIMEOUT,
                                    DEFAULT_REGISTRY_READ_TIMEOUT, logger);
        final ServiceRegistryAPI api =
            checkNull("api", apiFuture.get(TIMEOUT, MILLISECONDS));

        /* Bind with the wrong dialog type family */
        final ClientSocketFactory clientSocketFactory =
            new ClearClientSocketFactory("bar", 1, 2, null /* clientId */);
        final ServiceEndpoint serviceEndpoint = new ServiceEndpoint(
            new InetNetworkAddress("localhost", 0),
            SERVICE_REGISTRY_DIALOG_TYPE, clientSocketFactory);
        final CompletableFuture<Void> bindFuture =
            api.bind("foo", serviceEndpoint, TIMEOUT);
        bindFuture.get(TIMEOUT, MILLISECONDS);

        CompletableFuture<ServiceEndpoint> lookupFuture =
            api.lookup("foo", dialogTypeFamily, TIMEOUT);
        orTimeout(lookupFuture, TIMEOUT, MILLISECONDS)
            .handle((se, e) -> {
                    checkException(e, IllegalStateException.class,
                                   "Unexpected dialog type family");
                    return null;
                })
            .get();

        lookupFuture = api.lookup("foo", null, TIMEOUT);
        lookupFuture.get(TIMEOUT, MILLISECONDS);
    }

    /* -- Other methods -- */

    /**
     * Cancels the future if it does not complete in the specified amount of
     * time.
     *
     * TODO: Use CompletableFuture.orTimeout when we switch to Java 9 or
     * later
     */
    private static <T>
        CompletableFuture<T> orTimeout(CompletableFuture<T> future,
                                       long time,
                                       TimeUnit unit) {
        final Semaphore done = new Semaphore(0);
        CompletableFuture.runAsync(() -> {
                try {
                    if (!done.tryAcquire(time, unit)) {
                        future.cancel(false);
                    }
                } catch (InterruptedException e) {
                }
            });
        future.whenComplete((t, e) -> done.release());
        return future;
    }

    /**
     * Like CompletableFuture.get but use whenComplete to gain access to the
     * exception without the unwrapping that the get method does, which might
     * mask problems with CompletionException wrapping.
     */
    private static <T> T getWithTimeout(CompletableFuture<T> future,
                                        long time,
                                        TimeUnit unit)
        throws Exception {

        final AtomicReference<Throwable> exception = new AtomicReference<>();
        try {
            return orTimeout(future, time, unit)
                .whenComplete((t, e) -> exception.set(e))
                .get();
        } catch (Exception e) {
            if (exception.get() instanceof Exception) {
                throw (Exception) exception.get();
            }
            throw e;
        }
    }

    private @Nullable List<String> list(ServiceRegistryAPI api)
        throws Exception
    {
        return getWithTimeout(api.list(TIMEOUT), TIMEOUT, MILLISECONDS);
    }

    private @Nullable ServiceEndpoint lookup(ServiceRegistryAPI api,
                                             String name)
        throws Exception
    {
        return getWithTimeout(api.lookup(name, dialogTypeFamily, TIMEOUT),
                              TIMEOUT, MILLISECONDS);
    }

    private void bind(ServiceRegistryAPI api,
                      String name,
                      ServiceEndpoint endpoint)
        throws Exception
    {
        getWithTimeout(api.bind(name, endpoint, TIMEOUT),
                       TIMEOUT, MILLISECONDS);
    }

    private void unbind(ServiceRegistryAPI api, String name)
        throws Exception
    {
        getWithTimeout(api.unbind(name, TIMEOUT), TIMEOUT, MILLISECONDS);
    }

    private EndpointConfigBuilder createEndpointConfigBuilder(boolean server)
        throws Exception {

        final EndpointConfigBuilder builder = new EndpointConfigBuilder();
        if (secure) {
            builder.sslControl(
                createSSLConfig().makeSSLControl(server, logger));
        }
        return builder;
    }

    private SSLConfig createSSLConfig() {
        final File sslDir = SSLTestUtils.getTestSSLDir();
        final String clientAuthenticator = "dnmatch(CN=Unit Test)";
        final String serverhostVerifier = "dnmatch(CN=Unit Test)";
        final String keystorePath =
            new File(sslDir.getPath(), SSLTestUtils.SSL_KS_NAME).getPath();
        final String truststorePath =
            new File(sslDir.getPath(), SSLTestUtils.SSL_CTS_NAME).getPath();

        final Properties props = new Properties();
        props.setProperty(CLIENT_AUTHENTICATOR, clientAuthenticator);
        props.setProperty(SERVER_HOST_VERIFIER, serverhostVerifier);
        props.setProperty(KEYSTORE_FILE, keystorePath);
        props.setProperty(TRUSTSTORE_FILE, truststorePath);

        final SSLConfig config = new SSLConfig(props);
        config.setKeystorePassword("unittest".toCharArray());
        return config;
    }
}
