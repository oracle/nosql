/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.registry;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.MethodCallUtils.checkFastSerializeResponse;
import static oracle.kv.impl.async.MethodCallUtils.serializeMethodCall;
import static oracle.kv.impl.async.StandardDialogTypeFamily.CLIENT_ADMIN_SERVICE_TYPE_FAMILY;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import oracle.kv.TestBase;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.UnixDomainNetworkAddress;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.ClearClientSocketFactory;
import oracle.kv.impl.util.registry.ssl.SSLClientSocketFactory;

import org.junit.Test;

/** Test serialization for classes in this package. */
public class RegistrySerializationTest extends TestBase {
    private static final ClearClientSocketFactory clearCsf =
        new ClearClientSocketFactory(
            "mycsf", 5002, 5003, new ClientId(5004), 1 /* csfId */);
    private static final ServiceEndpoint serviceEndpointClear =
        new ServiceEndpoint(
            new InetNetworkAddress("myhost", 5000),
            new DialogType(CLIENT_ADMIN_SERVICE_TYPE_FAMILY, 1),
            clearCsf);

    /* Tests */

    @Test
    public void testServiceEndpoint() throws Exception {
        checkAll(serialVersionChecker(
                     serviceEndpointClear,
                     SerialVersion.MINIMUM, 0xbbb20e9050a78f29L),
                 serialVersionChecker(
                     new ServiceEndpoint(
                         new InetNetworkAddress("myhost", 5000),
                         new DialogType(CLIENT_ADMIN_SERVICE_TYPE_FAMILY, 2),
                         new SSLClientSocketFactory(
                             "mycsf", 5002, 5003, "mystore",
                             new ClientId(5004),
                             SSLClientSocketFactory.Use.USER,
                             2 /* csfId */)),
                     SerialVersion.MINIMUM, 0xfa053e05434cb68eL),
                 serialVersionChecker(
                     new ServiceEndpoint(
                         new UnixDomainNetworkAddress("/a/b/c", 5000),
                         new DialogType(CLIENT_ADMIN_SERVICE_TYPE_FAMILY, 3),
                         clearCsf),
                     SerialVersion.MINIMUM, 0x7f50652ae06aaf62L));
    }

    @Test
    public void testGetSerialVersionCall() throws Exception {
        final ServiceRegistry.GetSerialVersionCall call =
            serializeMethodCall(new ServiceRegistry.GetSerialVersionCall());
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractServiceRegistry() {
                @Override
                public CompletableFuture<Short>
                    getSerialVersion(short sv, long tm)
                {
                    return completedFuture(SerialVersion.CURRENT);
                }
            })
            .thenAccept(result -> assertSame(SerialVersion.CURRENT, result))
            .get();
        checkFastSerializeResponse(call, Short.valueOf((short) 3));
    }

    @Test
    public void testLookupCall() throws Exception {
        final ServiceRegistry.LookupCall call =
            serializeMethodCall(
                new ServiceRegistry.LookupCall("lookupName", null));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractServiceRegistry() {
                @Override
                public CompletableFuture<ServiceEndpoint> lookup(
                    short sv, String name, long timeoutMs)
                {
                    assertEquals("lookupName", name);
                    return completedFuture(serviceEndpointClear);
                }
            })
            .thenAccept(result -> assertSame(serviceEndpointClear, result))
            .get();
        checkFastSerializeResponse(call, serviceEndpointClear);
        checkFastSerializeResponse(call, null);
    }

    @Test
    public void testBindCall() throws Exception {
        final ServiceRegistry.BindCall call = serializeMethodCall(
            new ServiceRegistry.BindCall("bindName", serviceEndpointClear));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractServiceRegistry() {
                @Override
                public CompletableFuture<Void> bind(short sv, String name,
                                                    ServiceEndpoint endpoint,
                                                    long timeoutMs)
                {
                    assertEquals("bindName", name);
                    assertEquals(serviceEndpointClear, endpoint);
                    return completedFuture(null);
                }
            })
            .thenAccept(result -> assertSame(null, result))
            .get();
        checkFastSerializeResponse(call, null);
    }

    @Test
    public void testUnbindCall() throws Exception {
        final ServiceRegistry.UnbindCall call =
            serializeMethodCall(new ServiceRegistry.UnbindCall("unbindName"));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractServiceRegistry() {
                @Override
                public CompletableFuture<Void> unbind(short sv, String name,
                                                      long timeoutMs)
                {
                    assertEquals("unbindName", name);
                    return completedFuture(null);
                }
            })
            .thenAccept(result -> assertSame(null, result))
            .get();
        checkFastSerializeResponse(call, null);
    }

    @Test
    public void testListCall() throws Exception {
        final ServiceRegistry.ListCall list =
            serializeMethodCall(new ServiceRegistry.ListCall());
        final List<String> listResult = new ArrayList<>();
        listResult.add("a");
        listResult.add("b");
        checkFastSerializeResponse(list, listResult);
    }

    /* Other methods and classes */

    static class AbstractServiceRegistry implements ServiceRegistry {
        @Override
        public CompletableFuture<Short> getSerialVersion(short sv, long t) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<ServiceEndpoint>
            lookup(short sv, String n, long t) { throw new AssertionError(); }
        @Override
        public CompletableFuture<Void> bind(short sv, String n,
                                            ServiceEndpoint e, long t) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Void> unbind(short sv, String n, long t) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<List<String>> list(short sv, long t) {
            throw new AssertionError();
        }
    }
}
