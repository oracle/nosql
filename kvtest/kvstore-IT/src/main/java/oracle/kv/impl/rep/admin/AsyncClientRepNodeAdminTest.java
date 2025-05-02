/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.admin;

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.MethodCallUtils.checkFastSerializeResponse;
import static oracle.kv.impl.async.MethodCallUtils.fastSerializeResponse;
import static oracle.kv.impl.async.MethodCallUtils.serializeMethodCall;
import static oracle.kv.util.TestUtils.assertEqualClasses;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import oracle.kv.TestBase;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.api.table.TableMetadata.TableMetadataKey;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.security.util.SNKrbInstance;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.change.Remove;
import oracle.kv.impl.util.SerialVersion;

import com.sleepycat.je.rep.ReplicatedEnvironment;

import org.junit.Test;

/** Tests for the AsyncClientRepNodeAdmin class. */
public class AsyncClientRepNodeAdminTest extends TestBase {
    private static final MetadataInfo sampleTopologyInfo =
        new TopologyInfo(
            1, 2,
            Collections.singletonList(new Remove(1, new RepNodeId(2, 3))),
            new byte[] { 4, 5 });

    private static final Topology sampleTopology = new Topology("mystore");

    private static final KerberosPrincipals sampleKerberosPrincipals =
        new KerberosPrincipals(
            new SNKrbInstance[] { new SNKrbInstance("instName", 2) });

    /* Tests */

    @Test
    public void testSerializeGetSerialVersionCall() throws Exception {
        final AsyncClientRepNodeAdmin.GetSerialVersionCall call =
            serializeMethodCall(
                new AsyncClientRepNodeAdmin.GetSerialVersionCall());
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<Short>
                    getSerialVersion(short sv, long tm)
                {
                    return completedFuture(SerialVersion.CURRENT);
                }
            })
            .thenAccept(result -> assertSame(SerialVersion.CURRENT, result))
            .get();
        checkFastSerializeResponse(call, (short) 3);
    }

    @Test
    public void testSerializeGetTopologyCall() throws Exception {
        final AsyncClientRepNodeAdmin.GetTopologyCall call =
            serializeMethodCall(new AsyncClientRepNodeAdmin.GetTopologyCall(
                                    null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<Topology>
                    getTopology(short sv, AuthContext authCtx, long tm)
                {
                    assertEquals(null, authCtx);
                    return completedFuture(sampleTopology);
                }
            })
            .thenAccept(result -> assertSame(sampleTopology, result))
            .get();
        fastSerializeResponse(call, sampleTopology);
        fastSerializeResponse(call, null);
    }

    @Test
    public void testSerializeGetTopoSeqNumCall() throws Exception {
        final AsyncClientRepNodeAdmin.GetTopoSeqNumCall call =
            serializeMethodCall(new AsyncClientRepNodeAdmin.GetTopoSeqNumCall(
                                    null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<Integer>
                    getTopoSeqNum(short sv, AuthContext authCtx, long tm)
                {
                    assertEquals(null, authCtx);
                    return completedFuture(55);
                }
            })
            .thenAccept(result -> assertSame(55, result))
            .get();
        checkFastSerializeResponse(call, 55);
    }

    @Test
    public void testSerializeGetVlsnCall() throws Exception {
        final AsyncClientRepNodeAdmin.GetVlsnCall call =
            serializeMethodCall(new AsyncClientRepNodeAdmin.GetVlsnCall(
                                    null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<Long>
                    getVlsn(short sv, AuthContext authCtx, long tm)
                {
                    assertEquals(null, authCtx);
                    return completedFuture(55l);
                }
            })
            .thenAccept(result -> {
                          assertSame(55l, result);
                       })
            .get();
        checkFastSerializeResponse(call, 55l);
    }

    @Test
    public void testSerializeGetHAHostPortCall() throws Exception {
        final AsyncClientRepNodeAdmin.GetHAHostPortCall call =
            serializeMethodCall(new AsyncClientRepNodeAdmin.GetHAHostPortCall(
                                    null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<String>
                    getHAHostPort(short sv, AuthContext authCtx, long tm)
                {
                    assertEquals(null, authCtx);
                    return completedFuture("myhost:4444");
                }
            })
            .thenAccept(result -> assertEquals("myhost:4444", result))
            .get();
        checkFastSerializeResponse(call, "myhost:4444");
        checkFastSerializeResponse(call, null);
    }

    @Test
    public void testSerializeGetReplicationStateCall() throws Exception {
        final AsyncClientRepNodeAdmin.GetReplicationStateCall call =
            serializeMethodCall(
                new AsyncClientRepNodeAdmin.GetReplicationStateCall(
                    null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<ReplicatedEnvironment.State>
                    getReplicationState(short sv, AuthContext authCtx, long tm)
                {
                    assertEquals(null, authCtx);
                    return completedFuture(null);
                }
            })
            .thenAccept(result -> assertEquals(null, result))
            .get();
        checkFastSerializeResponse(call, ReplicatedEnvironment.State.MASTER);
        checkFastSerializeResponse(call, null);
    }

    @Test
    public void testSerializeGetMetadataSeqNumCall() throws Exception {
        for (final MetadataType testType :
                 asList(MetadataType.TOPOLOGY, null)) {
            final AsyncClientRepNodeAdmin.GetMetadataSeqNumCall call =
                serializeMethodCall(
                    new AsyncClientRepNodeAdmin.GetMetadataSeqNumCall(
                        testType, null /* authCtx */));
            call.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncClientRepNodeAdmin() {
                    @Override
                    public CompletableFuture<Integer>
                        getMetadataSeqNum(short sv, MetadataType type,
                                          AuthContext authCtx, long tm)
                    {
                        assertEquals(testType, type);
                        assertEquals(null, authCtx);
                        return completedFuture(55);
                    }
                })
            .thenAccept(result -> assertSame(55, result))
            .get();
            checkFastSerializeResponse(call, 55);
        }
    }

    @Test
    public void testSerializeGetMetadataCall() throws Exception {
        for (final MetadataType testType :
                 asList(MetadataType.TOPOLOGY, null)) {
            final AsyncClientRepNodeAdmin.GetMetadataCall call =
                serializeMethodCall(
                    new AsyncClientRepNodeAdmin.GetMetadataCall(
                        testType, null /* authCtx */));
            call.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncClientRepNodeAdmin() {
                    @Override
                    public CompletableFuture<Metadata<?>>
                        getMetadata(short sv, MetadataType type,
                                    AuthContext authCtx, long tm)
                    {
                        assertEquals(testType, type);
                        assertEquals(null, authCtx);
                        return completedFuture(sampleTopology);
                    }
                })
                .thenAccept(result -> assertSame(sampleTopology, result))
                .get();
            fastSerializeResponse(call, sampleTopology);
            fastSerializeResponse(call, null);
        }
    }

    @Test
    public void testSerializeGetMetadataStartCall() throws Exception {
        for (final MetadataType testType :
                 asList(MetadataType.TOPOLOGY, null)) {
            final AsyncClientRepNodeAdmin.GetMetadataStartCall call =
                serializeMethodCall(
                    new AsyncClientRepNodeAdmin.GetMetadataStartCall(
                        testType, 42, null /* authCtx */));
            call.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncClientRepNodeAdmin() {
                    @Override
                    public CompletableFuture<MetadataInfo>
                        getMetadata(short sv, MetadataType type, int seqNum,
                                    AuthContext authCtx, long tm)
                    {
                        assertEquals(testType, type);
                        assertEquals(42, seqNum);
                        assertEquals(null, authCtx);
                        return completedFuture(sampleTopologyInfo);
                    }
                })
                .thenAccept(result -> assertSame(sampleTopologyInfo, result))
                .get();
            fastSerializeResponse(call, sampleTopologyInfo);
            fastSerializeResponse(call, null);
        }
    }

    @Test
    public void testSerializeGetMetadataKeyCall() throws Exception {
        for (final MetadataType testType :
            new MetadataType[]{MetadataType.TOPOLOGY, null}) {
            for (final MetadataKey testKey :
                new TableMetadataKey[]{new TableMetadataKey("mytable"), null}) {
                final AsyncClientRepNodeAdmin.GetMetadataKeyCall call =
                    serializeMethodCall(
                        new AsyncClientRepNodeAdmin.GetMetadataKeyCall(
                            testType, testKey, 42, null /* authCtx */));
                call.callService(
                    SerialVersion.CURRENT, 10,
                    new AbstractAsyncClientRepNodeAdmin() {
                        @Override
                        public CompletableFuture<MetadataInfo>
                            getMetadata(short sv, MetadataType type,
                                        MetadataKey key, int seqNum,
                                        AuthContext authCtx, long tm)
                        {
                            assertEquals(testType, type);
                            assertEqualClasses(testKey, key);
                            assertEquals(42, seqNum);
                            assertEquals(null, authCtx);
                            return completedFuture(sampleTopologyInfo);
                        }
                    })
                .thenAccept(result -> assertSame(sampleTopologyInfo, result))
                .get();
                fastSerializeResponse(call, sampleTopologyInfo);
                fastSerializeResponse(call, null);
            }
        }
    }

    @Test
    public void testSerializeUpdateMetadataCall() throws Exception {
        for (final Metadata<?> testMetadata :
                 asList(sampleTopology, null)) {
            final AsyncClientRepNodeAdmin.UpdateMetadataCall call =
                serializeMethodCall(
                    new AsyncClientRepNodeAdmin.UpdateMetadataCall(
                        testMetadata, null /* authCtx */));
            call.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncClientRepNodeAdmin() {
                    @Override
                    public CompletableFuture<Void>
                        updateMetadata(short sv, Metadata<?> newMetadata,
                                       AuthContext authCtx, long tm)
                    {
                        assertEqualClasses(testMetadata, newMetadata);
                        assertEquals(null, authCtx);
                        return completedFuture(null);
                    }
                })
                .thenAccept(result -> assertEquals(null, result))
                .get();
            checkFastSerializeResponse(call, null);
        }
    }

    @Test
    public void testSerializeUpdateMetadataInfoCall() throws Exception {
        for (final MetadataInfo testInfo : asList(sampleTopologyInfo, null)) {
            final AsyncClientRepNodeAdmin.UpdateMetadataInfoCall call =
                serializeMethodCall(
                    new AsyncClientRepNodeAdmin.UpdateMetadataInfoCall(
                        testInfo, null /* authCtx */));
            call.callService(
                SerialVersion.CURRENT, 10,
                new AbstractAsyncClientRepNodeAdmin() {
                    @Override
                    public CompletableFuture<Integer>
                        updateMetadata(short sv, MetadataInfo metadataInfo,
                                       AuthContext authCtx, long tm)
                    {
                        assertEqualClasses(testInfo, metadataInfo);
                        assertEquals(null, authCtx);
                        return completedFuture(37);
                    }
                })
                .thenAccept(result -> assertSame(37, result))
                .get();
            checkFastSerializeResponse(call, 37);
        }
    }

    @Test
    public void testSerializeGetKerberosPrincipalsCall() throws Exception {
        final AsyncClientRepNodeAdmin.GetKerberosPrincipalsCall call =
            serializeMethodCall(
                new AsyncClientRepNodeAdmin.GetKerberosPrincipalsCall(
                    null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<KerberosPrincipals>
                    getKerberosPrincipals(short sv, AuthContext authCtx,
                                          long tm)
                {
                    assertEquals(null, authCtx);
                    return completedFuture(sampleKerberosPrincipals);
                }
            })
            .thenAccept(result -> assertSame(sampleKerberosPrincipals, result))
            .get();
        fastSerializeResponse(call, sampleKerberosPrincipals);
    }

    @Test
    public void testSerializeGetTableByIdCall() throws Exception {
        final AsyncClientRepNodeAdmin.GetTableByIdCall call =
            serializeMethodCall(
                new AsyncClientRepNodeAdmin.GetTableByIdCall(
                    42, null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<MetadataInfo>
                    getTableById(short sv, long tableId, AuthContext authCtx,
                                 long tm)
                {
                    assertEquals(42, tableId);
                    assertEquals(null, authCtx);
                    return completedFuture(null);
                }
            })
            .thenAccept(result -> assertSame(null, result))
            .get();
        checkFastSerializeResponse(call, null);
    }

    @Test
    public void testSerializeGetTableCall() throws Exception {
        final AsyncClientRepNodeAdmin.GetTableCall call =
            serializeMethodCall(new AsyncClientRepNodeAdmin.GetTableCall(
                                    "getTableNamespace", "getTableName", 33,
                                    null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<MetadataInfo>
                    getTable(short sv, String namespace, String tableName,
                             int cost, AuthContext authCtx, long tm)
                {
                    assertEquals("getTableNamespace", namespace);
                    assertEquals("getTableName", tableName);
                    assertEquals(33, cost);
                    assertEquals(null, authCtx);
                    return completedFuture(null);
                }
            })
            .thenAccept(result -> assertSame(null, result))
            .get();
        final AsyncClientRepNodeAdmin.GetTableCall nullCall =
            serializeMethodCall(new AsyncClientRepNodeAdmin.GetTableCall(
                                    null /* tableNamespace */,
                                    null /* tableName */, 33,
                                    null /* authCtx */));
        nullCall.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientRepNodeAdmin() {
                @Override
                public CompletableFuture<MetadataInfo>
                    getTable(short sv, String namespace, String tableName,
                             int cost, AuthContext authCtx, long tm)
                {
                    assertEquals(null, namespace);
                    assertEquals(null, tableName);
                    assertEquals(33, cost);
                    assertEquals(null, authCtx);
                    return completedFuture(null);
                }
            });
        checkFastSerializeResponse(call, null);
    }

    /* Other methods and classes */

    static class AbstractAsyncClientRepNodeAdmin
            implements AsyncClientRepNodeAdmin {
        @Override
        public CompletableFuture<Short> getSerialVersion(short sv, long t) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Topology>
            getTopology(short sv, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Integer>
            getTopoSeqNum(short sv, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Long>
            getVlsn(short sv, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<String>
            getHAHostPort(short sv, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<ReplicatedEnvironment.State>
            getReplicationState(short sv, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Integer>
            getMetadataSeqNum(short sv, MetadataType mt, AuthContext a,
                              long tm) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Metadata<?>>
            getMetadata(short sv, MetadataType type, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<MetadataInfo>
            getMetadata(short sv, MetadataType mt, int seqNum, AuthContext a,
                        long tm) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<MetadataInfo>
            getMetadata(short sv, MetadataType mt, MetadataKey mk, int sn,
                        AuthContext a, long tm) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Void>
            updateMetadata(short sv, Metadata<?> nm, AuthContext a, long tm) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Integer>
            updateMetadata(short sv, MetadataInfo mi, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<KerberosPrincipals>
            getKerberosPrincipals(short sv, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<MetadataInfo>
            getTableById(short sv, long ti, AuthContext a, long tm)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<MetadataInfo>
            getTable(short sv, String n, String tn, int c, AuthContext a,
                     long tm) {
            throw new AssertionError();
        }
    }
}
