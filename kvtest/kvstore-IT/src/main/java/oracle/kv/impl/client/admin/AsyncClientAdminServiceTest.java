/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.client.admin;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.MethodCallUtils.checkFastSerializeResponse;
import static oracle.kv.impl.async.MethodCallUtils.fastSerializeResponse;
import static oracle.kv.impl.async.MethodCallUtils.serializeMethodCall;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.TestBase;
import oracle.kv.impl.util.SerialVersion;

import oracle.nosql.common.contextlogger.CorrelationId;

import org.junit.Test;

/** Tests for the AsyncClientAdminService class. */
public class AsyncClientAdminServiceTest extends TestBase {
    private static final ExecutionInfo sampleExecutionInfo =
        new ExecutionInfoImpl(3, true, "info", "infoAsJson", true, true,
                              "errorMessage", true, "results");

    /* Tests */

    @Test
    public void testSerializeGetSerialVersionCall() throws Exception {
        final AsyncClientAdminService.GetSerialVersionCall call =
            serializeMethodCall(
                new AsyncClientAdminService.GetSerialVersionCall());
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientAdminService() {
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
    public void testSerializeExecutionCall() throws Exception {
        final char[] executeStatement = { 'e', 'x', 'e', 'c' };
        final LogContext logContext =
            new LogContext(
                new oracle.nosql.common.contextlogger.LogContext(
                    CorrelationId.getNext(), "entry", "origin", Level.INFO));
        final AsyncClientAdminService.ExecuteCall call =
            serializeMethodCall(new AsyncClientAdminService.ExecuteCall(
                                    executeStatement,
                                    "executeNamespace",
                                    true /* validateNamespace */,
                                    TableLimits.READ_ONLY,
                                    logContext,
                                    null /* authContext */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientAdminService() {
                @Override
                public CompletableFuture<ExecutionInfo> execute(
                    short sv, char[] statement, String namespace,
                    boolean validateNamespace, TableLimits limits,
                    LogContext lc, AuthContext authCtx, long timeoutMillis)
                {
                    assertArrayEquals(executeStatement, statement);
                    assertEquals("executeNamespace", namespace);
                    assertTrue(validateNamespace);
                    assertNotNull(limits);
                    assertNotNull(lc);
                    assertEquals(null, authCtx);
                    return completedFuture(sampleExecutionInfo);
                }
            })
            .thenAccept(result -> assertEquals(sampleExecutionInfo, result))
            .get();
        final AsyncClientAdminService.ExecuteCall nullCall =
            serializeMethodCall(new AsyncClientAdminService.ExecuteCall(
                                    null, /* executeStatement */
                                    null, /* executeNamespace */
                                    false, /* validateNamespace */
                                    null, /* limits */
                                    null, /* logContext */
                                    null /* authContext */));
        nullCall.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientAdminService() {
                @Override
                public CompletableFuture<ExecutionInfo> execute(
                    short sv, char[] statement, String namespace,
                    boolean validateNamespace, TableLimits limits,
                    LogContext lc, AuthContext authCtx, long timeoutMillis)
                {
                    assertEquals(null, statement);
                    assertEquals(null, namespace);
                    assertFalse(validateNamespace);
                    assertEquals(null, limits);
                    assertEquals(null, lc);
                    assertEquals(null, authCtx);
                    return completedFuture(sampleExecutionInfo);
                }
            })
            .thenAccept(result -> assertEquals(sampleExecutionInfo, result))
            .get();
        fastSerializeResponse(call, sampleExecutionInfo);
    }

    @Test
    public void testSerializeSetTableLimitsCall() throws Exception {
        final AsyncClientAdminService.SetTableLimitsCall call =
            serializeMethodCall(
                new AsyncClientAdminService.SetTableLimitsCall(
                    "setTableLimitsNamespace", "setTableLimitsTable",
                    TableLimits.READ_ONLY, null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientAdminService() {
                @Override
                public CompletableFuture<ExecutionInfo> setTableLimits(
                    short sv, String namespace, String tableName,
                    TableLimits limits, AuthContext authCtx, long tm)
                {
                    assertEquals("setTableLimitsNamespace", namespace);
                    assertEquals("setTableLimitsTable", tableName);
                    assertNotNull(limits);
                    assertEquals(null, authCtx);
                    return completedFuture(sampleExecutionInfo);
                }
            })
            .thenAccept(result -> assertEquals(sampleExecutionInfo, result))
            .get();
        final AsyncClientAdminService.SetTableLimitsCall nullCall =
            serializeMethodCall(new AsyncClientAdminService.SetTableLimitsCall(
                                    null, /* setTableLimitsNamespace */
                                    null, /* setTableLimitsTable */
                                    null, /* limits */
                                    null /* authCtx */));
        nullCall.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientAdminService() {
                @Override
                public CompletableFuture<ExecutionInfo> setTableLimits(
                    short sv, String namespace, String tableName,
                    TableLimits limits, AuthContext authCtx, long tm)
                {
                    assertEquals(null, namespace);
                    assertEquals(null, tableName);
                    assertEquals(null, limits);
                    assertEquals(null, authCtx);
                    return completedFuture(sampleExecutionInfo);
                }
            })
            .thenAccept(result -> assertEquals(sampleExecutionInfo, result))
            .get();
        fastSerializeResponse(call, sampleExecutionInfo);
    }

    @Test
    public void testSerializeGetExecutionStatusCall() throws Exception {
        final AsyncClientAdminService.GetExecutionStatusCall call =
            serializeMethodCall(
                new AsyncClientAdminService.GetExecutionStatusCall(
                    34, null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientAdminService() {
                @Override
                public CompletableFuture<ExecutionInfo> getExecutionStatus(
                    short sv, int planId, AuthContext authCtx, long tm)
                {
                    assertEquals(34, planId);
                    assertEquals(null, authCtx);
                    return completedFuture(sampleExecutionInfo);
                }
            })
            .thenAccept(result -> assertEquals(sampleExecutionInfo, result))
            .get();
        fastSerializeResponse(call, sampleExecutionInfo);
    }

    @Test
    public void testSerializeCanHandleDDLCall() throws Exception {
        final AsyncClientAdminService.CanHandleDDLCall call =
            serializeMethodCall(new AsyncClientAdminService.CanHandleDDLCall(
                                    null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientAdminService() {
                @Override
                public CompletableFuture<Boolean> canHandleDDL(
                    short sv, AuthContext authCtx, long tm)
                {
                    assertEquals(null, authCtx);
                    return completedFuture(null);
                }
            });
        checkFastSerializeResponse(call, true);
    }

    @Test
    public void testSerializeGetMasterRMIAddressCall() throws Exception {
        final AsyncClientAdminService.GetMasterRMIAddressCall call =
            serializeMethodCall(
                new AsyncClientAdminService.GetMasterRMIAddressCall(
                    null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientAdminService() {
                @Override
                public CompletableFuture<URI> getMasterRmiAddress(
                    short sv, AuthContext authCtx, long tm)
                {
                    assertEquals(null, authCtx);
                    return completedFuture(null);
                }
            })
            .thenAccept(result -> assertEquals(null, result))
            .get();
        checkFastSerializeResponse(call, new URI("file:abc"));
        checkFastSerializeResponse(call, null);
    }

    @Test
    public void testSerializeInterruptAndCancelCall() throws Exception {
        final AsyncClientAdminService.InterruptAndCancelCall call =
            serializeMethodCall(
                new AsyncClientAdminService.InterruptAndCancelCall(
                    34, null /* authCtx */));
        call.callService(
            SerialVersion.CURRENT, 10,
            new AbstractAsyncClientAdminService() {
                @Override
                public CompletableFuture<ExecutionInfo> interruptAndCancel(
                    short sv, int planId, AuthContext authCtx, long tm)
                {
                    assertEquals(34, planId);
                    assertEquals(null, authCtx);
                    return completedFuture(sampleExecutionInfo);
                }
            })
            .thenAccept(result -> assertEquals(sampleExecutionInfo, result))
            .get();
        fastSerializeResponse(call, sampleExecutionInfo);
    }

    /* Other methods and classes */

    static class AbstractAsyncClientAdminService
            implements AsyncClientAdminService {

        @Override
        public CompletableFuture<Short> getSerialVersion(short sv, long t) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<ExecutionInfo>
            execute(short sv, char[] s, String n, boolean v, TableLimits tl,
                    LogContext l, AuthContext a, long tm) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<ExecutionInfo>
            setTableLimits(short sv, String n, String tn, TableLimits tl,
                           AuthContext a, long tm) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<ExecutionInfo>
            getExecutionStatus(short sv, int p, AuthContext a, long t)
        {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<Boolean> canHandleDDL(short sv, AuthContext a,
                                                       long t) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<URI> getMasterRmiAddress(short sv,
                                                          AuthContext a,
                                                          long t) {
            throw new AssertionError();
        }
        @Override
        public CompletableFuture<ExecutionInfo>
            interruptAndCancel(short sv, int p, AuthContext a, long t)
        {
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
    }
}
