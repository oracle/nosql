/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.DurabilityException;
import oracle.kv.EntryStream;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.KeyValue;
import oracle.kv.KeyRange;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.ClientTestBase;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.stats.KVStats;
import oracle.kv.stats.NodeMetrics;
import oracle.kv.stats.OperationMetrics;

import org.junit.Assume;
import org.junit.Test;


/**
 * Verifies the API associated with KV statistics
 */
public class StatsTest extends ClientTestBase {

    private final LoginManager LOGIN_MGR = null;

    /* Number of requests (API calls) made by exerciseAllOps. */
    private static final int N_REQUESTS = 20;

    @Override
    public void setUp() throws Exception {
        /*
         *  This test does not create any tables, so no need to run
         *  multi-region table mode.
         */
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    @Test
    public void testStats()
        throws Exception {

        /*
         * Turn off async state updates and therefore use of NOP requests,
         * so that the op stats are deterministic.
         */
        RepNodeStateUpdateThread.setUpdateState(false);
        store.getStats(true);

        // TODO: It would be better to check the metrics after each operation
        // rather than in aggregate as is done currently.
        int expectNOps = exerciseAllOps();
        int expectNReq = N_REQUESTS;

        KVStats stats1 = store.getStats(false);
        // System.err.println(stats1);

        /* Verify no exceptions during toString method */
        store.getStats(false).toString();

        verifySerializeDeserialize(stats1);

        long nOps = 0;
        long nReq = 0;
        for (OperationMetrics metrics : stats1.getOpMetrics()) {

            /*
             * We should have exercised every operation. The iterator
             * metrics may have counts > 1 associated with them.
             *
             * Table-specific operations are not exercised or
             * counted in this test.
             */
            if (!isTableOperation(metrics.getOperationName())) {
                assertTrue(metrics.getOperationName(),
                           metrics.getTotalOpsLong() > 0);
                assertTrue(metrics.getOperationName(),
                           metrics.getTotalRequestsLong() > 0);
            }
            nOps += metrics.getTotalOpsLong();
            nReq += metrics.getTotalRequestsLong();
        }
        assertEquals(expectNOps, nOps);
        assertEquals(expectNReq, nReq);

        long totalRequests = 0;
        for (NodeMetrics metrics : stats1.getNodeMetrics()) {
            totalRequests += metrics.getRequestCount();
        }

        /* Accumulate more stats. */
        expectNOps += exerciseAllOps();
        expectNReq += N_REQUESTS;

        /* Clear after they are obtained, this time. */
        KVStats stats2 = store.getStats(true);

        assertEquals(stats1.getOpMetrics().size(),
                     stats2.getOpMetrics().size());

        /* Verify that counts have doubled. */
        nOps = 0;
        nReq = 0;
        Iterator<OperationMetrics> stats2i = stats2.getOpMetrics().iterator();
        for (OperationMetrics metrics1 : stats1.getOpMetrics()) {
            OperationMetrics metrics2 = stats2i.next();
            assertEquals(metrics1.getOperationName(),
                         metrics2.getOperationName());
            assertEquals(metrics2.toString(),
                         metrics1.getTotalOpsLong() * 2,
                         metrics2.getTotalOpsLong());
            nOps += metrics2.getTotalOpsLong();
            nReq += metrics2.getTotalRequestsLong();
        }
        assertFalse(stats2i.hasNext());
        assertEquals(expectNOps, nOps);
        assertEquals(expectNReq, nReq);

        long totalRequests2 = 0;
        for (NodeMetrics metrics : stats2.getNodeMetrics()) {
            totalRequests2 += metrics.getRequestCount();
        }
        assertEquals(totalRequests * 2, totalRequests2);

        /* Verify that stats have been reset, from previous call to clear */
        KVStats statsClear = store.getStats(false);

        for (OperationMetrics metrics : statsClear.getOpMetrics()) {
            assertEquals(0, metrics.getTotalOpsLong());
            assertEquals(0, metrics.getTotalRequestsLong());
        }

        for (NodeMetrics metrics : statsClear.getNodeMetrics()) {
            assertEquals(0, metrics.getRequestCount());
        }
        store.getStats(false).toString();
        // System.err.println(store.getStats(false));
    }

    /**
     * Test that OperationMetrics getTotal{Ops,Request}Long methods return the
     * correct values and that the int versions return max values. [#27517]
     */
    /* Ignore deprecation warnings for checks on obsolete methods */
    @SuppressWarnings("deprecation")
    @Test
    public void testLongStatLimits() {
        RequestDispatcher requestDispatcher =
            createNiceMock(RequestDispatcher.class);
        final Map<OpCode, LatencyResult> map = new HashMap<>();
        map.put(OpCode.GET,
                new LatencyResult(2, 1,
                                  0, 100, 1,
                                  1, 5, 0));
        map.put(OpCode.PUT,
                new LatencyResult(2_000_000_000_000L,
                            1_000_000_000_000L,
                            0, 100, 1,
                            1, 5, 0));
        expect(requestDispatcher.getLatencyStats("", false)).andReturn(map);
        replay(requestDispatcher);

        final KVStats stats = new KVStats("", false, requestDispatcher);
        for (OperationMetrics opMetrics : stats.getOpMetrics()) {
            if ("get".equals(opMetrics.getOperationName())) {
                assertEquals(1, opMetrics.getTotalOps());
                assertEquals(1, opMetrics.getTotalOpsLong());
                assertEquals(2, opMetrics.getTotalRequests());
                assertEquals(2, opMetrics.getTotalRequestsLong());
            } else if ("put".equals(opMetrics.getOperationName())) {
                assertEquals(Integer.MAX_VALUE, opMetrics.getTotalOps());
                assertEquals(1_000_000_000_000L, opMetrics.getTotalOpsLong());
                assertEquals(Integer.MAX_VALUE, opMetrics.getTotalRequests());
                assertEquals(2_000_000_000_000L,
                             opMetrics.getTotalRequestsLong());
            } else {
                fail("Unexpected opMetrics: " + opMetrics);
            }
        }
    }

    private void verifySerializeDeserialize(KVStats stats1)
        throws IOException, ClassNotFoundException {

        /* Serialize */
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutputStream oos = new ObjectOutputStream(bos) ;
        oos.writeObject(stats1);
        oos.close();

        /* De-serialize */
        ByteArrayInputStream bis =
                new ByteArrayInputStream(bos.toByteArray()) ;
        ObjectInputStream ois = new ObjectInputStream(bis);
        KVStats stats2 = (KVStats) ois.readObject();

        /* Verify that they are the same. */
        Iterator<OperationMetrics> s2oi = stats2.getOpMetrics().iterator();
        for (OperationMetrics metrics1 : stats1.getOpMetrics()) {
            OperationMetrics metrics2 = s2oi.next();
            assertEquals(metrics1.toString(), metrics2.toString());
        }

        Iterator<NodeMetrics> s2ni = stats2.getNodeMetrics().iterator();
        for (NodeMetrics metrics1 : stats1.getNodeMetrics()) {
            NodeMetrics metrics2 = s2ni.next();
            assertEquals(metrics1.toString(), metrics2.toString());
        }
    }

    /**
     * Utility method to exercise all operations in the API once, to ensure
     * that statistics are generated for each one of them.
     * @throws FaultException
     * @throws OperationExecutionException
     * @throws DurabilityException
     * @return number of ops executed.  The number of requests executed is
     * fixed and is always N_REQUESTS.
     */
    private int exerciseAllOps()
        throws DurabilityException,
               OperationExecutionException,
               FaultException {

        int nOps = 0;
        int nReq = 0;

        final Key key = Key.createKey("one", "two");
        final Value val = Value.createValue(new byte[1]);

        Version ver = store.put(key, val);
        store.putIfPresent(key, val);
        store.putIfAbsent(key, val);
        store.putIfVersion(key, val, ver);
        store.get(key);
        store.delete(key);
        store.deleteIfVersion(key, ver);
        store.put(Key.createKey("a", "x"), val);
        nOps += 8;
        nReq += 8;

        /* putBatch op */
        final TestStream stream =
            new TestStream(new KeyValue(Key.createKey("a", "1"), val));
        store.put(Arrays.asList((EntryStream<KeyValue>) stream), null);
        assertTrue(stream.isCompleted);
        nOps += 1;
        nReq += 1;

        final Key keyA = Key.createKey("a");
        nOps += store.multiDelete(keyA, null, null);
        nReq += 1;

        final OperationFactory factory = store.getOperationFactory();
        final List<Operation> ops = new ArrayList<Operation>();
        final Key keyB = Key.createKey("b");
        ops.add(factory.createPutIfAbsent(Key.createKey("b", "1"), val));
        ops.add(factory.createPutIfAbsent(Key.createKey("b", "2"), val));
        store.execute(ops);
        nOps += ops.size();
        nReq += 1;

        assertEquals(2, store.multiGet(keyB, null, null).size());
        assertEquals(2, store.multiGetKeys(keyB, null, Depth.CHILDREN_ONLY).
                        size());
        nOps += 4;
        nReq += 2;

        assertEquals(2, runIterator(store.multiGetIterator
                     (Direction.FORWARD, 2, keyB, null,
                      Depth.CHILDREN_ONLY)));
        assertEquals(2, runIterator(store.multiGetKeysIterator
                     (Direction.FORWARD, 2, keyB, null,
                      Depth.CHILDREN_ONLY)));
        nOps += 4;
        nReq += 2;

        /* multiGetBatch op */
        assertEquals(2, runIterator(store.storeIterator
                                    (Arrays.asList(keyB).iterator(),
                                     0, null, Depth.CHILDREN_ONLY,
                                     null, 0, null, null)));
        /* multiGetBatchKeys op */
        assertEquals(2, runIterator(store.storeKeysIterator
                                    (Arrays.asList(keyB).iterator(),
                                     0, null, Depth.CHILDREN_ONLY,
                                     null, 0, null, null)));
        nOps += 4;
        nReq += 2;

        nOps += runIterator(store.storeKeysIterator(Direction.UNORDERED,
                            100, null, new KeyRange("b"), null));
        nOps += runIterator(store.storeIterator(Direction.UNORDERED,
                            100, null, new KeyRange("b"), null));
        nReq += 2;

        exerciseNOP();
        nOps += 1;
        nReq += 1;

        assertEquals(N_REQUESTS, nReq);
        return nOps;
    }

    private int runIterator(Iterator<?> iter) {
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count += 1;
        }
        return count;
    }

    /**
     * The number of ops and requests executed is fixed and is always one.
     */
    private void exerciseNOP() {
        try {
            RequestDispatcher dispatcher =
                ((KVStoreImpl)store).getDispatcher();

            RepNodeState rns = dispatcher.getRepGroupStateTable().
                               getNodeState(new RepNodeId(1,1));
            dispatcher.executeNOP(rns, 5000, LOGIN_MGR);
        } catch (Exception e) {
            fail ("Unexpected exception:" + e);
        }
    }

    /**
     * Returns true if the named operation a table operation.  There is no
     * public API to get table operation names or state so this is fragile
     * and depends on the implementation of KVStats.OperationMetricsImpl.
     * getOperationName().
     */
    private boolean isTableOperation(String name) {
        if (name.equals("tableIterate") ||
            name.equals("tableKeysIterate") ||
            name.equals("multiGetTable") ||
            name.equals("multiGetTableKeys") ||
            name.equals("indexIterate") ||
            name.equals("indexKeysIterate") ||
            name.equals("multiDeleteTable") ||
            name.equals("multiGetBatchTable") ||
            name.contains("query") ||
            name.equals("multiGetBatchTableKeys") ||
            name.equals("getIdentity") ||
            name.equals("putResolve") ||
            name.equals("tableCharge")) {
            return true;
        }
        return false;
    }

    /**
     * The implementation of EntryStream<KeyValue> for putBulk operation.
     */
    private static class TestStream implements EntryStream<KeyValue> {

        private final List<KeyValue> list;
        private final Iterator<KeyValue> iterator;
        private boolean isCompleted;

        TestStream(KeyValue... keyValues) {
            list = Arrays.asList(keyValues);
            iterator = list.iterator();
            isCompleted = false;
        }

        @Override
        public String name() {
            return "TestStream";
        }

        @Override
        public KeyValue getNext() {
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }

        @Override
        public void completed() {
            isCompleted = true;
        }

        @Override
        public void keyExists(KeyValue entry) {
        }

        @Override
        public void catchException(RuntimeException runtimeException,
                                   KeyValue entry) {
        }
    }
}
