/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.tif;

import static com.sleepycat.je.utilint.VLSN.FIRST_VLSN;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.migration.generation.PartitionGenDBManager;
import oracle.kv.impl.rep.migration.generation.PartitionGeneration;
import oracle.kv.impl.rep.subscription.partreader.PartitionReader;
import oracle.kv.impl.rep.subscription.partreader.PartitionReaderCallBack;
import oracle.kv.impl.rep.subscription.partreader.PartitionReaderStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.rep.subscription.SubscriptionCallback;
import com.sleepycat.je.rep.subscription.SubscriptionConfig;

import org.junit.Test;

/**
 * Unit tests to test SubscriptionManager.
 */
public class SubscriptionTest extends TextIndexFeederTestBase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        config = new KVRepTestConfig(this, NUM_DC, NUM_SN, REP_Factor,
                                     numPartitions);

        /* incr. max concurrent streams migration source can service */
        final RepNodeId sourceId = new RepNodeId(1, 1);
        config.getRepNodeParams(sourceId).getMap().
            setParameter(ParameterState.RN_PM_CONCURRENT_SOURCE_LIMIT, "8");
    }

    @Override
    public void tearDown() throws Exception {
        config.stopRNs();
        super.tearDown();
    }

    /*
     * Test that would first preload some data and use replication consumer to
     * stream from source and verify all received data.
     */
    @Test
    public void testOngoingRepPhase() throws Exception {

        /* set data size */
        numRecords = numPartitions * 1024;

        prepareTestEnv();

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final SourceRepNode sourceRepNode =
            new SourceRepNode(testStoreName, rg1rn1);

        final SubscriptionConfig conf = buildConfig(rg1rn1.getEnv(60000));
        final SubscriptionManager subManager =
            new SubscriptionManager(sourceRepNode,
                                    new HostRepNode(tifNodeName, rg1rn1),
                                    conf, logger);

        /* replace default tif cbk with test cbk */
        final TestSubscriptionCbk cbk = new TestSubscriptionCbk(numRecords);
        conf.setCallback(cbk);

        /* get all data from replication stream */
        subManager.startStream(FIRST_VLSN);
        if (subManager.getState() != SubscriptionState.REPLICATION_STREAM) {
            fail("expect ongoing replication state but get " +
                 subManager.getState());
        }

        try {
            waitForTestDone(cbk);
        } catch (TimeoutException e) {
            fail("test timeout");
        }

        subManager.shutdown(SubscriptionState.SHUTDOWN);

        /* receive expected number of K/V pairs */
        assert (cbk.getReceivedKV().size() == testData.size());
        /* all operations are puts */
        assert (cbk.getNumPuts() == testData.size());
        /* no deletions */
        assert (cbk.getNumDels() == 0);

        /* verify each K/V received pair is correct */
        verify(cbk.getReceivedKV());
    }

    /*
     * Test that would first preload some data and use multiple, concurrent
     * partition readers to stream it from source
     */
    @Test
    public void testInitialRepPhase() throws Exception {

        final Set<TestPartReaderCbk> allPartCbks = new HashSet<>();

        /* set data size */
        numRecords = numPartitions * 1024;

        prepareTestEnv();

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final SourceRepNode sourceRepNode =
            new SourceRepNode(testStoreName, rg1rn1);

        final SubscriptionConfig conf = buildConfig(rg1rn1.getEnv(60000));
        final SubscriptionManager subManager =
            new SubscriptionManager(sourceRepNode,
                                    new HostRepNode(tifNodeName, rg1rn1),
                                    conf, logger);

        for (PartitionId pid : testDataByPartition.keySet()) {
            final int expected = testDataByPartition.get(pid).size();
            final TestPartReaderCbk cbk =
                new TestPartReaderCbk(subManager, pid, expected);
            allPartCbks.add(cbk);
            subManager.setPartitionReaderCallBack(pid, cbk);
        }

        /* use partition readers to transfer all data */
        subManager.startStream(subManager.getManagedPartitions());
        if (subManager.getState() != SubscriptionState.PARTITION_TRANSFER) {
            fail("expect initial subscription state but get " +
                 subManager.getState());
        }

        /* check each partition reader until all transfer is done */
        waitForTestDone(allPartCbks);

        /* check cbk of each partition to ensure all valid data */
        for (TestPartReaderCbk cbk : allPartCbks) {
            verifyByPartition(cbk.getPartitionId(), cbk.getReceivedKV());
        }
    }

    /* verify each K/V received is valid from generated test data */
    private void verify(Map<Key, Value> receivedKV) {
        for (Map.Entry<Key, Value> keyValueEntry : receivedKV.entrySet()) {
            final Key key = keyValueEntry.getKey();
            final Value val = keyValueEntry.getValue();
            verifyKV(key, val);
        }
    }

    /* verify if a set of KV are valid data from a partition */
    private void verifyByPartition(PartitionId partitionId,
                                   Map<Key, Value> receivedKV) {

        /* all received keys should from that partition */
        final List<Key> expectedKeys = testDataByPartition.get(partitionId);
        for (Map.Entry<Key, Value> entry : receivedKV.entrySet()) {
            final Key recvKey = entry.getKey();
            assertTrue("Expect received key " + recvKey + " belong to " +
                       partitionId, expectedKeys.contains(recvKey));
        }

        /* verify all received KV should be valid */
        verify(receivedKV);
    }

    /* wait for test done, by checking subscription callback */
    private void waitForTestDone(final TestSubscriptionCbk callBack)
        throws TimeoutException {

        final boolean success = new PollCondition(TEST_POLL_INTERVAL_MS,
                                                  TEST_POLL_TIMEOUT_MS) {
            @Override
            protected boolean condition() {
                return callBack.isTestDone();
            }
        }.await();

        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    /* wait for test done, by checking a set of part reader callback */
    private void waitForTestDone(final Set<TestPartReaderCbk> allCbks)
        throws TimeoutException {

        final boolean success = new PollCondition(TEST_POLL_INTERVAL_MS,
                                                  TEST_POLL_TIMEOUT_MS) {
            @Override
            protected boolean condition() {
                for (TestPartReaderCbk cbk : allCbks) {
                    if (!cbk.isTestDone()) {
                        return false;
                    }
                }

                /* all part readers are done */
                return true;
            }
        }.await();

        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    /**
     * Callback used in test to process subscription message
     * <p/>
     * The callback records each entry received from replication stream
     */
    private class TestSubscriptionCbk implements SubscriptionCallback {

        private int expectedPuts;
        private int numPuts;
        private int numDels;
        private Map<Key, Value> receivedKV;

        /* we know how many keys we expect to receive */
        TestSubscriptionCbk(int expectedPuts) {
            super();
            this.expectedPuts = expectedPuts;
            numDels = 0;
            numPuts = 0;
            receivedKV = new HashMap<>();
        }

        @Override
        public void processPut(long vlsn, byte[] k, byte[] v, long txnId,
                               DatabaseId unused, long ts, long exp,
                               boolean beforeImgEnabled,
                               byte[] valBeforeImg,
                               long tsBeforeImg,
                               long expBeforeImg) {
            /* skip entry from generation table */
            if (isFromPartGenTable(k, v)) {
                return;
            }
            numPuts++;
            /* remember what we receive */
            final Key key = Key.fromByteArray(k);
            final Value val = Value.fromByteArray(v);
            receivedKV.put(key, val);
        }

        @Override
        public void processDel(long vlsn, byte[] key, byte[] val, long txnId,
                               DatabaseId unused, long ts,
                               boolean beforeImgEnabled,
                               byte[] valBeforeImg,
                               long tsBeforeImg,
                               long expBeforeImg) {
            numDels++;
        }

        @Override
        public void processCommit(long vlsn, long txnId, long ts) {

        }

        @Override
        public void processAbort(long vlsn, long txnId, long ts) {

        }

        @Override
        public void processException(final Exception exp) {

        }

        Map<Key, Value> getReceivedKV() {
            return receivedKV;
        }

        public int getNumPuts() {
            return numPuts;
        }

        public int getNumDels() {
            return numDels;
        }

        boolean isTestDone() {
            return (numPuts == expectedPuts);
        }

        private boolean isFromPartGenTable(byte[] key, byte[] val) {
            try {
                PartitionGenDBManager.readPartIdFromKey(key);
                final PartitionGeneration pgn =
                PartitionGenDBManager.readPartGenFromVal(val);
                logger.info("Got PGN entry " + pgn);
            } catch (Exception exp) {
                /* cannot deserialize to PGN entry */
                return false;
            }

            return true;
        }
    }

    /**
     * Default callback to process each entry received from partition migration
     * stream.
     */
    private class TestPartReaderCbk implements PartitionReaderCallBack {

        private final SubscriptionManager manager;
        private final PartitionId partitionId;
        private int expectedPuts;
        private int numPuts;
        private Map<Key, Value> receivedKV;

        TestPartReaderCbk(SubscriptionManager manager,
                          PartitionId partitionId,
                          int expectedPuts) {
            this.manager = manager;
            this.partitionId = partitionId;
            this.expectedPuts = expectedPuts;

            numPuts = 0;
            receivedKV = new HashMap<>();
        }

        @Override
        public void processCopy(PartitionId pid, long vlsn,
                                long expirationTime,
                                byte[] k, byte[] v) {
            assert (partitionId.equals(pid));

            numPuts++;

            final Key key = Key.fromByteArray(k);
            final Value val = Value.fromByteArray(v);
            receivedKV.put(key, val);



            /* dump some trace into log for debugging */
            if (numPuts % 100 == 0) {
                logger.info("for" + partitionId +
                            ", total keys expected " + expectedPuts +
                            ", num of puts received " + numPuts);
            }

            if (numPuts == expectedPuts) {
                logger.info(partitionId + ": received all expected " +
                            expectedPuts + " keys");

            }
        }

        @Override
        public void processPut(PartitionId pid, long vlsn,
                               long expirationTime,
                               byte[] key, byte[] value, long txnId) {
            processCopy(pid, vlsn, expirationTime, key, value);
        }


        @Override
        public void processDel(PartitionId pid, long vlsn, byte[] key,
                               long txnId) {

        }

        @Override
        public void processPrepare(PartitionId pid, long txnId) {

        }

        @Override
        public void processCommit(PartitionId pid, long txnId) {

        }

        @Override
        public void processAbort(PartitionId pid, long txnId) {

        }

        @Override
        public void processEOD(PartitionId pid) {

        }

        public PartitionId getPartitionId() {
            return partitionId;
        }

        Map<Key, Value> getReceivedKV() {
            return receivedKV;
        }

        boolean isTestDone() {
            if (numPuts < expectedPuts) {
                return false;
            }
            final PartitionReader reader =
                manager.getPartitionReaderMap().get(partitionId);

            return (reader.getStatus()
                          .getState()
                          .equals(PartitionReaderStatus
                                      .PartitionRepState
                                      .DONE));

        }
    }
}
