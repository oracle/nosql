/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.subscription;

import static com.sleepycat.je.utilint.VLSN.FIRST_VLSN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;
import oracle.kv.impl.map.HashKeyToPartitionMap;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.rep.subscription.partreader.PartitionReader;
import oracle.kv.impl.rep.subscription.partreader.PartitionReaderCallBack;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.KeyGenerator;
import oracle.kv.impl.util.Pair;
import oracle.kv.impl.util.PollCondition;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.subscription.SubscriptionConfig;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import com.sleepycat.je.utilint.InternalException;

import org.junit.Test;

/**
 * Unit tests for Partition Reader
 */
public class PartitionReaderTest extends RepNodeTestBase {

    /* turn on trace only if necessary */
    private static final boolean traceOnConsole = false;

    /* environment config parameters */
    private static final int REP_Factor = 3;
    private static final int NUM_SN = 3;
    private static final int NUM_DC = 1;
    private static final String nodeName = "test-subscriber";
    private static final String nodeHostPortPair = "localhost:6000";

    /* test data parameters */
    private static final String keyPrefix = "TextSearchUnitTest_Key_";
    private static final String valPrefix = "TextSearchUnitTest_Value_";
    private int numRecords = 1024;
    private int numPartitions = 10;
    private Map<Key, Value> testData;
    private Map<PartitionId, List<Key>> testDataByPartition;

    private KVRepTestConfig config;
    private KVStore kvs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        config = new KVRepTestConfig(this,
                NUM_DC, NUM_SN, REP_Factor, numPartitions);

    }

    @Override
    public void tearDown() throws Exception {
        config.stopRepNodeServices();
        config = null;
        if (kvs != null) {
            kvs.close();
        }
        super.tearDown();
    }


    /**
     * Tests that expected exception will be raised if streaming a non-existent
     * partition from source node.
     *
     * @throws Exception
     */
    @Test
    public void testReaderAPIs() throws Exception {

        /* smaller test data to speed up test */
        numRecords = 100;
        prepareTestEnv();

        final PartitionId nonExistentPid = new PartitionId(numPartitions+1);
        final RepNodeId masterId = new RepNodeId(1, 1);
        RepNode repNode = config.getRN(masterId);
        ReplicatedEnvironment env = repNode.getEnv(0);
        SubscriptionConfig subConf = createSubscriptionConfig(masterId);

        /* a test callback to subscribe a non-existent partition */
        SubscriptionTestCallback1 callBack =
            new SubscriptionTestCallback1(nonExistentPid, 1);
        /* create a partition reader */
        PartitionReader reader = new PartitionReader(env, nonExistentPid,
                callBack, subConf, logger);
        callBack.setPartitionReader(reader);

        /*
         * start streaming data from the partition, expect exception due to
         * invalid partition id.
         */
        try{
            trace("Start partition reader for a " +
                  "non-existent partition " + nonExistentPid);
            reader.run();
            fail("Did not see expected exception");
        } catch (InternalException e){
            trace("Caught expected internal exception: " +
                  e.getMessage());
        }
    }

    /**
     * Tests basic usage of partition reader by streaming a single partition
     * from source node
     *
     * @throws Exception
     */
    @Test
    public void testPartitionReaderSinglePartition() throws Exception {

        prepareTestEnv();

        /* stream a particular partition */
        PartitionId partitionId = new PartitionId(1);

        List<Key> allExpectedKeys = testDataByPartition.get(partitionId);
        int numExpectedKeys = allExpectedKeys.size();
        trace("The test will stream " + numExpectedKeys +
              " keys from source for partition " + partitionId);

        /* the reader is running on source (master) node */
        RepNodeId masterId = findMaster(partitionId);
        RepNode repNode = config.getRN(masterId);
        ReplicatedEnvironment env = repNode.getEnv(0);
        SubscriptionConfig subConf = createSubscriptionConfig(masterId);

        trace("Stream from source a single partition: " + partitionId);

        /* a test callback to subscribe from a valid partition */
        SubscriptionTestCallback1 callBack =
            new SubscriptionTestCallback1(partitionId, numExpectedKeys);
        /* create a reader */
        PartitionReader reader = new PartitionReader(env, partitionId,
                callBack, subConf, logger);
        callBack.setPartitionReader(reader);

        /*
         * start streaming from the partition; after receiving entries from
         * that partition, reader will shut off.
         */
        reader.run();

        /* verify */
        verifyResult(allExpectedKeys, callBack.getKeys());
    }

    /**
     * Tests basic usage of partition reader by streaming a randomly
     * selected partition from source
     *
     * @throws Exception
     */
    @Test
    public void testPartitionReaderRandomPartition() throws Exception {

        final long seed = 1234567890;

        prepareTestEnv();

        /* stream a random partition */
        Random random = new Random(seed);

        /*
         * sample a partition between [2, numPartitions], skip the first
         * partition since it has been tested in another testcase
         */
        int id = Math.min(numPartitions, 2 + random.nextInt(numPartitions - 1));
        PartitionId partitionId = new PartitionId(id);

        trace("A random partition " + id + " is picked, seed of " +
              "random generator is " + seed);

        assertTrue("Partition Id must be in a valid range.",
                   (id >= 1) && (id <= numPartitions));
        List<Key> allExpectedKeys = testDataByPartition.get(partitionId);
        int numExpectedKeys = allExpectedKeys.size();
        trace("The test will stream " + numExpectedKeys +
              " keys from source for partition " + partitionId);

        /* the reader is running on source (master) node */
        RepNodeId masterId = findMaster(new PartitionId(id));
        RepNode repNode = config.getRN(masterId);
        ReplicatedEnvironment env = repNode.getEnv(0);
        SubscriptionConfig subConf = createSubscriptionConfig(masterId);

        SubscriptionTestCallback1 callBack =
            new SubscriptionTestCallback1(partitionId, numExpectedKeys);
        PartitionReader reader = new PartitionReader(env, partitionId,
                callBack, subConf, logger);
        callBack.setPartitionReader(reader);

        /* start streaming data from the partition */
        reader.run();

        /* verify */
        verifyResult(allExpectedKeys, callBack.getKeys());
    }

    /**
     * Tests that client is able to cancel an ongoing partition transfer
     *
     * @throws Exception
     */
    @Test
    public void testCancel() throws Exception {
        /* smaller data set to speedup test */
        numRecords = 1024*10;

        prepareTestEnv();

        /* stream a particular, single partition */
        PartitionId partitionId = new PartitionId(1);

        List<Key> allExpectedKeys = testDataByPartition.get(partitionId);
        int numExpectedKeys = allExpectedKeys.size();
        trace("The test will stream " + numExpectedKeys +
              " keys from source for partition " + partitionId +
              " and stop in the middle of replication after receiving " +
              numExpectedKeys / 2 + " keys");

        /* the reader is running on source (master) node */
        RepNodeId masterId = findMaster(partitionId);
        RepNode repNode = config.getRN(masterId);
        ReplicatedEnvironment env = repNode.getEnv(0);
        SubscriptionConfig subConf = createSubscriptionConfig(masterId);

        trace("Stream from source a single partition: " + partitionId);

        /* cancel reader after receiving half of all items */
        numExpectedKeys = numExpectedKeys/2;
        SubscriptionTestCallback2 callBack =
            new SubscriptionTestCallback2(partitionId, numExpectedKeys);
        PartitionReader reader = new PartitionReader(env, partitionId,
                callBack, subConf, logger);
        callBack.setPartitionReader(reader);

        /*
         * start streaming data from the partition, it will be canceled
         * within callback
         */
        reader.run();

        /* verify we only receive partial data */
        assertEquals("Received items does not match", numExpectedKeys,
                     callBack.getNumKeysReceived());

    }

    /**
     * Tests that partition reader is able to track the highest VLSN of all
     * keys in the partition
     *
     * @throws Exception
     */
    @Test
    public void testTrackingHighestVLSN() throws Exception {

        /* smaller data set to speedup test */
        numRecords = 1024*3;

        prepareTestEnv();

        /* stream a particular, single partition */
        PartitionId partitionId = new PartitionId(1);

        List<Key> allExpectedKeys = testDataByPartition.get(partitionId);
        int numExpectedKeys = allExpectedKeys.size();
        trace("The test will stream " + numExpectedKeys +
              " keys from source for partition " + partitionId);

        /* the reader is running on source (master) node */
        RepNodeId masterId = findMaster(partitionId);
        RepNode repNode = config.getRN(masterId);
        ReplicatedEnvironment env = repNode.getEnv(0);
        SubscriptionConfig subConf = createSubscriptionConfig(masterId);

        trace("Stream from source a single partition: " + partitionId);

        /* a test callback tracking highest VLSN seen in that partition */
        SubscriptionTestCallback3 callBack =
            new SubscriptionTestCallback3(partitionId, numExpectedKeys);
        PartitionReader reader =
            new PartitionReader(env, partitionId, callBack, subConf, logger);
        callBack.setPartitionReader(reader);

        /* start streaming data from the partition */
        reader.run();

        /* verify */
        assertEquals("Reader should return the same highest VLSN seen by " +
                     "the callback",
                     callBack.getHighestVLSNInTest(),
                     reader.getHighestVLSN());
    }

    /**
     * Tests that source sends error response and partition reader handles it.
     *
     * @throws Exception
     */
    @Test
    public void testSourceErrorResponse() throws Exception {

        /* smaller data set to speedup test */
        numRecords = 1024;
        prepareTestEnv();

        /* stream a particular, single partition */
        PartitionId partitionId = new PartitionId(1);

        /* the reader is running on source (master) node */
        final RepNodeId masterId = findMaster(partitionId);
        final RepNode repNode = config.getRN(masterId);
        final SubscriptionConfig subConf = createSubscriptionConfig(masterId);
        /* inject test hook in migration manager */
        final ResponseHook responseHook = new ResponseHook();
        repNode.getMigrationManager().setResponseHook(responseHook);

        /*
         * create a thread with partition reader, the reader should receive
         * an error response from source and raise an exception
         */
        PartitionReaderThread1 t =
            new PartitionReaderThread1(partitionId, repNode, subConf);

        t.start();

        /* source send error response */
        responseHook.waitForHook();
        responseHook.sendResponse(Response.UNKNOWN_SERVICE);

        waitForTestThreadDone(t);

        assert(t.isTestSucc());
    }

    /**
     * Tests if source node is BUSY, the partition reader will sleep and retry.
     */
    @Test
    public void testSourceBusy() throws Exception {

        /* smaller data set to speedup test */
        numRecords = 1024;
        prepareTestEnv();

        /* stream a particular, single partition */
        PartitionId partitionId = new PartitionId(1);

        /* the reader is running on source (master) node */
        final RepNodeId masterId = findMaster(partitionId);
        final RepNode repNode = config.getRN(masterId);
        final SubscriptionConfig subConf = createSubscriptionConfig(masterId);
        /* inject test hook in migration manager */
        final ResponseHook responseHook = new ResponseHook();
        repNode.getMigrationManager().setResponseHook(responseHook);

        /*
         * create a thread to receive partition, the reader should retry once
         * due to hooked busy response.
         */
        PartitionReaderThread2 t =
            new PartitionReaderThread2(partitionId,
                                       testDataByPartition.get(partitionId)
                                                          .size(),
                                       repNode, subConf);
        t.start();

        /* source send busy response, so partition reader will retry */
        trace("wait for the first hook");
        responseHook.waitForHook();
        trace("got the first hook, sending BUSY response");
        responseHook.sendResponse(Response.BUSY);

        /* nullify response so PMS can respond normally */
        trace("wait for the second hook");
        responseHook.waitForHook();
        trace("got the second hook, sending OK response");
        responseHook.sendResponse(null);

        waitForTestThreadDone(t);
        assert(t.isTestSucc());
    }

    /**
     * Tests active writes during partition transfer will be forwarded
     * correctly to Partition Reader.
     */
    @Test
    public void testForwardingActiveWrites() throws Exception {

        List<Pair<Key, Value>> expectedEntries = new ArrayList<>();

        /* empty db, will create test data later */
        numRecords = 0;
        prepareTestEnv();

        /* stream a single partition */
        PartitionId partitionId = new PartitionId(1);

        /* the reader is running on source (master) node */
        final RepNodeId masterId = findMaster(partitionId);
        final RepNode source = config.getRN(masterId);
        final SubscriptionConfig subConf = createSubscriptionConfig(masterId);
        final ReplicatedEnvironment env = source.getEnv(0);

        /* Generate sorted keys in the partition on the source node. */
        final int numKeys = 3;
        final Key[] keys =
            new KeyGenerator(source).getSortedKeys(partitionId, numKeys);
        final Value[] values = new Value[keys.length];
        for (int i = 0; i < keys.length; i++) {
            values[i] = Value.createValue(("Value " + i).getBytes());
        }

        /* write all keys into store */
        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        for (int i = 0; i < keys.length; i++) {
            Key key = keys[i];
            Value val = values[i];
            kvs.put(key, val, null, Durability.COMMIT_SYNC, 0, null);
        }

        /*
         * only expect [0] and [1], entry [2] will be overridden by later
         * update, see comments below.
         */
        expectedEntries.add(new Pair<>(keys[0], values[0]));
        expectedEntries.add(new Pair<>(keys[1], values[1]));

        /*
         * This will cause the migration source to wait before each read. The
         * initial wait will be before the first read by the cursor.
         */
        ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);

        /* start partition reader */
        SubscriptionTestCallback callBack =
            new SubscriptionTestCallback4(partitionId);
        PartitionReader reader = new PartitionReader(env, partitionId,
                callBack, subConf, logger);
        callBack.setPartitionReader(reader);

        PartitionReaderThread3 t =
                new PartitionReaderThread3(reader, partitionId, callBack,
                                           source, subConf);
        t.start();

        /* Likewise the source is waiting */
        readHook.waitForHook();
        /* Release the hook to send a single k/v to the target */
        readHook.releaseHook();     // keys[0] sent
        readHook.waitForHook();
        readHook.releaseHook();     // keys[1] sent

        /* create new value */
        final Value[] newValues = new Value[keys.length];
        for (int i = 0; i < keys.length; i++) {
            newValues[i] = Value.createValue(("NewValue " + i).getBytes());
        }

        /*
         * Insert behind the current key, expect forwarding, increment the
         * expected received entries
         */
        kvs.put(keys[0], newValues[0], null, Durability.COMMIT_SYNC, 0, null);
        expectedEntries.add(new Pair<>(keys[0], newValues[0]));

        /*
         * Update keys ahead of the current key, expect no forwarding, no
         * change to expected number of received entries
         */
        kvs.put(keys[2], newValues[2], null, Durability.COMMIT_SYNC, 0, null);
        expectedEntries.add(new Pair<>(keys[2], newValues[2]));

        /* Let the read from disk finish */
        readHook.waitForHook();
        source.getMigrationManager().setReadHook(null);
        readHook.releaseHook();

        waitForTestThreadDone(t);
        assert(t.isTestSucc());

        /* verify - check if received expected number of entries */
        SubscriptionTestCallback4 cbk =
                (SubscriptionTestCallback4)t.getCallBack();
        List<Pair<Key, Value>> receivedEntries = cbk.getOrderedEntries();

        /*
         * expect receive keys in following order:
         * 1)keys[0], values[0]
         * 2)keys[1], values[1]
         *
         * these two can arrive out of order
         * keys[0], newValues[0] <- active forwarding
         * keys[2], newValues[2] <- no active forwarding, only receive once
         * for keys[2] with new value
         */
        assertEquals("Mismatched number of received entries",
                     expectedEntries.size(),
                     receivedEntries.size());

        /* verify each received entry */
        verifyOrderedEntries(expectedEntries, receivedEntries);
    }

    /**
     * Tests usage of partition reader by streaming multiple partitions
     * in parallel from source
     *
     * @throws Exception
     */
    @Test
    public void testPartitionReaderMultiPartition() throws Exception {
        numRecords = 1024*10;
        prepareTestEnv();

        List<PartitionReaderThread> threads = new ArrayList<>();
        List<SubscriptionTestCallback1> callbacks = new ArrayList<>();

        for (int i = 1; i <= numPartitions; i++) {
            PartitionId partitionId = new PartitionId(i);
            int numExpectedKeys =
                    (testDataByPartition.containsKey(partitionId)) ?
                    testDataByPartition.get(partitionId).size() : 0;

            /* continue if empty partition */
            if (numExpectedKeys == 0) {
                continue;
            }

            /* the reader is running on source (master) node */
            final RepNodeId masterId = findMaster(partitionId);
            final RepNode repNode = config.getRN(masterId);
            final ReplicatedEnvironment env = repNode.getEnv(0);
            final SubscriptionConfig subConf =
                    createSubscriptionConfig(masterId);

            SubscriptionTestCallback1 callBack =
                new SubscriptionTestCallback1(partitionId, numExpectedKeys);
            PartitionReader reader = new PartitionReader(env, partitionId,
                                                         callBack, subConf,
                                                         logger);
            callBack.setPartitionReader(reader);

            callbacks.add(callBack);

            PartitionReaderThread3 t =
                    new PartitionReaderThread3(reader, partitionId, callBack,
                                               repNode, subConf);
            threads.add(t);
            trace("Thread for partition " + partitionId +
                  " has been created, expected number of keys to stream " +
                  numExpectedKeys);
        }

        /* fire all readers! */
        for (Thread t : threads) {
            /* start partition reader */
            t.start();
        }

        waitForMultiTestThreadDone(threads);

        /* verify */
        trace("All threads done, start verification");
        for (SubscriptionTestCallback1 callBack : callbacks) {
            PartitionId partitionId = callBack.getPartitionId();
            verifyResult(testDataByPartition.get(partitionId),
                         callBack.getKeys());
        }
        trace("All verification done");
    }


    /**
     * Verifies an order of list of received entries are expected
     *
     * @param expectedEntries ordered list of expected entries
     * @param receivedEntries ordered list of received entries
     */
    private void verifyOrderedEntries(List<Pair<Key, Value>> expectedEntries,
                                      List<Pair<Key, Value>> receivedEntries) {

        final StringBuilder allEntries = new StringBuilder();
        allEntries.append("\nExpected:\n").append(dumpEntries(expectedEntries));
        allEntries.append("\nActual:\n").append(dumpEntries(receivedEntries));

        /* order by key to remove race condition */
        expectedEntries = orderEntryByKey(expectedEntries);
        receivedEntries = orderEntryByKey(receivedEntries);

        allEntries.append("\nExpected after recorder by key:\n")
                  .append(dumpEntries(expectedEntries));
        allEntries.append("\nActual after reorder by key:\n")
                  .append(dumpEntries(receivedEntries));

        /* [0] [1] must arrive on order */
        for (int i = 0; i < 2; i++) {
            final Key expKey = expectedEntries.get(i).first();
            final Value expVal = expectedEntries.get(i).second();

            final Key actKey = receivedEntries.get(i).first();
            assertEquals("All entries " + allEntries + "\n" +
                         "Mismatch keys for entry: " + i,
                         expKey, actKey);

            final Value actVal = receivedEntries.get(i).second();
            assertEquals("All entries " + allEntries + "\n" +
                         "Mismatch values for entry: " + i,
                         expVal, actVal);
        }

        /* [2] [3] can arrive out of order */
        if (!match(expectedEntries.get(2), receivedEntries.get(2)) &&
            !match(expectedEntries.get(2), receivedEntries.get(3))) {
            fail("All entries " + allEntries + "\n" +
                 "Pair " + expectedEntries.get(2).toString() + " not found");
        }

        if (!match(expectedEntries.get(3), receivedEntries.get(2)) &&
            !match(expectedEntries.get(3), receivedEntries.get(3))) {
            fail("All entries " + allEntries + "\n" +
                 "Pair " + expectedEntries.get(2).toString() + " not found");
        }
    }

    private List<Pair<Key, Value>> orderEntryByKey(List<Pair<Key, Value>>
                                                       expectedEntries) {
        Collections.sort(expectedEntries,
                         new Comparator<Pair<Key, Value>>() {
                             @Override
                             public int compare(Pair<Key, Value> o1,
                                                Pair<Key, Value> o2) {
                                 return o1.first().compareTo(o2.first());
                             }
                         });

        return expectedEntries;
    }

    private String dumpEntries(List<Pair<Key, Value>> entries) {
        final StringBuilder builder = new StringBuilder();

        for (Pair<Key, Value> pair : entries) {
            builder.append("key: ").append(pair.first()).append(", val: ")
                   .append(pair.second());
            builder.append("\n");
        }

        return builder.toString();
    }

    /* return true if act and exp are equal */
    private static boolean match(Pair<Key, Value> act, Pair<Key, Value> exp) {

        return (act.first().equals(exp.first()) &&
                act.second().equals(exp.second()));
    }

    /**
     * Waits for a test thread done
     */
    private void waitForTestThreadDone(final PartitionReaderThread t) {
        boolean success = new PollCondition(1000, 30000) {
            @Override
            protected boolean condition() {
                return t.isTestDone();
            }
        }.await();
        assert(success);
    }

    /**
     * Waits for a test thread done
     */
    private void waitForMultiTestThreadDone(
        final List<PartitionReaderThread> threads) {

        boolean success = new PollCondition(1000, 60000) {
            @Override
            protected boolean condition() {
                for (PartitionReaderThread t : threads) {
                    if (!t.isTestDone()) {
                        return false;
                    }
                }
                return true;
            }
        }.await();

        assert(success);
    }

    /**
     *  A partition reader in a child thread
     */
    private abstract class PartitionReaderThread extends Thread {
        PartitionId pid;
        RepNode repNode;
        ReplicatedEnvironment env;
        SubscriptionConfig subConf;
        PartitionReader reader;
        boolean done;
        boolean succ;

        PartitionReaderThread(PartitionId pid,
                              RepNode repNode,
                              SubscriptionConfig subConf)
        {
            this.pid = pid;
            this.repNode = repNode;
            this.subConf = subConf;
            env = this.repNode.getEnv(0);
            reader = null;
            done = false;
            succ = false;
        }

        /**
         * Is test thread done?
         * @return true if test done
         */
        boolean isTestDone() {
            return done;
        }

        /**
         * Is test a success?
         * @return true if success
         */
        boolean isTestSucc() {
            return succ;
        }

        @Override
        public abstract void run();
    }

    /**
     * A partition reader in a child thread to capture error response
     * from source node
     */
    private class PartitionReaderThread1 extends PartitionReaderThread {
        PartitionReaderThread1(PartitionId id,
                               RepNode rn,
                               SubscriptionConfig config) {
            super(id, rn, config);
        }

        @Override
        public void run() {
            /* test callback, only stream 1 key */
            SubscriptionTestCallback1 callBack =
                    new SubscriptionTestCallback1(pid, 1);
            reader = new PartitionReader(env, pid, callBack, subConf, logger);
            callBack.setPartitionReader(reader);
            /* start streaming data from the partition */
            try {
                reader.run();
                /* fail test if no exception raised */
                fail("Test failed because no expected exception");
                succ = false;
            } catch (Exception e) {
                trace("Expected exception: " + e.getMessage());
                succ = true;
            } finally {
                done = true;
            }
        }
    }

    /**
     * A partition reader in a child thread to capture busy response
     * from PMS
     */
    private class PartitionReaderThread2 extends PartitionReaderThread {

        int expectedNumKeys;

        PartitionReaderThread2(PartitionId id,
                               int expNumKeys,
                               RepNode rn,
                               SubscriptionConfig config) {
            super(id, rn, config);
            expectedNumKeys = expNumKeys;
        }

        @Override
        public void run() {
            SubscriptionTestCallback1 callBack =
                    new SubscriptionTestCallback1(pid, expectedNumKeys);
            PartitionReader preader = new PartitionReader(env, pid,
                    callBack, subConf, logger);
            callBack.setPartitionReader(preader);

            /* start streaming data from the partition */
            try {
                preader.run();

                /* test should finish successfully after retry once */
                assert (preader.getNumRetry() == 1);
                succ = true;
            } catch (InternalException e) {
                trace("Unexpected exception: " + e.getMessage());
                succ = false;
            } finally {
                done = true;
            }
        }
    }

    /**
     * A partition reader in a child thread to capture active forward
     * writes from source.
     */
    private class PartitionReaderThread3 extends PartitionReaderThread {
        SubscriptionTestCallback callBack;

        PartitionReaderThread3(PartitionReader r,
                               PartitionId id,
                               SubscriptionTestCallback cbk,
                               RepNode rn,
                               SubscriptionConfig config) {
            super(id, rn, config);
            callBack = cbk;
            reader = r;
        }

        SubscriptionTestCallback getCallBack() {
            return callBack;
        }

        @Override
        public void run() {
            /* start streaming data from the partition */
            try {
                reader.run();
                succ = true;
            } catch (InternalException e) {
                trace("Unexpected exception: " + e.getMessage());
                succ = false;
            } finally {
                done = true;
            }
        }
    }

    /**
     * Base class for a hook which waits in the doHook() method, effectively
     * implementing a breakpoint.
     *
     * @param <T>
     */
    private static abstract class BaseHook<T> implements TestHook<T> {
        private boolean waiting = false;

        /**
         * Waits for a thread to wait on the hook. Note that this may not
         * operate properly when multiple threads wait on the same hook.
         */
        void waitForHook() {
            if (waiting) {
                return;
            }

            boolean success = new PollCondition(10, 20000) {

                @Override
                protected boolean condition() {
                    return waiting;
                }
            }.await();
            assert(success);
        }

        /**
         * Releases a single thread waiting on the hook. This method checks
         * to make sure someone is waiting and throws an assert if not.
         */
        synchronized void releaseHook() {
            assert waiting;
            waiting = false;
            notify();
        }

        /*
         * Will wait until release*() is called. Also releases anyone waiting
         * in waitForHook().
         */
        synchronized void waitForRelease() {
            waiting = true;
            try {
                wait();
            } catch (InterruptedException ex) {
                throw new AssertionError("hook wait interripted");
            }
        }
    }

    /**
     * Hook for allowing the test pause the source and to single step
     * sending DB records to the target.
     */
    private static class ReadHook extends BaseHook<DatabaseEntry> {
        private DatabaseEntry lastKey = null;
        private RuntimeException exception = null;

        /* -- From TestHook -- */
        @Override
        public void doHook(DatabaseEntry key) {
            waitForRelease();
            lastKey = key;

            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public String toString() {
            return "ReadHook[" +
                   ((lastKey == null) ? "null" :
                                        new String(lastKey.getData())) + "]";
        }
    }

    /**
     * Hook to inject error responses from the source to the target.
     */
    private static class ResponseHook
        extends BaseHook<AtomicReference<Response>> {

        private Response response;

        /**
         * Causes the hook to be released and will set the specified response.
         *
         * @param resp  response to send
         */
        void sendResponse(Response resp) {
            this.response = resp;
            releaseHook();
        }

        @Override
        public void doHook(AtomicReference<Response> arg) {
            waitForRelease();
            if (response != null) {
                arg.set(response);
            }
        }
    }

    /**
     * Prepares a test env, and create, load and verify test data
     */
    private void prepareTestEnv() {
        config.startRepNodeServices();
        createTestData();
        loadTestData();
        verifyTestData();

        trace("Test environment has been created successfully.");
    }

    /**
     * Creates a configuration for partition reader
     *
     * @return a valid configuration to be used by partition reader
     */
    private SubscriptionConfig createSubscriptionConfig(RepNodeId masterId)
        throws UnknownHostException {
        final RepNode sourceMaster = config.getRN(masterId);
        ReplicatedEnvironment masterEnv = sourceMaster.getEnv(1000);


        String sourceNode = masterEnv.getNodeName();

        ReplicationGroup group = masterEnv.getGroup();
        ReplicationNode member = group.getMember(sourceNode);
        int port = member.getPort();
        String host = member.getHostName();
        String sourceHostPortPair = host + ":" + port;
        String groupName = group.getName();
        UUID uuid = group.getRepGroupImpl().getUUID();

        trace("create configuration to source: " + sourceHostPortPair +
              " from " + nodeHostPortPair +
              "(group : " + groupName + ", uuid:" + uuid + ")");

        return new SubscriptionConfig(nodeName, config.getTestPath(),
                                      nodeHostPortPair,
                                      sourceHostPortPair,
                                      groupName, uuid);
    }

    /**
     * Creates test data and store them by partition
     */
    private void createTestData() {

        if (numRecords == 0) {
            return;
        }

        testData = new HashMap<>(numRecords);
        testDataByPartition =  new HashMap<>();
        HashKeyToPartitionMap partitionMap =
            new HashKeyToPartitionMap(numPartitions);

        for (int i = 0; i < numRecords; i++) {
            String keyStr = keyPrefix + Integer.toString(i);
            String valueStr = valPrefix + Integer.toString(i*i);
            Key key = Key.createKey(keyStr);
            Value value = Value.createValue(valueStr.getBytes());
            testData.put(key, value);

            PartitionId pid = partitionMap.getPartitionId(key.toByteArray());

            List<Key> keyList;
            if (!testDataByPartition.containsKey(pid)) {
                keyList = new ArrayList<>();
            } else {
                keyList = testDataByPartition.get(pid);
            }
            keyList.add(key);
            testDataByPartition.put(pid, keyList);
        }

        trace("Test data with " + testData.size() +
              " keys have been created for " +
              testDataByPartition.size() + " partitions.");
    }

    /**
     * Loads test data into store with local commit
     */
    private void loadTestData() {
        if (numRecords == 0) {
            return;
        }

        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        for (Map.Entry<Key, Value> entry : testData.entrySet()) {
            Key k = entry.getKey();
            Value v = entry.getValue();
            kvs.put(k, v, null, Durability.COMMIT_SYNC, 0, null);
        }
    }

    /**
     * Reads test data from the store and ensure they are correct
     */
    private void verifyTestData() {
        KVStore store = KVStoreFactory.getStore(config.getKVSConfig());
        int counter = 0;

        Iterator<KeyValueVersion> iter =
                store.storeIterator(Direction.UNORDERED, 1);
        while (iter.hasNext()) {
            final KeyValueVersion kvv = iter.next();
            Key k = kvv.getKey();
            Value v = kvv.getValue();
            verifyKV(k, v);
            counter++;
        }
        assertEquals("number of records mismatch!", numRecords, counter);

        trace("All loaded test data loaded and verified.");
    }

    private void verifyKV(Key k, Value v) {
        assertTrue("Unexpected key", testData.containsKey(k));
        Value expectedValue = testData.get(k);
        assertArrayEquals("Value mismatch!",
                expectedValue.toByteArray(), v.toByteArray());
    }

    /**
     * Verifies test results by comparing expected keys and received keys
     *
     * @param allExpectedKeys list of all expected keys
     * @param allRecvKeys list of all received keys
     */
    private void verifyResult(List<Key> allExpectedKeys,
                              List<byte[]> allRecvKeys) {

        assertEquals(allExpectedKeys.size(), allRecvKeys.size());

        List<Key> allReceivedKeys = new ArrayList<>(allRecvKeys.size());


        /* every received key is expected */
        for (byte[] keyByte : allRecvKeys) {
            Key key = Key.fromByteArray(keyByte);
            assertTrue("Unexpected key", allExpectedKeys.contains(key));
            allReceivedKeys.add(key);
        }

        /* we should receive every expected key */
        for (Key key : allExpectedKeys) {
            assertTrue("Unexpected key", allReceivedKeys.contains(key));
        }
    }

    /**
     * Finds master node id hosting the partition
     *
     * @param partitionId  id of partition
     *
     * @return RepNodeId of master
     */
    private RepNodeId findMaster(PartitionId partitionId) {
        final Topology topo = config.getTopology().getCopy();
        final PartitionMap map = topo.getPartitionMap();
        final RepGroupId repGroupId = map.getRepGroupId(partitionId);
        assertNotNull(repGroupId);

        /* rgX-rn1 is always chosen as the Master in unit tests */
        RepNodeId masterId = topo.getSortedRepNodeIds(repGroupId).get(0);
        trace("Master node hosting the partition " +
              partitionId + " is " + masterId.getFullName());

        return masterId;
    }

    private abstract class SubscriptionTestCallback
        implements PartitionReaderCallBack {

        PartitionId partitionId;
        int numKeysReceived;
        int numKeysExpected;

        /* handle of parent reader, allow  shutoff reader inside callback */
        PartitionReader reader;

        SubscriptionTestCallback(PartitionId pid,
                                 int expected) {
            partitionId = pid;
            numKeysExpected = expected;
            numKeysReceived = 0;

            reader = null;
        }

        SubscriptionTestCallback(PartitionId pid) {
            this(pid, 0);
        }

        public PartitionId getPartitionId() {
            return partitionId;
        }

        void setPartitionReader(PartitionReader r) {
            assertNotNull(r);
            reader = r;
        }

        int getNumKeysReceived() {
            return numKeysReceived;
        }

        @Override
        public void processCopy(PartitionId pid,
                                long vlsn,
                                long expirationTime,
                                byte[] key,
                                byte[] value) {
            assert (partitionId == pid);
        }

        @Override
        public void processPut(PartitionId pid,
                               long vlsn,
                               long expirationTime,
                               byte[] key,
                               byte[] value,
                               long txnId) {
            assert (partitionId == pid);
        }

        @Override
        public void processDel(PartitionId pid, long vlsn, byte[] key,
                               long txnId) {
            assert (partitionId == pid);
        }

        @Override
        public void processPrepare(PartitionId pid, long txnId) {
            assert (partitionId == pid);
        }

        @Override
        public void processCommit(PartitionId pid, long txnId) {
            assert (partitionId == pid);
        }

        @Override
        public void processAbort(PartitionId pid, long txnId) {
            assert (partitionId == pid);
        }

        @Override
        public void processEOD(PartitionId pid) {
            assert (partitionId == pid);

            trace("received EOD for partition " + partitionId +
                  " after receiving " + numKeysReceived + " items.");

            assertEquals("receive unexpected number items before EOD",
                         numKeysExpected, numKeysReceived);

            reader.shutdown();
        }
    }

    /*
     * Subscription callback used in different unit tests. The subscription
     * will exit after receiving expected number of keys from the feeder.
     */
    private class SubscriptionTestCallback1 extends SubscriptionTestCallback {
        private List<byte[]> keys;

        SubscriptionTestCallback1(PartitionId pid, int expected) {
            super(pid, expected);

            keys = new ArrayList<>();
        }

        @Override
        public void processCopy(PartitionId pid, long vlsn,
                                long expirationTime,
                                byte[] key, byte[] value) {
            processPut(pid, vlsn, expirationTime, key, value, 0);
        }

        @Override
        public void processPut(PartitionId pid, long vlsn,
                               long expirationTime,
                               byte[] key, byte[] value, long txnId) {

            assert (partitionId == pid);

            /* remember key and increment count */
            keys.add(key);
            numKeysReceived++;
        }

        public List<byte[]> getKeys() {
            return keys;
        }
    }

    /*
     * Subscription used in cancelling test
     */
    private class SubscriptionTestCallback2 extends SubscriptionTestCallback {

        SubscriptionTestCallback2(PartitionId pid, int expected) {
            super(pid, expected);
        }

        @Override
        public void processCopy(PartitionId pid, long vlsn,
                                long expirationTime,
                                byte[] key, byte[] value) {
            processPut(pid, vlsn, expirationTime, key, value, 0);
        }

        @Override
        public void processPut(PartitionId pid, long vlsn,
                               long expirationTime,
                               byte[] key, byte[] value, long txnId) {

            assert (partitionId == pid);

            numKeysReceived++;

            /* cancel if enough keys */
            if (numKeysReceived == numKeysExpected) {
                trace("Received " + numKeysReceived + " keys" +
                      ", stop replication and cancel the reader");
                reader.cancel(true);
            }
        }
    }

    /*
     * Subscription callback used in testcase tracking highest VLSN
     */
    private class SubscriptionTestCallback3 extends SubscriptionTestCallback {

        private long highestVLSN;

        SubscriptionTestCallback3(PartitionId pid,
                                  int expected) {
            super(pid, expected);
            highestVLSN = FIRST_VLSN;
        }

        @Override
        public void processCopy(PartitionId pid, long vlsn,
                                long expirationTime,
                                byte[] key, byte[] value) {
            processPut(pid, vlsn, expirationTime, key, value, 0);
        }

        @Override
        public void processPut(PartitionId pid, long vlsn,
                               long expirationTime,
                               byte[] key, byte[] value, long txnId) {

            assert (partitionId == pid);

            if (highestVLSN < vlsn) {
                highestVLSN = vlsn;
            }

            numKeysReceived++;
        }

        long getHighestVLSNInTest() {
            return highestVLSN;
        }
    }

    /*
     * Subscription callback in testing active write forwarding
     */
    private class SubscriptionTestCallback4 extends SubscriptionTestCallback {
        private List<Pair<Key, Value>>  data;

        SubscriptionTestCallback4(PartitionId pid) {
            super(pid);
            data = new ArrayList<>();
        }

        @Override
        public void processCopy(PartitionId pid, long vlsn,
                                long expirationTime,
                                byte[] key, byte[] value) {
            processPut(pid, vlsn, expirationTime, key, value, 0);
        }

        @Override
        public void processPut(PartitionId pid, long vlsn,
                               long expirationTime,
                               byte[] key, byte[] value, long txnId) {

            assert (partitionId == pid);

            if (key == null) {
                return;
            }

            Key k = Key.fromByteArray(key);
            Value v = Value.fromByteArray(value);
            data.add(new Pair<>(k, v));
            numKeysReceived++;
        }

        @Override
        public void processEOD(PartitionId pid) {
            assert (partitionId == pid);

            trace("received EOD for partition " + partitionId +
                  " after receiving " + numKeysReceived + " items.");

            reader.shutdown();
        }

        List<Pair<Key, Value>> getOrderedEntries() {
            return data;
        }
    }

    /*
     *  Logs trace in unit tests to console. This is a temporary fix until we
     *  are able to use logger to log the unit tests traces in Jenkins.
     */
    private void trace(String trace) {
        if (traceOnConsole) {
            System.err.println(trace);
        } else {
            logger.info(trace);
        }
    }
}
