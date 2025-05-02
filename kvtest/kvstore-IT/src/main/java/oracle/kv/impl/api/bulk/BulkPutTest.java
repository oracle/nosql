/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.bulk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import oracle.kv.BulkWriteOptions;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.EntryStream;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValue;
import oracle.kv.KeyValueVersion;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

/**
 * Unit test for KVStore.putBulk() API:
 *
 *  public void putBulk(BulkWriteOptions bulkWriteOptions,
 *                      List<EntryStream<KeyValue>> streams)
 *      throws DurabilityException,
 *             RequestTimeoutException,
 *             FaultException;
 */
public class BulkPutTest extends TestBase {

    private static CreateStore createStore;
    private static int startPort = 12250;
    private static KVStore store;
    private long seed;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        startStore();
        store = KVStoreFactory.getStore(createKVConfig(createStore));
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        if (store != null) {
            store.close();
        }

        if (createStore != null) {
            createStore.shutdown();
        }

        LoggerUtils.closeAllHandlers();
    }

    @Override
    public void setUp()
        throws Exception {

        seed = System.currentTimeMillis();
    }

    @Override
    public void tearDown()
        throws Exception {

        deleteAllRecords();
    }

    @Test
    public void testBasic() {
        final String keyPrefix = "/bulk/basic";
        final int[] testRecCounts = {0, 1, 1000};
        final int[] testNumStreams = {1, 3, 10};
        final int[] testPerShardParallelisms = {2, 5, 10};

        final BulkWriteOptions writeOptions =
            new BulkWriteOptions(null, 0, null);

        for (int recCount : testRecCounts) {
            for (int num : testNumStreams) {
                for (int preShardParallelism: testPerShardParallelisms) {
                    final TestStream[] streams =
                        createTestStreams(num, recCount, keyPrefix);

                    writeOptions.setStreamParallelism(num);
                    writeOptions.setPerShardParallelism(preShardParallelism);

                    runBulkPut(writeOptions, streams);

                    verifyResult(streams, recCount);
                    deleteRecordsByStreams(streams);
                }
            }
        }
    }

    @Test
    public void testInvalidArgument() {
        try {
            store.put(null, new BulkWriteOptions(null, 0, null));
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }

        try {
            store.put(new ArrayList<EntryStream<KeyValue>>(), null);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }

        try {
            List<EntryStream<KeyValue>> streams =
                new ArrayList<EntryStream<KeyValue>>();
            streams.add(null);
            store.put(streams, null);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testKeyExists() {
        final String keyPrefix = "/bulk/keyexists";
        int recCount = 1000;

        final BulkWriteOptions writeOptions =
            new BulkWriteOptions(null, 0, null);
        writeOptions.setStreamParallelism(2);

        /* Put 1000 records using stream[0] */
        TestStream[] streams = createTestStreams(1, recCount, keyPrefix);
        runBulkPut(writeOptions, streams[0]);
        verifyStreamResult(streams[0], false, 0, true);
        verifyRecords(streams[0].keyPrefix, recCount);

        /*
         * Put records using stream[0] and stream[1], 1000 records per stream,
         * the entries supplied by stream[0] are existed in the kvstore.
         */
        streams = createTestStreams(2, recCount, keyPrefix);
        runBulkPut(writeOptions, streams);
        verifyStreamResult(streams[0], false, recCount, true);
        verifyRecords(streams[0].keyPrefix, recCount);
        verifyStreamResult(streams[1], false, 0, true);
        verifyRecords(streams[1].keyPrefix, recCount);

        /*
         * Put records using stream[0] and stream[1], 2000 per stream, the
         * first 1000 entries supplied by stream[0] and stream[1] are existed
         * in the kvstore.
         */
        streams = createTestStreams(2, 2 * recCount, keyPrefix);
        runBulkPut(writeOptions, streams);
        verifyStreamResult(streams[0], false, recCount, true);
        verifyRecords(streams[0].keyPrefix, 2 * recCount);
        verifyStreamResult(streams[1], false, recCount, true);
        verifyRecords(streams[1].keyPrefix, 2 * recCount);
    }

    /**
     * Test case:
     *  Load 13 key/values to store, 11 of them has the same key "/bulk/key0".
     *
     * Load them using bulkput API, then verify if the order of invoking
     * EntryStream.keyExists() for 11 records with duplicated key is same
     * with that supplied by EntryStream.
     */
    @Test
    public void testInvokeKeyExistsOrder() {
        final String keyPrefix = "/bulk";
        int nKeys = 3;
        int dupCount = 10;

        List<KeyValue> keyValues = new ArrayList<KeyValue>();
        for (int k = 0; k < nKeys; k++) {
            Key key = Key.fromString(keyPrefix + "/key" + k);
            Value val = Value.createValue(String.valueOf(k).getBytes());
            keyValues.add(new KeyValue(key, val));
        }

        Key key = keyValues.get(0).getKey();
        for (int i = 0; i < dupCount; i++) {
            Value val = Value.createValue(String.valueOf(i).getBytes());
            keyValues.add(new KeyValue(key, val));
        }

        List<KeyValue> keyExists = new ArrayList<KeyValue>();
        final TestStream stream = new TestStream(keyValues) {
            @Override
            public void keyExists(KeyValue entry) {
                assertTrue(store.get(entry.getKey()) != null);
                keyExists.add(entry);
            }
        };

        BulkWriteOptions options = new BulkWriteOptions(null, 0, null);
        options.setStreamParallelism(1);

        runBulkPut(options, new TestStream[]{stream});

        /* Verify the record in store */
        verifyRecords(keyPrefix, keyValues.subList(0, nKeys));

        /* Verify the records in keyExists list */
        assertTrue(keyExists.size() == dupCount);
        int i = nKeys;
        for (KeyValue kv : keyExists) {
            KeyValue exp = keyValues.get(i++);
            assertTrue(exp.getKey().equals(kv.getKey()));
            assertTrue(exp.getValue().equals(kv.getValue()));
        }
    }

    @Test
    public void testBulkWriteOptions() {
        final String keyPrefix = "/bulk";
        int recCount = 1000;

        BulkWriteOptions options = new BulkWriteOptions(null, 0, null);
        try {
            options.setBulkHeapPercent(101);
            fail("Expected to catch IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
        try {
            options.setBulkHeapPercent(0);
            fail("Expected to catch IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
        try {
            options.setStreamParallelism(-1);
            fail("Expected to catch IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
        try {
            options.setStreamParallelism(0);
            fail("Expected to catch IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        /* Use defaults of BulkWriteOptions */
        TestStream[] streams = createTestStreams(3, recCount, keyPrefix);
        runBulkPut(null, streams);
        verifyResult(streams, recCount);

        /* BulkHeapPercent: 50, PerShardParallelism: 10, StreamParallelism: 3 */
        deleteRecords(keyPrefix);
        options.setBulkHeapPercent(50);
        assertEquals(50, options.getBulkHeapPercent());
        options.setPerShardParallelism(10);
        assertEquals(10, options.getPerShardParallelism());
        options.setStreamParallelism(3);
        assertEquals(3, options.getStreamParallelism());
        streams = createTestStreams(3, recCount, keyPrefix);
        runBulkPut(null, streams);
        verifyResult(streams, recCount);

        /* BulkHeapPercent: 1, PerShardParallelism: 1, StreamParallelism: 1 */
        deleteRecords(keyPrefix);
        options.setBulkHeapPercent(1);
        assertEquals(1, options.getBulkHeapPercent());
        options.setPerShardParallelism(1);
        assertEquals(1, options.getPerShardParallelism());
        options.setStreamParallelism(1);
        assertEquals(1, options.getStreamParallelism());
        streams = createTestStreams(3, recCount, keyPrefix);
        runBulkPut(null, streams);
        verifyResult(streams, recCount);
    }

    @Test
    public void testCaughtException() {
        final String keyPrefix = "/bulk";
        int recCount = 1000;

        final TestStream[] streams = new TestStream[2];
        streams[0] = new TestStream(getStreamPrefix(keyPrefix, 0),
                                    recCount, recCount - 1);
        streams[1] = new TestStream(getStreamPrefix(keyPrefix, 1), recCount);

        BulkWriteOptions options = new BulkWriteOptions(null, 0, null);
        options.setStreamParallelism(1);
        runBulkPut(options, true, streams);
        verifyStreamResult(streams[0], false, -1, false);
        verifyStreamResult(streams[1], false, -1, false);
    }

    @Test
    public void testOverwrite() {
        final String keyPrefix = "/bulk/keyexists";
        int recCount = 1000;

        final BulkWriteOptions writeOptions =
            new BulkWriteOptions(null, 0, null);
        writeOptions.setStreamParallelism(1);

        /* Put 1000 records using stream[0] */
        TestStream[] streams = createTestStreams(1, recCount, keyPrefix);
        runBulkPut(writeOptions, streams[0]);
        verifyStreamResult(streams[0], false, 0, true);
        verifyRecords(streams[0].keyPrefix, recCount);

        /* Put the 1000 records again with overwrite = false */
        streams = createTestStreams(1, recCount, keyPrefix);
        runBulkPut(writeOptions, streams[0]);
        verifyStreamResult(streams[0], false, recCount/* numKeyExists*/, true);
        verifyRecords(streams[0].keyPrefix, recCount);

        /* Put the 1000 records again with overwrite = true */
        streams = createTestStreams(1, recCount, keyPrefix);
        writeOptions.setOverwrite(true);
        runBulkPut(writeOptions, streams[0]);
        verifyStreamResult(streams[0], false, 0/* numKeyExists*/, true);
        verifyRecords(streams[0].keyPrefix, recCount);
    }

    private static void startStore()
        throws Exception {

        /*
         * Make a local 3x3 store.  This exercises the metadata
         * distribution code better than a 1x1 store.
         *
         * Cannot have subclasses override parameters because this is done in
         * static context.  Need another mechanism for that if subclasses need
         * alternative configuration.
         */
        createStore = new CreateStore("kvtest-" +
                                      BulkPutTest.class.getName() + "-store",
                                      startPort,
                                      3, /* n SNs */
                                      3, /* rf */
                                      10, /* n partitions */
                                      2, /* capacity per SN */
                                      2 * CreateStore.MB_PER_SN,
                                      false, /* use threads is false */
                                      null);
        createStore.start();
    }

    private static KVStoreConfig createKVConfig(CreateStore cs) {
        KVStoreConfig config = cs.createKVConfig();
        config.setDurability
            (new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.ReplicaAckPolicy.ALL));
        return config;
    }

    private TestStream[] createTestStreams(int nStreams,
                                           final int recCount,
                                           final String keyPrefix) {

        final TestStream[] streams = new TestStream[nStreams];
        for (int i = 0; i < nStreams; i++) {
            final String prefix = getStreamPrefix(keyPrefix, i);
            final TestStream stream = new TestStream(prefix, recCount);
            streams[i] = stream;
        }
        return streams;
    }

    private String getStreamPrefix(final String keyPrefix, final int index) {
        return keyPrefix + "/stream/" + index;
    }

    private void verifyResult(final TestStream[] streams,
                              final int recCount) {

        for (int i = 0; i < streams.length; i++) {
            final TestStream stream = streams[i];
            final String prefix = stream.keyPrefix;
            verifyStreamResult(stream, false, 0, true);
            verifyRecords(prefix, recCount);
        }
    }

    private void deleteRecordsByStreams(final TestStream[] streams) {

        for (int i = 0; i < streams.length; i++) {
            final TestStream stream = streams[i];
            deleteRecords(stream.keyPrefix);
        }
    }

    private void runBulkPut(final BulkWriteOptions options,
                            TestStream... streams) {
        runBulkPut(options, false, streams);
    }

    private void runBulkPut(final BulkWriteOptions options,
                            final boolean shouldFail,
                            TestStream... streams) {

        final List<EntryStream<KeyValue>> list;
        if (streams.length == 1) {
            list = Collections.singletonList((EntryStream<KeyValue>)streams[0]);
        } else {
            list = new ArrayList<EntryStream<KeyValue>>();
            for (TestStream stream : streams) {
                list.add(stream);
            }
        }
        try {
            store.put(list, options);
            if (shouldFail) {
                fail("Expected to be failed but actually not");
            }
        } catch (RuntimeException re) {
            if (!shouldFail) {
                throw new RuntimeException(
                    "Failed to execute putBulk operation: " + re.getMessage(),
                    re);
            }
        }
    }

    private void verifyStreamResult(final TestStream stream,
                                    boolean caughtException,
                                    int numKeyExists,
                                    boolean isCompleted) {

        if (caughtException) {
            assertNotNull(stream.exception);
        } else {
            assertNull(stream.exception);
        }
        if (numKeyExists != -1) {
            assertEquals(numKeyExists, stream.getNumExists());
        }
        assertTrue(stream.isCompleted == isCompleted);
    }

    private void deleteAllRecords() {
        deleteRecords(null);
    }

    /* Delete records that keys started with the given keyPrefix */
    private void deleteRecords(final String keyPrefix) {
        final Key parentKey =
            (keyPrefix == null) ? null : Key.fromString(keyPrefix);
        final Iterator<Key> iter =
            store.storeKeysIterator(Direction.UNORDERED, 0,
                                    parentKey, null, null);
        while (iter.hasNext()) {
            final Key key = iter.next();
            final boolean deleted = store.delete(key);
            assertTrue(deleted);
        }
    }

    private long getSeed() {
        return seed;
    }

    /* Verify the records */
    private void verifyRecords(final String keyPrefix, int expCount) {
        final Key parentKey = Key.fromString(keyPrefix);
        final Iterator<KeyValueVersion> iter =
            store.storeIterator(Direction.UNORDERED, 0, parentKey, null, null);
        int count = 0;
        while (iter.hasNext()) {
            final KeyValueVersion kvv = iter.next();
            final List<String> keyPath = kvv.getKey().getFullPath();
            final int index = Integer.parseInt(keyPath.get(keyPath.size() - 1));
            final KeyValue expKV = createKeyValue(keyPrefix, index, getSeed());
            verifyKeyValue(expKV, kvv);
            count++;
        }
        assertEquals(expCount, count);
    }

    /* Verify the records */
    private void verifyRecords(final String keyPrefix,
                               List<KeyValue> expRecords) {

        final Key parentKey = Key.fromString(keyPrefix);
        final Iterator<KeyValueVersion> iter =
            store.storeIterator(Direction.UNORDERED, 0, parentKey, null, null);
        int count = 0;
        while (iter.hasNext()) {
            final KeyValueVersion kvv = iter.next();
            final List<String> keyPath = kvv.getKey().getFullPath();
            if (count >= expRecords.size()) {
                fail("Unexpected key: " + Key.createKey(keyPath).toString());
            }
            final KeyValue expKV = expRecords.get(count);
            verifyKeyValue(expKV, kvv);
            count++;
        }
        assertEquals(expRecords.size(), count);
    }

    private KeyValue createKeyValue(final String keyPrefix,
                                    final int index,
                                    final long rseed) {
        final String string = keyPrefix + "/" + index;
        final Key key = Key.fromString(string);
        final Value value;
        if (useEmptyValue(index)) {
            value = Value.EMPTY_VALUE;
        } else {
            final byte[] bytes = getRandomBytes((rseed + index), 1, 1000);
            value = Value.createValue(bytes);
        }
        return new KeyValue(key, value);
    }

    private byte[] getRandomBytes(long rseed, int minLen, int maxLen) {
        final Random rand = new Random(rseed);
        final int len = rand.nextInt(maxLen - minLen) + minLen;
        final byte[] buf = new byte[len];
        rand.nextBytes(buf);
        return buf;
    }

    private boolean useEmptyValue(int index) {
        return (index % 20 == 19);
    }

    private void verifyKeyValue(final KeyValue exp,
                                final KeyValueVersion kvv) {
        final Key expKey = exp.getKey();
        final Key key = kvv.getKey();
        assertTrue(Arrays.equals(expKey.toByteArray(), key.toByteArray()));

        final Value expValue = exp.getValue();
        final Value value = kvv.getValue();
        assertTrue("Wrong value",
                   Arrays.equals(expValue.toByteArray(), value.toByteArray()));
    }

    class TestStream implements EntryStream<KeyValue> {

        private final int recordCount;
        private final String keyPrefix;
        private final int failedIndex;
        private final Iterator<KeyValue> iterator;

        private final AtomicInteger keyExists = new AtomicInteger();
        private int i = 0;
        private RuntimeException exception = null;
        private boolean isCompleted = false;
        private boolean throwException = false;

        TestStream(String keyPrefix, int recordCount) {
            this(keyPrefix, recordCount, -1);
        }

        TestStream(String keyPrefix, int recordCount, int failedIndex) {
            super();
            this.keyPrefix = keyPrefix;
            this.recordCount = recordCount;
            this.failedIndex = failedIndex;
            iterator = null;
        }

        TestStream(List<KeyValue> kvs) {
            super();
            keyPrefix = null;
            recordCount = kvs.size();
            iterator = kvs.iterator();
            failedIndex = -1;
        }

        @Override
        public String name() {
            return "TestRowStream";
        }

        @Override
        public KeyValue getNext() {

            if (iterator != null) {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            }

            if ( i++ >= recordCount) {
                return null;
            }
            if (i == failedIndex) {
                throw new IllegalArgumentException("Dummy exception");
            }
            final KeyValue kv = createKeyValue(keyPrefix, i, getSeed());
            return kv;
        }

        @Override
        public void completed() {
            isCompleted = true;
        }

        @Override
        public void keyExists(KeyValue entry) {
            keyExists.incrementAndGet();
        }

        public int getNumExists() {
            return keyExists.get();
        }

        public void setThrowException(boolean throwException) {
            this.throwException = throwException;
        }

        @Override
        public void catchException(RuntimeException runtimeException,
                                   KeyValue entry) {
            exception = runtimeException;
            if (throwException) {
                throw runtimeException;
            }
        }
    }
}
