/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.StoreIteratorException;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.parallelscan.ParallelScanHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.FieldRange;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test of bulk get table APIs:
 *  TableIterator<Row> tableIterator(Iterator<PrimaryKey> primaryKeyIterator,
 *                                   MultiRowOptions getOptions,
 *                                   TableIteratorOptions iterateOptions)
 *      throws ConsistencyException, RequestTimeoutException, FaultException;
 *
 *  TableIterator<PrimaryKey>
 *      tableKeysIterator(Iterator<PrimaryKey> primaryKeyIterator,
 *                        MultiRowOptions getOptions,
 *                        TableIteratorOptions iterateOptions)
 *      throws ConsistencyException, RequestTimeoutException, FaultException;
 *
 *  TableIterator<Row>
 *      tableIterator(List<Iterator<PrimaryKey>> primaryKeyIterators,
 *                    MultiRowOptions getOptions,
 *                    TableIteratorOptions iterateOptions)
 *      throws ConsistencyException, RequestTimeoutException, FaultException;
 *
 *  TableIterator<PrimaryKey>
 *      tableKeysIterator(List<Iterator<PrimaryKey>> primaryKeyIterators,
 *                        MultiRowOptions getOptions,
 *                        TableIteratorOptions iterateOptions)
 *      throws ConsistencyException, RequestTimeoutException, FaultException;
 */
public class TableBulkGetTest extends TableTestBase {

    private TableImpl userTable = null;
    private TableImpl addressTable = null;
    private TableImpl infoTable = null;
    private TableImpl emailTable = null;

    final static int numUsers = 20;
    final static int numRows = 15;

    private int numParentKeyIterators = 1;

    private static Random rand = new Random();
    private List<PrimaryKey> userKeys = new ArrayList<PrimaryKey>();
    private Map<Integer, List<PrimaryKey>> addressKeys =
        new HashMap<Integer, List<PrimaryKey>>();
    private Map<Integer, List<PrimaryKey>> infoKeys =
        new HashMap<Integer, List<PrimaryKey>>();
    private Map<Integer, List<PrimaryKey>> emailKeys =
        new HashMap<Integer, List<PrimaryKey>>();

    @BeforeClass
    public static void staticSetUp() throws Exception {
        TableTestBase.staticSetUp();
    }

    @Test
    public void testRunBulkGetTests()
        throws Exception {

        createTables();
        doTestArgEdgeCases(false);
        doTestArgEdgeCases(true);

        final int[] nParentKeyIterators = {1, 3};
        for (int num : nParentKeyIterators) {
            setNumParentKeyIterators(num);
            doTestValidateKey(false);
            doTestValidateKey(true);

            doTestCheckNThreadsHeuristic(false);
            doTestCheckNThreadsHeuristic(true);

            doTestBasicBulkGet(false);
            doTestBasicBulkGet(true);

            doTestSmallBatchSize(false);
            doTestSmallBatchSize(true);

            doTestMultiRowOptions(false);
            doTestMultiRowOptions(true);

            doTestExceptionPassingAcrossResultsQueue(false);
            doTestExceptionPassingAcrossResultsQueue(true);

            doTestCancelIteration(false);
            doTestCancelIteration(true);
        }
    }

    private void setNumParentKeyIterators(int num) {
        numParentKeyIterators = num;
    }

    private int getNumParentKeyIterators() {
        return numParentKeyIterators;
    }

    private void doTestArgEdgeCases(boolean keyOnly)
        throws Exception {

        /*
         * Check that passing a null Iterator<PrimaryKey> to
         * tableIterator(Iterator<PrimaryKey>, ...) throw IAE
         */
        try {
            final Iterator<PrimaryKey> keyIterator = null;
            if (keyOnly) {
                tableImpl.tableKeysIterator(keyIterator, null, null);
            } else {
                tableImpl.tableIterator(keyIterator, null, null);
            }
            fail("expected IAE");
        } catch (IllegalArgumentException expect) {
        }

        /*
         * Check that passing a null List<Iterator<PrimaryKey>>
         * tableIterator(List<Iterator<PrimaryKey>>, ...) throw IAE
         */
        try {
            final List<Iterator<PrimaryKey>> keyIterators = null;
            if (keyOnly) {
                tableImpl.tableKeysIterator(keyIterators, null, null);
            } else {
                tableImpl.tableIterator(keyIterators, null, null);
            }
            fail("expected IAE");
        } catch (IllegalArgumentException expect) {
        }

        /*
         * Check that passing an empty List<Iterator<PrimaryKey>> to
         * tableIterator(List<Iterator<PrimaryKey>>, ...) throw IAE
         */
        try {
            final List<Iterator<PrimaryKey>> keyIterators =
                new ArrayList<Iterator<PrimaryKey>>();
            if (keyOnly) {
                tableImpl.tableKeysIterator(keyIterators, null, null);
            } else {
                tableImpl.tableIterator(keyIterators, null, null);
            }
            fail("expected IAE");
        } catch (IllegalArgumentException expect) {
        }

        /*
         * Check that passing a List<Iterator<PrimaryKey>> that contains null
         * element to tableIterator(List<Iterator<PrimaryKey>>, ...) throw IAE
         */
        try {
            final List<Iterator<PrimaryKey>> keyIterators =
                new ArrayList<Iterator<PrimaryKey>>();
            keyIterators.add(null);
            if (keyOnly) {
                tableImpl.tableKeysIterator(keyIterators, null, null);
            } else {
                tableImpl.tableIterator(keyIterators, null, null);
            }
            fail("expected IAE");
        } catch (IllegalArgumentException expect) {
        }

        /*
         * Using Direction.FORWARD or Direction.REVERSE in TableIteratorOptions
         * for tableIterator(Iterator<PrimaryKey>, ...) throw IAE
         */
        Direction[] invalidDirections = {Direction.FORWARD, Direction.REVERSE};
        for (Direction direction : invalidDirections) {
            TableIteratorOptions tio = new TableIteratorOptions(direction, null,
                                                                0, null, 0, 0);
            try {
                if (keyOnly) {
                    tableImpl.tableKeysIterator(userKeys.iterator(), null, tio);
                } else {
                    tableImpl.tableIterator(userKeys.iterator(), null, tio);
                }
                fail("expected IAE");
            } catch (IllegalArgumentException expect) {
            }

            final List<Iterator<PrimaryKey>> keyIterators =
                new ArrayList<Iterator<PrimaryKey>>();
            keyIterators.add(userKeys.iterator());
            try {
                if (keyOnly) {
                    tableImpl.tableKeysIterator(keyIterators, null, tio);
                } else {
                    tableImpl.tableIterator(keyIterators, null, tio);
                }
                fail("expected IAE");
            } catch (IllegalArgumentException expect) {
            }
        }
    }

    /**
     * Test the heuristic for maxConcurrentRequests == 0.
     */
    private void doTestCheckNThreadsHeuristic(boolean keyOnly)
        throws Exception {

        final TableIteratorOptions tio = getTableIteratorOptions(0);
        runBulkGet(userKeys, tio, userKeys, keyOnly);
    }

    /**
     * Basic bulk get test
     */
    private void doTestBasicBulkGet(boolean keyOnly)
        throws Exception {

        runBasicBulkGetFullKey(keyOnly);
        runBasicBulkGetPartialKey(keyOnly);
    }

    /**
     * Parent keys are full primary key
     */
    private void runBasicBulkGetFullKey(boolean keyOnly)
        throws Exception {

        long psRecs = runBulkGet(userKeys, userKeys, keyOnly);
        final long ssRecs = countTableRecords(userTable.createPrimaryKey(),
                                              null);
        assertEquals(ssRecs, psRecs);
        assertEquals(numUsers, psRecs);

        int[] maxConcurrentRequests = {1, 9};
        for (int num: maxConcurrentRequests) {
            final TableIteratorOptions tio = getTableIteratorOptions(num);
            psRecs = runBulkGet(userKeys, tio, userKeys, keyOnly);
            assertEquals(ssRecs, psRecs);
            assertEquals(numUsers, psRecs);
        }
    }

    /**
     * Parent keys are partial key that contains the fields defined
     * for shard key
     */
    private void runBasicBulkGetPartialKey(boolean keyOnly)
        throws Exception {

        final int from = 1, to = 4;
        final List<PrimaryKey> keys = new ArrayList<PrimaryKey>();
        final List<PrimaryKey> expKeys = new ArrayList<PrimaryKey>();
        for (int i = from; i <= to; i++) {
            final PrimaryKey key = addressTable.createPrimaryKey();
            key.put("id", i);
            keys.add(key);
            expKeys.addAll(addressKeys.get(i));
        }

        final FieldRange fr = createFieldRange(addressTable, "id",
                                               new Integer[]{from, to},
                                               new boolean[]{true, true});
        final MultiRowOptions mro = fr.createMultiRowOptions();
        final long ssRecs = countTableRecords1(addressTable.createPrimaryKey(),
                                               mro);
        long psRecs = runBulkGet(keys, expKeys, keyOnly);
        assertEquals(ssRecs, psRecs);
        assertEquals(expKeys.size(), psRecs);

        int[] maxConcurrentRequests = {1, 9};
        for (int num: maxConcurrentRequests) {
            final TableIteratorOptions tio = getTableIteratorOptions(num);
            psRecs = runBulkGet(keys, tio, expKeys, keyOnly);
            assertEquals(ssRecs, psRecs);
            assertEquals(expKeys.size(), psRecs);
        }
    }

    /**
     * Make sure that exceptions get passed across the Results Queue.
     */
    private void doTestExceptionPassingAcrossResultsQueue(boolean keyOnly)
        throws Exception {

        assertTrue(runBulkGetWithExceptions(userKeys, keyOnly));
    }

    /**
     * Test if caught NoSuchElementException when call next() after
     * canceling the iteration
     */
    private void doTestCancelIteration(boolean keyOnly)
        throws Exception {

        final AtomicLong totalIteratorRows = new AtomicLong();
        final TableIteratorOptions tio = getTableIteratorOptions(3);
        final TableIterator<?> psIter;
        if (keyOnly) {
            psIter = tableImpl.tableKeysIterator(userKeys.iterator(), null, tio);
        } else {
            psIter = tableImpl.tableIterator(userKeys.iterator(), null, tio);
        }

        while (psIter.hasNext()) {
            psIter.next();
            if (totalIteratorRows.incrementAndGet() >= 10) {
                psIter.close();
                assertFalse(psIter.hasNext());
                try {
                    psIter.next();
                    fail("expected NoSuchElementException");
                } catch (NoSuchElementException NSEE) {
                    /* Expected. */
                }
                return;
            }
        }
        fail("never tried to terminate?");
    }

    /**
     * Test when the batchSize is very small, quick reading may need wait stream
     * to put result and stall reading will block stream to put result.
     */
    private void doTestSmallBatchSize(boolean keyOnly)
        throws Exception {

        final TableIteratorOptions tio = getTableIteratorOptions(9, 1);
        runSmallBatchSizeFullKey(tio, keyOnly);
        runSmallBatchSizePartialKey(tio, keyOnly);
    }

    private void runSmallBatchSizeFullKey(TableIteratorOptions tio,
                                          boolean keyOnly)
        throws Exception {

        final long psRecs = runBulkGet(userKeys,
                                       null /* MultiRowOptions */,
                                       tio /* TableIterateOptions */,
                                       false /* stallReading */,
                                       userKeys,
                                       keyOnly);

        assertEquals(psRecs, runBulkGet(userKeys,
                                        null /* MultiRowOptions */,
                                        tio /* TableIterateOptions */,
                                        true /* stallReading */,
                                        userKeys,
                                        keyOnly));
        final long ssRecs = countTableRecords(userTable.createPrimaryKey(),
                                              null);
        assertEquals(ssRecs, psRecs);
        assertEquals(numUsers, psRecs);
    }

    private void runSmallBatchSizePartialKey(TableIteratorOptions tio,
                                            boolean keyOnly)
        throws Exception {

        final int from = 5, to = 7;
        final List<PrimaryKey> keys = new ArrayList<PrimaryKey>();
        final List<PrimaryKey> expKeys = new ArrayList<PrimaryKey>();
        for (int i = from; i <= to; i++) {
            final PrimaryKey key = addressTable.createPrimaryKey();
            key.put("id", i);
            keys.add(key);
            expKeys.addAll(addressKeys.get(i));
        }

        final FieldRange fr = createFieldRange(addressTable, "id",
                                               new Integer[]{from, to},
                                               new boolean[]{true, true});
        final MultiRowOptions mro = fr.createMultiRowOptions();
        final long ssRecs =
            countTableRecords1(addressTable.createPrimaryKey(), mro);
        final long psRecs = runBulkGet(keys,
                                       null /* MultiRowOptions */,
                                       tio /* TableIterateOptions */,
                                       false /* stallReading */,
                                       expKeys,
                                       keyOnly);
        assertEquals(psRecs, runBulkGet(keys,
                                        null /* MultiRowOptions */,
                                        tio /* TableIterateOptions */,
                                        true /* stallReading */,
                                        expKeys,
                                        keyOnly));
        assertEquals(ssRecs, psRecs);
    }

    /**
     * An exception will be thrown out if the supplied primary key is not a
     * validate primary key:
     *  - Not belong to the same target table
     *  - Missing shard key field
     *  - The field specified in field range is the not first unspecified
     *    field of the primary key.
     */
    private void doTestValidateKey(boolean keyOnly)
        throws Exception {

        List<PrimaryKey> keys = new ArrayList<PrimaryKey>();
        /* Keys belongs to different tables */
        keys.addAll(userKeys);
        keys.addAll(addressKeys.get(0));
        assertTrue(runBulkGetCaughtException(keys, null, keyOnly));

        /* Contains an empty primary key */
        keys.clear();
        keys.addAll(userKeys);
        keys.add(userTable.createPrimaryKey());
        assertTrue(runBulkGetCaughtException(keys, null, keyOnly));

        /* Contains a null primary key */
        keys.clear();
        keys.add(userKeys.get(0));
        keys.add(null);
        keys.add(userKeys.get(0));
        assertTrue(runBulkGetCaughtException(keys, null, keyOnly));

        /*
         * Check if the field specified in field range is the first unspecified
         * field of the primary key.
         */
        keys.clear();
        keys.addAll(addressKeys.get(0));
        FieldRange fr = addressTable.createFieldRange("addrId");
        fr.setStart(0, true);
        MultiRowOptions mro = fr.createMultiRowOptions();
        assertTrue(runBulkGetCaughtException(keys, mro, keyOnly));

        keys.clear();
        final PrimaryKey key = emailTable.createPrimaryKey();
        key.put("id", 1);
        keys.add(key);
        fr = emailTable.createFieldRange("emailAddress").setStart("aa", true);
        mro = fr.createMultiRowOptions();
        assertTrue(runBulkGetCaughtException(keys, mro, keyOnly));

        /* Invalid ancestor or descendant table */
        keys.clear();
        keys.addAll(addressKeys.get(0));
        mro = new MultiRowOptions(null, Arrays.asList((Table)emailTable), null);
        assertTrue(runBulkGetCaughtException(keys, mro, keyOnly));

        mro = new MultiRowOptions(null, null, Arrays.asList((Table)userTable));
        assertTrue(runBulkGetCaughtException(keys, mro, keyOnly));
    }

    /**
     * Test using MultiRowOptions parameter
     */
    private void doTestMultiRowOptions(boolean keyOnly)
        throws Exception {

        final int numIds = 10;
        final int maxConcurrentRequests = 9;
        final Set<PrimaryKey> keys = genIdPrimaryKeys(addressTable, numIds);
        final int[] batchSizes = {0, 30};
        final boolean[][] bounds = new boolean[][] {
            {true, true},
            {false, true},
            {true, false},
            {false, false},
        };

        final Integer[][] addressIdRanges = new Integer[][] {
            {0, numRows - 1},
            {2, numRows/2},
            {numRows - 1, null},
            {null, 1},
        };

        /* Get User.address records with ranges on addrId */
        for (int batchSize: batchSizes) {
            for (Integer[] range: addressIdRanges) {
                for (boolean[] bound : bounds) {
                    final FieldRange fr = createFieldRange(addressTable,
                                                           "addrId",
                                                           range, bound);
                    final MultiRowOptions mro = fr.createMultiRowOptions();
                    List<PrimaryKey> expKeys =
                        getExpectedKeys(addressTable, keys, mro);
                    final TableIteratorOptions tio =
                        getTableIteratorOptions(maxConcurrentRequests,
                                                batchSize);
                    long ret = runBulkGet(keys, mro, tio, false, expKeys,
                                          keyOnly);
                    assertEquals(expKeys.size(), ret);

                    /* Add child table and ancestor table */
                    List<Table> tables = Arrays.asList((Table)emailTable);
                    mro.setIncludedChildTables(tables);
                    tables = Arrays.asList((Table)userTable);
                    mro.setIncludedParentTables(tables);
                    expKeys = getExpectedKeys(addressTable, keys, mro);
                    ret = runBulkGet(keys, mro, tio, false, expKeys, keyOnly);
                    assertEquals(expKeys.size(), ret);
                }
            }
        }

        /* Multiple ancestor tables */
        final List<Table> ancestors =
            Arrays.asList((Table)userTable, (Table)addressTable);
        MultiRowOptions mro = new MultiRowOptions(null, ancestors, null);
        List<PrimaryKey> expectedKeys = new ArrayList<PrimaryKey>();
        List<PrimaryKey> allEmailKeys = getAllEmailKeys();
        expectedKeys.addAll(userKeys);
        expectedKeys.addAll(getAllAddressKeys());
        expectedKeys.addAll(allEmailKeys);
        for (int size: batchSizes) {
            final TableIteratorOptions tio =
                getTableIteratorOptions(maxConcurrentRequests, size);
            long ret = runBulkGet(allEmailKeys, mro, tio, false,
                                  expectedKeys, keyOnly);
            assertEquals((3 * allEmailKeys.size()), ret);
        }

        /* Multiple child tables */
        final List<Table> children =
            Arrays.asList((Table)addressTable, (Table)infoTable);
        mro = new MultiRowOptions(null, null, children);
        expectedKeys.clear();
        expectedKeys.addAll(userKeys);
        expectedKeys.addAll(getAllAddressKeys());
        expectedKeys.addAll(getAllInfoKeys());
        for (int size: batchSizes) {
            final TableIteratorOptions tio =
                getTableIteratorOptions(maxConcurrentRequests, size);
            long ret = runBulkGet(userKeys, mro, tio, false,
                                  expectedKeys, keyOnly);
            assertEquals(expectedKeys.size(), ret);
        }
    }

    private Set<Integer> genIds(int num) {
        Set<Integer> ids = new HashSet<Integer>();
        for (int i = 0; i <num; i++) {
            ids.add(rand.nextInt(numUsers));
        }
        return ids;
    }

    private Set<PrimaryKey> genIdPrimaryKeys(Table table, int num) {
        Set<Integer> ids = genIds(num);
        Set<PrimaryKey> keys = new HashSet<PrimaryKey>();
        for (int id : ids) {
            final PrimaryKey key = table.createPrimaryKey();
            key.put("id", id);
            keys.add(key);
        }
        return keys;
    }

    private FieldRange createFieldRange(Table table,
                                        String field,
                                        Integer[] range,
                                        boolean[] bound) {
        FieldRange fieldRange = table.createFieldRange(field);
        if (range[0] != null) {
            fieldRange.setStart(range[0], bound[0]);
        }
        if (range[1] != null) {
            fieldRange.setEnd(range[1], bound[1]);
        }
        return fieldRange;
    }

    private List<PrimaryKey> getExpectedKeys(Table table,
                                             Set<PrimaryKey> parentKeys,
                                             MultiRowOptions mro) {

        List<PrimaryKey> retKeys = new ArrayList<PrimaryKey>();
        for (PrimaryKey key: parentKeys) {
            retKeys.addAll(getExpectedKeys(table, key, mro));
        }
        return retKeys;
    }

    private List<PrimaryKey> getExpectedKeys(Table table,
                                             PrimaryKey parentKey,
                                             MultiRowOptions mro) {

        final PrimaryKey pKey =
            (parentKey == null) ? table.createPrimaryKey() : parentKey;

        final TableIterator<PrimaryKey> iter =
            tableImpl.tableKeysIterator(pKey, mro, null);
        List<PrimaryKey> keys = new ArrayList<PrimaryKey>();
        while (iter.hasNext()) {
            final Row row = iter.next();
            keys.add(row.createPrimaryKey());
        }
        iter.close();
        return keys;
    }

    private long runBulkGet(Collection<PrimaryKey> parentKeys,
                            Collection<PrimaryKey> expectedKeys,
                            boolean keyOnly)
        throws Exception {

        return runBulkGet(parentKeys, null, null, false, expectedKeys, keyOnly);
    }

    private long runBulkGet(Collection<PrimaryKey> parentKeys,
                            TableIteratorOptions iterateOptions,
                            Collection<PrimaryKey> expectedKeys,
                            boolean keyOnly)
        throws Exception {

        return runBulkGet(parentKeys, null, iterateOptions, false,
                          expectedKeys, keyOnly);
    }

    /* Run a bulk get and return the row count. */
    private long runBulkGet(Collection<PrimaryKey> parentKeys,
                            MultiRowOptions getOptions,
                            TableIteratorOptions iterateOptions,
                            boolean stallReading,
                            Collection<PrimaryKey> expectedKeys,
                            boolean keyOnly)
        throws Exception {

        /*
         * If we're running batchSize == 1
         * indicates that we should stall reading the results a little so that
         * streams will be blocked putting results.
         */

        final AtomicLong totalStoreIteratorRecords = new AtomicLong();

        final TableIterator<?> iter = createTableIterator(parentKeys, keyOnly,
                                                          getOptions,
                                                          iterateOptions);
        while (iter.hasNext()) {
            if (stallReading) {
                try {
                    Thread.sleep(10);
                } catch (Exception E) {
                }
            }
            final PrimaryKey key;
            if (keyOnly) {
                key = (PrimaryKey)iter.next();
                final Row expectedRow = getExpectedRowById(key);
                assertTrue(expectedRow.createPrimaryKey().equals(key));
            } else {
                final Row row = (Row)iter.next();
                key = row.createPrimaryKey();
                final Row expectedRow = getExpectedRowById(key);
                assertTrue(expectedRow.equals(row));
            }
            if (expectedKeys != null) {
                assertTrue(expectedKeys.contains(key));
            }
            totalStoreIteratorRecords.incrementAndGet();
        }

        /* wait a while to update iterator metrics. */
        final List<DetailedMetrics> shardMetrics = iter.getShardMetrics();
        final long recordCount = totalStoreIteratorRecords.get();
        boolean result =  new PollCondition(10, 500) {
            @Override
            protected boolean condition() {
                return recordCount == tallyDetailedMetrics(shardMetrics);
            }
        }.await();

        assertTrue(result);

        iter.close();
        return totalStoreIteratorRecords.get();
    }

    /* Runs a bulk get with an exception injected. */
    private boolean runBulkGetWithExceptions(Collection<PrimaryKey> parentKeys,
                                             boolean keyOnly)
        throws Exception {

        final AtomicLong totalIteratorRows = new AtomicLong();
        final AtomicInteger hookCalls = new AtomicInteger();

        final StringBuilder sb = new StringBuilder();
        ((KVStoreImpl) store).setParallelScanHook
            (new ParallelScanHook() {
                    @Override
                    public boolean callback(Thread t,
                                            HookType hookType,
                                            String info) {
                        if (hookType == HookType.BEFORE_EXECUTE_REQUEST) {
                            /* Number of partitions = 10 */
                            if (hookCalls.incrementAndGet() >= 5) {
                                sb.append("Dummy exception was thrown out!");
                                throw new FaultException("test", false);
                            }
                        }
                        return true;
                    }
                });

        final TableIteratorOptions tio = getTableIteratorOptions(3);
        final TableIterator<?> iter = createTableIterator(parentKeys, keyOnly,
                                                          null, tio);

        List<Row> results = new ArrayList<>();
        boolean ret = false;
        try {
            while (iter.hasNext()) {
                results.add((Row)iter.next());
                totalIteratorRows.incrementAndGet();
            }
        } catch (StoreIteratorException sie) {
            ret = true;
        } finally {
            if (iter != null) {
                iter.close();
            }
            ((KVStoreImpl) store).setParallelScanHook(null);
        }

        if (!ret) {
            KVStoreImpl storeImpl = ((KVStoreImpl)store);
            Topology topo = storeImpl.getTopology();
            KeySerializer kser = storeImpl.getKeySerializer();

            sb.append("\nUser table id: ").append(userTable.getId());
            sb.append("\nNumber of parentKeys: ").append(parentKeys.size());
            sb.append("\nTotal rows returned: ").append(totalIteratorRows);
            sb.append("\nPrimary keys:\n");
            Iterator<Row> it = results.iterator();
            while(it.hasNext()) {
                Row row = it.next();
                Key key = ((RowImpl)row).getPrimaryKey(true);
                byte[] bytes = kser.toByteArray(key);
                PartitionId pid = topo.getPartitionId(bytes);
                sb.append(row.toJsonString(false)).append(": ")
                  .append(pid).append("\n");
            }
            sb.append("\n");
            System.out.println(sb.toString());
        }
        return ret;
    }

    /* Runs a bulk get, expect to catch an exception. */
    private boolean runBulkGetCaughtException(Collection<PrimaryKey> parentKeys,
                                              MultiRowOptions getOptions,
                                              boolean keyOnly)
        throws Exception {

        final TableIteratorOptions tio = getTableIteratorOptions(3);
        final TableIterator<?> iter = createTableIterator(parentKeys, keyOnly,
                                                          getOptions, tio);
        boolean ret = false;
        try {
            while (iter.hasNext()) {
                iter.next();
            }
        } catch (StoreIteratorException sie) {
            ret = true;
        } finally {
            iter.close();
        }
        return ret;
    }

    private TableIterator<?>
        createTableIterator(Collection<PrimaryKey> parentKeys,
                            boolean keyOnly,
                            MultiRowOptions mro,
                            TableIteratorOptions tio) {

        final TableIterator<?> iter;
        final int nParentKeyIterators = getNumParentKeyIterators();
        if (nParentKeyIterators == 1) {
            final Iterator<PrimaryKey> keyIterator = parentKeys.iterator();
            if (keyOnly) {
                iter = tableImpl.tableKeysIterator(keyIterator, mro, tio);
            } else {
                iter = tableImpl.tableIterator(keyIterator, mro, tio);
            }
        } else {
            assert nParentKeyIterators > 1;
            final List<Iterator<PrimaryKey>> keyIterators =
                createParentKeyIterators(parentKeys, nParentKeyIterators);
            if (keyOnly) {
                iter = tableImpl.tableKeysIterator(keyIterators, mro, tio);
            } else {
                iter = tableImpl.tableIterator(keyIterators, mro, tio);
            }
        }
        return iter;
    }

    private List<Iterator<PrimaryKey>>
        createParentKeyIterators(Collection<PrimaryKey> parentKeys, int num) {

        final int nKeys = parentKeys.size();
        final int nKeyIterators = (nKeys < num) ? nKeys : num;
        final int perIteratorKeys = (nKeys + nKeyIterators - 1)/nKeyIterators;
        final List<PrimaryKey> allKeys =
            Arrays.asList(parentKeys.toArray(new PrimaryKey[nKeys]));
        final List<Iterator<PrimaryKey>> keyIterators =
            new ArrayList<Iterator<PrimaryKey>>(nKeyIterators);
        for (int i = 0; i < nKeyIterators; i++) {
            final int from = i * perIteratorKeys;
            final int to = Math.min((i + 1) * perIteratorKeys, nKeys);
            final List<PrimaryKey> subKeys = allKeys.subList(from, to);
            keyIterators.add(subKeys.iterator());
        }
        return keyIterators;
    }

    private long tallyDetailedMetrics(List<DetailedMetrics> metrics) {
        long totalRecords = 0;
        for (DetailedMetrics dm : metrics) {
            if (dm != null) {
                totalRecords += dm.getScanRecordCount();
            }
        }
        return totalRecords;
    }

    /**
     * Create tables:
     *  User
     *      User.Address
     *          User.Address.Email
     *      User.Info
     *
     *  Tables definition:
     *      User(id integer, firstName string,
     *           lastName String, age Integer,
     *           code fixed_binary(10),
     *           primary key(id))
     *
     *      User.Address(addrId integer, type enum,
     *                   street string, city string,
     *                   state string, zip integer,
     *                   primary key(addrId))
     *
     *      User.Address.Email(emailAddress string,
     *                         emailType enum,
     *                         provider string,
     *                         primary key(emailAddress))
     *
     *      User.Info(s1 string, s2 string,
     *                myId integer,
     *                primary key(myId))
     */
    private void createTables() throws Exception {

        userTable = TableBuilder.createTableBuilder("User",
                                                    "Table of Users",
                                                    null)
            .addInteger("id")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addFixedBinary("code", 10)
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        addTable(userTable);

        /* Add a child table */
        addressTable = TableBuilder.createTableBuilder("Address",
                                   "Table of addresses for users",
                                   userTable)
            .addEnum("type", new String[]{"home", "work", "other"},
                     null, null, null)
            .addString("street")
            .addString("city")
            .addString("state")
            /* make zip nullable */
            .addInteger("zip", null, true, null)
            .addInteger("addrId")
            .primaryKey("addrId")
            .buildTable();
        addTable(addressTable, true, true);

        /*
         * Add a sibling table to Address called Info.  It has no
         * meaningful fields, it's just a presence.
         */
        infoTable = TableBuilder.createTableBuilder("Info",
                                                    "Table of info",
                                                    userTable)
            .addString("s1")
            .addString("s2")
            .addInteger("myid")
            .primaryKey("myid")
            .buildTable();
        addTable(infoTable, true, true);

        /*
         * Add another level of nesting -- a child of addressTable.
         * This is entirely fabricated and not intended to make sense.
         */
        emailTable =
            TableBuilder.createTableBuilder("Email",
                                            "email addresses",
                                            addressTable)
            .addString("emailAddress")
            .addEnum("emailType",
                     new String[]{"home", "work", "other"}, null, null, null)
            .addString("provider")
            .primaryKey("emailAddress")
            .buildTable();
        addTable(emailTable, true, true);

        /*
         * re-retrieve major tables to make sure they are current and
         * add indexes to userTable and to addressTable.
         */
        userTable = getTable("User");
        addressTable = getTable("User.Address");
        emailTable = getTable("User.Address.Email");
        infoTable = getTable("User.Info");

        addUserRows(numUsers);
        assertEquals(numUsers, userKeys.size());
        addAddressRows(numRows);
        addEmailRows(numRows);
        addInfoRows(numRows);
    }

    private void addUserRows(int num) {
        for (int i = 0; i < num; i++) {
            Row row = getUserRow(i);
            Version v = tableImpl.put(row, null, null);
            assertTrue(v != null);
            userKeys.add(row.createPrimaryKey());
        }
    }

    private Row getUserRow(int id) {
        Row row = userTable.createRow();
        row.put("id", id);
        row.put("firstName", "first_" + id);
        row.put("lastName", "last_" + id);
        row.put("age", id + 10);
        row.putFixed("code", new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        return row;
    }

    private void addAddressRows(int num) {
        TableIterator<Row> iter =
            tableImpl.tableIterator(userTable.createPrimaryKey(), null,
                                    null);
        while (iter.hasNext()) {
            Row userRow = iter.next();
            for (int i = 0; i < num; i++) {
                int id = userRow.get("id").asInteger().get();
                Row row = getAddressRow(id, i);
                Version v = tableImpl.put(row, null, null);
                assertTrue(v != null);
                List<PrimaryKey> keys = addressKeys.get(id);
                if (keys == null) {
                    keys = new ArrayList<PrimaryKey>();
                    addressKeys.put(id, keys);
                }
                keys.add(row.createPrimaryKey());
            }
        }
        iter.close();
    }

    private Row getAddressRow(int id, int i) {
        Row row = addressTable.createRow();
        row.put("id", id);
        row.putEnum("type", "home");
        row.put("street", "happy lane");
        /*
         * Make addrId of 0 be different
         */
        if (i == 0) {
            row.put("city", "Whoville");
        } else {
            row.put("city", "Smallville");
        }
        row.put("zip", i);
        row.put("state", "MT");
        row.put("addrId", i);
        return row;
    }

    private void addInfoRows(int num) {
        TableIterator<Row> iter =
            tableImpl.tableIterator(userTable.createPrimaryKey(), null, null);

        while (iter.hasNext()) {
            Row userRow = iter.next();
            for (int i = 0; i < num; i++) {
                final int id = userRow.get("id").asInteger().get();
                Row row = getInfoRow(id, i);
                Version v = tableImpl.put(row, null, null);
                assertTrue(v != null);
                List<PrimaryKey> keys = infoKeys.get(id);
                if (keys == null) {
                    keys = new ArrayList<PrimaryKey>();
                    infoKeys.put(id, keys);
                }
                keys.add(row.createPrimaryKey());
            }
        }
        iter.close();
    }

    private Row getInfoRow(int id, int i) {
        Row row = infoTable.createRow();
        row.put("id", id);
        row.put("myid", i);
        row.put("s1", "s1");
        row.put("s2", "s2");
        return row;
    }

    private void addEmailRows(int num) {
        TableIterator<Row> iter =
            tableImpl.tableIterator(addressTable.createPrimaryKey(), null, null);
        while (iter.hasNext()) {
            Row addrRow = iter.next();
            for (int i = 0; i < num; i++) {
                final int id = addrRow.get("id").asInteger().get();
                final int addrId = addrRow.get("addrId").asInteger().get();
                Row row = getEmailRow(id, addrId, i);
                Version v = tableImpl.put(row, null, null);
                assertTrue(v != null);
                List<PrimaryKey> keys = emailKeys.get(id);
                if (keys == null) {
                    keys = new ArrayList<PrimaryKey>();
                    emailKeys.put(id, keys);
                }
                keys.add(row.createPrimaryKey());
            }
        }
        iter.close();
    }

    private Row getEmailRow(int id, int addrId, int i) {
        String email = "joe" + i + "@myprovider.com";
        return getEmailRow(id, addrId, email);
    }

    private Row getEmailRow(int id, int addrId, String email) {
        Row row = emailTable.createRow();
        row.put("id", id);
        row.put("addrId", addrId);
        row.putEnum("emailType", "work");
        row.put("provider", "myprovider");
        row.put("emailAddress", email);
        return row;
    }

    private Row getExpectedRowById(PrimaryKey key) {
        Table table = key.getTable();

        assertTrue(key.get("id") != null);
        final int id = key.get("id").asInteger().get();
        if (table.getFullName().equals(userTable.getFullName())) {
            return getUserRow(id);
        } else if (table.getFullName().equals(addressTable.getFullName())) {
            final int addrId = key.get("addrId").asInteger().get();
            return getAddressRow(id, addrId);
        } else if (table.getFullName().equals(infoTable.getFullName())) {
            final int myId = key.get("myid").asInteger().get();
            return getInfoRow(id, myId);
        }
        assertTrue(table.getFullName().equals(emailTable.getFullName()));
        final int addrId = key.get("addrId").asInteger().get();
        final String email = key.get("emailAddress").asString().get();
        return getEmailRow(id, addrId, email);
    }

    private TableIteratorOptions
        getTableIteratorOptions(int maxConcurrentRequests) {

        return new TableIteratorOptions(Direction.UNORDERED, null, 0, null,
            maxConcurrentRequests, 0);
    }

    private TableIteratorOptions
        getTableIteratorOptions(int maxConcurrentRequests, int batchSize) {

        return new TableIteratorOptions(Direction.UNORDERED, null, 0, null,
            maxConcurrentRequests, batchSize);
    }

    private List<PrimaryKey> getAllAddressKeys() {
        return getAllKeys(addressKeys);
    }

    private List<PrimaryKey> getAllInfoKeys() {
        return getAllKeys(infoKeys);
    }

    private List<PrimaryKey> getAllEmailKeys() {
        return getAllKeys(emailKeys);
    }

    private List<PrimaryKey> getAllKeys(Map<Integer, List<PrimaryKey>> keyMap) {
        final List<PrimaryKey> allKeys = new ArrayList<PrimaryKey>();
        for (List<PrimaryKey> keys: keyMap.values()) {
            allKeys.addAll(keys);
        }
        return allKeys;
    }
}
