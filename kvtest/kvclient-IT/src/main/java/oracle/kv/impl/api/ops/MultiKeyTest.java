/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.OperationResult;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.systables.IndexStatsLeaseDesc;
import oracle.kv.impl.systables.PartitionStatsLeaseDesc;
import oracle.kv.impl.systables.TableStatsIndexDesc;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.table.TableAPI;

import org.junit.Assume;
import org.junit.Test;

/**
 * Tests multi-key API operations.
 */
public class MultiKeyTest extends ClientTestBase {

    /* A store handle with access to the internal/hidden keyspace. */
    private KVStore internalStore;

    @Override
    public void setUp()
        throws Exception {
        /*
         *  This test does not create any tables, so no need to run
         *  multi-region table mode.
         */
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        super.setUp();
        internalStore = KVStoreImpl.makeInternalHandle(store);
    }

    /**
     * A single test method is used to avoid the overhead of service setup and
     * teardown for every test.
     */
    @Test
    public void testAll() throws Throwable {
        basicTests();
        executeTests();
        pathTests();
    }

    /**
     * Test the big key whose length exceeds the limitation.
     */
    @Test
    public void testBigKey() throws Throwable {
        final OperationFactory factory = store.getOperationFactory();
        final List<Operation> ops = new ArrayList<Operation>();

        ArrayList<String> majorComponents = new ArrayList<String>();
        ArrayList<String> minorComponents = new ArrayList<String>();
        for (int i=0; i<Short.MAX_VALUE; i++) {
            majorComponents.add(String.valueOf(i));
            minorComponents.add(String.valueOf(i));
        }

        Exception e = null;
        try {
            ops.add(factory.createPutIfAbsent
                    (Key.createKey(majorComponents, minorComponents), strVal("a")));
            store.execute(ops);
        } catch (FaultException fe) {
            e = fe;
        }
        assertNotNull(e);
        assertTrue(e.getMessage().contains("exceeds maximum key"));

        ops.clear();

        e = null;
        try {
            store.multiGet(Key.createKey(majorComponents,
                    minorComponents), null, null);
        } catch (FaultException fe) {
            e = fe;
        }
        assertNotNull(e);
        assertTrue(e.getMessage().contains("exceeds maximum key"));

        e = null;
        try {
            store.multiDelete(Key.createKey(majorComponents,
                    minorComponents), null, null);
        } catch (FaultException fe) {
            e = fe;
        }
        assertNotNull(e);
        assertTrue(e.getMessage().contains("exceeds maximum key"));
    }

    /**
     * These are very basic tests. For more exhaustive testing we rely on
     * executeTests and pathTests.
     */
    private void basicTests()
        throws Throwable {

        final OperationFactory factory = store.getOperationFactory();
        final List<Operation> ops = new ArrayList<Operation>();
        List<OperationResult> results;

        ops.add(factory.createPutIfAbsent
                (Key.createKey("a", "a"), strVal("a")));
        ops.add(factory.createPutIfAbsent
                (Key.createKey("a", "b"), strVal("b")));

        results = store.execute(ops);
        assertNotNull(results);
        for (OperationResult result : results) {
            assertTrue(result.getSuccess());
        }

        ops.clear();

        ops.add(factory.createPutIfAbsent
                (Key.createKey("b", "a"), strVal("c")));
        ops.add(factory.createPutIfAbsent
                (Key.createKey("b", "b"), strVal("d")));

        results = store.execute(ops);
        assertNotNull(results);
        for (OperationResult result : results) {
            assertTrue(result.getSuccess());
        }

        SortedMap<Key, ValueVersion> map;

        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(2, map.size());
        checkVal("a", map.get(Key.createKey("a", "a")));
        checkVal("b", map.get(Key.createKey("a", "b")));

        Iterator<KeyValueVersion> iter = store.multiGetIterator
            (Direction.FORWARD, 1, Key.createKey("a"), null, null);
        assertTrue(iter.hasNext());
        checkVal(map.get(Key.createKey("a", "a")), iter.next());
        assertTrue(iter.hasNext());
        checkVal(map.get(Key.createKey("a", "b")), iter.next());
        assertFalse(iter.hasNext());

        try {
            iter.next();
            fail();
        } catch (NoSuchElementException expected) {
        }
        try {
            iter.remove();
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        map = store.multiGet(Key.createKey("b"), null, null);
        assertEquals(2, map.size());
        checkVal("c", map.get(Key.createKey("b", "a")));
        checkVal("d", map.get(Key.createKey("b", "b")));

        iter = store.multiGetIterator
            (Direction.FORWARD, 1, Key.createKey("b"), null, null);
        assertTrue(iter.hasNext());
        checkVal(map.get(Key.createKey("b", "a")), iter.next());
        assertTrue(iter.hasNext());
        checkVal(map.get(Key.createKey("b", "b")), iter.next());
        assertFalse(iter.hasNext());

        KeyValueVersion kvv;
        map = store.multiGet(Key.createKey("a"), null, null);
        map.putAll(store.multiGet(Key.createKey("b"), null, null));
        iter = store.storeIterator(Direction.UNORDERED, 1);
        for (int i = 0; i < 4; i += 1) {
            assertTrue(iter.hasNext());
            kvv = iter.next();
            assertTrue(map.containsKey(kvv.getKey()));
            checkVal(map.get(kvv.getKey()), kvv);
            map.remove(kvv.getKey());
        }
        assertFalse(iter.hasNext());

        int nDeleted;

        nDeleted = store.multiDelete(Key.createKey("a"), null, null);
        assertEquals(2, nDeleted);
        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(0, map.size());
        map = store.multiGet(Key.createKey("b"), null, null);
        assertEquals(2, map.size());

        nDeleted = store.multiDelete(Key.createKey("b"), null, null);
        assertEquals(2, nDeleted);
        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(0, map.size());
        map = store.multiGet(Key.createKey("b"), null, null);
        assertEquals(0, map.size());

        nDeleted = store.multiDelete(Key.createKey("a"), null, null);
        assertEquals(0, nDeleted);
        nDeleted = store.multiDelete(Key.createKey("b"), null, null);
        assertEquals(0, nDeleted);
    }

    /**
     * Tests KVStore.execute in success and failure modes.
     */
    private void executeTests()
        throws Throwable {

        final OperationFactory factory = store.getOperationFactory();
        final List<Operation> ops = new ArrayList<Operation>();
        List<OperationResult> results;
        SortedMap<Key, ValueVersion> map;

        /* Different major paths in one operation are not allowed. */
        ops.clear();
        ops.add(factory.createPut
                (Key.createKey("a", "a"), strVal("a")));
        assertSame(Operation.Type.PUT,
                   ops.get(ops.size() - 1).getType());
        ops.add(factory.createPut
                (Key.createKey("b", "b"), strVal("d")));
        assertSame(Operation.Type.PUT,
                   ops.get(ops.size() - 1).getType());
        try {
            store.execute(ops);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains
                ("Two operations have different major paths"));
        }
        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(0, map.size());
        map = store.multiGet(Key.createKey("b"), null, null);
        assertEquals(0, map.size());

        /* Same Key for two operations is not allowed. */
        ops.clear();
        ops.add(factory.createPut
                (Key.createKey("b", "b"), strVal("a")));
        assertSame(Operation.Type.PUT,
                   ops.get(ops.size() - 1).getType());
        ops.add(factory.createPut
                (Key.createKey("b", "b"), strVal("d")));
        assertSame(Operation.Type.PUT,
                   ops.get(ops.size() - 1).getType());
        try {
            store.execute(ops);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains
                ("More than one operation has the same Key"));
        }
        map = store.multiGet(Key.createKey("b"), null, null);
        assertEquals(0, map.size());

        /* Insert a and b. */
        ops.clear();
        ops.add(factory.createPutIfAbsent
                (Key.createKey("a", "a"), strVal("a"),
                 ReturnValueVersion.Choice.ALL, true /*abortOnFailure*/));
        assertSame(Operation.Type.PUT_IF_ABSENT,
                   ops.get(ops.size() - 1).getType());
        ops.add(factory.createPutIfAbsent
                (Key.createKey("a", "b"), strVal("b"),
                 ReturnValueVersion.Choice.ALL, true /*abortOnFailure*/));
        assertSame(Operation.Type.PUT_IF_ABSENT,
                   ops.get(ops.size() - 1).getType());
        results = store.execute(ops);
        assertNotNull(results);
        for (OperationResult result : results) {
            assertTrue(result.getSuccess());
            assertNull(result.getPreviousValue());
            assertNull(result.getPreviousVersion());
            assertNotNull(result.getNewVersion());
        }

        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(2, map.size());
        checkVal("a", map.get(Key.createKey("a", "a")));
        checkVal("b", map.get(Key.createKey("a", "b")));

        /* Update a and attempt to insert b, which will abort and fail. */
        ops.clear();
        ops.add(factory.createPutIfPresent
                (Key.createKey("a", "a"), strVal("c"),
                 ReturnValueVersion.Choice.ALL, true /*abortOnFailure*/));
        assertSame(Operation.Type.PUT_IF_PRESENT,
                   ops.get(ops.size() - 1).getType());
        ops.add(factory.createPutIfAbsent
                (Key.createKey("a", "b"), strVal("d"),
                 ReturnValueVersion.Choice.ALL, true /*abortOnFailure*/));
        assertSame(Operation.Type.PUT_IF_ABSENT,
                   ops.get(ops.size() - 1).getType());
        try {
            store.execute(ops);
            fail();
        } catch (OperationExecutionException e) {
            assertEquals(1, e.getFailedOperationIndex());
            final Operation op = e.getFailedOperation();
            final Key opKey = Key.createKey("a", "b");
            assertSame(Operation.Type.PUT_IF_ABSENT, op.getType());
            assertEquals(opKey, op.getKey());
            final OperationResult result = e.getFailedOperationResult();
            assertFalse(result.getSuccess());
            checkVal(map.get(opKey), result);
            assertNull(result.getNewVersion());
        }

        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(2, map.size());
        checkVal("a", map.get(Key.createKey("a", "a")));
        checkVal("b", map.get(Key.createKey("a", "b")));

        /*
         * Same as above but pass abortOnFailure=false for insert.  Reverse key
         * order to test that operation sorting by key returns results in the
         * correct list slots.
         */
        OperationResult result;
        ops.clear();
        ops.add(factory.createPutIfAbsent
                (Key.createKey("a", "b"), strVal("d"),
                 ReturnValueVersion.Choice.ALL, false /*abortOnFailure*/));
        assertSame(Operation.Type.PUT_IF_ABSENT,
                   ops.get(ops.size() - 1).getType());
        ops.add(factory.createPutIfPresent
                (Key.createKey("a", "a"), strVal("c"),
                 ReturnValueVersion.Choice.ALL, true /*abortOnFailure*/));
        assertSame(Operation.Type.PUT_IF_PRESENT,
                   ops.get(ops.size() - 1).getType());
        results = store.execute(ops);
        assertNotNull(results);
        assertEquals(2, results.size());
        result = results.get(0);
        assertFalse(result.getSuccess());
        assertNull(result.getNewVersion());
        checkVal(map.get(Key.createKey("a", "b")), result);
        result = results.get(1);
        assertTrue(result.getSuccess());
        checkVal(map.get(Key.createKey("a", "a")), result);
        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(2, map.size());
        checkVal("c", map.get(Key.createKey("a", "a")));
        checkVal("b", map.get(Key.createKey("a", "b")));

        /*
         * put, putIfAbsent and putIfPresent are tested above.  Do a basic
         * success/failure test of putIfVersion and delete methods below.  We
         * rely to some degree on code sharing between execute and single key
         * write operations, and on execute to treat all operations the same.
         */

        /* putIfVersion. Second op fails with wrong version. */
        ops.clear();
        ops.add(factory.createPutIfVersion
                (Key.createKey("a", "a"), strVal("a"),
                 map.get(Key.createKey("a", "a")).getVersion()));
        assertSame(Operation.Type.PUT_IF_VERSION,
                   ops.get(ops.size() - 1).getType());
        ops.add(factory.createPutIfVersion
                (Key.createKey("a", "b"), strVal("d"),
                 map.get(Key.createKey("a", "a")).getVersion()));
        assertSame(Operation.Type.PUT_IF_VERSION,
                   ops.get(ops.size() - 1).getType());
        results = store.execute(ops);
        assertNotNull(results);
        assertEquals(2, results.size());
        assertTrue(results.get(0).getSuccess());
        assertFalse(results.get(1).getSuccess());
        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(2, map.size());
        checkVal("a", map.get(Key.createKey("a", "a")));
        checkVal("b", map.get(Key.createKey("a", "b")));

        /* deleteIfVersion. First op fails with wrong version. */
        ops.clear();
        ops.add(factory.createDeleteIfVersion
                (Key.createKey("a", "a"),
                 map.get(Key.createKey("a", "b")).getVersion()));
        assertSame(Operation.Type.DELETE_IF_VERSION,
                   ops.get(ops.size() - 1).getType());
        ops.add(factory.createDeleteIfVersion
                (Key.createKey("a", "b"),
                 map.get(Key.createKey("a", "b")).getVersion()));
        assertSame(Operation.Type.DELETE_IF_VERSION,
                   ops.get(ops.size() - 1).getType());
        results = store.execute(ops);
        assertNotNull(results);
        assertEquals(2, results.size());
        assertFalse(results.get(0).getSuccess());
        assertTrue(results.get(1).getSuccess());
        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(1, map.size());
        checkVal("a", map.get(Key.createKey("a", "a")));

        /* delete. Second op fails because key doesn't exist. */
        ops.clear();
        ops.add(factory.createDelete
                (Key.createKey("a", "a")));
        assertSame(Operation.Type.DELETE,
                   ops.get(ops.size() - 1).getType());
        ops.add(factory.createDelete
                (Key.createKey("a", "b")));
        assertSame(Operation.Type.DELETE,
                   ops.get(ops.size() - 1).getType());
        results = store.execute(ops);
        assertNotNull(results);
        assertEquals(2, results.size());
        assertTrue(results.get(0).getSuccess());
        assertFalse(results.get(1).getSuccess());
        map = store.multiGet(Key.createKey("a"), null, null);
        assertEquals(0, map.size());
    }

    private void checkVal(String expect, ValueVersion valVers) {
        assertEquals(strVal(expect), valVers.getValue());
        assertNotNull(valVers.getVersion());
    }

    private void checkVal(ValueVersion expectValVers, OperationResult result) {
        assertEquals(expectValVers.getValue(), result.getPreviousValue());
        assertEquals(expectValVers.getVersion(), result.getPreviousVersion());
    }

    private void checkVal(ValueVersion expectValVers, KeyValueVersion result) {
        assertEquals(expectValVers.getValue(), result.getValue());
        assertEquals(expectValVers.getVersion(), result.getVersion());
    }

    private Value strVal(String s) {
        return Value.createValue(s.getBytes());
    }

    private static int MAX_LEVEL = 3;
    private static String[] PATHS = {
        "..111",
        ".111.NNNNN",
        "aaa.1111.NNNNN",
        "aaa.1111.PPPPP",
        "aaa.1111.TTTTT",
        "aaa.333.NNNNN",
        "aaa.333.OOOOO",
        "aaa.55",
        "aaa.7.NNNNN",
        "aaa.9",
        "cc..NNNNN",
        "cc.1.",
    };

    private static int[] BATCH_SIZES = { 1, 2, 5, 100 };
    int batchSizeIndex = -1;

    /**
     * Runs through permuations of Depths and KeyRanges for the tree structure
     * defined by PATHS, doing multiGet and multiDelete tests.
     * <p>
     * We test with all types of keys in this test, including those in the
     * internal key space (//), so the internalStore handle is used.
     */
    private void pathTests()
        throws Throwable {

        for (int majorPathLevel = 0;
             majorPathLevel < MAX_LEVEL;
             majorPathLevel += 1) {

            final boolean[][] levelToDataNodePermutations = {
                { false, false, true },
                { false, true, true },
                { true, false, true },
                { true, true, true },
            };

            for (final boolean[] levelToDataNode :
                 levelToDataNodePermutations) {

                final Tree root = buildTree(majorPathLevel, levelToDataNode);
                final boolean doDump = false;
                if (doDump) {
                    dump(root);
                }

                try {
                    insertData(root);

                    root.visitAll(new Visitor() {
                        @Override
                        public boolean visit(Tree tree) throws Throwable {
                            for (final Depth depth :
                                 EnumSet.allOf(Depth.class)) {

                                /* Tests with null range. */
                                multiGetPathTests(tree, null, depth, false);
                                multiGetPathTests(tree, null, depth, true);
                                multiDeletePathTests(tree, null, depth);

                                /* Tests with ranges. */
                                for (final KeyRange range :
                                     getKeyRangePermutations(tree)) {

                                    multiGetPathTests(tree, range, depth,
                                                      false);
                                    multiGetPathTests(tree, range, depth,
                                                      true);
                                    multiDeletePathTests(tree, range, depth);
                                }
                            }
                            return true;
                        }
                    });

                    deleteData(root);
                } catch (Throwable e) {
                    System.err.println
                        ("majorPathLevel=" + majorPathLevel +
                         " levelToDataNode=" +
                         Arrays.toString(levelToDataNode));
                    throw e;
                }
            }
        }
        close();
    }

    private void multiGetPathTests(final Tree parent,
                                   final KeyRange range,
                                   final Depth depth,
                                   final boolean keysOnly)
        throws Throwable {

        batchSizeIndex += 1;
        if (batchSizeIndex >= BATCH_SIZES.length) {
            batchSizeIndex = 0;
        }
        final int batchSize = BATCH_SIZES[batchSizeIndex];
        Iterator<Key> iter;

        if (!parent.isRoot()) {

            /* multiGet and multiGetKeys */
            if (keysOnly) {
                final SortedSet<Key> set =
                    internalStore.multiGetKeys(parent.key, range, depth);
                iter = set.iterator();
            } else {
                final SortedMap<Key, ValueVersion> map =
                    internalStore.multiGet(parent.key, range, depth);
                checkMapValues(map);
                iter = map.keySet().iterator();
            }

            if (parent.inMajorPath && !parent.isMajorPath) {
                assertFalse(iter.hasNext());
            } else {
                multiGetPathCheck(iter, parent, range, depth, keysOnly);
            }

            /* multiGetIterator and multiGetKeysIterator, Direction.FORWARD */
            if (keysOnly) {
                iter = internalStore.multiGetKeysIterator
                    (Direction.FORWARD, batchSize, parent.key, range, depth);
            } else {
                final SortedMap<Key, ValueVersion> map =
                    new TreeMap<Key, ValueVersion>();

                final Iterator<KeyValueVersion> kvvIter =
                    internalStore.multiGetIterator
                    (Direction.FORWARD, batchSize, parent.key, range, depth);

                Key prevKey = null;
                while (kvvIter.hasNext()) {
                    final KeyValueVersion kvv = kvvIter.next();
                    if (prevKey != null) {
                        assertTrue(prevKey.compareTo(kvv.getKey()) < 0);
                    }
                    prevKey = kvv.getKey();
                    assertFalse(map.containsKey(kvv.getKey()));
                    map.put(kvv.getKey(),
                            new ValueVersion(kvv.getValue(),
                                             kvv.getVersion()));
                }
                checkMapValues(map);
                iter = map.keySet().iterator();
            }

            if (parent.inMajorPath && !parent.isMajorPath) {
                /* Partial major path should return nothing. */
                assertFalse(iter.hasNext());
            } else {
                multiGetPathCheck(iter, parent, range, depth, keysOnly);
            }

            /* multiGetIterator and multiGetKeysIterator, Direction.REVERSE */
            if (keysOnly) {
                iter = internalStore.multiGetKeysIterator
                    (Direction.REVERSE, batchSize, parent.key, range, depth);
                /* Sort in forward order before checking them. */
                final SortedSet<Key> sortedSet = new TreeSet<Key>();
                Key prevKey = null;
                while (iter.hasNext()) {
                    final Key key = iter.next();
                    if (prevKey != null) {
                        assertTrue(prevKey.compareTo(key) > 0);
                    }
                    if (key.equals(parent.key)) {
                        assertNull(prevKey);
                    } else {
                        prevKey = key;
                    }
                    assertFalse(sortedSet.contains(key));
                    sortedSet.add(key);
                }
                iter = sortedSet.iterator();
            } else {
                final SortedMap<Key, ValueVersion> map =
                    new TreeMap<Key, ValueVersion>();

                final Iterator<KeyValueVersion> kvvIter =
                    internalStore.multiGetIterator
                    (Direction.REVERSE, batchSize, parent.key, range, depth);

                Key prevKey = null;
                while (kvvIter.hasNext()) {
                    final KeyValueVersion kvv = kvvIter.next();
                    if (prevKey != null) {
                        assertTrue(prevKey.compareTo(kvv.getKey()) > 0);
                    }
                    if (kvv.getKey().equals(parent.key)) {
                        assertNull(prevKey);
                    } else {
                        prevKey = kvv.getKey();
                    }
                    assertFalse(map.containsKey(kvv.getKey()));
                    map.put(kvv.getKey(),
                            new ValueVersion(kvv.getValue(),
                                             kvv.getVersion()));
                }
                checkMapValues(map);
                iter = map.keySet().iterator();
            }

            if (parent.inMajorPath && !parent.isMajorPath) {
                /* Partial major path should return nothing. */
                assertFalse(iter.hasNext());
            } else {
                multiGetPathCheck(iter, parent, range, depth, keysOnly);
            }
        }

        /* storeIterator and storeKeysIterator, Direction.UNORDERED */
        if (parent.key == null ||
            (parent.key.getMinorPath().size() == 0 &&
             parent.inMajorPath && !parent.isMajorPath)) {
            if (keysOnly) {
                iter = internalStore.storeKeysIterator
                    (Direction.UNORDERED, batchSize, parent.key, range, depth);
                /* Sort unordered results before checking them. */
                final SortedSet<Key> sortedSet = new TreeSet<Key>();
                while (iter.hasNext()) {
                    final Key key = iter.next();
                    assertFalse(sortedSet.contains(key));
                    sortedSet.add(key);
                }
                iter = sortedSet.iterator();
            } else {
                final SortedMap<Key, ValueVersion> map =
                    new TreeMap<Key, ValueVersion>();

                final Iterator<KeyValueVersion> kvvIter =
                    internalStore.storeIterator
                    (Direction.UNORDERED, batchSize, parent.key, range, depth);

                while (kvvIter.hasNext()) {
                    final KeyValueVersion kvv = kvvIter.next();
                    assertFalse(map.containsKey(kvv.getKey()));
                    map.put(kvv.getKey(),
                            new ValueVersion(kvv.getValue(),
                                             kvv.getVersion()));
                }
                checkMapValues(map);
                iter = map.keySet().iterator();
            }

            multiGetPathCheck(iter, parent, range, depth, keysOnly);
        }
    }

    /**
     * Check that values match keys.
     */
    private void checkMapValues(SortedMap<Key, ValueVersion> map) {
        TableAPI tableAPI = internalStore.getTableAPI();
        waitForTable(tableAPI, IndexStatsLeaseDesc.TABLE_NAME);
        waitForTable(tableAPI, PartitionStatsLeaseDesc.TABLE_NAME);
        waitForTable(tableAPI, TableStatsPartitionDesc.TABLE_NAME);
        waitForTable(tableAPI, TableStatsIndexDesc.TABLE_NAME);
        List<TableImpl> list = new ArrayList<TableImpl>();
        list.add((TableImpl)tableAPI.getTable(IndexStatsLeaseDesc.TABLE_NAME));
        list.add((TableImpl)tableAPI.getTable(
                PartitionStatsLeaseDesc.TABLE_NAME));
        list.add((TableImpl)tableAPI.getTable(
                TableStatsPartitionDesc.TABLE_NAME));
        list.add((TableImpl)tableAPI.getTable(TableStatsIndexDesc.TABLE_NAME));

        for (SortedMap.Entry<Key, ValueVersion> entry : map.entrySet()) {
            /* Filter data in stats tables */
            if (isStatsTableRecord(entry.getKey().toByteArray(), list)) {
                continue;
            }
            assertTrue(Arrays.equals
                (entry.getKey().toByteArray(),
                 entry.getValue().getValue().getValue()));
            assertNotNull(entry.getValue().getVersion());
        }
    }

    /**
     * Checks that iterator values returned from a multi-get operation match
     * the nodes in the test Tree.
     */
    private void multiGetPathCheck(final Iterator<Key> iter,
                                   final Tree parent,
                                   final KeyRange range,
                                   final Depth depth,
                                   final boolean keysOnly)
        throws Throwable {

        try {
            /* Visit selected nodes in this tree. */
            traverseTree(parent, range, depth, new Visitor() {
                @Override
                public boolean visit(Tree tree) {

                    /* Expect this node was returned. */
                    assertTrue(tree.toString(), iter.hasNext());
                    final Key key = iter.next();
                    assertEquals(tree.key, key);
                    return true;
                }
            });

            TableAPI tableAPI = internalStore.getTableAPI();
            List<TableImpl> list = new ArrayList<TableImpl>();
            list.add((TableImpl)tableAPI.getTable(
                IndexStatsLeaseDesc.TABLE_NAME));
            list.add((TableImpl)tableAPI.
                    getTable(PartitionStatsLeaseDesc.TABLE_NAME));
            list.add((TableImpl)tableAPI.
                    getTable(TableStatsPartitionDesc.TABLE_NAME));
            list.add((TableImpl)tableAPI.getTable(
                TableStatsIndexDesc.TABLE_NAME));
            int keyCount = 0;

            /* Filter out all data in stats table */
            while(iter.hasNext()) {
                if (!isStatsTableRecord(iter.next().toByteArray(), list)) {
                    keyCount++;
                }

            }

            /* Expect no more nodes were returned. */
            assertTrue(keyCount == 0);

        } catch (Throwable e) {
            System.err.println
                ("parent=" + parent +
                 " depth=" + depth +
                 " range=" + range +
                 " keysOnly=" + keysOnly);
            throw e;
        }
    }

    private void multiDeletePathTests(final Tree parent,
                                      final KeyRange range,
                                      final Depth depth)
        throws Throwable {

        try {
            if (parent.isRoot()) {
                return;
            }

            /* With partial major path, multiDelete should return zero. */
            if (parent.inMajorPath && !parent.isMajorPath) {
                final int nDeleted =
                    internalStore.multiDelete(parent.key, null, null);
                assertEquals(0, nDeleted);
                return;
            }

            /* Other searches returns the data node descendents. */
            final int nDeleted =
                internalStore.multiDelete(parent.key, range, depth);
            final int[] expectNDeleted = new int[1];

            /*
             * Use execute to re-insert deleted keys.  This compensates for
             * delete to enable further tests.
             */
            final List<Operation> compensatingOps = new ArrayList<Operation>();

            /* Visit selected nodes in this tree. */
            traverseTree(parent, range, depth, new Visitor() {
                @Override
                public boolean visit(Tree tree) {

                    /* Expect this node was deleted. */
                    final Key key = tree.key;
                    final ValueVersion val = internalStore.get(key);
                    assertNull(val);
                    expectNDeleted[0] += 1;

                    compensatingOps.add
                        (internalStore.getOperationFactory().createPutIfAbsent
                            (key, Value.createValue(key.toByteArray())));

                    return true;
                }
            });

            assertEquals(expectNDeleted[0], nDeleted);

            if (nDeleted > 0) {
                internalStore.execute(compensatingOps);
            }

        } catch (Throwable e) {
            System.err.println
                ("parent=" + parent +
                 " depth=" + depth +
                 " range=" + range);
            throw e;
        }
    }

    private void traverseTree(final Tree parent,
                              final KeyRange range,
                              final Depth depth,
                              final Visitor visitor)
        throws Throwable {

        /* Visit all nodes in this tree. */
        parent.visitAll(new Visitor() {
            @Override
            public boolean visit(Tree tree) throws Throwable {

                /* Skip immediate children that are not in the KeyRange. */
                if (tree.parent == parent) {
                    final boolean isInRange = inRange(tree, range);
                    if (range != null) {
                        /* Test KeyRange.inRange while we're here. */
                        assertEquals(isInRange,
                                     range.inRange(parent.key, tree.key));
                    }
                    if (!isInRange) {
                        /* Skip over this subtree. */
                        return false;
                    }
                }

                /* Skip non-data nodes, they do not have records. */
                if (!tree.isDataNode) {
                    return true;
                }

                /* Only expect records at the depth requested. */
                switch (depth) {
                    case PARENT_AND_DESCENDANTS:
                        break;
                    case PARENT_AND_CHILDREN:
                        if (tree != parent && tree.parent != parent) {
                            return true;
                        }
                        break;
                    case DESCENDANTS_ONLY:
                        if (tree == parent) {
                            return true;
                        }
                        break;
                    case CHILDREN_ONLY:
                        if (tree.parent != parent) {
                            return true;
                        }
                        break;
                    default:
                        fail();
                }

                /* Pass this node to the caller's visitor. */
                visitor.visit(tree);
                return true;
            }
        });
    }

    private void insertData(Tree root)
        throws Throwable {

        root.visitAll(new Visitor() {
            @Override
            public boolean visit(Tree tree) {
                if (tree.isDataNode) {
                    final Key key = tree.key;
                    final Version vers = internalStore.putIfAbsent
                        (key, Value.createValue(key.toByteArray()));
                    assertNotNull(vers);
                }
                return true;
            }
        });
    }

    private void deleteData(Tree root)
        throws Throwable {

        root.visitAll(new Visitor() {
            @Override
            public boolean visit(Tree tree) {
                if (tree.isDataNode) {
                    final boolean deleted = internalStore.delete(tree.key);
                    assertTrue(deleted);
                    final ValueVersion val = internalStore.get(tree.key);
                    assertNull(val);
                }
                return true;
            }
        });

        root.visitAll(new Visitor() {
            @Override
            public boolean visit(Tree tree) {
                if (tree.isMajorPath) {
                    final SortedMap<Key, ValueVersion> map =
                        internalStore.multiGet
                        (tree.key, null, Depth.PARENT_AND_DESCENDANTS);
                    assertTrue(map.isEmpty());
                }
                return true;
            }
        });

        final Iterator<Key> iter =
            internalStore.storeKeysIterator(Direction.UNORDERED, 1);

        TableAPI tableAPI = internalStore.getTableAPI();
        List<TableImpl> list = new ArrayList<TableImpl>();
        list.add((TableImpl)tableAPI.getTable(IndexStatsLeaseDesc.TABLE_NAME));
        list.add((TableImpl)tableAPI.getTable(PartitionStatsLeaseDesc.TABLE_NAME));
        list.add((TableImpl)tableAPI.getTable(TableStatsPartitionDesc.TABLE_NAME));
        list.add((TableImpl)tableAPI.getTable(TableStatsIndexDesc.TABLE_NAME));
        int keyCount = 0;

        while(iter.hasNext()) {
            /* Only accumulate the data is not in stats tables */
            if (!isStatsTableRecord(iter.next().toByteArray(), list)) {
                keyCount++;
            }
        }

        assertTrue(keyCount == 0);
    }

    private Tree buildTree(int majorPathLevel, boolean[] levelToDataNode) {
        final Tree root = new Tree();
        for (String path : PATHS) {
            Tree parent = root;
            int level = 0;
            final String[] names = path.split("\\.", -1);
            for (int i = 0; i < names.length; i += 1) {
                final String name = names[i];
                final boolean isLast = (i == names.length - 1);
                final boolean inMajorPath = level <= majorPathLevel;
                final boolean isMajorPath = inMajorPath &&
                                            (level == majorPathLevel ||
                                             isLast);
                final boolean isDataNode = isLast ||
                                           (levelToDataNode[level] &&
                                            (isMajorPath || !inMajorPath));
                parent = parent.addChild(name, inMajorPath, isMajorPath,
                                         isDataNode);
                level += 1;
            }
        }
        return root;
    }

    void dump(Tree root) throws Throwable {
        System.out.println("--- dump start ---");
        root.visitAll(new Visitor() {
            @Override
            public boolean visit(Tree tree) {
                if (tree.isDataNode) {
                    System.out.println(tree.key);
                }
                return true;
            }
        });
        System.out.println("--- dump end ---");
    }

    /**
     * Implements range checking using Tree nodes to test the algorithm
     * implemented by KVStore.
     */
    private boolean inRange(Tree child, KeyRange range) {

        if (range == null) {
            return true;
        }

        final String name = child.name;
        final String start = range.getStart();
        final String end = range.getEnd();

        if (range.isPrefix()) {
            return name.startsWith(start);
        }

        if (start != null) {
            final int cmp = name.compareTo(start);
            if (range.getStartInclusive() ? (cmp < 0) : (cmp <= 0)) {
                return false;
            }
        }

        if (end != null) {
            final int cmp = name.compareTo(end);
            if (range.getEndInclusive() ? (cmp > 0) : (cmp >= 0)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns a permutation of ranges for the children of the given parent.
     */
    private Collection<KeyRange> getKeyRangePermutations(Tree parent) {

        final Set<KeyRange> ranges = new HashSet<KeyRange>();

        for (String name1 : parent.children.keySet()) {

            for (String start : new String[] { null, name1 }) {

                /* Prefix range. */
                if (start != null) {
                    ranges.add(new KeyRange(start));
                }

                for (String name2 : parent.children.keySet()) {

                    for (String end : new String[] { null, name2 }) {

                        /* Start/end ranges. */
                        if (start == null && end == null) {
                            continue;
                        }
                        if (start != null &&
                            end != null &&
                            start.compareTo(end) >= 0) {
                            continue;
                        }
                        ranges.add(new KeyRange(start, true, end, true));
                        ranges.add(new KeyRange(start, false, end, true));
                        ranges.add(new KeyRange(start, true, end, false));
                        ranges.add(new KeyRange(start, false, end, false));
                    }
                }
            }
        }

        return ranges;
    }

    /**
     * Called for each node in a tree.
     */
    private interface Visitor {
        boolean visit(Tree tree) throws Throwable;
    }

    /**
     * Represents a tree node and its children.  A tree node may or may not
     * correspond to a database record, since not all parent paths have data.
     */
    private class Tree {
        final Tree parent;
        final String name;
        final boolean inMajorPath;
        final boolean isMajorPath;
        final boolean isDataNode;
        final SortedMap<String, Tree> children = new TreeMap<String, Tree>();
        final Key key;

        /**
         * Creates the root node.
         */
        Tree() {
            this(null, null, true, false, false);
        }

        /**
         * Creates a non-root node.
         */
        Tree(Tree parent,
             String name,
             boolean inMajorPath,
             boolean isMajorPath,
             boolean isDataNode) {

            this.parent = parent;
            this.name = name;
            this.inMajorPath = inMajorPath;
            this.isMajorPath = isMajorPath;
            this.isDataNode = isDataNode;

            if (parent == null) {
                key = null;
                return;
            }
            if (parent.key == null) {
                assertTrue(parent.isRoot());
                assertTrue(inMajorPath);
                key = Key.createKey(name);
                return;
            }
            final List<String> major;
            final List<String> minor;
            if (inMajorPath) {
                major = new ArrayList<String>(parent.key.getMajorPath());
                major.add(name);
                minor = parent.key.getMinorPath();
            } else {
                major = parent.key.getMajorPath();
                minor = new ArrayList<String>(parent.key.getMinorPath());
                minor.add(name);
            }
            key = Key.createKey(major, minor);
            /* Test KeyRange.isPrefix while we're here. */
            assertTrue(parent.key.isPrefix(key));
        }

        boolean isRoot() {
            return parent == null;
        }

        Tree addChild(String childName,
                      boolean childInMajorPath,
                      boolean childIsMajorPath,
                      boolean childDataNode) {
            Tree child = children.get(childName);
            if (child != null) {
                return child;
            }
            child = new Tree(this, childName, childInMajorPath,
                             childIsMajorPath, childDataNode);
            children.put(childName, child);
            return child;
        }

        void visitAll(Visitor visitor) throws Throwable {
            if (!visitor.visit(this)) {
                return;
            }
            for (Tree child : children.values()) {
                child.visitAll(visitor);
            }
        }

        @Override
        public String toString() {
            return "<Tree name=" + name +
                   " inMajorPath= " + inMajorPath +
                   " isMajorPath= " + isMajorPath +
                   " isDataNode= " + isDataNode +
                   ' ' + key + '>';
        }
    }

    /**
     * Ensures that keys in the internal keyspace (//) are not visible to store
     * iterators.
     * <p>
     * Uses 'store' to read, internal keys should be hidden.  Uses
     * 'internalStore' to write keys in the internal key space (//).
     */
    @Test
    public void testHiddenInternalKeyspace() throws Throwable {

        /* While here, ensure internal handle can't be closed. */
        try {
            internalStore.close();
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        /* Test data. */
        final Set<Key> internalKeys = new HashSet<Key>();
        for (String s : new String[] { "/", "//", "//a", "//a/-/b" }) {
            internalKeys.add(Key.fromString(s));
        }
        final Set<Key> externalKeys = new HashSet<Key>();
        for (String s : new String[] { "/%01", "/a", "/b//-/c" }) {
            externalKeys.add(Key.fromString(s));
        }

        /* Test with all internal and external keys. */
        hiddenTest(internalKeys, externalKeys);

        /* Test with one internal and external key at a time. */
        for (Key iKey : internalKeys) {
            for (Key eKey : externalKeys) {
                hiddenTest(Collections.singleton(iKey),
                           Collections.singleton(eKey));
            }
        }
    }

    private void hiddenTest(Set<Key> internalKeys, Set<Key> externalKeys) {

        final Set<Key> allKeys = new HashSet<Key>();
        allKeys.addAll(internalKeys);
        allKeys.addAll(externalKeys);

        /* Insert all keys with 'internalStore'. */
        for (Key key : allKeys) {
            final Version vers = internalStore.putIfAbsent
                (key, Value.createValue(key.toByteArray()));
            assertNotNull(vers);
        }

        /*
         * Internal keys should be hidden to 'store'.  Only external keys
         * should be returned.
         *
         * Only the storeIterator and storeKeysIterator methods are tested.  It
         * is not possible to read the internal keyspace with get, multiGet or
         * multiGetKeys because these methods take a parent key parameter, and
         * the parent key is checked to ensure it is not in the internal
         * keyspace; this is tested by prohibitInternalKeyspaceTests in
         * BasicOperationTest.  Furthermore, we need only test storeIterator
         * and storeKeysIterator with a null parentKey param; a non-null
         * parentKey is also tested by prohibitInternalKeyspaceTests.
         *
         * TODO: Add tests for reverse ordered full-store iteration, when we
         * support this in the future.
         */
        final Iterator<KeyValueVersion> iter =
            store.storeIterator(Direction.UNORDERED, 1);
        final Set<Key> foundKeys = new HashSet<Key>();
        while (iter.hasNext()) {
            foundKeys.add(iter.next().getKey());
        }
        assertEquals(externalKeys, foundKeys);

        Iterator<Key> keyIter =
            store.storeKeysIterator(Direction.UNORDERED, 1);
        foundKeys.clear();
        while (keyIter.hasNext()) {
            foundKeys.add(keyIter.next());
        }
        assertEquals(externalKeys, foundKeys);

        /* Test with KeyRange with null begin key. */
        keyIter = store.storeKeysIterator
            (Direction.UNORDERED, 1, null,
             new KeyRange(null, false, "z", true),
             null);
        foundKeys.clear();
        while (keyIter.hasNext()) {
            foundKeys.add(keyIter.next());
        }
        assertEquals(externalKeys, foundKeys);

        /* Test with KeyRange with empty inclusive begin key. */
        keyIter = store.storeKeysIterator
            (Direction.UNORDERED, 1, null,
             new KeyRange("", true, null, false),
             null);
        foundKeys.clear();
        while (keyIter.hasNext()) {
            foundKeys.add(keyIter.next());
        }
        assertEquals(externalKeys, foundKeys);

        /* Test with KeyRange with empty exclusive begin key. */
        keyIter = store.storeKeysIterator
            (Direction.UNORDERED, 1, null,
             new KeyRange("", false, null, false),
             null);
        foundKeys.clear();
        while (keyIter.hasNext()) {
            foundKeys.add(keyIter.next());
        }
        assertEquals(externalKeys, foundKeys);

        /* Delete all keys with 'internalStore'. */
        for (Key key : allKeys) {
            final boolean deleted = internalStore.delete(key);
            assertTrue(deleted);
        }
    }

    /**
     * Check whether a key is in tables
     */
    private boolean isStatsTableRecord(byte[] key, List<TableImpl> tableList) {
        for (TableImpl table : tableList) {
            if (table.findTargetTable(key) != null) {
                return true;
            }
        }
        return false;
    }
}
