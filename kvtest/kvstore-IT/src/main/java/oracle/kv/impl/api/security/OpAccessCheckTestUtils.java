/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.security;

import static oracle.kv.Depth.PARENT_AND_DESCENDANTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;

import oracle.kv.Direction;
import oracle.kv.EntryStream;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyValue;
import oracle.kv.KeyValueVersion;
import oracle.kv.OperationFactory;
import oracle.kv.OperationResult;
import oracle.kv.ParallelScanIterator;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.StoreIteratorException;
import oracle.kv.UnauthorizedException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.stats.KVStats;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.TableOperationResult;

public class OpAccessCheckTestUtils {

    /* A dummy version */
    protected final static Version dummyVer =
        new Version(UUID.randomUUID(), 123456789l);

    /**
     * Test write ops by user or server that should not encounter any
     * exception
     *
     * @param testStore store for test
     * @param helperStore store helping to do recovery and cleanup in the test,
     * usually is privileged to read and write
     * @param key key for put test
     * @param value value for put test
     */
    public static void testValidPutOps(KVStore testStore,
                                       KVStore helperStore,
                                       Key key,
                                       Value value)
        throws Exception {
        Version v = null;
        Version sv1 = null;
        Version sv2 = null;
        OperationFactory of = testStore.getOperationFactory();
        List<OperationResult> opRes;
        try {
            /* put */
            v = testStore.put(key, value);
            assertNotNull(v);

            /* putIfVersion */
            sv1 = testStore.putIfVersion(key, value, v);
            assertNotNull(sv1);

            /* putIfPresent */
            sv2 = testStore.putIfPresent(key, value);
            assertNotNull(sv2);

            /* putIfAbsent */
            v = testStore.putIfAbsent(key, value);
            assertNull(v);

            /* put as operation */
            opRes = testStore.execute(Arrays.asList(of.createPut(key, value)));
            assertTrue(opRes.get(0).getSuccess());
            sv2 = opRes.get(0).getNewVersion();

            /* putIfPresent as operation */
            opRes = testStore.execute(
                Arrays.asList(of.createPutIfPresent(key, value)));
            assertTrue(opRes.get(0).getSuccess());
            sv2 = opRes.get(0).getNewVersion();

            /* putIfAbsent as operation */
            opRes = testStore.execute(
                Arrays.asList(of.createPutIfAbsent(key, value)));
            assertFalse(opRes.get(0).getSuccess());

            /* putIfVersion as operation */
            opRes = testStore.execute(
                Arrays.asList(of.createPutIfVersion(key, value, sv2)));
            assertTrue(opRes.get(0).getSuccess());
            sv2 = opRes.get(0).getNewVersion();

            /* putBulk */
            final TestKVStream stream =
                new TestKVStream(new KeyValue(key, value));
            testStore.put(
                Collections.singletonList((EntryStream<KeyValue>)stream), null);
            assertTrue(stream.isCompleted());
        } finally {
            /* Do some cleanup */
            helperStore.delete(key);
        }
    }

    /**
     * Tests delete ops by user that should not encounter any exception.  Note
     * that the test key should have an existing entry in the store.
     *
     * @param testStore store for test
     * @param helperStore store helping to do recovery and cleanup in the test,
     * usually is privileged to read and write
     * @param key key for delete test
     * @param value value for delete test
     */
    public static void testValidDeleteOps(KVStore testStore,
                                          KVStore helperStore,
                                          Key key,
                                          Value value)
        throws Exception {
        Version v = null;
        boolean b;
        int n;
        OperationFactory of = testStore.getOperationFactory();
        List<OperationResult> opRes;
        try {

            /* delete */
            b = testStore.delete(key);
            assertTrue(b);

            v = helperStore.put(key, value);
            assertNotNull(v);

            /* deleteIfVersion */
            b = testStore.deleteIfVersion(key, v);
            assertTrue(b);

            v = helperStore.put(key, value);
            assertNotNull(v);

            /* multiDelete */
            n = testStore.multiDelete(key, null, PARENT_AND_DESCENDANTS);
            assertEquals(1, n);

            v = helperStore.put(key, value);
            assertNotNull(v);

            /* delete as operation */
            opRes = testStore.execute(Arrays.asList(of.createDelete(key)));
            assertTrue(opRes.get(0).getSuccess());

            v = helperStore.put(key, value);
            assertNotNull(v);

            /* deleteIfVersion as operation */
            opRes = testStore.execute(
                Arrays.asList(of.createDeleteIfVersion(key, v)));
            assertTrue(opRes.get(0).getSuccess());
        } finally {
            /* Do some cleanup */
            helperStore.put(key, value);
        }
    }

    /**
     * Tests put ops by user that should not succeed due to unmet
     * authorization.
     */
    public static void testDeniedPutOps(KVStore store,
                                        Key key,
                                        Value value)
        throws Exception {

        OperationFactory of = store.getOperationFactory();

        try {
            store.put(key, value);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* putIfVersion */
        try {
            store.putIfVersion(key, value, dummyVer);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* putIfPresent */
        try {
            store.putIfPresent(key, value);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* putIfAbsent */
        try {
            store.putIfAbsent(key, value);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* put as operation */
        try {
            store.execute(
                Arrays.asList(of.createPut(key, value)));
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* putIfPresent as operation */
        try {
            store.execute(
                Arrays.asList(of.createPutIfPresent(key, value)));
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* putIfAbsent as operation */
        try {
            store.execute(
                Arrays.asList(of.createPutIfAbsent(key, value)));
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* putIfVersion as operation */
        try {
            store.execute(
                Arrays.asList(of.createPutIfVersion(key, value, dummyVer)));
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* putBulk */
        final TestKVStream stream =
            new TestKVStream(new KeyValue(key, value));
        try {
            store.put(
                Collections.singletonList((EntryStream<KeyValue>)stream), null);
            fail("expected exception");
        } catch (FaultException fe) {
            assertTrue(fe.getCause() instanceof UnauthorizedException);
            assertTrue(stream.isCaughtException());
        }
    }

    /**
     * Tests delete ops by user that should not succeed due to unmet
     * authorization.
     */
    public static void testDeniedDeleteOps(KVStore store,
                                           Key key,
                                           boolean accessInternalSpace)
        throws Exception {

        /* A dummy version */
        final UUID uuid = UUID.randomUUID();
        final long vlsn = 123456789;
        Version v = new Version(uuid, vlsn);

        OperationFactory of = store.getOperationFactory();

        /* delete */
        try {
            store.delete(key);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* deleteIfVersion */
        try {
            store.deleteIfVersion(key, v);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* delete as operation */
        try {
            store.execute(Arrays.asList(of.createDelete(key)));
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* deleteIfVersion as operation */
        try {
            store.execute(Arrays.asList(of.createDeleteIfVersion(key, v)));
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* multiDelete */
        if (accessInternalSpace) {
            try {
                store.multiDelete(key, null, PARENT_AND_DESCENDANTS);
                fail("expected exception");
            } catch (UnauthorizedException ue) {
            }
        } else {
            int count = store.multiDelete(key, null, PARENT_AND_DESCENDANTS);
            assertEquals(count, 0);
        }
    }

    /**
     * Tests read ops by user that should not encounter any exception.  Note
     * that the test key should have an existing entry in the store.
     */
    public static void testValidReadOps(KVStore store,
                                        Key key,
                                        boolean isServer) {
        KeyCounts cnts = null;
        ValueVersion vv = null;
        SortedMap<Key, ValueVersion> smkvv;
        SortedSet<Key> ssk;

        Iterator<Key> ik;
        Iterator<KeyValueVersion> ikvv;

        /* get */
        vv = store.get(key);
        assertNotNull(vv);

        /* multiGet */
        smkvv = store.multiGet(key, null, PARENT_AND_DESCENDANTS);
        assertEquals(1, smkvv.size());

        /* multiGetKeys */
        ssk = store.multiGetKeys(key, null, PARENT_AND_DESCENDANTS);
        assertEquals(1, ssk.size());

        /* multiGetIterator */
        ikvv = store.multiGetIterator(Direction.FORWARD, 10,
                                      key, null, PARENT_AND_DESCENDANTS);
        cnts = countKVVIter(ikvv);
        if (isServer) {
            assertEquals(1, cnts.serverCount);
        } else {
            assertEquals(1, cnts.userCount);
            assertEquals(0, cnts.serverCount);
        }

        /* multiGetKeysIterator */
        ik = store.multiGetKeysIterator(Direction.FORWARD, 10,
                                        key, null, PARENT_AND_DESCENDANTS);
        cnts = countKeyIter(ik);
        if (isServer) {
            assertEquals(1, cnts.serverCount);
        } else {
            assertEquals(1, cnts.userCount);
            assertEquals(0, cnts.serverCount);
        }

        /* storeKeysIterator */
        /* TODO - Problematic use of K/V iteration due to system tables
        ik = store.storeKeysIterator(Direction.UNORDERED, 10);
        cnts = countKeyIter(ik);
        if (isServer) {
            assertTrue(cnts.serverCount >= 1);
        } else {
            assertEquals(1, cnts.userCount);
            assertEquals(0, cnts.serverCount);
        }
        */

        /* storeIterator */
        /* TODO - Problematic use of K/V iteration due to system tables
        ikvv = store.storeIterator(Direction.UNORDERED, 10);
        cnts = countKVVIter(ikvv);
        if (isServer) {
            assertTrue(cnts.serverCount >= 1);
        } else {
            assertEquals(1, cnts.userCount);
            assertEquals(0, cnts.serverCount);
        }
        */

        /* following methods not applicable for keys with minor path */
        if (key.getMinorPath().isEmpty()) {

            /* storeKeysIterator with root key */
            ik = store.storeKeysIterator(Direction.UNORDERED, 10, key,
                                         null, PARENT_AND_DESCENDANTS);
            cnts = countKeyIter(ik);
            if (isServer) {
                assertTrue(cnts.serverCount >= 1);
            } else {
                assertEquals(1, cnts.userCount);
                assertEquals(0, cnts.serverCount);
            }

            /* storeIterator with root key */
            ikvv = store.storeIterator(Direction.UNORDERED, 10, key,
                                       null, PARENT_AND_DESCENDANTS);
            cnts = countKVVIter(ikvv);
            if (isServer) {
                assertTrue(cnts.serverCount >= 1);
            } else {
                assertEquals(1, cnts.userCount);
                assertEquals(0, cnts.serverCount);
            }
        }

        /* storeIterator(Iterator<Key>) */
        final ParallelScanIterator<KeyValueVersion> pikvv =
            store.storeIterator(Arrays.asList(key).iterator(), 10,
                                null, PARENT_AND_DESCENDANTS, null,
                                0, null, new StoreIteratorConfig());
        try {
            cnts = countKVVIter(pikvv);
            if (isServer) {
                assertEquals(1, cnts.serverCount);
            } else {
                assertEquals(1, cnts.userCount);
                assertEquals(0, cnts.serverCount);
            }
        } finally {
            pikvv.close();
        }

        /* storeKeysIterator(Iterator<Key>) */
        final ParallelScanIterator<Key> pik =
            store.storeKeysIterator(Arrays.asList(key).iterator(), 10,
                                    null, PARENT_AND_DESCENDANTS, null,
                                    0, null, new StoreIteratorConfig());
        try {
            cnts = countKeyIter(pik);
            if (isServer) {
                assertEquals(1, cnts.serverCount);
            } else {
                assertEquals(1, cnts.userCount);
                assertEquals(0, cnts.serverCount);
            }
        } finally {
            pik.close();
        }

        /* Some miscellaneous checks */

        KVStats stats = store.getStats(false);
        assertNotNull(stats);
    }

    public static void testDeniedReadOps(KVStore store,
                                         Key key,
                                         boolean internalSpaceRead) {

        /* get */
        try {
            store.get(key);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        if (internalSpaceRead) {
            deniedInternalSpaceRead(store, key);
        } else {
            deniedUserSpaceRead(store, key);
        }
    }

    /* User execute multi-key operation to read internal space */
    private static void deniedInternalSpaceRead(KVStore store, Key key) {
        Iterator<Key> ik;
        Iterator<KeyValueVersion> ikvv;

        /* multiGet */
        try {
            store.multiGet(key, null, PARENT_AND_DESCENDANTS);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* multiGetKeys */
        try {
            store.multiGetKeys(key, null, PARENT_AND_DESCENDANTS);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* multiGetIterator */
        try {
            ikvv = store.multiGetIterator(Direction.FORWARD, 10, key, null,
                                          PARENT_AND_DESCENDANTS);
            countKVVIter(ikvv);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* multiGetKeysIterator */
        try {
            ik = store.multiGetKeysIterator(Direction.FORWARD, 10, key, null,
                                            PARENT_AND_DESCENDANTS);
            countKeyIter(ik);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* storeKeysIterator */
        try {
            ik = store.storeKeysIterator(Direction.UNORDERED, 10, key, null,
                                         PARENT_AND_DESCENDANTS);
            countKeyIter(ik);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* storeIterator */
        try {
            ikvv = store.storeIterator(Direction.UNORDERED, 10, key, null,
                                       PARENT_AND_DESCENDANTS);
            countKVVIter(ikvv);
            fail("expected exception");
        } catch (UnauthorizedException ue) {
        }

        /* storeIterator(Iterator<Key>) */
        ParallelScanIterator<KeyValueVersion> pikvv = null;
        try {
            pikvv = store.storeIterator(Arrays.asList(key).iterator(), 10,
                                        null, PARENT_AND_DESCENDANTS, null,
                                        0, null, new StoreIteratorConfig());
            countKVVIter(pikvv);
            fail("expected exception");
        } catch (StoreIteratorException sie) {
            assertTrue(sie.getCause() instanceof UnauthorizedException);
        } finally {
            if (pikvv != null) {
                pikvv.close();
            }
        }

        /* storeKeysIterator(Iterator<Key>) */
        ParallelScanIterator<Key> pik = null;
        try {
            pik = store.storeKeysIterator(Arrays.asList(key).iterator(), 10,
                                          null, PARENT_AND_DESCENDANTS, null,
                                          0, null, new StoreIteratorConfig());
            countKeyIter(pik);
            fail("expected exception");
        } catch (StoreIteratorException sie) {
            assertTrue(sie.getCause() instanceof UnauthorizedException);
        } finally {
            if (pik != null) {
                pik.close();
            }
        }
    }

    /* User execute multi-key operation to read user space */
    private static void deniedUserSpaceRead(KVStore store, Key key) {
        Iterator<Key> ik;
        Iterator<KeyValueVersion> ikvv;
        KeyCounts counts;

        /* multiGet */
        final SortedMap<Key, ValueVersion> map =
            store.multiGet(key, null, PARENT_AND_DESCENDANTS);
        assertEquals(map.size(), 0);

        /* multiGetKeys */
        final SortedSet<Key> keys =
            store.multiGetKeys(key, null, PARENT_AND_DESCENDANTS);
        assertEquals(keys.size(), 0);

        /* multiGetIterator */
        ikvv = store.multiGetIterator(Direction.FORWARD, 10, key, null,
                                      PARENT_AND_DESCENDANTS);
        counts = countKVVIter(ikvv);
        assertEquals(counts.userCount, 0);
        assertEquals(counts.serverCount, 0);

        /* multiGetKeysIterator */
        ik = store.multiGetKeysIterator(Direction.FORWARD, 10, key, null,
                                        PARENT_AND_DESCENDANTS);
        counts = countKeyIter(ik);
        assertEquals(counts.userCount, 0);
        assertEquals(counts.serverCount, 0);

        /* following methods not applicable for keys with minor path */
        if (key.getMinorPath().isEmpty()) {
            /* storeKeysIterator */
            ik = store.storeKeysIterator(Direction.UNORDERED, 10, key, null,
                                         PARENT_AND_DESCENDANTS);
            counts = countKeyIter(ik);
            assertEquals(counts.userCount, 0);
            assertEquals(counts.serverCount, 0);

            /* storeIterator */
            ikvv = store.storeIterator(Direction.UNORDERED, 10, key, null,
                                       PARENT_AND_DESCENDANTS);
            counts = countKVVIter(ikvv);
            assertEquals(counts.userCount, 0);
            assertEquals(counts.serverCount, 0);
        }

        /* storeIterator(Iterator<Key>, ...) */
        final StoreIteratorConfig config =
            new StoreIteratorConfig().setMaxConcurrentRequests(0);
        ikvv = store.storeIterator(Arrays.asList(key).iterator(), 10, null,
                                   PARENT_AND_DESCENDANTS, null, 0, null,
                                   config);
        counts = countKVVIter(ikvv);
        assertEquals(counts.userCount, 0);
        assertEquals(counts.serverCount, 0);

        /* storeKeysIterator(Iterator<Key>, ...) */
        ik = store.storeKeysIterator(Arrays.asList(key).iterator(), 10, null,
                                     PARENT_AND_DESCENDANTS, null, 0, null,
                                     config);
        counts = countKVVIter(ikvv);
        assertEquals(counts.userCount, 0);
        assertEquals(counts.serverCount, 0);
    }

    /**
     * Tests table delete ops by user that should not encounter any exception.
     * Note that the test key should NOT have any existing entry in the table.
     *
     * @param testStore store for test
     * @param helperStore store helping to do recovery and cleanup in the test,
     * usually is privileged to read and write
     * @param row row for insert test
     */
    public static void testValidTableInsertOps(KVStore testStore,
                                               KVStore helperStore,
                                               Row row)
        throws Exception {
        final TableAPI tableAPI = testStore.getTableAPI();
        final TableOperationFactory tof =
            tableAPI.getTableOperationFactory();
        final PrimaryKey key = row.createPrimaryKey();
        List<TableOperationResult> tableOpRes;
        Version ver;

        assertNull("Key should not have an existing entry: " + key,
                   helperStore.getTableAPI().get(key, null));

        try {
            /* PutIfAbsent */
            ver = tableAPI.putIfAbsent(row, null, null);
            assertNotNull(ver);

            /* PutIfPresent */
            ver = tableAPI.putIfPresent(row, null, null);
            assertNotNull(ver);

            /* Put */
            ver = tableAPI.put(row, null, null);
            assertNotNull(ver);

            /* PutIfVersion */
            ver = tableAPI.putIfVersion(row, ver, null, null);
            assertNotNull(ver);

            assertTrue(helperStore.getTableAPI().delete(key, null, null));

            /* PutIfAbsent as operation */
            tableOpRes = tableAPI.execute(
                Arrays.asList(tof.createPutIfAbsent(row, null, true)),
                null);
            assertTrue(tableOpRes.get(0).getSuccess());

            /* PutIfPresent as operation */
            tableOpRes = tableAPI.execute(
                Arrays.asList(tof.createPutIfPresent(row, null, true)),
                null);
            assertTrue(tableOpRes.get(0).getSuccess());

            /* Put as operation */
            tableOpRes = tableAPI.execute(
                Arrays.asList(tof.createPut(row, null, true)),
                null);
            assertTrue(tableOpRes.get(0).getSuccess());
            ver = tableOpRes.get(0).getNewVersion();
            assertNotNull(ver);

            /* PutIfVersion as operation */
            tableOpRes = tableAPI.execute(
                Arrays.asList(tof.createPutIfVersion(row, ver, null, true)),
                null);
            assertTrue(tableOpRes.get(0).getSuccess());

            /* PutBulk */
            final TestRowStream stream = new TestRowStream(row);
            tableAPI.put(Collections.singletonList((EntryStream<Row>)stream),
                         null);
            assertTrue(stream.isCompleted());
        } finally {
            /* clean-up */
            helperStore.getTableAPI().delete(key, null, null);
        }
    }

    /**
     * Tests table read ops by user that should not encounter any exception.
     * Note that the test key should have an existing entry in the table.
     */
    public static void testValidTableReadOps(KVStore testStore,
                                             PrimaryKey key,
                                             IndexKey idxKey) {
        final TableAPI tableAPI = testStore.getTableAPI();

        /* Get */
         final Row row = tableAPI.get(key, null);
         assertNotNull(row);

         /* MultiGet */
         final List<Row> rowRes = tableAPI.multiGet(key, null, null);
         assertEquals(rowRes.size(), 1);

         /* MultiGetKey */
         final List<PrimaryKey> keyRes =
             tableAPI.multiGetKeys(key, null, null);
         assertEquals(keyRes.size(), 1);

         /* tableIterator */
         TableIterator<?> tableIter =
             tableAPI.tableIterator(key, null, null);
         KeyCounts cnts = countTableIter(tableIter);
         assertEquals(cnts.userCount, 1);
         assertEquals(cnts.serverCount, 0);

         /* tableKeyIterator */
         tableIter = tableAPI.tableKeysIterator(key, null, null);
         cnts = countTableIter(tableIter);
         assertEquals(cnts.userCount, 1);
         assertEquals(cnts.serverCount, 0);

         if (idxKey != null) {
             /* tableIterator using index */
             tableIter = tableAPI.tableIterator(idxKey, null, null);
             cnts = countTableIter(tableIter);
             assertEquals(cnts.userCount, 1);
             assertEquals(cnts.serverCount, 0);

             /* tableKeyIterator using index */
             tableIter = tableAPI.tableKeysIterator(idxKey, null, null);
             cnts = countTableIter(tableIter);
             assertEquals(cnts.userCount, 1);
             assertEquals(cnts.serverCount, 0);
         }

         /* tableIterator(Iterator<PrimaryKey>...) */
         tableIter = tableAPI.tableIterator(Arrays.asList(key).iterator(),
                                            null, null);
         cnts = countTableIter(tableIter);
         assertEquals(cnts.userCount, 1);
         assertEquals(cnts.serverCount, 0);

         /* tableKeyIterator(Iterator<PrimaryKey>...) */
         tableIter = tableAPI.tableKeysIterator(Arrays.asList(key).iterator(),
                                                null, null);
         cnts = countTableIter(tableIter);
         assertEquals(cnts.userCount, 1);
         assertEquals(cnts.serverCount, 0);
    }

    /**
     * Tests table delete ops by user that should not encounter any exception.
     * Note that the test key should have an existing entry in the table.
     */
    public static void testValidTableDeleteOps(KVStore testStore,
                                               KVStore helperStore,
                                               PrimaryKey key)
        throws Exception {

        final TableAPI tableAPI = testStore.getTableAPI();
        final TableOperationFactory tof = tableAPI.getTableOperationFactory();
        final Row origRow = helperStore.getTableAPI().get(key, null);
        Version ver;
        List<TableOperationResult> tableOpRes;

        try {
            /* Delete */
            assertTrue(tableAPI.delete(key, null, null));

            /* restore the deleted value */
            ver = helperStore.getTableAPI().put(origRow, null, null);
            assertNotNull(ver);

            /* DeleteIfVersion */
            assertTrue(tableAPI.deleteIfVersion(key, ver, null, null));

            /* restore the deleted value */
            ver = helperStore.getTableAPI().put(origRow, null, null);
            assertNotNull(ver);

            final int cnts = tableAPI.multiDelete(key, null, null);
            assertEquals(cnts, 1);

            /* restore the deleted value */
            ver = helperStore.getTableAPI().put(origRow, null, null);
            assertNotNull(ver);

            /* Delete as operation */
            tableOpRes = tableAPI.execute(
                Arrays.asList(tof.createDelete(key, null, true)),
                null);
            assertTrue(tableOpRes.get(0).getSuccess());

            /* restore the deleted value */
            ver = helperStore.getTableAPI().put(origRow, null, null);
            assertNotNull(ver);

            /* DeleteIfVersion as operation */
            tableOpRes = tableAPI.execute(
                Arrays.asList(tof.createDeleteIfVersion(key, ver, null, true)),
                null);
            assertTrue(tableOpRes.get(0).getSuccess());
        } finally {
            /* restore the deleted row */
            helperStore.getTableAPI().put(origRow, null, null);
        }
    }

    public static void testDeniedTableDeleteOps(KVStore testStore,
                                                final PrimaryKey key) {
        final TableAPI tableAPI = testStore.getTableAPI();
        final TableOperationFactory tof = tableAPI.getTableOperationFactory();

        /* Delete */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.delete(key, null, null);
            }
        }.exec();

        /* DeleteIfVersion */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.deleteIfVersion(key, dummyVer, null, null);
            }
        }.exec();

        /* Delete as operation */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.execute(
                    Arrays.asList(tof.createDelete(key, null, true)),
                    null);
            }
        }.exec();

        /* DeleteIfVersion as operation */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.execute(
                    Arrays.asList(
                        tof.createDeleteIfVersion(key, dummyVer, null, true)),
                    null);
            }
        }.exec();

        /* multiDelete */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                final int cnts = tableAPI.multiDelete(key, null, null);
                assertEquals(cnts, 1);
            }
        }.exec();
    }

    public static void testDeniedTableReadOps(KVStore testStore,
                                              final PrimaryKey key,
                                              final IndexKey idxKey) {
        final TableAPI tableAPI = testStore.getTableAPI();

        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.get(key, null);
            }
        }.exec();

        /* MultiGet */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                final List<Row> rowRes = tableAPI.multiGet(key, null, null);
                assertEquals(rowRes.size(), 1);
            }
        }.exec();

        /* MultiGetKey */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                final List<PrimaryKey> keyRes =
                    tableAPI.multiGetKeys(key, null, null);
                assertEquals(keyRes.size(), 1);
            }
        }.exec();

        /* tableIterator */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                TableIterator<?> tableIter =
                    tableAPI.tableIterator(key, null, null);
                countTableIter(tableIter);
            }
        }.exec();

        /* tableKeyIterator */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                TableIterator<?> tableIter =
                    tableAPI.tableKeysIterator(key, null, null);
                countTableIter(tableIter);
            }
        }.exec();

        if (idxKey != null) {
            /* tableIterator using index */
            new TestDeniedExecution() {
                @Override
                void perform() throws Exception {
                    TableIterator<?> tableIter =
                        tableAPI.tableIterator(idxKey, null, null);
                    countTableIter(tableIter);
                }
            }.exec();

            /* tableKeyIterator using index */
            new TestDeniedExecution() {
                @Override
                void perform() throws Exception {
                    TableIterator<?> tableIter =
                        tableAPI.tableKeysIterator(idxKey, null, null);
                    countTableIter(tableIter);
                }
            }.exec();
        }

        /* tableIterator(Iterator<PrimaryKey>, ...) */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                TableIterator<?> tableIter =
                    tableAPI.tableIterator(Arrays.asList(key).iterator(),
                        null, null);
                countTableIter(tableIter);
            }
        }.exec();

        /* tableKeysIterator(Iterator<PrimaryKey>, ...) */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                TableIterator<?> tableIter =
                    tableAPI.tableKeysIterator(Arrays.asList(key).iterator(),
                        null, null);
                countTableIter(tableIter);
            }
        }.exec();
    }

    public static void testDeniedTableInsertOps(KVStore testStore,
                                                final Row row) {
        final TableAPI tableAPI = testStore.getTableAPI();
        final TableOperationFactory tof = tableAPI.getTableOperationFactory();

        /* Put */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.put(row, null, null);
            }
        }.exec();

        /* PutIfAbsent */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.putIfAbsent(row, null, null);
            }
        }.exec();

        /* PutIfPresent */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.putIfPresent(row, null, null);
            }
        }.exec();

        /* PutIfVerison */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.putIfVersion(row, dummyVer, null, null);
            }
        }.exec();

        /* Put as operation */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.execute(
                    Arrays.asList(tof.createPut(row, null, true)), null);
            }
        }.exec();

        /* PutIfAbsent as operation */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.execute(
                    Arrays.asList(tof.createPutIfAbsent(row, null, true)),
                    null);
            }
        }.exec();

        /* PutIfPresent as operation */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.execute(
                    Arrays.asList(tof.createPutIfPresent(row, null, true)),
                    null);
            }
        }.exec();

        /* PutIfVersion as operation */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                tableAPI.execute(
                    Arrays.asList(
                        tof.createPutIfVersion(row, dummyVer, null, true)),
                    null);
            }
        }.exec();

        /* PutBulk */
        new TestDeniedExecution() {
            @Override
            void perform() throws Exception {
                final TestRowStream stream = new TestRowStream(row);
                tableAPI.put(
                    Collections.singletonList((EntryStream<Row>)stream), null);
            }
        }.exec();
    }

    private static KeyCounts countTableIter(TableIterator<?> iter) {
        int userCount = 0;
        while (iter.hasNext()) {
            iter.next();
            userCount++;
        }
        return new KeyCounts(userCount, 0);
    }

    private static KeyCounts countKeyIter(Iterator<Key> iter) {
        int userCount = 0;
        int serverCount = 0;
        while (iter.hasNext()) {
            Key key = iter.next();
            List<String> majorPath = key.getMajorPath();
            if (majorPath.size() >= 2 &&
                majorPath.get(0).isEmpty() &&
                majorPath.get(1).isEmpty()) {
                serverCount++;
            } else {
                userCount++;
            }
        }
        return new KeyCounts(userCount, serverCount);
    }

    private static KeyCounts countKVVIter(Iterator<KeyValueVersion> iter) {
        int userCount = 0;
        int serverCount = 0;
        while (iter.hasNext()) {
            Key key = iter.next().getKey();
            List<String> majorPath = key.getMajorPath();
            if (majorPath.size() >= 2 &&
                majorPath.get(0).isEmpty() &&
                majorPath.get(1).isEmpty()) {
                serverCount++;
            } else {
                userCount++;
            }
        }
        return new KeyCounts(userCount, serverCount);
    }

    private static abstract class TestDeniedExecution {
        void exec() {
            try {
                perform();
                fail("Expected UnauthorizedException");
            } catch (UnauthorizedException uae) {
                /* expected, ignore */
            } catch (StoreIteratorException sie) {
                if (!(sie.getCause() instanceof UnauthorizedException)) {
                    throw sie;
                }
            } catch (FaultException fe) {
                if (!(fe.getCause() instanceof UnauthorizedException)) {
                    throw fe;
                }
            } catch (Exception e) {
                fail("Expected UnauthorizedException, but was " + e);
            }
        }
        abstract void perform() throws Exception;
    }

    private static class KeyCounts {
        private final int userCount;
        private final int serverCount;
        private KeyCounts(int userCount, int serverCount) {
            this.userCount = userCount;
            this.serverCount = serverCount;
        }
    }

    /**
     * Implementation of EntryStream<KeyValue> used for Store.putBulk().
     */
    private static class TestKVStream extends TestStream<KeyValue> {
        TestKVStream(KeyValue... entries) {
            super(entries);
        }
    }

    /**
     * Implementation of EntryStream<Row> used for TableAPI.putBulk().
     */
    private static class TestRowStream extends TestStream<Row> {
        TestRowStream(Row... entries) {
            super(entries);
        }
    }

    private static abstract class TestStream<T> implements EntryStream<T> {

        private final List<T> list;
        private final Iterator<T> iterator;
        private boolean isCompleted;
        private Exception exception;

        /*
         * Suppress warnings about potential heap pollution. This code appears
         * to be safe the same way that Collections.addAll is.
         */
        @SuppressWarnings({"all", "varargs"})
        @SafeVarargs
        TestStream(@SuppressWarnings("unchecked") T... entries) {
            list = new ArrayList<T>(entries.length);
            for (T entry : entries) {
                list.add(entry);
            }
            iterator = list.iterator();
            isCompleted = false;
            exception = null;
        }

        @Override
        public String name() {
            return "TestPutKVStream";
        }

        @Override
        public T getNext() {
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
        public void keyExists(T entry) {
        }

        @Override
        public void catchException(RuntimeException runtimeException, T entry) {
            exception = runtimeException;
            throw runtimeException;
        }

        public boolean isCompleted() {
            return isCompleted;
        }

        public boolean isCaughtException() {
            return exception != null;
        }
    }
}
