/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableLimits.NO_CHANGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.BulkWriteOptions;
import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.EntryStream;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.test.TestHook;
import oracle.kv.table.FieldRange;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiGetResult;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.TableUtils;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * This is a class to test per operation cost calculation using various
 * table api's.
 */
public class TableThroughputTest extends TableTestBase {

    final float rsF          = 2.5f;
    final int   minReadBytes = 1024;
    final int   minRKB       = (int)Math.ceil(minReadBytes/1024.0);

    final ExecuteRequestHook executeRequestHook = new ExecuteRequestHook();

    @BeforeClass
    public static void staticSetUp() throws Exception {
        //TODO: remove this after MRTable is put on cloud.
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        TableTestBase.staticSetUp(1, 1, 1);
    }

    @Test
    public void testDeleteRRNoneRowPresent() throws Exception {
        testdelete(Choice.NONE, true);
    }

    @Test
    public void testDeleteRRNoneRowAbsent() throws Exception {
        testdelete(Choice.NONE, false);
    }

    @Test
    public void testDeleteRRVersionRowPresent() throws Exception {
        testdelete(Choice.VERSION, true);
    }

    @Test
    public void testDeleteRRVersionRowAbsent() throws Exception {
        testdelete(Choice.VERSION, false);
    }

    @Test
    public void testDeleteRRValueRowPresent() throws Exception {
        testdelete(Choice.VALUE, true);
    }


    @Test
    public void testDeleteRRValueRowAbsent() throws Exception {
        testdelete(Choice.VALUE, false);
    }

    @Test
    public void testDeleteRRAllRowPresent() throws Exception {
        testdelete(Choice.ALL, true);
    }


    @Test
    public void testDeleteRRAllRowAbsent() throws Exception {
        testdelete(Choice.ALL, false);
    }

    @Test
    public void testDeleteIfVersionRRNoneRowPresentMatch() throws Exception {
        testdeleteIfVersion(Choice.NONE, true, true);
    }

    @Test
    public void testDeleteIfVersionRRNoneRowPresentNoMatch() throws Exception {
        testdeleteIfVersion(Choice.NONE, true, false);
    }

    @Test
    public void testDeleteIfVersionRRNoneRowAbsent() throws Exception {
        testdeleteIfVersion(Choice.NONE, false, false);
    }

    @Test
    public void testDeleteIfVersionRRVersionRowPresentMatch() throws Exception {
        testdeleteIfVersion(Choice.VERSION, true, true);
    }

    @Test
    public void testDeleteIfVersionRRVersionRowPresentNoMatch()
        throws Exception {
        testdeleteIfVersion(Choice.VERSION, true, false);
    }

    @Test
    public void testDeleteIfVersionRRVersionRowAbsent() throws Exception {
        testdeleteIfVersion(Choice.VERSION, false, false);
    }

    @Test
    public void testDeleteIfVersionRRValueRowPresentMatch() throws Exception {
        testdeleteIfVersion(Choice.VALUE, true, true);
    }

    @Test
    public void testDeleteIfVersionRRValueRowPresentNoMatch() throws Exception {
        testdeleteIfVersion(Choice.VALUE, true, false);
    }

    @Test
    public void testDeleteIfVersionRRValueRowAbsent() throws Exception {
        testdeleteIfVersion(Choice.VALUE, false, false);
    }

    @Test
    public void testDeleteIfVersionRRAllRowPresentMatch() throws Exception {
        testdeleteIfVersion(Choice.ALL, true, true);
    }

    @Test
    public void testDeleteIfVersionRRAllRowPresentNoMatch() throws Exception {
        testdeleteIfVersion(Choice.ALL, true, true);
    }

    @Test
    public void testDeleteIfVersionRRAllRowAbsent() throws Exception {
        testdeleteIfVersion(Choice.ALL, false, false);
    }

    @Test
    public void testmultiDeleteRowsPresent() throws Exception {
        testmultiDelete(true, false);
    }

    @Test
    public void testmultiDeleteRowsAbsent() throws Exception {
        testmultiDelete(false, false);
    }

    @Test
    public void testmultiDeleteNoneWithFieldRange() throws Exception {
        testmultiDelete(true, true);
    }

    @Test
    public void testgetConsistencyNoneRowPresent()
        throws Exception {
        testget(Consistency.NONE_REQUIRED, true);
    }

    @Test
    public void testgetConsistencyAbsoluteRowPresent()
        throws Exception {
        testget(Consistency.ABSOLUTE, true);
    }

    @Test
    public void testgetConsistencyNoneRowAbsent()
        throws Exception {
        testget(Consistency.NONE_REQUIRED, false);
    }

    @Test
    public void testgetConsistencyAbsoluteRowAbsent()
        throws Exception {
        testget(Consistency.ABSOLUTE, false);
    }

    @Test
    public void testmultiGetConsistencyNoneRowsPresent()
        throws Exception {
        testmultiGet(Consistency.NONE_REQUIRED, true, false, false);
    }

    @Test
    public void testmultiGetConsistencyAbsoluteRowsPresent()
        throws Exception {
        testmultiGet(Consistency.ABSOLUTE, true, false, false);
    }

    @Test
    public void testmultiGetConsistencyNoneRowsAbsent()
        throws Exception {
        testmultiGet(Consistency.NONE_REQUIRED, false, false, false);
    }

    @Test
    public void testmultiGetConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testmultiGet(Consistency.ABSOLUTE, false, false, false);
    }

    @Test
    public void testmultiGetConsistencyNoneWithFieldRange()
        throws Exception {
        testmultiGet(Consistency.NONE_REQUIRED, true, true, false);
    }

    @Test
    public void testmultiGetConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testmultiGet(Consistency.ABSOLUTE, true, true, false);
    }

    @Test
    public void testmultiGetConsistencyNoneWithNestedTables()
        throws Exception {
        testmultiGet(Consistency.NONE_REQUIRED, true, true, true);
    }

    @Test
    public void testmultiGetConsistencyAbsoluteWithNestedTables()
        throws Exception {
        testmultiGet(Consistency.ABSOLUTE, true, true, true);
    }

    @Test
    public void testmultiGetKeysConsistencyNoneRowsPresent()
        throws Exception {
        testmultiGetKeys(Consistency.NONE_REQUIRED, true, false, false);
    }

    @Test
    public void testmultiGetKeysConsistencyAbsoluteRowsPresent()
        throws Exception {
        testmultiGetKeys(Consistency.ABSOLUTE, true, false, false);
    }

    @Test
    public void testmultiGetKeysConsistencyNoneRowsAbsent()
        throws Exception {
        testmultiGetKeys(Consistency.NONE_REQUIRED, false, false, false);
    }

    @Test
    public void testmultiGetKeysConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testmultiGetKeys(Consistency.ABSOLUTE, false, false, false);
    }

    @Test
    public void testmultiGetKeysConsistencyNoneWithFieldRange()
        throws Exception {
        testmultiGetKeys(Consistency.NONE_REQUIRED, true, true, false);
    }

    @Test
    public void testmultiGetKeysConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testmultiGetKeys(Consistency.ABSOLUTE, true, true, false);
    }

    @Test
    public void testmultiGetKeysConsistencyNoneWithNestedTables()
        throws Exception {
        testmultiGetKeys(Consistency.NONE_REQUIRED, true, true, true);
    }

    @Test
    public void testmultiGetKeysConsistencyAbsoluteWithNestedTables()
        throws Exception {
        testmultiGetKeys(Consistency.ABSOLUTE, true, true, true);
    }

    @Test
    public void testtableIteratorPKeyConsistencyNoneRowsPresent()
        throws Exception {
        testtableIteratorPKey(Consistency.NONE_REQUIRED,
                              true,
                              false,
                              false,
                              false);
    }

    @Test
    public void testtableIteratorPKeyConsistencyAbsoluteRowsPresent()
        throws Exception {
        testtableIteratorPKey(Consistency.ABSOLUTE,
                              true,
                              false,
                              false,
                              false);
    }

    @Test
    public void testtableIteratorPKeyConsistencyNoneRowsAbsent()
        throws Exception {
        testtableIteratorPKey(Consistency.NONE_REQUIRED,
                              false,
                              false,
                              false,
                              false);
    }

    @Test
    public void testtableIteratorPKeyConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testtableIteratorPKey(Consistency.ABSOLUTE,
                              false,
                              false,
                              false,
                              false);
    }

    @Test
    public void testtableIteratorPKeyConsistencyNoneWithFieldRange()
        throws Exception {
        testtableIteratorPKey(Consistency.NONE_REQUIRED,
                              true,
                              true,
                              false,
                              false);
    }

    @Test
    public void testtableIteratorPKeyConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testtableIteratorPKey(Consistency.ABSOLUTE,
                              true,
                              true,
                              false,
                              false);
    }

    @Test
    public void testtableIteratorPKeyConsistencyNoneWithNestedTables()
        throws Exception {
        testtableIteratorPKey(Consistency.NONE_REQUIRED,
                              true,
                              true,
                              true,
                              false);
    }

    @Test
    public void testtableIteratorPKeyConsistencyAbsoluteWithNestedTables()
        throws Exception {
        testtableIteratorPKey(Consistency.ABSOLUTE,
                              true,
                              true,
                              true,
                              false);
    }

    @Test
    public void testtableIteratorPKeyConsistencyNoneWithFullTableScan()
        throws Exception {
        testtableIteratorPKey(Consistency.NONE_REQUIRED,
                              true,
                              false,
                              true,
                              true);
    }

    @Test
    public void testtableIteratorPKeyConsistencyAbsoluteWithFullTableScan()
        throws Exception {
        testtableIteratorPKey(Consistency.ABSOLUTE,
                              true,
                              false,
                              true,
                              true);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyNoneRowsPresent()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.NONE_REQUIRED,
                                  true,
                                  false,
                                  false,
                                  false);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyAbsoluteRowsPresent()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.ABSOLUTE,
                                  true,
                                  false,
                                  false,
                                  false);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyNoneRowsAbsent()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.NONE_REQUIRED,
                                  false,
                                  false,
                                  false,
                                  false);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.ABSOLUTE,
                                  false,
                                  false,
                                  false,
                                  false);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyNoneWithFieldRange()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.NONE_REQUIRED,
                                  true,
                                  true,
                                  false,
                                  false);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.ABSOLUTE,
                                  true,
                                  true,
                                  false,
                                  false);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyNoneWithNestedTables()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.NONE_REQUIRED,
                                  true,
                                  true,
                                  true,
                                  false);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyAbsoluteWithNestedTables()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.ABSOLUTE,
                                  true,
                                  true,
                                  true,
                                  false);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyNoneWithFullTableScan()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.NONE_REQUIRED,
                                  true,
                                  false,
                                  true,
                                  true);
    }

    @Test
    public void testtableKeysIteratorPKeyConsistencyAbsoluteWithFullTableScan()
        throws Exception {
        testtableKeysIteratorPKey(Consistency.ABSOLUTE,
                                  true,
                                  false,
                                  true,
                                  true);
    }

    @Test
    public void testtableIteratorIKeyConsistencyNoneRowsPresent()
        throws Exception {
        testtableIteratorIKey(Consistency.NONE_REQUIRED, true, false, false);
    }

    @Test
    public void testtableIteratorIKeyConsistencyAbsoluteRowsPresent()
        throws Exception {
        testtableIteratorIKey(Consistency.ABSOLUTE, true, false, false);
    }

    @Test
    public void testtableIteratorIKeyConsistencyNoneRowsAbsent()
        throws Exception {
        testtableIteratorIKey(Consistency.NONE_REQUIRED, false, false, false);
    }

    @Test
    public void testtableIteratorIKeyConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testtableIteratorIKey(Consistency.ABSOLUTE, false, false, false);
    }

    @Test
    public void testtableIteratorIKeyConsistencyNoneWithFieldRange()
        throws Exception {
        testtableIteratorIKey(Consistency.NONE_REQUIRED, true, true, false);
    }

    @Test
    public void testtableIteratorIKeyConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testtableIteratorIKey(Consistency.ABSOLUTE, true, true, false);
    }

    @Test
    public void testtableIteratorIKeyConsistencyNoneWithFullTableScan()
        throws Exception {
        testtableIteratorIKey(Consistency.NONE_REQUIRED, true, false, true);
    }

    @Test
    public void testtableIteratorIKeyConsistencyAbsoluteWithFullTableScan()
        throws Exception {
        testtableIteratorIKey(Consistency.ABSOLUTE, true, false, true);
    }

    @Test
    public void testtableKeysIteratorIKeyConsistencyNoneRowsPresent()
        throws Exception {
        testtableKeysIteratorIKey(Consistency.NONE_REQUIRED, true, false, false);
    }

    @Test
    public void testtableKeysIteratorIKeyConsistencyAbsoluteRowsPresent()
        throws Exception {
        testtableKeysIteratorIKey(Consistency.ABSOLUTE, true, false, false);
    }

    @Test
    public void testtableKeysIteratorIKeyConsistencyNoneRowsAbsent()
        throws Exception {
        testtableKeysIteratorIKey(Consistency.NONE_REQUIRED, false, false, false);
    }

    @Test
    public void testtableKeysIteratorIKeyConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testtableKeysIteratorIKey(Consistency.ABSOLUTE, false, false, false);
    }

    @Test
    public void testtableKeysIteratorIKeyConsistencyNoneWithFieldRange()
        throws Exception {
        testtableKeysIteratorIKey(Consistency.NONE_REQUIRED, true, true, false);
    }

    @Test
    public void testtableKeysIteratorIKeyConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testtableKeysIteratorIKey(Consistency.ABSOLUTE, true, true, false);
    }

    @Test
    public void testtableKeysIteratorIKeyConsistencyNoneWithFullTableScan()
        throws Exception {
        testtableKeysIteratorIKey(Consistency.NONE_REQUIRED, true, false, true);
    }

    @Test
    public void testtableKeysIteratorIKeyConsistencyAbsoluteWithFullTableScan()
        throws Exception {
        testtableKeysIteratorIKey(Consistency.ABSOLUTE, true, false, true);
    }

    @Test
    public void testputIfAbsentRRNoneRowPresent() throws Exception {
        testputIfAbsent(Choice.NONE, true);
    }

    @Test
    public void testputIfAbsentRRNoneRowAbsent() throws Exception {
        testputIfAbsent(Choice.NONE, false);
    }
    @Test
    public void testputIfAbsentRRVersionRowPresent() throws Exception {
        testputIfAbsent(Choice.VERSION, true);
    }

    @Test
    public void testputIfAbsentRRVersionRowAbsent() throws Exception {
        testputIfAbsent(Choice.VERSION, false);
    }

    @Test
    public void testputIfAbsentRRValueRowPresent() throws Exception {
        testputIfAbsent(Choice.VALUE, true);
    }

    @Test
    public void testputIfAbsentRRValueRowAbsent() throws Exception {
        testputIfAbsent(Choice.VALUE, false);
    }

    @Test
    public void testputIfAbsentRRAllRowPresent() throws Exception {
        testputIfAbsent(Choice.ALL, true);
    }

    @Test
    public void testputIfAbsentRRAllRowAbsent() throws Exception {
        testputIfAbsent(Choice.ALL, false);
    }

    @Test
    public void testputIfPresentRRNoneRowPresent() throws Exception {
        testputIfPresent(Choice.NONE, true);
    }

    @Test
    public void testputIfPresentRRNoneRowAbsent() throws Exception {
        testputIfPresent(Choice.NONE, false);
    }

    @Test
    public void testputIfPresentRRVersionRowPresent() throws Exception {
        testputIfPresent(Choice.VERSION, true);
    }

    @Test
    public void testputIfPresentRRVersionRowAbsent() throws Exception {
        testputIfPresent(Choice.VERSION, false);
    }

    @Test
    public void testputIfPresentRRValueRowPresent() throws Exception {
        testputIfPresent(Choice.VALUE, true);
    }

    @Test
    public void testputIfPresentRRValueRowAbsent() throws Exception {
        testputIfPresent(Choice.VALUE, false);
    }

    @Test
    public void testputIfPresentRRAllRowPresent() throws Exception {
        testputIfPresent(Choice.ALL, true);
    }

    @Test
    public void testputIfPresentRRAllRowAbsent() throws Exception {
        testputIfPresent(Choice.ALL, false);
    }

    @Test
    public void testputIfVersionRRNoneRowPresent() throws Exception {
        testputIfVersion(Choice.NONE, true);
    }

    @Test
    public void testputIfVersionRRNoneRowAbsent() throws Exception {
        testputIfVersion(Choice.NONE, false);
    }

    @Test
    public void testputIfVersionRRVersionRowPresent() throws Exception {
        testputIfVersion(Choice.VERSION, true);
    }

    @Test
    public void testputIfVersionRRVersionRowAbsent() throws Exception {
        testputIfVersion(Choice.VERSION, false);
    }

    @Test
    public void testputIfVersionRRValueRowPresent() throws Exception {
        testputIfVersion(Choice.VALUE, true);
    }

    @Test
    public void testputIfVersionRRValueRowAbsent() throws Exception {
        testputIfVersion(Choice.VALUE, false);
    }

    @Test
    public void testputIfVersionRRAllRowPresent() throws Exception {
        testputIfVersion(Choice.ALL, true);
    }

    @Test
    public void testputIfVersionRRAllRowAbsent() throws Exception {
        testputIfVersion(Choice.ALL, false);
    }

    @Test
    public void testputRRNoneRowPresent() throws Exception {
        testput(Choice.NONE, true);
    }

    @Test
    public void testputRRNoneRowAbsent() throws Exception {
        testput(Choice.NONE, false);
    }

    @Test
    public void testputRRVersionRowPresent() throws Exception {
        testput(Choice.VERSION, true);
    }

    @Test
    public void testputRRVersionRowAbsent() throws Exception {
        testput(Choice.VERSION, false);
    }

    @Test
    public void testputRRValueRowPresent() throws Exception {
        testput(Choice.VALUE, true);
    }

    @Test
    public void testputRRValueRowAbsent() throws Exception {
        testput(Choice.VALUE, false);
    }

    @Test
    public void testputRRAllRowPresent() throws Exception {
        testput(Choice.ALL, true);
    }

    @Test
    public void testputRRAllRowAbsent() throws Exception {
        testput(Choice.ALL, false);
    }

    /*
     * TODO: The target table of bulk put operation can be multiple, we need
     * enhance track throughput to support tracking multiple tables during
     * the execution of a operation, enable below 2 tests after fixing the
     * problem.
     */
    //@Test
    public void testputBatchRowsPresent() throws Exception {
        testputBatch(true);
    }

    //@Test
    public void testputBatchRowsAbsent() throws Exception {
        testputBatch(false);
    }

    @Test
    public void testbulkGetTableListIteratorPKeyConsistencyNoneRowsPresent()
        throws Exception {
        testbulkGetTableListIteratorPKey(Consistency.NONE_REQUIRED,
                                         true,
                                         false,
                                         false);
    }

    @Test
    public void testbulkGetTableListIteratorPKeyConsistencyAbsoluteRowsPresent()
        throws Exception {
        testbulkGetTableListIteratorPKey(Consistency.ABSOLUTE,
                                         true,
                                         false,
                                         false);
    }

    @Test
    public void testbulkGetTableListIteratorPKeyConsistencyNoneRowsAbsent()
        throws Exception {
        testbulkGetTableListIteratorPKey(Consistency.NONE_REQUIRED,
                                         false,
                                         false,
                                         false);
    }

    @Test
    public void testbulkGetTableListIteratorPKeyConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testbulkGetTableListIteratorPKey(Consistency.ABSOLUTE,
                                         false,
                                         false,
                                         false);
    }

    @Test
    public void testbulkGetTableListIteratorPKeyConsistencyNoneWithFieldRange()
        throws Exception {
        testbulkGetTableListIteratorPKey(Consistency.NONE_REQUIRED,
                                         true,
                                         true,
                                         false);
    }

    @Test
    public void
        testbulkGetTableListIteratorPKeyConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testbulkGetTableListIteratorPKey(Consistency.ABSOLUTE,
                                         true,
                                         true,
                                         false);
    }

    @Test
    public void
        testbulkGetTableListIteratorPKeyConsistencyNoneWithNestedTables()
        throws Exception {
        testbulkGetTableListIteratorPKey(Consistency.NONE_REQUIRED,
                                         true,
                                         true,
                                         true);
    }

    @Test
    public void
        testbulkGetTableListIteratorPKeyConsistencyAbsoluteWithNestedTables()
        throws Exception {
        testbulkGetTableListIteratorPKey(Consistency.ABSOLUTE,
                                         true,
                                         true,
                                         true);
    }

    @Test
    public void testbulkGetTableIteratorPKeyConsistencyNoneRowsPresent()
        throws Exception {
        testbulkGetTableIteratorPKey(Consistency.NONE_REQUIRED,
                                     true,
                                     false,
                                     false);
    }

    @Test
    public void testbulkGetTableIteratorPKeyConsistencyAbsoluteRowsPresent()
        throws Exception {
        testbulkGetTableIteratorPKey(Consistency.ABSOLUTE,
                                     true,
                                     false,
                                     false);
    }

    @Test
    public void testbulkGetTableIteratorPKeyConsistencyNoneRowsAbsent()
        throws Exception {
        testbulkGetTableIteratorPKey(Consistency.NONE_REQUIRED,
                                     false,
                                     false,
                                     false);
    }

    @Test
    public void testbulkGetTableIteratorPKeyConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testbulkGetTableIteratorPKey(Consistency.ABSOLUTE,
                                     false,
                                     false,
                                     false);
    }

    @Test
    public void testbulkGetTableIteratorPKeyConsistencyNoneWithFieldRange()
        throws Exception {
        testbulkGetTableIteratorPKey(Consistency.NONE_REQUIRED,
                                     true,
                                     true,
                                     false);
    }

    @Test
    public void testbulkGetTableIteratorPKeyConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testbulkGetTableIteratorPKey(Consistency.ABSOLUTE,
                                     true,
                                     true,
                                     false);
    }

    @Test
    public void testbulkGetTableIteratorPKeyConsistencyNoneWithNestedTables()
        throws Exception {
        testbulkGetTableIteratorPKey(Consistency.NONE_REQUIRED,
                                     true,
                                     true,
                                     true);
    }

    @Test
    public void
        testbulkGetTableIteratorPKeyConsistencyAbsoluteWithNestedTables()
        throws Exception {
        testbulkGetTableIteratorPKey(Consistency.ABSOLUTE,
                                     true,
                                     true,
                                     true);
    }

    @Test
    public void testbulkGetTableKeysIteratorPKeyConsistencyNoneRowsPresent()
        throws Exception {
        testbulkGetTableKeysIteratorPKey(Consistency.NONE_REQUIRED,
                                         true,
                                         false,
                                         false);
    }

    @Test
    public void testbulkGetTableKeysIteratorPKeyConsistencyAbsoluteRowsPresent()
        throws Exception {
        testbulkGetTableKeysIteratorPKey(Consistency.ABSOLUTE,
                                         true,
                                         false,
                                         false);
    }

    @Test
    public void testbulkGetTableKeysIteratorPKeyConsistencyNoneRowsAbsent()
        throws Exception {
        testbulkGetTableKeysIteratorPKey(Consistency.NONE_REQUIRED,
                                         false,
                                         false,
                                         false);
    }

    @Test
    public void testbulkGetTableKeysIteratorPKeyConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testbulkGetTableKeysIteratorPKey(Consistency.ABSOLUTE,
                                         false,
                                         false,
                                         false);
    }

    @Test
    public void testbulkGetTableKeysIteratorPKeyConsistencyNoneWithFieldRange()
        throws Exception {
        testbulkGetTableKeysIteratorPKey(Consistency.NONE_REQUIRED,
                                         true,
                                         true,
                                         false);
    }

    @Test
    public void testbulkGetTableKeysIteratorPKeyConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testbulkGetTableKeysIteratorPKey(Consistency.ABSOLUTE,
                                         true,
                                         true,
                                         false);
    }

    @Test
    public void
        testbulkGetTableKeysIteratorPKeyConsistencyNoneWithNestedTables()
        throws Exception {
        testbulkGetTableKeysIteratorPKey(Consistency.NONE_REQUIRED,
                                         true,
                                         true,
                                         true);
    }

    @Test
    public void
        testbulkGetTableKeysIteratorPKeyConsistencyAbsoluteWithNestedTables()
        throws Exception {
        testbulkGetTableKeysIteratorPKey(Consistency.ABSOLUTE,
                                         true,
                                         true,
                                         true);
    }

    @Test
    public void testbulkGetTableKeysListIteratorPKeyConsistencyNoneRowsPresent()
        throws Exception {
        testbulkGetTableKeysListIteratorPKey(Consistency.NONE_REQUIRED,
                                             true,
                                             false,
                                             false);
    }

    @Test
    public void
        testbulkGetTableKeysListIteratorPKeyConsistencyAbsoluteRowsPresent()
        throws Exception {
        testbulkGetTableKeysListIteratorPKey(Consistency.ABSOLUTE,
                                             true,
                                             false,
                                             false);
    }

    @Test
    public void testbulkGetTableKeysListIteratorPKeyConsistencyNoneRowsAbsent()
        throws Exception {
        testbulkGetTableKeysListIteratorPKey(Consistency.NONE_REQUIRED,
                                             false,
                                             false,
                                             false);
    }

    @Test
    public void
        testbulkGetTableKeysListIteratorPKeyConsistencyAbsoluteRowsAbsent()
        throws Exception {
        testbulkGetTableKeysListIteratorPKey(Consistency.ABSOLUTE,
                                             false,
                                             false,
                                             false);
    }

    @Test
    public void
        testbulkGetTableKeysListIteratorPKeyConsistencyNoneWithFieldRange()
        throws Exception {
        testbulkGetTableKeysListIteratorPKey(Consistency.NONE_REQUIRED,
                                             true,
                                             true,
                                             false);
    }

    @Test
    public void
        testbulkGetTableKeysListIteratorPKeyConsistencyAbsoluteWithFieldRange()
        throws Exception {
        testbulkGetTableKeysListIteratorPKey(Consistency.ABSOLUTE,
                                             true,
                                             true,
                                             false);
    }

    @Test
    public void
        testbulkGetTableKeysListIteratorPKeyConsistencyNoneWithNestedTables()
        throws Exception {
        testbulkGetTableKeysListIteratorPKey(Consistency.NONE_REQUIRED,
                                             true,
                                             true,
                                             true);
    }

    @Test
    public void
        testbulkGetTableKeysListIteratorPKeyConsistencyAbsoluteWithNestedTables()
        throws Exception {
        testbulkGetTableKeysListIteratorPKey(Consistency.ABSOLUTE,
                                             true,
                                             true,
                                             true);
    }

    private void testdelete(Choice choice, boolean isRowPresent)
            throws Exception {
        Row           row;
        ReturnRow     rr;
        KVStoreConfig kvconfig  = createKVConfig(createStore);
        int           rowSizeKB = 0;
        int           idxSizeKB = 0;

        store     = KVStoreFactory.getStore(kvconfig);
        tableImpl = (TableAPIImpl) store.getTableAPI();

        final TableImpl  table = buildUserTable(store);
        final PrimaryKey pkey  = table.createPrimaryKey();

        setTableLimits(table, new TableLimits(1000, 1000, NO_CHANGE));

        row = createRow(table, "testDelete", 1, rsF);
        tableImpl.put(row, null, null);

        if(isRowPresent) {
            row = createRow(table, "testDelete", 2, rsF);
            tableImpl.put(row, null, null);
            rowSizeKB = getRowSizeKB(row);
            idxSizeKB = getIndexSizeKB(table, row);
        }

        pkey.put("firstName", "firsttestDelete2");
        pkey.put("lastName", "lasttestDelete");

        rr = table.createReturnRow(choice);

        startTallyThroughput();

        boolean ret = tableImpl.delete(pkey, rr, null);
        assertEquals(isRowPresent, ret);

        int expectedWriteKB = isRowPresent ? (rowSizeKB+idxSizeKB) : 0;
        int expectedReadKB  = !isRowPresent ?
                                  minRKB : choice.needValue() ?
                                      rowSizeKB : minRKB;
        /* Read costs incurred during a write are doubled */
        expectedReadKB *= 2;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testdeleteIfVersion(Choice choice,
                                     boolean isRowPresent,
                                     boolean match)
            throws Exception {
        Row           row;
        ReturnRow     rr;
        Version       version = null;
        KVStoreConfig kvconfig  = createKVConfig(createStore);
        int           rowSizeKB = 0;
        int           idxSizeKB = 0;

        store     = KVStoreFactory.getStore(kvconfig);
        tableImpl = (TableAPIImpl) store.getTableAPI();

        final TableImpl  table = buildUserTable(store);
        final PrimaryKey pkey  = table.createPrimaryKey();

        setTableLimits(table, new TableLimits(1000, 1000, NO_CHANGE));

        row = createRow(table, "testDeleteIfVersion", 1, rsF);
        if(isRowPresent) {
            version = tableImpl.put(row, null, null);
            rowSizeKB = getRowSizeKB(row);
            idxSizeKB = getIndexSizeKB(table, row);
        } else {
            assert !match;
        }

        if (!match || !isRowPresent) {
            version = new Version(new UUID(0, 0), 1);
        }
        assert version != null;

        pkey.put("firstName", "firsttestDeleteIfVersion1");
        pkey.put("lastName", "lasttestDeleteIfVersion");

        rr = table.createReturnRow(choice);

        startTallyThroughput();

        boolean ret = tableImpl.deleteIfVersion(pkey, version, rr, null);
        assertEquals(isRowPresent && match, ret);

        int expectedWriteKB = isRowPresent && match ? (rowSizeKB+idxSizeKB) : 0;
        int expectedReadKB  = !isRowPresent ?
                                  minRKB : (choice.needValue() && !match) ?
                                      rowSizeKB : minRKB;
        /* Read costs incurred during a write are doubled */
        expectedReadKB *= 2;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testmultiDelete(boolean isRowPresent,
                                 boolean isFieldRange) throws Exception {
        Row             row;
        KVStoreConfig   kvconfig  = createKVConfig(createStore);
        int             nRows     = 100;
        int             rowSizeKB = 0;
        int             idxSizeKB = 0;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        store     = KVStoreFactory.getStore(kvconfig);
        tableImpl = (TableAPIImpl) store.getTableAPI();

        final TableImpl  table = buildUserTable(store);
        final PrimaryKey pkey  = table.createPrimaryKey();

        setTableLimits(table, new TableLimits(1000, 1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testmultiDeleteSet1", i, rsF);
            tableImpl.put(row, null, null);
        }

        if(isRowPresent) {
            for(int i=0; i<nRows; i++) {
                row = createRow(table, "testmultiDeleteSet2", i, rsF);
                tableImpl.put(row, null, null);
                rowSizeKB += getRowSizeKB(row);
                idxSizeKB += getIndexSizeKB(table, row);
            }
        }

        pkey.put("lastName", "lasttestmultiDeleteSet2");

        if(isFieldRange) {
            fr = table.createFieldRange("firstName");
            fr.setStart("firsttestmultiDeleteSet290", true);
            fr.setEnd("firsttestmultiDeleteSet299", true);
            mro = fr.createMultiRowOptions();
        }

        int expDelRowCnt = isFieldRange ?
                               nRows/10 : isRowPresent ?
                                   nRows : 0;
        startTallyThroughput();

        int actDelRowCnt = tableImpl.multiDelete(pkey, mro, null);
        assertEquals("Incorrect deleted rows", expDelRowCnt, actDelRowCnt);

        int expectedWriteKB = isFieldRange ?
                                  (rowSizeKB+idxSizeKB)/10 : isRowPresent ?
                                      (rowSizeKB+idxSizeKB) : 0;
        int expectedReadKB  = isFieldRange ?
                                  expDelRowCnt*minRKB : isRowPresent ?
                                      expDelRowCnt*minRKB : minRKB;
        expectedReadKB      *= 2;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testget(Consistency consistency,
                         boolean isRowPresent) throws Exception {
        Row row;
        int rowSizeKB = 0;

        final ReadOptions ros   = new ReadOptions(consistency, 0, null);
        final TableImpl   table = buildUserTable(store);
        final PrimaryKey  pkey  = table.createPrimaryKey();

        setTableLimits(table, new TableLimits(1000, 1000, NO_CHANGE));

        row = createRow(table, "testGet", 1, rsF);
        tableImpl.put(row, null, null);

        if(isRowPresent) {
            row = createRow(table, "testGet", 2, rsF);
            tableImpl.put(row, null, null);
            rowSizeKB = getRowSizeKB(row);
        }

        pkey.put("firstName", "firsttestGet2");
        pkey.put("lastName", "lasttestGet");

        startTallyThroughput();
        row = tableImpl.get(pkey, ros);
        if(isRowPresent) {
            assertNotNull(row);
        }
        else {
            assertNull(row);
        }

        int expectedWriteKB = 0;
        int expectedReadKB  = isRowPresent ? rowSizeKB : minRKB;
        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testmultiGet(Consistency consistency,
                              boolean isRowPresent,
                              boolean isFieldRange,
                              boolean isNestedTable) throws Exception {
        Row             row;
        int             nRows     = 100;
        int             rowSizeKB = 0;
        TableImpl       table     = buildUserTable(store);
        TableImpl       tablec    = null;
        TableImpl       tablegc   = null;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        final ReadOptions ros = new ReadOptions(consistency, 0, null);

        setTableLimits(table,
                       new TableLimits(1000, 1000,
                                       NO_CHANGE, NO_CHANGE,
                                       1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testmultiGetSet1", i, rsF);
            tableImpl.put(row, null, null);
        }

        if(isRowPresent) {
            for(int i=0; i<nRows; i++) {
                row = createRow(table, "testmultiGetSet2", i, rsF);
                tableImpl.put(row, null, null);
                rowSizeKB += getRowSizeKB(row);
            }

            if(isNestedTable) {
                tablec  = buildUserChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowc = createRowChild(tablec, "testmultiGetSet2", i);
                    tableImpl.put(rowc, null, null);
                    rowSizeKB += getRowSizeKB(rowc);
                }

                tablegc = buildUserGrandChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowgc = createRowGrandChild(tablegc,
                                                    "testmultiGetSet2",
                                                    i);
                    tableImpl.put(rowgc, null, null);
                    rowSizeKB += getRowSizeKB(rowgc);
                }

                table  = getTable("User");
                tablec = getTable("User.Child");
            }
        }

        final PrimaryKey pkey = table.createPrimaryKey();
        pkey.put("lastName", "lasttestmultiGetSet2");

        if(isFieldRange) {
            fr = table.createFieldRange("firstName");
            fr.setStart("firsttestmultiGetSet290", true);
            fr.setEnd("firsttestmultiGetSet299", true);
            mro = fr.createMultiRowOptions();

            if(isNestedTable) {
                mro.setIncludedChildTables(Arrays.asList(tablec, tablegc));
            }
        }

        int expGetRowCnt = isNestedTable ?
                               (nRows/10)*3 : isFieldRange ?
                                   nRows/10 : isRowPresent ?
                                       nRows : 0;

        startTallyThroughput();

        int actGetRowCnt = tableImpl.multiGet(pkey, mro, ros).size();
        assertEquals("Incorrect rows returned by multiGet",
                     expGetRowCnt,
                     actGetRowCnt);

        int expectedWriteKB = 0;
        int expectedReadKB  = isNestedTable ?
                                  rowSizeKB/10 : isFieldRange ?
                                      rowSizeKB/10 : isRowPresent ?
                                          rowSizeKB : minRKB;
        if (isRowPresent) {
            expectedReadKB += expGetRowCnt * minRKB; /* read keys */
        }
        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testmultiGetKeys(Consistency consistency,
                                  boolean isRowPresent,
                                  boolean isFieldRange,
                                  boolean isNestedTable) throws Exception {
        Row             row;
        int             nRows     = 100;
        TableImpl       table     = buildUserTable(store);
        TableImpl       tablec    = null;
        TableImpl       tablegc   = null;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        final ReadOptions ros = new ReadOptions(consistency, 0, null);

        setTableLimits(table,
                       new TableLimits(1000, 1000,
                                       NO_CHANGE, NO_CHANGE,
                                       1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testmultiGetSet1", i, rsF);
            tableImpl.put(row, null, null);
        }

        if(isRowPresent) {
            for(int i=0; i<nRows; i++) {
                row = createRow(table, "testmultiGetSet2", i, rsF);
                tableImpl.put(row, null, null);
            }

            if(isNestedTable) {
                tablec  = buildUserChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowc = createRowChild(tablec, "testmultiGetSet2", i);
                    tableImpl.put(rowc, null, null);
                }

                tablegc = buildUserGrandChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowgc = createRowGrandChild(tablegc,
                                                    "testmultiGetSet2",
                                                    i);
                    tableImpl.put(rowgc, null, null);
                }

                table  = getTable("User");
                tablec = getTable("User.Child");
            }
        }

        final PrimaryKey pkey = table.createPrimaryKey();
        pkey.put("lastName", "lasttestmultiGetSet2");

        if(isFieldRange) {
            fr = table.createFieldRange("firstName");
            fr.setStart("firsttestmultiGetSet290", true);
            fr.setEnd("firsttestmultiGetSet299", true);
            mro = fr.createMultiRowOptions();

            if(isNestedTable) {
                mro.setIncludedChildTables(Arrays.asList(tablec, tablegc));
            }
        }

        int expGetRowCnt = isNestedTable ?
                               (nRows/10)*3 : isFieldRange ?
                                   nRows/10 : isRowPresent ?
                                       nRows : 0;
        startTallyThroughput();
        int actGetRowCnt = tableImpl.multiGetKeys(pkey, mro, ros).size();
        assertEquals("Incorrect rows returned by multiGetKeys",
                     expGetRowCnt,
                     actGetRowCnt);

        int expectedWriteKB = 0;
        int expectedReadKB = (expGetRowCnt > 0) ? expGetRowCnt * minRKB : minRKB;
        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testtableIteratorPKey(Consistency consistency,
                                       boolean isRowPresent,
                                       boolean isFieldRange,
                                       boolean isNestedTable,
                                       boolean isFullTableScan)
        throws Exception {
        Row             row;
        int             nRows     = 100;
        int             rowSizeKB = 0;
        TableImpl       table     = buildUserTable(store);
        TableImpl       tablec    = null;
        TableImpl       tablegc   = null;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        setTableLimits(table,
                       new TableLimits(3000, 3000,
                                       NO_CHANGE, NO_CHANGE,
                                       1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testtableIteratorPKeySet1", i, rsF);
            tableImpl.put(row, null, null);
            if(isFullTableScan) {
                rowSizeKB += getRowSizeKB(row);
            }
        }

        if(isRowPresent) {
            for(int i=0; i<nRows; i++) {
                row = createRow(table, "testtableIteratorPKeySet2", i, rsF);
                tableImpl.put(row, null, null);
                rowSizeKB += getRowSizeKB(row);
            }

            if(isNestedTable) {
                tablec  = buildUserChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowc = createRowChild(tablec,
                                              "testtableIteratorPKeySet2",
                                              i);
                    tableImpl.put(rowc, null, null);
                    rowSizeKB += getRowSizeKB(rowc);
                }

                tablegc = buildUserGrandChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowgc = createRowGrandChild(tablegc,
                                                    "testtableIteratorPKeySet2",
                                                    i);
                    tableImpl.put(rowgc, null, null);
                    rowSizeKB += getRowSizeKB(rowgc);
                }

                table  = getTable("User");
                tablec = getTable("User.Child");
            }
        }

        final PrimaryKey pkey = table.createPrimaryKey();
        if(!isFullTableScan) {
            pkey.put("lastName", "lasttesttableIteratorPKeySet2");
        }

        if(isFieldRange) {
            fr = table.createFieldRange("firstName");
            fr.setStart("firsttesttableIteratorPKeySet290", true);
            fr.setEnd("firsttesttableIteratorPKeySet299", true);
            mro = fr.createMultiRowOptions();

            if(isNestedTable) {
                mro.setIncludedChildTables(Arrays.asList(tablec, tablegc));
            }
        }

        if(isFullTableScan) {
            mro = new MultiRowOptions(null,
                                      null,
                                      Arrays.asList(tablec, tablegc));
        }

        int expRowCnt = isFullTableScan ?
                            nRows*4 : isNestedTable ?
                                (nRows/10)*3 : isFieldRange ?
                                    nRows/10 : isRowPresent ?
                                        nRows : 0;
        int actRowCnt = 0;

        TableIteratorOptions tio = getTableIteratorOptions(Direction.UNORDERED,
                                                           consistency,
                                                           expRowCnt);

        startTallyThroughput();

        MultiGetResult<Row> mgRes;
        byte[] continuationKey = null;

        while(true) {
            mgRes = tableImpl.multiGet(pkey, continuationKey, mro, tio);
            actRowCnt += mgRes.getResult().size();
            if (mgRes.getContinuationKey() == null) {
                break;
            }
            continuationKey = mgRes.getContinuationKey();
        }

        assertEquals("Incorrect rows returned by tableIterator",
                     expRowCnt,
                     actRowCnt);

        int expectedWriteKB = 0;
        int expectedReadKB  = isFullTableScan ?
                                  rowSizeKB : isNestedTable ?
                                      rowSizeKB/10 : isFieldRange ?
                                          rowSizeKB/10 : isRowPresent ?
                                              rowSizeKB : minRKB;

        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testtableKeysIteratorPKey(Consistency consistency,
                                           boolean isRowPresent,
                                           boolean isFieldRange,
                                           boolean isNestedTable,
                                           boolean isFullTableScan)
        throws Exception {
        Row             row;
        int             nRows     = 100;
        TableImpl       table     = buildUserTable(store);
        TableImpl       tablec    = null;
        TableImpl       tablegc   = null;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        setTableLimits(table,
                       new TableLimits(1000, 1000,
                                       NO_CHANGE, NO_CHANGE,
                                       1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testtableIteratorPKeySet1", i, rsF);
            tableImpl.put(row, null, null);
        }

        if(isRowPresent) {
            for(int i=0; i<nRows; i++) {
                row = createRow(table, "testtableIteratorPKeySet2", i, rsF);
                tableImpl.put(row, null, null);
            }

            if(isNestedTable) {
                tablec  = buildUserChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowc = createRowChild(tablec,
                                              "testtableIteratorPKeySet2",
                                              i);
                    tableImpl.put(rowc, null, null);
                }

                tablegc = buildUserGrandChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowgc = createRowGrandChild(tablegc,
                                                    "testtableIteratorPKeySet2",
                                                    i);
                    tableImpl.put(rowgc, null, null);
                }

                table  = getTable("User");
                tablec = getTable("User.Child");
            }
        }

        final PrimaryKey pkey = table.createPrimaryKey();
        if(!isFullTableScan) {
            pkey.put("lastName", "lasttesttableIteratorPKeySet2");
        }

        if(isFieldRange) {
            fr = table.createFieldRange("firstName");
            fr.setStart("firsttesttableIteratorPKeySet290", true);
            fr.setEnd("firsttesttableIteratorPKeySet299", true);
            mro = fr.createMultiRowOptions();

            if(isNestedTable) {
                mro.setIncludedChildTables(Arrays.asList(tablec, tablegc));
            }
        }

        if(isFullTableScan) {
            mro = new MultiRowOptions(null,
                                      null,
                                      Arrays.asList(tablec, tablegc));
        }

        int expRowCnt = isFullTableScan ?
                            nRows*4 : isNestedTable ?
                                (nRows/10)*3 : isFieldRange ?
                                    nRows/10 : isRowPresent ?
                                        nRows : 0;
        int actRowCnt = 0;

        TableIteratorOptions tio = getTableIteratorOptions(Direction.UNORDERED,
                                                           consistency,
                                                           expRowCnt);

        startTallyThroughput();

        MultiGetResult<PrimaryKey> mgRes;
        byte[] continuationKey = null;
        while(true) {
            mgRes = tableImpl.multiGetKeys(pkey, continuationKey,
            		                       mro, tio);
            actRowCnt += mgRes.getResult().size();
            if (mgRes.getContinuationKey() == null) {
                break;
            }
            continuationKey = mgRes.getContinuationKey();
        }

        assertEquals("Incorrect rows returned by tableIterator",
                     expRowCnt,
                     actRowCnt);

        int expectedWriteKB = 0;
        int expectedReadKB = expRowCnt > 0 ? expRowCnt * minRKB : minRKB ;

        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testtableIteratorIKey(Consistency consistency,
                                       boolean isRowPresent,
                                       boolean isFieldRange,
                                       boolean isFullTableScan)
        throws Exception {
        Row             row;
        int             nRows     = 100;
        int             rowSizeKB = 0;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        final TableImpl table = buildUserTable(store);

        setTableLimits(table,
                       new TableLimits(1000, 1000,
                                       NO_CHANGE, NO_CHANGE,
                                       1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testtableIteratorIKey", i, rsF);
            tableImpl.put(row, null, null);
            rowSizeKB += getRowSizeKB(row);
        }

        Index    fnameIdx    = table.getIndex("FirstName");
        IndexKey fnameIdxKey = fnameIdx.createIndexKey();
        if(isRowPresent) {
            if(isFieldRange) {
                fr = table.createFieldRange("firstName");
                fr.setStart("firsttesttableIteratorIKey90", true);
                fr.setEnd("firsttesttableIteratorIKey99", true);
                mro = fr.createMultiRowOptions();
            }
            else if (isFullTableScan) {
            }
            else {
                fnameIdxKey.put("firstName", "firsttesttableIteratorIKey1");
            }
        }
        else {
            fnameIdxKey.put("firstName", "firsttesttableIteratorIKey100");
        }

        int expRowCnt = isFullTableScan ?
                            nRows : isFieldRange ?
                                    nRows/10 : isRowPresent ?
                                        1 : 0;
        int actRowCnt = 0;

        TableIteratorOptions tio = getTableIteratorOptions(Direction.UNORDERED,
                                                           consistency,
                                                           expRowCnt);
        MultiGetResult<Row> mgRes;
        byte[] continuationKey = null;

        startTallyThroughput();

        while(true) {
            mgRes = tableImpl.multiGet(fnameIdxKey, continuationKey,
            		                   mro, tio);
            actRowCnt += mgRes.getResult().size();
            if (mgRes.getContinuationKey() == null) {
                break;
            }
            continuationKey = mgRes.getContinuationKey();
        }

        assertEquals("Incorrect rows returned by tableIterator",
                     expRowCnt,
                     actRowCnt);

        int expectedWriteKB = 0;
        int expectedReadKB  = isFullTableScan ?
                                rowSizeKB : isFieldRange ?
                                    rowSizeKB/10 : isRowPresent ?
                                        rowSizeKB/nRows : minRKB;
        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testtableKeysIteratorIKey(Consistency consistency,
                                           boolean isRowPresent,
                                           boolean isFieldRange,
                                           boolean isFullTableScan)
        throws Exception {
        Row             row;
        int             nRows     = 100;
        int             idxSizeKB = 0;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        final TableImpl table = buildUserTable(store);

        setTableLimits(table, new TableLimits(1000, 1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testtableIteratorIKey", i, rsF);
            tableImpl.put(row, null, null);
            idxSizeKB += getIndexSizeKB(table, row, "FirstName");
        }

        Index    fnameIdx    = table.getIndex("FirstName");
        IndexKey fnameIdxKey = fnameIdx.createIndexKey();
        if(isRowPresent) {
            if(isFieldRange) {
                fr = table.createFieldRange("firstName");
                fr.setStart("firsttesttableIteratorIKey90", true);
                fr.setEnd("firsttesttableIteratorIKey99", true);
                mro = fr.createMultiRowOptions();
            }
            else if (isFullTableScan) {
            }
            else {
                fnameIdxKey.put("firstName", "firsttesttableIteratorIKey1");
            }
        }
        else {
            fnameIdxKey.put("firstName", "firsttesttableIteratorIKey100");
        }

        int expRowCnt = isFullTableScan ?
                            nRows : isFieldRange ?
                                    nRows/10 : isRowPresent ?
                                        1 : 0;
        int actRowCnt = 0;
        TableIteratorOptions tio = getTableIteratorOptions(Direction.UNORDERED,
                                                           consistency,
                                                           expRowCnt);
        startTallyThroughput();

        MultiGetResult<KeyPair> mgRes;
        byte[] continuationKey = null;
        while(true) {
            mgRes = tableImpl.multiGetKeys(fnameIdxKey, continuationKey,
            		                       mro, tio);
            actRowCnt += mgRes.getResult().size();
            if (mgRes.getContinuationKey() == null) {
                break;
            }
            continuationKey = mgRes.getContinuationKey();
        }

        assertEquals("Incorrect rows returned by tableIterator",
                     expRowCnt,
                     actRowCnt);

        int expectedWriteKB = 0;

        int expectedReadKB  = isFullTableScan ?
                        idxSizeKB : isFieldRange ?
                            idxSizeKB/10 : isRowPresent ?
                                idxSizeKB/nRows : minRKB;
        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testbulkGetTableListIteratorPKey(Consistency consistency,
                                                  boolean isRowPresent,
                                                  boolean isFieldRange,
                                                  boolean isNestedTable)
        throws Exception {

        Row             row;
        int             nRows     = 100;
        int             rowSizeKB = 0;
        TableImpl       table     = buildUserTable(store);
        TableImpl       tablec    = null;
        TableImpl       tablegc   = null;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        setTableLimits(table,
                       new TableLimits(1000, 1000,
                                       NO_CHANGE, NO_CHANGE,
                                       1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testtableIteratorListPKeySet1", i, rsF);
            tableImpl.put(row, null, null);
        }

        if(isRowPresent) {
            for(int i=0; i<nRows; i++) {
                row = createRow(table, "testtableIteratorListPKeySet2", i, rsF);
                tableImpl.put(row, null, null);
                rowSizeKB += getRowSizeKB(row);
            }

            if(isNestedTable) {
                tablec  = buildUserChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowc = createRowChild(tablec,
                                              "testtableIteratorListPKeySet2",
                                              i);
                    tableImpl.put(rowc, null, null);
                    rowSizeKB += getRowSizeKB(rowc);
                }

                tablegc = buildUserGrandChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowgc = createRowGrandChild(tablegc,
                                                    "testtableIteratorListPKeySet2",
                                                    i);
                    tableImpl.put(rowgc, null, null);
                    rowSizeKB += getRowSizeKB(rowgc);
                }

                table  = getTable("User");
                tablec = getTable("User.Child");
            }
        }

        final PrimaryKey pkey        = table.createPrimaryKey();
        final PrimaryKey pkey2       = table.createPrimaryKey();
        final List<PrimaryKey> PKeys = new ArrayList<PrimaryKey>();
        final List<Iterator<PrimaryKey>> keyIterators =
            new ArrayList<Iterator<PrimaryKey>>(2);
        pkey.put("lastName", "lasttesttableIteratorListPKeySet2");
        pkey2.put("lastName", "lasttesttableIteratorListPKeySet2");
        // Add the retrieval keys to the list.
        PKeys.add(pkey);
        PKeys.add(pkey2);
        keyIterators.add(PKeys.iterator());

        if(isFieldRange) {
            fr = table.createFieldRange("firstName");
            fr.setStart("firsttesttableIteratorListPKeySet290", true);
            fr.setEnd("firsttesttableIteratorListPKeySet299", true);
            mro = fr.createMultiRowOptions();

            if(isNestedTable) {
                mro.setIncludedChildTables(Arrays.asList(tablec, tablegc));
            }
        }

        int expRowCnt = isNestedTable ?
                                (nRows/10)*3 : isFieldRange ?
                                    nRows/10 : isRowPresent ?
                                        nRows : 0;
        int actRowCnt = 0;

        TableIteratorOptions tio = getTableIteratorOptions(Direction.UNORDERED,
                                                           consistency,
                                                           expRowCnt);

        startTallyThroughput();

        TableIterator<Row>   titer = tableImpl.tableIterator(
                                             keyIterators,
                                             mro,
                                             tio);
        try {
            while(titer.hasNext()) {
                  actRowCnt++;
                  titer.next();
            }
        } finally {
              if(titer != null) {
                 titer.close();
              }
        }
        assertEquals("Incorrect rows returned by tableIterator",
                     expRowCnt,
                     actRowCnt);

        int expectedWriteKB = 0;
        int expectedReadKB  = isNestedTable ?
                                      rowSizeKB/10 : isFieldRange ?
                                          rowSizeKB/10 : isRowPresent ?
                                              rowSizeKB : minRKB;
        if (isRowPresent) {
            expectedReadKB += expRowCnt * minRKB;  /* read keys */
        }
        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testbulkGetTableIteratorPKey(Consistency consistency,
                                              boolean isRowPresent,
                                              boolean isFieldRange,
                                              boolean isNestedTable)
        throws Exception {

        Row             row;
        int             nRows     = 100;
        int             rowSizeKB = 0;
        TableImpl       table     = buildUserTable(store);
        TableImpl       tablec    = null;
        TableImpl       tablegc   = null;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        setTableLimits(table,
                       new TableLimits(1000, 1000,
                                       NO_CHANGE, NO_CHANGE,
                                       1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testtableIteratorPKeySet1", i, rsF);
            tableImpl.put(row, null, null);
        }

        if(isRowPresent) {
            for(int i=0; i<nRows; i++) {
                row = createRow(table, "testtableIteratorPKeySet2", i, rsF);
                tableImpl.put(row, null, null);
                rowSizeKB += getRowSizeKB(row);
            }

            if(isNestedTable) {
                tablec  = buildUserChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowc = createRowChild(tablec,
                                              "testtableIteratorPKeySet2",
                                              i);
                    tableImpl.put(rowc, null, null);
                    rowSizeKB += getRowSizeKB(rowc);
                }

                tablegc = buildUserGrandChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowgc = createRowGrandChild(tablegc,
                                                    "testtableIteratorPKeySet2",
                                                    i);
                    tableImpl.put(rowgc, null, null);
                    rowSizeKB += getRowSizeKB(rowgc);
                }

                table = getTable("User");
                tablec = getTable("User.Child");
            }
        }

        final PrimaryKey pkey        = table.createPrimaryKey();
        final PrimaryKey pkey2       = table.createPrimaryKey();
        final List<PrimaryKey> PKeys = new ArrayList<PrimaryKey>(2);

        pkey.put("lastName", "lasttesttableIteratorPKeySet2");
        pkey2.put("lastName", "lasttesttableIteratorPKeySet2");
        // Add the retrieval keys to the list.
        PKeys.add(pkey);
        PKeys.add(pkey2);
        final Iterator<PrimaryKey> keyIterator = PKeys.iterator();

        if(isFieldRange) {
            fr = table.createFieldRange("firstName");
            fr.setStart("firsttesttableIteratorPKeySet290", true);
            fr.setEnd("firsttesttableIteratorPKeySet299", true);
            mro = fr.createMultiRowOptions();

            if(isNestedTable) {
                mro.setIncludedChildTables(Arrays.asList(tablec, tablegc));
            }
        }

        int expRowCnt = isNestedTable ?
                                (nRows/10)*3 : isFieldRange ?
                                    nRows/10 : isRowPresent ?
                                        nRows : 0;
        int actRowCnt = 0;

        TableIteratorOptions tio = getTableIteratorOptions(Direction.UNORDERED,
                                                           consistency,
                                                           expRowCnt);
        startTallyThroughput();

        TableIterator<Row>  titer = tableImpl.tableIterator(
                                            keyIterator,
                                            mro,
                                            tio);
        try {
            while(titer.hasNext()) {
                  actRowCnt++;
                  titer.next();
            }
        } finally {
              if(titer != null) {
                 titer.close();
              }
        }
        assertEquals("Incorrect rows returned by tableIterator",
                     expRowCnt,
                     actRowCnt);

        int expectedWriteKB = 0;
        int expectedReadKB  = isNestedTable ?
                                      rowSizeKB/10 : isFieldRange ?
                                          rowSizeKB/10 : isRowPresent ?
                                              rowSizeKB : minRKB;
        if (isRowPresent) {
            expectedReadKB += expRowCnt * minRKB ;  /* read keys */
        }
        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testbulkGetTableKeysIteratorPKey(Consistency consistency,
                                                  boolean isRowPresent,
                                                  boolean isFieldRange,
                                                  boolean isNestedTable)
        throws Exception {

        Row             row;
        int             nRows     = 100;
        TableImpl       table     = buildUserTable(store);
        TableImpl       tablec    = null;
        TableImpl       tablegc   = null;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        setTableLimits(table,
                       new TableLimits(1000, 1000,
                                       NO_CHANGE, NO_CHANGE,
                                       1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testtableKeysIteratorPKeySet1", i, rsF);
            tableImpl.put(row, null, null);
        }

        if(isRowPresent) {
            for(int i=0; i<nRows; i++) {
                row = createRow(table, "testtableKeysIteratorPKeySet2", i, rsF);
                tableImpl.put(row, null, null);
            }

            if(isNestedTable) {
                tablec  = buildUserChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowc = createRowChild(tablec,
                                              "testtableKeysIteratorPKeySet2",
                                              i);
                    tableImpl.put(rowc, null, null);
                }

                tablegc = buildUserGrandChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowgc =
                        createRowGrandChild(tablegc,
                                            "testtableKeysIteratorPKeySet2",
                                                    i);
                    tableImpl.put(rowgc, null, null);
                }

                table = getTable("User");
                tablec = getTable("User.Child");
            }
        }

        final PrimaryKey pkey        = table.createPrimaryKey();
        final PrimaryKey pkey2       = table.createPrimaryKey();
        final List<PrimaryKey> PKeys = new ArrayList<PrimaryKey>(2);

        pkey.put("lastName", "lasttesttableKeysIteratorPKeySet2");
        pkey2.put("lastName", "lasttesttableKeysIteratorPKeySet2");
        // Add the retrieval keys to the list.
        PKeys.add(pkey);
        PKeys.add(pkey2);
        final Iterator<PrimaryKey> keyIterator = PKeys.iterator();

        if(isFieldRange) {
            fr = table.createFieldRange("firstName");
            fr.setStart("firsttesttableKeysIteratorPKeySet290", true);
            fr.setEnd("firsttesttableKeysIteratorPKeySet299", true);
            mro = fr.createMultiRowOptions();

            if(isNestedTable) {
                mro.setIncludedChildTables(Arrays.asList(tablec, tablegc));
            }
        }

        int expRowCnt = isNestedTable ?
                                (nRows/10)*3 : isFieldRange ?
                                    nRows/10 : isRowPresent ?
                                        nRows : 0;
        int actRowCnt = 0;
        TableIteratorOptions tio = getTableIteratorOptions(Direction.UNORDERED,
                                                           consistency,
                                                           expRowCnt);

        startTallyThroughput();

        TableIterator <PrimaryKey> titer = tableImpl.tableKeysIterator(
                                                   keyIterator,
                                                   mro,
                                                   tio);
        try {
            while(titer.hasNext()) {
                  actRowCnt++;
                  titer.next();
            }
        } finally {
              if(titer != null) {
                 titer.close();
              }
        }
        assertEquals("Incorrect rows returned by tableIterator",
                     expRowCnt,
                     actRowCnt);

        int expectedWriteKB = 0;
        int expectedReadKB = (expRowCnt > 0) ? expRowCnt * minRKB : minRKB;
        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testbulkGetTableKeysListIteratorPKey(Consistency consistency,
                                                      boolean isRowPresent,
                                                      boolean isFieldRange,
                                                      boolean isNestedTable)
        throws Exception {

        Row             row;
        int             nRows     = 100;
        TableImpl       table     = buildUserTable(store);
        TableImpl       tablec    = null;
        TableImpl       tablegc   = null;
        FieldRange      fr        = null;
        MultiRowOptions mro       = null;

        setTableLimits(table,
                       new TableLimits(1000, 1000,
                                       NO_CHANGE, NO_CHANGE,
                                       1000, NO_CHANGE));

        for(int i=0; i<nRows; i++) {
            row = createRow(table, "testtableKeysListIteratorPKeySet1", i, rsF);
            tableImpl.put(row, null, null);
        }

        if(isRowPresent) {
            for(int i=0; i<nRows; i++) {
                row = createRow(table,
                                "testtableKeysListIteratorPKeySet2",
                                i,
                                rsF);
                tableImpl.put(row, null, null);
            }

            if(isNestedTable) {
                tablec  = buildUserChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowc =
                        createRowChild(tablec,
                                       "testtableKeysListIteratorPKeySet2",
                                       i);
                    tableImpl.put(rowc, null, null);
                }

                tablegc = buildUserGrandChildTable(store);
                for(int i=0; i<nRows; i++) {
                    Row rowgc =
                        createRowGrandChild(tablegc,
                                            "testtableKeysListIteratorPKeySet2",
                                                    i);
                    tableImpl.put(rowgc, null, null);
                }

                table = getTable("User");
                tablec = getTable("User.Child");
            }
        }

        final PrimaryKey pkey  = table.createPrimaryKey();
        final PrimaryKey pkey2 = table.createPrimaryKey();
        final List<PrimaryKey> PKeys = new ArrayList<PrimaryKey>();
        final List<Iterator<PrimaryKey>> keyIterators =
            new ArrayList<Iterator<PrimaryKey>>(2);
        pkey.put("lastName", "lasttesttableKeysListIteratorPKeySet2");
        pkey2.put("lastName", "lasttesttableKeysListIteratorPKeySet2");
        // Add the retrieval keys to the list.
        PKeys.add(pkey);
        PKeys.add(pkey2);
        keyIterators.add(PKeys.iterator());

        if(isFieldRange) {
            fr = table.createFieldRange("firstName");
            fr.setStart("firsttesttableKeysListIteratorPKeySet290", true);
            fr.setEnd("firsttesttableKeysListIteratorPKeySet299", true);
            mro = fr.createMultiRowOptions();

            if(isNestedTable) {
                mro.setIncludedChildTables(Arrays.asList(tablec, tablegc));
            }
        }

        int expRowCnt = isNestedTable ?
                                (nRows/10)*3 : isFieldRange ?
                                    nRows/10 : isRowPresent ?
                                        nRows : 0;
        int actRowCnt = 0;

        TableIteratorOptions tio = getTableIteratorOptions(Direction.UNORDERED,
                                                           consistency,
                                                           expRowCnt);

        startTallyThroughput();

        TableIterator <PrimaryKey> titer = tableImpl.tableKeysIterator(
                                                   keyIterators,
                                                   mro,
                                                   tio);
        try {
            while(titer.hasNext()) {
                  actRowCnt++;
                  titer.next();
            }
        } finally {
              if(titer != null) {
                 titer.close();
              }
        }
        assertEquals("Incorrect rows returned by tableIterator",
                     expRowCnt,
                     actRowCnt);

        int expectedWriteKB = 0;

        int expectedReadKB = (expRowCnt > 0) ? expRowCnt * minRKB : minRKB;
        expectedReadKB      = consistency == Consistency.ABSOLUTE ?
                                  expectedReadKB*2 :
                                  expectedReadKB;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testput(Choice choice, boolean isRowPresent) throws Exception {
        KVStoreConfig kvconfig  = createKVConfig(createStore);

        store = KVStoreFactory.getStore(kvconfig);
        tableImpl = (TableAPIImpl) store.getTableAPI();

        final TableImpl table = buildUserTable(store);

        setTableLimits(table, new TableLimits(1000, 1000, NO_CHANGE));

        final int existingRowKB;
        if (isRowPresent) {
            final Row row = createRow(table, "testput", 1, rsF);
            tableImpl.put(row, null, null);
            existingRowKB = getRowSizeKB(row);
        } else {
            existingRowKB = 0;
        }

        final Row row = createRow(table, "testput", 1, rsF * 2);
        final ReturnRow rr = table.createReturnRow(choice);

        startTallyThroughput();

        tableImpl.put(row, rr, null);
        final int rowSizeKB = getRowSizeKB(row);
        final int idxSizeKB = isRowPresent ? 0 : getIndexSizeKB(table, row);

        int expectedWriteKB = existingRowKB + rowSizeKB + idxSizeKB;
        int expectedReadKB  = !isRowPresent ? 0 : choice.needValue() ?
                                  existingRowKB * 2 : choice.needVersion() ?
                                      minRKB * 2 : 0;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testputIfVersion(Choice choice,
                                  boolean isRowPresent) throws Exception {
        Row           row;
        ReturnRow     rr;
        KVStoreConfig kvconfig  = createKVConfig(createStore);
        int           rowSizeKB = 0;

        store = KVStoreFactory.getStore(kvconfig);
        tableImpl = (TableAPIImpl) store.getTableAPI();

        final TableImpl table = buildUserTable(store);

        setTableLimits(table, new TableLimits(1000, 1000, NO_CHANGE));

        row = createRow(table, "testputIfVersion", 1, rsF);

        rr  = table.createReturnRow(choice);

        Version v1 = tableImpl.put(row, rr, null);

        // Bump the version
        final Version v2 = tableImpl.put(row, rr, null);

        startTallyThroughput();

        if(!isRowPresent) {
            // This should fail because the version has changed
            Version v = tableImpl.putIfVersion(row, v1, rr, null);
            assertNull(v);
            rowSizeKB = getRowSizeKB(row);
        }
        else {
            // This should work
            Version v = tableImpl.putIfVersion(row, v2, rr, null);
            assertNotNull(v);
            rowSizeKB = getRowSizeKB(row);
        }

        int expectedWriteKB = isRowPresent ? (rowSizeKB + rowSizeKB) : 0;
        /*
         * For putIfVersion op, the previous row will only be initialized if
         * the operation fails with a version mismatch.
         */
        int expectedReadKB  = isRowPresent ?
                                  minRKB : choice.needValue() ?
                                      rowSizeKB : minRKB;
        expectedReadKB      *= 2;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testputIfPresent(Choice choice,
                                  boolean isRowPresent) throws Exception {
        Row           row;
        ReturnRow     rr;
        Version       version;
        KVStoreConfig kvconfig  = createKVConfig(createStore);
        int           rowSizeKB = 0;

        store = KVStoreFactory.getStore(kvconfig);
        tableImpl = (TableAPIImpl) store.getTableAPI();

        final TableImpl table = buildUserTable(store);

        setTableLimits(table,
                       new TableLimits(1000, 1000,
                                       NO_CHANGE, NO_CHANGE,
                                       NO_CHANGE, NO_CHANGE));

        row = createRow(table, "testputIfPresent", 1, rsF);

        if (isRowPresent) {
            row = createRow(table, "testputIfPresent", 2, rsF);
            tableImpl.put(row, null, null);
            rowSizeKB = getRowSizeKB(row);
        }

        rr = table.createReturnRow(choice);

        startTallyThroughput();

        version = tableImpl.putIfPresent(row, rr, null);
        if (isRowPresent) {
            assertNotNull(version);
        } else {
            assertNull(version);
            rowSizeKB = getRowSizeKB(row);
        }

        int expectedWriteKB = isRowPresent ? (rowSizeKB + rowSizeKB) : 0;
        int expectedReadKB  = !isRowPresent ?
                                  minRKB : choice.needValue() ?
                                      rowSizeKB : minRKB;
        expectedReadKB      *= 2;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testputIfAbsent(Choice choice,
                                 boolean isRowPresent) throws Exception {
        Row           row;
        ReturnRow     rr;
        Version       version;
        KVStoreConfig kvconfig  = createKVConfig(createStore);
        int           rowSizeKB = 0;
        int           idxSizeKB = 0;

        store = KVStoreFactory.getStore(kvconfig);
        tableImpl = (TableAPIImpl) store.getTableAPI();

        final TableImpl table = buildUserTable(store);

        setTableLimits(table, new TableLimits(1000, 1000, NO_CHANGE));

        row = createRow(table, "testputIfAbsent", 1, rsF);

        if(isRowPresent) {
            row = createRow(table, "testputIfAbsent", 2, rsF);
            tableImpl.put(row, null, null);
            rowSizeKB = getRowSizeKB(row);
            idxSizeKB = getIndexSizeKB(table, row);
        }

        rr = table.createReturnRow(choice);

        startTallyThroughput();

        version = tableImpl.putIfAbsent(row, rr, null);
        if (isRowPresent) {
            assertNull(version);
        } else {
            assertNotNull(version);
            rowSizeKB = getRowSizeKB(row);
            idxSizeKB = getIndexSizeKB(table, row);
        }

        int expectedWriteKB = isRowPresent ? 0 : (rowSizeKB+idxSizeKB);
        int expectedReadKB  = !isRowPresent ?
                                  minRKB : choice.needValue() ?
                                      rowSizeKB : minRKB;
        expectedReadKB      *= 2;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private void testputBatch(boolean isRowPresent) throws Exception {
        Row    row;
        KVStoreConfig   kvconfig      = createKVConfig(createStore);
        int rowSizeKB                 = 0;
        int idxSizeKB                 = 0;
        int nRows                     = 100;

        final int streamParallelism   = 1;
        final int perShardParallelism = 2;
        final int heapPercent         = 70;
        final TableImpl table         = buildUserTable(store);

        final BulkWriteOptions bulkWriteOptions;

        store     = KVStoreFactory.getStore(kvconfig);
        tableImpl = (TableAPIImpl) store.getTableAPI();

        // setting the durability
        Durability defaultDurability =
            new Durability(Durability.SyncPolicy.NO_SYNC, // Mastersync
        Durability.SyncPolicy.NO_SYNC, // Replica sync
        Durability.ReplicaAckPolicy.ALL);
        // initializing the BulkWriteOptions with durability
        bulkWriteOptions = new BulkWriteOptions(defaultDurability, 0, null);
        // set the other bulkWriteOptions <streamParallelism,
        // perShardParallelism, heapPercent>
        bulkWriteOptions.setStreamParallelism(streamParallelism);
        bulkWriteOptions.setPerShardParallelism(perShardParallelism);
        bulkWriteOptions.setBulkHeapPercent(heapPercent);

        final List<EntryStream<Row>> streams =
            new ArrayList<EntryStream<Row>>(streamParallelism);

        setTableLimits(table, new TableLimits(1000, 1000, NO_CHANGE));

        for (int i = 1; i <= nRows; i++) {
            row = createRow(table, "testputbatchSet1", i,rsF);
            tableImpl.put(row, null, null);
            rowSizeKB += getRowSizeKB(row);
            idxSizeKB += getIndexSizeKB(table, row);
        }

        if (isRowPresent) {
            for (int i = 1; i <= nRows; i++) {
                 row = createRow(table, "testputbatchSet2", i, rsF);
                 tableImpl.put(row, null, null);
                 rowSizeKB += getRowSizeKB(row);
                 idxSizeKB += getIndexSizeKB(table, row);
            }
        }

        for (int i = 1; i <= nRows; i++) {
            streams.add(new Stream(table, "testputbatchSet2", i, rsF));
        }

        startTallyThroughput();

        tableImpl.put(streams, bulkWriteOptions);

        int expectedWriteKB = isRowPresent ? 0 : (rowSizeKB + idxSizeKB);
        int expectedReadKB  = minRKB;
        expectedReadKB      *= 2;

        checkResult(expectedReadKB, expectedWriteKB);
    }

    private TableImpl buildUserTable(KVStore kvstore)
        throws Exception {

        String tableDdl = "create table if not exists User(firstName string, lastName string, age integer, address string, binary binary, primary key(shard(lastName),firstName))";
        executeDdl(tableDdl, null, true);

        String indexDdl = "create index if not exists FirstName on User(firstName)";
        executeDdl(indexDdl, null, true);
        indexDdl = "create index if not exists Age on User(age)";
        executeDdl(indexDdl, null, true);

        TableAPI tableapiH = kvstore.getTableAPI();
        return (TableImpl)tableapiH.getTable("User");
    }

    private TableImpl buildUserChildTable(KVStore kvstore)
        throws Exception {

        String tableDdl = "create table if not exists User.Child(childpk integer, primary key(childpk))";
        executeDdl(tableDdl, null, true);

        TableAPI tableapiH = kvstore.getTableAPI();
        return (TableImpl)tableapiH.getTable("User.Child");
    }

    private TableImpl buildUserGrandChildTable(KVStore kvstore)
        throws Exception {

        String tableDdl = "create table if not exists User.Child.Grandchild(grandchildpk integer, primary key(grandchildpk))";
        executeDdl(tableDdl, null, true);

        TableAPI tableapiH = kvstore.getTableAPI();
        return (TableImpl)tableapiH.getTable("User.Child.Grandchild");
    }

    private Row createRow(TableImpl table,
                          String keySfx,
                          int pkeySfx,
                          float nKB) {
        assert nKB > 0;
        final Row row = table.createRow();
        row.put("firstName", ("first" + keySfx + pkeySfx));
        row.put("lastName", ("last" + keySfx));
        row.put("age", 10);
        row.put("address", "10 Happy Lane");
        int size = (int)(1024*(nKB-1));
        row.put("binary", new byte[size]);
        return row;
    }

    private Row createRowChild(TableImpl table,
                               String keySfx,
                               int pkeySfx) {
        final Row row = table.createRow();
        row.put("firstName", ("first" + keySfx + pkeySfx));
        row.put("lastName", ("last" + keySfx));
        row.put("childpk", pkeySfx);
        return row;
    }

    private Row createRowGrandChild(TableImpl table,
                                    String keySfx,
                                    int pkeySfx) {
        final Row row = table.createRow();
        row.put("firstName", ("first" + keySfx + pkeySfx));
        row.put("lastName", ("last" + keySfx));
        row.put("childpk", pkeySfx);
        row.put("grandchildpk", pkeySfx);
        return row;
    }

    /**
     * Checks the read and write KB returned in the last operation.
     */
    private void checkResult(int expectedReadKB, int expectedWriteKB) {

        final int actualReadKB  = executeRequestHook.getReadKB();
        final int actualWriteKB = executeRequestHook.getWriteKB();

        if(expectedReadKB  != actualReadKB ||
           expectedWriteKB != actualWriteKB) {
            fail("readKB expected:<" + expectedReadKB + "> but was:<" + actualReadKB + ">, writeKB expected:<" + expectedWriteKB + "> but was:<" + actualWriteKB + ">");
        }

        if(actualReadKB == 0 && actualWriteKB == 0) {
            fail("No operation should return with both ReadKB & WriteKB == 0");
        }
    }

    private int getRowSizeKB(Row row) {
        int rowSizeBytes = TableUtils.getKeySize(row) +
                           TableUtils.getDataSize(row) +
                           64;
        int rowSizeKB    = (int)Math.ceil(rowSizeBytes/1024.0);
        return rowSizeKB;
    }

    private int getIndexSizeKB(TableImpl table, Row row) {
        Map<String,Index> idxMap       = table.getIndexes();
        Set<?>            idxSet       = idxMap.entrySet();
        Iterator<?>       itr          = idxSet.iterator();
        int               idxSizeBytes = 0;
        int               idxSizeKB    = 0;

        while(itr.hasNext()) {
            Map.Entry<?, ?> idxMapE = (Map.Entry<?, ?>)itr.next();
            String          idxName = (String)idxMapE.getKey();
            Index           idx     = (Index)idxMapE.getValue();
            IndexKey        idxKey  = idx.createIndexKey();

            if(idxName.equals("FirstName")) {
                idxKey.put("firstName", row.get("firstName").asString().get());
            }
            else if(idxName.equals("Age")) {
                idxKey.put("age", row.get("age").asInteger().get());
            }

            idxSizeBytes = TableUtils.getKeySize(row) +
                           TableUtils.getKeySize(idxKey) +
                           12;
            idxSizeKB += (int)Math.ceil(idxSizeBytes/1024.0);
        }

        return idxSizeKB;
    }

    private int getIndexSizeKB(TableImpl table, Row row, String idxName) {
        int      idxSizeBytes = 0;
        int      idxSizeKB    = 0;
        Index    idx          = table.getIndex(idxName);
        IndexKey idxKey       = idx.createIndexKey();

        idxKey.put("firstName", row.get("firstName").asString().get());
        idxSizeBytes = TableUtils.getKeySize(row) +
                       TableUtils.getKeySize(idxKey) +
                       12;
        idxSizeKB    = (int)Math.ceil(idxSizeBytes/1024.0);
        return idxSizeKB;
    }

    private void startTallyThroughput() {
        executeRequestHook.reset();
        tableImpl.getStore().setExecuteRequestHook(executeRequestHook);
    }

    private TableIteratorOptions getTableIteratorOptions(Direction direction,
                                                         Consistency consistency,
                                                         int expRowCnt) {
        if (expRowCnt < 100) {
            return new TableIteratorOptions(direction,
                                            consistency,
                                            0,
                                            null);
        }

        /*
         * Configure the TableIteratorOptions.batchResultsSize to expRowCnt + 1
         * to make sure all rows are fetched in one batch.
         */
        return new TableIteratorOptions(direction,
                                        consistency,
                                        0,
                                        null,
                                        0,
                                        expRowCnt + 1);
    }

    @Override
    public void tearDown() throws Exception {
        tableImpl.getStore().setExecuteRequestHook(null);
        executeDdl("drop table if exists User.Child.Grandchild", null, true);
        executeDdl("drop table if exists User.Child", null, true);
        executeDdl("drop table if exists User", null, true);
    }

    class Stream implements EntryStream<Row> {
        final TableImpl    table;
        private final int        pkeySfx;
        private final String     keySfx;
        private final float      nKB;
        private final AtomicLong keyExistsCount;
        private long       count;
        private final int        nRows = 98;

        public Stream(TableImpl table, String string, int i, float f) {
            count++;
            this.table     = table;
            this.keySfx    = string;
            this.pkeySfx   = i;
            this.nKB       = f;
            count          = 0;

            keyExistsCount = new AtomicLong();
        }

        Row entryToString(Row row) {
            return row;
        }

        @Override
        public Row getNext() {
            if (count == nRows) {
                return null;
            }
            count++;
            return createRow(table, keySfx, pkeySfx, nKB);
        }

        @Override
        public void completed() {
        }

        @Override
        public void catchException(RuntimeException exception, Row row) {
            System.err.println(name() + " catch exception: " +
                               exception.getMessage() + " for entry: " +
                               entryToString(row)); throw exception;

        }

        /**
	     * @param arg0
	     */
        @Override
        public void keyExists(Row arg0) {
            keyExistsCount.incrementAndGet();
        }

        @Override
        public String name() {
             return count +  "- [" + count + "]";
        }

        public long getCount() {
            return count;
        }

        public long getKeyExistsCount() {
            return keyExistsCount.get();
        }

    }

    /**
     * The test hook is used to tally the readKB and writeKB during the
     * execution of operation.
     */
    private class ExecuteRequestHook implements TestHook<Result> {

        private AtomicInteger readKB = new AtomicInteger();
        private AtomicInteger writeKB = new AtomicInteger();

        @Override
        public void doHook(Result result) {
            readKB.addAndGet(result.getReadKB());
            writeKB.addAndGet(result.getWriteKB());
        }

        int getReadKB() {
            return readKB.get();
        }

        int getWriteKB() {
            return writeKB.get();
        }

        void reset() {
            readKB.set(0);
            writeKB.set(0);
        }
    }
}
