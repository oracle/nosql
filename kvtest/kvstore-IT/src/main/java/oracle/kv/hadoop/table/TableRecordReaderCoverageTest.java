/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.hadoop.table;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.apache.hadoop.mapred.JobConf;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests that provide test coverage for the <code>TableRecordReader</code>
 * and the <code>TableRecordReaderBase</code> classes from the
 * <code>oracle.kv.hadoop.table</code> package.
 */
public class TableRecordReaderCoverageTest
                 extends HadoopTableCoverageTestBase {
    /*
     * Special mocked objects for testing TableRecordReader initialization.
     * One mock is KVStore with corresponding mocked TableAPIImpl in which
     * the store references a null table. The other references a non-null
     * table.
     */
    private static KVStore mockedStoreNullTable;
    private static TableAPIImpl mockedTableApiNullTable;

    private static KVStore mockedStoreNonNullTable;
    private static TableAPIImpl mockedTableApiNonNullTable;

    /* Non-mocked TableRecorders to test. */
    private static TableRecordReader recRdrNullTable;
    private static TableRecordReader recRdrNonNullTable;

    private static TableRecordReader recRdrAllPartitions;
    private static TableRecordReader recRdrSinglePartition;
    private static TableRecordReader recRdrByIndex;
    private static TableRecordReader recRdrNativeAllPartitions;
    private static TableRecordReader recRdrNativeSinglePartition;
    private static TableRecordReader recRdrNativeShards;
    private static TableRecordReader recRdrDefaultQueryBy;

    /* For when the TableRecordReader has a null store. */
    private static TableRecordReader recRdrNullStore;

    /*
     * Special, non-mocked TableInputSplits used by different tests in this
     * class; created by HadoopTableIntegrationTest.createTableInputSplit.
     */
    private static TableInputSplit splitPrimaryAllPartitions;
    private static TableInputSplit splitPrimarySinglePartition;
    private static TableInputSplit splitQueryByIndex;
    private static TableInputSplit splitNativeAllPartitions;
    private static TableInputSplit splitNativeSinglePartition;
    private static TableInputSplit splitNativeShards;
    private static TableInputSplit splitDefaultQueryBy;

    private boolean deleteUserSecurityDir = false;

    @BeforeClass
    public static void staticSetUp() throws Exception {

        THIS_CLASS = TableRecordReaderCoverageTest.class;
        THIS_CLASS_NAME = THIS_CLASS.getSimpleName();
        HadoopTableCoverageTestBase.staticSetUp();

        /* Create special, non-default mocks for this test class. */
        mockedStoreNullTable = createMock(KVStore.class);
        mockedStoreNonNullTable = createMock(KVStore.class);

        mockedTableApiNullTable = createMock(TableAPIImpl.class);
        mockedTableApiNonNullTable = createMock(TableAPIImpl.class);

        /* Create special, concrete TableInputSplits for this test class. */
        splitPrimaryAllPartitions =
           HadoopTableIntegrationTest.createTableInputSplit(
              defaultTableJobConf,
              TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS);
        splitPrimarySinglePartition =
           HadoopTableIntegrationTest.createTableInputSplit(
              defaultTableJobConf,
              TableInputSplit.QUERY_BY_PRIMARY_SINGLE_PARTITION);
        splitQueryByIndex =
           HadoopTableIntegrationTest.createTableInputSplit(
              defaultTableJobConf, TableInputSplit.QUERY_BY_INDEX);
        splitNativeAllPartitions =
           HadoopTableIntegrationTest.createTableInputSplit(
              defaultTableJobConf,
              TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS);
        splitNativeSinglePartition =
           HadoopTableIntegrationTest.createTableInputSplit(
              defaultTableJobConf,
              TableInputSplit.QUERY_BY_ONQL_SINGLE_PARTITION);
        splitNativeShards =
           HadoopTableIntegrationTest.createTableInputSplit(
              defaultTableJobConf, TableInputSplit.QUERY_BY_ONQL_SHARDS);
        splitDefaultQueryBy =
           HadoopTableIntegrationTest.createTableInputSplit(
              defaultTableJobConf, -1);

        /* Create special, concrete TableRecordReaders for this test class. */
        recRdrNullTable =
            createTableRecordReader(mockedStoreNullTable, defaultTableJobConf);
        recRdrNonNullTable = createTableRecordReader(mockedStoreNonNullTable,
                                                     defaultTableJobConf);

        recRdrAllPartitions = createTableRecordReader(mockedStoreNonNullTable,
                                                      defaultTableJobConf);
        recRdrSinglePartition =
            createTableRecordReader(mockedStoreNonNullTable,
                                    defaultTableJobConf);
        recRdrByIndex = createTableRecordReader(mockedStoreNonNullTable,
                                                defaultTableJobConf);
        recRdrNativeAllPartitions =
            createTableRecordReader(mockedStoreNonNullTable,
                                    defaultTableJobConf);
        recRdrNativeSinglePartition =
            createTableRecordReader(mockedStoreNonNullTable,
                                    defaultTableJobConf);
        recRdrNativeShards = createTableRecordReader(mockedStoreNonNullTable,
                                                     defaultTableJobConf);
        recRdrDefaultQueryBy = createTableRecordReader(mockedStoreNonNullTable,
                                                       defaultTableJobConf);

        recRdrNullStore = createTableRecordReader(null, defaultTableJobConf);

        /* Set expectations for the mocks created by this class. */
        expect(mockedStoreNullTable.getTableAPI())
            .andReturn(mockedTableApiNullTable).anyTimes();
        expect(mockedTableApiNullTable.getTable(defaultTableName))
            .andReturn(HadoopTableCoverageTestBase.nullTable).anyTimes();

        expect(mockedStoreNonNullTable.getTableAPI())
            .andReturn(mockedTableApiNonNullTable).anyTimes();
        expect(mockedTableApiNonNullTable.getTable(defaultTableName))
            .andReturn(HadoopTableCoverageTestBase.defaultMockedTable)
                .anyTimes();

        /* Replay the mocks created by this class. */
        replay(mockedStoreNullTable);
        replay(mockedStoreNonNullTable);

        replay(mockedTableApiNullTable);
        replay(mockedTableApiNonNullTable);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        /*
         * If the user security directory does not exist before the test
         * runs, then check in the tearDown method to see if that directory
         * was created by the test and, of so, then delete that directory.
         */
        if (!(new File(TableRecordReaderBase.USER_SECURITY_DIR).exists())) {
            deleteUserSecurityDir = true;
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        final File userSecurityDirFd =
                       new File(TableRecordReaderBase.USER_SECURITY_DIR);
        if (deleteUserSecurityDir && userSecurityDirFd.exists()) {
            final String[] names = userSecurityDirFd.list();
            for (String name : names) {
                final File nameFd = new File(userSecurityDirFd, name);
                nameFd.delete();
            }
            userSecurityDirFd.delete();
        }
    }

    /*
     * Note that unchecked warnings are suppressed here to avoid warnings
     * produced by the calls to the EasyMock methods that take a Class.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testNextKeyValue() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final IndexKey indexKey = createMock(IndexKey.class);
        replay(indexKey);

        final int nPartitions = 7;
        final int partitionSize = 5;
        final int nShards = 5;

        /* 1. QUERY_BY_INDEX
         *    non-empty partitionSets (not used)
         *    non-empty mockedShardSet (used)
         *
         * Note: for this case (QUERY_BY_INDEX and non-empty shardSet),
         *       the shardSet must be mocked rather than a concrete Set
         *       from createShardSet(). This is because when nextKeyValue()
         *       is invoked on the tableRecordReader under test, the method
         *       TableRecordReaderBase.getNextIterator is ultimately called,
         *       which iterates over the shardSet input to the
         *       tableRecordReader. If a concrete shardSet is used by the
         *       tableRecordReader, then the mock framework will fail the
         *       test because of an 'unexpected call to tableIterator()',
         *       indicating an issue with that concrete shardSet. To
         *       handle this situation, a mockedShardset is used for this
         *       one case; where all other test cases for nextKeyValue()
         *       can use a concrete shardSet.
         */
        int queryBy = TableInputSplit.QUERY_BY_INDEX;
        List<Set<Integer>> partitionSets = createPartitionSets(
                                               nPartitions, partitionSize);

        final Set<RepGroupId> mockedShardSet = createMock(Set.class);
        final Iterator<RepGroupId> mockedShardSetIterator =
            createMock(Iterator.class);
        final RepGroupId mockedRepGroupId = createMock(RepGroupId.class);
        final KVStore mockedStore = createMock(KVStore.class);
        final TableAPIImpl mockedTableApiImpl = createMock(TableAPIImpl.class);
        /*
         * nextKeyValue() exits only when getNextIterator() returns a
         * null TableIterator.
         */
        final TableIterator<Row> nullTableIterator = null;

        expect(mockedShardSet.size()).andReturn(1).anyTimes();
        /*
         * TODO: change mockedShardSet.isEmpty() to return false if/when
         *       it becomes desirable to simulate/mock the behavior of
         *       the TableRecordReaderBase.getNextIterator() method to
         *       achieve more coverage; which is currently not necessary.
         */
        expect(mockedShardSet.isEmpty()).andReturn(false).anyTimes();
        expect(mockedShardSet.iterator())
            .andReturn(mockedShardSetIterator).anyTimes();
        mockedShardSet.clear();
        expectLastCall();

        expect(mockedShardSetIterator.hasNext())
            .andReturn(true).once();
        expect(mockedShardSetIterator.hasNext())
            .andReturn(false).once();

        expect(mockedShardSetIterator.next())
            .andReturn(mockedRepGroupId).once();

        expect(mockedStore.getTableAPI())
            .andReturn(mockedTableApiImpl).anyTimes();
        expect(mockedTableApiImpl
            .tableIterator(anyObject(IndexKey.class),
                           anyObject(MultiRowOptions.class),
                           anyObject(TableIteratorOptions.class),
                           anyObject(Set.class)))
                .andReturn(nullTableIterator).once();

        replay(mockedShardSet);
        replay(mockedShardSetIterator);
        replay(mockedRepGroupId);
        replay(mockedStore);
        replay(mockedTableApiImpl);

        TableRecordReader tableRecordReader =
            getInitializedTableRecordReader(methodName,
                                            defaultMockedPrimaryKey,
                                            indexKey,
                                            defaultPrimaryKeyProperty,
                                            defaultFieldRangeColName,
                                            defaultFieldRangeProperty,
                                            partitionSets,
                                            mockedShardSet,
                                            queryBy,
                                            defaultWhereClause);
        tableRecordReader.setIndexKey(indexKey);
        tableRecordReader.setStore(mockedStore);
        tableRecordReader.nextKeyValue();

        /* From this point on, can use concrete shardSet rather than mocked. */

        /* 2. QUERY_BY_INDEX
         *    non-empty partitionSets (not used), empty shardSet (used)
         */
        queryBy = TableInputSplit.QUERY_BY_INDEX;
        partitionSets = createPartitionSets(nPartitions, partitionSize);
        Set<RepGroupId> shardSet = createShardSet(0); /* empty shardSet */
        tableRecordReader =
            getInitializedTableRecordReader(methodName,
                                            defaultMockedPrimaryKey,
                                            indexKey,
                                            defaultPrimaryKeyProperty,
                                            defaultFieldRangeColName,
                                            defaultFieldRangeProperty,
                                            partitionSets,
                                            shardSet,
                                            queryBy,
                                            defaultWhereClause);
        tableRecordReader.setIndexKey(indexKey);
        tableRecordReader.nextKeyValue();

        /* 3. QUERY_BY_PRIMARY_ALL_PARTITIONS
         *    non-empty partitionSets (used), non-empty shardSet (not used)
         */
        queryBy = TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS;
        partitionSets = createPartitionSets(nPartitions, partitionSize);
        shardSet = createShardSet(nShards);
        tableRecordReader =
            getInitializedTableRecordReader(methodName,
                                            defaultMockedPrimaryKey,
                                            indexKey,
                                            defaultPrimaryKeyProperty,
                                            defaultFieldRangeColName,
                                            defaultFieldRangeProperty,
                                            partitionSets,
                                            shardSet,
                                            queryBy,
                                            defaultWhereClause);
        tableRecordReader.setPrimaryKey(defaultMockedPrimaryKey);
        tableRecordReader.nextKeyValue();

        /* 4. QUERY_BY_PRIMARY_SINGLE_PARTITION
         *    empty partitionSets (used), non-empty shardSet (not used)
         */
        queryBy = TableInputSplit.QUERY_BY_PRIMARY_SINGLE_PARTITION;
        partitionSets = createPartitionSets(0, 0); /* empty partitionSets */
        shardSet = createShardSet(nShards);
        tableRecordReader =
            getInitializedTableRecordReader(methodName,
                                            defaultMockedPrimaryKey,
                                            indexKey,
                                            defaultPrimaryKeyProperty,
                                            defaultFieldRangeColName,
                                            defaultFieldRangeProperty,
                                            partitionSets,
                                            shardSet,
                                            queryBy,
                                            defaultWhereClause);
        tableRecordReader.setPrimaryKey(defaultMockedPrimaryKey);
        tableRecordReader.nextKeyValue();

        /* 5. QUERY_BY_ONQL_ALL_PARTITIONS
         *    non-empty partitionSets (used), non-empty shardSet (not used)
         */
        queryBy = TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS;
        partitionSets = createPartitionSets(nPartitions, partitionSize);
        shardSet = createShardSet(nShards);
        tableRecordReader =
            getInitializedTableRecordReader(methodName,
                                            defaultMockedPrimaryKey,
                                            indexKey,
                                            defaultPrimaryKeyProperty,
                                            defaultFieldRangeColName,
                                            defaultFieldRangeProperty,
                                            partitionSets,
                                            shardSet,
                                            queryBy,
                                            defaultWhereClause);
        tableRecordReader.setPrimaryKey(defaultMockedPrimaryKey);
        try {


            tableRecordReader.nextKeyValue();
        } catch (Throwable t) {
            t.printStackTrace();
            fail(methodName + "failed. Unexpected exception encountered " +
                 "in TableRecordReader.nextKeyValue method: " + t);
        }

        /* 6. QUERY_BY_ONQL_SHARDS
         *    non-empty partitionSets (used), non-empty shardSet (not used)
         */
        queryBy = TableInputSplit.QUERY_BY_ONQL_SHARDS;
        partitionSets = createPartitionSets(nPartitions, partitionSize);
        shardSet = createShardSet(nShards);
        tableRecordReader =
            getInitializedTableRecordReader(methodName,
                                            defaultMockedPrimaryKey,
                                            indexKey,
                                            defaultPrimaryKeyProperty,
                                            defaultFieldRangeColName,
                                            defaultFieldRangeProperty,
                                            partitionSets,
                                            shardSet,
                                            queryBy,
                                            defaultWhereClause);
        tableRecordReader.setPrimaryKey(defaultMockedPrimaryKey);
        try {
            tableRecordReader.nextKeyValue();
        } catch (Throwable t) {
            t.printStackTrace();
            fail(methodName + "failed. Unexpected exception encountered " +
                 "in TableRecordReader.nextKeyValue method: " + t);
        }
    }

    /* --- Test that exercises all aspects of createFieldRange() method --- */

    /**
     * This test exercises the following code paths of the method,
     * TableRecordReaderBase.createFieldRange():
     *
     *  1. the code path that encounters an IllegalArgumentException when
     *     fieldRangeProperty is missing its leading left curly brace.
     *  2. the code path that encounters an IllegalArgumentException when
     *     fieldRangeProperty is missing its ending right curly brace.
     *  3. the code path that exercises the part of the getFieldRange()
     *     method of TableRecordReaderBase that handles null input for
     *     the rangeFieldProp argument; which allows null to be input without
     *     resulting in an exception.
     *  4. the code path that encounters an IllegalArgumentException when
     *     the fieldRangeProperty is missing one or more of the components
     *     that are required in that property to be valid ("name", "start",
     *     and "end").
     *  5. the code path that encounters an IllegalArgumentException when
     *     the non-required name/value components of the fieldRangeProperty
     *     are not separated by colons.
     *  6. the code path that encounters an IllegalArgumentException when
     *     the value specified for the "name" of the field over which the
     *     range will be created is not encapsulated by double quotes
     *     (is missing the left quote, the right quote, or both).
     *  7. the code path that encounters an IllegalArgumentException
     *     when the value specified for the "start" component of the
     *     fieldRangeProperty has a left quote, but not a right quote.
     *  8. the code path that encounters an IllegalArgumentException
     *     when the value specified for the "start" component of the
     *     fieldRangeProperty has a right quote, but not a left quote.
     *  9. the code path that exercises the part of the createFieldRange()
     *     method of TableRecordReaderBase that handles the value specified
     *     for the "start" component of the fieldRangeProperty when that
     *     value is correctly encapsulated by double quotes.
     * 10. the code path that exercises the part of the createFieldRange()
     *     method of TableRecordReaderBase that handles the value specified
     *     for the "start" component of the fieldRangeProperty when that
     *     value is not encapsulated by double quotes; which means it is
     *     will be interpretted as a scalar.
     * 11. the code path that encounters an IllegalArgumentException
     *     when the value specified for the "end" component of the
     *     fieldRangeProperty has a left quote, but not a right quote.
     * 12. the code path that encounters an IllegalArgumentException
     *     when the value specified for the "end" component of the
     *     fieldRangeProperty has a right quote, but not a left quote.
     * 13. the code path that exercises the part of the createFieldRange()
     *     method of TableRecordReaderBase that handles the value specified
     *     for the "end" component of the fieldRangeProperty when that
     *     value is correctly encapsulated by double quotes.
     * 14. the code path that exercises the part of the createFieldRange()
     *     method of TableRecordReaderBase that handles the value specified
     *     for the "end" component of the fieldRangeProperty when that
     *     value is not encapsulated by double quotes; which means it is
     *     will be interpretted as a scalar.
     * 15. the code path that exercises the part of the createFieldRange()
     *     method of TableRecordReaderBase that creates a FieldRange for
     *     a field of type FieldDef.Type.INTEGER (field named, 'count').
     * 16. the code path that exercises the part of the createFieldRange()
     *     method of TableRecordReaderBase that creates a FieldRange for
     *     a field of type FieldDef.Type.LONG (field named, 'lcount').
     * 17. the code path that exercises the part of the createFieldRange()
     *     method of TableRecordReaderBase that creates a FieldRange for
     *     a field of type FieldDef.Type.DOUBLE (field named, 'price').
     * 18. the code path that exercises the part of the createFieldRange()
     *     method of TableRecordReaderBase that creates a FieldRange for
     *     a field of type FieldDef.Type.FLOAT (field named, 'fprice').
     * 19. the code path that encounters an IllegalArgumentException
     *     when createFieldRange() attempts to create a FieldRange for
     *     a field of type FieldDef.Type.NUMBER (field named, 'dealerid').
     *
     * ***
     * *** TODO: update TableRecordReaderBase.createFieldRange to handle new
     * ***       data types; NUMBER, TIMESTAMP, JSON.
     * ***
     *
     * 20. the code path that encounters a NumberFormatException when
     *     the value specified for the "name" component of the
     *     fieldRangeProperty references a field that is a numerical
     *     type (ex. the 'count' field is of type FieldDef.Type.INTEGER),
     *     but the value specified for the "start" component to apply
     *     to the field is non-numerical.
     *
     *     -- NOTE --
     *
     * As described above, some of the code paths exercised by this test
     * result in an IllegalArgumentException or a NumberFormatException
     * thrown by createFieldRange. But note that those exceptions are
     * always caught, logged, and swallowed by the method that calls
     * createFieldRange; which is, TableRecordReaderBase.getFieldRange().
     * Thus, for all of the code paths, this test does not expect to
     * receive an exception. It merely sets the conditions that cause
     * the targetted code segment in createFieldRange to be exercised.
     * As a result, this method fails only if an unexpected exception
     * occurs.
     */
    @Test
    public void testCreateFieldRange() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final String primaryKeyProp = "{\"type\":\"truck\"}";

        final String[] fieldRangeColNames =
        { "model", "model", "model", "model", "model", "model", "model",
          "model", "model", "model", "model", "model", "model", "model",
          "count", "lcount", "price", "fprice", "dealerid", "count"
        };

        final String[] fieldRangeProps =
        {
            /* no leading left curly brace ==> IAE */
            "\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false}",

            /* no ending right curly brace ==> IAE */
            "{\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false",

            /* null fieldRangeProp ==> getFieldRange returns null (okay). */
            null,

            /* no "name" component ==> IAE */
            "{\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false}",

            /* no colon between names/value pairs ==> IAE */
            "{\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\"XX" +
            "true,\"end\":\"Sebring\",\"endInclusive\"XXfalse}",

            /* name: val not encapsulated by double quotes ==> IAE */
            "{\"name\":model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false}",

            /* start: val starts but not ends with double quotes ==> IAE */
            "{\"name\":\"model\",\"start\":\"Imperial,\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false}",

            /* start: val ends but not starts with double quotes ==> IAE */
            "{\"name\":\"model\",\"start\":Imperial\",\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false}",

            /* start: val encapsulated by double quotes ==> String (okay) */
            "{\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false}",

            /* start: val no double quotes ==> scalar (okay) */
            "{\"name\":\"model\",\"start\":Imperial,\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false}",

            /* end: val starts but not ends with double quotes ==> IAE */
            "{\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":\"Sebring,\"endInclusive\":false}",

            /* end: val ends but not starts with double quotes ==> IAE */
            "{\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":Sebring\",\"endInclusive\":false}",

            /* end: val encapsulated by double quotes ==> String (okay) */
            "{\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false}",

            /* end: val no double quotes ==> scalar (okay) */
            "{\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":Sebring,\"endInclusive\":false}",

            /* FieldDef.Type: INTEGER ==> okay */
            "{\"name\":\"count\",\"start\":33,\"startInclusive\":" +
            "true,\"end\":37,\"endInclusive\":true}",

            /* FieldDef.Type: LONG ==> okay */
            "{\"name\":\"lcount\",\"start\":55,\"startInclusive\":" +
            "true,\"end\":57,\"endInclusive\":true}",

            /* FieldDef.Type: DOUBLE ==> okay */
            "{\"name\":\"price\",\"start\":75.33,\"startInclusive\":" +
            "true,\"end\":77.97,\"endInclusive\":true}",

            /* FieldDef.Type: FLOAT ==> okay */
            "{\"name\":\"fprice\",\"start\":23.59,\"startInclusive\":" +
            "true,\"end\":27.95,\"endInclusive\":true}",

            /* FieldDef.Type: NUMBER ==> IAE */
            "{\"name\":\"dealerid\",\"start\":23456.19,\"startInclusive\":" +
            "true,\"end\":41173.999,\"endInclusive\":true}",

            /* FieldDef.Type: NOT-A-NUMBER ==> NumberFormatException */
            "{\"name\":\"count\",\"start\":countstart,\"startInclusive\":" +
            "true,\"end\":endcount,\"endInclusive\":true}"
        };

        /* # of fieldRangeColNames must equal # of fieldRangeProps */
        assertTrue(
            "# of fieldRangeColNames [" + fieldRangeColNames.length + "]" +
            " != # of fieldRangeProps [" + fieldRangeProps.length + "]",
            fieldRangeProps.length == fieldRangeColNames.length);

        for (int i = 0; i < fieldRangeColNames.length; i++) {
            final String fieldRangeColName = fieldRangeColNames[i];
            final String fieldRangeProp = fieldRangeProps[i];
            try {
                final TableRecordReader tableRecordReader =
                    getInitializedTableRecordReader(
                        methodName,
                        defaultMockedPrimaryKey,
                        defaultMockedIndexKey,
                        primaryKeyProp,
                        fieldRangeColName,
                        fieldRangeProp,
                        defaultPartitionSets,
                        defaultShardSet,
                        defaultQueryBy,
                        defaultWhereClause);

                assertTrue("null TableRecordReader returned by " +
                           "getInitializedTableRecordReader with " +
                           "fieldRange Column Name = " + fieldRangeColName +
                           " and fieldRange Property = " + fieldRangeProp,
                           tableRecordReader != null);

            } catch (Throwable t) {
                t.printStackTrace();
                fail(methodName + "failed. Unexpected exception encountered " +
                     "when initializing a TableRecordReader with a " +
                     "fieldRangeColName of '" + fieldRangeColName + "' and " +
                     "fieldRangeProp of '" + fieldRangeProp + "': " + t);
            }
        }
    }

    /* --- Tests to exercise the getters and setters as well as close --- */

    @Test
    public void testClose() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final String mockedTableName = methodName + "_mockedTableName";
        final KVStoreImpl mockedStore = createMock(KVStoreImpl.class);
        final TableAPIImpl mockedTableApiImpl = createMock(TableAPIImpl.class);
        final TableIterator<Row> mockedTableIterator =
            createMock(TableIterator.class);
        final StatementResult mockedStatementResult =
            createMock(StatementResult.class);

        final PrimaryKey mockedPrimaryKey = createMock(PrimaryKey.class);
        final IndexKey mockedIndexKey = createMock(IndexKey.class);
        final Row mockedRow = createMock(Row.class);

        final MultiRowOptions mockedMultiRowOptions =
            createMock(MultiRowOptions.class);
        final TableIteratorOptions mockedTableIteratorOptions =
            createMock(TableIteratorOptions.class);

        Set<Integer> partitionSet = null;
        if (defaultPartitionSets != null) {
            if (defaultPartitionSets.isEmpty()) {
                partitionSet = new HashSet<>();
            } else {
                partitionSet = defaultPartitionSets.get(0);
            }
        }
        final Table mockedTable =
            HadoopTableCoverageTestBase.createMockedVehicleTable(
                                            mockedStore,
                                            mockedTableName,
                                            mockedPrimaryKey,
                                            mockedIndexKey,
                                            defaultPrimaryKeyProperty,
                                            mockedTableApiImpl,
                                            mockedMultiRowOptions,
                                            mockedTableIteratorOptions,
                                            partitionSet,
                                            defaultShardSet,
                                            mockedTableIterator,
                                            mockedStatementResult,
                                            mockedRow,
                                            defaultFieldRangeColName);

        /* Create concrete, non-mocked objects. */
        final JobConf vehicleTableJobConf =
            HadoopTableCoverageTestBase.getTableJobConf(
                                            mockedTableName,
                                            defaultPrimaryKeyProperty,
                                            defaultFieldRangeProperty);
        final TableRecordReader tableRecordReader =
            createTableRecordReader(mockedStore, vehicleTableJobConf);

        /*
         * The close() is called on both the KVStore and the TableIterator
         * when the TableRecordReader under test is closed at the end of
         * of this test. In both cases, the close() method is void; which
         * means they have to be handled specially. To handle those void
         * methods on each mocked object, do the following:
         * 1. Call the close() method on the mocked object itself
         *    (not on the TableRecordReader being tested).
         * 2. Mark the close() method as the last call on the mocked object.
         * 3. Replay the mocked object.
         * 4. Call the close() method on the TableRecordReader being tested
         *    at the END of the test, which ultimately makes the last call
         *    to the close() method on both the mocked TableIterator
         *    and the mocked KVStore.
         */
        mockedStore.close();
        expectLastCall();
        replay(mockedStore);

        mockedTableIterator.close();
        expectLastCall();
        replay(mockedTableIterator);

        /* Create mocked TableInputSplit with all necessary expectations. */
        final TableInputSplit mockedTableInputSplit =
            createMockedTableInputSplit(mockedTableName,
                                        defaultPrimaryKeyProperty,
                                        defaultFieldRangeProperty,
                                        defaultPartitionSets,
                                        defaultShardSet,
                                        defaultQueryBy,
                                        defaultWhereClause);

        /* Place all other mocks in replay state. */
        replay(mockedTableIteratorOptions);
        replay(mockedMultiRowOptions);
        replay(mockedTable);
        replay(mockedIndexKey);
        replay(mockedStatementResult);
        replay(mockedRow);
        replay(mockedPrimaryKey);
        replay(mockedTableInputSplit);
        replay(mockedTableApiImpl);

        /* Exercise methods on the concrete TableRecordReader under test. */
        tableRecordReader.initialize(
                mockedTableInputSplit, defaultTableTaskAttemptContext);
        tableRecordReader.nextKeyValue();
        tableRecordReader.getProgress();
        tableRecordReader.setMultiRowOptions(defaultFieldRange);
        tableRecordReader.setPrimaryKey(mockedPrimaryKey);
        tableRecordReader.setIndexKey(mockedIndexKey);

        final String queryStr = "make = 'Chrysler";

        /* Cover different branches in setQueryInfo. */
        tableRecordReader.setQueryInfo(
            TableInputSplit.QUERY_BY_PRIMARY_SINGLE_PARTITION, queryStr);
        tableRecordReader.nextKeyValue();
        tableRecordReader.setQueryInfo(
            TableInputSplit.QUERY_BY_INDEX, queryStr);
        tableRecordReader.nextKeyValue();
        tableRecordReader.setQueryInfo(
            TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS, queryStr);
        tableRecordReader.nextKeyValue();
        tableRecordReader.setQueryInfo(
            TableInputSplit.QUERY_BY_ONQL_SINGLE_PARTITION, queryStr);
        tableRecordReader.nextKeyValue();
        tableRecordReader.setQueryInfo(
            TableInputSplit.QUERY_BY_ONQL_SHARDS, queryStr);
        tableRecordReader.nextKeyValue();

        /* Make the expected last call to the close() method on the store. */
        tableRecordReader.close();
    }

    /**
     * Exercises initialize if-block that handles null kvstore in the
     * record reader.
     */
    @Test
    public void testGetKvTable() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        recRdrNonNullTable.initialize(
                defaultTableInputSplit, defaultTableTaskAttemptContext);
        final Table table = recRdrNonNullTable.getKvTable();
        assertNotNull(table);
    }

    @Test
    public void testGetPrimaryKey() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final String[] primaryKeyProps =
        {
            "{\"type\":\"truck\"}", /* valid JSON ==> okay */
            "type\"XX\"truck\"}\"", /* invalid JSON ==> IAE */
            null /* null ==> wildcard retKey */
        };
        final String fieldRangeColName = "model";
        final String fieldRangeProp =
            "{\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
            "true,\"end\":\"Sebring\",\"endInclusive\":false}";

        for (int i = 0; i < primaryKeyProps.length; i++) {
            final String primaryKeyProp = primaryKeyProps[i];
            try {
                final TableRecordReader tableRecordReader =
                    getInitializedTableRecordReader(
                        methodName,
                        defaultMockedPrimaryKey,
                        defaultMockedIndexKey,
                        primaryKeyProp,
                        fieldRangeColName,
                        fieldRangeProp,
                        defaultPartitionSets,
                        defaultShardSet,
                        defaultQueryBy,
                        defaultWhereClause);

                assertTrue("null TableRecordReader returned by " +
                           "getInitializedTableRecordReader with " +
                           "primaryKey Property = " + primaryKeyProp,
                           tableRecordReader != null);
            } catch (Throwable t) {
                t.printStackTrace();
                fail(methodName + "failed. Unexpected exception encountered " +
                     "when initializing a TableRecordReader with a " +
                     "primaryKeyProp of '" + primaryKeyProp + "': " + t);
            }
        }
    }

    @Test
    public void testGetProgress() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final int[] queryByArray =
            { TableInputSplit.QUERY_BY_INDEX,
              TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS };
        final int nBranches = 3; /* empty, non-empty, null */

        List<Set<Integer>> partitionSets = null;
        Set<RepGroupId> shardSet = null;

        /* Exercise each branch in the getProgress() method. */
        for (int i = 0; i < queryByArray.length; i++) {

            final int queryBy = queryByArray[i];

            if (i == TableInputSplit.QUERY_BY_INDEX) {

                partitionSets = defaultPartitionSets;
                for (int j = 0; j < nBranches; j++) {
                    if (j == 0) { /* empty shardSet */
                        shardSet = new HashSet<RepGroupId>();
                    } else if (j == 1) { /* non-empty shardSet */
                        shardSet = new HashSet<RepGroupId>();
                        for (int k = 0; k < 5; k++) {
                            shardSet.add(new RepGroupId(k));
                        }
                    } else { /* null shardSet */
                        shardSet = null;
                    }
                    final TableRecordReader tableRecordReader =
                        getInitializedTableRecordReader(
                            methodName,
                            defaultMockedPrimaryKey,
                            defaultMockedIndexKey,
                            defaultPrimaryKeyProperty,
                            defaultFieldRangeColName,
                            defaultFieldRangeProperty,
                            partitionSets,
                            shardSet,
                            queryBy,
                            defaultWhereClause);

                    final float progress = tableRecordReader.getProgress();

                    assertTrue("QUERY_BY_INDEX: " +
                               "expected 0.0 from getProgress, " +
                               " but returned the value " + progress,
                               progress == 0.0);
                }

            } else { /* i != TableInputSplit.QUERY_BY_INDEX */

                shardSet = defaultShardSet;
                for (int j = 0; j < nBranches; j++) {
                    if (j == 0) { /* empty partitionSets */

                        partitionSets = new ArrayList<Set<Integer>>();

                    } else if (j == 1) { /* non-empty partitionSets */

                        /* Add to partitionSets to make it non-empty */
                        partitionSets = new ArrayList<Set<Integer>>();
                        for (int m = 0; m < 7; m++) {
                            final Set<Integer> partitionSet = new HashSet<>();
                            for (int n = 0; n < 5; n++) {
                                partitionSet.add(n);
                            }
                            partitionSets.add(partitionSet);
                        }

                    } else { /* null partitionSets */
                        partitionSets = null;
                    }
                    final TableRecordReader tableRecordReader =
                        getInitializedTableRecordReader(
                            methodName,
                            defaultMockedPrimaryKey,
                            defaultMockedIndexKey,
                            defaultPrimaryKeyProperty,
                            defaultFieldRangeColName,
                            defaultFieldRangeProperty,
                            partitionSets,
                            shardSet,
                            queryBy,
                            defaultWhereClause);

                    final float progress = tableRecordReader.getProgress();

                    assertTrue("QUERY_BY_PRIMARY_ALL_PARTITIONS: " +
                               "expected 0.0 from getProgress, " +
                               " but returned the value " + progress,
                               progress == 0.0);
                }
            }
        }
    }

    /* --- Tests to exercise different parts of the initialize method --- */

    /**
     * Exercises initialize if-block that handles null kvstore in the
     * record reader.
     */
    @Test
    public void testInitializeNullStore() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        try {
            recRdrNullStore.initialize(
                defaultTableInputSplit, defaultTableTaskAttemptContext);
            fail("expected Exception because of null kvstore in the record " +
                 "reader but record reader contains a non-null kvstore");
        } catch (FaultException e) /* CHECKSTYLE:OFF */ {
        } catch (KVSecurityException e) {
        } /* CHECKSTYLE:ON */
    }

    /**
     * Exercises initialize if-block that handles null table returned by
     * TableApiImpl.getTable method.
     */
    @Test
    public void testInitializeNullTable() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        try {
            recRdrNullTable.initialize(
                defaultTableInputSplit, defaultTableTaskAttemptContext);
            fail("expected RuntimeException because of null table returned " +
                 "by call to KVStore.getTableAPI(), but non-null table was " +
                 "returned");
        } catch (RuntimeException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    /**
     * Exercises initialize if-block that handles non-null table returned by
     * TableApiImpl.getTable method. Test fails if/when exception occurs.
     */
    @Test
    public void testInitializeNonNullTable() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");
        recRdrNonNullTable.initialize(
            defaultTableInputSplit, defaultTableTaskAttemptContext);
    }

    /**
     * Exercises initialize loggers when the RecordReader's TableInputSplit
     * returns a queryBy value of QUERY_BY_PRIMARY_ALL_PARTITIONS.
     * Test fails if/when exception occurs.
     */
    @Test
    public void testInitializeAllPartitions() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");
        recRdrAllPartitions.initialize(
            splitPrimaryAllPartitions, defaultTableTaskAttemptContext);
    }

    /**
     * Exercises initialize loggers when the RecordReader's TableInputSplit
     * returns a queryBy value of QUERY_BY_PRIMARY_SINGLE_PARTITION.
     * Test fails if/when exception occurs.
     */
    @Test
    public void testInitializeSinglePartition() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");
        recRdrSinglePartition.initialize(
            splitPrimarySinglePartition, defaultTableTaskAttemptContext);
    }

    /**
     * Exercises initialize loggers when the RecordReader's TableInputSplit
     * returns a queryBy value of QUERY_BY_INDEX.
     * Test fails if/when exception occurs.
     */
    @Test
    public void testInitializeByIndex() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");
        recRdrByIndex.initialize(
            splitQueryByIndex, defaultTableTaskAttemptContext);
    }

    /**
     * Exercises initialize loggers when the RecordReader's TableInputSplit
     * returns a queryBy value of QUERY_BY_ONQL_ALL_PARTITIONS.
     * Test fails if/when exception occurs.
     */
    @Test
    public void testInitializeNativeAllPartitions() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");
        recRdrNativeAllPartitions.initialize(
            splitNativeAllPartitions, defaultTableTaskAttemptContext);
    }

    /**
     * Exercises initialize loggers when the RecordReader's TableInputSplit
     * returns a queryBy value of QUERY_BY_ONQL_SINGLE_PARTITION.
     * Test fails if/when exception occurs.
     */
    @Test
    public void testInitializeNativeSinglePartition() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");
        recRdrNativeSinglePartition.initialize(
            splitNativeSinglePartition, defaultTableTaskAttemptContext);
    }

    /**
     * Exercises initialize loggers when the RecordReader's TableInputSplit
     * returns a queryBy value of QUERY_BY_ONQL_SHARDS.
     * Test fails if/when exception occurs.
     */
    @Test
    public void testInitializeNativeShards() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");
        recRdrNativeShards.initialize(
            splitNativeShards, defaultTableTaskAttemptContext);
    }

    /**
     * Exercises initialize loggers when the RecordReader's TableInputSplit
     * returns a queryBy value of 'none of the possible values'
     * (the default logger output). Test fails if/when exception occurs.
     */
    @Test
    public void testInitializeDefaultQueryBy() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");
        recRdrDefaultQueryBy.initialize(
            splitDefaultQueryBy, defaultTableTaskAttemptContext);
    }

    /* --- Convenience methods with common code needed by this class --- */

    /*
     * Common code for creating and initializing a concrete (non-mocked)
     * TableRecordReader with the characteristics/properties that will
     * allow a given test to verify and exercise specific methods,
     * instructions, and branches of the TableRecordReader that need
     * test coverage.
     */
    public static TableRecordReader getInitializedTableRecordReader(
                            final String tableNamePrefix,
                            final PrimaryKey primaryKey,
                            final IndexKey indexKey,
                            final String primaryKeyProperty,
                            final String fieldRangeColName,
                            final String fieldRangeProperty,
                            final List<Set<Integer>> partitionSets,
                            final Set<RepGroupId> shardSet,
                            final int queryBy,
                            final String whereClause)
                                throws Exception {

        final String mockedTableName = tableNamePrefix + "_mockedTableName";

        /* Create the mocks to input to createMockedVehicleTable. */
        final KVStoreImpl mockedStore = createMock(KVStoreImpl.class);
        final TableAPIImpl mockedTableApiImpl = createMock(TableAPIImpl.class);
        final StatementResult mockedStatementResult =
            createMock(StatementResult.class);
        final TableIterator<Row> mockedTableIterator =
            createMock(TableIterator.class);
        final Row mockedRow = createMock(Row.class);

        final MultiRowOptions mockedMultiRowOptions =
            createMock(MultiRowOptions.class);
        final TableIteratorOptions mockedTableIteratorOptions =
            createMock(TableIteratorOptions.class);

        /*
         * Create a mocked Table and associate it with the mocked objects
         * created above. Also set all necessary expectations on the mocked
         * Table as well as the other mocked objects that are input.
         */
        Set<Integer> partitionSet = null;
        if (partitionSets != null) {
            if (partitionSets.isEmpty()) {
                partitionSet = new HashSet<>();
            } else {
                partitionSet = partitionSets.get(0);
            }
        }
        final Table mockedTable =
            HadoopTableCoverageTestBase.createMockedVehicleTable(
                                            mockedStore,
                                            mockedTableName,
                                            primaryKey,
                                            indexKey,
                                            primaryKeyProperty,
                                            mockedTableApiImpl,
                                            mockedMultiRowOptions,
                                            mockedTableIteratorOptions,
                                            partitionSet,
                                            shardSet,
                                            mockedTableIterator,
                                            mockedStatementResult,
                                            mockedRow,
                                            fieldRangeColName);

        /* Create concrete, non-mocked objects. */
        final JobConf vehicleTableJobConf =
            HadoopTableCoverageTestBase.getTableJobConf(
                                            mockedTableName,
                                            primaryKeyProperty,
                                            fieldRangeProperty);
        final TableRecordReader tableRecordReader =
            createTableRecordReader(mockedStore, vehicleTableJobConf);

        /* Create mocked TableInputSplit with necessary expectations set. */
        final TableInputSplit mockedTableInputSplit =
            createMockedTableInputSplit(mockedTableName,
                                        primaryKeyProperty,
                                        fieldRangeProperty,
                                        partitionSets,
                                        shardSet,
                                        queryBy,
                                        whereClause);

        /* Place all mocks in replay state. For any mocks in which a test
         * that calls this method to create and intialize a TableRecordReader
         * eventually invokes a void method on the mock, handle the void
         * method call by doing the following (using the close() method
         * as an example:
         *
         * 1. Call the close() method on the mocked object itself
         *    (not on the TableRecordReader being tested).
         * 2. Mark the close() method as the last call on the mocked object.
         * 3. Replay the mocked object.
         */
        mockedStore.close();
        expectLastCall();
        replay(mockedStore);

        replay(mockedTableApiImpl);
        replay(mockedTable);
        replay(mockedTableIterator);
        replay(mockedRow);
        replay(mockedTableIteratorOptions);
        replay(mockedMultiRowOptions);
        replay(mockedTableInputSplit);

        /* Exercise methods on the concrete TableRecordReader under test. */
        tableRecordReader.initialize(
            mockedTableInputSplit, defaultTableTaskAttemptContext);
        return tableRecordReader;
    }

    /*
     * Creates and returns a concrete (non-mocked) instance of
     * TableRecordReader on which testing can be performed.
     */
    private static TableRecordReader createTableRecordReader(
       final KVStore mockedStore, final JobConf jobConf) {

        try {
            defaultTableTaskAttemptContext =
                new TableTaskAttemptContext(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
            fail("EXCEPTION ON SETUP: " + e);
        }
        final TableRecordReader recordReader = new TableRecordReader();
        if (mockedStore != null) {
            recordReader.setStore(mockedStore);
        }
        return recordReader;
    }
}
