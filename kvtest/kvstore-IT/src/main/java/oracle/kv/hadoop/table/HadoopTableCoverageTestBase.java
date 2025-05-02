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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.ParamConstant;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

/**
 * Base class containing common methods and fields for the
 * coverage based unit tests for the various classes from the
 * <code>oracle.kv.hadoop.table</code> package.
 */
public class HadoopTableCoverageTestBase extends TestBase {

    private static final String LOGGER_RESOURCE_ID = "logger";

    protected static Class<?> THIS_CLASS;
    protected static String THIS_CLASS_NAME;
    protected static Logger staticLogger;

    /* Common/default mocked objects. */
    protected static KVStore defaultMockedStore;
    protected static TableAPIImpl defaultMockedTableApi;

    protected static Table defaultMockedTable;
    protected static PrimaryKey defaultMockedPrimaryKey;
    protected static Index defaultMockedIndex;
    protected static IndexKey defaultMockedIndexKey;
    protected static Row defaultMockedRow;
    protected static RecordDef defaultMockedRecordDef;
    protected static RecordValue defaultMockedRecordValue;

    /* Not mocked, but shared by various mocks. */
    protected static Table nullTable = null;

    /*
     * Common non-mocked objects needed by the method
     * HadoopTableIntegrationTest.createTableInputSplit to
     * create a common TableInputSplit.
     */
    protected static TableInputSplit defaultTableInputSplit;
    protected static TableTaskAttemptContext defaultTableTaskAttemptContext;

    /* Defaults for various initializations. */
    protected static String defaultStoreName; /* initialize in staticSetUp */
    protected static String defaultTableName; /* initialize in staticSetUp */
    protected static String defaultIndexName; /* initialize in staticSetUp */
    protected static String[] defaultHelperHosts = new String[]
            { "helperHost01:5000", "helperHost02:5001", "helperHost03:5003" };

    protected static Map<String, String> defaultPrimaryKeyMap =
                                             new HashMap<>();
    static {
        defaultPrimaryKeyMap.put("type", "auto");
        defaultPrimaryKeyMap.put("make", "Chrysler");
        defaultPrimaryKeyMap.put("model", "Imperial");
        defaultPrimaryKeyMap.put("class", "AllWheelDrive");
    }
    protected static String defaultPrimaryKeyProperty =
        "{\"type\":\"auto\",\"make\":\"Chrysler\"," +
         "\"model\":\"Imperial\",\"class\":\"AllWheelDrive\"}";

    protected static FieldRange defaultFieldRange;
    protected static String defaultFieldRangeColName = "model";
    protected static String defaultFieldRangeProperty =
           "{\"name\":\"model\",\"start\":\"Imperial\",\"startInclusive\":" +
           "true,\"end\":\"Sebring\",\"endInclusive\":false}";

    protected static Map<String, String> defaultIndexKeyMap = new HashMap<>();
    static {
        defaultIndexKeyMap.put("model", "Imperial");
    }

    protected static JobConf defaultTableJobConf;
    protected static List<Set<Integer>> defaultPartitionSets =
                                          new ArrayList<Set<Integer>>();
    protected static Set<RepGroupId> defaultShardSet =
                                          new HashSet<RepGroupId>();
    protected static int defaultQueryBy =
                           TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS;
    protected static String defaultWhereClause = "type = 'auto'";

    /* Defaults for getter methods on TableInputSplit. */
    protected static Direction direction = Direction.FORWARD;
    protected static Consistency consistency = Consistency.ABSOLUTE;
    protected static long timeout = 30000L;
    protected static TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    protected static int maxRequests = 15793;
    protected static int batchSize = 1000;
    protected static int maxBatches = 20;

    /* Defaults for setting properties in TableJobConf. */
    protected static String consistencyStr = "ABSOLUTE";
    protected static String timeoutStr = "30000";
    protected static String maxRequestsStr = "15793";
    protected static String batchSizeStr = "1000";
    protected static String maxBatchesStr = "20";

    protected static String loginFileBaseDir = "/tmp";
    protected static String trustFileBaseDir = loginFileBaseDir;
    protected static String userName = "testUSER_NAME";
    protected static String walletLoc = loginFileBaseDir;

    public static void staticSetUp() throws Exception {

        /*
         * Note that ClientLoggerUtils.getLogger creates a logger with name
         * that is the concatenation of the name of the given test class with
         * the given resource id String value. Thus, to configure the logger
         * for the test class, one needs to specify the results of that
         * contenation in the logger configuration file. For example,
         * if the name of the current test class is,
         * oracle.kv.hadoop.table.TableRecordReaderCoverageTest,
         * and if the resource id input to ClientLoggerUtils.getLogger
         * is, "logger", then to log at the FINE level, one would specify
         * the following in the logger configuration file:
         *
         * oracle.kv.hadoop.table.
         *                    TableRecordReaderCoverageTest.logger.level=FINE
         */
        staticLogger =
            ClientLoggerUtils.getLogger(THIS_CLASS, LOGGER_RESOURCE_ID);

        defaultStoreName = "kvStore-" + THIS_CLASS_NAME;
        defaultTableName = THIS_CLASS_NAME + "_Table";
        defaultIndexName = THIS_CLASS_NAME + "_Index";

        for (int i = 0; i < 1; i++) {
            final Set<Integer> partitionSet = new HashSet<>();
            for (int j = 0; j < 1; j++) {
                partitionSet.add(j);
            }
            defaultPartitionSets.add(partitionSet);
        }
        for (int i = 0; i < 1; i++) {
            defaultShardSet.add(new RepGroupId(i));
        }

        /* Create default store mocks (KVStore, TableAPIImpl). */
        defaultMockedStore = createMock(KVStore.class);
        defaultMockedTableApi = createMock(TableAPIImpl.class);

        /* Create default table mocks (Table, PrimaryKey, IndexKey, Row). */
        defaultTableSetup();

        /* Create concrete TableInputSplit. */
        defaultTableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                                               defaultTableJobConf);
        replay(defaultMockedStore);
        replay(defaultMockedTableApi);

        replay(defaultMockedTable);
        replay(defaultMockedPrimaryKey);
        replay(defaultMockedIndex);
        replay(defaultMockedIndexKey);
        replay(defaultMockedRow);
        replay(defaultMockedRecordDef);
        replay(defaultMockedRecordValue);
    }

    /* --- Convenience methods with common code needed by the tests --- */

    /*
     * Creates a default Table mock and a default concrete JobConf that
     * can be re-used by the tests.
     */
    protected static void defaultTableSetup() throws Exception {

        final TableBuilder builder =
            TableBuilder.createTableBuilder(defaultTableName);

        builder.addString("type");
        builder.addString("make");
        builder.addString("model");
        builder.addString("class");
        builder.addString("color");
        builder.addDouble("price");
        builder.addInteger("count");
        builder.primaryKey("type", "make", "model", "class");
        final TableImpl tableImpl = builder.buildTable();

        /* Initialize some state used in the mock tests. */
        final List<String> keyFieldNames = tableImpl.getPrimaryKey();
        final Map<String, FieldDef> fieldNameTypeMap =
            new HashMap<String, FieldDef>();
        for (String fieldName : tableImpl.getFields()) {
            fieldNameTypeMap.put(fieldName, tableImpl.getField(fieldName));
        }
        defaultFieldRange =
            new FieldRange(defaultFieldRangeColName,
                           fieldNameTypeMap.get(defaultFieldRangeColName), 0);

        defaultMockedTable = createMock(Table.class);
        defaultMockedPrimaryKey = createMock(PrimaryKey.class);
        defaultMockedIndex = createMock(Index.class);
        defaultMockedIndexKey = createMock(IndexKey.class);
        defaultMockedRow = createMock(Row.class);
        defaultMockedRecordDef = createMock(RecordDef.class);
        defaultMockedRecordValue = createMock(RecordValue.class);

        /* Set expectations for the mocked Table. */
        expect(defaultMockedTable.getName())
            .andReturn(defaultTableName).anyTimes();

        expect(defaultMockedTable.getFields())
            .andReturn(new ArrayList<String>(fieldNameTypeMap.keySet()))
                .anyTimes();

        for (Map.Entry<String, FieldDef> entry :
                 fieldNameTypeMap.entrySet()) {
            expect(defaultMockedTable.getField(entry.getKey()))
                .andReturn(entry.getValue()).anyTimes();
        }

        expect(defaultMockedTable.createPrimaryKey())
            .andReturn(defaultMockedPrimaryKey).anyTimes();

        expect(defaultMockedTable
            .createPrimaryKeyFromJson(defaultPrimaryKeyProperty, false))
            .andReturn(defaultMockedPrimaryKey).anyTimes();

        expect(defaultMockedTable.getPrimaryKey())
            .andReturn(keyFieldNames).anyTimes();

        expect(defaultMockedTable.getShardKey())
            .andReturn(keyFieldNames).anyTimes();

        expect(defaultMockedTable.createFieldRange(defaultFieldRangeColName))
            .andReturn(defaultFieldRange).anyTimes();

        expect(defaultMockedTable.getIndexes())
            .andReturn(Collections.singletonMap(
                           defaultIndexName, defaultMockedIndex)).anyTimes();

        expect(defaultMockedTable.getIndex(defaultIndexName))
            .andReturn(defaultMockedIndex).anyTimes();

        /* Set expectations for the mocked Row. */
        expect(defaultMockedRow.createPrimaryKey())
            .andReturn(defaultMockedPrimaryKey).anyTimes();

        /* Set expectations for the mocked PrimaryKey. */
        expect(defaultMockedPrimaryKey.getFieldNames())
            .andReturn(keyFieldNames).anyTimes();

        expect(defaultMockedPrimaryKey.getDefinition())
            .andReturn(defaultMockedRecordDef).anyTimes();

        /* Each field of the PrimaryKey is a StringDef. */
        for (String fieldName : keyFieldNames) {
            expect(defaultMockedRecordDef.getFieldDef(fieldName))
                .andReturn(FieldDefImpl.Constants.stringDef).anyTimes();

            expect(defaultMockedPrimaryKey.
                       put(fieldName, defaultPrimaryKeyMap.get(fieldName)))
                .andReturn(defaultMockedRecordValue).anyTimes();
        }

        /* Set expectations for the mocked Index. */
        expect(defaultMockedIndex.getName())
            .andReturn(defaultIndexName).anyTimes();

        expect(defaultMockedIndex.getFields())
            .andReturn(new ArrayList<String>(defaultIndexKeyMap.keySet()))
                .anyTimes();

        expect(defaultMockedIndex.createIndexKey())
            .andReturn(defaultMockedIndexKey).anyTimes();

        expect(defaultMockedIndex.createFieldRange(defaultFieldRangeColName))
            .andReturn(defaultFieldRange).anyTimes();

        expect(defaultMockedIndexKey.getDefinition())
            .andReturn(defaultMockedRecordDef).anyTimes();

        /* Set expectations for the mocked IndexKey. */
        for (Map.Entry<String, String> entry : defaultIndexKeyMap.entrySet()) {
            expect(defaultMockedIndexKey.put(entry.getKey(), entry.getValue()))
                .andReturn(defaultMockedRecordValue).anyTimes();
        }

        expect(defaultMockedIndexKey.getIndex())
            .andReturn(defaultMockedIndex).anyTimes();

        /* Set expectations for the mocked IndexKey. */

        defaultTableJobConf = getTableJobConf(defaultTableName,
                                              defaultPrimaryKeyProperty,
                                              defaultFieldRangeProperty);
    }

    /*
     * Creates and returns a JobConf populated with the given parameters
     * as well as default values. The object returned by this method is
     * used by the tests where a JobConf is needed.
     */
    protected static JobConf getTableJobConf(final String tableName,
                                             final String primaryKeyProperty,
                                             final String fieldRangeProperty)
                                                 throws Exception {

        final Configuration conf = new Configuration();

        conf.set(ParamConstant.KVSTORE_NAME.getName(), defaultStoreName);

        final StringBuilder helperHostsBuf =
                                new StringBuilder(defaultHelperHosts[0]);
        for (int i = 1; i < defaultHelperHosts.length; i++) {
            helperHostsBuf.append("," + defaultHelperHosts[i]);
        }
        final String helperHostsStr = helperHostsBuf.toString();
        conf.set(ParamConstant.KVSTORE_NODES.getName(), helperHostsStr);

        final StringBuilder hadoopLocsBuf = new StringBuilder();
        final String[] helperHosts = helperHostsStr.trim().split(",");
        for (int i = 0; i < helperHosts.length; i++) {
            /* Strip off the ':port' suffix */
            final String[] hostPort = (helperHosts[i]).trim().split(":");
            if (i != 0) {
                hadoopLocsBuf.append(",");
            }
            hadoopLocsBuf.append(hostPort[0]);
        }
        conf.set(ParamConstant.KVHADOOP_NODES.getName(),
                 hadoopLocsBuf.toString());

        conf.set(ParamConstant.TABLE_NAME.getName(), tableName);
        if (primaryKeyProperty != null) {
            conf.set(ParamConstant.PRIMARY_KEY.getName(), primaryKeyProperty);
        }
        if (fieldRangeProperty != null) {
            conf.set(ParamConstant.FIELD_RANGE.getName(), fieldRangeProperty);
        }
        conf.set(ParamConstant.CONSISTENCY.getName(), consistencyStr);
        conf.set(ParamConstant.TIMEOUT.getName(), timeoutStr);
        conf.set(ParamConstant.MAX_REQUESTS.getName(), maxRequestsStr);
        conf.set(ParamConstant.BATCH_SIZE.getName(), batchSizeStr);
        conf.set(ParamConstant.MAX_BATCHES.getName(), maxBatchesStr);

        conf.set(
          KVSecurityConstants.SECURITY_FILE_PROPERTY, loginFileBaseDir);
        conf.set(
          KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY, trustFileBaseDir);
        conf.set(KVSecurityConstants.AUTH_USERNAME_PROPERTY, userName);
        conf.set(KVSecurityConstants.AUTH_WALLET_PROPERTY, walletLoc);

        return new JobConf(conf);
    }

    /*
     * Creates and returns a mock of the VehicleTable from the examples.
     *
     * Note that in addition to creating the Table mock, this method also
     * sets appropriate expectations on that mock. This method also sets
     * appropriate expectations on the TableAPIImpl mock that is input.
     *
     * NOTE that for any mock on which expectations are set in this method,
     * those mocks should be placed in the replay state after calling this
     * method.
     *
     * Also note that unchecked warnings are suppressed here to avoid warnings
     * produced by the calls to the EasyMock.anyObject(classname.class).
     */
    @SuppressWarnings("unchecked")
    protected static Table createMockedVehicleTable(
                               final KVStoreImpl kvstore,
                               final String tableName,
                               final PrimaryKey primaryKey,
                               final IndexKey indexKey,
                               final String primaryKeyProperty,
                               final TableAPIImpl tableApiImpl,
                               final MultiRowOptions multiRowOptions,
                               final TableIteratorOptions tableIteratorOptions,
                               final Set<Integer> partitionSet,
                               final Set<RepGroupId> shardSet,
                               final TableIterator<Row> tableIterator,
                               final StatementResult statementResult,
                               final Row row,
                               final String fieldRangeColName)
                                 throws Exception {

        final TableBuilder builder =
            TableBuilder.createTableBuilder(tableName);

        builder.addString("type");
        builder.addString("make");
        builder.addString("model");
        builder.addString("class");
        builder.addString("color");
        builder.addDouble("price");
        builder.addFloat("fprice");
        builder.addInteger("count");
        builder.addLong("lcount");
        builder.addNumber("dealerid");
        builder.addTimestamp("delivered", 9);
        builder.primaryKey("type", "make", "model", "class");
        final TableImpl tableImpl = builder.buildTable();

        /* Initialize some state used in the mock. */
        final List<String> keyFieldNames = tableImpl.getPrimaryKey();
        final Map<String, FieldDef> fieldNameTypeMap =
            new HashMap<String, FieldDef>();
        for (String fieldName : tableImpl.getFields()) {
            fieldNameTypeMap.put(fieldName, tableImpl.getField(fieldName));
        }
        final FieldRange fieldRange =
            new FieldRange(fieldRangeColName,
                           fieldNameTypeMap.get(fieldRangeColName), 0);

        final Table mockedVehicleTable = createMock(Table.class);

        /* Set expectations for the mocked KVStoreImpl that was input. */
        expect(kvstore.getTableAPI()).andReturn(tableApiImpl).anyTimes();
        expect(kvstore.executeSyncPartitions(
                           anyObject(String.class),
                           anyObject(ExecuteOptions.class),
                           anyObject(Set.class)))
            .andReturn(statementResult).anyTimes();
        expect(kvstore.executeSyncShards(
                           anyObject(String.class),
                           anyObject(ExecuteOptions.class),
                           anyObject(Set.class)))
            .andReturn(statementResult).anyTimes();

        /*
         * Set expectations for the mocked TableAPIImpl that was input.
         *
         * Note that for now, anyObject is used for the input parameters.
         * When/if it ever becomes necessary to do 'real' iteration over
         * the table (via PrimaryKey or Indexkey), then the calls below can
         * be changed to expect the actual (mocked) objects that were input
         * to this method. Mixing actual objects with anyObject(class type)
         * is not allowed.
         */
        if (primaryKey != null && multiRowOptions != null &&
            tableIteratorOptions != null) {

            expect(tableApiImpl.tableIterator(
                                    anyObject(PrimaryKey.class),
                                    anyObject(MultiRowOptions.class),
                                    anyObject(TableIteratorOptions.class)))
                .andReturn(tableIterator).anyTimes();
            expect(tableApiImpl.tableIterator(
                                    anyObject(PrimaryKey.class),
                                    anyObject(MultiRowOptions.class),
                                    anyObject(TableIteratorOptions.class),
                                    anyObject(Set.class)))
                .andReturn(tableIterator).anyTimes();

        } else {

            throw new NullPointerException(
                "at least one of the following 3 parameters input to " +
                "HadoopTableCoverageTestBase.createMockedVehicleTable is " +
                "null: primaryKey = " + primaryKey + ", multiRowOptions = " +
                multiRowOptions + ", tableIteratorOptions = " +
                tableIteratorOptions +
                " (also, partitionSet = " + partitionSet + ")");
        }

        if (indexKey != null && multiRowOptions != null &&
            tableIteratorOptions != null) {

            expect(tableApiImpl.tableIterator(
                                    anyObject(IndexKey.class),
                                    anyObject(MultiRowOptions.class),
                                    anyObject(TableIteratorOptions.class)))
                .andReturn(tableIterator).anyTimes();
            expect(tableApiImpl.tableIterator(
                                    anyObject(IndexKey.class),
                                    anyObject(MultiRowOptions.class),
                                    anyObject(TableIteratorOptions.class),
                                    anyObject(Set.class)))
                .andReturn(tableIterator).anyTimes();

        } else {

            throw new NullPointerException(
                "at least one of the following 3 parameters input to " +
                "HadoopTableCoverageTestBase.createMockedVehicleTable is " +
                "null: indexKey = " + indexKey + ", multiRowOptions = " +
                multiRowOptions + ", tableIteratorOptions = " +
                tableIteratorOptions +
                " (also, shardSet = " + shardSet + ")");
        }

        expect(tableApiImpl.getTable(tableName))
            .andReturn(mockedVehicleTable).anyTimes();

        /* Set expectations for the mocked TableIterator that was input. */
        expect(tableIterator.hasNext()).andReturn(true).anyTimes();
        expect(tableIterator.next()).andReturn(row).anyTimes();
        tableIterator.close(); /* Handle expectation for void close(). */
        expectLastCall();

        /* Set expectations for the mocked Row returned by the iterators. */
        expect(row.createPrimaryKey()).andReturn(primaryKey).anyTimes();

        /* Set expectations for the mocked StatementResult. */
        final TableIterator<RecordValue> mockedTableIterator =
            createMock(TableIterator.class);
        expect(statementResult.iterator()).andReturn(mockedTableIterator)
            .anyTimes();

        /* Set expectations for the mocked Table that will be returned. */
        expect(mockedVehicleTable.getName())
            .andReturn(tableName).anyTimes();

        expect(mockedVehicleTable.getFields())
            .andReturn(new ArrayList<String>(fieldNameTypeMap.keySet()))
                .anyTimes();

        for (Map.Entry<String, FieldDef> entry :
                 fieldNameTypeMap.entrySet()) {
            expect(mockedVehicleTable.getField(entry.getKey()))
                .andReturn(entry.getValue()).anyTimes();
        }
        expect(mockedVehicleTable.createPrimaryKey())
            .andReturn(primaryKey).anyTimes();

        /* Determine if primaryKeyProperty is valid JSON. Throw IAE if not. */
        try {
            CommandJsonUtils.readObjectValue(primaryKeyProperty);

            expect(mockedVehicleTable
                   .createPrimaryKeyFromJson(primaryKeyProperty, false))
                .andReturn(primaryKey).anyTimes();
        } catch (IllegalArgumentException | NullPointerException e) {
            expect(mockedVehicleTable
                   .createPrimaryKeyFromJson(primaryKeyProperty, false))
                .andThrow(new IllegalArgumentException()).anyTimes();
        }

        expect(mockedVehicleTable.getPrimaryKey())
            .andReturn(keyFieldNames).anyTimes();

        expect(mockedVehicleTable.getShardKey())
            .andReturn(keyFieldNames).anyTimes();

        expect(mockedVehicleTable.createFieldRange(fieldRangeColName))
            .andReturn(fieldRange).anyTimes();

        expect(mockedVehicleTable.getIndexes())
            .andReturn(Collections.singletonMap(
                           defaultIndexName, defaultMockedIndex)).anyTimes();

        expect(mockedVehicleTable.getIndex(defaultIndexName))
            .andReturn(defaultMockedIndex).anyTimes();

        return mockedVehicleTable;
    }

    /*
     * Creates and returns a mock of TableInputSplit with the all
     * necessary expectations set on the mock that is returned.
     */
    protected static TableInputSplit createMockedTableInputSplit(
                                        final String tableName,
                                        final String primaryKeyProperty,
                                        final String fieldRangeProperty,
                                        final List<Set<Integer>> partitionSets,
                                        final Set<RepGroupId> shardSet,
                                        final int queryBy,
                                        final String whereClause)
                                           throws Exception {

        final TableInputSplit retTableInputSplit =
            createMock(TableInputSplit.class);

        expect(retTableInputSplit.getTableName())
            .andReturn(tableName).anyTimes();
        expect(retTableInputSplit.getKVStoreName())
            .andReturn(defaultStoreName).anyTimes();
        expect(retTableInputSplit.getKVHelperHosts())
            .andReturn(defaultHelperHosts).anyTimes();
        expect(retTableInputSplit.getPrimaryKeyProperty())
            .andReturn(primaryKeyProperty).anyTimes();
        expect(retTableInputSplit.getFieldRangeProperty())
            .andReturn(fieldRangeProperty).anyTimes();
        expect(retTableInputSplit.getDirection())
            .andReturn(direction).anyTimes();
        expect(retTableInputSplit.getConsistency())
            .andReturn(consistency).anyTimes();
        expect(retTableInputSplit.getTimeout())
            .andReturn(timeout).anyTimes();
        expect(retTableInputSplit.getTimeoutUnit())
            .andReturn(timeUnit).anyTimes();
        expect(retTableInputSplit.getMaxRequests())
            .andReturn(maxRequests).anyTimes();
        expect(retTableInputSplit.getBatchSize())
            .andReturn(batchSize).anyTimes();
        expect(retTableInputSplit.getPartitionSets())
            .andReturn(partitionSets).anyTimes();
        expect(retTableInputSplit.getShardSet())
            .andReturn(shardSet).anyTimes();
        expect(retTableInputSplit.getQueryBy()).andReturn(queryBy).anyTimes();
        expect(retTableInputSplit.getWhereClause())
            .andReturn(whereClause).anyTimes();

        return retTableInputSplit;
    }

    /*
     * Creates and returns a concrete list of partition sets that can be
     * used by various coverage tests.
     */
    protected List<Set<Integer>> createPartitionSets(final int nPartitions,
                                                     final int partitionSize) {

        final List<Set<Integer>> partitionSets = new ArrayList<Set<Integer>>();
        for (int i = 0; i < nPartitions; i++) {
            final Set<Integer> partitionSet = new HashSet<>();
            for (int j = 0; j < partitionSize; j++) {
                partitionSet.add(j);
            }
            partitionSets.add(partitionSet);
        }
        return partitionSets;
    }

    /*
     * Creates and returns a concrete set of RepGroupIds (shards) that can be
     * used by various coverage tests.
     */
    protected Set<RepGroupId> createShardSet(final int nShards) {
        final Set<RepGroupId> shardSet = new HashSet<RepGroupId>();
        for (int i = 0; i < nShards; i++) {
            shardSet.add(new RepGroupId(i));
        }
        return shardSet;
    }
}
