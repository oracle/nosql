/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.proxy.protocol;

import static oracle.nosql.proxy.ProxySerialization.getCapacityMode;
import static oracle.nosql.proxy.ProxySerialization.getReplicaState;
import static oracle.nosql.proxy.ProxySerialization.getTableState;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ON_DEMAND;
import static oracle.nosql.proxy.protocol.BinaryProtocol.PROVISIONED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.QUERY_V4;
import static oracle.nosql.proxy.protocol.BinaryProtocol.QUERY_V5;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.Consistency;
import oracle.kv.StatementResult;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.query.QueryStatementResultImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableAPIImpl.OpResultWrapper;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.query.runtime.ResumeInfo.VirtualScan;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.table.FieldValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationResult;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.nson.Nson.NsonSerializer;
import oracle.nosql.nson.util.NettyByteOutputStream;
import oracle.nosql.nson.util.NioByteOutputStream;
import oracle.nosql.proxy.DataService.WriteMultipleOpInfo;
import oracle.nosql.proxy.DataServiceHandler;
import oracle.nosql.proxy.DataServiceHandler.RequestContext;
import oracle.nosql.proxy.DataServiceHandler.TableOperationInfo;
import oracle.nosql.proxy.ProxySerialization;
import oracle.nosql.proxy.ProxySerialization.FieldValueWriterImpl;
import oracle.nosql.proxy.ProxySerialization.RowReaderImpl;
import oracle.nosql.proxy.sc.IndexResponse;
import oracle.nosql.proxy.sc.ListTableResponse;
import oracle.nosql.proxy.sc.ReplicaStatsResponse;
import oracle.nosql.proxy.sc.TableUsageResponse;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.util.tmi.IndexInfo;
import oracle.nosql.util.tmi.IndexInfo.IndexField;
import oracle.nosql.util.tmi.ReplicaInfo;
import oracle.nosql.util.tmi.ReplicaStats;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableLimits;
import oracle.nosql.util.tmi.TableUsage;

/*
 * QUESTIONS:
 * o is "header" useful or should header fields just start at top-level?
 * o is "payload" useful? Same rationale. Has implications on ordering of
 *   fields, but as long as header fields come first it may be ok
 * o flatter structure may be more efficient in the end
 *
 * Requests. Header is generic, payload is operation specific
 * {
 *  "header" : {
 *    "version" : 4,
 *    "op" : 7,
 *    "timeout" : 4500,
 *  },
 *  "payload" : {
 *    "table_name" : "mytable"
 *    "consistency" : 1,
 *    "data" : {
 *      "id" : 5
 *    }
 *  }
 * }
 *
 * Payloads
 * TableRequest
 *  "payload" : {
 *    "statement": "..."
 *    "limits" : {
 *      read, write, storage
 *     }
 *      "id" : 5
 *    }
 *
 *
 * Response
 * {
 *   "error_code": int (code)
 *   "exception": "..."
 *   "consumed": {
 *      "read_units": int,
 *      "read_kb": int,
 *      "write_kb": int
 *    }
 *   "row": {
 *     "modified": ..
 *     "version": ..
 *     "expires": ..
 *     "value": {}
 *   }
 *
 * }
 *
 */

public class NsonProtocol {
    public static int V4_VERSION = 4;

    /*
     * request fields
     */
    public static String ABORT_ON_FAIL = "a";
    public static String BATCH_COUNTER = "bc";
    public static String BIND_VARIABLES = "bv";
    public static String COMPARTMENT_OCID = "cc";
    public static String CONSISTENCY = "co";
    public static String CONTINUATION_KEY = "ck";
    public static String DATA = "d";
    public static String DEFINED_TAGS = "dt";
    public static String DRL_OPTIN = "dro";
    public static String DURABILITY = "du";
    public static String END = "en";
    public static String ETAG = "et";
    public static String EXACT_MATCH = "ec";
    public static String FIELDS = "f";
    public static String FREE_FORM_TAGS = "ff";
    public static String GET_QUERY_PLAN = "gq";
    public static String GET_QUERY_SCHEMA = "gs";
    public static String HEADER = "h";
    public static String IDEMPOTENT = "ip";
    public static String IDENTITY_CACHE_SIZE = "ic";
    public static String INCLUSIVE = "in";
    public static String INDEX = "i";
    public static String INDEXES = "ix";
    public static String IS_JSON = "j";
    public static String IS_PREPARED = "is";
    public static String IS_SIMPLE_QUERY = "iq";
    public static String KEY = "k";
    public static String KV_VERSION = "kv";
    public static String LAST_INDEX = "li";
    public static String LIST_MAX_TO_READ = "lx";
    public static String LIST_START_INDEX = "ls";
    public static String MATCH_VERSION = "mv";
    public static String MAX_QUERY_PARALLELISM = "mp";
    public static String MAX_READ_KB = "mr";
    public static String MAX_SHARD_USAGE_PERCENT = "ms";
    public static String MAX_WRITE_KB = "mw";
    public static String NAME = "m";
    public static String NAMESPACE = "ns";
    public static String NUMBER_LIMIT = "nl";
    public static String NUM_OPERATIONS = "no";
    public static String NUM_QUERY_OPERATIONS = "nq";
    public static String OPERATION = "op";
    public static String OPERATIONS = "os";
    public static String OPERATION_ID = "od";
    public static String OP_CODE = "o";
    public static String PATH = "pt";
    public static String PAYLOAD = "p";
    public static String PREFER_THROTTLING = "pg";
    public static String PREPARE = "pp";
    public static String PREPARED_QUERY = "pq";
    public static String PREPARED_STATEMENT = "ps";
    public static String QUERY = "q";
    public static String QUERY_BATCH_TRACES = "qts";
    public static String QUERY_NAME = "qn";
    public static String QUERY_OPERATION_NUM = "on";
    public static String QUERY_VERSION = "qv";
    public static String RANGE = "rg";
    public static String RANGE_PATH = "rp";
    public static String READ_THROTTLE_COUNT = "rt";
    public static String READ_UNITS = "ru";
    public static String REGION = "rn";
    public static String RESOURCE = "ro";
    public static String RESOURCE_ID = "rd";
    public static String RETURN_ROW = "rr";
    public static String ROW_METADATA = "mt";
    public static String SHARD_ID = "si";
    public static String SERVER_MEMORY_CONSUMPTION = "sm";
    public static String START = "sr";
    public static String STATEMENT = "st";
    public static String STORAGE_THROTTLE_COUNT = "sl";
    public static String SYSTEM = "sy";
    public static String TABLES = "tb";
    public static String TABLE_DDL = "td";
    public static String TABLE_NAME = "n";
    public static String TABLE_OCID = "to";
    public static String TABLE_USAGE = "u";
    public static String TABLE_USAGE_PERIOD = "pd";
    public static String TIMEOUT = "t";
    public static String TOPO_SEQ_NUM = "ts";
    public static String TRACE_AT_LOG_FILES = "tf";
    public static String TRACE_LEVEL = "tl";
    public static String TTL = "tt";
    public static String TYPE = "y";
    public static String UPDATE_TTL = "ut";
    public static String VALUE = "l";
    public static String VERSION = "v";
    public static String VIRTUAL_SCAN = "vs";
    public static String VIRTUAL_SCANS = "vssa";
    public static String VIRTUAL_SCAN_SID = "vssid";
    public static String VIRTUAL_SCAN_PID = "vspid";
    public static String VIRTUAL_SCAN_NUM_TABLES = "vsnt";
    public static String VIRTUAL_SCAN_CURRENT_INDEX_RANGE = "vscir";
    public static String VIRTUAL_SCAN_PRIM_KEY = "vspk";
    public static String VIRTUAL_SCAN_SEC_KEY = "vssk";
    public static String VIRTUAL_SCAN_MOVE_AFTER = "vsma";
    public static String VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY = "vsjdrk";
    public static String VIRTUAL_SCAN_JOIN_PATH_TABLES = "vsjpt";
    public static String VIRTUAL_SCAN_JOIN_PATH_KEY = "vsjpk";
    public static String VIRTUAL_SCAN_JOIN_PATH_SEC_KEY = "vsjpsk";
    public static String VIRTUAL_SCAN_JOIN_PATH_MATCHED = "vsjpm";
    public static String WRITE_MULTIPLE = "wm";
    public static String WRITE_THROTTLE_COUNT = "wt";
    public static String WRITE_UNITS = "wu";

    /*
     * response fields
     */
    public static String ERROR_CODE = "e";
    public static String EXCEPTION = "x";
    public static String NUM_DELETIONS = "nd";
    public static String PROXY_TOPO_SEQNUM = "pn";
    public static String RETRY_HINT = "rh";
    public static String SHARD_IDS = "sa";
    public static String SUCCESS = "ss";
    public static String TOPOLOGY_INFO = "tp";
    public static String WM_FAILURE = "wf";
    public static String WM_FAIL_INDEX = "wi";
    public static String WM_FAIL_RESULT = "wr";
    public static String WM_SUCCESS = "ws";

    /* table metadata */
    public static String INITIALIZED = "it";
    public static String REPLICAS = "rc";
    public static String SCHEMA_FROZEN = "sf";
    public static String TABLE_SCHEMA = "ac";
    public static String TABLE_STATE = "as";

    /* system request */
    public static String SYSOP_RESULT = "rs";
    public static String SYSOP_STATE = "ta";

    /* throughput used and limits */
    public static String CONSUMED = "c";
    public static String LIMITS = "lm";
    public static String LIMITS_MODE = "mo";
    public static String READ_KB = "rk";
    public static String STORAGE_GB = "sg";
    public static String WRITE_KB = "wk";

    /* row metadata */
    public static String EXPIRATION = "xp";
    public static String CREATION_TIME = "ct";
    public static String MODIFIED = "md";
    public static String ROW = "r";
    public static String ROW_VERSION = "rv";

    /* operation metadata */
    public static String EXISTING_MOD_TIME = "em";
    public static String EXISTING_VALUE = "el";
    public static String EXISTING_VERSION = "ev";
    public static String EXISTING_ROW_METADATA = "ed";
    public static String GENERATED = "gn";
    public static String RETURN_INFO = "ri";

    /* query response fields */
    public static String DRIVER_QUERY_PLAN = "dq";
    public static String MATH_CONTEXT_CODE = "mc";
    public static String MATH_CONTEXT_ROUNDING_MODE = "rm";
    public static String MATH_CONTEXT_PRECISION = "cp";
    public static String NOT_TARGET_TABLES = "nt";
    public static String NUM_RESULTS = "nr";
    public static String QUERY_OPERATION = "qo";
    public static String QUERY_PLAN_STRING = "qs";
    public static String QUERY_RESULTS = "qr";
    public static String QUERY_RESULT_SCHEMA = "qc";
    public static String REACHED_LIMIT = "re";
    public static String SORT_PHASE1_RESULTS = "p1";
    public static String TABLE_ACCESS_INFO = "ai";

    /* replica stats response fields */
    public static String NEXT_START_TIME = "ni";
    public static String REPLICA_STATS = "ra";
    public static String REPLICA_LAG = "rl";
    public static String TIME = "tm";

    /*
     * common read/write methods from here down
     */
    public static void writeGetResponse(RequestContext rc,
                                        DataServiceHandler handler,
                                        Result result,
                                        Consistency consistency,
                                        TableAPIImpl tableApi,
                                        TableImpl table,
                                        RowSerializer primaryKey)
        throws IOException {

        /* error code
         * consumed
         * row...
         */
        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, ERROR_CODE, 0);
            writeConsumedCapacity(ns,
                                  handler,
                                  result.getReadKB(), // read units
                                  getReadKB(
                                      result.getReadKB(),
                                      consistency == Consistency.ABSOLUTE),
                                  result.getWriteKB());
            writeTopologyInfo(ns, tableApi.getStore().getTopology(),
                              rc.driverTopoSeqNum);

            if (result.getSuccess()) {
                writeRow(ns, result, tableApi, table, primaryKey);
            }
            ns.endMap(0);
        }
    }

    public static void writeDeleteResponse(RequestContext rc,
                                           DataServiceHandler handler,
                                           Result result,
                                           TableAPIImpl tableApi,
                                           RowSerializer pkey,
                                           ReturnRow returnRow)
        throws IOException {

        /* error code
         * consumed
         * success/fail
         * return row
         */
        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, ERROR_CODE, 0);
            writeConsumedCapacity(ns,
                                  handler,
                                  result.getReadKB(), // read units
                                  getReadKB(result.getReadKB(), true),
                                  result.getWriteKB());
            writeTopologyInfo(ns, tableApi.getStore().getTopology(),
                              rc.driverTopoSeqNum);
            /* did the delete happen? */
            writeMapField(ns, SUCCESS, result.getSuccess());
            if (returnRow != null) {
                /* the original key is used for the key */
                writeReturnRow(ns, result, returnRow, tableApi, pkey);
            }
            ns.endMap(0);
        }
    }

    public static void writeMultiDeleteResponse(RequestContext rc,
                                                DataServiceHandler handler,
                                                Result result,
                                                TableAPIImpl tableApi)
        throws IOException {

        /* error code
         * consumed
         * num deletions
         * continuation key
         */
        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, ERROR_CODE, 0);
            writeConsumedCapacity(ns,
                                  handler,
                                  result.getReadKB(), // read units
                                  getReadKB(result.getReadKB(), true),
                                  result.getWriteKB());
            writeTopologyInfo(ns, tableApi.getStore().getTopology(),
                              rc.driverTopoSeqNum);
            writeMapField(ns, NUM_DELETIONS, result.getNDeletions());
            if (result.getPrimaryResumeKey() != null) {
                writeMapField(ns, CONTINUATION_KEY,
                              result.getPrimaryResumeKey());
            }
            ns.endMap(0);
        }
    }

    public static void writePutResponse(RequestContext rc,
                                        DataServiceHandler handler,
                                        Result result,
                                        TableAPIImpl tableApi,
                                        RowSerializer row,
                                        ReturnRow returnRow)
        throws IOException {

        /* error code
         * consumed
         * version
         * return row
         * generated
         */
        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, ERROR_CODE, 0);
            writeConsumedCapacity(ns,
                                  handler,
                                  result.getReadKB(), // read units
                                  getReadKB(result.getReadKB(), true),
                                  result.getWriteKB());
            writeTopologyInfo(ns, tableApi.getStore().getTopology(),
                              rc.driverTopoSeqNum);
            Version version = result.getNewVersion();
            if (version != null) {
                writeMapField(ns, ROW_VERSION, version.toByteArray());
                /* only write generated value if put happened */
                if (result.getGeneratedValue() != null) {
                    writeGeneratedValue(ns, result.getGeneratedValue());
                }
            }
            if (returnRow != null) {
                /* write the existing row */
                writeReturnRow(ns, result, returnRow, tableApi, row);
            }

            ns.endMap(0);
        }
    }

    /* namespace is on-prem only */
    public static void writeGetTableResponse(RequestContext rc,
                                             TableInfo info,
                                             String[] tags,
                                             String namespace,
                                             TenantManager tm)
        throws IOException {

        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, COMPARTMENT_OCID, info.getCompartmentId());
            writeMapField(ns, TABLE_OCID, info.getTableOcid());
            writeMapField(ns, NAMESPACE, namespace);
            writeMapField(ns, TABLE_NAME, info.getTableName());
            writeMapField(ns, TABLE_STATE, getTableState(info.getStateEnum()));
            if (info.getTableLimits() != null) {
                writeLimits(ns, info.getTableLimits());
            }
            writeMapField(ns, TABLE_SCHEMA, info.getSchema());
            writeMapField(ns, TABLE_DDL, info.getDdl());
            writeMapField(ns, OPERATION_ID, info.getOperationId());
            /*
             * These are JSON strings, treat them as string.
             * See IAMAccessContext.getExistingTags for format
             * NOTE: system tags are not part of the protocol at this time
             */
            if (tags != null) {
                writeMapField(ns, FREE_FORM_TAGS, tags[0]);
                writeMapField(ns, DEFINED_TAGS, tags[1]);
            }
            if (info.getETag() != null) {
                String et = JsonUtils.encodeBase64(info.getETag());
                writeMapField(ns, ETAG, et);
            }

            if (info.getSchemaState() == TableInfo.SchemaState.FROZEN) {
                writeMapField(ns, SCHEMA_FROZEN, true);
            }

            if (info.isMultiRegion()) {
                writeMapField(ns, INITIALIZED, info.isInitialized());
                writeReplicas(ns, info.getReplicas().values(), tm);
            }
            ns.endMap(0);
        }
    }

    /*
     * WriteMultiple result:
     *  consumed capacity
     *  # use existence of fields as success/fail
     *  "wm_success": [ {result}, {result} ]
     *  "wm_failure": {
     *      "index": int
     *      "fail_result": {}
     *   }
     */
    public static void writeWriteMultipleResponse(
        RequestContext rc,
        DataServiceHandler handler,
        Result result,
        WriteMultipleOpInfo info,
        TableAPIImpl tableApi,
        TableOpExecutionException toee/* if failure */)

        throws IOException {

        if (result == null && toee == null) {
            throw new IllegalStateException(
                "WriteMultiple: Result or exception must be non-null");
        }
        final List<TableOperationResult> results =
            result != null ? tableApi.createResultsFromExecuteResult(
                result, info.tableOps) : null;

        int readKB = toee != null ? toee.getReadKB() : result.getReadKB();
        int writeKB = toee != null ? toee.getWriteKB() : result.getWriteKB();
        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, ERROR_CODE, 0);
            writeConsumedCapacity(ns,
                                  handler,
                                  readKB, // read units
                                  getReadKB(readKB, true),
                                  writeKB);
            writeTopologyInfo(ns, tableApi.getStore().getTopology(),
                              rc.driverTopoSeqNum);

            if (results != null) {
                /* success case is an array of results */
                startArray(ns, WM_SUCCESS);

                int index = 0;
                for (TableOperationResult opResult: results) {
                    TableOperation.Type type =
                        info.tableOps.get(index).getType();
                    TableOperationInfo topInfo = rc.tableOpInfos.get(index);
                    boolean isDel =
                        (type == TableOperation.Type.DELETE ||
                         type == TableOperation.Type.DELETE_IF_VERSION);
                    FieldValue generatedValue =
                        topInfo.genInfo != null ?
                        topInfo.genInfo.getGeneratedValue() : null;
                    ns.startMap(0);
                    writeOperationResult(ns, opResult,
                                         topInfo.table, generatedValue,
                                         topInfo.returnInfo,
                                         isDel);
                    ns.endMap(0);
                    ns.endArrayField(0);
                    index++;
                }
                endArray(ns, WM_SUCCESS);
            } else {
                int failIndex = toee.getFailedOperationIndex();
                TableOperationInfo topInfo = rc.tableOpInfos.get(failIndex);
                /* failure is an index to the failed op plus the failure info */
                startMap(ns, WM_FAILURE);
                writeMapField(ns, WM_FAIL_INDEX, failIndex);

                /* the failed operation */
                startMap(ns, WM_FAIL_RESULT);
                writeOperationResult(ns, toee.getFailedOperationResult(),
                                     topInfo.table, null,
                                     topInfo.returnInfo,
                                     false); // ignored on failure
                endMap(ns, WM_FAIL_RESULT);
                endMap(ns, WM_FAILURE);
            }
            ns.endMap(0);
        }
    }

    /**
     * A single operation result:
     * {
     *  "success": true|false -- did the op happen
     *  "version" (of new row, optional, put-only
     *  "generated" (optional, put-only)
     *  "existing_value" (optional, based on return info)
     * }
     */
    private static void writeOperationResult(NsonSerializer ns,
                                             TableOperationResult result,
                                             TableImpl table,
                                             FieldValue generatedValue,
                                             boolean returnInfo,
                                             boolean isDelete)
        throws IOException {

        /* assume that we are inside the containing map already */
        writeMapField(ns, SUCCESS, result.getSuccess());
        if (result.getSuccess() &&
            !isDelete &&
            result.getNewVersion() != null) {
            writeMapField(ns, ROW_VERSION,
                          result.getNewVersion().toByteArray());
            /*
             * Only write generated value if the operation actually
             * succeeded in putting a new row
             */
            if (generatedValue != null) {
                writeGeneratedValue(ns, generatedValue);
            }
        }
        /* only write return info when requested */
        if (returnInfo) {
            /*
             * TODO: share writing this with put and delete (see
             * writeReturnRow).
             *
             * ReturnRow information ends up fully materializing the Row
             * so if we can get the keys from both put and delete
             * methods in a RowImpl this can all be more easily centralized.
             * The proxy doesn't have easy access to them
             */
            OpResultWrapper opResult = (OpResultWrapper)result;
            RowImpl row = (RowImpl) opResult.getPreviousRow();
            if (row != null) {
                startMap(ns, RETURN_INFO);
                /* first write the value */
                ns.startMapField(EXISTING_VALUE);
                ProxySerialization.writeFieldValue(ns.getStream(), row);
                ns.endMapField(EXISTING_VALUE);

                Version version = opResult.getPreviousVersion();
                long creationTime = row.getCreationTime();
                long modTime = row.getLastModificationTime();

                writeMapField(ns, CREATION_TIME, creationTime);
                writeMapField(ns, EXISTING_MOD_TIME, modTime);
                writeMapField(ns, EXISTING_VERSION, version.toByteArray());
                writeMapField(ns, EXISTING_ROW_METADATA, row.getRowMetadata());
                endMap(ns, RETURN_INFO);
            }
        }
    }

    public static void writeSystemResponse(RequestContext rc,
                                           StatementResult res,
                                           String operationId,
                                           String statement)
        throws IOException {

        int state = res.isDone() ? 0 : 1; // 0 is COMPLETE, 1 is WORKING

        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, SYSOP_STATE, state);
            writeMapField(ns, OPERATION_ID, operationId);
            writeMapField(ns, STATEMENT, statement);
            writeMapField(ns, SYSOP_RESULT, res.getResult());
            ns.endMap(0);
        }
    }

    public static void writeListTableResponse(RequestContext rc,
                                              ListTableResponse response)
        throws IOException {

        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            String[] tables = response.getTables();
            if (tables.length > 0) {
                /* if no tables, no element */
                startArray(ns, TABLES);
                for (String table : tables) {
                    ns.stringValue(table);
                    ns.endArrayField(0);
                }
                endArray(ns, TABLES);
                writeMapField(ns, LAST_INDEX, response.getLastIndexReturned());
            }
            ns.endMap(0);
        }
    }

    public static void writeGetIndexesResponse(RequestContext rc,
                                               IndexResponse response)
        throws IOException {

        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            IndexInfo[] indexes = response.getIndexInfo();
            if (indexes.length > 0) {
                /* if no indexes, no element */
                startArray(ns, INDEXES);
                for (IndexInfo index : indexes) {
                    ns.startMap(0);
                    writeMapField(ns, NAME, index.getIndexName());
                    startArray(ns, FIELDS);
                    for (IndexField field : index.getIndexFields()) {
                        ns.startMap(0);
                        writeMapField(ns, PATH, field.getPath());
                        writeMapField(ns, TYPE, field.getType());
                        ns.endMap(0);
                        ns.endArrayField(0);
                    }
                    endArray(ns, FIELDS);
                    ns.endMap(0);
                    ns.endArrayField(0);
                }
                endArray(ns, INDEXES);
            }
            ns.endMap(0);
        }
    }

    public static void writeTableUsageResponse(RequestContext rc,
                                               TableUsageResponse response,
                                               String tableName)
        throws IOException {

        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, TABLE_NAME, tableName);
            final TableUsage[] usageRecords = response.getTableUsage();
            if (usageRecords != null && usageRecords.length > 0) {
                startArray(ns, TABLE_USAGE);
                for (TableUsage record : usageRecords) {
                    ns.startMap(0);
                    writeMapField(ns, START,
                                  timeToString(record.getStartTimeMillis()));
                    writeMapField(ns, TABLE_USAGE_PERIOD,
                                  record.getSecondsInPeriod());
                    writeMapField(ns, READ_UNITS, record.getReadUnits());
                    writeMapField(ns, WRITE_UNITS, record.getWriteUnits());
                    writeMapField(ns, STORAGE_GB, record.getStorageGB());
                    writeMapField(ns, READ_THROTTLE_COUNT,
                                  record.getReadThrottleCount());
                    writeMapField(ns, WRITE_THROTTLE_COUNT,
                                  record.getWriteThrottleCount());
                    writeMapField(ns, STORAGE_THROTTLE_COUNT,
                                  record.getStorageThrottleCount());
                    writeMapField(ns, MAX_SHARD_USAGE_PERCENT,
                                  record.getMaxPartitionUsage());
                    ns.endMap(0);
                    ns.endArrayField(0);
                }
                endArray(ns, TABLE_USAGE);
            }
            ns.endMap(0);
        }
    }

    public static void writeInternalOpResponse(ByteOutputStream bos,
                                               String response)
        throws IOException {

        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(bos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, PAYLOAD, response);
            ns.endMap(0);
        }
    }

    /*
     * TABLE_NAME (string)
     * NEXT_START_TIME (long)
     * REPLICA_STATS (Map<string, Array<ReplicaStats>>)
     *   key - replicaName (string)
     *   value - Array<ReplicaStats>
     *     <ReplicaStats>:
     *       TIME (long)
     *       REPLICA_LAG (int)
     */
    public static void writeReplicaStatsResponse(ByteOutputStream bos,
                                                 ReplicaStatsResponse response,
                                                 String tableName)
         throws IOException {

        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(bos.getByteBuf())) {

            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);

            writeMapField(ns, TABLE_NAME, tableName);
            writeMapField(ns, NEXT_START_TIME, response.getNextStartTime());

            final Map<String, List<ReplicaStats>> records =
                 response.getStatsRecords();
            if (records != null && records.size() > 0) {
                startMap(ns, REPLICA_STATS);
                for (Map.Entry<String, List<ReplicaStats>> record :
                    records.entrySet()) {
                    String replica = record.getKey();
                    startArray(ns, replica);
                    for (ReplicaStats stats : record.getValue()) {
                        ns.startMap(0);
                        writeMapField(ns, TIME, stats.getTime());
                        writeMapField(ns, REPLICA_LAG, stats.getReplicaLag());
                        ns.endMap(0);
                        ns.endArrayField(0);
                    }
                    endArray(ns, replica);
                }
                endMap(ns, REPLICA_STATS);
            }

            ns.endMap(0);
        }
    }

    /**
     * writes an error response into the payload
     *   "error_code": int (code)
     *   "exception": "..."
     *   // optional TODO if possible: FUTURE
     *   "consumed": {
     *      "read_units": int,
     *      "read_kb": int,
     *      "write_kb": int
     *    }
     *   "retry_hint": ...
     */
    public static void writeErrorResponse(RequestContext rc,
                                          String message,
                                          int errorCode)
        throws IOException {

        try (NettyByteOutputStream nos =
             new NettyByteOutputStream(rc.bbos.getByteBuf())) {
            NsonSerializer ns = new NsonSerializer(nos);
            ns.startMap(0);
            writeMapField(ns, ERROR_CODE, errorCode);
            writeMapField(ns, EXCEPTION, message);
            ns.endMap(0);
        }
    }

    public static void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     int value) throws IOException {
        ns.startMapField(fieldName);
        ns.integerValue(value);
        ns.endMapField(fieldName);
    }

    public static void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     long value) throws IOException {
        ns.startMapField(fieldName);
        ns.longValue(value);
        ns.endMapField(fieldName);
    }

    public static void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     byte[] value) throws IOException {
        ns.startMapField(fieldName);
        ns.binaryValue(value);
        ns.endMapField(fieldName);
    }

    public static void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     byte[] value,
                                     int offset,
                                     int length) throws IOException {
        ns.startMapField(fieldName);
        ns.binaryValue(value, offset, length);
        ns.endMapField(fieldName);
    }

    public static void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     String[] value) throws IOException {
        if (value == null || value.length == 0) {
            return;
        }
        ns.startMapField(fieldName);
        ns.startArray(0);
        for (String s : value) {
            ns.stringValue(s);
            ns.incrSize(1);
        }
        ns.endArray(0);
        ns.endMapField(fieldName);
    }

    public static void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     int[] value) throws IOException {
        if (value == null || value.length == 0) {
            return;
        }
        ns.startMapField(fieldName);
        ns.startArray(0);
        for (int i : value) {
            ns.integerValue(i);
            ns.incrSize(1);
        }
        ns.endArray(0);
        ns.endMapField(fieldName);
    }

    public static void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     String value) throws IOException {
        /* allow null to be a no-op */
        if (value == null) {
            return;
        }
        ns.startMapField(fieldName);
        ns.stringValue(value);
        ns.endMapField(fieldName);
    }

    public static void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     boolean value) throws IOException {
        ns.startMapField(fieldName);
        ns.booleanValue(value);
        ns.endMapField(fieldName);
    }

    protected static void writeLimits(NsonSerializer ns, TableLimits limits)
        throws IOException {
        if (limits != null) {
            startMap(ns, LIMITS);
            writeMapField(ns, READ_UNITS, limits.getReadUnits());
            writeMapField(ns, WRITE_UNITS, limits.getWriteUnits());
            writeMapField(ns, STORAGE_GB, limits.getTableSize());
            int mode = -1;
            if (limits.modeIsAutoScaling()) {
                mode = ON_DEMAND;
            } else if (limits.modeIsProvisioned()) {
                mode = PROVISIONED;
            } else {
                throw new IllegalArgumentException("Invalid TableLimits, " +
                    "unknown mode");
            }
            writeMapField(ns, LIMITS_MODE, mode);
            endMap(ns, LIMITS);
        }
    }

    private static void writeConsumedCapacity(NsonSerializer ns,
                                              DataServiceHandler handler,
                                              int readUnits,
                                              int readKB,
                                              int writeKB)
        throws IOException {

        /* consumed capacity has no meaning on-premise */
        if (handler.isOnPrem()) {
            return;
        }

        startMap(ns, CONSUMED);
        writeMapField(ns, READ_UNITS, readUnits);
        writeMapField(ns, READ_KB, readKB);
        writeMapField(ns, WRITE_KB, writeKB);
        endMap(ns, CONSUMED);
    }

    /*
     * If the driver's view of the shard set is different than the
     * proxy's view, write the shard set back to the driver.
     * Topology info:
     *   "topology_info" : {
     *      "PROXY_TOPO_SEQNUM" : int
     *      "SHARD_IDS" : [ int, ... ]
     * }
     */
    private static void writeTopologyInfo(NsonSerializer ns,
                                          Topology proxyTopo,
                                          int driverTopoSeqNum)
        throws IOException {
        writeTopologyInfo(ns, proxyTopo, driverTopoSeqNum, -1);
    }

    /*
     * If the driver's view of the shard set is different than the
     * proxy's view, write the shard set back to the driver.
     * Topology info:
     *   "topology_info" : {
     *      "PROXY_TOPO_SEQNUM" : int
     *      "SHARD_IDS" : [ int, ... ]
     * }
     * if queryVersion is non-negative, and below QUERY_V4, write
     * topo info back in QUERY_V3 compatible fashion:
     *  "PROXY_TOPO_SEQNUM" : int
     *  "SHARD_IDS" : [ int, ... ]
     *
     * A negative queryVersion says "this is not a query operation", so no
     * backward-compatibility is needed.
     */
    private static void writeTopologyInfo(NsonSerializer ns,
                                          Topology proxyTopo,
                                          int driverTopoSeqNum,
                                          int queryVersion)
        throws IOException {

        int proxyTopoSeqNum = proxyTopo.getSequenceNumber();

        /* don't check driver topo num if < QUERY_V4 */
        if ((driverTopoSeqNum >= -1 && proxyTopoSeqNum > driverTopoSeqNum) ||
            (queryVersion > 0 && queryVersion < QUERY_V4)) {

            if (queryVersion < 0 || queryVersion >= QUERY_V4) {
                startMap(ns, TOPOLOGY_INFO);
            }
            writeMapField(ns, PROXY_TOPO_SEQNUM, proxyTopoSeqNum);
            List<RepGroupId> groupIds = proxyTopo.getSortedRepGroupIds();
            if (groupIds.size() > 0) {
                int[] shardIds = new int[groupIds.size()];
                int i = 0;
                for (RepGroupId id : groupIds) {
                    shardIds[i++] = id.getGroupId();
                }
                writeMapField(ns, SHARD_IDS, shardIds);
            }
            if (queryVersion < 0 || queryVersion >= QUERY_V4) {
                endMap(ns, TOPOLOGY_INFO);
            }
        }
    }

// TODO: figure out how to call this from ProxySerialization
// copied verbatim from there
    /**
     * Writes Table information to OutputStream
     * Format:
     *  #tables (1)
     *  table namespace
     *  for each table:
     *    table names
     *    access (QueryOperation)
     */
    static void writeTableAccessInfo(NioByteOutputStream out,
                                     PrepareCB cbInfo)
        throws IOException {

        String[] notTargetTables = cbInfo.getNotTargetTables();
        int num = (notTargetTables == null) ? 1 : notTargetTables.length + 1;

        out.writeByte(num);
        SerializationUtil.writeString(out, cbInfo.getNamespace());
        /* Target table */
        SerializationUtil.writeString(out, cbInfo.getTableName());
        /* Other non-target tables if exists */
        if (notTargetTables != null) {
            for (String name : notTargetTables) {
                SerializationUtil.writeString(out, name);
            }
        }
        out.writeByte(cbInfo.getOperation().ordinal());
    }


    /**
     * Serialize the prepared query.
     * Serialized query is:
     *  table access info
     *  prepared query itself
     */
    private static void writePreparedQuery(NsonSerializer ns,
                                           NioByteOutputStream buf,
                                           PrepareCB cbInfo,
                                           PreparedStatementImpl prep,
                                           Topology topo)
        throws IOException {

        if (buf.isDirect()) {
            throw new IOException("Invalid direct buffer in output stream");
        }

        /*
         * write the namespace, tablename and operation as separate fields,
         * so the driver doesn't have to try to parse them out of the
         * byte array
         */
        writeMapField(ns, NAMESPACE, cbInfo.getNamespace());
        writeMapField(ns, TABLE_NAME, cbInfo.getTableName());
        writeMapField(ns, QUERY_OPERATION,
                      (int)cbInfo.getOperation().ordinal());
        int maxParallelism = computeMaxParallelism(prep, topo);
        writeMapField(ns, MAX_QUERY_PARALLELISM, maxParallelism);

        /*
         * serialize the table access info and prepared query into a
         * byte array, then write that to the nson output.
         * This depends on the passed-in buf being not-direct
         */

        /*
         * the driver expects both of these to be in a full single byte array
         */
        buf.setWriteIndex(0); // reset to beginning of temp buffer
        writeTableAccessInfo(buf, cbInfo);
        prep.serializeForProxy(buf);

        writeMapField(ns, PREPARED_QUERY, buf.array(), 0, buf.getOffset());
    }

    /*
     * Single partition, along with any query that requires sorting or
     * aggregation on client: 0 (indicates no parallelism possible)
     * All shards: num shards
     * All partitions: number of partitions
     */
    private static int computeMaxParallelism(PreparedStatementImpl prep,
                                             Topology topo) {
        if (prep.getDistributionKind() == null) {
            /* this happens for update queries */
            return 0;
        }

        if (!prep.isSimpleQuery() || prep.getDistributionKind().equals(
                PreparedStatementImpl.DistributionKind.SINGLE_PARTITION)) {
            return 0;
        }
        if (prep.getDistributionKind().equals(
                PreparedStatementImpl.DistributionKind.ALL_SHARDS)) {
            return topo.getNumRepGroups();
        }
        /* else ALL_PARTITIONS */
        return topo.getNumPartitions();
    }

    public static void writeQueryFinish(NsonSerializer ns,
                                        DataServiceHandler handler,
                                        short queryVersion,
                                        QueryStatementResultImpl qres,
                                        PreparedStatementImpl prep,
                                        byte[] contKey,
                                        int readUnits,
                                        int readKB,
                                        int writeKB,
                                        Topology topo,
                                        int driverTopoSeqNum,
                                        PrepareCB cbInfo,
                                        boolean isPrepared,
                                        boolean getQueryPlan,
                                        boolean getQuerySchema,
                                        boolean isSimpleQuery,
                                        int batchCounter,
                                        String proxyBatchName)
        throws IOException {

        /* create an array-backed stream for serializing byte arrays */
        NioByteOutputStream buf = new NioByteOutputStream(8192, false);

        if (qres != null && qres.hasSortPhase1Result()) {
            /* generate Binary field for Phase1 results */
            qres.writeSortPhase1Results(buf);
            if (buf.getOffset() > 0) {
                writeMapField(ns, SORT_PHASE1_RESULTS,
                              buf.array(), 0, buf.getOffset());
            }
        }

        writeConsumedCapacity(ns, handler, readUnits, readKB, writeKB);
        writeTopologyInfo(ns, topo, driverTopoSeqNum, queryVersion);

        if (contKey != null) {
            writeMapField(ns, CONTINUATION_KEY, contKey);
        }

        if (isPrepared == false) {
            /* Write the proxy-side query plan. */
            writePreparedQuery(ns, buf, cbInfo, prep, topo);
            /* Write the driver-side query plan. */
            FieldValueWriterImpl valWriter = new FieldValueWriterImpl();
            buf.setWriteIndex(0); // reset to beginning
            prep.serializeForDriver(buf, queryVersion, valWriter);
            /* note: above may add numIterators, numRegisters, externalVars */
            // TODO: separate into native nson
            if (buf.getOffset() > 0) {
                writeMapField(ns, DRIVER_QUERY_PLAN,
                              buf.array(), 0, buf.getOffset());
            }
            if (getQueryPlan) {
                writeMapField(ns, QUERY_PLAN_STRING, prep.toString());
            }
            if (getQuerySchema) {
                writeMapField(ns, QUERY_RESULT_SCHEMA,
                              prep.getResultDef().toString());
            }
        } else if (!isSimpleQuery) {
            if (qres != null) {
                writeMapField(ns, REACHED_LIMIT, qres.reachedLimit());
            }
        }

        if (qres == null) {
            return;
        }

        RuntimeControlBlock rcb = qres.getRCB();

        /* Write the virtual scan info */
        List<VirtualScan> virtualScans = rcb.getNewVirtualScans();

        if (virtualScans != null) {
            startArray(ns, VIRTUAL_SCANS);
            for (VirtualScan vs : virtualScans) {
                writeVirtualScan(ns, vs, queryVersion);
                ns.endArrayField(0);
            }
            endArray(ns, VIRTUAL_SCANS);
        }

        if (rcb.getTraceLevel() > 0) {

            Map<String, String> traces = new TreeMap<String, String>();

            String proxyTrace =  rcb.getTrace();
            if (proxyTrace != null) {
                traces.put(proxyBatchName, proxyTrace);
            }

            Map<String, String> rntraces = prep.getQueryPlan().getRNTraces(rcb);
            if (rntraces != null) {
                traces.putAll(rntraces);
            }

            if (!traces.isEmpty()) {
                startArray(ns, QUERY_BATCH_TRACES);
                for (Map.Entry<String, String> entry : traces.entrySet()) {
                    ns.stringValue(entry.getKey());
                    ns.endArrayField(0);
                    ns.stringValue(entry.getValue());
                    ns.endArrayField(0);
                }
                endArray(ns, QUERY_BATCH_TRACES);
            }
        }
    }

    private static void writeVirtualScan(NsonSerializer ns,
                                         VirtualScan vs,
                                         short queryVersion)
        throws IOException {

        ns.startMap(0);
        writeMapField(ns, VIRTUAL_SCAN_SID, vs.sid());
        writeMapField(ns, VIRTUAL_SCAN_PID, vs.pid());

        int numTables = 1;
        if (queryVersion >= QUERY_V5) {
            numTables = vs.numTables();
            writeMapField(ns, VIRTUAL_SCAN_NUM_TABLES, numTables);
        }

        for (int t = 0; t < numTables; ++t) {
            writeMapField(ns, VIRTUAL_SCAN_CURRENT_INDEX_RANGE,
                          vs.currentIndexRange(t));
            writeMapField(ns, VIRTUAL_SCAN_PRIM_KEY,
                          vs.primKey(t));
            writeMapField(ns, VIRTUAL_SCAN_SEC_KEY,
                          vs.secKey(t));
            writeMapField(ns, VIRTUAL_SCAN_MOVE_AFTER,
                          vs.moveAfterResumeKey(t));
            writeMapField(ns, VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY,
                          vs.descResumeKey(t));
            writeMapField(ns, VIRTUAL_SCAN_JOIN_PATH_TABLES,
                          vs.joinPathTables(t));
            writeMapField(ns, VIRTUAL_SCAN_JOIN_PATH_KEY,
                          vs.joinPathKey(t));
            writeMapField(ns, VIRTUAL_SCAN_JOIN_PATH_SEC_KEY,
                          vs.joinPathSecKey(t));
            writeMapField(ns, VIRTUAL_SCAN_JOIN_PATH_MATCHED,
                          vs.joinPathMatched(t));
        }
        ns.endMap(0);
    }

    protected static int getReadKB(int readUnits, boolean isAbsolute) {
        return (isAbsolute ? readUnits >> 1 : readUnits);
    }

    /**
     * Writes a row which includes row metadata and the value
     */
    protected static void writeRow(NsonSerializer ns,
                                   Result result,
                                   TableAPIImpl tableApi,
                                   TableImpl table,
                                   RowSerializer primaryKey)
        throws IOException {

        startMap(ns, ROW);
        /* row metadata */
        writeMapField(ns, CREATION_TIME, result.getPreviousCreationTime());
        writeMapField(ns, MODIFIED, result.getPreviousModificationTime());
        writeMapField(ns, EXPIRATION, result.getPreviousExpirationTime());
        writeMapField(ns, ROW_VERSION,
                      result.getPreviousVersion().toByteArray());
        writeMapField(ns, ROW_METADATA, result.getPreviousValue() != null ?
            result.getPreviousValue().getRowMetadata() : null);
        /* row value is last */
        /* TODO: when available, direct Avro to NSON? */
        ns.startMapField(VALUE);
        RowReaderImpl reader =
            new RowReaderImpl(ns.getStream(), table);
        tableApi.createRowFromGetResult(
            result, primaryKey, reader);
        reader.done();
        ns.endMapField(VALUE);
        endMap(ns, ROW);
    }

    protected static void writeReturnRow(NsonSerializer ns,
                                         Result result,
                                         ReturnRow returnRow,
                                         TableAPIImpl tableApi,
                                         RowSerializer rowKey)
        throws IOException {

        /*
         * Return row is only written if the existing row is available.
         */
        if (result.getPreviousVersion() == null) {
            return;
        }

        startMap(ns, RETURN_INFO);

        /* first write the value */
        ns.startMapField(EXISTING_VALUE);
        RowReaderImpl reader =
            new RowReaderImpl(ns.getStream(), rowKey.getTable());
        tableApi.initReturnRowFromResult(returnRow, rowKey, result, reader);
        reader.done();
        ns.endMapField(EXISTING_VALUE);

        Version version = reader.getVersion();

        long creationTime = result.getPreviousCreationTime();
        writeMapField(ns, CREATION_TIME, creationTime);

        long modTime = result.getPreviousModificationTime();
        writeMapField(ns, EXISTING_MOD_TIME, modTime);

        if (version != null) {
            writeMapField(ns, EXISTING_VERSION, version.toByteArray());
        }
        String prevRowMetadata = result.getPreviousValue().getRowMetadata();
        if (prevRowMetadata != null) {
            writeMapField(ns, EXISTING_ROW_METADATA, prevRowMetadata);
        }
        endMap(ns, RETURN_INFO);
    }

    protected static void writeGeneratedValue(NsonSerializer ns,
                                              FieldValue value)
        throws IOException {

            ns.startMapField(GENERATED);
            ProxySerialization.writeFieldValue(ns.getStream(), value);
            ns.endMapField(GENERATED);
    }

    protected static void writeReplicas(NsonSerializer ns,
                                        Collection<ReplicaInfo> replicas,
                                        TenantManager tm)
        throws IOException {

        startArray(ns, REPLICAS);
        for (ReplicaInfo rep : replicas) {
            ns.startMap(0);
            writeMapField(ns, REGION,
            		      tm.translateToRegionName(rep.getServiceName()));
            writeMapField(ns, TABLE_STATE, getReplicaState(rep.getState()));
            if (rep.getTableOcid() != null) {
                writeMapField(ns, TABLE_OCID, rep.getTableOcid());
            }
            if (rep.getWriteLimit() > 0) {
                writeMapField(ns, WRITE_UNITS, rep.getWriteLimit());
            }
            if (rep.getMode() != null) {
                writeMapField(ns, LIMITS_MODE, getCapacityMode(rep.getMode()));
            }
            ns.endMap(0);
            ns.endArrayField(0);
        }
        endArray(ns, REPLICAS);
    }

    protected static void startMap(NsonSerializer ns, String name)
        throws IOException {
        ns.startMapField(name);
        ns.startMap(0);
    }

    protected static void endMap(NsonSerializer ns, String name)
        throws IOException {
        ns.endMap(0);
        ns.endMapField(name);
    }

    protected static void startArray(NsonSerializer ns, String name)
        throws IOException {
        ns.startMapField(name);
        ns.startArray(0);
    }

    protected static void endArray(NsonSerializer ns, String name)
        throws IOException {
        ns.endArray(0);
        ns.endMapField(name);
    }

    /*
     * Use Nson value for this
     */
    protected static String timeToString(long timestamp) {
        return new oracle.nosql.nson.values.TimestampValue(timestamp).
            getString();
    }
}
