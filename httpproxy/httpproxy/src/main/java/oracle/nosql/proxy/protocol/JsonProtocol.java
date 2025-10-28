/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.nosql.proxy.protocol;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import oracle.kv.Consistency;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.query.QueryStatementResultImpl;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.IdentityColumnInfo;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.StringDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableJsonUtils;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.query.PrepareCallback.QueryOperation;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.nosql.common.JsonBuilder;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.ProxySerialization;
import oracle.nosql.proxy.sc.ListTableInfoResponse;
import oracle.nosql.proxy.sc.TableUtils;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.util.fault.ErrorCode;
import oracle.nosql.util.tmi.IndexInfo;
import oracle.nosql.util.tmi.IndexInfo.IndexField;
import oracle.nosql.util.tmi.IndexInfo.IndexState;
import oracle.nosql.util.tmi.KmsKeyInfo;
import oracle.nosql.util.tmi.ReplicaInfo;
import oracle.nosql.util.tmi.ReplicaInfo.ReplicaState;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableInfo.ActivityPhase;
import oracle.nosql.util.tmi.TableLimits;
import oracle.nosql.util.tmi.TableUsage;
import oracle.nosql.util.tmi.WorkRequest;

/**
 * JSON protocol related:
 *  - Constants
 *  - Class JsonPayLoad to parse the JSON payload in request
 *  - Class JsonBuilder and methods to build up the response payload
 */
/* ignore deprecated code related to JsonFactory creation */
@SuppressWarnings("deprecation")
public class JsonProtocol extends Protocol {

    /* System tags appended to free table */
    public static final Map<String, Map<String, Object>> FREE_TIER_SYS_TAGS =
         Collections.singletonMap("orcl-cloud",
             Collections.singletonMap("free-tier-retained", "true"));

    public static final String REST_VERSION_V1 = "20190828";
    public static final String REST_CURRENT_VERSION = REST_VERSION_V1;

    public static final String URL_PATH_DELIMITER = "/";
    public static final String DATE_FORMAT_PATTERN =
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static final String COMPARTMENT_ID = "compartmentId";
    public static final String CONSISTENCY = "consistency";
    public static final String INDEX_NAME = "indexName";
    public static final String IS_GET_QUERY_PLAN = "isGetQueryPlan";
    public static final String IS_GET_RETURN_ROW = "isGetReturnRow";
    public static final String IS_IF_EXISTS = "isIfExists";
    public static final String IF_MATCH = "if-match";
    public static final String KEY = "key";
    public static final String LIFE_CYCLE_STATE = "lifecycleState";
    public static final String LIMIT = "limit";
    public static final String NAME = "name";
    public static final String PAGE = "page";
    public static final String QUERY_PLAN = "queryPlan";
    public static final String SORT_BY = "sortBy";
    public static final String SORT_ORDER = "sortOrder";
    public static final String STATEMENT = "statement";
    public static final String TABLE_NAME_OR_ID = "tableNameOrId";
    public static final String TIME_START = "timeStart";
    public static final String TIME_END = "timeEnd";
    public static final String TIMEOUT_IN_MS = "timeoutInMs";
    public static final String WORK_REQUEST_ID = "workRequestId";

    public static final String OPC_REQUEST_ID = "opc-request-id";
    public static final String OPC_RETRY_TOKEN = "opc-retry-token";
    public static final String OPC_WORK_REQUEST_ID = "opc-work-request-id";
    public static final String ETAG = "etag";
    public static final String OPC_NEXT_PAGE = "opc-next-page";
    public static final String OPC_RETRY_AFTER = "retry-after";

    public static final String TABLE_RESOURCE_KIND = "table";
    public static final String INDEX_RESOURCE_KIND = "index";

    public static final String ROW_RESOURCE_KIND = "row";
    public static final String QUERY_RESOURCE_KIND = "query";

    public static final String SORT_BY_ENUM_TIMECREATED = "timeCreated";
    public static final String SORT_BY_ENUM_DISPLAYNAME = "name";
    public static final String SORT_ORDER_ASCENDING = "ASC";
    public static final String SORT_ORDER_DESCENDING = "DESC";

    /* CreateTableDetails */
    public static final String CREATE_TABLE_DETAILS = "CreateTableDetails";
    public static final String DDL_STATEMENT = "ddlStatement";
    public static final String TABLE_LIMITS = "tableLimits";
    public static final String SCHEMA = "schema";
    public static final String MAX_READ_UNITS = "maxReadUnits";
    public static final String MAX_WRITE_UNITS = "maxWriteUnits";
    public static final String MAX_STORAGE_IN_GBS = "maxStorageInGBs";
    public static final String CAPACITY_MODE = "capacityMode";
    public static final String PROVISIONED = "PROVISIONED";
    public static final String ON_DEMAND = "ON_DEMAND";
    public static final String IS_AUTO_RECLAIMABLE = "isAutoReclaimable";
    public static final String FREE_FORM_TAGS = "freeformTags";
    public static final String DEFINED_TAGS = "definedTags";
    public static final String SYSTEM_TAGS = "systemTags";

    /* UpdateTableDetails */
    public static final String UPDATE_TABLE_DETAILS = "UpdateTableDetails";

    /* CreateIndexDetails */
    public static final String CREATE_INDEX_DETAILS = "CreateIndexDetails";
    public static final String KEYS = "keys";
    public static final String COLUMN_NAME = "columnName";
    public static final String JSON_PATH = "jsonPath";
    public static final String JSON_FIELD_TYPE = "jsonFieldType";

    /* ChangeTableCompartmentDetails */
    public static final String CHANGE_TABLE_COMPARTMENT_DETAILS =
            "ChangeTableCompartmentDetails";
    public static final String FROM_COMPARTMENT_ID = "fromCompartmentId";
    public static final String TO_COMPARTMENT_ID = "toCompartmentId";

    /* Put request payload */
    public static final String UPDATE_ROW_DETAILS ="UpdateRowDetails";
    public static final String IDENTITY_CACHE_SIZE ="identityCacheSize";
    public static final String IS_EXACT_MATCH = "isExactMatch";
    public static final String IS_TTL_USE_TABLE_DEFAULT = "isTtlUseTableDefault";
    public static final String OPTION = "option";
    public static final String TTL = "ttl";
    public static final String VALUE = "value";

    /* RequestUsage */
    private static final String READ_UNITS_CONSUMED = "readUnitsConsumed";
    private static final String WRITE_UNITS_CONSUMED = "writeUnitsConsumed";

    /* UpdateRowResult */
    private static final String VERSION = "version";
    private static final String EXISTING_VERSION = "existingVersion";
    private static final String EXISTING_VALUE = "existingValue";
    private static final String GENERATED_VALUE = "generatedValue";
    private static final String USAGE = "usage";

    /* Get Row*/
    private static final String TIME_OF_EXPIRATION = "timeOfExpiration";

    /* DeleteRowResult */
    private static final String IS_SUCCESS = "isSuccess";

    /* Summarize result */
    private static final String OPERATION = "operation";
    private static final String TABLE_NAME = "tableName";
    public static final String IS_IF_NOT_EXISTS = "isIfNotExists";
    private static final String SYNTAX_ERROR = "syntaxError";

    /* Query request payload */
    public static final String QUERY_DETAILS = "QueryDetails";
    public static final String IS_PREPARED = "isPrepared";
    public static final String CONSISTENCY_EVENTUAL = "EVENTUAL";
    public static final String CONSISTENCY_ABSOLUTE = "ABSOLUTE";
    public static final String MAX_READ_IN_KBS = "maxReadInKBs";
    public static final String VARIABLES = "variables";

    /* QueryResult */
    private static final String ITEMS = "items";

    /* Table */
    public static final String ID = "id";
    public static final String TIME_CREATED = "timeCreated";
    public static final String TIME_UPDATED = "timeUpdated";
    private static final String LIFE_CYCLE_DETAILS = "lifecycleDetails";
    private static final String PRIMARY_KEY = "primaryKey";
    private static final String SHARD_KEY = "shardKey";
    private static final String COLUMNS = "columns";
    private static final String TYPE = "type";
    private static final String IS_NULLABLE = "isNullable";
    private static final String DEFAULT_VALUE = "defaultValue";
    private static final String IS_AS_UUID = "isAsUuid";
    private static final String IS_GENERATED = "isGenerated";
    private static final String IDENTITY = "identity";
    private static final String IS_ALWAYS = "isAlways";
    private static final String IS_NULL = "isNull";
    public static final String SCHEMA_STATE = "schemaState";
    public static final String IS_MULTI_REGION = "isMultiRegion";
    public static final String IS_LOCAL_INITIALIZED =
            "isLocalReplicaInitialized";
    public static final String LOCAL_INIT_PERCENT =
            "localReplicaInitializationInPercent";
    public static final String REPLICAS = "replicas";

    /* TableCollection */
    private static final String MAX_AUTO_RECLAIMABLE_TABLES =
            "maxAutoReclaimableTables";
    private static final String AUTO_RECLAIMABLE_TABLES =
            "autoReclaimableTables";
    private static final String MAX_ON_DEMAND_CAPACITY_TABLES =
            "maxOnDemandCapacityTables";
    private static final String ON_DEMAND_CAPACITY_TABLES =
            "onDemandCapacityTables";
    private static final String AVAILABLE_REPLICATION_REGIONS =
            "availableReplicationRegions";

    /* TableUsageSummary */
    private static final String SECONDS_IN_PERIOD = "secondsInPeriod";
    private static final String READ_UNITS = "readUnits";
    private static final String WRITE_UNITS = "writeUnits";
    private static final String STORAGE_IN_GBS = "storageInGBs";
    private static final String READ_THROTTLE_COUNT = "readThrottleCount";
    private static final String WRITE_THROTTLE_COUNT = "writeThrottleCount";
    private static final String STORAGE_THROTTLE_COUNT = "storageThrottleCount";
    private static final String MAX_SHARD_SIZE_USAGE_IN_PERCENT =
            "maxShardSizeUsageInPercent";

    /* WorkRequest */
    private static final String OPERATION_TYPE = "operationType";
    private static final String STATUS = "status";
    private static final String RESOURCES = "resources";
    private static final String PERCENT_COMPLETE = "percentComplete";
    private static final String TIME_ACCEPTED = "timeAccepted";
    private static final String TIME_STARTED = "timeStarted";
    private static final String TIME_FINISHED = "timeFinished";
    private static final String ENTITY_TYPE = "entityType";
    private static final String ACTION_TYPE = "actionType";
    private static final String IDENTIFIER = "identifier";
    private static final String ENTITY_URI = "entityUri";
    private static final String CODE = "code";
    private static final String MESSAGE = "message";
    private static final String TIMESTAMP = "timestamp";

    /* GetIndex result */
    private static final String TABLE_ID = "tableId";

    /* AddReplica */
    public static final String CREATE_REPLICA_DETAILS = "CreateReplicaDetails";
    public static final String REGION = "region";

    /* For configuration APIs */
    public static final String UPDATE_CONFIGURATION_DETAILS =
        "UpdateConfigurationDetails";
    public static final String ENVIRONMENT = "environment";
    public static final String HOSTED_ENVIRONMENT = "HOSTED";
    public static final String MULTI_TENANCY_ENVIRONMENT = "MULTI_TENANCY";
    public static final String IS_OPC_DRY_RUN = "is-opc-dry-run";

    public static final String KMS_KEY = "kmsKey";
    public static final String KMS_VAULT_ID = "kmsVaultId";
    public static final String KMS_KEY_STATE = "kmsKeyState";

    public enum PutOption {
        IF_ABSENT,
        IF_PRESENT
    };

    public enum LifecycleState {
        ALL("All"),
        CREATING("Creating resource is in-progress"),
        UPDATING("Updating resource is in-progress"),
        ACTIVE("The resource is ACTIVE"),
        DELETING("Deleting resource is in-progress"),
        DELETED("The resource has been deleted"),
        FAILED("Operation on the resource failed"),
        INACTIVE("The resource is in-active");

        private String details;
        LifecycleState(String details) {
            this.details = details;
        }

        public String getDetails() {
            return details;
        }
    };

    public enum SortBy {
        NAME,
        TIMECREATED;
    };

    public enum SortOrder {
        ASC,
        DESC;
    };

    /*
     * common code to create a JSON parser
     */
    static private JsonFactory jsonFactory;
    static {
        /*
         * Enable some non-default features:
         *  - allow leading 0 in numerics
         *  - allow non-numeric values such as INF, -INF, NaN for float and
         *    double
         *
         * Disables:
         *  - The "NaN" ("not a number", that is, not real number) float/double
         *    values are output as quoted strings.
         */
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS,
                              true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS,
                              true);
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS,
                              false);
    }

    public static JsonParser createJsonParser(InputStream in)
        throws IOException {
        return jsonFactory.createParser(in);
    }

    public static JsonParser createJsonParser(Reader in)
        throws IOException {
        return jsonFactory.createParser(in);
    }

    /**
     * Builds the response payload for Get-Table operation
     *
     * TODO: Remove calling formatTableOcid() to replace '_' with '.' in table
     *       OCID, it should be handled in SC.
     */
    public static String buildTable(TableInfo ti,
                                    String[] tagJsons,
                                    TenantManager tm) {
        final JsonBuilder jb = JsonBuilder.create();

        LifecycleState lfstate = getLifecycleState(ti);
        jb.append(ID, ti.getTableOcid())
          .append(NAME, ti.getTableName())
          .append(COMPARTMENT_ID, ti.getCompartmentId())
          .append(TIME_CREATED, formatDateString(ti.getCreateTime()))
          .append(TIME_UPDATED, (ti.getUpdateTime() > 0 ?
                                 formatDateString(ti.getUpdateTime()) : null))
          .append(LIFE_CYCLE_STATE, lfstate.name())
          .append(LIFE_CYCLE_DETAILS, lfstate.getDetails())
          .append(DDL_STATEMENT, ti.getDdl())
          .append(SCHEMA_STATE, getSchemaState(ti.getSchemaState()))
          .append(IS_MULTI_REGION, ti.isMultiRegion());

        /* freeformTags, definedTags, systemTags */
        if (tagJsons != null) {
            appendTags(jb, tagJsons);
        }

        /* IsAutoReclaimable, timeOfExpiration */
        appendAutoReclaimable(jb, ti);

        /* tableLimits */
        appendTableLimits(jb, ti.getTableLimits());

        /* Schema */
        TableImpl table;
        if (ti.getSchema() != null) {
            jb.startObject(SCHEMA);
            table = TableJsonUtils.fromJsonString(ti.getSchema(), null);
            /* primaryKey */
            buildStringArray(jb, PRIMARY_KEY, table.getPrimaryKey());
            /* shardKey */
            buildStringArray(jb, SHARD_KEY, table.getShardKey());

            /* columns */
            jb.startArray(COLUMNS);
            RecordDefImpl rowDef = table.getRowDef();
            FieldMap fieldMap = table.getFieldMap();
            for (String fname : table.getFields()) {
                int pos = rowDef.getFieldPos(fname);
                FieldValue defVal = null;
                if (fieldMap.getFieldMapEntry(pos).hasDefaultValue()) {
                    defVal = table.getDefaultValue(fname);
                }

                FieldDef fdef = table.getField(fname);
                jb.startObject(null)
                  .append(NAME, fname)
                  .append(TYPE, getType(fdef))
                  .append(IS_NULLABLE, table.isNullable(fname))
                  .append(DEFAULT_VALUE,
                          (defVal != null ? defVal.toString() : null))
                  .append(IS_AS_UUID, fdef.isUUIDString())
                  .append(IS_GENERATED,
                          (fdef.isUUIDString() ?
                              ((StringDefImpl)fdef).isGenerated() : false))
                  .endObject();
            }
            jb.endArray();

            /* TTL */
            if (table.getDefaultTTL() != null) {
                jb.append(TTL, table.getDefaultTTL().toDays());
            }

            /* Identity */
            IdentityColumnInfo idInfo = table.getIdentityColumnInfo();
            if (idInfo != null) {
                jb.startObject(IDENTITY)
                  .append(COLUMN_NAME,
                          table.getFields().get(idInfo.getIdentityColumn()))
                  .append(IS_ALWAYS, idInfo.isIdentityGeneratedAlways())
                  .append(IS_NULL, idInfo.isIdentityOnNull())
                  .endObject();
            }
            jb.endObject();

            /* MR table info */
            if (ti.isMultiRegion()) {
                jb.append(IS_LOCAL_INITIALIZED, ti.isInitialized());
                jb.append(LOCAL_INIT_PERCENT, ti.getInitializePercent());
                jb.startArray(REPLICAS);
                ti.getReplicas().forEach((serviceName, info) -> {
                    appendReplica(jb,
                        tm.translateToRegionName(serviceName),
                        info);
                });
                jb.endArray();
            }
        }

        return jb.toString();
    }

    private static String getSchemaState(TableInfo.SchemaState state) {
        switch(state) {
        case MUTABLE:
            return "MUTABLE";
        case FROZEN:
            return "FROZEN";
        default:
            throw new IllegalArgumentException("Invalid schema state: " + state);
        }
    }

    private static void appendReplica(JsonBuilder jb,
                                      String regionName,
                                      ReplicaInfo info) {

        LifecycleState state = getLifecycleState(info.getState());
        jb.startObject(null)
          .append(REGION, regionName)
          .append(TABLE_ID, info.getTableOcid())
          .append(MAX_WRITE_UNITS, info.getWriteLimit())
          .append(LIFE_CYCLE_STATE, state.name())
          .append(LIFE_CYCLE_DETAILS, state.getDetails());
        if (info.getMode() != null) {
            jb.append(CAPACITY_MODE, getCapacityMode(info.getMode()));
        }
        jb.endObject();
    }

    private static String getType(FieldDef def) {
        StringBuilder sb = new StringBuilder();
        appendType(sb, def);
        return sb.toString();
    }

    private static void appendType(StringBuilder sb, FieldDef def) {
        String type = (def.isFixedBinary()) ?
                       FieldDef.Type.BINARY.name() : def.getType().name();

        /* append type name */
        sb.append(type);

        /*
         * append additional information for types:
         *   TIMESTAMP(precision),
         *   BINARY(size),
         *   ENUM(value[,value[,...]]),
         *   ARRAY(type),
         *   MAP(type),
         *   RECORD(field1 type1[, field2 type2 [,...]])
         */
        if (def.isTimestamp()) {
            sb.append("(").append(def.asTimestamp().getPrecision()).append(")");
        } else if (def.isFixedBinary()) {
            sb.append("(").append(def.asFixedBinary().getSize()).append(")");
        } else if (def.isEnum()) {
            sb.append("(");
            appendEnumValues(sb, def.asEnum());
            sb.append(")");
        } else if (def.isArray()) {
            sb.append("(");
            appendType(sb, def.asArray().getElement());
            sb.append(")");
        } else if (def.isMap()) {
            sb.append("(");
            appendType(sb, def.asMap().getElement());
            sb.append(")");
        } else if (def.isRecord()) {
            /* Record */
            sb.append("(");
            appendRecordElements(sb, def.asRecord());
            sb.append(")");
        }
    }

    private static void appendEnumValues(StringBuilder sb, EnumDef def) {
        boolean first = true;
        for (String value : def.getValues()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(value);
        }
    }

    private static void appendRecordElements(StringBuilder sb, RecordDef rdef) {
        boolean first = true;
        for (String field : rdef.getFieldNames()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(field).append(" ");
            appendType(sb, rdef.getFieldDef(field));
        }
    }

    private static void buildStringArray(JsonBuilder jb,
                                         String name,
                                         List<String> array) {
        jb.startArray(name);
        for (String str : array) {
            jb.append(str);
        }
        jb.endArray();
    }

    private static String formatDateString(long millis) {
        return formatDateString(new Timestamp(millis));
    }

    private static String formatDateString(Timestamp ts) {
        return TimestampUtils.formatString(ts, DATE_FORMAT_PATTERN, true);
    }

    /**
     * Build Get-WorkRequest response
     */
    public static String buildWorkRequest(WorkRequest workRequest) {
        final JsonBuilder jb = JsonBuilder.create();
        buildWorkRequestSummary(jb, workRequest);
        return jb.toString();
    }

    private static void buildWorkRequestSummary(JsonBuilder jb,
                                                WorkRequest request) {

        final WorkRequest.Status status = request.getStatus();
        jb.append(OPERATION_TYPE, mapOperationType(request.getType()))
          .append(STATUS, request.getStatus().name())
          .append(ID, request.getId())
          .append(COMPARTMENT_ID, request.getCompartmentId())
          .append(PERCENT_COMPLETE, getWorkRequestProgress(status))
          .append(TIME_ACCEPTED, formatDateString(request.getTimeAccepted()));

        if (request.getTimeStarted() > 0) {
            jb.append(TIME_STARTED, formatDateString(request.getTimeStarted()));
        }

        if (request.getTimeFinished() > 0) {
            jb.append(TIME_FINISHED, formatDateString(request.getTimeFinished()));
        }

        String entityUrl = null;
        String entityId = request.getEntityId();
        if (request.getEntityType() == WorkRequest.EntityType.TABLE) {
            entityUrl = buildGetTableUrl(REST_CURRENT_VERSION,
                                         request.getEntityName(),
                                         request.getCompartmentId());
            if (entityId == null) {
                entityId = NameUtils.makeQualifiedName(
                                request.getCompartmentId(),
                                request.getEntityName());
            }
        } else {
            entityUrl = buildGetConfigurationUrl(REST_CURRENT_VERSION,
                                                 request.getCompartmentId());
        }

        /* resources */
        jb.startArray(RESOURCES)
            .startObject(null)
                .append(ENTITY_TYPE, request.getEntityType().name())
                .append(ACTION_TYPE, request.getActionType().name())
                .append(IDENTIFIER, request.getEntityId())
                .append(ENTITY_URI, entityUrl)
             .endObject()
          .endArray();
    }

    private static String mapOperationType(WorkRequest.OperationType type) {
        switch(type) {
        case CREATE_TABLE:
        case UPDATE_TABLE:
        case DELETE_TABLE:
            return type.name();
        case UPDATE_KMS_KEY:
        case REMOVE_KMS_KEY:
            return "UPDATE_CONFIGURATION";
        default:
            throw new IllegalArgumentException(
                "Unknown WorkRequest OperationType: " + type);
        }
    }

    /**
     * Build List-WorkRequests response
     */
    public static String buildWorkRequestCollection(WorkRequest[] requests) {
        JsonBuilder jb = JsonBuilder.create();;
        jb.startArray(ITEMS);
        for (WorkRequest request : requests) {
            jb.startObject(null);
            buildWorkRequestSummary(jb, request);
            jb.endObject();
        }
        jb.endArray();
        return jb.toString();
    }

    /**
     * Build List-WorkRequestErrors response
     */
    public static String buildWorkRequestErrors(WorkRequest workRequest) {
        final JsonBuilder jb = JsonBuilder.create();
        jb.startArray(ITEMS);
        if (workRequest.getErrorCode() != ErrorCode.NO_ERROR) {
            buildWorkRequestError(jb, workRequest);
        }
        jb.endArray();
        return jb.toString();
    }

    private static void buildWorkRequestError(JsonBuilder jb,
                                              WorkRequest workRequest) {
        long time = (workRequest.getTimeFinished() > 0) ?
                     workRequest.getTimeFinished() :
                     workRequest.getTimeStarted();

        jb.startObject(null)
            .append(CODE, workRequest.getErrorCode().getType())
            .append(MESSAGE, workRequest.getErrorMessage())
            .append(TIMESTAMP, formatDateString(time))
          .endObject();
    }

    /**
     * Build List-WorkRequestLogs response
     */
    public static String buildWorkRequestLogs(WorkRequest workRequest) {
        final JsonBuilder jb = JsonBuilder.create();
        jb.startArray(ITEMS);
        buildWorkRequestLog(jb, workRequest);
        jb.endArray();
        return jb.toString();
    }

    private static void buildWorkRequestLog(JsonBuilder jb,
                                            WorkRequest workRequest) {
        long time = (workRequest.getTimeFinished() > 0) ?
                     workRequest.getTimeFinished() :
                     workRequest.getTimeAccepted();
        jb.startObject(null)
            .append(MESSAGE, workRequest.getErrorMessage())
            .append(TIMESTAMP, formatDateString(time))
        .endObject();
    }

    /**
     * Since we don't have progress percentage information returned from SC,
     * just simple return the progress based on the state:
     *  ACCEPTED: 0
     *  IN_PROGRESS: 50
     *  SUCCEEDED/FAILED: 100
     */
    private static int getWorkRequestProgress(WorkRequest.Status state) {
        if (state == WorkRequest.Status.ACCEPTED) {
            return 0;
        } else if (state == WorkRequest.Status.IN_PROGRESS) {
            return 50;
        }
        /* SUCCEEDED, fAILED */
        return 100;
    }

    /**
     * Builds GET TABLE url:
     *  /<api_version>/tables/<tableNameOrOcid>?compartmentId=<compartmentId>
     */
    private static String buildGetTableUrl(String root,
                                           String tableNameOrOcid,
                                           String compartmentId) {
        StringBuilder sb = new StringBuilder(URL_PATH_DELIMITER);
        sb.append(root)
          .append(URL_PATH_DELIMITER)
          .append("tables")
          .append(URL_PATH_DELIMITER)
          .append(tableNameOrOcid);
        if (compartmentId != null) {
            sb.append("?")
              .append(COMPARTMENT_ID)
              .append("=")
              .append(compartmentId);
        }
        return sb.toString();
    }

    /**
     * Builds GET Configuration url:
     *  /<api_version>/configuration?compartmentId=<compartmentId>
     */
    private static String buildGetConfigurationUrl(String root,
                                                   String compartmentId) {
        StringBuilder sb = new StringBuilder(URL_PATH_DELIMITER);
        sb.append(root)
          .append(URL_PATH_DELIMITER)
          .append("configuration?compartmentId=")
          .append(compartmentId);
        return sb.toString();
    }

    /**
     * Returns the LifecycleState based on the TableInfo.State and
     * TableInfo.Activity.
     */
    private static LifecycleState getLifecycleState(TableInfo ti) {
        switch (ti.getStateEnum()) {
        case ACTIVE:
            if (ti.getActivityPhaseEnum() != ActivityPhase.ACTIVE) {
                return LifecycleState.INACTIVE;
            }
            return LifecycleState.ACTIVE;
        case CREATING:
            return LifecycleState.CREATING;
        case UPDATING:
            return LifecycleState.UPDATING;
        case DROPPING:
            return LifecycleState.DELETING;
        case DROPPED:
            return LifecycleState.DELETED;
        default:
            throw new IllegalArgumentException(
                "Unexpected TableState: " + ti.getStateEnum());
        }
    }

    private static LifecycleState getLifecycleState(ReplicaState state) {
        switch(state) {
        case CREATING:
            return LifecycleState.CREATING;
        case UPDATING:
            return LifecycleState.UPDATING;
        case DROPPING:
            return LifecycleState.DELETING;
        case ACTIVE:
            return LifecycleState.ACTIVE;
        default:
            throw new IllegalArgumentException(
                "Unexpected ReplicaState: " + state);
        }
    }

    /**
     * Maps the given consistency string to oracle.kv.Consistency object.
     */
    public static Consistency mapToKVConsistency(String consistency) {
        if (consistency != null) {
            if (consistency.equalsIgnoreCase("absolute")) {
                return Consistency.ABSOLUTE;
            } else if (consistency.equalsIgnoreCase("eventual")) {
                return Consistency.NONE_REQUIRED;
            }
            throw new IllegalArgumentException("Invalid consistency: " +
                                               consistency);
        }
        return null;
    }

    /**
     * Build the response payload for List-Tables operation
     */
    public static String buildTableCollection(ListTableInfoResponse res,
                                              AccessContext actx,
                                              TenantManager tm,
                                              SkLogger logger) {
        JsonBuilder jb = JsonBuilder.create();
        jb.startArray(ITEMS);
        for (TableInfo tableInfo : res.getTableInfos()) {
            buildTableSummary(jb, tableInfo, actx);
        }
        jb.endArray();

        jb.append(MAX_AUTO_RECLAIMABLE_TABLES,
                  res.getMaxAutoReclaimableTables());
        jb.append(AUTO_RECLAIMABLE_TABLES,
                  res.getAutoReclaimableTables());
        jb.append(MAX_ON_DEMAND_CAPACITY_TABLES,
                  res.getMaxAutoScalingTables());
        jb.append(ON_DEMAND_CAPACITY_TABLES,
                  res.getAutoScalingTables());

        if (res.getAvailableReplicas() != null) {
            jb.startArray(AVAILABLE_REPLICATION_REGIONS);

            /* Sort region names in alphabetical order */
            Set<String> regionNames = new TreeSet<String>();
            String localServiceName = tm.getLocalServiceName();
            for (String serviceName : res.getAvailableReplicas()) {
                if (!serviceName.equals(localServiceName)) {
                    try {
                        regionNames.add(tm.translateToRegionName(serviceName));
                    } catch (IllegalArgumentException ex) {
                        /*
                         * Ignore the unknown service name to local service
                         * directory.
                         *
                         * This may occur if permissible region was whitelisted
                         * before local service directory was updated. To not
                         * block console, just ignore the unknown region and put
                         * SEVERE log to remind ourselves.
                         */
                        logger.severe("Unknown service name '" + serviceName +
                                      "': " + ex.getMessage());;
                    }
                }
            }
            for (String name : regionNames) {
                jb.append(name);
            }
            jb.endArray();
        }

        return jb.toString();
    }

    /**
     * Builds the TableSummary JSON String.
     */
    private static void buildTableSummary(JsonBuilder jb,
                                          TableInfo ti,
                                          AccessContext actx) {
        LifecycleState state = getLifecycleState(ti);
        jb.startObject(null)
          .append(ID, ti.getTableOcid())
          .append(NAME, ti.getTableName())
          .append(COMPARTMENT_ID, ti.getCompartmentId())
          .append(TIME_CREATED, formatDateString(ti.getCreateTime()))
          .append(TIME_UPDATED, (ti.getUpdateTime() > 0 ?
                                 formatDateString(ti.getUpdateTime()) : null))
          .append(LIFE_CYCLE_STATE, state.name())
          .append(LIFE_CYCLE_DETAILS, state.getDetails())
          .append(SCHEMA_STATE, getSchemaState(ti.getSchemaState()))
          .append(IS_MULTI_REGION, ti.isMultiRegion());

        /* tableLimits */
        appendTableLimits(jb, ti.getTableLimits());

        /* freeformTags, definedTags, systemTags */
        if (ti.getTags() != null) {
            String[] tagJsons = actx.getExistingTags(ti.getTags());
            if (tagJsons != null) {
                appendTags(jb, tagJsons);
            }
        }

        /* IsAutoReclaimable, timeOfExpiration */
        appendAutoReclaimable(jb, ti);

        jb.endObject();
    }

    /**
     * Appends tags to Json output.
     */
    private static void appendTags(JsonBuilder jb, String[] tagJsons) {
        if (tagJsons[0] != null) {
            jb.appendJson(FREE_FORM_TAGS, tagJsons[0]);
        }
        if (tagJsons[1] != null) {
            jb.appendJson(DEFINED_TAGS, tagJsons[1]);
        }
        if (tagJsons[2] != null) {
            jb.appendJson(SYSTEM_TAGS, tagJsons[2]);
        }
    }

    /**
     * Appends isAutoReclaimable and timeOfExpiration
     */
    private static void appendAutoReclaimable(JsonBuilder jb, TableInfo ti) {
        jb.append(IS_AUTO_RECLAIMABLE, ti.isAutoReclaimable());
        if (getLifecycleState(ti) == LifecycleState.INACTIVE &&
            ti.getTimeOfExpiration() > 0) {
            jb.append(TIME_OF_EXPIRATION,
                      formatDateString(ti.getTimeOfExpiration()));
        }
    }

    /**
     * Appends table limits
     */
    private static void appendTableLimits(JsonBuilder jb, TableLimits limits) {
        /* tableLimits */
        if (limits != null) {
            jb.startObject(TABLE_LIMITS)
              .append(MAX_READ_UNITS, limits.getReadUnits())
              .append(MAX_WRITE_UNITS, limits.getWriteUnits())
              .append(MAX_STORAGE_IN_GBS, limits.getTableSize())
              .append(CAPACITY_MODE, getCapacityMode(limits.getMode()))
              .endObject();
        }
    }

    private static String getCapacityMode(String mode) {
        if (TableLimits.modeIsProvisioned(mode)) {
            return PROVISIONED;
        }
        if (TableLimits.modeIsAutoScaling(mode)) {
            return ON_DEMAND;
        }
        throw new IllegalArgumentException("Invalid capacity mode: " + mode);
    }

    /**
     * Build the response payload for Get-Index operation.
     */
    public static String buildIndex(String compartmentId,
                                    String tableName,
                                    String tableOcid,
                                    IndexInfo indexInfo,
                                    TableImpl table) {

        JsonBuilder jb = JsonBuilder.create();

        LifecycleState state = mapToLifecycleState(indexInfo.getState());
        jb.append(NAME, indexInfo.getIndexName())
          .append(COMPARTMENT_ID, compartmentId)
          .append(TABLE_NAME, tableName)
          .append(TABLE_ID, tableOcid)
          .append(LIFE_CYCLE_STATE, state.name())
          .append(LIFE_CYCLE_DETAILS, state.getDetails());
        buildIndexFields(jb, indexInfo, table);
        return jb.toString();
    }

    private static void buildIndexFields(JsonBuilder jb,
                                         IndexInfo info,
                                         TableImpl table) {
        jb.startArray(KEYS);
        if (isIndexHasJsonField(info) && table != null) {
            addIndexWithJsonField(jb, info, table);
        } else {
            for (IndexField field : info.getIndexFields()) {
                jb.startObject(null);
                jb.append(COLUMN_NAME, field.getPath());
                jb.endObject();
            }
        }
        jb.endArray();
    }

    /**
     * Adds IndexKey information for the index that contains JSON field.
     */
    private static void addIndexWithJsonField(JsonBuilder jb,
                                              IndexInfo info,
                                              TableImpl table) {
        int num = info.getIndexFields().length;
        List<String> paths = new ArrayList<>(num);
        List<FieldDef.Type> types = new ArrayList<>(num);

        FieldDef.Type ftype;
        for (IndexField idxFld : info.getIndexFields()) {
            paths.add(idxFld.getPath());
            if (idxFld.getType() != null) {
                ftype = FieldDef.Type.valueOf(idxFld.getType().toUpperCase());
            } else {
                ftype = null;
            }
            types.add(ftype);
        }

        /*
         * Get oracle.kv.impl.api.table.IndexImpl.IndexField from IndexImpl
         */
        IndexImpl index;
        String indexName = info.getIndexName();
        if (table.getIndexes().containsKey(indexName)) {
            index = (IndexImpl)table.getIndex(indexName);
        } else {
            index = new IndexImpl(info.getIndexName(), table,
                                  paths, types, null);
        }

        String column;
        String jsonPath;
        String type;
        oracle.kv.impl.api.table.IndexImpl.IndexField idxFld;
        int i = 0;
        for (IndexField field : info.getIndexFields()) {
            jb.startObject(null);
            type = field.getType();
            if (type != null) {
                idxFld = index.getIndexPath(i);
                column = idxFld.getJsonFieldPath().getPathName();
                /*
                 * Extract the <internal-json-path> from full index field
                 * path <json-field>.<internal-json-path>
                 */
                jsonPath = field.getPath().substring(column.length() + 1);
                jb.append(COLUMN_NAME, column);
                jb.append(JSON_PATH, jsonPath);
                jb.append(JSON_FIELD_TYPE, type);
            } else {
                jb.append(COLUMN_NAME, field.getPath());
            }
            jb.endObject();
            i++;
        }
    }

    public static boolean isIndexHasJsonField(IndexInfo index) {
        for (IndexField field : index.getIndexFields()) {
            if (field.getType() != null) {
                return true;
            }
        }
        return false;
    }

    public static String buildIndexCollection(IndexInfo[] indexInfos,
                                              TableImpl table) {
        JsonBuilder jb = JsonBuilder.create();
        jb.startArray(ITEMS);
        for (IndexInfo info : indexInfos) {
            LifecycleState state = mapToLifecycleState(info.getState());
            jb.startObject(null);
            jb.append(NAME, info.getIndexName())
              .append(LIFE_CYCLE_STATE, state.name())
              .append(LIFE_CYCLE_DETAILS, state.getDetails());
            buildIndexFields(jb, info, table);
            jb.endObject();
        }
        jb.endArray();
        return jb.toString();
    }

    private static LifecycleState mapToLifecycleState(IndexState state) {
        switch (state) {
        case ACTIVE:
            return LifecycleState.ACTIVE;
        case CREATING:
            return LifecycleState.CREATING;
        case DROPPING:
            return LifecycleState.DELETING;
        case DROPPED:
            return LifecycleState.DELETED;
        }
        return null;
    }

    /**
     * Build the response payload for Get-TableUsage operation.
     */
    public static String buildTableUsageCollection(TableUsage[] usages) {
        final JsonBuilder jb = JsonBuilder.create();
        jb.startArray(ITEMS);
        for (TableUsage usage : usages) {
            buildTableUsageSummary(jb, usage);
        }
        jb.endArray();
        return jb.toString();
    }

    private static void buildTableUsageSummary(JsonBuilder jb,
                                               TableUsage usage) {
        jb.startObject(null)
            .append(TIME_STARTED, formatDateString(usage.getStartTimeMillis()))
            .append(SECONDS_IN_PERIOD, usage.getSecondsInPeriod())
            .append(READ_UNITS, usage.getReadUnits())
            .append(WRITE_UNITS, usage.getWriteUnits())
            .append(STORAGE_IN_GBS, usage.getStorageGB())
            .append(READ_THROTTLE_COUNT, usage.getReadThrottleCount())
            .append(WRITE_THROTTLE_COUNT, usage.getWriteThrottleCount())
            .append(STORAGE_THROTTLE_COUNT, usage.getStorageThrottleCount())
            .append(MAX_SHARD_SIZE_USAGE_IN_PERCENT,
                    usage.getMaxPartitionUsage())
        .endObject();
    }

    /**
     * Build the response play for Row-Put operation
     */
    public static String buildUpdateRowResult(Result result,
                                              ReturnRow prevRow) {

        final JsonBuilder jb = JsonBuilder.create();

        buildRequestUsage(jb, result);

        Version newVersion = result.getNewVersion();
        if (newVersion!= null) {
            jb.append(VERSION, encodeVersion(newVersion));
        }

        if (prevRow != null && !prevRow.isEmpty()) {
            buildFieldValue(jb, EXISTING_VALUE, prevRow);
            jb.append(EXISTING_VERSION, encodeVersion(prevRow.getVersion()));
        }

        if (result.getGeneratedValue() != null) {
            jb.append(GENERATED_VALUE, result.getGeneratedValue().toString());
        }
        return jb.toString();
    }

    /**
     * Build the response payload for Row-Get operation
     */
    public static String buildRow(Result result, oracle.kv.table.Row row) {
        JsonBuilder jb = JsonBuilder.create();
        buildRequestUsage(jb, result);
        if (row != null) {
            buildFieldValue(jb, VALUE, row);
            long expirationTime = row.getExpirationTime();
            if (expirationTime > 0) {
                jb.append(TIME_OF_EXPIRATION, formatDateString(expirationTime));
            }
        }
        return jb.toString();
    }

    /**
     * Build the response payload for Delete-Row operation.
     */
    public static String buildDeleteRowResult(Result result,
                                              ReturnRow prevRow) {
        final JsonBuilder jb = JsonBuilder.create();
        buildRequestUsage(jb, result);
        jb.append(IS_SUCCESS, result.getSuccess());
        if (prevRow != null && !prevRow.isEmpty()) {
            buildFieldValue(jb, EXISTING_VALUE, prevRow);
            jb.append(EXISTING_VERSION, encodeVersion(prevRow.getVersion()));
        }
        return jb.toString();
    }

    /**
     * Build the response payload for Query-Prepare operation.
     */
    public static String buildPreparedStatement(String statement,
                                                String queryPlan,
                                                int readUnits) {
        final JsonBuilder jb = JsonBuilder.create();
        buildRequestUsage(jb, readUnits, 0);
        jb.append(STATEMENT, statement);
        if (queryPlan != null) {
            jb.appendJson(QUERY_PLAN, queryPlan);
        }
        return jb.toString();
    }

    /**
     * Build the response payload for Query-Summarize operation.
     */
    public static String buildStatementSummary(TableUtils.PrepareCB cbinfo,
                                               String syntaxError) {

        final JsonBuilder jb = JsonBuilder.create();
        if (cbinfo != null) {
            QueryOperation op = cbinfo.getOperation();
            jb.append(OPERATION, op.name())
              .append(TABLE_NAME, cbinfo.getTableName())
              .append(INDEX_NAME, cbinfo.getIndexName());
            if (op == QueryOperation.CREATE_TABLE ||
                op == QueryOperation.CREATE_INDEX) {
              jb.append(IS_IF_NOT_EXISTS, cbinfo.getIfNotExists());
            } else if (op == QueryOperation.DROP_TABLE ||
                       op == QueryOperation.DROP_INDEX) {
              jb.append(IS_IF_EXISTS, cbinfo.getIfExists());
            }
        }
        if (syntaxError != null) {
            jb.append(SYNTAX_ERROR, syntaxError.replace('\n', ' '));
        }
        return jb.toString();
    }

    /**
     * Build the response payload for Query-Query operation.
     */
    public static int buildQueryResult(JsonBuilder jb,
                                       QueryStatementResultImpl qsr,
                                       int prepCost) {
        int count = 0;
        jb.startArray(ITEMS);
        for (RecordValue rec : qsr) {
            buildFieldValue(jb, null, rec);
            count++;
        }
        jb.endArray();

        buildRequestUsage(jb, qsr.getReadKB() + prepCost, qsr.getWriteKB());
        return count;
    }

    /**
     * Appends FieldValue to Json output.
     *
     * Public for unit test
     */
    public static void buildFieldValue(JsonBuilder jb,
                                       String field,
                                       FieldValue value) {
        if (value == null || value.isNull() || value.isJsonNull()) {
            jb.append(field, null);
            return;
        }

        switch (value.getType()) {
        case BOOLEAN:
            jb.append(field, value.asBoolean().get());
            break;
        case BINARY:
            jb.append(field, JsonUtils.encodeBase64(value.asBinary().get()));
            break;
        case FIXED_BINARY:
            jb.append(field,
                      JsonUtils.encodeBase64(value.asFixedBinary().get()));
            break;
        case INTEGER:
            jb.append(field, value.asInteger().get());
            break;
        case LONG:
            jb.append(field, value.asLong().get());
            break;
        case FLOAT:
            jb.append(field, value.asFloat().get());
            break;
        case DOUBLE:
            double dval = value.asDouble().get();
            /*
             * The 3 double special values NaN, Infinity and -Infinity results
             * in JSON parsing failure in OCI sdk, it looks like the OCI sdk
             * doesn't handle these special values correctly.
             *
             * The workaround here is returning String format instead of doubles
             * for these 3 special values in JSON response, it is not pretty
             * solution as these values will be parsed as String value.
             */
            if (Double.isFinite(dval)) {
                jb.append(field, dval);
            } else {
                jb.append(field, Double.toString(dval));
            }
            break;
        case NUMBER:
            /*
             * Print all digits of BigDecimal without scientific notation, this
             * is to prevent the value being parsed as double in oci driver.
             * e.g. 1.23456789E10, print it as 12345678900.
             */
            jb.appendJson(field, value.asNumber().get().toPlainString());
            break;
        case TIMESTAMP:
            String timestampStr = value.asTimestamp().toString();
            /*
             * TODO: remove code below after upgrade to kv 21.1 which append
             * final "Z" to timestamp string.
             */
            if (!timestampStr.endsWith("Z")) {
                timestampStr = timestampStr + "Z";
            }
            jb.append(field, timestampStr);
            break;
        case ENUM:
        case STRING:
            jb.append(field, value.toString());
            break;
        case ARRAY: {
            ArrayValue av = value.asArray();
            jb.startArray(field);
            for (int i = 0; i < av.size(); i++) {
                buildFieldValue(jb, null, av.get(i));
            }
            jb.endArray();
            break;
        }
        case MAP: {
            MapValue mv = value.asMap();
            jb.startObject(field);
            for (Entry<String, FieldValue> e: mv.getFields().entrySet()) {
                buildFieldValue(jb, e.getKey(), e.getValue());
            }
            jb.endObject();
            break;
        }
        case RECORD:{
            RecordValue rv = value.asRecord();
            jb.startObject(field);
            for (String name : rv.getFieldNames()) {
                buildFieldValue(jb, name, rv.get(name));
            }
            jb.endObject();
            break;
        }
        default:
            throw new IllegalStateException("Unexpected type: " +
                                             value.getType());
        }
    }

    public static String buildConfiguration(KmsKeyInfo key) {
        JsonBuilder jb = JsonBuilder.create();
        jb.append(ENVIRONMENT, (key.isHostedEnv() ? HOSTED_ENVIRONMENT :
                                                    MULTI_TENANCY_ENVIRONMENT));
        if (key.getState() != null) {
            jb.startObject(KMS_KEY);
            jb.append(KMS_KEY_STATE, mapKmsKeyState(key.getState()));
            if (key.getKeyId() != null) {
                jb.append(ID, key.getKeyId())
                  .append(KMS_VAULT_ID, key.getVaultId())
                  .append(TIME_CREATED, formatDateString(key.getCreateTime()))
                  .append(TIME_UPDATED, formatDateString(key.getUpdateTime()));
            }
            jb.endObject();
        }
        return jb.toString();
    }

    private static String mapKmsKeyState(KmsKeyInfo.KeyState state) {
        switch (state) {
        case UPDATING:
            return "UPDATING";
        case ACTIVE:
            return "ACTIVE";
        case DELETED:
            return "DELETED";
        case FAILED:
            return "FAILED";
        case REVERTING:
            return "REVERTING";
        case DISABLED:
            return "DISABLED";
        default:
            throw new IllegalStateException("Unexpected KeyState: " + state);
        }
    }

    /**
     * Build the JSON string for RequestUsage
     */
    private static void buildRequestUsage(JsonBuilder jb, Result result) {
        buildRequestUsage(jb, result.getReadKB(), result.getWriteKB());
    }

    private static void buildRequestUsage(JsonBuilder jb,
                                          int readUnits,
                                          int writeUnits) {
        jb.startObject(USAGE);
        jb.append(READ_UNITS_CONSUMED, readUnits);
        jb.append(WRITE_UNITS_CONSUMED, writeUnits);
        jb.endObject();
    }

    /**
     * Encodes the byte array of version to base64 string.
     */
    private static String encodeVersion(Version version) {
        return ProxySerialization.encodeBase64(version.toByteArray());
    }

    /**
     * Encode the specified byte array into a url-safed Base64 encoded string.
     * This string can be decoded using {@link #urlDecodeBase64}.
     *
     * @param buffer the input buffer
     *
     * @return the encoded string
     */
    public static String urlEncodeBase64(byte[] buffer) {
        return Base64.getUrlEncoder().encodeToString(buffer);
    }

    /**
     * Decode the specified Base64 string into a byte array. The string must
     * have been encoded using {@link #urlEncodeBase64} or the same algorithm.
     *
     * @param binString the encoded input string
     *
     * @return the decoded array
     */
    public static byte[] urlDecodeBase64(String binString) {
        return Base64.getUrlDecoder().decode(binString);
    }

    /**
     * Converts the Tags object to JSON string.
     *
     * The tags can be freeformTags, definedTags or systemTags:
     *   o freeformTags: Map<String, String>
     *   o systemTags, definedTags: Map<String, Map<String, Object>>
     *     Object can be String, Integer or Boolean.
     */
    public static String tagsToJson(Map<String, ? extends Object> tags) {
        final JsonBuilder jb = JsonBuilder.create();
        for (Map.Entry<String, ? extends Object> e : tags.entrySet()) {
            if (e.getValue() instanceof Map) {
                jb.startObject(e.getKey());
                @SuppressWarnings("unchecked")
                Map<String, Object> props = (Map<String, Object>)e.getValue();
                for (Map.Entry<String, Object> prop : props.entrySet()) {
                    jb.append(prop.getKey(), prop.getValue());
                }
                jb.endObject();
            } else {
                jb.append(e.getKey(), e.getValue());
            }
        }
        return jb.toString();
    }

    public static void checkNotNullEmpty(String param, String value) {
        checkNotNull(param, value);
        checkNotEmpty(param, value);
    }

    public static void checkNotNull(String param, Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException("Invalid " + param + ": " +
                param + " should not be null");
        }
    }

    public static void checkNotEmpty(String param, String value) {
        if (isEmpty(value)) {
            throw new IllegalArgumentException("Invalid " + param + ": " +
                param + " should not be empty or contain white space only");
        }
    }

    public static boolean isEmpty(String value) {
        return value != null && value.trim().isEmpty();
    }

    public static void checkNonNegativeInt(String name, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("Invalid " + name +
                ", it should not be negative value: " + value);
        }
    }

    /**
     * The parser used to parse JSON payload of Http Request.
     *
     * It is used to parse the given InputStream of JSON string, traverse each
     * field value.
     *   - JsonPayload.isField(<name>) method is to check if the current field
     *     name is the given name, it does case-insensitive comparison.
     *   - If field is int, string or boolean, call JsonPayload.readInt(),
     *     JsonPayload.readString() and JsonPayload.readBoolean() to read
     *     its value. If the actual current value type is not expected type,
     *     throw IllegalArgumentException.
     *   - If the field value is Json Object, use JsonPayload.readObject() to
     *     read JsonObject, then traverse its nested field using
     *     JsonObject.readXXX()
     *   - If the field value is Json Array, use JsonPayload.readArray() to
     *     read JsonArray, then traverse its elements using
     *     JsonArray.readValue(JsonToken)
     *
     * <pre>
     * {@code
     *    JsonPayload pl = new JsonPayload(in);
     *    while (pl.hasNext()) {
     *        if (pl.isField("tableName") {
     *            tableName = pl.readString();
     *        } else if (pl.isField("limit")) {
     *            limit = pl.readInt();
     *        } else if (pl.isField("value")) {
     *            obj = pl.readObject();
     *            while (obj.hasNext()) {
     *                <traverse nested object>
     *            }
     *        } else if (pl.isField("arrayValue")) {
     *            array = pl.readArray();
     *            while (array.hasNext()) {
     *                <traverse nested array>
     *            }
     *        }
     *        ...
     *    }
     * }
     * </pre>
     */
    public static class JsonPayload extends JsonObject
        implements AutoCloseable {

        private final InputStreamHelper in;

        public JsonPayload(ByteInputStream in) throws IOException {
            this(new ByteInputStreamHelper(in));
        }

        public JsonPayload(String jsonString) throws IOException {
            this(new ByteArrayHelper(jsonString.getBytes()));
        }

        private JsonPayload(InputStreamHelper in) throws IOException {
            super(createJsonParser(in.getInputStream()), in);
            this.in = in;
            jp.nextToken();
        }

        @Override
        public void close() {
            try {
                if (jp != null) {
                    jp.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ignored) {
            }
        }
    }

    public static class JsonObject extends ComplexValue {
        private String currentField;

        public JsonObject(JsonParser jp, InputStreamHelper in) {
            super(jp, JsonToken.END_OBJECT, in);
            currentField = null;
        }

        @Override
        protected void nextValue() throws IOException {
            currentField = jp.getCurrentName();
            jp.nextToken();
        }

        public String getCurrentField() {
            return currentField;
        }

        public boolean isField(String fieldName) {
            return currentField.equalsIgnoreCase(fieldName);
        }
    }

    public static class JsonArray extends ComplexValue {
        JsonArray(JsonParser jp, InputStreamHelper in) {
            super(jp, JsonToken.END_ARRAY, in);
        }
    }

    private static class ComplexValue extends JsonValue {
        private final JsonToken end;
        private boolean hasNext;

        public ComplexValue(JsonParser jp, JsonToken end, InputStreamHelper in) {
            super(jp, in);
            hasNext = false;
            this.end = end;
        }

        protected void nextValue() throws IOException {
        }

        public boolean hasNext() throws IOException {
            if (hasNext) {
                return true;
            }

            JsonToken next = jp.nextToken();
            if (next != null && next != end) {
                hasNext = true;
                nextValue();
            }
            return hasNext;
        }

        /* Skips the current value */
        public void skipValue() throws IOException {
            jp.skipChildren();
            hasNext = false;
        }

        @Override
        protected Object readValue(JsonToken tokenType) {
            hasNext = false;
            return super.readValue(tokenType);
        }

        @Override
        public String readValueAsJson() {
            hasNext = false;
            return super.readValueAsJson();
        }
    }

    private static class JsonValue {
        protected final JsonParser jp;
        private final InputStreamHelper in;

        JsonValue(JsonParser jp, InputStreamHelper in) {
            this.jp = jp;
            this.in = in;
        }

        public JsonParser getJsonParser() {
            return jp;
        }

        public String readString() {
            return (String)readValue(JsonToken.VALUE_STRING);
        }

        public int readInt() {
            Integer val = (Integer)readValue(JsonToken.VALUE_NUMBER_INT);
            return (val != null) ? val.intValue() : 0;
        }

        public boolean readBool() {
            Boolean val = (Boolean)readValue(JsonToken.VALUE_TRUE);
            return (val != null) ? val.booleanValue() : false;
        }

        public JsonObject readObject() {
            return (JsonObject)readValue(JsonToken.START_OBJECT);
        }

        public JsonArray readArray() throws IOException {
            return (JsonArray)readValue(JsonToken.START_ARRAY);
        }

        public Object readValue() {
            return readValue(null);
        }

        protected Object readValue(JsonToken expToken) {
            Object value = null;
            JsonToken token = null;

            if (jp.currentToken() == JsonToken.VALUE_NULL) {
                return null;
            }
            try {
                token = (expToken != null) ? expToken : jp.currentToken();
                switch (token) {
                case VALUE_STRING:
                    value = getStringValue(jp);
                    break;
                case VALUE_NUMBER_INT:
                    value = getIntValue(jp);
                    break;
                case VALUE_TRUE:
                case VALUE_FALSE:
                    value = getBoolValue(jp);
                    break;
                case START_OBJECT:
                    if (jp.currentToken() != JsonToken.START_OBJECT) {
                        throw new IllegalArgumentException(
                            "Invalid token type for Json Object: " +
                            jp.currentToken());
                    }
                    return new JsonObject(jp, in);
                case START_ARRAY:
                    if (jp.currentToken() != JsonToken.START_ARRAY) {
                        throw new IllegalArgumentException(
                            "Invalid token type for Json Array: " +
                            jp.currentToken());
                    }
                    return new JsonArray(jp, in);
                default:
                    throw new IllegalArgumentException(
                        "Unexpected token while parsing JSON: " + token);
                }
            } catch (IOException ioe) {
                throw new IllegalArgumentException(
                    "Failed to parse JSON input: " + ioe.getMessage());
            }
            return value;
        }

        public String readValueAsJson() {
            JsonToken token = jp.currentToken();
            if (token == JsonToken.VALUE_NULL) {
                /*
                 * Prints the Json NULL value as null without quotes, it can be
                 * distinguished from string of "null" base on the quotes.
                 */
                return "null";
            }

            try {
                /* Json object or array */
                if (token.isStructStart()) {
                    int from = (int)(jp.getTokenLocation().getByteOffset());
                    jp.skipChildren();
                    int to = (int)jp.getCurrentLocation().getByteOffset();
                    return new String(in.readBytes(from, to - from));
                }

                /* Primitive types */
                String text = jp.getText();
                if (token == JsonToken.VALUE_STRING) {
                    /* Add quotes to string value */
                    return addQuotes(text);
                }
                return text;
            } catch (IOException ioe) {
                throw new IllegalArgumentException(
                    "Failed to parse JSON input: " + ioe.getMessage());
            }
        }

        private String getStringValue(JsonParser jp) throws IOException {
            JsonToken token = jp.getCurrentToken();
            if (token.isStructStart() || token.isStructEnd()) {
                throw new IllegalArgumentException(
                    "Not a valid string value: " + token);
            }
            return jp.getText();
        }

        private int getIntValue(JsonParser jp) throws IOException {
            JsonToken token = jp.getCurrentToken();
            if (token.isNumeric()) {
                return jp.getIntValue();
            }
            if (token == JsonToken.VALUE_STRING) {
                String str = jp.getText();
                try {
                    return Integer.valueOf(str);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException(
                        "Not a valid int value: " + str);
                }
            }
            throw new IllegalArgumentException(
                "Invalid type for int value: " + token);
        }

        private boolean getBoolValue(JsonParser jp) throws IOException {

            JsonToken token = jp.getCurrentToken();
            if (token.isBoolean()) {
                return jp.getBooleanValue();
            }
            if (token == JsonToken.VALUE_STRING) {
                return Boolean.valueOf(jp.getText());
            }
            throw new IllegalArgumentException(
                "Invalid type for boolean value: " + token);
        }

        private static String addQuotes(String str) {
            return "\"" + str + "\"";
        }
    }

    public static Map<String, String> parseFreeFormTags(String jsonTags)
        throws IOException {
        if (jsonTags == null) {
            return null;
        }
        try (JsonPayload jp = new JsonPayload(jsonTags)) {
            return parseFreeformTags(jp.readObject());
        }
    }

    public static Map<String, Map<String, Object>>
        parseDefinedTags(String jsonTags) throws IOException {

        if (jsonTags == null) {
            return null;
        }
        try (JsonPayload jp = new JsonPayload(jsonTags)) {
            return parseDefinedTags(jp.readObject());
        }
    }

    /**
     * The freeformTags is represented as Map<String, String>, the value can
     * not be null.
     */
    public static Map<String, String> parseFreeformTags(JsonObject jo)
        throws IOException {

        final Map<String, String> tags = new HashMap<>();
        while (jo.hasNext()) {
            String key = jo.getCurrentField();
            Object value = jo.readValue();
            if (value == null) {
                throw new IllegalArgumentException("Invalid freeformTags, " +
                    "the value of key '" + key +"' should not be null");
            }
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("Invalid freeformTags, " +
                    "the value of key '" + key + "' should be STRING but get " +
                    value.getClass().getName());
            }
            tags.put(key, (String)value);
        }
        return tags;
    }

    /**
     * The definedTags is represented as Map<String, Map<String, Object>>,
     * the Object can be String, Integer or Boolean type.
     */
    public static Map<String, Map<String, Object>>
        parseDefinedTags(JsonObject jo)
        throws IOException {

        final Map<String, Map<String, Object>> tags = new HashMap<>();
        while (jo.hasNext()) {
            String namespace = jo.getCurrentField();
            JsonObject jotag = jo.readObject();
            if (jotag == null) {
                throw new IllegalArgumentException("Invalid definedTags, " +
                    "the tags of namespace '" + namespace +
                    "' should not be null");
            }
            Map<String, Object> kvs = new HashMap<>();
            while (jotag.hasNext()) {
                String key = jotag.getCurrentField();
                Object value = jotag.readValue();
                if (value == null) {
                    throw new IllegalArgumentException("Invalid definedTags, " +
                        "the value of key '" + namespace + "." + key +
                        "' should not be null");
                }
                if (!(value instanceof Integer) &&
                    !(value instanceof String) &&
                    !(value instanceof Boolean)) {
                    throw new IllegalArgumentException("Invalid definedTags, " +
                        "the value of key '" + namespace + "." + key +
                        " should be Integer, String or Boolean but get " +
                        value.getClass().getName());
                }
                kvs.put(key, value);
            }
            tags.put(namespace, kvs);
        }
        return tags;
    }

    private static interface InputStreamHelper {
        /* Returns the InputStream */
        InputStream getInputStream();

        /* Reads len bytes from the specified offset from the input stream */
        byte[] readBytes(int offset, int len) throws IOException;

        default void close() {
            /* do nothing */
        }
    }

    /*
     * InputStreamHelper uses a ByteInputStream
     */
    private static class ByteInputStreamHelper implements InputStreamHelper {

        private final ByteInputStream in;

        ByteInputStreamHelper(ByteInputStream in) {
            this.in = in;
        }

        @Override
        public InputStream getInputStream() {
            return in;
        }

        @Override
        public byte[] readBytes(int offset, int len) throws IOException {
            byte[] bytes = new byte[len];
            int origOffset = in.getOffset();
            in.setOffset(offset);
            in.read(bytes, 0, bytes.length);
            in.setOffset(origOffset);
            return bytes;
        }

        @Override
        public void close() {
            if (in != null) {
                in.close();
            }
        }
    }

    /*
     * InputStreamHelper uses a byte array
     */
    private static class ByteArrayHelper implements InputStreamHelper {
        private final byte[] buf;
        private final ByteArrayInputStream in;

        ByteArrayHelper(byte[] buf) {
            this.buf = buf;
            in = new ByteArrayInputStream(buf);
        }

        @Override
        public InputStream getInputStream() {
            return in;
        }

        @Override
        public byte[] readBytes(int offset, int len) {
            if (offset + len > buf.length) {
                throw new IllegalArgumentException(
                    String.format("Range [%d, %d) out of bounds for length %d",
                                  offset, len, buf.length));
            }
            return Arrays.copyOfRange(buf, offset, offset + len);
        }
    }
}
