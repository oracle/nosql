/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.util.tmi.ReplicaInfo.ReplicaState;

/**
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and the tenant manager.
 *
 * To serialize a Java object into a Json string:
 *   Foo foo;
 *   String jsonPayload = JsonUtils.toJson(foo);
 *
 * To deserialize a Json string into this object:
 *   Foo foo = JsonUtils.fromJson(<jsonstring>, Foo.class);
 *
 * This class is also used within the proxy to ferry table related info.
 *
 * The field "tableId" is used to convey the table's unique Id but only within
 * a process; it does not appear in the serialized form, mainly to avoid an
 * unnecessary protocol change.
 */
public class TableInfo {

    public enum TableState {
        CREATING,
        DROPPING,
        DROPPED,
        ACTIVE,
        UPDATING
    }

    /**
     * The activity phase of auto reclaimable table(also known as always
     * free tier table).
     * ActivityPhase state can be changed from ACTIVE -> IDLE_P1 -> IDLE_P2,
     * and from IDLE_P1/IDLE_P2 back to ACTIVE.
     */
    public enum ActivityPhase {
        /*
         * Auto reclaimable table is in ACTIVE phase if there was some data
         * operations on this table recently, by default is within 30 days.
         */
        ACTIVE,
        /*
         * Auto reclaimable table will be changed to IDLE_P1 phase if there was
         * no data operation on this table more than certain days, by default
         * is more than 30 days. We will send the first customer notification
         * to user to notify this table becomes inactive.
         */
        IDLE_P1,
        /*
         * Auto reclaimable table will be changed from IDLE_P1 phase to IDLE_P2
         * phase if still no data operation on this table, by default is more
         * than 75 days. We will send the second customer notification to user
         * to notify this table will be reclaimed soon if still no data
         * operation on this table.
         */
        IDLE_P2
    }

    public enum SchemaState {
        /* The schema can be changed. The table is not eligible for replication.*/
        MUTABLE,
        /* The schema is immutable. The table is eligible for replication. */
        FROZEN
    }

    /*
     * Version 2 changes:
     *   Added support for auto scaling tables. The actual change is in the
     *   TableLimits class, which is a field in this class.
     *   Limits Mode was added.
     */
    @SuppressWarnings("unused")
    private static final int AUTO_SCALING_TABLE_VERSION = 2;

    /*
     * Version 3 changes:
     *   Support for MR tables, added new fields:
     *     o SchemaState schemaState
     *     o ReplicaState replicaState
     *     o boolean initialized
     *     o Map<String, ReplicaInfo> replicas
     *     o int mrTableVersion
     *     o int initializePercent
     */
    private static final int MR_TABLE_VERSION = 3;

    private static final int CURRENT_VERSION = MR_TABLE_VERSION;

    private int version;

    private String tableName;
    private String tenantId;
    /*
     * "state" is a TableState, but is in String form because Jackson 1 can't
     * deserialize an enum.
     */
    private String state;

    private TableLimits tableLimits;
    private long createTime;
    private long updateTime;
    private transient long tableId;
    private String compartmentId;
    private String ocid;
    private byte[] tags;
    private String operationId;
    private int schemaVersion;
    private String schema;
    private String ddl;
    private String kvTableName;

    /*
     * True if this is an auto reclaimable table(also known as always free tier
     * table).
     * Auto reclaimable table will be reclaimed automatically if table no data
     * operation for a long while(by default is 90 days).
     */
    private boolean autoReclaimable;
    /*
     * Field for auto reclaimable table only.
     * @see ActivityPhase.
     * We update this field value in two ways.
     * 1) There is a background service will update it regularly base on the
     * data operation records in ActualThroughput table.
     * 2) Also Proxy will tell SC to update it when receiving a data operation
     * of inactive Table. So user can see its table become active immediately
     * after a data operation.
     */
    private String activityPhase;
    /*
     * Field for auto reclaimable table only.
     * Indicates when this table will be automatically reclaimed,
     * if there was no more data operation on this table.
     */
    private long timeOfExpiration;

    /* schema state */
    private SchemaState schemaState;

    /*
     * MR table information
     */

    /* Local replica state. */
    private ReplicaState replicaState;

    /* Indicates if local replica table is initialized */
    private boolean initialized;

    /* Remote replicas info, map key is serviceName of remote replica. */
    private Map<String, ReplicaInfo> replicas;

    /*
     * The version of schema changes to MR table, it is increased by one after
     * apply a MR ddl change, reset to 0 once back to singleton table.
     */
    private int mrTableVersion;

    /* The percentage of initialization process, transient and not persisted. */
    private int initializePercent;

    /* Needed for serialization */
    public TableInfo() {
    }

    /**
     * For On-prem TenantManager
     */
    public TableInfo(String tableName,
                     String tenantId,
                     TableState stateEnum,
                     TableLimits tableLimits,
                     long createTime) {
        this(tableName, tenantId, null /* compartmentId */, stateEnum,
             tableLimits, createTime, 0L /* updateTime */,
             null /* operationId */, false /* autoReclaimable */,
             ActivityPhase.ACTIVE.name(), 0L /* timeOfExpiration */);
    }

    /**
     * For Cloud TenantManager
     */
    public TableInfo(String tableName,
                     String tenantId,
                     String compartmentId,
                     TableState stateEnum,
                     TableLimits tableLimits,
                     long createTime,
                     long updateTime,
                     String operationId,
                     boolean autoReclaimable,
                     String activityPhase,
                     long timeOfExpiration) {

        this(CURRENT_VERSION, tableName, tenantId, compartmentId, stateEnum,
             tableLimits, createTime, updateTime, operationId, autoReclaimable,
             activityPhase, timeOfExpiration);
    }

    public TableInfo(int version,
                     String tableName,
                     String tenantId,
                     String compartmentId,
                     TableState stateEnum,
                     TableLimits tableLimits,
                     long createTime,
                     long updateTime,
                     String operationId,
                     boolean autoReclaimable,
                     String activityPhase,
                     long timeOfExpiration) {
        this.version = version;
        this.tableName = tableName;
        this.tenantId = tenantId;
        this.compartmentId = compartmentId;
        this.state = stateEnum.name();
        this.tableLimits = tableLimits;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.operationId = operationId;
        this.autoReclaimable = autoReclaimable;
        this.activityPhase = activityPhase;
        this.timeOfExpiration = timeOfExpiration;
        this.schemaVersion = 0;
        this.schemaState = SchemaState.MUTABLE;
    }

    public int getVersion() {
        return version;
    }

    /**
     * @return the state
     */
    public String getState() {
        return state;
    }

    /**
     * @return the tableLimits
     */
    public TableLimits getTableLimits() {
        return tableLimits;
    }

    public void setTableLimits(TableLimits tableLimits) {
        this.tableLimits = tableLimits;
    }

    /**
     * @return the table OCID
     */
    public String getTableOcid() {
        return ocid;
    }

    /**
     * Sets table OCID
     * @param ocid set table OCID
     */
    public void setTableOcid(String ocid) {
        this.ocid = ocid;
    }

    /**
     * @return the schema
     */
    public String getSchema() {
        return schema;
    }

    /*
     * Sets the schema
     * @param schema the table schema, in JSON format
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * @return the schema version
     */
    public int getSchemaVersion() {
        return schemaVersion;
    }

    /**
     * Sets the schema version
     * @param schemaVersion the table schema version
     */
    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    /**
     * @return the create table ddl statement
     */
    public String getDdl() {
        return ddl;
    }

    /**
     * Sets the create table ddl statement
     * @param ddl the create table ddl statement
     */
    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return the tenantId
     */
    public String getTenantId() {
        return tenantId;
    }

    /**
     * @return the compartmentId
     */
    public String getCompartmentId() {
        return compartmentId;
    }

    /**
     * Sets the compartmentId
     */
    public void setCompartmentId(String compartmentId) {
        this.compartmentId = compartmentId;
    }

    /**
     * Sets the kvTableName
     */
    public void setKVTableName(String kvTableName) {
        this.kvTableName = kvTableName;
    }

    /**
     * Returns the kvTableName if set, otherwise return its table ocid.
     */
    public String getKVTableName() {
        if (kvTableName != null) {
            return kvTableName;
        }
        return getTableOcid();
    }

    /**
     * @return the createTime
     */
    public long getCreateTime() {
        return createTime;
    }

    /**
     * Sets the updateTime
     */
    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    /**
     * @return the last update time.
     * If updateTime is 0, then return createTime. Otherwise, return updateTime.
     */
    public long getUpdateTime() {
        return updateTime;
    }

    /**
     * @return state in enum form.
     */
    public TableState getStateEnum() {
        return TableState.valueOf(state);
    }

    /**
     * Update the state
     */
    public void setStateEnum(TableState stateEnum) {
        state = stateEnum.name();
    }

    /**
     * @return operationId.
     */
    public String getOperationId() {
        return operationId;
    }

    /**
     * Sets the operationId
     */
    public void setOperationId(String operationId) {
        this.operationId = operationId;
    }

    /**
     * @return autoReclaimable
     */
    public boolean isAutoReclaimable() {
        return autoReclaimable;
    }

    /**
     * Sets the autoReclaimable.
     */
    public void setAutoReclaimable(boolean autoReclaimable) {
        this.autoReclaimable  = autoReclaimable;
    }

    /**
     * @return timeOfExpiration
     */
    public long getTimeOfExpiration() {
        return timeOfExpiration;
    }

    /**
     * Sets the timeOfExpiration.
     */
    public void setTimeOfExpiration(long timeOfExpiration) {
        this.timeOfExpiration  = timeOfExpiration;
    }

    /**
     * @return activityPhase
     */
    public String getActivityPhase() {
        return activityPhase;
    }

    /**
     * @return activityPhase in enum form.
     */
    public ActivityPhase getActivityPhaseEnum() {
        return (activityPhase != null)? ActivityPhase.valueOf(activityPhase) :
                                        null;
    }

    /**
     * Sets the activityPhase.
     */
    public void setActivityPhase(String activityPhase) {
        this.activityPhase  = activityPhase;
    }

    /**
     * @return the tableId; zero means the id is not available.
     */
    public long getTableId() {
        return tableId;
    }

    /**
     * Sets the tableId
     */
    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public void setTags(byte[] tags) {
        this.tags = tags;
    }

    public byte[] getTags() {
        return tags;
    }

    /* Generates ETag */
    public byte[] getETag() {
        /*
         * The "updateTime" reflects the last change to the table, the changes
         * including update table schema/limit/tags, index create/drop.
         */
        final ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(updateTime);
        return buffer.array();
    }

    public void setReplicaState(ReplicaState state) {
        replicaState = state;
    }

    public ReplicaState getReplicaState() {
        return replicaState;
    }

    public void setInitialized(boolean value) {
        initialized = value;
    }

    /*
     * Initialized flag is for MR table, always return true for singleton
     * table
     */
    public boolean isInitialized() {
        return isMultiRegion() ? initialized : true;
    }

    public boolean isMultiRegion() {
        return getReplicaState() != null;
    }

    public void putRemoteReplica(ReplicaInfo replica) {
        if (replicas == null) {
            replicas = new HashMap<String, ReplicaInfo>();
        }
        replicas.put(replica.getServiceName(), replica);
    }

    public boolean removeRemoteReplica(String podToRemove) {
        return replicas.remove(podToRemove) != null;
    }

    public Map<String, ReplicaInfo> getReplicas() {
        if (replicas != null) {
            return replicas;
        }
        return Collections.emptyMap();
    }

    public ReplicaInfo getReplicaInfo(String pod) {
        if (replicas != null) {
            return replicas.get(pod);
        }
        return null;
    }

    public void clearAllReplicas() {
        replicaState = null;
        initialized = false;
        replicas = null;
    }

    public void setSchemaState(SchemaState state) {
        schemaState = state;
    }

    public SchemaState getSchemaState() {
        return schemaState;
    }

    public boolean isFrozen() {
        return schemaState == SchemaState.FROZEN;
    }

    public void setInitializePercent(int percent) {
        initializePercent = percent;
    }

    public int getInitializePercent() {
        if (isInitialized()) {
            return 100;
        }
        return initializePercent;
    }

    public void setMRTableVersion(int version) {
        mrTableVersion = version;
    }

    public int getMRTableVersion() {
        return mrTableVersion;
    }

    public boolean evolveIfOldVersion() {
        if (version == CURRENT_VERSION) {
            return false;
        }

        if (version < MR_TABLE_VERSION) {
            schemaState = SchemaState.MUTABLE;
            initialized = true;
        }
        version = CURRENT_VERSION;
        return true;
    }

    public Map<String, Object> toMapForAudit() {
        Map<String, Object> map = new HashMap<>();
        map.put("lifecycle", state);
        map.put("createTime", createTime);
        map.put("updateTime", updateTime);
        map.put("compartmentId", compartmentId);
        map.put("schemaVersion", schemaVersion);
        map.put("schema", schema);
        if (isMultiRegion()) {
            map.put("replicas", replicas);
        }
        return map;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        String str = "TableInfo [tableName=" + tableName
            + ", tenantId=" + tenantId
            + ", compartmentId=" + compartmentId
            + ", tableOCID=" + ocid
            + ", tableId=" + tableId
            + ", state=" + state + ", tableLimits=" + tableLimits
            + ", schemaVersion=" + schemaVersion + ", schema=" + schema
            + ", ddl=" + ddl + ", createTime=" + createTime
            + ", operationId=" + operationId
            + ", timeOfExpiration=" + timeOfExpiration
            + ", activityPhase=" + activityPhase
            + ", autoReclaimable=" + autoReclaimable;
        if (replicaState != null) {
            str += ", replicaState=" + replicaState +
                   ", initialized=" + initialized;
            if (replicas != null) {
                str += ", replicas=" + replicas.values();
            }
        }
        str += "]";
        return str;
    }
}
