/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorCode;
import oracle.nosql.util.fault.RequestFault;
import oracle.nosql.util.tmi.ReplicaInfo.ReplicaState;
import oracle.nosql.util.tmi.WorkRequest.ActionType;
import oracle.nosql.util.tmi.WorkRequest.EntityType;
import oracle.nosql.util.tmi.WorkRequest.OperationType;

/**
 * A bean class to record DDL events.
 *
 * About the tableOcid in DdlHistoryEntry:
 *   1. The tableOcid in DDLHistoryEntry stores the ocid of target table that
 *      the ddl request operated on. For create-table operation, the tableOcid
 *      will be generated and stored in DdlHistoryEntry once the workRequest
 *      is accepted and set to table metadata when execute create-table.
 *
 *   2. The tableOcid of DdlHistoryEnty is set in ACCEPT phase and may be
 *      update during EXECUTING.
 *      o In ACCEPT phase,
 *       - If table metadata exists, set the DdlHistoryEntry.tableOcid with the
 *         table ocid in metadata.
 *       - Otherwise, if ddl is create-table operation, then generate the table
 *         ocid and set to DdlHistoryEntry.tableOcid.
 *
 *      o In EXECUTING phase,
 *       - If table metdata already exists, update the tableOcid of
 *         DdlHistoryEntry with the one in metadata, because it is possible that
 *         tableOcid in DdlHistoryEntry is different from the one table metadata.
 *
 *         e.g. there are 2 create table requests for the same table submitted,
 *         both of them are in ACCEPT phase, 2 new tableOcid generated
 *         respectively for Req1 and Req2:
 *             Req1: create table t1(...)
 *             Req2: create table if not exists t1(...)
 *
 *         The Req1 will be executed firstly, then Req2. After Req1 executed
 *         successfully, Req1.tableOcid is used as t1's ocid. So Req2.tableOcid
 *         is different from actual t1's ocid, it should be updated to actual
 *         t1's ocide at earliest chance, that is the beginning of EXECUTING.
 *
 *   3. The tableOcid in DdlHistoryEntry can be null if ddl operation is not
 *      create-table and table not exists. e.g. drop table but table not exists.
 *      So when persist DdlHistoryEntry and read from store, need to handle null
 *      value for table ocid, see the DdlHistoryMD.writeRow() and readRow()
 *      method
 */
public class DdlHistoryEntry {

    /*
     * The possible types of DDL operation in the history. This enum is
     * used only to supply string values for each type.
     *
     * All operations prefixed with 'parent' are cross-region operations that
     * create and manage sub-tasks that implement multi-region DDL operations
     */
    public enum DdlOp {
        createTable,
        alter,                /* table schema evolution */
        update,               /* update table limits */
        dropTable,
        createIndex,
        dropIndex,
        changeCompartment,
        updateTableReplica,   /* update table replica info */
        parentAddReplica,
        parentAddReplicaTable,
        parentDropReplica,
        parentDropReplicaTable,
        parentCreateIndex,
        parentDropIndex,
        parentAlterTable,
        parentUpdateTable;

        public static boolean isParentOp(DdlOp op) {
            return op != null && op.ordinal() >= parentAddReplica.ordinal();
        }
    }

    public enum Status {
        ACCEPTED {
            @Override
            public int getCode() {
                return 1;
            }

            @Override
            public String getName() {
                return "Accepted";
            }
        },
        INPROGRESS {
            @Override
            public int getCode() {
                return 2;
            }

            @Override
            public String getName() {
                return "In_Progress";
            }
        },
        SUCCEEDED {
            @Override
            public int getCode() {
                return 3;
            }

            @Override
            public String getName() {
                return "Succeeded";
            }
        },
        FAILED {
            @Override
            public int getCode() {
                return 4;
            }

            @Override
            public String getName() {
                return "Failed";
            }
        };

        public abstract int getCode();
        public abstract String getName();

        private static final Map<Integer, Status> code2EnumMap =
            new HashMap<>();
        static {
            for(Status st: Status.values()) {
                code2EnumMap.put(st.getCode(), st);
            }
        }

        public static Status getByCode(int statusCode) {
            return code2EnumMap.get(statusCode);
        }
    }

    public enum ReplicationExecutionState {
        PRECHECKING(0),
        PRECHECKED(1),
        EXECUTING(2),
        DONE(3),
        ROLLBACK(4);

        private static final ReplicationExecutionState[] VALUES = values();

        private int code;

        ReplicationExecutionState(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static ReplicationExecutionState getByCode(int code) {
            try {
                return VALUES[code];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                        "unknown ReplicationExecutionState code: " + code);
            }
        }
    }

    public enum ReplicaOp {
        INITIALIZE(0),
        SET_LOCAL(1),
        ADD_REPLICA(2),
        SET_REPLICA(3),
        REMOVE_REPLICA(4),
        REMOVE_ALL(5);

        private int code;

        ReplicaOp(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        private static ReplicaOp[] VALUES = values();

        public static ReplicaOp getOp(int code) {
            try {
                return VALUES[code];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                        "unknown ReplicaInputs.Type code: " + code);
            }
        }
    }

    public enum Owner {
        USER,
        SYSTEM
    }

    private String workRequestId;
    private String tenantId;
    private String compartmentId;
    private String tableName;
    private String tableOcid;
    private String operation;  /* String values from TMRequest */
    private int status;
    private Timestamp createTime;
    private Timestamp startTime;
    private Timestamp updateTime;

    private String indexName;  /* Empty unless operation involves an index. */
    private byte[] matchETag;  /* the ETag to match */

    private boolean existFlag; /* IF EXISTS or IF NOT EXISTS was specified. */
    private byte[] tags;
    private String ddl;        /* the ddl that was executed */
    private String resultMsg;
    private String errorCode;
    private int planId;
    private String opcRequestId;
    private TableLimits limits; /* when relevant, limits are included */

    /* Indicates the ddl is to sync table limits with tenant limits */
    private boolean isSyncTableLimits;

    /*
     * Specify true to create an auto reclaimable table(also known as always
     * free tier table). Auto reclaimable table will be reclaimed automatically
     * if table no data operation for a long while(by default is 90 days).
     * This only works for create-table DDL.
     */
    private boolean autoReclaimable;
    private String retryToken;

    /*
     * Used to store the generated ddl:
     *   - Alter table ddl when using create-table-ddl for update table
     *     operation
     *   - Create table ddl for add-replica-table without if-not-exists if
     *     original ddl has if-not-exists
     */
    private String newDdl;

    /*
     * Fields for multi-region ddl
     *
     * indexDdls: array of index statements, used by add-replica.
     * homeServiceName: the pod which issued the DDL. The application code
     *                  executed against this pod.
     * targetServiceName: the target pod, used by add-replica, drop-replica.
     *                    For add-replica, it is new pod to be added.
     *                    For drop-replica, it is the pod to be dropped.
     * recipientServiceName: the recipient for the cross-region ddl request.
     *                       the cross region ddl executes here.
     * parentRequestId: parent work request Id
     * subDdlCreateTime: actual create time of the sub request, used for cross
     *                   region ddl only.
     * remoteRequestIds: store remote parent request ids.
     * subRequests: store the local sub requests.
     * currentSubRequest: store the index of current sub request.
     * repExecState: parent work execute state.
     * oboToken: the obo token.
     *
     * updateReplicaType: update replica op type.
     * replicaState: local replica info.
     * isInitialized: local replica info.
     * replicas: remote replicas info.
     * replicaToRemove: replica to be removed
     *
     * oldLimits: used to rollback to old limits on failure
     * owner: the owner of ddl, the values are USER, SYSTEM.
     *        SYSTEM: rollback ddl
     *        USER: other requests
     *
     * mrTableVerion: the MR table version on which the ddl was created
     */
    private String[] indexDdls;
    private int indexKeySize;
    private String homeServiceName;
    private String targetServiceName;
    private String recipientServiceName;
    private String parentRequestId;
    private long subDdlCreateTime;
    /* map of service name -> remote work request ids*/
    private Map<String, String> remoteRequestIds;
    private List<SubRequest> subRequests;
    private int currentSubRequest = -1;
    private ReplicationExecutionState repExecutionState;
    private String oboToken;

    /* Used for updating replica information */
    private ReplicaOp replicaOp;
    private ReplicaState replicaState;
    private boolean isInitialized;
    private ReplicaInfo[] replicas;
    private String replicaToRemove;

    /* Used for updating table limits */
    private TableLimits oldLimits;
    /*
     * The owner of ddl. Ddl owned by SYSTEM will skip checking and force to
     * execute, this is for rollback. We should make sure rollback Ddl not to
     * be blocked by capacity check.
     */
    private Owner owner;

    private int mrTableVersion;

    public DdlHistoryEntry(String workRequestId, String tenantId,
                           String compartmentId, String tableName,
                           String operation, int status,
                           Timestamp createTime, Timestamp updateTime,
                           String resultMsg, String errorCode,
                           String opcRequestId, String retryToken) {
        this(workRequestId, tenantId, compartmentId, tableName, operation,
             status, createTime, updateTime, null /* indexName */,
             null /* matchETag*/, false /* existFlag */, null /* tags */,
             null /* ddl */, resultMsg, errorCode, 0 /* planId */, opcRequestId,
             false /* autoReclaimble */, null /* limits */,
             null /* retryToken */);
    }

    public DdlHistoryEntry(String workRequestId, String tenantId,
                           String compartmentId, String tableName,
                           String operation, int status,
                           Timestamp createTime, Timestamp updateTime,
                           String indexName, byte[] matchETag,
                           boolean existFlag, byte[] tags, String ddl,
                           String resultMsg, String errorCode, int planId,
                           String opcRequestId, boolean autoReclaimable,
                           TableLimits limits, String retryToken) {
        this.workRequestId = workRequestId;
        this.tenantId = tenantId;
        this.compartmentId = compartmentId;
        this.tableName = tableName;
        this.operation = operation;
        this.status = status;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.indexName = indexName;
        this.matchETag = matchETag;
        this.existFlag = existFlag;
        this.tags = tags;
        this.ddl = ddl;
        this.resultMsg = resultMsg;
        this.errorCode = errorCode;
        this.planId = planId;
        this.opcRequestId = opcRequestId;
        this.limits = limits;
        this.autoReclaimable = autoReclaimable;
        this.retryToken = retryToken;
        this.owner = Owner.USER;
    }

    /* Need for serialization */
    public DdlHistoryEntry() {
    }

    public void setStatusEnum(Status status) {
        this.status = status.getCode();
    }

    public void newUpdateTime() {
        this.updateTime = new Timestamp(System.currentTimeMillis());
    }

    public void markSucceeded(String message) {
        status = Status.SUCCEEDED.getCode();
        resultMsg = message;
    }

    public void markFailed(Exception ex) {
        ErrorCode errCode;
        if (ex instanceof RequestFault) {
            final RequestFault rf = (RequestFault) ex;
            errCode = rf.getError();
        } else {
            errCode = ErrorCode.INTERNAL_SERVER_ERROR;
        }

        markFailed(errCode, ex.getMessage());
    }

    public void markFailed(ErrorCode errCode, String msg) {
        status = Status.FAILED.getCode();
        setError(errCode, msg);
    }

    public void setError(ErrorCode errCode, String msg) {
        errorCode = errCode.name();
        resultMsg = msg;
    }

    public void setPlanId(int planId) {
        this.planId = planId;
    }

    public String getWorkRequestId() {
        return workRequestId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getOperation() {
        return operation;
    }

    /**
     * @return operation in enum form.
     */
    public DdlOp getOperationEnum() {
        return DdlOp.valueOf(operation);
    }

    public int getStatus() {
        return status;
    }

    /**
     * @return status in enum form.
     */
    public Status getStatusEnum() {
        return Status.getByCode(status);
    }

    public void setCreateTime(Timestamp time) {
        createTime = time;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public String getIndexName() {
        return indexName;
    }

    public byte[] getMatchETag() {
        return matchETag;
    }

    public boolean isExistFlag() {
        return existFlag;
    }

    public String getCompartmentId() {
        return compartmentId;
    }

    public byte[] getTags() {
        return tags;
    }

    public void setTags(byte[] tags) {
        this.tags = tags;
    }

    public String getDdl() {
        return ddl;
    }

    public String getResultMsg() {
        return resultMsg;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public ErrorCode getErrorCodeEnum() {
        if (errorCode == null) {
            return ErrorCode.NO_ERROR;
        }
        return ErrorCode.valueOf(errorCode);
    }

    public int getPlanId() {
        return planId;
    }

    public void setLimits(TableLimits limits) {
        this.limits = limits;
    }

    public TableLimits getLimits() {
        return limits;
    }

    public void setOpcRequestId(String opcRequestId) {
        this.opcRequestId = opcRequestId;
    }

    public String getOpcRequestId() {
        return opcRequestId;
    }

    public void setTableOcid(String tableOcid) {
        this.tableOcid = tableOcid;
    }

    public String getTableOcid() {
        return tableOcid;
    }

    public boolean isAutoReclaimable() {
        return autoReclaimable;
    }

    public String getRetryToken() {
        return retryToken;
    }

    public void setNewDdl(String newDdl) {
        this.newDdl = newDdl;
    }

    public String getNewDdl() {
        return newDdl;
    }

    /*
     * MR table ddl related
     */
    public String getHomeServiceName() {
        return homeServiceName;
    }

    public void setHomeServiceName(String homeServiceName) {
        this.homeServiceName = homeServiceName;
    }

    public boolean isHomeRegion() {
        return getParentWorkRequestId() == null;
    }

    public void setTargetServiceName(String serviceName) {
        targetServiceName = serviceName;
    }

    public String getTargetServiceName() {
        return targetServiceName;
    }

    public void setRecipientServiceName(String serviceName) {
        recipientServiceName = serviceName;
    }

    public String getRecipientServiceName() {
        return recipientServiceName;
    }

    public void setIndexDdls(String[] indexDdls) {
        this.indexDdls = indexDdls;
    }

    public String[] getIndexDdls() {
        return indexDdls;
    }

    public void setIndexKeySize(int size) {
        indexKeySize = size;
    }

    public int getIndexKeySize() {
        return indexKeySize;
    }

    public void setSubDdlCreateTime(long createTime) {
        subDdlCreateTime = createTime;
    }

    public long getSubDdlCreateTime() {
        return subDdlCreateTime;
    }

    public void setParentWorkRequestId(String parentWorkRequestId) {
        this.parentRequestId = parentWorkRequestId;
    }

    public String getParentWorkRequestId() {
        return parentRequestId;
    }

    public void addSubWorkRequest(String reqId) {
        addSubWorkRequest(reqId, (Condition)null);
    }

    public void addSubWorkRequest(String reqId, Condition cond) {
        addSubWorkRequest(new SubRequest(reqId,
                            (cond == null ? null : new Condition[]{cond}),
                            false));
    }

    public void addSubWorkRequest(String reqId, Condition[] conds) {
        addSubWorkRequest(new SubRequest(reqId, conds, false));
    }

    public void addSubWorkRequest(SubRequest req) {
        if (subRequests == null) {
            subRequests = new ArrayList<SubRequest>();
        }
        subRequests.add(req);
    }

    public List<SubRequest> getSubWorkRequests() {
        if (subRequests != null) {
            return subRequests;
        }
        return Collections.emptyList();
    }

    public void setIdxCurrentSubRequest(int currentSubRequest) {
        this.currentSubRequest = currentSubRequest;
    }

    public int getIdxCurrentSubRequest() {
        return currentSubRequest;
    }

    public boolean hasNextSubRequest() {
        return (subRequests != null) &&
                currentSubRequest + 1 < subRequests.size();
    }

    public SubRequest nextSubRequest() {
        if (hasNextSubRequest()) {
            currentSubRequest++;
            return getCurrentSubRequest();
        }
        return null;
    }

    public SubRequest getCurrentSubRequest() {
        return getSubRequest(currentSubRequest);
    }

    public String getCurrentSubRequestId() {
        SubRequest subReq = getSubRequest(currentSubRequest);
        if (subReq != null) {
            return subReq.getWorkRequestId();
        }
        return null;
    }

    public Condition[] getConditions(int index) {
        SubRequest subReq = getSubRequest(index);
        if (subReq != null) {
            return subReq.getConditions();
        }
        return  null;
    }

    private SubRequest getSubRequest(int index) {
        if (subRequests != null &&
            (index >= 0 && index < subRequests.size())) {
            return subRequests.get(index);
        }
        return null;
    }

    public void setRemoteRequestIds(Map<String, String> remoteRequestIds) {
        this.remoteRequestIds = remoteRequestIds;
    }

    public void addRemoteRequestId(String serviceName, String reqId) {
        if (remoteRequestIds == null) {
            remoteRequestIds = new HashMap<String, String>();
        }
        remoteRequestIds.put(serviceName, reqId);
    }

    public String getRemoteRequestId(String serviceName) {
        if (remoteRequestIds != null) {
            return remoteRequestIds.get(serviceName);
        }
        return null;
    }

    public Map<String, String> getRemoteRequestIds() {
        if (remoteRequestIds != null) {
            return remoteRequestIds;
        }
        return Collections.emptyMap();
    }

    public boolean isParentOp() {
        return DdlOp.isParentOp(getOperationEnum());
    }

    public boolean isSubRequest() {
        return !isParentOp() && getParentWorkRequestId() != null;
    }

    public void setReplicationExecutionState(ReplicationExecutionState state) {
        repExecutionState = state;
    }

    public ReplicationExecutionState getReplicationExecutionState() {
        return repExecutionState;
    }

    public boolean isPrechecked() {
        return repExecutionState != null &&
               repExecutionState.compareTo(
                   ReplicationExecutionState.PRECHECKED) >= 0;
    }

    public boolean isDone() {
        return status == Status.SUCCEEDED.getCode() ||
               status == Status.FAILED.getCode() ;
    }

    public void setReplicaOp(ReplicaOp op) {
        replicaOp = op;
    }

    public ReplicaOp getReplicaOp() {
        return replicaOp;
    }

    public void setReplicaState(ReplicaState state) {
        replicaState = state;
    }

    public ReplicaState getReplicaState() {
        return replicaState;
    }

    public void setInitialized(boolean initialized) {
        isInitialized = initialized;
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    public void setReplicas(ReplicaInfo[] replicas) {
        this.replicas = replicas;
    }

    public ReplicaInfo[] getReplicas() {
        return replicas;
    }

    public void setReplicaToRemove(String serviceName) {
        replicaToRemove = serviceName;
    }

    public String getReplicaToRemove() {
        return replicaToRemove;
    }

    public void setOboToken(String token) {
        oboToken = token;
    }

    public String getOboToken() {
        return oboToken;
    }

    public void setOldLimits(TableLimits limits) {
        oldLimits = limits;
    }

    public TableLimits getOldLimits() {
        return oldLimits;
    }

    public void setSyncTableLimits(boolean value) {
        isSyncTableLimits = value;
    }

    public boolean isSyncTableLimit() {
        return isSyncTableLimits;
    }

    public void setOwner(Owner owner) {
        this.owner = owner;
    }

    public Owner getOwner() {
        return owner != null ? owner : Owner.USER;
    }

    public void setMRTableVersion(int version) {
        this.mrTableVersion = version;
    }

    public int getMRTableVersion() {
        return mrTableVersion;
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }

    /*
     * Converts to a WorkRequest object, representing general work request
     * information.
     */
    public WorkRequest toWorkRequest() {
        OperationType operationType = null;
        WorkRequest.Status workRequestStatus = null;
        ActionType actionType = null;

        DdlOp op = getOperationEnum();
        if (op == DdlOp.createTable) {
            operationType = OperationType.CREATE_TABLE;
        } else if (op == DdlOp.dropTable) {
            operationType = OperationType.DELETE_TABLE;
        } else {
            operationType = OperationType.UPDATE_TABLE;
        }

        long timeFinished = 0;
        switch (getStatusEnum()) {
        case ACCEPTED:
            workRequestStatus = WorkRequest.Status.ACCEPTED;
            actionType = ActionType.IN_PROGRESS;
            break;
        case INPROGRESS:
            workRequestStatus = WorkRequest.Status.IN_PROGRESS;
            actionType = ActionType.IN_PROGRESS;
            break;
        case SUCCEEDED:
            workRequestStatus = WorkRequest.Status.SUCCEEDED;
            if (op == DdlOp.createTable) {
                actionType = ActionType.CREATED;
            } else if (op == DdlOp.dropTable) {
                actionType = ActionType.DELETED;
            } else {
                actionType = ActionType.UPDATED;
            }
            timeFinished = updateTime.getTime();
            break;
        case FAILED:
            workRequestStatus = WorkRequest.Status.FAILED;
            actionType = ActionType.UPDATED;
            timeFinished = updateTime.getTime();
            break;
        }

        return new WorkRequest(workRequestId,
                               operationType,
                               workRequestStatus,
                               compartmentId,
                               tableOcid,
                               tableName,
                               EntityType.TABLE,
                               getTags(),
                               actionType,
                               createTime.getTime(),
                               (startTime != null ?
                                   startTime.getTime() : 0),
                               timeFinished,
                               getErrorCodeEnum(),
                               resultMsg);
    }

    /* The local sub ddl request information */
    public static class SubRequest {
        private String workRequestId;
        private Condition[] conditions;
        private boolean isDone;
        private Map<String, String> parameters;

        public SubRequest(String workRequestId,
                          Condition[] conditions,
                          boolean isDone) {
            this.workRequestId = workRequestId;
            this.conditions = conditions;
            this.isDone = isDone;
        }

        public String getWorkRequestId() {
            return workRequestId;
        }

        public Condition[] getConditions() {
            return conditions;
        }

        public void setIsDone(boolean val) {
            isDone = val;
        }

        public boolean isDone() {
            return isDone;
        }

        public void addParameter(String name, String value) {
            if (parameters == null) {
                parameters = new HashMap<>();
            }
            parameters.put(name, value);
        }

        public String getParameter(String name) {
            if (parameters != null) {
                return parameters.get(name);
            }
            return null;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        @Override
        public String toString() {
            return "SubRequest [reqId=" + workRequestId +
                    ", isDone=" + isDone + "]";
        }
    }

    /* The pre-condition for sub ddl request */
    public static class Condition {
        private Type type;
        private boolean result;
        private long startTime;
        private long updateTime;
        private int timeoutMs;

        public enum Type {
            SENDER_READY(0),
            RECEIVER_READY(1),
            REMOTE_DDL_DONE(2),
            REMOTE_SUB_DDL_DONE(3),
            GET_TABLE_OCID(4),
            FLUSH_PROXY_CACHE(5);

            private int code;

            Type(int code) {
                this.code = code;
            }

            public int getCode() {
                return code;
            }

            private static Type[] VALUES = values();

            public static Type getType(int code) {
                try {
                    return VALUES[code];
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new IllegalArgumentException(
                            "unknown Condition.Type code: " + code);
                }
            }
        }

        Condition(Type type, int timeoutMs) {
            this.type = type;
            result = false;
            this.timeoutMs = timeoutMs;
        }

        public Type getType() {
            return type;
        }

        public void setMatched(boolean val) {
            result = val;
        }

        public boolean matched() {
            return result;
        }

        public void setStartTime(long time) {
            startTime = time;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setUpdateTime(long time) {
            updateTime = time;
        }

        public long getUpdateTime() {
            return updateTime;
        }

        public int getTimeoutMs() {
            return timeoutMs;
        }

        protected void toStringBuilder(StringBuilder sb) {
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Condition [type=").append(getType())
              .append(", matched=").append(matched());
            if (getTimeoutMs() > 0) {
                sb.append(", timeoutMs=").append(getTimeoutMs());
            }
            if (getStartTime() > 0) {
                sb.append(", startTime=").append(getStartTime());
            }
            if (getUpdateTime() > 0) {
                sb.append(", updateTime=").append(getUpdateTime());
            }
            toStringBuilder(sb);
            sb.append("]");
            return sb.toString();
        }

        /**
         * Check if the remote ddl request is done.
         */
        public static class RemoteDdlDoneCondition extends Condition {
            private String serviceName;

            public RemoteDdlDoneCondition(String serviceName) {
                super(Type.REMOTE_DDL_DONE, 0 /* timeoutSec */);
                this.serviceName = serviceName;
            }

            public String getServiceName() {
                return serviceName;
            }

            @Override
            protected void toStringBuilder(StringBuilder sb) {
                sb.append(", service=").append(serviceName);
            }
        }

        public static class RemoteSubDdlDoneCondition extends Condition {
            private final String serviceName;
            private final int idxSubDdl;

            public RemoteSubDdlDoneCondition(String serviceName, int idxSubDdl) {
                super(Type.REMOTE_SUB_DDL_DONE, 0 /* timeoutSec */);
                this.serviceName = serviceName;
                this.idxSubDdl = idxSubDdl;
            }

            public String getServiceName() {
                return serviceName;
            }

            public int getIdxSubDdl() {
                return idxSubDdl;
            }

            @Override
            protected void toStringBuilder(StringBuilder sb) {
                sb.append(", service=").append(serviceName)
                  .append(", idxSubDdl=" + getIdxSubDdl());
            }
        }

        /**
         * Used to check with sender/receiver streaming agents.
         */
        public static class StreamAgentCondition extends Condition {
            private String serviceName;

            protected StreamAgentCondition(Type type, String serviceName) {
                super(type, 0 /* timeoutSec */);
                this.serviceName = serviceName;
            }

            public String getServiceName() {
                return serviceName;
            }

            @Override
            protected void toStringBuilder(StringBuilder sb) {
                sb.append(", service=").append(serviceName);
            }
        }

        public static class SenderReadyCondition extends StreamAgentCondition {
            public SenderReadyCondition(String serviceName) {
                super(Type.SENDER_READY, serviceName);
            }
        }

        public static class ReceiverReadyCondition extends StreamAgentCondition {
            public ReceiverReadyCondition(String serviceName) {
                super(Type.RECEIVER_READY, serviceName);
            }
        }

        /**
         * To get remote table ocid.
         */
        public static class GetTableOcidCondition extends Condition {

            private final String pod;
            private final int idxSubRequest;

            public GetTableOcidCondition(String pod, int idxSubRequest) {
                super(Type.GET_TABLE_OCID, 0 /* timeoutSec */);
                this.pod = pod;
                this.idxSubRequest = idxSubRequest;
            }

            public String getPod() {
                return pod;
            }

            public int getIdxSubRequest() {
                return idxSubRequest;
            }

            @Override
            protected void toStringBuilder(StringBuilder sb) {
                sb.append(", pod=").append(pod)
                  .append(", idxSubRequest=" + getIdxSubRequest());
            }
        }

        /**
         * To flush table from proxy cache
         */
        public static class FlushProxyCache extends Condition {
            public FlushProxyCache(int timeoutMs) {
                super(Type.FLUSH_PROXY_CACHE, timeoutMs);
            }
        }
    }
}
