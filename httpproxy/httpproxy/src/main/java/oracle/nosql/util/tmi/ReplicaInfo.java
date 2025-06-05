/*-
 * Copyright (C) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 */

package oracle.nosql.util.tmi;

import java.util.Objects;

import oracle.nosql.common.json.JsonUtils;

/*
 * Represents remote replica table information:
 *  - serviceName. the service name of remote replica region
 *  - tableOcid. the ocid of remote replica table.
 *  - state. the replica table state.
 *  - writeLimit. the write limit of remote replica table.
 *  - mode. the capacity mode of remote replica table limits.
 */
public class ReplicaInfo {

    /*
     * CREATING
     *   - For add new replica, CREATING is the initial state and transferred
     *     to ACTIVE after add-replica complete. Replication will be initialized
     *     and started after CREATING.
     *
     * UPDATING
     *   - For update remote replica table, like create/drop index and update
     *     table limits or TTL. UPDATING is the initial state and transferred
     *     to ACTIVE after update ddl done on remote replica. Data replication
     *     is not affected by this state.
     *
     * DROPING
     *   - For drop replica, DROPPING is the initial state and the replica info
     *     will be removed from TableInfo replicas after drop-replica complete.
     *     Replication will be finished after DROPPING.
     *
     * ACTIVE
     *   - The stable state after CREATING or UPDATING.
     */
    public enum ReplicaState {
        CREATING(0),
        UPDATING(1),
        DROPPING(2),
        ACTIVE(3);

        private static final ReplicaState[] VALUES = values();

        ReplicaState(int code) {
            if (code != ordinal()) {
                throw new IllegalArgumentException("Wrong ReplicaState code");
            }
        }

        public int getCode() {
            return ordinal();
        }

        public static ReplicaState getReplicaState(int code) {
            try {
                return VALUES[code];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                        "unknown ReplicaState code: " + code);
            }
        }
    }

    private String serviceName;
    private String tableOcid;
    private int writeLimit;
    private String mode;
    private ReplicaState state;

    /* Needed for serialization */
    public ReplicaInfo() {}

    public ReplicaInfo(String serviceName,
                       String tableOcid,
                       ReplicaState state,
                       int writeLimit,
                       String mode) {
        this.serviceName = serviceName;
        this.tableOcid = tableOcid;
        this.state = state;
        this.writeLimit = writeLimit;
        this.mode = mode;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setTableOcid(String tableOcid) {
        this.tableOcid = tableOcid;
    }

    public String getTableOcid() {
        return tableOcid;
    }

    public void setState(ReplicaState state) {
        this.state = state;
    }

    public ReplicaState getState() {
        return state;
    }

    public void setWriteLimit(int writeLimit, String mode) {
        this.writeLimit = writeLimit;
        this.mode = mode;
    }

    public int getWriteLimit() {
        return writeLimit;
    }

    public String getMode() {
        return mode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || !(other instanceof ReplicaInfo)) {
            return false;
        }

        ReplicaInfo o1 = (ReplicaInfo)other;
        return Objects.equals(getServiceName(), o1.getServiceName()) &&
               Objects.equals(getTableOcid(), o1.getTableOcid()) &&
               Objects.equals(getState(), o1.getState()) &&
               getWriteLimit() == o1.getWriteLimit();
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}
