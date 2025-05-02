/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.rep.migration;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;

/**
 * Object for reporting status of a partition migration.
 */
public class PartitionMigrationStatus implements Serializable {
    private static final long serialVersionUID = 1L;

    private final static String TARGET_PREFIX = "Target ";
    private final static String SOURCE_PREFIX = "Source ";

    private final boolean forTarget;

    /* Common */
    private final static String PARTITION_KEY = "Partition";
    private final int partition;
    private final static String TARGET_SHARD_KEY = "Target Shard";
    private final int targetShard;
    private final static String SOURCE_SHARD_KEY = "Source Shard";
    private final int sourceShard;
    private final long operations;
    private final static String START_TIME_KEY = "Start Time";
    private final long startTime;
    private final static String END_TIME_KEY = "End Time";
    private final long endTime;

    /* Target specific */
    private final static String STATE_KEY = "State";
    private final PartitionMigrationState state;
    private final static String REQUEST_TIME_KEY = "Request Time";
    private final long requestTime;
    private final static String ATTEMPTS_KEY = "Attempts";
    private final int attempts;
    private final static String BUSY_RESPONSES_KEY = "Busy Responses";
    private final int busyResponses;
    private final static String ERRORS_KEY = "Errors";
    private final int errors;

    /* Source specific */
    private final static String RECORDS_SENT_KEY = "Records Sent";
    private final long recordsSent;
    private final static String CLIENT_OPS_SENT_KEY = "Client Ops Sent";
    private final long clientOpsSent;

    /**
     * Returns a status object representing a partition migration target
     * initialized with data from the specified map. If there is insufficient
     * values to construct a status object, null is returned.
     *
     * @param map key/value pairs
     * @return a status object or null
     */
    public static
        PartitionMigrationStatus parseTargetStatus(Map<String,String> map) {
        String v = map.get(PARTITION_KEY);
        if (v == null) {
            return null;
        }
        final int partition = Integer.valueOf(v);
        v = map.get(TARGET_SHARD_KEY);
        if (v == null) {
            return null;
        }
        final int targetShard = Integer.valueOf(v);
        v = map.get(SOURCE_SHARD_KEY);
        if (v == null) {
            return null;
        }
        final int sourceShard = Integer.valueOf(v);
        v = map.get(STATE_KEY);
        if (v == null) {
            return null;
        }
        final PartitionMigrationState state =
            PartitionMigrationState.valueOf(v);

        v = map.get(REQUEST_TIME_KEY);
        final long requestTime = (v == null) ? 0L : Long.valueOf(v);
        v = map.get(TARGET_PREFIX + START_TIME_KEY);
        final long startTime = (v == null) ? 0L : Long.valueOf(v);
        v = map.get(TARGET_PREFIX + END_TIME_KEY);
        final long endTime = (v == null) ? 0L : Long.valueOf(v);
        v = map.get(ATTEMPTS_KEY);
        final int attempts = (v == null) ? 0 : Integer.valueOf(v);
        v = map.get(BUSY_RESPONSES_KEY);
        final int busyResponses = (v == null) ? 0 : Integer.valueOf(v);
        v = map.get(ERRORS_KEY);
        final int errors = (v == null) ? 0 : Integer.valueOf(v);

        return new PartitionMigrationStatus(state,
                                            partition,
                                            targetShard,
                                            sourceShard,
                                            0,//operations,
                                            requestTime,
                                            startTime,
                                            endTime,
                                            attempts,
                                            busyResponses,
                                            errors);
    }

    /**
     * Returns a status object representing a partition migration source
     * initialized with data from the specified map. If there are insufficient
     * values to construct a status object, null is returned.
     *
     * @param map key/value pairs
     * @return a status object or null
     */
    public static
        PartitionMigrationStatus parseSourceStatus(Map<String,String> map) {
        String v = map.get(PARTITION_KEY);
        if (v == null) {
            return null;
        }
        final int partition = Integer.valueOf(v);
        v = map.get(TARGET_SHARD_KEY);
        if (v == null) {
            return null;
        }
        final int targetShard = Integer.valueOf(v);
        v = map.get(SOURCE_SHARD_KEY);
        if (v == null) {
            return null;
        }
        final int sourceShard = Integer.valueOf(v);

        v = map.get(SOURCE_PREFIX + START_TIME_KEY);
        final long startTime = (v == null) ? 0L : Long.valueOf(v);
        v = map.get(SOURCE_PREFIX + END_TIME_KEY);
        final long endTime = (v == null) ? 0L : Long.valueOf(v);
        v = map.get(RECORDS_SENT_KEY);
        final long recordsSent = (v == null) ? 0L : Long.valueOf(v);
        v = map.get(CLIENT_OPS_SENT_KEY);
        final long clientOpsSent = (v == null) ? 0L : Long.valueOf(v);


        return new PartitionMigrationStatus(partition,
                                            targetShard,
                                            sourceShard,
                                            0,//operations,
                                            startTime,
                                            endTime,
                                            recordsSent,
                                            clientOpsSent);
    }

    /* Construct a migration target specific status */
    PartitionMigrationStatus(PartitionMigrationState state,
                             int partition,
                             int targetShard,
                             int sourceShard,
                             long operations,
                             long requestTime,
                             long startTime,
                             long endTime,
                             int attempts,
                             int busyResponses,
                             int errors) {
        assert state != null;
        this.state = state;
        this.partition = partition;
        this.targetShard = targetShard;
        this.sourceShard = sourceShard;
        this.operations = operations;
        this.requestTime = requestTime;
        this.startTime = startTime;
        this.endTime = endTime;
        this.attempts = attempts;
        this.busyResponses = busyResponses;
        this.errors = errors;
        this.recordsSent = 0;       /* source only */
        this.clientOpsSent = 0;     /* source only */
        forTarget = true;
    }

    /* Construct a migration source specific status */
    PartitionMigrationStatus(int partition,
                             int targetShard,
                             int sourceShard,
                             long operations,
                             long startTime,
                             long endTime,
                             long recordsSent,
                             long clientOpsSent) {
        this.state = null;          /* target only */
        this.partition = partition;
        this.targetShard = targetShard;
        this.sourceShard = sourceShard;
        this.requestTime = 0;       /* target only */
        this.startTime = startTime;
        this.endTime = endTime;
        this.attempts = 0;          /* target only */
        this.busyResponses = 0;     /* target only */
        this.errors = 0;            /* target only */
        this.operations = operations;
        this.recordsSent = recordsSent;
        this.clientOpsSent = clientOpsSent;
        forTarget = false;
    }

    /**
     * Returns true if this object represents the status of a partition
     * migration target.
     *
     * @return true if status for a target
     */
    public boolean forTarget() {
        return forTarget;
    }

    /**
     * Returns true if this object represents the status of a partition
     * migration source.
     *
     * @return true if status for a source
     */
    public boolean forSource() {
        return !forTarget;
    }

    /**
     * Gets the state of the partition migration. The state is only valid
     * if the object represents a partition migration target. If the object
     * represents a partition migration source null is returned.
     *
     * @return the state of the partition migration
     */
    public PartitionMigrationState getState() {
        return state;
    }

    /**
     * Gets the partition that the migration is affecting.
     *
     * @return the partition
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Gets the target shard.
     *
     * @return the target shard
     */
    public int getTargetShard() {
        return targetShard;
    }

    /**
     * Gets the source shard.
     *
     * @return the source shard
     */
    public int getSourceShard() {
        return sourceShard;
    }

    /**
     * Gets the number of operations that have been sent or received.
     *
     * @return the number of operations
     */
    public long getOperations() {
        return operations;
    }

    /**
     * Gets the time when the initial request for partition migration was
     * received on the target node.
     *
     * @return the time when the initial request
     */
    public long getRequestTime() {
        return requestTime;
    }

    /**
     * Gets the most recent start time of the partition migration process.
     * If no attempt as been made to migrate a partition zero is returned.
     *
     * @return the most recent start time
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Gets the time the last migration process ended, either by completing
     * or canceled due to error or admin intervention If a migration has
     * not started, or there is an ongoing migration zero is returned.
     *
     * @return the time the last migration process ended
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * Gets the number of times the target has attempted to start the
     * partition migration.
     *
     * @return the number of times the target has attempted to start the
     * partition migration
     */
    public int getAttempts() {
        return attempts;
    }

    /**
     * Gets the number of times the target received a busy response from the
     * source node.
     *
     * @return the number of times the target received a busy response
     */
    public int getBusyResponses() {
        return busyResponses;
    }

    /**
     * Gets the number of attempts to start partition migration which
     * failed with a non-busy error.
     *
     * @return the number of non-busy errors
     */
    public int getErrors() {
        return errors;
    }

    /**
     * Gets the number of DB records which have been sent from the source.
     *
     * @return the number of DB records sent
     */
    public long getRecordsSent() {
        return recordsSent;
    }

    /**
     * Gets the number of client operation which have been sent from the source.
     *
     * @return the number of client operation sent
     */
    public long getClientOpsSent() {
        return clientOpsSent;
    }

    /**
     * Returns the contents of this object as a map of key/value strings.
     */
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<String, String>();
        map.put(PARTITION_KEY, String.valueOf(partition));
        map.put(TARGET_SHARD_KEY, String.valueOf(targetShard));
        map.put(SOURCE_SHARD_KEY, String.valueOf(sourceShard));

        if (forTarget) {
            map.put(STATE_KEY, state.name());
            map.put(REQUEST_TIME_KEY, String.valueOf(requestTime));
            map.put(TARGET_PREFIX + START_TIME_KEY, String.valueOf(startTime));
            map.put(TARGET_PREFIX + END_TIME_KEY, String.valueOf(endTime));
            map.put(ATTEMPTS_KEY, String.valueOf(attempts));
            map.put(BUSY_RESPONSES_KEY, String.valueOf(busyResponses));
            map.put(ERRORS_KEY, String.valueOf(errors));
        } else {
            map.put(SOURCE_PREFIX + START_TIME_KEY, String.valueOf(startTime));
            map.put(SOURCE_PREFIX + END_TIME_KEY, String.valueOf(endTime));
            map.put(RECORDS_SENT_KEY, String.valueOf(recordsSent));
            map.put(CLIENT_OPS_SENT_KEY, String.valueOf(clientOpsSent));
        }
        return map;
    }

    @Override
    public String toString() {
        return display("");
    }

    public String display(String prefix) {
        final StringBuilder sb = new StringBuilder();
        if (forTarget) {
            sb.append(prefix).append("Partition migration target status:");

            sb.append("\n").append(prefix);
            sb.append(STATE_KEY + "=").append(state.name());
        } else {
            sb.append(prefix).append("Partition migration source status:");
        }

        sb.append("\n").append(prefix);
        sb.append(PARTITION_KEY + "=").append(partition);

        sb.append("\n").append(prefix);
        sb.append(TARGET_SHARD_KEY + "=").append(targetShard);

        sb.append("\n").append(prefix);
        sb.append(SOURCE_SHARD_KEY + "=").append(sourceShard);

        if (forTarget) {
            sb.append("\n").append(prefix);
            sb.append(REQUEST_TIME_KEY + "=").append(toDate(requestTime));
        }

        sb.append("\n").append(prefix);
        sb.append(START_TIME_KEY + "=").append(toDate(startTime));

        sb.append("\n").append(prefix);
        sb.append(END_TIME_KEY + "=").append(toDate(endTime));

        if (forTarget) {

            sb.append("\n").append(prefix);
            sb.append(ATTEMPTS_KEY + "=").append(attempts);

            sb.append("\n").append(prefix);
            sb.append(BUSY_RESPONSES_KEY + "=").append(busyResponses);

            sb.append("\n").append(prefix);
            sb.append(ERRORS_KEY + "=").append(errors);
        } else {
            sb.append("\n").append(prefix);
            sb.append(RECORDS_SENT_KEY + "=").append(recordsSent);

            sb.append("\n").append(prefix);
            sb.append(CLIENT_OPS_SENT_KEY + "=").append(clientOpsSent);
        }
        return sb.toString();
    }

    private String toDate(long t) {
        return (t == 0) ? "N/A" : new Date(t).toString();
    }
}
