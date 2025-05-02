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

package oracle.kv.impl.tif.esclient.esResponse;

import java.io.IOException;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ClusterHealthResponse extends ESResponse implements
        JsonResponseObjectMapper<ClusterHealthResponse> {

    private String clusterName;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public int getNumberOfPendingTasks() {
        return numberOfPendingTasks;
    }

    public void setNumberOfPendingTasks(int numberOfPendingTasks) {
        this.numberOfPendingTasks = numberOfPendingTasks;
    }

    public int getNumberOfInFlightFetch() {
        return numberOfInFlightFetch;
    }

    public void setNumberOfInFlightFetch(int numberOfInFlightFetch) {
        this.numberOfInFlightFetch = numberOfInFlightFetch;
    }

    public int getDelayedUnassignedShards() {
        return delayedUnassignedShards;
    }

    public void setDelayedUnassignedShards(int delayedUnassignedShards) {
        this.delayedUnassignedShards = delayedUnassignedShards;
    }

    public long getTaskMaxWaitingTime() {
        return taskMaxWaitingTime;
    }

    public void setTaskMaxWaitingTime(long taskMaxWaitingTime) {
        this.taskMaxWaitingTime = taskMaxWaitingTime;
    }

    public boolean isTimedOut() {
        return timedOut;
    }

    public void setTimedOut(boolean timedOut) {
        this.timedOut = timedOut;
    }

    public int getNumberOfNodes() {
        return numberOfNodes;
    }

    public void setNumberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }

    public int getNumberOfDataNodes() {
        return numberOfDataNodes;
    }

    public void setNumberOfDataNodes(int numberOfDataNodes) {
        this.numberOfDataNodes = numberOfDataNodes;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public void setActiveShards(int activeShards) {
        this.activeShards = activeShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public void setRelocatingShards(int relocatingShards) {
        this.relocatingShards = relocatingShards;
    }

    public int getActivePrimaryShards() {
        return activePrimaryShards;
    }

    public void setActivePrimaryShards(int activePrimaryShards) {
        this.activePrimaryShards = activePrimaryShards;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public void setInitializingShards(int initializingShards) {
        this.initializingShards = initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    public void setUnassignedShards(int unassignedShards) {
        this.unassignedShards = unassignedShards;
    }

    public double getActiveShardsPercent() {
        return activeShardsPercent;
    }

    public void setActiveShardsPercent(double activeShardsPercent) {
        this.activeShardsPercent = activeShardsPercent;
    }

    public ClusterHealthStatus getClusterHealthStatus() {
        return clusterHealthStatus;
    }

    public void setClusterHealthStatus(ClusterHealthStatus clusterHealthStatus) {
        this.clusterHealthStatus = clusterHealthStatus;
    }

    private int numberOfPendingTasks = 0;
    private int numberOfInFlightFetch = 0;
    private int delayedUnassignedShards = 0;
    private long taskMaxWaitingTime = 0L;
    private boolean timedOut = false;
    private int numberOfNodes = 0;
    private int numberOfDataNodes = 0;
    private int activeShards = 0;
    private int relocatingShards = 0;
    private int activePrimaryShards = 0;
    private int initializingShards = 0;
    private int unassignedShards = 0;
    private double activeShardsPercent = 0.0;
    private ClusterHealthStatus clusterHealthStatus = null;

    private static final String CLUSTER_NAME = "cluster_name";
    private static final String STATUS = "status";
    private static final String TIMED_OUT = "timed_out";
    private static final String NUMBER_OF_NODES = "number_of_nodes";
    private static final String NUMBER_OF_DATA_NODES = "number_of_data_nodes";
    private static final String NUMBER_OF_PENDING_TASKS =
        "number_of_pending_tasks";
    private static final String NUMBER_OF_IN_FLIGHT_FETCH =
        "number_of_in_flight_fetch";
    private static final String DELAYED_UNASSIGNED_SHARDS =
        "delayed_unassigned_shards";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS =
        "task_max_waiting_in_queue_millis";
    private static final String ACTIVE_SHARDS_PERCENT =
        "active_shards_percent";
    private static final String ACTIVE_PRIMARY_SHARDS =
        "active_primary_shards";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";

    public ClusterHealthResponse() {

    }

    /**
     * These are the fields to be parsed available in response. Other fields
     * with scalar values are ignored. Non Scalar fields will be skipped.
     * 
     * {
     * 
     * "cluster_name" : "oracle_kv",
     * 
     * "status" : "green",
     * 
     * "timed_out" : false,
     * 
     * "number_of_nodes" : 2,
     * 
     * "number_of_data_nodes" : 2,
     * 
     * "active_primary_shards" : 1,
     * 
     * "active_shards" : 2,
     * 
     * "relocating_shards" : 0,
     * 
     * "initializing_shards" : 0,
     * 
     * "unassigned_shards" : 0,
     * 
     * "delayed_unassigned_shards" : 0,
     * 
     * "number_of_pending_tasks" : 0,
     * 
     * "number_of_in_flight_fetch" : 0,
     * 
     * "task_max_waiting_in_queue_millis" : 0,
     * 
     * "active_shards_percent_as_number" : 100.0
     *
     * }
     */
    @Override
    public ClusterHealthResponse buildFromJson(JsonParser parser)
        throws IOException {

        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
        String currentFieldName = null;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {

            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.currentName();
                continue;
            } else if (token.isScalarValue()) {

                if (CLUSTER_NAME.equals(currentFieldName)) {
                    clusterName = parser.getText();
                } else if (STATUS.equals(currentFieldName)) {
                    String status = parser.getText();
                    clusterHealthStatus =
                        ClusterHealthStatus.fromString(status);
                } else if (TIMED_OUT.equals(currentFieldName)) {
                    timedOut = parser.getBooleanValue();
                } else if (NUMBER_OF_NODES.equals(currentFieldName)) {
                    numberOfNodes = parser.getIntValue();
                } else if (NUMBER_OF_DATA_NODES
                           .equals(currentFieldName)) {
                    numberOfDataNodes = parser.getIntValue();
                } else if (NUMBER_OF_PENDING_TASKS
                           .equals(currentFieldName)) {
                    numberOfPendingTasks = parser.getIntValue();
                } else if (NUMBER_OF_IN_FLIGHT_FETCH
                           .equals(currentFieldName)) {
                    numberOfInFlightFetch = parser.getIntValue();
                } else if (DELAYED_UNASSIGNED_SHARDS
                           .equals(currentFieldName)) {
                    delayedUnassignedShards = parser.getIntValue();
                } else if (TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS
                           .equals(currentFieldName)) {
                    taskMaxWaitingTime = parser.getLongValue();
                } else if (ACTIVE_SHARDS_PERCENT
                           .equals(currentFieldName)) {
                    activeShardsPercent = parser.getDoubleValue();
                } else if (ACTIVE_PRIMARY_SHARDS
                           .equals(currentFieldName)) {
                    activePrimaryShards = parser.getIntValue();
                } else if (ACTIVE_SHARDS.equals(currentFieldName)) {
                    activeShards = parser.getIntValue();
                } else if (RELOCATING_SHARDS.equals(currentFieldName)) {
                    relocatingShards = parser.getIntValue();
                } else if (INITIALIZING_SHARDS.equals(currentFieldName)) {
                    initializingShards = parser.getIntValue();
                } else if (UNASSIGNED_SHARDS.equals(currentFieldName)) {
                    unassignedShards = parser.getIntValue();
                }

            } else if (token.isStructStart()) {
                parser.skipChildren();
            }
        }

        parsed(true);
        return this;
    }

    @Override
    public ClusterHealthResponse buildErrorReponse(ESException e) {
        return null;
    }

    @Override
    public ClusterHealthResponse buildFromRestResponse(RestResponse restResp) {
        return null;
    }

    public static enum ClusterHealthStatus {
        GREEN((byte) 0), YELLOW((byte) 1), RED((byte) 2);

        private byte value;

        ClusterHealthStatus(byte value) {
            this.value = value;
        }

        public byte value() {
            return value;
        }

        public static ClusterHealthStatus fromValue(byte value) {
            switch (value) {
                case 0:
                    return GREEN;
                case 1:
                    return YELLOW;
                case 2:
                    return RED;
                default:
                    throw new IllegalArgumentException(value
                            + " is invalid for ClusterHealthStatus");

            }

        }

        public static ClusterHealthStatus fromString(String status) {
            status = status.trim();
            if (status.equalsIgnoreCase("green")) {
                return GREEN;
            } else if (status.equalsIgnoreCase("yellow")) {
                return YELLOW;
            } else if (status.equalsIgnoreCase("red")) {
                return RED;
            } else {
                throw new IllegalArgumentException
                    ("unknown cluster health status [" + status + "]");
            }
        }
    }

}
