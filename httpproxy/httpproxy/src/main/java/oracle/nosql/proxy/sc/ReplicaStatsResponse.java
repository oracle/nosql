/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.sc;

import java.util.List;
import java.util.Map;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.ReplicaStats;

/**
 * Response to a TenantManager getReplicaStats operation.
 */
public class ReplicaStatsResponse extends CommonResponse {
    /* Map of region name and its replica stats. */
    private final Map<String, List<ReplicaStats>> statsRecords;
    private final long nextStartTime;

    public ReplicaStatsResponse(int httpResponse,
                                Map<String, List<ReplicaStats>> statsRecords,
                                long startTime) {
        super(httpResponse);
        this.statsRecords = statsRecords;
        nextStartTime = getNextStartTime(startTime, statsRecords);
    }

    public ReplicaStatsResponse(ErrorResponse err) {
        super(err);
        statsRecords = null;
        nextStartTime = 0;
    }

    /**
     * Returns a map of Replica name and its ReplicaStats objects
     */
    public Map<String, List<ReplicaStats>> getStatsRecords() {
        return statsRecords;
    }

    public long getNextStartTime() {
        return nextStartTime;
    }

    /*
     * Return the next start time used to pass to GetReplicaStats request to
     * fetch next batch of records
     *   - If no more record returned, return the current start time.
     *   - Return the max timestamp of records plus 1ms.
     */
    private long getNextStartTime(long startTime,
                                  Map<String, List<ReplicaStats>> results) {

        if (results.isEmpty()) {
            return startTime;
        }

        long lastStartTime = startTime;
        for (List<ReplicaStats> records : results.values()) {
            lastStartTime = Math.max(lastStartTime,
                                     records.get(records.size() - 1).getTime());
        }
        return lastStartTime + 1;
    }

    @Override
    public String successPayload() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"replicaStats\":")
              .append(JsonUtils.prettyPrint(statsRecords)).append(",")
              .append("\"nextStartTime\": ").append(nextStartTime)
              .append("}");
            return sb.toString();
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }
}
