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

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.TableInfo;

/**
 * Response to a TenantManager listTableInfos operation.
 */
public class ListTableInfoResponse extends CommonResponse {

    private final TableInfo[] tableInfos;
    /* The maximum number of reclaimable tables allowed in the tenancy. */
    private final int maxAutoReclaimableTables;
    /* The current number of reclaimable tables in the tenancy. */
    private final int autoReclaimableTables;
    /* The maximum number of auto scaling tables allowed in the tenancy. */
    private int maxAutoScalingTables;
    /* The current number of auto scaling tables in the tenancy. */
    private int autoScalingTables;
    /* The regions that are available for replication.*/
    private String[] availableReplicas;
    private final int lastIndexReturned;

    public ListTableInfoResponse(int httpResponse,
                                 TableInfo[] tableInfos,
                                 int maxAutoReclaimableTables,
                                 int autoReclaimableTables,
                                 int maxAutoScalingTables,
                                 int autoScalingTables,
                                 String[] availableReplicas,
                                 int lastIndexReturned) {
        super(httpResponse);
        this.tableInfos = tableInfos;
        this.maxAutoReclaimableTables = maxAutoReclaimableTables;
        this.autoReclaimableTables = autoReclaimableTables;
        this.maxAutoScalingTables = maxAutoScalingTables;
        this.autoScalingTables = autoScalingTables;
        this.availableReplicas = availableReplicas;
        this.lastIndexReturned = lastIndexReturned;
    }

    public ListTableInfoResponse(ErrorResponse err) {
        super(err);
        tableInfos = null;
        maxAutoReclaimableTables = 0;
        autoReclaimableTables = 0;
        lastIndexReturned = 0;
    }

    /**
     * Returns a list of TableInfos
     */
    public TableInfo[] getTableInfos() {
        return tableInfos;
    }

    public int getMaxAutoReclaimableTables() {
        return maxAutoReclaimableTables;
    }

    public int getAutoReclaimableTables() {
        return autoReclaimableTables;
    }

    public int getMaxAutoScalingTables() {
        return maxAutoScalingTables;
    }

    public int getAutoScalingTables() {
        return autoScalingTables;
    }

    public int getLastIndexReturned() {
        return lastIndexReturned;
    }

    public String[] getAvailableReplicas() {
        return availableReplicas;
    }

    /**
     * {
     *   "tables" : [...],
     *   "maxAutoReclaimableTables": <m>,
     *   "autoReclaimableTables": <n>,
     *   "avaiableReplicas": [<name>]
     *   "lastIndex" : 5
     * }
     *
     */
    @Override
    public String successPayload() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"tables\": ");
            sb.append(JsonUtils.prettyPrint(tableInfos)).append(",");
            sb.append("\"maxAutoReclaimableTables\": ")
              .append(maxAutoReclaimableTables).append(",");
            sb.append("\"autoReclaimableTables\": ")
              .append(autoReclaimableTables).append(",");

            if (availableReplicas != null) {
                boolean first = true;
                sb.append("\"availableReplicas\": [");
                for (String name : availableReplicas) {
                    sb.append(name);
                    if (first) {
                        first = false;
                    } else {
                        sb.append(", ");
                    }
                    sb.append("\"").append(name).append("\"");
                }
                sb.append("]");
            }

            sb.append("\"lastIndex\": ").append(tableInfos.length).append("}");
            return sb.toString();
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ListTableInfoResponse [tableInfos=[");
        if (tableInfos == null) {
            sb.append("null");
        } else {
            for (int i = 0; i < tableInfos.length; i++) {
                sb.append(tableInfos[i].toString());
                if (i < (tableInfos.length - 1)) {
                    sb.append(",");
                }
            }
        }
        sb.append("]]");
        return sb.toString();
    }
}
