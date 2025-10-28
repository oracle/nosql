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

package oracle.nosql.proxy.sc;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.TableUsage;

/**
 * Response to a TenantManager getTableUsage operation.
 */
public class TableUsageResponse extends CommonResponse {
    private final TableUsage[] tableUsage;
    private final int lastIndexReturned;

    public TableUsageResponse(int httpResponse,
                              TableUsage[] tableUsage,
                              int lastIndexReturned) {
        super(httpResponse);
        this.tableUsage = tableUsage;
        this.lastIndexReturned = lastIndexReturned;
    }

    public TableUsageResponse(ErrorResponse err) {
        super(err);
        tableUsage = null;
        lastIndexReturned = 0;
    }

    /**
     * Returns an array of TableUsage objects
     */
    public TableUsage[] getTableUsage() {
        return tableUsage;
    }

    public int getLastIndexReturned() {
        return lastIndexReturned;
    }

    @Override
    public String successPayload() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"usages\": ");
            sb.append(JsonUtils.prettyPrint(tableUsage)).append(",");
            sb.append("\"lastIndexReturned\": ")
              .append(lastIndexReturned).append("}");
            return sb.toString();
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GetTableUsageResponse [tableUsage=[");
        if (tableUsage == null) {
            sb.append("null");
        } else {
            for (int i = 0; i < tableUsage.length; i++) {
                tableUsage[i].toBuilder(sb);
                if (i < (tableUsage.length - 1)) {
                    sb.append(",");
                }
            }
        }
        sb.append("]]");
        return sb.toString();
    }
}
