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
import oracle.nosql.util.tmi.TableHistoryInfo;

/**
 * Response to a TenantManager getTableHistory operation.
 */
public class TableHistoryResponse extends CommonResponse {
    private final TableHistoryInfo tableHistoryInfo;
    private final int lastItem;

    public TableHistoryResponse(int httpResponse,
                                TableHistoryInfo tableHistoryInfo) {
        super(httpResponse);
        this.tableHistoryInfo = tableHistoryInfo;
        this.lastItem = 0;
    }

    public TableHistoryResponse(ErrorResponse err) {
        super(err);
        tableHistoryInfo = null;
        lastItem = 0;
    }

    /**
     * Returns a TableHistoryInfo object describing the table on success, null
     * on failure.
     */
    public TableHistoryInfo getTableHistoryInfo() {
        return tableHistoryInfo;
    }

    /**
     * Returns the index of the last history item returned. This allows
     * paging of history.
     */
    public int getLastHistoryIndex() {
        return lastItem;
    }

    @Override
    public String successPayload() {
        try {
            /* TODO: does this include last index? */
            return JsonUtils.prettyPrint(tableHistoryInfo);
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "GetTableHistoryResponse [tableHistoryInfo=" + tableHistoryInfo +
            ", toString()="  + super.toString() + "]";
    }
}
