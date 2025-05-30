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

/**
 * Response to a TenantManager listTables operation.
 */
public class ListTableResponse extends CommonResponse {

    private final String[] tables;
    private final int lastIndexReturned;

    public ListTableResponse(int httpResponse,
                             String[] tables,
                             int lastIndexReturned) {
        super(httpResponse);
        this.tables = tables;
        this.lastIndexReturned = lastIndexReturned;
    }

    public ListTableResponse(ErrorResponse err) {
        super(err);
        tables = null;
        lastIndexReturned = 0;
    }

    /**
     * Returns a list of table names with compartments, or null on failure
     */
    public String[] getTables() {
        return tables;
    }

    public int getLastIndexReturned() {
        return lastIndexReturned;
    }

    /**
     * {
     *   "tables" : [...],
     *   "lastIndex" : 5
     * }
     *
     */
    @Override
    public String successPayload() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"tables\": ");
            sb.append(JsonUtils.prettyPrint(tables)).append(",");
            sb.append("\"lastIndex\": ").append(tables.length).append("}");
            return sb.toString();
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }
}
