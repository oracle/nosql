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
import oracle.nosql.util.tmi.TableInfo;

/**
 * Response to a TenantManager getTable or createTable operation. In the
 * case of a createTable operation the status of the table will be CREATING and
 * getTable needs to be called to check whether the table has been created or
 * not.
 */
public class GetTableResponse extends CommonResponse {
    private final TableInfo tableInfo;

    public GetTableResponse(int httpResponse,
                            TableInfo tableInfo) {
        super(httpResponse);
        this.tableInfo = tableInfo;
    }

    public GetTableResponse(ErrorResponse err) {
        super(err);
        tableInfo = null;
    }

    /**
     * Returns a TableInfo object describing the table on success, null
     * on failure.
     */
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public String successPayload() {
        try {
            return JsonUtils.prettyPrint(tableInfo);
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }

    @Override
    public String toString() {
        return "GetTableResponse [tableInfo=" + tableInfo + ", toString()="
                + super.toString() + "]";
    }
}
