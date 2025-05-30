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

import java.util.Arrays;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.IndexInfo;

/**
 * Response to a TenantManager operation to get an index or indexes
 */
public class IndexResponse extends CommonResponse {
    private final IndexInfo[] indexes;
    private final int lastIndexReturned;

    public IndexResponse(int httpResponse,
                         IndexInfo[] indexes,
                         int lastIndexReturned) {
        super(httpResponse);
        this.indexes = indexes;
        this.lastIndexReturned = lastIndexReturned;
    }

    public IndexResponse(ErrorResponse err) {
        super(err);
        indexes = null;
        lastIndexReturned = 0;
    }

    /**
     * Returns an array of IndexInfo or null on failure
     */
    public IndexInfo[] getIndexInfo() {
        return indexes;
    }

    public int getLastIndexReturned() {
        return lastIndexReturned;
    }

    @Override
    public String successPayload() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"indexes\": ");
            sb.append(JsonUtils.prettyPrint(indexes)).append(",");
            sb.append("\"lastIndexReturned\": ")
              .append(lastIndexReturned).append("}");
            return sb.toString();
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexResponse [ indexes=");
        if (indexes == null) {
            sb.append("null");
        } else {
            sb.append(Arrays.toString(indexes));
        }
        sb.append(", lastIndexReturned=").append(lastIndexReturned).append("]");
        return sb.toString();
    }

}
