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

package oracle.nosql.proxy;


/**
 * This class encapsulates request limits that are enforced per-request.
 * The limits, if present, are acquired from the TenantManager based on
 * table name and tenant id.
 *
 * It mirrors a related class, TableRequestLimits, from the nosqlutils library
 * but that class is not used directly in order to insulate the proxy from
 * changes.
 */
public class RequestLimits {

    private final int primaryKeySizeLimit;   // Primary key size limit, bytes
    private final int rowSizeLimit;          // Per single row size limit
    private final int requestSizeLimit;      // Per request size limit
    private final int requestReadKBLimit;    // Read data size
    private final int requestWriteKBLimit;   // Write data size
    private final int queryStringSizeLimit;  // Query string size
    private final int batchOpNumberLimit;    // ops allowed in a single batch
    private final int batchRequestSizeLimit; // total size of a batched op
    private final int queryUpdateSizeLimit;  // query update size

    private static int PRIMARY_KEY_SIZE_LIMIT = 64; /* 64b */
    private static int ROW_SIZE_LIMIT = 512 * 1024; /* 512kb */
    private static int REQUEST_SIZE_LIMIT = 2 * 1024 * 1024; /* 2mb */
    private static int REQUEST_READ_KB_LIMIT = 2 * 1024; /* 2mb */
    private static int REQUEST_WRITE_KB_LIMIT = 2 * 1024; /* 2mb */
    private static int QUERY_STRING_SIZE_LIMIT = 10 * 1024; /* 10kb */
    private static int BATCH_OP_NUMBER_LIMIT = 50;
    private static int BATCH_REQUEST_SIZE_LIMIT =
        25 * 1024 * 1024; /* 25mb */

    /*
     * These default limis are only used by Cloudsim at this point, which
     * implies tests as well.
     */
    private static RequestLimits defaultLimits =
        new RequestLimits(PRIMARY_KEY_SIZE_LIMIT,
                          ROW_SIZE_LIMIT,
                          REQUEST_SIZE_LIMIT,
                          REQUEST_READ_KB_LIMIT,
                          REQUEST_WRITE_KB_LIMIT,
                          QUERY_STRING_SIZE_LIMIT,
                          BATCH_OP_NUMBER_LIMIT,
                          BATCH_REQUEST_SIZE_LIMIT);

    public static RequestLimits defaultLimits() {
        return defaultLimits;
    }

    /*
     * MAX_VALUE-1 isn't sufficient. Divide by 3 leaves plenty of room for
     * limits and code that might do limits-based math
     */
    private static int largeLimit = Integer.MAX_VALUE/3;
    private static RequestLimits defaultNoLimits =
        new RequestLimits(largeLimit,
                          largeLimit,
                          largeLimit,
                          largeLimit,
                          largeLimit,
                          largeLimit,
                          largeLimit,
                          largeLimit);

    public static RequestLimits defaultNoLimits() {
        return defaultNoLimits;
    }

    public RequestLimits(int primaryKeySizeLimit,
                         int rowSizeLimit,
                         int requestSizeLimit,
                         int requestReadKBLimit,
                         int requestWriteKBLimit,
                         int queryStringSizeLimit,
                         int batchOpNumberLimit,
                         int batchRequestSizeLimit) {

        this.primaryKeySizeLimit = primaryKeySizeLimit;
        this.rowSizeLimit = rowSizeLimit;
        this.requestSizeLimit = requestSizeLimit;
        this.requestReadKBLimit = requestReadKBLimit;
        this.requestWriteKBLimit = requestWriteKBLimit;
        this.queryStringSizeLimit = queryStringSizeLimit;
        this.batchOpNumberLimit = batchOpNumberLimit;
        this.batchRequestSizeLimit = batchRequestSizeLimit;

        /* this is calculated */
        queryUpdateSizeLimit = rowSizeLimit + queryStringSizeLimit;
    }

    public int getPrimaryKeySizeLimit() {
        return primaryKeySizeLimit;
    }

    public int getRowSizeLimit() {
        return rowSizeLimit;
    }

    public int getRequestSizeLimit() {
        return requestSizeLimit;
    }

    public int getRequestReadKBLimit() {
        return requestReadKBLimit;
    }

    public int getRequestWriteKBLimit() {
        return requestWriteKBLimit;
    }

    public int getQueryStringSizeLimit() {
        return queryStringSizeLimit;
    }

    public int getBatchOpNumberLimit() {
        return batchOpNumberLimit;
    }

    public int getBatchRequestSizeLimit() {
        return batchRequestSizeLimit;
    }

    public int getQueryUpdateSizeLimit() {
        return queryUpdateSizeLimit;
    }
}
