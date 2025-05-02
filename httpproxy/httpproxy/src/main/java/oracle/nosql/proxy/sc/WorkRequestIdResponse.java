/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
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

import oracle.nosql.util.fault.ErrorResponse;

public class WorkRequestIdResponse  extends CommonResponse {

    private final String workRequestId;

    public WorkRequestIdResponse(int httpResponse, String workRequestId) {
        super(httpResponse);
        this.workRequestId = workRequestId;
    }

    public WorkRequestIdResponse(ErrorResponse err) {
        super(err);
        workRequestId = null;
    }

    /**
     * Returns workRequest Id, null on failure.
     */
    public String getWorkRequestId() {
        return workRequestId;
    }

    @Override
    public String successPayload() {
        try {
            return workRequestId;
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }

    @Override
    public String toString() {
        return "GetWorkRequestIdResponse [workRequestId=" + workRequestId +
                ", toString()=" + super.toString() + "]";
    }
}