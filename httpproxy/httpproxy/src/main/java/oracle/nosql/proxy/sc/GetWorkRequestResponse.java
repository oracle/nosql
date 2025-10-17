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
import oracle.nosql.util.tmi.WorkRequest;

/**
 * Response to a TenantManager getWorkRequest operation.
 */
public class GetWorkRequestResponse extends CommonResponse {
    private final WorkRequest workRequest;

    public GetWorkRequestResponse(int httpResponse, WorkRequest workRequest) {
        super(httpResponse);
        this.workRequest = workRequest;
    }

    public GetWorkRequestResponse(ErrorResponse err) {
        super(err);
        workRequest = null;
    }

    /**
     * Returns a WorkRequest object
     */
    public WorkRequest getWorkRequest() {
        return workRequest;
    }

    @Override
    public String successPayload() {
        try {
            return JsonUtils.print(workRequest);
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }

    @Override
    public String toString() {
        return "GetWorkRequestResponse [workRequest=" + workRequest +
                ", toString()=" + super.toString() + "]";
    }
}
