/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.subscription;

import com.sleepycat.je.DatabaseException;

/**
 * Object represents exception if feeder fails to apply the filter change
 */
public class FilterChangeException extends DatabaseException {

    private static final long serialVersionUID = 1;

    /* change request id */
    private final String reqId;

    public FilterChangeException(String reqId, String error) {
        super(error);
        this.reqId = reqId;
    }

    public String getReqId() {
        return reqId;
    }
}
