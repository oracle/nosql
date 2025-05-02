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

package com.sleepycat.je.rep.stream;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.Serializable;

import com.sleepycat.je.utilint.TracerFormatter;

/**
 * Object that represents the filter change result from feeder and would
 * be sent to client over the wire during streaming.
 */
public class FeederFilterChangeResult implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        /* the change is applied successfully at feeder */
        OK,

        /* the change is not applicable, filter state does not change */
        NOT_APPLICABLE,

        /* fail to apply the change, filter state is undetermined and
         * subscription will terminate with exception.
         */
        FAIL
    }

    private final String reqId;

    private final Status status;

    private final long vlsn;

    private final long timestamp;

    private final String error;

    /**
     * Constructs a successful filter change response
     *
     * @param reqId     id of request
     * @param vlsn      effective vlsn
     * @param timestamp timestamp of change applied
     */
    public FeederFilterChangeResult(String reqId, long vlsn, long timestamp) {
        if (reqId == null || reqId.isEmpty()) {
            throw new IllegalArgumentException("Request id cannot be null or " +
                                               "empty");
        }
        if (vlsn == INVALID_VLSN || vlsn == NULL_VLSN) {
            throw new IllegalArgumentException("Effective vlsn cannot be null");
        }
        if (timestamp <= 0) {
            throw new IllegalArgumentException("Effective timestamp cannot be" +
                                               " non-positive.");
        }

        this.reqId = reqId;
        this.vlsn = vlsn;
        this.timestamp = timestamp;
        status = Status.OK;
        error = null;
    }

    /**
     * Constructs a failed filter change response
     *
     * @param reqId     id of request
     * @param status    status of request
     * @param error     error message
     */
    public FeederFilterChangeResult(String reqId, Status status, String error) {
        if (reqId == null || reqId.isEmpty()) {
            throw new IllegalArgumentException("Request id cannot be null or " +
                                               "empty");
        }
        if (error == null || error.isEmpty()) {
            throw new IllegalArgumentException("Error message cannot be " +
                                               "null or empty");
        }

        this.reqId = reqId;
        this.status = status;
        this.error = error;

        vlsn = NULL_VLSN;
        timestamp = 0;
    }

    /**
     * Returns the status code of the request
     *
     * @return the status code of the request
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Returns the request ID of the filter change to which this response is
     * targeted
     *
     * @return the ID of the associated request
     */
    public String getReqId() {
        return reqId;
    }

    /**
     * Returns the effective VLSN if filter change applied successfully to
     * feeder filter, null vlsn otherwise.
     *
     * @return the effective VLSN if filter change applied successfully to
     * feeder filter, null vlsn otherwise.
     */
    public long getVLSN() {
        return vlsn;
    }

    /**
     * Returns the timestamp when filter change is effective at feeder, 0
     * otherwise.
     *
     * @return the timestamp when filter change is effective at feeder, 0
     * otherwise.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the error message if feeder filter fails to apply the change,
     * or null if change is successfully applied.
     *
     * @return the error message if feeder filter fails to apply the change,
     * or null
     */
    public String getError() {
        return error;
    }

    @Override
    public String toString() {
        if (status.equals(Status.FAIL)) {
            return "[fail, id=" + reqId +
                   ((error == null || error.isEmpty()) ? "" :
                       ", error=" + error) + "]";
        }

        if (status.equals(Status.NOT_APPLICABLE)) {
            return "[not applicable, id=" + reqId + "]";
        }

        if (status.equals(Status.OK)) {
            return "[success, id=" + reqId + ", VLSN=" + vlsn +
                   ", time=" + new TracerFormatter().getDate(timestamp) + "]";
        }

        throw new IllegalArgumentException("Unsupported status " + status);
    }
}
