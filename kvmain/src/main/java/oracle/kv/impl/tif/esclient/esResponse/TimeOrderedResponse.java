/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.tif.esclient.esResponse;

import oracle.kv.impl.tif.esclient.restClient.RestResponse;

/**
 * A TimeOrderedResponse is queued up in a PriorityQueue.
 * The natural ordering used for priority queue 
 * is defined on the basis of most recent response.
 * 
 * This is required because monitoring client sends
 * request to get available ES Nodes periodically.
 * It is possible that one request may get stuck for long,
 * because the ES Node the request went to is not responding.
 * A later request may go to different node and get a response.
 * 
 * Latest Response is one with the latest cluster state.
 * 
 */

public class TimeOrderedResponse implements Comparable<TimeOrderedResponse> {

    private final RestResponse response;
    private final long timeInNanos;
    private final Exception exception;

    public TimeOrderedResponse(RestResponse response, long timeInNanos) {
        this.response = response;
        this.timeInNanos = timeInNanos;
        this.exception = null;
    }

    public TimeOrderedResponse(Exception exception, long timeInNanos) {
        this.exception = exception;
        this.timeInNanos = timeInNanos;
        this.response = null;
    }

    @Override
    public int compareTo(TimeOrderedResponse o) {
        return compareTo(this, o);
    }

    private int compareTo(TimeOrderedResponse o1, TimeOrderedResponse o2) {
        if (o1.getTimeInNanos() > o2.getTimeInNanos()) {
            return 1;
        } else if (o1.getTimeInNanos() < o2.getTimeInNanos()) {
            return -1;
        } else {
            return 0;
        }
    }

    public RestResponse getResponse() {
        return response;
    }

    public long getTimeInNanos() {
        return timeInNanos;
    }

    public Exception getException() {
        return exception;
    }

}
