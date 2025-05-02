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

package oracle.kv.impl.xregion.stat;

import java.util.concurrent.atomic.AtomicLong;

import oracle.nosql.common.json.JsonUtils;

/**
 * Request and response manager statistics
 */
public class ReqRespStat {
    /**
     * # of requests
     */
    private final AtomicLong requests = new AtomicLong(0);
    /**
     * # of responses
     */
    private final AtomicLong responses = new AtomicLong(0);

    public long getRequests() {
        return requests.get();
    }

    public long getResponses() {
        return responses.get();
    }

    /* agent setters */
    public void incrRequest() {
        requests.getAndIncrement();
    }

    public long getReqAndRefresh() {
        return requests.getAndSet(0);
    }

    public void incrResponse() {
        responses.getAndIncrement();
    }

    public long getRespAndRefresh() {
        return responses.getAndSet(0);
    }

    @Override
    public String toString() {
        return JsonUtils.print(this, true);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ReqRespStat)) {
            return false;
        }
        final ReqRespStat other = (ReqRespStat) obj;
        return super.equals(obj) &&
               requests.get() == other.requests.get() &&
               responses.get() == other.responses.get();
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
               Long.hashCode(requests.get()) +
               Long.hashCode(responses.get());
    }
}
