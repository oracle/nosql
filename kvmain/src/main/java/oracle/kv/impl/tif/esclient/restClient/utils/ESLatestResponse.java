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

package oracle.kv.impl.tif.esclient.restClient.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import oracle.kv.impl.tif.esclient.esResponse.TimeOrderedResponse;

/**
 * A utility classs to maintain the latest response for TimeOrderedResponses.
 * 
 * The getResponse call will block if no response was set ever.
 * 
 * In future this can change to use a stack with a backlog size, if required.
 * 
 * Currently only one the latest state suffices.
 * 
 * This class is currently only used to maintain the, most recent state of
 * available ES nodes.
 * 
 * It is updated by ESNodeMonitor which fetches the Node status from ES
 * periodically.
 * 
 * ESNodeMonitor also serves as FailureListener for ESHttpClient. OnFailure
 * also updates the ESNodeStatus as failureListener is mostly notified because
 * of an unresponsive ES Node.
 * 
 * The state needs to be atomically updated.
 *
 */
public class ESLatestResponse {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notNull = lock.newCondition();

    private volatile TimeOrderedResponse latestResponse = null;

    public void setIfLatest(TimeOrderedResponse resp) {
        if (resp == null) {
            return;
        }

        // Failure Response.
        if (resp.getResponse() == null && resp.getException() != null) {
            lock.lock();
            try {
                if (latestResponse != null) {
                    if (resp.compareTo(latestResponse) > 0) {
                        latestResponse = resp;
                    }
                } else {
                    latestResponse = resp;
                }
                notNull.signalAll();
            } finally {
                lock.unlock();
            }

            return;
        }
        // Success Response
        lock.lock();
        try {
            if (latestResponse != null) {
                if (resp.compareTo(latestResponse) > 0 &&
                        resp.getResponse() != null &&
                        resp.getResponse().isSuccess()) {

                    latestResponse = resp;
                }
            } else {
                latestResponse = resp;
                notNull.signalAll();
            }

        } finally {
            lock.unlock();
        }

    }

    /*
     * Note that the typical use case of this object is that same thread will
     * call setIfLatest and getResponse in that order.
     * 
     * In the erroneous case where a thread could not set any latestResponse
     * value, this thread will get stuck in getResponse until timeout or
     * another thread comes and sets the latest response. The caller can choose
     * to either wait for the next Thread from ESNodeMonitor to set the
     * latestResponse or get out with a null value by setting the timeout
     * appropriately. This case does not seem possible though and this call
     * will not be blocked mostly.
     * 
     * The current usage is in ESNodeMonitor which will cause threads invoking
     * (set,get) methods periodically.
     */
    public TimeOrderedResponse get(long timeout, TimeUnit unit)
        throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (latestResponse == null) {
                if (timeout <= 0) {
                    return null;
                }
                notNull.await(timeout, unit);
            }
            return latestResponse;
        } finally {
            lock.unlock();
        }
    }

}
