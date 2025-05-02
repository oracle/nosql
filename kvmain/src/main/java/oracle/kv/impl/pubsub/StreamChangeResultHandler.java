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

package oracle.kv.impl.pubsub;

import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.topo.RepGroupId;

import com.sleepycat.je.rep.stream.ChangeResultHandler;
import com.sleepycat.je.rep.stream.FeederFilterChangeResult;
import com.sleepycat.je.utilint.RateLimitingLogger;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * Object that represents the callback to process the stream change result
 */
class StreamChangeResultHandler implements ChangeResultHandler {

    /* time to wait for change result in ms */
    private final static int CHANGE_RESULT_WAIT_MS = 500;

    private static final int RL_MAX__OBJ = 1024;
    private static final int RL_INTV_MS = 1000 * 5;

    /* change result from feeder */
    private FeederFilterChangeResult result;

    /* parent worker thread to initiate the request */
    private final StoppableThread changeThread;

    private final RepGroupId gid;
    private final StreamChangeReq request;

    private final Logger logger;
    private final RateLimitingLogger<String> rllogger;

    StreamChangeResultHandler(StoppableThread changeThread,
                              RepGroupId gid,
                              StreamChangeReq request,
                              Logger logger) {
        this.changeThread = changeThread;
        this.gid = gid;
        this.request = request;
        this.logger = logger;
        result = null;
        rllogger = new RateLimitingLogger<>(RL_INTV_MS, RL_MAX__OBJ, logger);
    }

    @Override
    public synchronized void onResult(FeederFilterChangeResult res) {
        if (res == null) {
            throw new IllegalArgumentException("Result cannot be null");
        }
        result = res;
        notifyAll();
    }

    /**
     * Returns the change result or null if not available before timing out.
     *
     * @param timeoutMs the timeout in milliseconds
     *
     * @return the change result, or null if shutdown
     *
     * @throws TimeoutException if timeout in waiting
     * @throws InterruptedException if interrupted
     */
    synchronized FeederFilterChangeResult getResult(long timeoutMs)
        throws TimeoutException, InterruptedException {
        final long stop = System.currentTimeMillis() + timeoutMs;
        final long startMs = System.currentTimeMillis();
        while (!isResultReady()) {

            /* timeout */
            long waitMs = stop - System.currentTimeMillis();
            if (waitMs <= 0) {
                /* timeout */
                final String msg = "Timeout in waiting for ms=" + timeoutMs +
                                   ", timeoutMs=" + timeoutMs +
                                   ", elapsedMs=" +
                                   (System.currentTimeMillis() - startMs) +
                                   ", request=" + request;
                logger.warning(lm(msg));
                throw new TimeoutException(msg);
            }

            try {
                wait(CHANGE_RESULT_WAIT_MS);
                rllogger.log(gid.getFullName(), Level.INFO,
                             lm("Waiting for change result" +
                                ", timeoutMs=" + timeoutMs +
                                ", elapsedMs=" +
                                (System.currentTimeMillis() - startMs)));
            } catch (InterruptedException e) {
                final String msg = "Interrupted in waiting for change result " +
                                   ", elapsedMs=" +
                                   (System.currentTimeMillis() - startMs);
                logger.warning(lm(msg));
                throw new InterruptedException(msg);
            }

            /* change thread has shutdown or died, return immediately */
            if (changeThread.isShutdown()) {
                logger.warning(lm("Change stream has shutdown" +
                                  ", elapsedMs=" +
                                  (System.currentTimeMillis() - startMs)));
                return null;
            }

        }
        logger.info(lm("Change stream result is ready" +
                       ", elapsedMs=" + (System.currentTimeMillis() - startMs) +
                       ", result=" + result));
        return result;
    }


    /**
     * Returns true if the change result from feeder is ready, false otherwise.
     *
     * @return true if the change result from feeder is ready, false otherwise.
     */
    synchronized private boolean isResultReady() {
        return result != null;
    }

    private String lm(String msg) {
        return "[ChangeResultHandler-" + gid + "][ReqId=" +
               request.getReqId() + "] " + msg;
    }
}
