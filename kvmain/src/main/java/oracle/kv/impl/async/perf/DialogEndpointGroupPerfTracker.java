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

package oracle.kv.impl.async.perf;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.IOBufferPool;
import oracle.kv.impl.util.RateLimitingLogger;


/**
 * Tracks the dialog performance metrics of an endpoint group.
 */
public class DialogEndpointGroupPerfTracker {

    // TODO: The following fields are used as a temporary fix for the issue
    // that the async data overflows the stats ([KVSTORE-1818]). Remove when
    // the more suitable solution is ready.
    /**
     * Whether to do rate limiting. Default to true. Can be set to false for
     * testing.
     */
    public static volatile boolean enableRateLimiting = true;
    /**
     * The duration in seconds that we want to keep for stats data.
     * Default to 7 days.
     */
    private static final int STATS_ROTATION_MAX_DURATION_SECS = 7 * 24 * 3600;
    /**
     * The number of bytes we can generate per second such that we can satisfy
     * STATS_ROTATION_MAX_DURATION_SECS with default settings.
     *
     * We use the default value of ParameterState.SN_LOG_FILE_COUNT_DEFAULT
     * (20) and ParameterState.SN_LOG_FILE_LIMIT_DEFAULT (5M), but we do not
     * want to import ParameterState since this class can be used in the
     * client.
     */
    private static final long MAX_OBTAIN_NUM_BYTES_PER_SEC =
        20 * 5 * 1024 * 1024
        / STATS_ROTATION_MAX_DURATION_SECS;
    /**
     * The approximate number of bytes per handler in DialogEndpointPerf. This
     * is estimated from release test data.
     */
    private static final long NUM_BYTES_PER_ENDPOINT = 5000;
    /**
     * The timestamp in nanos when we last obtain the perf.
     */
    private volatile long lastObtainTimeNanos = -1;
    /**
     * The duration after which we can obtain a new perf without vilating
     * STATS_ROTATION_MAX_DURATION_SECS.
     */
    private volatile long nextObtainDurationNanos = 0;

    /** The logger. */
    private final RateLimitingLogger<String> rateLimitingLogger;
    /** Resource manager perf tracker */
    private final DialogResourceManagerPerfTracker
        dialogResourceManagerPerfTracker;
    /**
     * A map of endpoint perf name and the perf tracker. Creator endpoints will
     * use the remote IP address as the name. Responder endpoints will use the
     * local listening port number.
     *
     * Note that the entries in the map is never removed. We do not expect the
     * map to be very large. The number of creator endpoints is as many as the
     * number of RNs which should not exceeds several hundreds. The number of
     * responder endpoints is as many as the number of service which should not
     * exceeds a hundred.
     */
    private final Map<String, DialogEndpointPerfTracker>
        endpointPerfTrackers = new ConcurrentHashMap<>();

    private volatile long currIntervalStart;

    public DialogEndpointGroupPerfTracker(Logger logger,
                                          boolean forClientOnly)
    {
        this.rateLimitingLogger = new RateLimitingLogger<String>(
            30 * 60 * 1000 /* 30 minutes */,
            10 /* types of objects */,
            logger);
        this.dialogResourceManagerPerfTracker =
            new DialogResourceManagerPerfTracker(forClientOnly);
    }

    /** Returns an endpoint perf tracker. */
    public DialogEndpointPerfTracker getDialogEndpointPerfTracker(
        String endpointPerfName,
        boolean isCreator)
    {
        return endpointPerfTrackers.computeIfAbsent(
            endpointPerfName,
            (name) ->
            new DialogEndpointPerfTracker(name, isCreator));
    }

    /** Removes the endpoint perf tracker. */
    public void removeEndpointPerfTracker(DialogEndpointPerfTracker tracker) {
        endpointPerfTrackers.remove(tracker.getName(), tracker);
    }

    /** Returns the dialog resource manager perf tracker. */
    public DialogResourceManagerPerfTracker
        getDialogResourceManagerPerfTracker() {

        return dialogResourceManagerPerfTracker;
    }

    public Optional<DialogEndpointGroupPerf > obtain(String watcherName,
                                                     boolean clear) {
        final long currNanos = System.nanoTime();
        if (enableRateLimiting
            && (currNanos - lastObtainTimeNanos <= nextObtainDurationNanos)) {
            return Optional.empty();
        }
        final long ts = currIntervalStart;
        final long te = System.currentTimeMillis();
        currIntervalStart = te;
        final Map<String, DialogEndpointPerf> creatorPerf =
            new HashMap<>();
        final Map<String, DialogEndpointPerf> responderPerf =
            new HashMap<>();
        endpointPerfTrackers.values().forEach(
            (tracker) -> {
                final DialogEndpointPerf perf =
                    tracker.obtain(watcherName, clear);
                if (perf.isCreator()) {
                    creatorPerf.put(perf.getName(), perf);
                } else {
                    responderPerf.put(perf.getName(), perf);
                }
            });
        final DialogEndpointGroupPerf groupPerf =
            new DialogEndpointGroupPerf(
                ts, te, creatorPerf, responderPerf,
                dialogResourceManagerPerfTracker.obtain(),
                IOBufferPool.obtain(watcherName, clear));
        final int numEndpointPerfs = groupPerf.getNumEndpointPerfs();
        final long nbytes = NUM_BYTES_PER_ENDPOINT * numEndpointPerfs;
        final long delaySecs = nbytes / MAX_OBTAIN_NUM_BYTES_PER_SEC;
        final long delayNanos = TimeUnit.SECONDS.toNanos(delaySecs);
        lastObtainTimeNanos = currNanos;
        nextObtainDurationNanos = delayNanos;
        rateLimitingLogger.log(
            "obtain", Level.INFO,
            () -> String.format(
                "Obtained %s with %s endpoints and estimated %s bytes; "
                + "next obtain after %s seconds",
                DialogEndpointGroupPerf.class.getSimpleName(),
                numEndpointPerfs, nbytes, delaySecs));
        return Optional.of(groupPerf);
    }

    /**
     * Returns the number of endpoint perf trackers for testing.
     */
    public int getNumOfEndpointPerfTrackers() {
        return endpointPerfTrackers.size();
    }
}
