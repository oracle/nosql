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

package oracle.kv.impl.api;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.stats.KVStats;
import oracle.nosql.common.json.ObjectNode;

/**
 * Monitors the KVStats.
 */
public class KVStatsMonitor {

    /* The executor */
    private final ScheduledExecutorService executor;
    /* The kvstore */
    private final KVStoreImpl kvstore;
    /* The logger */
    private final Logger logger;
    /* The log interval */
    private final long logIntervalMillis;
    /* The callback */
    private final Consumer<KVStats> callback;
    /* The timestamp of last stats collecting call */
    private volatile long lastMonitorTimeMillis = System.currentTimeMillis();
    /*
     * The future of the stats obtaining task. Access must within the
     * synchronized block of this object
     */
    private Future<?> future = null;

    KVStatsMonitor(KVStoreImpl kvstore,
                   String clientID,
                   long logIntervalMillis,
                   Consumer<KVStats> callback) {
        this.executor = getExecutor();
        this.kvstore = kvstore;
        this.logger = ClientLoggerUtils.getLogger(KVStats.class, clientID);
        this.logIntervalMillis = logIntervalMillis;
        this.callback = callback;
    }

    private ScheduledExecutorService getExecutor() {
        final EndpointGroup endpointGroup =
            AsyncRegistryUtils.getEndpointGroupOrNull();
        if (endpointGroup == null) {
            return Executors.newScheduledThreadPool(
                1,
                new KVThreadFactory(KVStatsMonitor.class.getName(), logger));
        }
        /*
         * TODO: Need to try with another executor service if this one throws
         * RejectedExecutionException and the endpoint group is not shutdown
         */
        return endpointGroup.getSchedExecService();
    }

    public Logger getLogger() {
        return logger;
    }

    public long getLogIntervalMillis() {
        return logIntervalMillis;
    }

    public Consumer<KVStats> getCallback() {
        return callback;
    }

    public void start() {
        synchronized(this) {
            if (future != null) {
                return;
            }
            logger.log(Level.INFO, () -> String.format(
                "Monitoring KVStats with interval=%s ms",
                logIntervalMillis));
            future = executor.scheduleWithFixedDelay(
                this::obtainStats, 0, logIntervalMillis,
                TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Logs the stats.
     *
     * The format is as follows:
     * <pre>
     * <loggerPrefix>
     * JL|KVStats|(<humanReadableTimeRange>)/(<timestampRange>)|<KVStatsSummaryJson>
     * JL|DialogEndpointGroupPerf|(<humanReadableTimeRange>)/(<timestampRange>)|<DialogEndpointGroupPerfJson>
     * JL|NioChannelThreadPoolPerf|(<humanReadableTimeRange>)/(<timestampRange>)|<NioChannelThreadPoolPerfJson>
     * </pre>
     */
    private void obtainStats() {
        try {
            final KVStats stats = kvstore.getMonitorStats();
            final long ts = lastMonitorTimeMillis;
            final long te = System.currentTimeMillis();
            lastMonitorTimeMillis = te;
            final StringBuilder sb = new StringBuilder("\n");
            /* Print a json line of a summery of KVStats */
            sb.append(FormatUtils
                      .toJsonLine("KVStats", ts, te, stats.toJson()))
                .append("\n");
            final ObjectNode dialogEndpointGroupPerf =
                stats.getDialogEndpointGroupPerf();
            if (dialogEndpointGroupPerf != null) {
                sb.append(
                    FormatUtils.toJsonLine(
                        "DialogEndpointGroupPerf", ts, te,
                        dialogEndpointGroupPerf))
                    .append("\n");
            }
            final ObjectNode nioChannelThreadPoolPerf =
                stats.getNioChannelThreadPoolPerf();
            if (nioChannelThreadPoolPerf != null) {
                sb.append(
                    FormatUtils.toJsonLine(
                        "NioChannelThreadPoolPerf", ts, te,
                        nioChannelThreadPoolPerf))
                    .append("\n");
            }
            logger.log(Level.INFO, () -> sb.toString());
            callback.accept(stats);
        } catch (Throwable t) {
            logger.log(Level.INFO,
                       String.format("Error obtaining stats: %s",
                                     CommonLoggerUtils.getStackTrace(t)));
        }
    }

    public void stop() {
        synchronized(this) {
            if (future != null) {
                future.cancel(false);
            }
        }
    }
}
