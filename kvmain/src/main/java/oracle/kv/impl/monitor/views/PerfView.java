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

package oracle.kv.impl.monitor.views;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.measurement.ConciseStats;
import oracle.kv.impl.measurement.EnvStats;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.RepEnvStats;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.MonitorKeeper;
import oracle.kv.impl.monitor.View;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Takes a StatsPacket from the RepNodes and dispatches it to appropriate
 * listeners.
 */
public class PerfView implements View {

    private final MonitorKeeper admin;

    private final Set<ViewListener<PerfEvent>> listeners;
    private final Logger statsLogger;

    public PerfView(AdminServiceParams params, MonitorKeeper admin) {
        this.admin = admin;
        listeners = new HashSet<ViewListener<PerfEvent>>();

        statsLogger =
            LoggerUtils.getStatFileLogger(this.getClass(),
                                          params.getGlobalParams(),
                                          params.getStorageNodeParams());
    }

    @Override
    public String getName() {
        return Monitor.PERF_FILE_VIEW;
    }

    @Override
    public Set<MeasurementType> getTargetMetricTypes() {
        return Collections.singleton(Metrics.RNSTATS);
    }

    /**
     * Distribute the perf stats to any listeners, packaged as
     * a PerfEvent.
     */
    @Override
    public void applyNewInfo(ResourceId resourceId,  Measurement m) {

        if (resourceId.getType() == ResourceType.ARB_NODE) {
            return;
        }

        /* TODO add new read/write stats to .perf output too? */
        final StatsPacket statsPacket = (StatsPacket) m;
        final LatencyInfo singleInterval =
            statsPacket.get(PerfStatType.USER_SINGLE_OP_INT);
        final LatencyInfo singleCumulative =
            statsPacket.get(PerfStatType.USER_SINGLE_OP_CUM);
        final LatencyInfo multiInterval =
            statsPacket.get(PerfStatType.USER_MULTI_OP_INT);
        final LatencyInfo multiCumulative =
            statsPacket.get(PerfStatType.USER_MULTI_OP_CUM);

        final RepEnvStats repStats = statsPacket.getRepEnvStats();

        /*
         * Only create a PerfEvent if there were single or multi operations
         * in this interval, and therefore some kind of new activity to report.
         */
        if ((singleInterval.getLatency().getOperationCount() != 0) ||
            (multiInterval.getLatency().getOperationCount() != 0)) {

            final PerfEvent event = new PerfEvent
                (resourceId,
                 singleInterval, singleCumulative,
                 admin.getLatencyCeiling(resourceId),
                 admin.getThroughputFloor(resourceId),
                 multiInterval, multiCumulative);

            for (ViewListener<PerfEvent> listener : listeners) {
                listener.newInfo(resourceId, event);
            }
        }

        /*
         * Send information to the .stat file by logging it to the statsLogger.
         */
        final EnvStats envStats = statsPacket.getEnvStats();
        if (envStats != null) {
            statsLogger.info(displayConciseStats(resourceId,
                                                 statsPacket.getStart(),
                                                 statsPacket.getEnd(),
                                                 envStats));
        }

        if (repStats != null) {
            statsLogger.info(displayConciseStats(resourceId,
                                                 statsPacket.getStart(),
                                                 statsPacket.getEnd(),
                                                 repStats));
        }

        final List<ConciseStats> otherStats = statsPacket.getOtherStats();
        if (otherStats != null) {
            for (ConciseStats stats : otherStats) {
                statsLogger.info(displayConciseStats(resourceId,
                                                     statsPacket.getStart(),
                                                     statsPacket.getEnd(),
                                                     stats));
            }
        }

        /* Log additional stats associated with the packet itself */
        statsLogger.info(displayConciseStats(resourceId,
                                             statsPacket.getStart(),
                                             statsPacket.getEnd(),
                                             statsPacket));
    }

    public synchronized void addListener(ViewListener<PerfEvent> l) {
        listeners.add(l);
    }

    public synchronized
        void removeListener(ViewListener<PerfEvent> l) {
        listeners.remove(l);
    }

    @Override
    public void close() {
        /* Nothing to do. */
    }

    private String displayConciseStats(ResourceId resourceId,
                                       long start,
                                       long end,
                                       ConciseStats stats) {
        final StringBuilder sb = new StringBuilder();
        sb.append(resourceId + " (" +
                  FormatUtils.formatTimeMillis(start) +  " -> " +
                  FormatUtils.formatTimeMillis(end) + ")\n");
        sb.append(stats.getFormattedStats());
        return sb.toString();
    }
}
