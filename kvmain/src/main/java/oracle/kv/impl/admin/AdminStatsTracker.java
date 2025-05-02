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

package oracle.kv.impl.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.EnvStats;
import oracle.kv.impl.measurement.JVMStats;
import oracle.kv.impl.measurement.RepEnvStats;
import oracle.kv.impl.measurement.LoggingStats;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.ScheduleStart;
import oracle.kv.impl.util.LoggingStatsTracker;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;

/**
 * Statistics pertaining to an Admin node.
 */
public class AdminStatsTracker {

    private final ScheduledExecutorService collector;
    private Future<?> collectorFuture;
    protected List<Listener> listeners = new ArrayList<Listener>();

    /* Timestamp for the end of the last collection period. */
    private long lastEnd;

    private final Logger logger;
    private final Admin admin;

    @SuppressWarnings("unused")
    private ParameterMap adminParamsMap;
    private ParameterMap globalParamsMap;
    private GlobalParamsListener globalParamsListener;
    private final JVMStats.Tracker jvmStatsTracker = new JVMStats.Tracker();
    /* Configuration used to collect stats. */
    private final StatsConfig config = new StatsConfig().setClear(true);
    /* End of log at the time environment stats were last collected. */
    private long lastEndOfLog = 0;

    /**
     */
    public AdminStatsTracker(Admin admin,
                             ParameterMap globalParamsMap) {

        this.admin = admin;
        AdminServiceParams params = admin.getParams();
        this.logger =
            LoggerUtils.getLogger(AdminStatsTracker.class, params);
        ThreadFactory factory = new CollectorThreadFactory
            (logger, params.getAdminParams().getAdminId());
        collector = new ScheduledThreadPoolExecutor(1, factory);
        globalParamsListener = new GlobalParamsListener();
        initialize(globalParamsMap);
    }

    private void initialize(ParameterMap newGlobalParamsMap) {
        if (newGlobalParamsMap != null) {
            globalParamsMap = newGlobalParamsMap.copy();
        }
        if (collectorFuture != null) {
            logger.info("Cancelling current AdminStatsCollector");
            collectorFuture.cancel(true);
        }

        DurationParameter dp =
            (DurationParameter) globalParamsMap.getOrDefault(
                ParameterState.GP_COLLECTOR_INTERVAL);
        collectorFuture = ScheduleStart.scheduleAtFixedRate(collector,
                                                            dp,
                                                            new CollectStats(),
                                                            logger);
        lastEnd = System.currentTimeMillis();
    }

    synchronized public void newGlobalParameters(ParameterMap oldMap,
                                                 ParameterMap newMap) {

        if (paramsDiffer(oldMap, newMap,
                         ParameterState.GP_COLLECTOR_INTERVAL)) {
            initialize(newMap);
        }
    }

    /**
     * @return true if map1 and map2 have different param values.
     */
    private boolean paramsDiffer(ParameterMap map1,
                                 ParameterMap map2,
                                 String param) {
        return (!(map1.get(param).equals(map2.get(param))));
    }

    public void close() {
        collector.shutdown();
        collectorFuture.cancel(true);
        jvmStatsTracker.shutdown();
    }

    /**
     * Invoked by the asynchronous collection job and at admin node close.
     */
    synchronized public void pushStats() {

        logger.fine("Collecting admin stats");
        long useStart = lastEnd;
        long useEnd = System.currentTimeMillis();
        AdminId id = admin.getParams().getAdminParams().getAdminId();
        StatsPacket packet =
            new StatsPacket(useStart, useEnd,
                            id.getFullName(),
                            "0" /* shard */);

        if (admin.getParams().getAdminParams().
            getCollectEnvStats()) {
            ReplicatedEnvironment repEnv =
                admin.getEnv();

            /*
             * Check if the env is open; this method may be called after the
             * AdminService has stopped.
             */
            if ((repEnv != null) && (repEnv.isValid())) {
                EnvironmentStats envStats = repEnv.getStats(config);

                /*
                 * Collect environment stats if there has been some app
                 * activity, or if there has been some env maintenance related
                 * write activity, independent of the app, say due to cleaning,
                 * checkpointing, replication, etc.
                 */
                if (envStats.getEndOfLog() != lastEndOfLog) {
                    packet.add(new EnvStats(useStart, useEnd, envStats));
                    ReplicatedEnvironmentStats repStats =
                        repEnv.getRepStats(config);
                    packet.add(new RepEnvStats(useStart, useEnd, repStats));
                    lastEndOfLog = envStats.getEndOfLog();
                }
            }
            packet.add(jvmStatsTracker.createStats(useStart, useEnd));
        }

        final LoggingStats loggingStats = LoggingStatsTracker.getStats(
            admin.getParams().getAdminParams().getAdminId(),
            admin.getParams().getGlobalParams().getKVStoreName(),
            useStart, useEnd);
        if (loggingStats != null) {
            packet.add(loggingStats);
        }

        lastEnd = useEnd;

        sendPacket(packet);
        logger.fine(packet.toString());
    }

    /**
     * Simple Runnable to send statistics to the service's monitor agent
     */
    private class CollectStats implements Runnable {

        @Override
        public void run() {
            pushStats();
        }
    }

    private class CollectorThreadFactory extends KVThreadFactory {
        private final AdminId adminId;

        CollectorThreadFactory(Logger logger, AdminId adminId) {
            super(null, logger);
            this.adminId = adminId;
        }

        @Override
        public String getName() {
            return  adminId + "_MonitorAgentCollector";
        }
    }

    /**
     * An AdminStatsTracker.Listener can be implemented by clients of this
     * interface to receive statistics when they are collected.
     */
    public interface Listener {
        void receiveStats(StatsPacket packet);
    }

    public void addListener(Listener lst) {
        listeners.add(lst);
    }

    public void removeListener(Listener lst) {
        listeners.remove(lst);
    }

    private void sendPacket(StatsPacket packet) {
        for (Listener lst : listeners) {
            lst.receiveStats(packet);
        }
    }

    public ParameterListener getGlobalParamsListener() {
        return globalParamsListener;
    }

    /**
     * Global parameter change listener.
     */
    private class GlobalParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            newGlobalParameters(oldMap, newMap);
        }
    }
}
