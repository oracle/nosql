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

package oracle.kv.impl.arb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.ArbiterNodeStats;
import oracle.kv.impl.measurement.JVMStats;
import oracle.kv.impl.measurement.LoggingStats;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.ScheduleStart;
import oracle.kv.impl.util.LoggingStatsTracker;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.arbiter.ArbiterStats;

/**
 * Stats pertaining to an arbiter node.
 */
public class ArbStatsTracker {

    private final AgentRepository monitorBuffer;
    private final ScheduledExecutorService collector;
    private Future<?> collectorFuture;
    protected List<Listener> listeners = new ArrayList<Listener>();

    /* Timestamp for the end of the last collection period. */
    private long lastEnd;

    private final Logger logger;
    private final ArbNodeService arbNodeService;

    @SuppressWarnings("unused")
    private ParameterMap arbParamsMap;
    private ParameterMap globalParamsMap;
    private GlobalParamsListener globalParamsListener;
    private ARBParamsListener arbParamsListener;
    private final JVMStats.Tracker jvmStatsTracker = new JVMStats.Tracker();

    /**
     */
    public ArbStatsTracker(ArbNodeService arbNodeService,
                           ParameterMap arbParamsMap,
                           ParameterMap globalParamsMap,
                           AgentRepository monitorBuffer) {

        this.arbNodeService = arbNodeService;
        this.monitorBuffer = monitorBuffer;
        ArbNodeService.Params params = arbNodeService.getParams();
        this.logger =
            LoggerUtils.getLogger(ArbStatsTracker.class, params);
        ThreadFactory factory = new CollectorThreadFactory
            (logger, params.getArbNodeParams().getArbNodeId());
        collector = new ScheduledThreadPoolExecutor(1, factory);
        globalParamsListener = new GlobalParamsListener();
        arbParamsListener = new ARBParamsListener();
        initialize(arbParamsMap, globalParamsMap);
    }

    /**
     * Used for initialization during constructions and from newParameters()
     * NOTE: newParameters() results in loss of cumulative stats and reset of
     * trackingStart.
     */
    private void initialize(ParameterMap newARBParamsMap,
                            ParameterMap newGlobalParamsMap) {
        if (newARBParamsMap != null) {
            arbParamsMap = newARBParamsMap.copy();
        }
        if (newGlobalParamsMap != null) {
            globalParamsMap = newGlobalParamsMap.copy();
        }
        if (collectorFuture != null) {
            logger.info("Cancelling current ArbStatsCollector");
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

    @SuppressWarnings("unused")
    synchronized public void newARBParameters(ParameterMap oldMap,
                                              ParameterMap newMap) {

        /*
         * Not currently using any arbiter parameters.
         */
    }

    synchronized public void newGlobalParameters(ParameterMap oldMap,
                                                 ParameterMap newMap) {

        /*
        * Caller ensures that the maps are different, check for
        * differences that matter to this class.  Re-init if *any* of the
        * parameters are different.
        */
        if (paramsDiffer(oldMap, newMap,
                         ParameterState.GP_COLLECTOR_INTERVAL)) {
            initialize(null /* newRNParamsMap */, newMap);
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
     * Invoked by the async collection job and at arb node close.
     */
    synchronized public void pushStats() {

        logger.fine("Collecting arbiter stats");
        long useStart = lastEnd;
        long useEnd = System.currentTimeMillis();

        StatsPacket packet =
            new StatsPacket(useStart, useEnd,
                            arbNodeService.getArbNodeId().getFullName(),
                            arbNodeService.getArbNodeId().getGroupName());

        if (arbNodeService.getParams().getArbNodeParams().
            getCollectEnvStats()) {
            ArbNode an = arbNodeService.getArbNode();
            if (an != null) {
                ArbiterStats anStats =
                    an.getStats(true);
                if (anStats != null) {
                    packet.add(new ArbiterNodeStats(useStart, useEnd, anStats));
                }
            }

            packet.add(jvmStatsTracker.createStats(useStart, useEnd));
        }

        final LoggingStats loggingStats = LoggingStatsTracker.getStats(
            arbNodeService.getArbNodeId(),
            arbNodeService.getParams().getGlobalParams().getKVStoreName(),
            useStart, useEnd);
        if (loggingStats != null) {
            packet.add(loggingStats);
        }

        lastEnd = useEnd;

        monitorBuffer.add(packet);
        sendPacket(packet);
        logger.fine(packet.toString());
    }

    /**
     * Simple Runnable to send stats to the service's monitor agent
     */
    private class CollectStats implements Runnable {

        @Override
        public void run() {
            pushStats();
        }
    }

    /**
     * Collector threads are named KVAgentMonitorCollector and log uncaught
     * exceptions to the monitor logger.
     */
    private class CollectorThreadFactory extends KVThreadFactory {
        private final ArbNodeId arbNodeId;

        CollectorThreadFactory(Logger logger, ArbNodeId arbNodeId) {
            super(null, logger);
            this.arbNodeId = arbNodeId;
        }

        @Override
        public String getName() {
            return  arbNodeId + "_MonitorAgentCollector";
        }
    }

    /**
     * An ArbStatsTracker.Listener can be implemented by clients of this
     * interface to receive stats when they are collected.
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

    public ParameterListener getARBParamsListener() {
        return arbParamsListener;
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

    /**
     * ArbNode parameter change listener.
     */
    private class ARBParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            newARBParameters(oldMap, newMap);
        }
    }
}
