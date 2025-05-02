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

package oracle.kv.impl.sna;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.LoggingStats;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.LoggingStatsTracker;
import oracle.kv.impl.util.ScheduleStart;

/**
 * Collect stats specific to the storage node agent.
 */
public class SnaLoggingStatsTracker {

    private final StorageNodeAgent storageNodeAgent;
    private final ScheduledExecutorService collector;
    private final List<Listener> listeners = new ArrayList<>();

    private volatile Future<?> collectorFuture;

    /** Timestamp for the end of the last collection period. */
    private volatile long lastEndMs = System.currentTimeMillis();

    private volatile Logger logger;

    private volatile ParameterMap globalParamsMap;

    public SnaLoggingStatsTracker(StorageNodeAgent storageNodeAgent,
                                  Logger logger) {
        this.storageNodeAgent = storageNodeAgent;
        this.logger = logger;
        final ThreadFactory factory =
            new CollectorThreadFactory(logger,
                                       storageNodeAgent.getStorageNodeId());
        collector = new ScheduledThreadPoolExecutor(1, factory);
    }

    /**
     * Used for initialization during constructions and from newParameters()
     */
    void initialize(ParameterMap newGlobalParamsMap) {
        if (newGlobalParamsMap != null) {
            globalParamsMap = newGlobalParamsMap.copy();
        }
        if (collectorFuture != null) {
            logger.info("Cancelling current SnaLoggingStatsTracker collector");
            collectorFuture.cancel(true);
        }

        final DurationParameter dp = (DurationParameter)
            globalParamsMap.getOrDefault(ParameterState.GP_COLLECTOR_INTERVAL);
        logger.fine(() ->
                    "Scheduling SnaLoggingStatsTracker collector for " + dp);
        collectorFuture = ScheduleStart.scheduleAtFixedRate(
            collector, dp, new CollectStats(), logger);
        lastEndMs = System.currentTimeMillis();
    }

    public synchronized void newGlobalParameters(ParameterMap oldMap,
                                                 ParameterMap newMap) {

        /*
         * Caller ensures that the maps are different, check for
         * differences that matter to this class.  Re-init if *any* of the
         * parameters are different.
         */
        if ((oldMap == null) ||
            paramsDiffer(oldMap, newMap,
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
        return !map1.get(param).equals(map2.get(param));
    }

    public synchronized void setLogger(Logger logger) {
        this.logger = logger;
    }

    public void close() {
        pushStats();
        collector.shutdown();
        if (collectorFuture != null) {
            collectorFuture.cancel(true);
        }
    }

    /**
     * Invoked by the async collection job and at SNA close.
     */
    public synchronized void pushStats() {
        logger.fine("Collecting SNA logging stats");

        final long useStartMs = lastEndMs;
        final long useEndMs = System.currentTimeMillis();

        final LoggingStats loggingStats = LoggingStatsTracker.getStats(
            storageNodeAgent.getStorageNodeId(),
            storageNodeAgent.getStoreName(),
            useStartMs, useEndMs);
        if (loggingStats != null) {
            final StatsPacket packet = new StatsPacket(
                useStartMs, useEndMs,
                storageNodeAgent.getStorageNodeId().getFullName(),
                "0" /* stand-in value for shard name */);
            packet.add(loggingStats);
            lastEndMs = useEndMs;
            sendPacket(packet);
            logger.fine(() -> loggingStats.toString());
        }
    }

    /**
     * Simple Runnable to stats to the service's monitor agent
     */
    private class CollectStats implements Runnable {

        @Override
        public void run() {
            try {
                pushStats();
            } catch (Exception e) {
                /*
                 * Capture any exceptions thrown by the stats task and prevent
                 * it from killing the job; we want to ensure that stats
                 * collection keeps going on.
                 */
                logger.severe("Push SNA stats error: " + e);
            }
        }
    }

    /**
     * Collector threads are named KVAgentMonitorCollector and log uncaught
     * exceptions to the monitor logger.
     */
    private class CollectorThreadFactory extends KVThreadFactory {
        private final StorageNodeId snId;

        CollectorThreadFactory(Logger logger, StorageNodeId snId) {
            super(null, logger);
            this.snId = snId;
        }

        @Override
        public String getName() {
            return snId + "_MonitorAgentCollector";
        }
    }

    /**
     * An SNAStatsTracker.Listener can be implemented by clients of this
     * interface to receive stats when they are collected.
     */
    public interface Listener {
        void receiveStats(StatsPacket packet);
    }

    public void addListener(Listener lst) {
        listeners.add(lst);
    }

    public void addListener(AgentRepository agentRepository) {
        listeners.add(p -> {
                logger.fine(
                    () -> "Logging SNA stats to agent repository: " + p);
                agentRepository.add(p);
            });
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
        return this::newGlobalParameters;
    }
}
