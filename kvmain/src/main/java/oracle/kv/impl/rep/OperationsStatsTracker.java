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

package oracle.kv.impl.rep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;
import oracle.kv.impl.measurement.EnvStats;
import oracle.kv.impl.measurement.JVMStats;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LoggingStats;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.RepEnvStats;
import oracle.kv.impl.measurement.ReplicationState;
import oracle.kv.impl.measurement.StatType;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.LatencyTracker;
import oracle.kv.impl.util.LoggingStatsTracker;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.ScheduleStart;
import oracle.kv.impl.util.WatcherNames;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.nosql.common.sklogger.measure.LatencyElement;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.utilint.CommonLoggerUtils;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;

/**
 * Stats pertaining to a single replication node.
 */
public class OperationsStatsTracker {

    private static final int WAIT_FOR_HANDLE = 100;

    /**
     * Operation types for latency tracking.
     */
    private static final StatType[] LATENCY_TRACK_ENUMS =
        Stream.of(OpCode.values(),
                  PerfStatType.getSummaryStats(),
                  PerfStatType.getCumulativeSummaryStats())
        .flatMap(Arrays::stream)
        .toArray(StatType[]::new);

    static final String COLLECTOR_THREAD_NAME_SUFFIX = "_MonitorAgentCollector";

    private final AgentRepository monitorBuffer;
    private final ScheduledExecutorService collector;
    private Future<?> collectorFuture;
    protected List<Listener> listeners = new ArrayList<Listener>();

    /*
     * The actual tracker. This is a member variable that can be replaced
     * if/when new parameters are set.
     */
    private volatile SummarizingLatencyTracker latencyTracker;

    /* Timestamp for the start of all operation tracking. */
    private long trackingStart;

    /* Timestamp for the end of the last collection period. */
    private long lastEnd;
    /* End of log at the time environment stats were last collected. */
    private long lastEndOfLog = 0;
    /* Configuration used to collect stats. */
    private final StatsConfig config = new StatsConfig().setClear(true);

    private final Logger logger;
    private final RepNodeService repNodeService;

    private ParameterMap rnParamsMap;
    private ParameterMap globalParamsMap;
    private GlobalParamsListener globalParamsListener;
    private RNParamsListener rnParamsListener;
    /**
     * Log no more than 1 threshold alert every 5 minutes.
     */
    private static final int LOG_SAMPLE_PERIOD_MS = 5 * 60 * 1000;

    /**
     * The max number of types of threshold alerts that will be logged;
     * for example, 'single op interval latency above ceiling',
     * 'multi-op interval throughput below floor', etc.
     */
    private static final int MAX_LOG_TYPES = 5;

    /**
     * Encapsulates the logger used by this class. When this logger is used,
     * for each type of PerfEvent, the rate at which this logger writes
     * records corresponding to the given type will be bounded; to prevent
     * overwhelming the store's log file.
     */
    private final RateLimitingLogger<String> eventLogger;

    /** Maintain the history needed to track JVM stats. */
    private final JVMStats.Tracker jvmStatsTracker = new JVMStats.Tracker();

    /**
     */
    public OperationsStatsTracker(RepNodeService repNodeService,
                                  ParameterMap rnParamsMap,
                                  ParameterMap globalParamsMap,
                                  AgentRepository monitorBuffer) {

        this.repNodeService = repNodeService;
        this.monitorBuffer = monitorBuffer;
        RepNodeService.Params params = repNodeService.getParams();
        this.logger =
            LoggerUtils.getLogger(OperationsStatsTracker.class, params);
        this.eventLogger = new RateLimitingLogger<String>
            (LOG_SAMPLE_PERIOD_MS, MAX_LOG_TYPES, logger);
        ThreadFactory factory = new CollectorThreadFactory
            (logger, params.getRepNodeParams().getRepNodeId());
        collector = new ScheduledThreadPoolExecutor(1, factory);
        globalParamsListener = new GlobalParamsListener();
        rnParamsListener = new RNParamsListener();
        initialize(rnParamsMap, globalParamsMap);
    }

    /**
     * For unit test only, effectively a no-op stats tracker, to disable stats
     * tracking.
     */
    public OperationsStatsTracker() {
        latencyTracker = new SummarizingLatencyTracker(null, 0, 0, 0);
        monitorBuffer = null;
        collector = null;
        collectorFuture = null;
        logger = null;
        eventLogger = null;
        repNodeService = null;
        trackingStart = 0;
    }

    /**
     * Used for initialization during constructions and from newParameters()
     * NOTE: newParameters() results in loss of cumulative stats and reset of
     * trackingStart.
     */
    private void initialize(ParameterMap newRNParamsMap,
                            ParameterMap newGlobalParamsMap) {
        if (newRNParamsMap != null) {
            rnParamsMap = newRNParamsMap.copy();
        }
        if (newGlobalParamsMap != null) {
            globalParamsMap = newGlobalParamsMap.copy();
        }
        if (collectorFuture != null) {
            logger.info("Cancelling current operationStatsCollector");
            collectorFuture.cancel(true);
        }

        latencyTracker = new SummarizingLatencyTracker
            (logger,
             rnParamsMap.get(ParameterState.SP_ACTIVE_THRESHOLD).asInt(),
             ParameterUtils.getThreadDumpIntervalMillis(rnParamsMap),
             rnParamsMap.get(ParameterState.SP_THREAD_DUMP_MAX).asInt());

        DurationParameter dp =
            (DurationParameter) globalParamsMap.getOrDefault(
                ParameterState.GP_COLLECTOR_INTERVAL);
        collectorFuture = ScheduleStart.scheduleAtFixedRate(collector,
                                                            dp,
                                                            new CollectStats(),
                                                            logger);
        lastEnd = System.currentTimeMillis();
        trackingStart = lastEnd;
    }

    public SummarizingLatencyTracker getLatencyTracker() {
        return latencyTracker;
    }

    synchronized public void newRNParameters(ParameterMap oldMap,
                                             ParameterMap newMap) {

        /*
         * Caller ensures that the maps are different, check for
         * differences that matter to this class.  Re-init if *any* of the
         * parameters are different.
         */
        if (paramsDiffer(oldMap, newMap, ParameterState.SP_THREAD_DUMP_MAX) ||
            paramsDiffer(oldMap, newMap, ParameterState.SP_ACTIVE_THRESHOLD) ||
            paramsDiffer(oldMap, newMap, ParameterState.SP_DUMP_INTERVAL)) {
            initialize(newMap, null /* newGlobalParamsMap */);
        }
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
     * Invoked by the async collection job and at rep node close.
     */
    synchronized public void pushStats() {

        logger.fine("Collecting latency stats");
        long useStart = lastEnd;
        long useEnd = System.currentTimeMillis();

        final StatsPacket packet =
            new StatsPacket(useStart, useEnd,
                            repNodeService.getRepNodeId().getFullName(),
                            repNodeService.getRepNodeId().getGroupName());

        /*
         * Gather up interval and cumulative stats. Note to use trackingStart
         * for cumulative stats so that throughputs are computed correct.
         */
        latencyTracker.obtain(WatcherNames.SERVER_STATS_TRACKER,
                              StatType::isInterval)
            .forEach((key, result) ->
                     packet.add(
                         new LatencyInfo(
                             getPerfStatTypeFromKey(key),
                             key.isInterval() ? useStart : trackingStart,
                             useEnd, result)));

        /* Get the table throughput and size stats */
        packet.set
            (repNodeService.getRepNode().getTableManager().getTableInfo());

        packet.add
            (new ReplicationState(useStart, useEnd, getReplicationState()));

        if (repNodeService.getParams().getRepNodeParams().
            getCollectEnvStats()) {
            ReplicatedEnvironment repEnv =
                repNodeService.getRepNode().getEnv(WAIT_FOR_HANDLE);

            /*
             * Check if the env is open; this method may be called after the
             * repNodeService has stopped.
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

        if (repNodeService.getParams().getRepNodeParams().
            getCollectEndpointGroupStats()) {
            final EndpointGroup endpointGroup =
                AsyncRegistryUtils.getEndpointGroupOrNull();
            if (endpointGroup != null) {
                if (endpointGroup instanceof NioEndpointGroup) {
                    final NioEndpointGroup egrp =
                        (NioEndpointGroup) endpointGroup;
                    egrp.obtainDialogEndpointGroupPerf(
                        WatcherNames.SERVER_STATS_TRACKER, true)
                        .map((p) -> p.getConciseStats())
                        .ifPresent((s) -> packet.add(s));
                    egrp.obtainChannelThreadPoolPerf(
                        WatcherNames.SERVER_STATS_TRACKER, true)
                        .map((p) -> p.getConciseStats())
                        .ifPresent((s) -> packet.add(s));
                } else {
                    endpointGroup
                        .getDialogEndpointGroupPerfTracker()
                        .obtain(WatcherNames.SERVER_STATS_TRACKER, true)
                        .map((p) -> p.getConciseStats())
                        .ifPresent((s) -> packet.add(s));
                }
            }
        }

        final LoggingStats loggingStats = LoggingStatsTracker.getStats(
            repNodeService.getRepNodeId(),
            repNodeService.getParams().getGlobalParams().getKVStoreName(),
            useStart, useEnd);
        if (loggingStats != null) {
            packet.add(loggingStats);
        }

        RequestHandlerImpl reqHandler = repNodeService.getReqHandler();
        Map<String, AtomicInteger> exceptionStat =
            reqHandler.getAndResetExceptionCounts();
        for(Entry<String, AtomicInteger> entry : exceptionStat.entrySet()) {
            packet.add(entry.getKey(), entry.getValue().get());
        }

        packet.setActiveRequests(reqHandler.getActiveRequests());
        packet.setTotalRequests(reqHandler.getAndResetTotalRequests());
        packet.setAverageQueuedAsyncRequests(
            reqHandler.getAverageQueuedAsyncRequests());
        packet.setMaxQueuedAsyncRequests(
            reqHandler.getAndResetMaxQueuedAsyncRequests());
        final LatencyElement.Result asyncRequestTimeStatsNanos =
            reqHandler.getAsyncRequestQueueTimeStatsNanos(
                WatcherNames.SERVER_STATS_TRACKER);
        packet.setAsyncRequestQueueTimeAverageNanos(
            asyncRequestTimeStatsNanos.getAverage());
        packet.setAsyncRequestQueueTime95thNanos(
            asyncRequestTimeStatsNanos.getPercent95());
        packet.setAsyncRequestQueueTime99thNanos(
            asyncRequestTimeStatsNanos.getPercent99());
        lastEnd = useEnd;

        logThresholdAlerts(Level.WARNING, packet);

        monitorBuffer.add(packet);
        sendPacket(packet);
        logger.log(Level.FINE, () -> packet.toString());
    }

    private PerfStatType getPerfStatTypeFromKey(StatType key) {
        if (key instanceof PerfStatType) {
            return (PerfStatType) key;
        }
        if (key instanceof OpCode) {
            final OpCode opCode = (OpCode) key;
            return opCode.isInterval() ?
                opCode.getIntervalMetric() : opCode.getCumulativeMetric();
        }
        throw new IllegalStateException(String.format(
            "Unexpected enum key for latency map: %s", key));
    }

    private State getReplicationState() {
        State state = State.UNKNOWN;
        try {
            final ReplicatedEnvironment env =
                repNodeService.getRepNode().getEnv(WAIT_FOR_HANDLE);
            if (env != null) {
                try {
                    state = env.getState();
                } catch (IllegalStateException ise) {
                    /* State cannot be queried if detached. */
                    state = State.DETACHED;
                }
            }
        } catch (EnvironmentFailureException ignored) /* CHECKSTYLE_OFF */ {
            /* The environment is invalid. */
        } /* CHECKSTYLE_ON */
        return state;
    }

    /**
     * Simple Runnable to send latency stats to the service's monitor agent
     */
    private class CollectStats implements Runnable {

        @Override
        public void run() {
            try {
                pushStats();
            } catch(Exception e) {
                /*
                 * Capture any exceptions thrown by the stats task and prevent
                 * it from killing the job; we want to ensure that stats
                 * collection keeps going on.
                 */
                eventLogger.log(
                    "push stats error",
                    Level.WARNING,
                    () -> String.format(
                        "push RN operation stats error: %s",
                        CommonLoggerUtils.getStackTrace(e)));
            }
        }
    }

    /**
     * Collector threads are named KVAgentMonitorCollector and log uncaught
     * exceptions to the monitor logger.
     */
    private class CollectorThreadFactory extends KVThreadFactory {
        private final RepNodeId repNodeId;

        CollectorThreadFactory(Logger logger, RepNodeId repNodeId) {
            super(null, logger);
            this.repNodeId = repNodeId;
        }

        @Override
        public String getName() {
            return  repNodeId + COLLECTOR_THREAD_NAME_SUFFIX;
        }
    }

    /**
     * Tracks the per-op-type, single-op and multi-op summary stats.
     *
     * <p>Instead of using the roll up mechanism, we directly collect the
     * summary stats to preserve the 95th/99th stats.
     *
     * <p>NOP ops are excluded.
     */
    public static class SummarizingLatencyTracker
        extends LatencyTracker<StatType> {

        public SummarizingLatencyTracker(Logger stackTraceLogger,
                                         int activeThreadThreshold,
                                         long threadDumpIntervalMillis,
                                         int threadDumpMax) {
            super(LATENCY_TRACK_ENUMS, stackTraceLogger, activeThreadThreshold,
                  threadDumpIntervalMillis, threadDumpMax);
        }

        /**
         * Note that markFinish may be called with a null op type.
         */
        @Override
        public void markFinish(StatType type, long startTime, int numRecords) {
            if (type == null) {
                super.markFinish(type, startTime, numRecords);
                return;
            }

            if (!(type instanceof OpCode)) {
                throw new IllegalStateException(
                    String.format("Expected only OpCode, but got %s", type));
            }
            final OpCode opType = (OpCode) type;
            super.markFinish(opType, startTime, numRecords);

            if (opType.equals(OpCode.NOP)) {
                /* No need to update parent types for NOP. */
                return;
            }

            /*
             * For all the parent types of this operation, update the latency
             * as well. These updates are not paired with a markStart.
             */
            PerfStatType ptype;
            ptype = opType.getIntervalMetric();
            while (ptype != null) {
                super.markFinish(ptype, startTime, numRecords, false);
                ptype = ptype.getParent();
            }
            ptype = opType.getCumulativeMetric();
            while (ptype != null) {
                super.markFinish(ptype, startTime, numRecords, false);
                ptype = ptype.getParent();
            }
        }
    }

    /**
     * An OperationsStatsTracker.Listener can be implemented by clients of this
     * interface to recieve stats when they are collected.
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

    private void logThresholdAlerts(Level level, StatsPacket packet) {

        /* Better to not log at all than risk a possible NPE. */
        if (logger == null || eventLogger == null ||
            repNodeService == null || level == null || packet == null) {
            return;
        }
        if (!logger.isLoggable(level)) {
            return;
        }

        final RepNodeParams params = repNodeService.getRepNodeParams();
        if (params == null) {
            return;
        }

        final int ceiling = params.getLatencyCeiling();
        final int floor = params.getThroughputFloor();

        /* For single operation within a given time interval */
        final LatencyInfo singleOpIntervalLatencyInfo =
                        packet.get(PerfStatType.USER_SINGLE_OP_INT);

        if (PerfEvent.latencyCeilingExceeded(
                          ceiling, singleOpIntervalLatencyInfo)) {
            eventLogger.log("single-op-interval-latency", level,
                            "single op interval latency above ceiling [" +
                            ceiling + "]");
        }

        if (PerfEvent.throughputFloorExceeded(
                          floor, singleOpIntervalLatencyInfo)) {
            eventLogger.log("single-op-interval-throughput",
                            level, "single op interval throughput below " +
                            "floor [" + floor + "]");
        }

        /* For multiple operations within a given time interval */
        final LatencyInfo multiOpIntervalLatencyInfo =
            packet.get(PerfStatType.USER_MULTI_OP_INT);

        if (PerfEvent.latencyCeilingExceeded(
                          ceiling, multiOpIntervalLatencyInfo)) {
            eventLogger.log("multi-op-interval-latency", level,
                            "multi-op interval latency above ceiling [" +
                            ceiling + "]");
        }

        if (PerfEvent.throughputFloorExceeded(
                          floor, multiOpIntervalLatencyInfo)) {
            eventLogger.log("multi-op-interval-throughput", level,
                            "multi-op interval throughput below floor [" +
                            floor + "]");
        }
    }

    public ParameterListener getGlobalParamsListener() {
        return globalParamsListener;
    }

    public ParameterListener getRNParamsListener() {
        return rnParamsListener;
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
     * RepNode parameter change listener.
     */
    private class RNParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            newRNParameters(oldMap, newMap);
        }
    }
}
