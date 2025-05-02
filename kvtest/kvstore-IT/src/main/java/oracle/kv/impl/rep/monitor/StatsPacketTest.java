/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.monitor;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import oracle.kv.TestBase;
import oracle.kv.impl.measurement.EnvStats;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.RepEnvStats;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;

import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.log.LogStatDefinition;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition;
import com.sleepycat.je.rep.impl.node.ReplayStatDefinition;
import com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition;
import com.sleepycat.je.utilint.AtomicLongMapStat;
import com.sleepycat.je.utilint.IntegralLongAvg;
import com.sleepycat.je.utilint.IntegralLongAvgStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.MapStat;
import com.sleepycat.je.utilint.Stat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;

import org.junit.Test;



public class StatsPacketTest  extends TestBase {

    private static final long nReads = 10L;
    private static final long nWrites = 20L;
    private static final long nOps = 10000L;
    private static final int activeRequests = 30;
    private static final int totalRequests = 40000;
    private static final int avgQueuedAsyncRequests = 50;
    private static final int maxQueuedAsyncRequests = 60;
    private static final int asyncRequestQueueTimeAvgMicros = 70;
    private static final int asyncRequestQueueTime95thMicros = 80;
    private static final int asyncRequestQueueTime99thMicros = 90;

    private long start;
    private long end;
    private String nodeId;
    private String rgId;
    private StatsPacket statsPacket;

    private List<LatencyInfo> latencies;
    private EnvStats envStats;
    private RepEnvStats kvRepEnvStats;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        start = 10000L;
        end = 20000L;
        nodeId = "rg1-rn1";
        rgId = "rg1";
        latencies = new ArrayList<LatencyInfo>();
        statsPacket = makeStatsPacket();
    }

    @Test
    public void testToOpJsonString() {
        String opStr = statsPacket.toOpJsonString();
        JsonNode opNode = JsonUtils.parseJsonNode(opStr);
        assertEquals(nodeId, opNode.get("resource").asText());
        assertEquals(rgId, opNode.get("shard").asText());
        assertEquals(end, opNode.get("reportTime").asLong());
        for (LatencyInfo latency : latencies) {
            final String typeName =
                PerfStatType.getType(latency.getPerfStatId()).toString();
            assertEquals(latency.getLatency().getOperationCount(),
                         opNode.get(typeName + "_TotalOps").asLong());
            assertEquals(latency.getLatency().getRequestCount(),
                         opNode.get(typeName + "_TotalReq").asLong());
            assertEquals(latency.getThroughputPerSec(),
                         opNode.get(typeName + "_PerSec").asLong());
            assertEquals(
                NANOSECONDS.toMillis(latency.getLatency().getMin()),
                opNode.get(typeName + "_Min").asLong());
            assertEquals(
                NANOSECONDS.toMillis(latency.getLatency().getMax()),
                opNode.get(typeName + "_Max").asLong());
            assertEquals(
                NANOSECONDS.toMillis(latency.getLatency().getAverage()),
                opNode.get(typeName + "_Avg").asDouble(),
                0.0001f);
            assertEquals(
                NANOSECONDS.toMillis(latency.getLatency().getPercent95()),
                opNode.get(typeName + "_95th").asLong());
            assertEquals(
                NANOSECONDS.toMillis(latency.getLatency().getPercent99()),
                opNode.get(typeName + "_99th").asLong());
        }
        assertEquals(activeRequests,
                     opNode.get("Active_Requests").asLong());
        assertEquals(totalRequests,
                     opNode.get("Total_Requests").asLong());
        assertEquals(avgQueuedAsyncRequests,
                     opNode.get("Avg_Queued_Async_Requests").asLong());
        assertEquals(maxQueuedAsyncRequests,
                     opNode.get("Max_Queued_Async_Requests").asLong());
        assertEquals(asyncRequestQueueTimeAvgMicros,
                     opNode.get(
                         "Async_Request_Queue_Time_Avg_Micros").asLong());
        assertEquals(asyncRequestQueueTime95thMicros,
                     opNode.get(
                         "Async_Request_Queue_Time_95th_Micros").asLong());
        assertEquals(asyncRequestQueueTime99thMicros,
                     opNode.get(
                         "Async_Request_Queue_Time_99th_Micros").asLong());
    }

    @Test
    public void testToEnvJsonString() {
        String envStr = statsPacket.toEnvJsonString();
        JsonNode envNode = JsonUtils.parseJsonNode(envStr);
        assertEquals(nodeId, envNode.get("resource").asText());
        assertEquals(rgId, envNode.get("shard").asText());
        assertEquals(end, envNode.get("reportTime").asLong());
        for (StatGroup sg : envStats.getStats().getStatGroups()) {
            if (sg == null) {
                continue;
            }
            for (Map.Entry<StatDefinition, Stat<?>> e :
                     sg.getStats().entrySet()) {

                String name = sg.getName() + "_" + e.getKey().getName();
                Object val = e.getValue().get();
                assertEquals(((Number) val).longValue(),
                             envNode.get(name).asLong());
            }
        }
        for (StatGroup sg : kvRepEnvStats.getStats().getStatGroups()) {
            if (sg == null) {
                continue;
            }
            for (Map.Entry<StatDefinition, Stat<?>> e :
                     sg.getStats().entrySet()) {

                String name = sg.getName() + "_" + e.getKey().getName();
                Stat<?> stat = e.getValue();
                if (stat instanceof MapStat) {
                    Map<?, ?> statMap = ((MapStat<?, ?>) stat).getMap();
                    Map<?, ?> envMap =
                        JsonUtils.fromJson(envNode.get(name).toString(),
                                           Map.class);
                    assertEquals(statMap.size(), envMap.size());
                    for (Object key : statMap.keySet()) {
                        assertEquals(
                            String.valueOf(key),
                            ((Number) statMap.get(key)).longValue(),
                            ((Number) envMap.get(key)).longValue());
                    }
                } else {
                    Object val = stat.get();
                    assertEquals(((Number) val).longValue(),
                                 envNode.get(name).asLong());
                }
            }
        }
    }

    private StatsPacket makeStatsPacket() {

        /* For constructing the latency info components contained in the
         * expected sub-string.
         */
        StatsPacket packet = new StatsPacket(start, end, nodeId, rgId);

        packet.setActiveRequests(activeRequests);
        packet.setTotalRequests(totalRequests);
        packet.setAverageQueuedAsyncRequests(avgQueuedAsyncRequests);
        packet.setMaxQueuedAsyncRequests(maxQueuedAsyncRequests);
        packet.setAsyncRequestQueueTimeAverageNanos(
            MICROSECONDS.toNanos(asyncRequestQueueTimeAvgMicros));
        packet.setAsyncRequestQueueTime95thNanos(
            MICROSECONDS.toNanos(asyncRequestQueueTime95thMicros));
        packet.setAsyncRequestQueueTime99thNanos(
            MICROSECONDS.toNanos(asyncRequestQueueTime99thMicros));

        final long putIntTotalOps=400_000_000_000L;
        final long putIntTotalReq=500_000_000_000L;
        final long putIntMin = MILLISECONDS.toNanos(1);
        final long putIntMax = MILLISECONDS.toNanos(10);
        final long putIntAvg = MILLISECONDS.toNanos(5);
        final long putInt95Th = MILLISECONDS.toNanos(5);
        final long putInt99Th = MILLISECONDS.toNanos(9);
        final long putIntOverflow =36;

        final long putCumTotalOps = 600_000_000_000L;
        final long putCumTotalReq = 700_000_000_000L;
        final long putCumMin = MILLISECONDS.toNanos(1);
        final long putCumMax = MILLISECONDS.toNanos(20);
        final long putCumAvg = MILLISECONDS.toNanos(10);
        final long putCum95Th = MILLISECONDS.toNanos(15);
        final long putCum99Th = MILLISECONDS.toNanos(19);
        final long putCumOverflow = 76;

        final long singleIntTotalOps = 1_000_000_000_000L;
        final long singleIntTotalReq = 2_000_000_000_000L;
        final long singleIntMin = MILLISECONDS.toNanos(1);
        final long singleIntMax = MILLISECONDS.toNanos(100);
        final long singleIntAvg = MILLISECONDS.toNanos(10);
        final long singleInt95Th = MILLISECONDS.toNanos(95);
        final long singleInt99Th = MILLISECONDS.toNanos(99);
        final long singleIntOverflow = 136;

        final long singleCumTotalOps = 3_000_000_000_000L;
        final long singleCumTotalReq = 4_000_000_000_000L;
        final long singleCumMin = MILLISECONDS.toNanos(2);
        final long singleCumMax = MILLISECONDS.toNanos(200);
        final long singleCumAvg = MILLISECONDS.toNanos(20);
        final long singleCum95Th = MILLISECONDS.toNanos(195);
        final long singleCum99Th = MILLISECONDS.toNanos(199);
        final long singleCumOverflow = 236;

        final long multiIntTotalOps = 5_000_000_000_000L;
        final long multiIntTotalReq = 6_000_000_000_000L;
        final long multiIntMin = MILLISECONDS.toNanos(1);
        final long multiIntMax = MILLISECONDS.toNanos(200);
        final long multiIntAvg = MILLISECONDS.toNanos(10);
        final long multiInt95Th = MILLISECONDS.toNanos(195);
        final long multiInt99Th = MILLISECONDS.toNanos(199);
        final long multiIntOverflow =336;

        final long multiCumTotalOps = 7_000_000_000_000L;
        final long multiCumTotalReq = 8_000_000_000_000L;
        final long multiCumMin = MILLISECONDS.toNanos(2);
        final long multiCumMax = MILLISECONDS.toNanos(200);
        final long multiCumAvg = MILLISECONDS.toNanos(50);
        final long multiCum95Th = MILLISECONDS.toNanos(195);
        final long multiCum99Th = MILLISECONDS.toNanos(199);
        final long multiCumOverflow = 436;

        /* Add put interval non-commulative latency info to the packet. */
        LatencyInfo latency = new LatencyInfo(
            PerfStatType.PUT_INT, start, end,
            new LatencyResult(
                putIntTotalReq, putIntTotalOps,
                putIntMin, putIntMax, putIntAvg,
                putInt95Th, putInt99Th, putIntOverflow));
        latencies.add(latency);
        packet.add(latency);
        /* Add put interval cummulative latency info to the packet. */
        latency = new LatencyInfo(
            PerfStatType.PUT_CUM, start, end,
            new LatencyResult(
                putCumTotalReq, putCumTotalOps,
                putCumMin, putCumMax, putCumAvg,
                putCum95Th, putCum99Th, putCumOverflow));
        latencies.add(latency);
        packet.add(latency);
        /* Add single interval non-commulative latency info to the packet. */
        latency = new LatencyInfo(
            PerfStatType.USER_SINGLE_OP_INT, start, end,
            new LatencyResult(
                singleIntTotalReq, singleIntTotalOps,
                singleIntMin, singleIntMax, singleIntAvg,
                singleInt95Th, singleInt99Th, singleIntOverflow));
        latencies.add(latency);
        packet.add(latency);

        /* Add single interval cummulative latency info to the packet. */
        latency = new LatencyInfo(
            PerfStatType.USER_SINGLE_OP_CUM, start, end,
            new LatencyResult(
                singleCumTotalReq, singleCumTotalOps,
                singleCumMin, singleCumMax, singleCumAvg,
                singleCum95Th, singleCum99Th, singleCumOverflow));
        latencies.add(latency);
        packet.add(latency);

        /* Add multi interval non-cummulative latency info to the packet. */
        latency = new LatencyInfo(
            PerfStatType.USER_MULTI_OP_INT, start, end,
            new LatencyResult(
                multiIntTotalReq, multiIntTotalOps,
                multiIntMin, multiIntMax, multiIntAvg,
                multiInt95Th, multiInt99Th, multiIntOverflow));
        latencies.add(latency);
        packet.add(latency);

        /* Add multi interval cummulative latency info to the packet. */
        latency = new LatencyInfo(
            PerfStatType.USER_MULTI_OP_CUM, start, end,
            new LatencyResult(
                multiCumTotalReq, multiCumTotalOps,
                multiCumMin, multiCumMax, multiCumAvg,
                multiCum95Th, multiCum99Th, multiCumOverflow));
        latencies.add(latency);
        packet.add(latency);

        /* Add EnvStats info to the packet */
        final StatGroup logStats =
            new StatGroup(LogStatDefinition.GROUP_NAME,
                          LogStatDefinition.GROUP_DESC);
        final LongStat readStats = new LongStat(
            logStats, LogStatDefinition.FILEMGR_RANDOM_READS);
        readStats.add(nReads);
        final LongStat writeStats = new LongStat(
            logStats, LogStatDefinition.FILEMGR_RANDOM_WRITES);
        writeStats.add(nWrites);
        EnvironmentStats environmentStats = new EnvironmentStats();
        environmentStats.setLogStats(logStats);
        envStats = new EnvStats(start, end, environmentStats);
        packet.add(envStats);

        /* Add RepEnvStats info to the packet */
        final StatGroup replayStats =
            new StatGroup(ReplayStatDefinition.GROUP_NAME,
                          ReplayStatDefinition.GROUP_DESC);
        final LongStat nCommits = new LongStat(replayStats,
                                               ReplayStatDefinition.N_COMMITS);
        nCommits.add(nOps);
        final AtomicLongMapStat replicaLastCommitVLSNMap =
            new AtomicLongMapStat(
                replayStats,
                FeederManagerStatDefinition.REPLICA_LAST_COMMIT_VLSN_MAP);
        replicaLastCommitVLSNMap.createStat("rn1").set(1);
        replicaLastCommitVLSNMap.createStat("rn2").set(2);
        final StatGroup protocolStats =
            new StatGroup(BinaryProtocolStatDefinition.GROUP_NAME,
                          BinaryProtocolStatDefinition.GROUP_DESC);
        final IntegralLongAvgStat msgWriteRate =
            new IntegralLongAvgStat(
                protocolStats,
                BinaryProtocolStatDefinition.BYTES_WRITE_RATE,
                0, 0);
        msgWriteRate.set(new IntegralLongAvg(1000000, 1000000));
        final ReplicatedEnvironmentStats jeRepEnvStats =
            new ReplicatedEnvironmentStats();
        jeRepEnvStats.setStatGroup(replayStats);
        jeRepEnvStats.setStatGroup(protocolStats);
        kvRepEnvStats = new RepEnvStats(start, end, jeRepEnvStats);
        packet.add(kvRepEnvStats);

        return packet;
    }
}
