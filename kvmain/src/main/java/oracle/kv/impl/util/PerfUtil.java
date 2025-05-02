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

package oracle.kv.impl.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.sleepycat.utilint.LatencyStat;

/**
 * Utility methods for performance tuning.
 */
public class PerfUtil {

    private static volatile long T0 = System.nanoTime();

    public static void setT0() {
        T0 = System.nanoTime();
    }

    /**
     * Clear all perf stats.
     */
    public static void clear() {
        LatencyPerf.clear();
        ThroughputPerf.clear();
    }


    /**
     * A utility for profiling the time spent on sections of a single-threaded
     * code path.
     *
     * This utility is expected to be used for debugging purpose only.
     *
     * Note that the methods are thread-safe for sections of different names,
     * but it is not expected for the caller to profile the same section from
     * multiple threads.
     *
     * To use instrument a method with:
     *      LatencyPerf.begin("section");
     *      ...
     *      LatencyPerf.end("section");
     *
     * And print the result with:
     *      LatencyPerf.print()
     *
     */
    public static class LatencyPerf {

        private static final Map<String, LPerfStat> lpStats =
            new ConcurrentHashMap<String, LPerfStat>();
        private static final Map<String, Long> startTimeNs =
            new ConcurrentHashMap<String, Long>();
        /**
         * Begins a section.
         *
         * @param name name of the section
         */
        public static void begin(String name) {
            begin(name, 1000, 1000);
        }

        /**
         * Begins a section.
         *
         * @param name name of the section
         * @param maxMillis expected max latency
         * @param thresholdMillis threshold of abnormal latency to print
         */
        public static void begin(String name,
                                 long maxMillis,
                                 long thresholdMillis) {
            startTimeNs.put(name, Long.valueOf(System.nanoTime()));
            if (!lpStats.containsKey(name)) {
                lpStats.put(name, new LPerfStat(maxMillis, thresholdMillis));
            }
        }

        /**
         * Ends a section.
         *
         * @param name name of the section
         */
        public static void end(String name) {
            end(name, null);
        }

        /**
         * Ends a section.
         *
         * @param name name of the section
         * @param info info to print with the section if it is abnormal
         */
        public static void end(String name, Object info) {
            final Long start = startTimeNs.get(name);
            lpStats.get(name).set(System.nanoTime() - start, start, info);
        }

        /**
         * Clears the stats.
         */
        public static void clear() {
            lpStats.clear();
            startTimeNs.clear();
        }

        /**
         * Prints the stats to system out.
         */
        public static void print() {
            System.out.println(getStatsString());
        }

        /**
         * Prints the stats to the logger.
         */
        public static void print(Logger logger, Level level) {
            if (logger.isLoggable(level)) {
                logger.log(level, getStatsString());
            }
        }

        /**
         * Returns the perf stats string.
         */
        private static String getStatsString() {
            final StringBuilder sb =
                new StringBuilder("[PerfUtil.LatencyPerf]\n");
            final List<String> keys = new ArrayList<String>();
            keys.addAll(lpStats.keySet());
            Collections.sort(keys);
            for (String key : keys) {
                sb.append(String.format("(%s ms)", key)).
                    append("\n").append(lpStats.get(key));
            }
            return sb.toString();
        }

        private static class LPerfStat {
            private final LatencyStat latencyStat;
            private final AtomicInteger count = new AtomicInteger();
            private final long thresholdMillis;
            private final Queue<String> abnormalInfo =
                new ConcurrentLinkedQueue<String>();

            private LPerfStat(long max, long threshold) {
                this.latencyStat = new LatencyStat(max);
                this.thresholdMillis = threshold;
            }

            private void set(long val, Long start, Object info) {
                latencyStat.set(val);
                final int cnt = count.incrementAndGet();
                final double valMs = val / 1.0e6;
                final double startMs = (start - T0) / 1.0e6;
                final double endMs = startMs + valMs;
                if (valMs >= thresholdMillis) {
                    abnormalInfo.add(String.format(
                                         "#%d %.2fms (%.2f, %.2f) %s",
                                         cnt, valMs, startMs, endMs,
                                         (info == null) ? "" : info));
                }
            }

            @Override
            public String toString() {
                final StringBuilder sb = new StringBuilder();
                sb.append("\t").append(latencyStat.calculate()).append("\n");
                for (String info : abnormalInfo) {
                    sb.append("\t").append(info).append("\n");
                }
                return sb.toString();
            }
        }
    }


    /**
     * A utility for profiling the throughput of a event.
     *
     * This utility is expected to be used for debugging purpose only. The
     * methods are thread-safe.
     *
     * To instrument a event:
     *      ThroughputPerf.increment("event");
     *
     * And print the result with:
     *      ThroughputPerf.print();
     */
    public static class ThroughputPerf {

        private static final Map<String, TPerfStat> tpStats =
            new ConcurrentHashMap<String, TPerfStat>();

        /**
         * Increments the event count and update the stats.
         *
         * @param name name of the event
         */
        public static int increment(String name) {
            return increment(name, 1.0f);
        }

        /**
         * Increments the event count and update the stats.
         *
         * @param name name of the event
         * @param min minimal expected throughput
         */
        public static int increment(String name, float min) {
            final TPerfStat stat =
                tpStats.computeIfAbsent(name, k -> new TPerfStat(min));
            return stat.increment();
        }

        /**
         * Clears the stats.
         */
        public static void clear() {
            tpStats.clear();
        }

        /**
         * Prints the stats to system out.
         */
        public static void print() {
            System.out.println(getStatsString());
        }

        /**
         * Prints the stats to the logger.
         */
        public static void print(Logger logger, Level level) {
            if (logger.isLoggable(level)) {
                logger.log(level, getStatsString());
            }
        }

        /**
         * Returns the perf stats string.
         */
        private static String getStatsString() {
            final StringBuilder sb =
                new StringBuilder("[PerfUtil.ThroughputPerf]\n");
            final List<String> keys = new ArrayList<String>();
            keys.addAll(tpStats.keySet());
            Collections.sort(keys);
            for (String key : keys) {
                sb.append(String.format("(%s #/sec)", key)).
                    append("\n").append(tpStats.get(key));
            }
            return sb.toString();
        }

        private static class TPerfStat {
            private final long startTimeNs = System.nanoTime();
            private final LatencyStat intervalStat;
            private volatile int count = 0;
            private volatile long lastTimeNs = -1;

            private TPerfStat(float min) {
                this.intervalStat = new LatencyStat((int) (1.0 / min * 1000));
            }

            private synchronized int increment() {
                count ++;
                long curr = System.nanoTime();
                if (lastTimeNs == -1) {
                    lastTimeNs = curr;
                } else {
                    intervalStat.set(curr - lastTimeNs);
                    lastTimeNs = curr;
                }
                return count;
            }

            @Override
            public String toString() {
                final StringBuilder sb = new StringBuilder();
                sb.append("\t").
                    append(String.format(
                               "throughput=%.2f",
                               count / ((lastTimeNs - startTimeNs) / 1.0e9))).
                    append("\n").
                    append("\tinterval stats: ").
                    append(intervalStat.calculate()).
                    append("\n");
                return sb.toString();
            }
        }
    }
}

