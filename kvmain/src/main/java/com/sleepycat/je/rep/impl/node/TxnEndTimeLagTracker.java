/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.impl.node;

import static com.sleepycat.je.log.LogEntryType.LOG_TXN_COMMIT;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.sleepycat.je.rep.stream.InputWireRecord;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.utilint.LagStats;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.json_simple.JsonArray;
import com.sleepycat.json_simple.JsonObject;

/**
 * Tracks the (expected) transaction-end time lag.
 *
 * <h2>Motivation and Background</h2>
 *
 * The Replica class computes a lag metrics in milli-second. The lag is the
 * difference between the current time on the replica and the timestamp
 * (assigned by the master) of the latest transaction committed on the replica
 * if the replica has more entries to commit; otherwise, it is the difference
 * between the current time on replica and the timestamp of the latest
 * heartbeat sent by the master.
 *
 * This lag is used (1) to decide whether the replica is eligible to serve the
 * "get" requests with a certain time consistency values and (2) to reflect the
 * performance metrics of the replica.
 *
 * The following diagram shows the relationship between the computed lag and
 * time consistency. In the diagram, wi(j) represents the i-th write in the
 * replay queue which was committed on the master at time j; ri[j](k)
 * represents the i-th read of time consistency j at time k. The lines from the
 * replay queue indicates when the writes are polled from the replay queue and
 * committed on the replica. The read r1 is first tested at time 5, which,
 * intuitively with time consistency, indicates it must see all the writes
 * before time 5 - 2 = 3. This includes w1 and w2.  Since it only sees w1, it
 * has to register for a latch and wait.  From the perspective of using the lag
 * to implement time consistency, since the replay queue is not empty, we
 * compute the lag with the most recent commits on the replica. The lag at time
 * 5 is 5 - 1 = 4 &gt; 2 and thus it has to wait. When w2 and w3 are replicated
 * at time 7 on replica, the lag is computed again with 7 - 5 = 2 == 2 which
 * passed the test.
 *
 * {@code
 *              Replica Commit    Replica Replay Queue
 *
 *  0                |            |
 *  1                |     -------|---- w1(1)
 *  2                |    /       |
 *  3                |----  ------|---- w2(3)
 *  4                |     /      |
 *  5  r1[2](5)  ----|    / ------|---- w3(5)
 *  6            \   |   / /      |
 *  7 (r1[2](7))  ---|-----       |---- w4(7)
 *  8 (r1[2](8))     |            |
 *  9                |            |---- w5(9)
 * 10                |            |
 * }
 * The situation with computing with heartbeat is similar but a bit more
 * complicated with heartbeat interval and the variance of network latency.
 *
 * This implementation can be improved to be less conservative. For example, as
 * is shown on the diagram, if r1 were tested at time 8, the lag would be 8 - 5
 * = 3 &gt; 2 which would fail the test. However, since we know there is no
 * write at time 6, it could in fact met the time consistency criteria.
 * Furthermore, the definition of time consistency as of release 22.1 can be
 * improved as well.  Please see readme/TimeConsistency.md for more detail.
 *
 * The lag also captures two important performance metrics of the replica,
 * namely, the network latency w.r.t. the master and the delay of commits from
 * replica replay queue. In the case where the lag is computed with heatbeat,
 * it captures the network latency (there is an issue that the lag may
 * include the heartbeat interval, see the section of Implementation
 * Considerations). In the case where the lag is computed with txn-end time on
 * master, it combines both metrics. It would be nice to separate these two but
 * that would require extra spaces to record the arrival time of the txn-end
 * messages which does not seem to worth the effort. We could use other metrics
 * such as the arrival rate and the relay queue size to obtain the average
 * queue delay.
 *
 * Over the years, there have been several occurrences of related questions on
 * what is the threshold for abnormal lag values that worth attention. For
 * example, there are health check mechanisms that prevent plans to continue if
 * the lag between replica and master is abnormal. Currently we resort to a
 * fixed threshold of several seconds to distinguish such abnormal state.
 * However, there is not much justification to the choice of this threshold.
 * Jira ticket [KVSTORE-1041] reports an issue with tests which tracks down to
 * the problem where the time for the lag to reduce to a threshold has been
 * increasing over the releases. This issue indicates that it is desirable to
 * track the lag value over the releases so that we could tell if there is any
 * performance degradation. This tracker mechanism intends to be the first step
 * to track this lag metrics and tests out some anomaly detection mechanisms.
 *
 * Anormaly detection is a broad area of research topics. <cite><a
 * href="https://www.researchgate.net/publication/270274504_Anomaly_Detection_in_Computer_Networks_A_State-of-the-Art_Review">Baddar,
 * et al.</a></cite> covers helpful basics regarding this area in their survey.
 * In particular, the anomaly detection for the lag metric is a single-feature
 * (as opposed to multi-feature), collective-granularity (as opposed to point
 * or context granularity) problem. Ideally, we would like to have a behavioral
 * (as opposed to signature-based) detector since we may need to distinguish
 * anomaly behavior from expected system load change. We may use more
 * sophisticated approaches such as machine learning for offline mechanisms
 * but a statistical approach is more suitable for online processing.
 *
 * <h2>Goal and Expected Usage</h2>
 *
 * Due to the difficulty of anomaly detection, this tracker aims to achieve
 * the following simpler functionalities:
 * <ol>
 * <li>Detects the boundary for the initial catch-up of the replica. A replica,
 * initialized after reconnecting to the master, will have to stream and catch
 * up to the master log. During this time, the replica is not considered ready
 * to use. This tracker will report by logging messages of the initialization
 * and catch-up events. This tracker logs three types of catch-up events:
 * </ol>
 * <ol type="i">
 * <li>the txn-end time lag between master and replica is below a threshold for
 * the first time;</li>
 * <li>the replica receives a transaction that needs ack for the first
 * time; and</li>
 * <li>the replica has committed the transaction of the sync-up VLSN. The
 * sync-up VLSN is the largest VLSN of the master when the replica is connected
 * to the master.
 * </li>
 * <li>Captures the history of lag statistics for offline anomaly detection.
 * This tracker will reports the lag statistics to the stats mechanism to
 * reserve data for offline use. </li>
 * <li>Provides hints of anomaly online. This tracker will utilize simple
 * mechanisms to detect anomaly, provides hints and report to the stats
 * mechanism.</li>
 * </ol>
 *
 * The most immediate usage of this tracker is to track catching-up performance
 * with logged initialization and catch-up events. This is to follow up on
 * [KVSTORE-1041]. We should install scripts to obtain the catching-up time for
 * our release tests and verify if there is any performance degradation among
 * releases.
 *
 * We could start to collect the reported history of lag statistics, correlate
 * with other events of the store and experiment with offline anomaly
 * detection.
 *
 * We could also start to verify if the online hints are good enough for the
 * purpose of health check. If it turns out to be reliable enough we could
 * replace the fixed threshold with a decision from this tracker. For example,
 * the following predicate might be a good candidate:
 * <pre>
 * {@code
 *    def is_replica_health:
 *        return is_caught_up() && is_normal()
 * }
 * </pre>
 *
 *
 * <h2>Implementation Considerations</h2>
 *
 * One of the criteria for replica-caught-up is when the lag is small enough.
 * The exact threshold ideally can be computed from some service-level
 * agreement (e.g., to meet the agreed-upon ack timeout) or learned from other
 * healthy replicas.  Currently since these mechanisms are not in place, we set
 * this threshold with a configurable parameter.
 *
 * This lag is computed in two places: the first being when the Replica thread
 * processes a txn-end or a heartbeat message; the second being when a "get"
 * request of is checked for time consistency. This tracker tracks the lag
 * computed at the first type of occurrences. The subtle difference between the
 * value computed in these two types is that the values computed at the second
 * can be larger than the actual value by around a heartbeat interval. To
 * demonstrate, suppose we have a fixed network latency 10ms, there is no new
 * writes, and the heartbeat interval is 1 second. At the processing of each
 * heartbeat, the lag will be updated to 10ms which is the actual value.
 * However, in between the hearbeats, the lag will increment with time until
 * the next heartbeat.
 *
 * We use a log-scale histogram to capture the lag. This seems to provide
 * enough statistical information with good precision under very small memory
 * usage. We keep track of four kinds of histogram.  A current histogram
 * captures the most recent lag statistics. It is reset to empty every update
 * interval.  A past histogram is updated incrementally every update interval
 * with the current histogram by the update interval through the entire replica
 * history. This past histogram is intended for the use of the offline anomaly
 * detection. Going through the reported past histogram entries can obtain the
 * full history of the behavior of the lag statistics. We also track two other
 * past histogram but applies dampening. These two histogram is used to capture
 * long-term and short-term past behavior of the lag. The online anomaly
 * detection works by comparing the current histogram to long-term and
 * short-term histogram. By applying dampening, we can limit the impact of
 * anomaly behaviors in the long-ago past. By distinguishing long-term and
 * short-term histograms, we can provide hints as to whether a recent anomaly
 * behavior is a more persistent change which could represents a workload
 * change.
 *
 * The online anomaly detection relies on a distance computation between two
 * histogram. See the documentation on {@link DampeningHistogram#getDistance}
 * for the justification of the distance computation algorithm. We report the
 * distance between the current histogram and the long-term/short-term
 * histograms to the statistics mechanism as hints for the anomaly. That is,
 * currently we do not apply any further analysis on the resulted distance to
 * give a definite answer. More experience is needed to implement an algorithm
 * for a more definite answer.
 *
 * The methods of this tracker will be called on the critical paths for the
 * replica stream replay. Therefore, we want to minimize the performance impact
 * and avoid synchronization in the replay thread. On the other hand, stats
 * collection are done in the other threads which requires synchronization.
 * This results in the following design.  Most fields in this class and
 * associated methods used to track the lag are not thread safe since they will
 * be used soly in the replica replay thread.  A json object which is an atomic
 * reference field is used to summarize the stats for this tracker. That field
 * is updated when there is a new lag value and the field has not been updated
 * for an interval ammortizing the synchronization cost. This means, however,
 * that there is a delay between what is reported in the stats mechanism and
 * the most current result. Since the replica stream loop gets heartbeat every
 * heartbeat interval. Under normal circumstances, the delay will be
 * approximately the update interval (default to 10 seconds) plus the heartbeat
 * interval (default to 250 ms) and the network latency (usually less than 100
 * ms, but up to read timeout which defaults to 60 s in JE, but changed
 * to 5 s in KV) - in all around 15 second.
 */
public class TxnEndTimeLagTracker {

    /* Histogram boundary constants */
    static final long NANOS_1_MS = 1_000_000L;
    static final long NANOS_10_MS = 10 * NANOS_1_MS;
    static final long NANOS_100_MS = 100 * NANOS_1_MS;
    static final long NANOS_1_S = 1000 * NANOS_1_MS;
    static final long NANOS_10_S = 10 * NANOS_1_S;
    static final long NANOS_100_S = 100 * NANOS_1_S;
    static final long NANOS_1000_S = 1000 * NANOS_1_S;

    /** Histogram bucket upper bounds. */
    private static final long[] BUCKET_UPPERBOUNDS =
        new long[] {
            NANOS_1_MS,
            NANOS_10_MS,
            NANOS_100_MS,
            NANOS_1_S,
            NANOS_10_S,
            NANOS_100_S,
            NANOS_1000_S,
            Long.MAX_VALUE,
        };

    /**
     * Returns the index for the given lag.
     */
    private static int getIndex(long lagNanos) {
        if (lagNanos < NANOS_1_S) {
            if (lagNanos < NANOS_10_MS) {
                if (lagNanos < NANOS_1_MS) {
                    return 0;
                }
                return 1;
            } else if (lagNanos < NANOS_100_MS) {
                return 2;
            }
            return 3;
        } else if (lagNanos < NANOS_100_S) {
            if (lagNanos < NANOS_10_S) {
                return 4;
            }
            return 5;
        } else if (lagNanos < NANOS_1000_S) {
            return 6;
        }
        return 7;
    }

    /** The histogram for the current period. */
    private final CurrHistogram currHisto;
    /** The histogram for all past. */
    private final PastHistogram pastHisto;
    /** The histogram for the short-term past. */
    private final DampeningHistogram shortTermPastHisto;
    /** The histogram for the long-term past. */
    private final DampeningHistogram longTermPastHisto;
    /** Update interval. */
    private final long updateIntervalNanos;
    /** The initial catch-up threshold. */
    private final long initialThresholdNanos;
    /** The logger. */
    private final Consumer<String> infoLogger;
    /** The lag stats to report. */
    private final LagStats lagStats;
    /**
     * The actual json used by lag stats to report. All LagStats instances
     * shares this same json object. Use atomic for synchronization between
     * updates to the stats in the Replica thread and the collection.
     */
    private final AtomicReference<JsonObject> currStats =
        new AtomicReference<>();

    /*
     * The following fields do not need synchronization since all of them are
     * updated and accessed in the Replica thread.
     */

    /** The init time in nanos. */
    private long initTimeNanos;
    /**
     * The flag indicating the initial catch-up threshold is passed after the
     * latest re-init.
     */
    private boolean pastInitialCatchupThreshold = false;
    /*
     * The flag indicating that we have received the first txn-commit that needs
     * an ack after the latest re-init.
     */
    private boolean pastFirstCommitNeedsAck = false;
    /** The  sync-up VLSN with the master. */
    private long syncupVLSN = -1;
    /*
     * The flat indicating that we have passed the sync-up VLSN after the latest
     * re-init.
     */
    private boolean pastSyncUpVLSN = false;
    /** Last time when currStats is updated. */
    private long lastUpdateTimeNanos = 0;


    /**
     * Constructs the stats tracker.
     *
     * @param updateIntervalNanos the update interval in nano-seconds. The
     * stats reported to the StatManager mechanism is updated every this
     * interval. During the period, the lag values are collected into a
     * histogram and will be used to compute distance with past histograms.
     * @param shortTermPeriodNanos the short-term period in nano-seconds. The
     * distance between current histogram and a past histogram with this
     * short-term period is computed which can be used to detect short-term
     * anomaly.
     * @param longTermPeriodNanos the long-term period in nano-seconds. The
     * distance between current histogram and a past histogram with this
     * long-term period is computed which can be used to detect long-term
     * anomaly.
     * @param initialThresholdNanos the initial catch up threshold in
     * nano-seconds. A message is logged once when this threshold is passed
     * every time the replica is connected to the master.
     * @param statGroup the stat group used for the stats
     * @param infoLogger the log routine used for init and catch-up messages.
     * Using a Consumer so that we do not need to pass along the environment
     * for logging.
     */
    public TxnEndTimeLagTracker(long updateIntervalNanos,
                                long initialThresholdNanos,
                                long shortTermPeriodNanos,
                                long longTermPeriodNanos,
                                StatGroup statGroup,
                                Consumer<String> infoLogger) {
        this.updateIntervalNanos =
            ensurePositive("updateIntervalNanos", updateIntervalNanos);
        this.initialThresholdNanos =
            ensurePositive("initialThresholdNanos", initialThresholdNanos);
        this.currHisto = new CurrHistogram();
        this.pastHisto = new PastHistogram();
        this.shortTermPastHisto =
            new DampeningHistogram(
                ensurePositive("shortTermPeriodNanos", shortTermPeriodNanos));
        this.longTermPastHisto =
            new DampeningHistogram(
                ensurePositive("longTermPeriodNanos", longTermPeriodNanos));
        this.infoLogger = infoLogger;
        this.lagStats = new LagStats(statGroup, currStats);
    }

    private static long ensurePositive(String name, long val) {
        if (val <= 0) {
            throw new IllegalArgumentException(String.format(
                "Expected %s to be positive, got %s",
                name, val));
        }
        return val;
    }

    /**
     * Re-init when the replica re-connects to a master.
     */
    public void reinit(long vlsn) {
        final long currTimeNanos = System.nanoTime();
        initTimeNanos = currTimeNanos;
        pastInitialCatchupThreshold = false;
        pastFirstCommitNeedsAck = false;
        lastUpdateTimeNanos = currTimeNanos;
        syncupVLSN = vlsn;
        infoLogger.accept(
            String.format(
                "%s reinit", TxnEndTimeLagTracker.class.getSimpleName()));
    }

    /**
     * Observes a new entry.
     */
    public void observe(long lagNanos,
                        long currTimeNanos,
                        Protocol.Entry entry) {
        final InputWireRecord wireRecord = entry.getWireRecord();
        if (LOG_TXN_COMMIT.equalsType(wireRecord.getEntryType())) {
            final Protocol.Commit commit = (Protocol.Commit) entry;
            final long vlsn = wireRecord.getVLSN();
            if (!pastFirstCommitNeedsAck && commit.getNeedsAck()) {
                infoLogger.accept(
                    String.format(
                        "%s received the first commit (vlsn=%s) "
                        + "that needs ack, "
                        + "%s ms after init",
                        TxnEndTimeLagTracker.class.getSimpleName(),
                        vlsn,
                        TimeUnit.NANOSECONDS.toMillis(
                            currTimeNanos - initTimeNanos)));
                pastFirstCommitNeedsAck = true;
            }
            if (!pastSyncUpVLSN && (vlsn >= syncupVLSN)) {
                infoLogger.accept(
                    String.format(
                        "%s passed the sync-up vlsn (%s) %s ms after init",
                        TxnEndTimeLagTracker.class.getSimpleName(),
                        syncupVLSN,
                        TimeUnit.NANOSECONDS.toMillis(
                            currTimeNanos - initTimeNanos)));
                pastSyncUpVLSN = true;
            }
        }
        observe(lagNanos, currTimeNanos);
    }

    /**
     * Observes a new lag value in nano-seconds.
     */
    public void observe(long lagNanos,
                        long currTimeNanos) {
        if (!pastInitialCatchupThreshold
            && (lagNanos <= initialThresholdNanos)) {
            infoLogger.accept(
                String.format(
                    "%s passed commit time lag threshold (%s ms) "
                    + "%s ms after init, "
                    + "current is %s ms",
                    TxnEndTimeLagTracker.class.getSimpleName(),
                    TimeUnit.NANOSECONDS.toMillis(initialThresholdNanos),
                    TimeUnit.NANOSECONDS.toMillis(
                        currTimeNanos - initTimeNanos),
                    TimeUnit.NANOSECONDS.toMillis(lagNanos)));
            pastInitialCatchupThreshold = true;
        }
        currHisto.observe(lagNanos);
        if (currTimeNanos - lastUpdateTimeNanos >= updateIntervalNanos) {
            updateCurrStats();
            lastUpdateTimeNanos = currTimeNanos;
        }
    }

    /**
     * Updates the stats for the last period.
     */
    private void updateCurrStats() {
        setCurrStats();
        shortTermPastHisto.merge(currHisto);
        longTermPastHisto.merge(currHisto);
        pastHisto.merge(currHisto);
        currHisto.reset();
    }

    private void setCurrStats() {
        final JsonObject obj = new JsonObject();
        final JsonArray currHistoCounts = new JsonArray();
        Arrays.stream(currHisto.getSnapshot())
            .forEach((c) -> currHistoCounts.add(c));
        obj.put("currHisto", currHistoCounts);
        final JsonArray pastHistoCounts = new JsonArray();
        Arrays.stream(pastHisto.getSnapshot())
            .forEach((c) -> pastHistoCounts.add(c));
        obj.put("pastHisto", pastHistoCounts);
        obj.put("shortTermDistance",
                shortTermPastHisto.getDistance(currHisto));
        obj.put("longTermDistance",
                longTermPastHisto.getDistance(currHisto));
        obj.put("shortTermAverage",
                shortTermPastHisto.getAverage());
        obj.put("longTermAverage",
                longTermPastHisto.getAverage());
        lagStats.set(obj);
    }

    /**
     * Histogram using a long array as count.
     */
    private class LongHistogram {

        protected final long[] counts = new long[BUCKET_UPPERBOUNDS.length];
        protected long total = 0;

        /**
         * Observes a lag with the given value.
         */
        public void observe(long lagNanos) {
            counts[getIndex(lagNanos)] ++;
            total ++;
        }

        /**
         * Returns the current snapshot of the histogram counts.
         */
        public long[] getSnapshot() {
            return Arrays.copyOf(counts, counts.length);
        }
    }

    /**
     * The histogram for the current period.
     */
    class CurrHistogram extends LongHistogram {

        public void reset() {
            Arrays.fill(counts, 0);
            total = 0;
        }
    }

    /**
     * The histogram for the past history.
     */
    class PastHistogram extends LongHistogram {

        /**
         * Merge with a CurrHistogram.
         */
        public void merge(CurrHistogram other) {
            total = 0;
            for (int i = 0; i < counts.length; ++i) {
                counts[i] += other.counts[i];
                total += counts[i];
            }
        }

        /**
         * Returns the average of this histogram.
         *
         * The average is computed excluding the last bucket which is used to
         * capture unreasonably high lag values. The upper bound for the last
         * bucket will skew the computation.
         */
        public long getAverage() {
            if (total == 0) {
                return 0;
            }
            /*
             * Use double to prevent overflow. Double.MAX_VALUE equals to
             * 1.79e308 ns = 5.6e291 years. That is, the accumlated latency
             * reaches that many years which is extremely unlikely to happen.
             */
            double sum = 0;
            for (int i = 0; i < counts.length - 1; ++i) {
                sum += counts[i] * BUCKET_UPPERBOUNDS[i];
            }
            /*
             * Cast to long also will not overflow since the largest upper
             * bound is around 1000 seconds (we are excluding the overflow
             * bucket when computing average) and therefore, the average should
             * be less than that value.
             */
            return (long) (sum / total);
        }
    }

    /**
     * A dampening histogram for the past history.
     */
    class DampeningHistogram {

        /** Count histogram. Using double since we will do dampening. */
        private final double[] counts = new double[BUCKET_UPPERBOUNDS.length];
        /** Sum of counts. */
        private double total = 0;
        /** The last merge timestamp. */
        private long lastMergeTimeNanos = System.nanoTime();
        /**
         * The period for the dampening factor.
         *
         * See also, http://en.wikipedia.org/wiki/
         * Moving_average#Application_to_measuring_computer_performance.
         *
         * For a period P, two values and the associated timestamps (v1, t1)
         * and (v2, t2), t2 greater than t1, the dampening average of the two
         * values is computed as v = (1 - d) * v2 + d * v1, where d = e^(-(t2 -
         * t1) / P). If t2 - t1 = P, then d = 0.37.  That is, a value P in the
         * past contributes to around 37% of the computed value.
         */
        private final long dampeningPeriodNanos;

        private DampeningHistogram(long dampeningPeriodNanos) {
            this.dampeningPeriodNanos = dampeningPeriodNanos;
        }

        public long getDampeningPeriodNanos() {
            return dampeningPeriodNanos;
        }

        /**
         * Dampens the counts and merge with a CurrHistogram.
         */
        public void merge(CurrHistogram other) {
            merge(other, System.nanoTime());
        }

        public void merge(CurrHistogram other, long currTimeNanos) {
            if (currTimeNanos < lastMergeTimeNanos) {
                /* Something is wrong with the time, just ignore. */
                return;
            }
            final double d = Math.exp(
                -((currTimeNanos - lastMergeTimeNanos)
                  /((double) dampeningPeriodNanos)));
            total = 0;
            for (int i = 0; i < counts.length; ++i) {
                counts[i] = ((1 - d) * other.counts[i] + counts[i] * d);
                total += counts[i];
            }
        }

        /**
         * Computes the normalized distance against a current histogram based
         * on Cha's definition and algorithm.
         *
         * See https://cedar.buffalo.edu/~srihari/papers/PRJ02-DM.pdf(equation
         * 9 for the definition and section 5.2 for the algorithm). The paper
         * defines the distance between two histogram as the minimum movement
         * to make to transform one histogram to the other. Specifically, the
         * distance of two histogram A and B is defined as
         *
         *                 n - 1
         * D(A, B) = min ( sum   (  d(a_i, b_j) ))
         *           A,B   i,j=0
         *
         * where n is the total number of items of the histogram; a_i and b_j
         * are the i th element of A and the j th element of B (note that they
         * are not the counts in the bucket); and d(a_i, b_j) is the difference
         * between the value of a_i and b_j, i.e, the distance of the buckets
         * that holds a_i and b_j. Computing the distance in a niave way is
         * O(n!) in complexity but the paper provides an O(B) algorithm where B
         * is the number of buckets. The pseudocode is listed as following:
         * {@code
         * pre1xsum = 0
         * h_dist = 0
         * for k = 0 to b - 1:
         *   prefixsum + = A[k] - B[k]
         *   h_dist + = | prefixsum |
         * return(h_dist)
         * }
         *
         * As is stated in the paper, the advantage of this definition includes
         * (1) Many other distance definition (see
         * http://www.fisica.edu.uy/~cris/teaching/Cha_pdf_distances_2007.pdf)
         * has a so-called shuffling invariant property while this definition
         * is shuffling dependent. For example, if we treat the histogram
         * counts as a vector with each bucket as a dimension, the distance is
         * not dependent on the bucket boundary values. (2) This definition is
         * able to distinguish mutli-modal histograms comparing to definitions
         * based on the first order statistics such as average and min/max.
         * (3) The distance has a intuitive real-world implication which makes
         * it easier to set threshold.
         *
         * The returned distance value is computed with normalized histogram so
         * that both histogram counts has a total of 1. Therefore the returned
         * distance of 1 milli-second could mean that all the values are off by
         * 1 milli-second or that 10% of the values are off by 10
         * milli-seconds. The distance value is also signed with a negative
         * value indicating this PastHistogram has a smaller average. Another
         * difference in this implementation comparing to the listed pseudocode
         * above is that the histograms bucket values in this implementation
         * increment by a value larger than one.
         *
         * The computation excludes the last bucket which is used to capture
         * unreasonably high lag values. The upper bound for the last
         * bucket will skew the computation.
         *
         * @return the distance, {@code null} if at least one of the histogram
         * has no data
         */
        public Double getDistance(CurrHistogram other) {
            final double thisTotal =
                total - counts[counts.length - 1];
            final double thatTotal =
                other.total - other.counts[counts.length - 1];
            if ((thisTotal == 0) || (thatTotal == 0)) {
                return null;
            }
            double prefixSum = 0;
            double absDistance = 0;
            double averageDiff = 0;
            for (int i = 0; i < counts.length; ++i) {
                prefixSum += (counts[i] / thisTotal
                              - other.counts[i] / thatTotal);
                if (i < counts.length - 1) {
                    absDistance += Math.abs(prefixSum)
                        * (BUCKET_UPPERBOUNDS[i + 1] - BUCKET_UPPERBOUNDS[i]);
                }
                averageDiff += prefixSum * BUCKET_UPPERBOUNDS[i];
            }
            return ((averageDiff > 0) ? 1 : -1 ) * absDistance;
        }

        /**
         * Returns the average of this histogram.
         *
         * The average is computed excluding the last bucket which is used to
         * capture unreasonably high lag values. The upper bound for the last
         * bucket will skew the computation.
         */
        public double getAverage() {
            if (total == 0) {
                return 0;
            }
            double sum = 0;
            for (int i = 0; i < counts.length - 1; ++i) {
                sum += counts[i] * BUCKET_UPPERBOUNDS[i];
            }
            return (sum / total);
        }
    }

    /* For testing. */

    public CurrHistogram createCurrHistogram() {
        return new CurrHistogram();
    }

    public PastHistogram createPastHistogram() {
        return new PastHistogram();
    }

    public DampeningHistogram createDampeningHistogram(
        long dampeningPeriodNanos)
    {
        return new DampeningHistogram(dampeningPeriodNanos);
    }

    public long getUpdateIntervalNanos() {
        return updateIntervalNanos;
    }

    public long getInitialThresholdNanos() {
        return initialThresholdNanos;
    }

    public long getShortTermPeriodNanos() {
        return shortTermPastHisto.getDampeningPeriodNanos();
    }

    public long getLongTermPeriodNanos() {
        return longTermPastHisto.getDampeningPeriodNanos();
    }
}
