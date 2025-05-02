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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import oracle.kv.impl.async.dialog.DialogContextImpl;
import oracle.kv.impl.util.ObjectUtil;
import oracle.kv.impl.util.WatcherNames;
import oracle.nosql.common.sklogger.measure.LatencyElement;
import oracle.nosql.common.sklogger.measure.LongPercentileElement;
import oracle.nosql.common.sklogger.measure.ThroughputElement;

/**
 * Tracks the dialog event metrics for an endpoint across mutliple connections.
 *
 * <p>The perf tracker is expected to be polled from time to time by calling
 * the {@link #obtain} method.
 *
 * <p>Note that, on the creator side, one endpoint is created for each remote
 * address (host/port) and hence one perf tracker is assigned for each remote
 * address (host/port). On the responder side, endpoints spawn from the same
 * listening port are assigned to the same perf tracker. This is because we
 * cannot distinguish different interfaces in the dialog layer when a new
 * connection is established. In a word, there are as many perf tracker as the
 * number of listening ports. Typically, we will have one perf tracker per
 * JVM.
 *
 * <p>This means that all the metrics collected here is shared among all the
 * endpoints on the responder side. The rationale is that at the responder
 * side, all the dialogs are executed within the same environment. Plus there
 * is only one important dialogs that have a performance impact: the
 * AsyncRequestHandler interface. Therefore, this overview stats should
 * suffice. Finer-grain stats may be built at different layers.
 *
 * <h1>Dialog Info Metrics</h1>
 *
 * <p>It is useful to present a dialog event breakdown for diaganosis
 * especially for performance issues. The {@link DialogInfoPerf} object collect
 * event time and information for dialog events such as init, read/write,
 * send/receive and finish/abort. These dialog event breakdown are reported
 * here in two approaches: (1) a stats on the latency between two consecutive
 * events, (2) a series of records for dialog events about specific dialogs.
 * The first approach provides a statistical overview; the second a more
 * intuitive picture for specific dialogs.
 *
 * <h2>Info Records</h2>
 *
 * <p>The dialog info records are queued up waiting to be polled. To prevent
 * from memory explosion, the number of queued event records, between two
 * consecutive polling, is designed to be fixed as {@code
 * maxNumPollingInfoRecords} and extra records are dropped.
 *
 * <p>The queueing mechanism resulted in the limitation that dialog event
 * records will always be cleared no matter the {@code clear} flag in the
 * {@code getMetrics} call.
 *
 * <p>To mitigate the performance impact, we should not generate more records
 * than necessary. It also makes more sense to generate records that are
 * separated in time (e.g., versus crowded around the boundary of polling
 * time). For the above two reasons, the dialogs are also sampled for log
 * records which is controlled by a {@code infoRecordSampleRate}. The {@code
 * infoRecordSampleRate} is adjusted according to the {@code
 * maxNumPollingEventRecords} and the throughput of the last polling interval
 * (monitor log interval which is a constant).
 *
 * <p>The dialogs on the creator endpoints are sampled exactly with the {@code
 * infoRecordSampleRate}. We would like to correlate the event records of the
 * same dialog on both the creator and responder sides and therefore a flag
 * indicating the dialog is sampled for event record is sent over the wire.
 *
 * <p>The sample rate for dialog event records on the responder endpoints is
 * applied on the sampled dialogs from the creator endpoints, i.e.,  one dialog
 * is sampled for event record every {@code infoRecordSampleRate}
 * creator-sampled. With this mechanism we can ensure most of the event records
 * logged at the responder endpoint will apppear on the creator endpoint log.
 *
 */
public class DialogEndpointPerfTracker {

    /**
     * A hidden system property to specify the {@code
     * latencyBreakdownSampleRate}.  We do not expect the value to change. The
     * value adjust the performance impact for collecting dialog latency
     * breakdown stats.  The default value should make it sufficient that the
     * impact is negeligible. The actual value used is always the maximum pow
     * of 2 value that is less than or equal to the specified.
     */
    public static final String LATENCY_BREAKDOWN_SAMPLE_RATE =
        "oracle.kv.async.perf.endpoint.latency.breakdown.sample.rate";
    public static final int LATENCY_BREAKDOWN_SAMPLE_RATE_DEFAULT = 1024;

    /**
     * Info record initial sample rate. Info record sample rate is dynamically
     * adjusted. The initial rate could be any value large enough to ammortize
     * the performance impact. Default to 1024. We do not expect the value to
     * change, but expose it and make it not final for testing.
     */
    public static int INFO_RECORD_INITIAL_SAMPLE_RATE = 1024;

    /**
     * A hidden system property to specify the {@code
     * maxNumPollingEventRecords}. We do not expect the value to change. We
     * expect people would just accept the values we pick as. Showing more less
     * event records only marginally adds more information. And our values are
     * already small and format pretty enough so that people should not be
     * annoyed by the records.
     */
    private static final String MAX_NUM_POLLING_INFO_RECORDS =
        "oracle.kv.async.perf.max.num.polling.info.records";

    /**
     * The default creator-side maxNumPollingEventRecords. To give an example,
     * with a default value of 4, a default monitor logging interval of 1
     * minute and a kvstore of 18 shards with rf=3. We will generate 18(shards)
     * * 3(rf) * 4(log records) = 216 log records per minute.
     */
    private static final int
        DEFAULT_CREATOR_MAX_NUM_POLLING_INFO_RECORDS = 4;
    /**
     * The default responder-side maxNumPollingEventRecords. To give an
     * example, with a default value of 16, a default stats file collect
     * interval of 1 minute, we will generate 16 log records per minute. The
     * default of RN_ACCEPT_MAX_ACTIVE_CONNS_DEFAULT is 8192, which means
     * averagely speaking, every 8192(connections) / 16(log records) = 512
     * sampled dialogs from all the connections, one will be picked.
     */
    private static final int
        DEFAULT_RESPONDER_MAX_NUM_POLLING_INFO_RECORDS = 16;
    /**
     * A watcher name for accumulated dialog throughput. One usage of the
     * accumulated dialog throughput is to tell if there is any problem of
     * missing dialog events processing (e.g., callback queued but not
     * processed due to race condition) by comparing the total volumes of dialog
     * events.
     */
    private static final String ACCUMULATED_DIALOG_THROUGHPUT_WATCHER =
        String.format("%s.accumulated.dialog.throughput.watcher",
                      DialogEndpointPerfTracker.class.getSimpleName());

    private final String name;
    private final boolean isCreator;

    /* General metrics */

    /* Throughput metrics */
    private final ThroughputElement dialogStartThroughput;
    private final ThroughputElement dialogDropThroughput;
    private final ThroughputElement dialogFinishThroughput;
    private final ThroughputElement dialogAbortThroughput;

    /* Latency metrics */
    private final LatencyElement finishedDialogLatency;
    private final LatencyElement abortedDialogLatency;
    private final LatencyElement pingLatency;

    /* Dialog concurrency metrics */
    private final AtomicInteger activeDialogCounter = new AtomicInteger(0);
    private final LongPercentileElement dialogConcurrency;

    /* Event latency metrics */

    /* The event latency sample rate specified by a system property */
    private final int latencyBreakdownSampleRate =
        Integer.getInteger(LATENCY_BREAKDOWN_SAMPLE_RATE,
                           LATENCY_BREAKDOWN_SAMPLE_RATE_DEFAULT);
    /*
     * The adjusted event latency sample rate which is the largest power of 2
     * less or equal to eventLatencySampleRate, equivalently, the value with
     * the highest single one-bit of eventLatencySampleRate.
     */
    private final long latencyBreakdownSampleRateHighestOneBit =
        PerfUtil.getSampleRateHighestOneBit(latencyBreakdownSampleRate);
    /* A counter on started dialogs for latency breakdown sampling */
    private final AtomicLong latencyBreakdownSampleCounter = new AtomicLong(0);
    /* A map of events to latency metrics */
    private final Map<DialogEvent.Span, LatencyElement> latencyBreakdown =
        new ConcurrentHashMap<>();

    /* Dialog info perf record metrics */
    /*
     * The adjusted info record sample rate which is the largest power of 2
     * less or equal to a computed sample rate. The sample rate is computed
     * according to the throughput of last period. Initialize using 1024.
     */
    private volatile long infoRecordSampleRateHighestOneBit =
        PerfUtil.getSampleRateHighestOneBit(INFO_RECORD_INITIAL_SAMPLE_RATE);
    /*
     * Indicates the sampled list is full to stop sampling. Volatile for read,
     * modify inside the synchronziation block of the sampled list.
     */
    private volatile boolean isInfoRecordQueueFull = false;
    /* A counter for dialogs sampled by the endpoints for event record */
    private final AtomicLong creatorInfoRecordCounter = new AtomicLong(0);
    /* The maximum number of info records we hold between two polling */
    private final int maxNumPollingInfoRecords;
    /* Sampled dialog info records.  Access must synchronized on the list. */
    private final Queue<DialogInfoPerf> infoRecordQueue = new ArrayDeque<>();

    enum SampleDecision {
        NONE(false, false),
        SAMPLE(true, false),
        SAMPLE_FOR_RECORD(true, true);
        final boolean shouldSample;
        final boolean sampleForRecord;
        SampleDecision(boolean shouldSample, boolean sampleForRecord) {
            this.shouldSample = shouldSample;
            this.sampleForRecord = sampleForRecord;
        }
        static SampleDecision get(boolean shouldSample,
                                  boolean sampleForRecord) {
            if (sampleForRecord) {
                return SAMPLE_FOR_RECORD;
            }
            if (shouldSample) {
                return SAMPLE;
            }
            return NONE;
        }
    }

    /**
     * Constructs a dialog endpoint perf tracker.
     */
    public DialogEndpointPerfTracker(String name, boolean isCreator) {
        ObjectUtil.checkNull("name", name);
        this.name = name;
        this.isCreator = isCreator;

        this.dialogStartThroughput = new ThroughputElement();
        this.dialogDropThroughput = new ThroughputElement();
        this.dialogFinishThroughput = new ThroughputElement();
        this.dialogAbortThroughput = new ThroughputElement();

        this.finishedDialogLatency = new LatencyElement();
        this.abortedDialogLatency = new LatencyElement();
        this.pingLatency = new LatencyElement();

        this.dialogConcurrency = new LongPercentileElement();

        this.maxNumPollingInfoRecords = Integer.getInteger(
            MAX_NUM_POLLING_INFO_RECORDS,
            isCreator ?
            DEFAULT_CREATOR_MAX_NUM_POLLING_INFO_RECORDS :
            DEFAULT_RESPONDER_MAX_NUM_POLLING_INFO_RECORDS);
    }

    /**
     * Returns the name.
     */
    public String getName() {
        return name;
    }

    /**
     * Called when a dialog is started on a creator endpoint.
     */
    public void onDialogStarted(DialogContextImpl context) {
        countDialogStartAndSample(context, shouldSampleForCreator());
    }

    private SampleDecision shouldSampleForCreator() {
        final long n = latencyBreakdownSampleCounter.incrementAndGet();
        final boolean sampleForLatency = shouldSampleForEventLatency(n);
        final boolean sampleForRecord = shouldSampleForEventRecord(n);
        return SampleDecision.get(sampleForLatency, sampleForRecord);
    }

    private void countDialogStartAndSample(DialogContextImpl context,
                                           SampleDecision sampleDecision) {
        dialogConcurrency.observe(activeDialogCounter.incrementAndGet());
        dialogStartThroughput.observe(1);
        if (sampleDecision.shouldSample) {
            context.sample(sampleDecision.sampleForRecord);
        }
    }

    private boolean shouldSampleForEventLatency(long count) {
        return PerfUtil.shouldSample(
            count, latencyBreakdownSampleRateHighestOneBit);
    }

    private boolean shouldSampleForEventRecord(long count) {
        return (PerfUtil
                .shouldSample(count, infoRecordSampleRateHighestOneBit)
                && (!isInfoRecordQueueFull));
    }

    /**
     * Called when a dialog is started on a responder endpoint.
     */
    public void onDialogStarted(DialogContextImpl context,
                                boolean sampledOnCreatorEndpoint) {
        countDialogStartAndSample(
            context, shouldSampleForResponder(sampledOnCreatorEndpoint));
    }

    private SampleDecision shouldSampleForResponder(
        boolean sampledOnCreatorEndpoint) {

        final long n = latencyBreakdownSampleCounter.incrementAndGet();
        final boolean sampleForLatency = shouldSampleForEventLatency(n);
        final boolean sampleForRecord =
            sampledOnCreatorEndpoint &&
            shouldSampleForEventRecord(
                creatorInfoRecordCounter.incrementAndGet());
        return SampleDecision.get(sampleForLatency, sampleForRecord);
    }

    /**
     * Called when a dialog is dropped.
     */
    public void onDialogDropped() {
        dialogDropThroughput.observe(1);
    }

    /**
     * Called when a dialog is finished.
     *
     * @param context the dialog context
     */
    public void onDialogFinished(DialogContextImpl context) {
        dialogFinishThroughput.observe(1);
        activeDialogCounter.decrementAndGet();
        finishedDialogLatency.observe(context.getLatencyNanos());
        addDialogInfoPerf(context);
    }

    /**
     * Adds the dialog info perf if present.
     */
    private void addDialogInfoPerf(DialogContextImpl context) {
        context.getInfoPerfTracker().ifPresent((t) -> {
            addLatencyBreakdown(t);
            if (t.isSampledForRecord()) {
                addDialogInfoRecord(t);
            }
        });
    }

    /**
     * Adds to the latency breakdown.
     */
    private void addLatencyBreakdown(DialogInfoPerfTracker tracker) {
        final Map<DialogEvent.Span, Long> map =
            tracker.getLatencyBreakdown();
        for (Map.Entry<DialogEvent.Span, Long> entry :
             map.entrySet()) {

            final DialogEvent.Span key = entry.getKey();
            final long val = entry.getValue();
            final LatencyElement element =
                latencyBreakdown.computeIfAbsent(
                    key,  (k) -> new LatencyElement());
            element.observe(val);
        }
    }

    /**
     * Adds a sampled dialog perf.
     */
    private void addDialogInfoRecord(DialogInfoPerfTracker tracker) {
        synchronized(infoRecordQueue) {
            if (isInfoRecordQueueFull) {
                return;
            }
            if (infoRecordQueue.size() < maxNumPollingInfoRecords) {
                infoRecordQueue.add(tracker.getDialogInfoPerf());
            } else {
                isInfoRecordQueueFull = true;
            }
        }
    }

    /**
     * Called when a dialog is aborted.
     *
     * @param context the dialog context
     */
    public void onDialogAborted(DialogContextImpl context) {
        dialogAbortThroughput.observe(1);
        activeDialogCounter.decrementAndGet();
        abortedDialogLatency.observe(context.getLatencyNanos());
        addDialogInfoPerf(context);
    }

    /**
     * Called when we obtained a ping ack with its associated ping timestamp.
     */
    public void onPingAck(long timestampNanos) {
        pingLatency.observe(System.nanoTime() - timestampNanos);
    }

    /**
     * Returns the dialog endpoint perf since last time.
     */
    public DialogEndpointPerf obtain(String watcherName, boolean clear) {
        final ThroughputElement.Result dialogStartThroughputResult =
            dialogStartThroughput.obtain(watcherName, clear);
        final DialogEndpointPerf metrics = new DialogEndpointPerf(
            name, isCreator,
            dialogStartThroughputResult,
            dialogDropThroughput.obtain(watcherName, clear),
            dialogFinishThroughput.obtain(watcherName, clear),
            dialogAbortThroughput.obtain(watcherName, clear),
            dialogStartThroughput.obtain(
                ACCUMULATED_DIALOG_THROUGHPUT_WATCHER, false),
            dialogDropThroughput.obtain(
                ACCUMULATED_DIALOG_THROUGHPUT_WATCHER, false),
            dialogFinishThroughput.obtain(
                ACCUMULATED_DIALOG_THROUGHPUT_WATCHER, false),
            dialogAbortThroughput.obtain(
                ACCUMULATED_DIALOG_THROUGHPUT_WATCHER, false),
            dialogConcurrency.obtain(watcherName, clear),
            finishedDialogLatency.obtain(watcherName, clear),
            abortedDialogLatency.obtain(watcherName, clear),
            pingLatency.obtain(watcherName, clear),
            latencyBreakdown.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> e.getKey(),
                    e -> e.getValue().obtain(watcherName, clear))),
            getInfoRecords(watcherName));
        computeInfoRecordSampleRate(
            (long) dialogStartThroughputResult.getThroughputPerSecond());
        return metrics;
    }

    /**
     * Returns the queue of sampled dialog perf objects and clears the queue.
     */
    private List<DialogInfoPerf> getInfoRecords(String watcherName) {
        /*
         * Event records are only collected for two watchers: the client-side
         * kvstats monitor and the server-side stats collection.
         */
        if (!(watcherName.equals(WatcherNames.KVSTATS_MONITOR) ||
              watcherName.equals(
                  WatcherNames.SERVER_STATS_TRACKER))) {
            return Collections.emptyList();
        }
        synchronized(infoRecordQueue) {
            final List<DialogInfoPerf> ret =
                new ArrayList<>(infoRecordQueue);
            infoRecordQueue.clear();
            isInfoRecordQueueFull = false;
            return ret;
        }
    }

    private void computeInfoRecordSampleRate(long startThroughput) {
        final long throughput = isCreator ?
            startThroughput : creatorInfoRecordCounter.get();
        final long infoRecordSampleRate = Math.max(
            (maxNumPollingInfoRecords == 0) ?
            Long.MAX_VALUE : throughput / maxNumPollingInfoRecords,
            /*
             * If the last interval has a very small throughput, we end up
             * sampling every dialog for event record, but that is fine since
             * we have a small event record queue.
             */
            1);
        infoRecordSampleRateHighestOneBit =
            PerfUtil.getSampleRateHighestOneBit(infoRecordSampleRate);
    }
}
