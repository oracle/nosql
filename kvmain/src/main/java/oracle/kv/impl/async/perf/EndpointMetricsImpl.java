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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import oracle.kv.impl.measurement.StatsUtils;
import oracle.nosql.common.sklogger.measure.ThroughputElement;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Implements the endpoint metrics.
 */
@SuppressWarnings("deprecation")
public class EndpointMetricsImpl
    implements oracle.kv.stats.EndpointMetrics, Serializable {

    private static final long serialVersionUID = 1L;

    private static final long ONE_SECOND_IN_NANOS = 1_000_000_000L;

    public final String name;
    public final boolean isCreator;
    public final double dialogStartThroughput;
    public final double dialogDropThroughput;
    public final double dialogFinishThroughput;
    public final double dialogAbortThroughput;
    public final MetricStatsImpl dialogConcurrency;
    public final long eventRecordSampleRateHighestOneBit;
    /*
     * TODO: Make these field final and remove readObject method starting with
     * the 23.1 release.
     */
    public volatile MetricStatsImpl finishedDialogLatencyNanos;
    public volatile MetricStatsImpl abortedDialogLatencyNanos;
    public volatile Map<DialogEventPerf.EventSpan, MetricStatsImpl>
        eventLatencyNanosMap;
    /*
     * TODO: Remove transient after 23.1 release. Marked transient because
     * DialogEventPerf was not serilzable in 20.2 release. The
     * EndpointMetricsImpl is used for client logging (see KVStatsMonitor),
     * server-side operation stats logging (see OperationStatsTracker), JMX
     * (see o.k.i.m.j.RepNode) and PerfTrackListener (see
     * CommandServiceAPI#registerPerfTrackerListener, PerfView and
     * StatsPacket). The serialization of EndpointMetricsImpl and hence
     * DialogEventPerf is used by JMX and PerfTrackListener. Due to the change
     * for using Json instead of String to report stats in 20.3, this field is
     * changed from non-serializable to serializable. To maintain
     * compatibility, this field will be missing between 20.3 and 23.1. This is
     * OK since the field will still be present on both the client and server
     * logging.
     */
    public volatile transient Collection<DialogEventPerf> eventPerfRecords;
    /*
     * TODO: Remove these with the 23.1 release.
     */
    public final MetricStatsImpl finishedDialogLatencyMs =
        new MetricStatsImpl(0, 0, 0, 0, 0, 0);
    public final MetricStatsImpl abortedDialogLatencyMs =
        new MetricStatsImpl(0, 0, 0, 0, 0, 0);
    public final Map<DialogEventPerf.EventSpan, oracle.kv.stats.MetricStats>
        eventLatencyMillisMap = Collections.emptyMap();
    public final String eventRecords = "";
    public final List<String> sampledDialogPerfs = Collections.emptyList();

    public EndpointMetricsImpl(
        String name,
        boolean isCreator,
        double dialogStartThroughput,
        double dialogDropThroughput,
        double dialogFinishThroughput,
        double dialogAbortThroughput,
        MetricStatsImpl finishedDialogLatencyNanos,
        MetricStatsImpl abortedDialogLatencyNanos,
        MetricStatsImpl dialogConcurrency,
        Map<DialogEventPerf.EventSpan, MetricStatsImpl> eventLatencyNanosMap,
        long eventRecordSampleRateHighestOneBit,
        Collection<DialogEventPerf> eventPerfRecords) {

        this.name = name;
        this.isCreator = isCreator;
        this.dialogStartThroughput = dialogStartThroughput;
        this.dialogDropThroughput = dialogDropThroughput;
        this.dialogFinishThroughput = dialogFinishThroughput;
        this.dialogAbortThroughput = dialogAbortThroughput;
        this.finishedDialogLatencyNanos = finishedDialogLatencyNanos;
        this.abortedDialogLatencyNanos = abortedDialogLatencyNanos;
        this.dialogConcurrency = dialogConcurrency;
        this.eventLatencyNanosMap =
            Collections.unmodifiableMap(eventLatencyNanosMap);
        this.eventRecordSampleRateHighestOneBit =
            eventRecordSampleRateHighestOneBit;
        this.eventPerfRecords = eventPerfRecords;
    }

    @Override
    public String getName() {
        return name;
    }

    public boolean isCreator() {
        return isCreator;
    }

    @Override
    public double getDialogStartThroughput() {
        return dialogStartThroughput;
    }

    @Override
    public double getDialogDropThroughput() {
        return dialogDropThroughput;
    }

    @Override
    public double getDialogFinishThroughput() {
        return dialogFinishThroughput;
    }

    @Override
    public double getDialogAbortThroughput() {
        return dialogAbortThroughput;
    }


    @Override
    public oracle.kv.stats.MetricStats getFinishedDialogLatencyNanos() {
        return finishedDialogLatencyNanos;
    }

    @Override
    public oracle.kv.stats.MetricStats getAbortedDialogLatencyNanos() {
        return abortedDialogLatencyNanos;
    }

    @Override
    public oracle.kv.stats.MetricStats getDialogConcurrency() {
        return dialogConcurrency;
    }

    @Override
    public Map<String, oracle.kv.stats.MetricStats> getEventLatencyNanosMap() {
        return eventLatencyNanosMap.entrySet().stream().
            collect(Collectors.toMap(
                e -> e.getKey().toString(), e -> e.getValue()));
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    public JsonObject toJson() {
        final JsonObject object = new JsonObject();
        object.add(
            "dialogStartThroughput",
            (new ThroughputElement.Result(
                (long) dialogStartThroughput, ONE_SECOND_IN_NANOS))
            .toJson().getElement());
        object.add(
            "dialogDropThroughput",
            (new ThroughputElement.Result(
                (long) dialogDropThroughput, ONE_SECOND_IN_NANOS))
            .toJson().getElement());
        object.add(
            "dialogFinishThroughput",
            (new ThroughputElement.Result(
                (long) dialogFinishThroughput, ONE_SECOND_IN_NANOS))
            .toJson().getElement());
        object.add(
            "dialogAbortThroughput",
            (new ThroughputElement.Result(
                (long) dialogAbortThroughput, ONE_SECOND_IN_NANOS))
            .toJson().getElement());
        object.add("finishedDialogLatencyNanos",
                   finishedDialogLatencyNanos.toJson());
        object.add("abortedDialogLatencyNanos",
                   abortedDialogLatencyNanos.toJson());
        object.add("dialogConcurrency",
                   dialogConcurrency.toJson());
        object.add("eventLatencyNanos",
                   eventLatencyNanosMapToJson());
        addEventPerfRecords(object);
        return object;
    }

    private JsonArray eventLatencyNanosMapToJson() {
        return eventLatencyNanosMap.entrySet().stream().
            sorted(Map.Entry.comparingByKey()).
            map(e -> {
                final JsonObject jsonEntry = new JsonObject();
                jsonEntry.add(e.getKey().toString(), e.getValue().toJson());
                return jsonEntry;
            }).
            collect(StatsUtils.getArrayCollector());
    }

    private void addEventPerfRecords(JsonObject object) {
        final boolean notSampledForRecord =
            (eventRecordSampleRateHighestOneBit ==
             Long.highestOneBit(Long.MAX_VALUE));
        if (notSampledForRecord) {
            return;
        }
        object.addProperty("eventRecordSampleRate",
                           eventRecordSampleRateHighestOneBit);
        object.add(
            "eventPerfRecords",
            eventPerfRecords.stream().map((r) -> r.toJson())
            .collect(StatsUtils.getArrayCollector()));
    }

    /**
     * Initialize the null fields because the object was serialized from a
     * version prior to 20.3 when the field was added.
     */
    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();
        if (finishedDialogLatencyNanos == null) {
            finishedDialogLatencyNanos = new MetricStatsImpl(0, 0, 0, 0, 0, 0);
        }
        if (abortedDialogLatencyNanos == null) {
            abortedDialogLatencyNanos = new MetricStatsImpl(0, 0, 0, 0, 0, 0);
        }
        if (eventLatencyNanosMap == null) {
            eventLatencyNanosMap = Collections.emptyMap();
        }
        if (eventPerfRecords == null) {
            eventPerfRecords = Collections.emptyList();
        }
    }
}
