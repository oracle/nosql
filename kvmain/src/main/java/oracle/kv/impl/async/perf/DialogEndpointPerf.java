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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.JsonSerializationUtils;
import oracle.nosql.common.jss.ObjectNodeSerializable;
import oracle.nosql.common.sklogger.measure.LatencyElement;
import oracle.nosql.common.sklogger.measure.LongValueStats;
import oracle.nosql.common.sklogger.measure.LongValueStatsField;
import oracle.nosql.common.sklogger.measure.ThroughputElement;

/**
 * A collection of dialog perf metrics associated with one endpoint.
 */
public class DialogEndpointPerf extends ObjectNodeSerializable {

    /** The default latency breakdown. */
    public static final Map<DialogEvent.Span, LatencyElement.Result>
        DEFAULT_LATENCY_BREAKDOWN = Collections.emptyMap();

    /** The field map. */
    public static final Map<String, Field<?, DialogEndpointPerf>> FIELDS =
        new HashMap<>();
    /**
     * The string name associated with the endpoint, e.g.,
     * rg1-rn2(100.104.89.181:41541).
     */
    public static final String NAME =
        StringField.create(FIELDS, "name", DialogEndpointPerf::getName);
    /**
     * The boolean value indicating whether the endpoint is a creator.
     */
    public static final String IS_CREATOR =
        BooleanField.create(FIELDS, "isCreator", DialogEndpointPerf::isCreator);
    /** The throughput result of started dialogs for the last period. */
    public static final String DIALOG_START_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "dialogStartThroughput",
            DialogEndpointPerf::getDialogStartThroughput);
    /**
     * The throughput result of dropped dialogs for the last period. Dropped
     * dialogs are those never started due to endpoint shutting down or
     * resource limits.
     */
    public static final String DIALOG_DROP_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "dialogDropThroughput",
            DialogEndpointPerf::getDialogDropThroughput);
    /** The throughput result of finished dialogs for the last period. */
    public static final String DIALOG_FINISH_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "dialogFinishThroughput",
            DialogEndpointPerf::getDialogFinishThroughput);
    /**
     * The throughput result of aborted dialogs for the last period. Dialogs
     * are aborted mostly due to errors such as IOException.
     */
    public static final String DIALOG_ABORT_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "dialogAbortThroughput",
            DialogEndpointPerf::getDialogAbortThroughput);
    /** The throughput result of started dialogs from the beginning. */
    public static final String ACCUMULATED_DIALOG_START_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "accumulatedDialogStartThroughput",
            DialogEndpointPerf::getAccumulatedDialogStartThroughput);
    /**
     * The throughput result of dropped dialogs from the beginning. Dropped
     * dialogs are those never started due to endpoint shutting down or
     * resource limits.
     */
    public static final String ACCUMULATED_DIALOG_DROP_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "accumulatedDialogDropThroughput",
            DialogEndpointPerf::getAccumulatedDialogDropThroughput);
    /** The throughput result of finished dialogs from the beginning. */
    public static final String ACCUMULATED_DIALOG_FINISH_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "accumulatedDialogFinishThroughput",
            DialogEndpointPerf::getAccumulatedDialogFinishThroughput);
    /**
     * The throughput result of aborted dialogs from the beginning. Dialogs are
     * aborted mostly due to errors such as IOException.
     */
    public static final String ACCUMULATED_DIALOG_ABORT_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "accumulatedDialogAbortThroughput",
            DialogEndpointPerf::getAccumulatedDialogAbortThroughput);
    /** The statistics of the concurrent number of dialogs. */
    public static final String DIALOG_CONCURRENCY =
        LongValueStatsField.create(
            FIELDS, "dialogConcurrency",
            DialogEndpointPerf::getDialogConcurrency);
    /** The latency statistics of the finished dialogs. */
    public static final String FINISHED_DIALOG_LATENCY =
        LatencyElement.ResultField.create(
            FIELDS, "finishedDialogLatencyNanos",
            DialogEndpointPerf::getFinishedDialogLatency);
    /** The latency statistics of the aborted dialogs. */
    public static final String ABORTED_DIALOG_LATENCY =
        LatencyElement.ResultField.create(
            FIELDS, "abortedDialogLatencyNanos",
            DialogEndpointPerf::getAbortedDialogLatency);
    /** The latency statistics of the ping messages. */
    public static final String PING_LATENCY =
        LatencyElement.ResultField.create(
            FIELDS, "pingLatencyNanos",
            DialogEndpointPerf::getPingLatency);
    /**
     * The breakdown of dialog latency between events. The events include INIT,
     * WRITE, SEND, RECV, READ, FIN and ABORT.
     */
    public static final String DIALOG_LATENCY_BREAKDOWN =
        Field.create(FIELDS, "dialogLatencyBreakdownNanos",
                     (o, p, n, d) -> writeLatencyBreakdown(o, p, n),
                     (p, d) -> readLatencyBreakdown(p),
                     DEFAULT_LATENCY_BREAKDOWN);
    /**
     * The lists of dialog info which are sampled records with info of specific
     * dialogs, including dialog ID, read/write size and event times.
     */
    public static final String DIALOG_INFO_RECORDS =
        FilteredListField.create(FIELDS, "dialogInfoRecords",
                                 DialogEndpointPerf::getDialogInfoRecords,
                                 DialogInfoPerf::new);

    /**
     * Writes the latency breakdown map to a json array. An array is used so
     * that the resulting output is sorted according to event order which is
     * more human readable.
     */
    private static void writeLatencyBreakdown(
        DialogEndpointPerf perf,
        ObjectNode payload,
        String name)
    {
        final ArrayNode array =
            perf.dialogLatencyBreakdown.entrySet().stream()
            .sorted(
                (e1, e2) ->
                (perf.isCreator
                 ?  DialogEvent.SpanComparator.FOR_CREATOR
                 : DialogEvent.SpanComparator.FOR_RESPONDER)
                .compare(e1.getKey(), e2.getKey()))
            .filter(e -> !e.getValue().isDefault())
            .map(e -> {
                final ObjectNode jsonEntry = JsonUtils.createObjectNode();
                jsonEntry.put(e.getKey().toString(), e.getValue().toJson());
                return jsonEntry; })
            .collect(JsonSerializationUtils.getArrayCollector());
        if (array.isEmpty()) {
            return;
        }
        payload.put(name, array);
    }

    /**
     * Reads the latency breakdown map from a json payload.
     */
    private static Map<DialogEvent.Span, LatencyElement.Result>
        readLatencyBreakdown(JsonNode payload)
    {
        if (!payload.isArray()) {
            return DEFAULT_LATENCY_BREAKDOWN;
        }
        final Map<DialogEvent.Span, LatencyElement.Result> result =
            new HashMap<>();
        final ArrayNode array = payload.asArray();
        for (JsonNode e : array) {
            if (!e.isObject()) {
                continue;
            }
            final ObjectNode obj = e.asObject();
            for (Map.Entry<String, JsonNode> entry : obj.entrySet()) {
                final DialogEvent.Span span =
                    DialogEvent.Span.create(entry.getKey());
                if (span == null) {
                    continue;
                }
                result.put(span, new LatencyElement.Result(entry.getValue()));
            }
        }
        return result;
    }

    private final String name;
    private final boolean isCreator;
    private final ThroughputElement.Result dialogStartThroughput;
    private final ThroughputElement.Result dialogDropThroughput;
    private final ThroughputElement.Result dialogFinishThroughput;
    private final ThroughputElement.Result dialogAbortThroughput;
    private final ThroughputElement.Result accumulatedDialogStartThroughput;
    private final ThroughputElement.Result accumulatedDialogDropThroughput;
    private final ThroughputElement.Result accumulatedDialogFinishThroughput;
    private final ThroughputElement.Result accumulatedDialogAbortThroughput;
    private final LongValueStats dialogConcurrency;
    private final LatencyElement.Result finishedDialogLatency;
    private final LatencyElement.Result abortedDialogLatency;
    private final LatencyElement.Result pingLatency;
    private final Map<DialogEvent.Span, LatencyElement.Result>
        dialogLatencyBreakdown;
    private final List<DialogInfoPerf> dialogInfoRecords;

    /** Private constructor. */
    public DialogEndpointPerf(
        String name,
        boolean isCreator,
        ThroughputElement.Result dialogStartThroughput,
        ThroughputElement.Result dialogDropThroughput,
        ThroughputElement.Result dialogFinishThroughput,
        ThroughputElement.Result dialogAbortThroughput,
        ThroughputElement.Result accumulatedDialogStartThroughput,
        ThroughputElement.Result accumulatedDialogDropThroughput,
        ThroughputElement.Result accumulatedDialogFinishThroughput,
        ThroughputElement.Result accumulatedDialogAbortThroughput,
        LongValueStats dialogConcurrency,
        LatencyElement.Result finishedDialogLatency,
        LatencyElement.Result abortedDialogLatency,
        LatencyElement.Result pingLatency,
        Map<DialogEvent.Span, LatencyElement.Result> dialogLatencyBreakdown,
        List<DialogInfoPerf> dialogInfoRecords)
    {
        this.name = name;
        this.isCreator = isCreator;
        this.dialogStartThroughput = dialogStartThroughput;
        this.dialogDropThroughput = dialogDropThroughput;
        this.dialogFinishThroughput = dialogFinishThroughput;
        this.dialogAbortThroughput = dialogAbortThroughput;
        this.accumulatedDialogStartThroughput =
            accumulatedDialogStartThroughput;
        this.accumulatedDialogDropThroughput =
            accumulatedDialogDropThroughput;
        this.accumulatedDialogFinishThroughput =
            accumulatedDialogFinishThroughput;
        this.accumulatedDialogAbortThroughput =
            accumulatedDialogAbortThroughput;
        this.dialogConcurrency = dialogConcurrency;
        this.finishedDialogLatency = finishedDialogLatency;
        this.abortedDialogLatency = abortedDialogLatency;
        this.pingLatency = pingLatency;
        this.dialogLatencyBreakdown = dialogLatencyBreakdown;
        this.dialogInfoRecords = dialogInfoRecords;
    }

    public DialogEndpointPerf(JsonNode payload) {
        this(readField(FIELDS, payload, NAME),
             readField(FIELDS, payload, IS_CREATOR),
             readField(FIELDS, payload, DIALOG_START_THROUGHPUT),
             readField(FIELDS, payload, DIALOG_DROP_THROUGHPUT),
             readField(FIELDS, payload, DIALOG_FINISH_THROUGHPUT),
             readField(FIELDS, payload, DIALOG_ABORT_THROUGHPUT),
             readField(FIELDS, payload, ACCUMULATED_DIALOG_START_THROUGHPUT),
             readField(FIELDS, payload, ACCUMULATED_DIALOG_DROP_THROUGHPUT),
             readField(FIELDS, payload, ACCUMULATED_DIALOG_FINISH_THROUGHPUT),
             readField(FIELDS, payload, ACCUMULATED_DIALOG_ABORT_THROUGHPUT),
             readField(FIELDS, payload, DIALOG_CONCURRENCY),
             readField(FIELDS, payload, FINISHED_DIALOG_LATENCY),
             readField(FIELDS, payload, ABORTED_DIALOG_LATENCY),
             readField(FIELDS, payload, PING_LATENCY),
             readField(FIELDS, payload, DIALOG_LATENCY_BREAKDOWN),
             readField(FIELDS, payload, DIALOG_INFO_RECORDS));
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }

    /* Getters */

    public String getName() {
        return name;
    }

    public boolean isCreator() {
        return isCreator;
    }

    public ThroughputElement.Result getDialogStartThroughput() {
        return dialogStartThroughput;
    }

    public ThroughputElement.Result getDialogDropThroughput() {
        return dialogDropThroughput;
    }

    public ThroughputElement.Result getDialogFinishThroughput() {
        return dialogFinishThroughput;
    }

    public ThroughputElement.Result getDialogAbortThroughput() {
        return dialogAbortThroughput;
    }

    public ThroughputElement.Result getAccumulatedDialogStartThroughput() {
        return accumulatedDialogStartThroughput;
    }

    public ThroughputElement.Result getAccumulatedDialogDropThroughput() {
        return accumulatedDialogDropThroughput;
    }

    public ThroughputElement.Result getAccumulatedDialogFinishThroughput() {
        return accumulatedDialogFinishThroughput;
    }

    public ThroughputElement.Result getAccumulatedDialogAbortThroughput() {
        return accumulatedDialogAbortThroughput;
    }

    public LongValueStats getDialogConcurrency() {
        return dialogConcurrency;
    }

    public LatencyElement.Result getFinishedDialogLatency() {
        return finishedDialogLatency;
    }

    public LatencyElement.Result getAbortedDialogLatency() {
        return abortedDialogLatency;
    }

    public LatencyElement.Result getPingLatency() {
        return pingLatency;
    }

    public Map<DialogEvent.Span, LatencyElement.Result>
        getDialogLatencyBreakdown()
    {
        return dialogLatencyBreakdown;
    }

    public List<DialogInfoPerf> getDialogInfoRecords() {
        return dialogInfoRecords;
    }

    /* Methods to return other metrics objects. */

    /**
     * Returns an EndpointMetrics representation.
     * */
    @SuppressWarnings("deprecation")
    public oracle.kv.stats.EndpointMetrics getEndpointMetrics() {
        return new oracle.kv.stats.EndpointMetrics(){
            @Override
            public String getName() {
                return name;
            }
            @Override
            public double getDialogStartThroughput() {
                return DialogEndpointPerf.this
                    .getDialogStartThroughput().getThroughputPerSecond();
            }
            @Override
            public double getDialogDropThroughput() {
                return DialogEndpointPerf.this
                    .getDialogAbortThroughput().getThroughputPerSecond();
            }
            @Override
            public double getDialogFinishThroughput() {
                return DialogEndpointPerf.this
                    .getDialogFinishThroughput().getThroughputPerSecond();
            }
            @Override
            public double getDialogAbortThroughput() {
                return DialogEndpointPerf.this
                    .getDialogAbortThroughput().getThroughputPerSecond();
            }
            @Override
            public oracle.kv.stats.MetricStats
                getFinishedDialogLatencyNanos()
            {
                return new MetricStatsImpl(
                    DialogEndpointPerf.this.getFinishedDialogLatency());
            }
            @Override
            public oracle.kv.stats.MetricStats
                getAbortedDialogLatencyNanos()
            {
                return new MetricStatsImpl(
                    DialogEndpointPerf.this.getAbortedDialogLatency());
            }
            @Override
            public oracle.kv.stats.MetricStats getDialogConcurrency() {
                return new MetricStatsImpl(
                    DialogEndpointPerf.this.getDialogConcurrency());
            }
            @Override
            public Map<String, oracle.kv.stats.MetricStats>
                getEventLatencyNanosMap()
            {
                return DialogEndpointPerf.this.dialogLatencyBreakdown
                    .entrySet().stream()
                    .collect(Collectors.toMap(
                        (e) -> e.getKey().toString(),
                        (e) -> new MetricStatsImpl(e.getValue())));
            }
        };
    }
}
