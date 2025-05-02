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
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;

/**
 * Summarized dialog perf metrics associated with the endpoint group.
 */
public class DialogEndpointSummarizedPerf extends ObjectNodeSerializable {

    /** The default. */
    public static final DialogEndpointSummarizedPerf DEFAULT =
        new DialogEndpointSummarizedPerf(
            Collections.emptyMap(),
            Collections.emptyMap());
    /** The field map. */
    public static final Map<String, Field<?, DialogEndpointSummarizedPerf>>
        FIELDS = new HashMap<>();
    /**
     * The summary of perf metrics of creator endpoints, including
     * min/avg/mean/max of various throughput values and min/avg/mean/max of
     * various latency values.
     */
    public static final String CREATOR_SUMMARY =
        ObjectNodeField.create(
            FIELDS, "creatorSummary", (p) -> p.creatorSummary);
    /**
     * The summary of perf metrics of responder endpoints, including
     * min/avg/mean/max of various throughput values and min/avg/mean/max of
     * various latency values.
     */
    public static final String RESPONDER_SUMMARY =
        ObjectNodeField.create(
            FIELDS, "responderSummary", (p) -> p.responderSummary);
    /** The field name for number of endpoints. */
    public static final String NUM_ENDPINTS = "numEndpoints";
    /** The field name for dialog throughput. */
    public static final String THROUGHPUT = "throughputPerSecond";
    /** The field name for dialog start throughtput summary. */
    public static final String DIALOG_START = "dialogStart";
    /** The field name for dialog drop throughtput summary. */
    public static final String DIALOG_DROP = "dialogDrop";
    /** The field name for dialog finish throughtput summary. */
    public static final String DIALOG_FINISH = "dialogFinish";
    /** The field name for dialog finish throughtput summary. */
    public static final String DIALOG_ABORT = "dialogAbort";
    /** The field name for dialog latency. */
    public static final String LATENCY = "latencyNanos";
    /** The field name for average finished dialog latency summary. */
    public static final String FINISHED_DIALOG_AVG = "finishedDialogAvg";
    /** The field name for 95th finished dialog latency summary. */
    public static final String FINISHED_DIALOG_95TH = "finishedDialog95th";
    /** The field name for average aborted dialog latency summary. */
    public static final String ABORTED_DIALOG_AVG = "abortedDialogAvg";
    /** The field name for 95th aborted dialog latency summary. */
    public static final String ABORTED_DIALOG_95TH = "abortedDialog95th";
    /** The field name for average dialog concurrency summary. */
    public static final String DIALOG_CONCURRENCY_AVG = "dialogConcurrencyAvg";
    /** The field name for 95th dialog concurrency summary. */
    public static final String DIALOG_CONCURRENCY_95TH =
        "dialogConcurrency95th";


    private final ObjectNode creatorSummary;
    private final ObjectNode responderSummary;

    public DialogEndpointSummarizedPerf(
        Map<String, DialogEndpointPerf> creatorPerfs,
        Map<String, DialogEndpointPerf> responderPerfs)
    {
        this(summarizePerfs(creatorPerfs),
             summarizePerfs(responderPerfs));
    }

    private static ObjectNode summarizePerfs(
        Map<String, DialogEndpointPerf> perfs)
    {
        final ObjectNode payload = JsonUtils.createObjectNode();
        if (perfs.isEmpty()) {
            return payload;
        }
        payload.put(NUM_ENDPINTS, perfs.size());
        summarizeThroughput(payload, perfs);
        summarizeLatency(payload, perfs);
        summarizeConcurrency(payload, perfs);
        return payload;
    }

    private static void summarizeThroughput(
        ObjectNode payload,
        Map<String, DialogEndpointPerf> perfs)
    {
        final ObjectNode result = JsonUtils.createObjectNode();
        addThroughputSummary(
            result, perfs, DIALOG_START,
            (p) -> p.getDialogStartThroughput().getThroughputPerSecond());
        addThroughputSummary(
            result, perfs, DIALOG_DROP,
            (p) -> p.getDialogDropThroughput().getThroughputPerSecond());
        addThroughputSummary(
            result, perfs, DIALOG_FINISH,
            (p) -> p.getDialogFinishThroughput().getThroughputPerSecond());
        addThroughputSummary(
            result, perfs, DIALOG_ABORT,
            (p) -> p.getDialogAbortThroughput().getThroughputPerSecond());
        payload.put(THROUGHPUT, result);
    }

    private static void addThroughputSummary(
        ObjectNode obj,
        Map<String, DialogEndpointPerf> perfs,
        String name,
        ToDoubleFunction<DialogEndpointPerf> mapper)
    {
        PerfUtil.addSummary(
            obj, name,
            perfs.values().stream().mapToDouble(mapper));
    }

    private static void summarizeLatency(
        ObjectNode payload,
        Map<String, DialogEndpointPerf> perfs)
    {
        final ObjectNode result = JsonUtils.createObjectNode();
        addLongValueStatsSummary(
            result, perfs, FINISHED_DIALOG_AVG,
            (p) -> p.getFinishedDialogLatency().getOperationCount() > 0,
            (p) -> p.getFinishedDialogLatency().getAverage());
        addLongValueStatsSummary(
            result, perfs, FINISHED_DIALOG_95TH,
            (p) -> p.getFinishedDialogLatency().getOperationCount() > 0,
            (p) -> p.getFinishedDialogLatency().getPercent95());
        addLongValueStatsSummary(
            result, perfs, ABORTED_DIALOG_AVG,
            (p) -> p.getAbortedDialogLatency().getOperationCount() > 0,
            (p) -> p.getAbortedDialogLatency().getAverage());
        addLongValueStatsSummary(
            result, perfs, ABORTED_DIALOG_95TH,
            (p) -> p.getAbortedDialogLatency().getOperationCount() > 0,
            (p) -> p.getAbortedDialogLatency().getPercent95());
        payload.put(LATENCY, result);
    }

    private static void addLongValueStatsSummary(
        ObjectNode obj,
        Map<String, DialogEndpointPerf> perfs,
        String name,
        Predicate<DialogEndpointPerf> filter,
        ToLongFunction<DialogEndpointPerf> mapper)
    {
        PerfUtil.addSummary(
            obj, name,
            perfs.values().stream().filter(filter).mapToLong(mapper));
    }

    private static void summarizeConcurrency(
        ObjectNode payload,
        Map<String, DialogEndpointPerf> perfs)
    {
        addLongValueStatsSummary(
            payload, perfs, DIALOG_CONCURRENCY_AVG,
            (p) -> p.getDialogConcurrency().getCount() > 0,
            (p) -> p.getDialogConcurrency().getAverage());
        addLongValueStatsSummary(
            payload, perfs, DIALOG_CONCURRENCY_95TH,
            (p) -> p.getDialogConcurrency().getCount() > 0,
            (p) -> p.getDialogConcurrency().getPercent95());
    }

    /**
     * Creates the {@code DialogEndpointSummarizedPerf} from a json payload. This is
     * used to de-serialize this object.
     */
    public DialogEndpointSummarizedPerf(JsonNode payload) {
        this((ObjectNode) readField(FIELDS, payload, CREATOR_SUMMARY),
             (ObjectNode) readField(FIELDS, payload, RESPONDER_SUMMARY));
    }

    private DialogEndpointSummarizedPerf(
        ObjectNode creatorSummary,
        ObjectNode responderSummary)
    {
        this.creatorSummary = creatorSummary;
        this.responderSummary = responderSummary;
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }
}
