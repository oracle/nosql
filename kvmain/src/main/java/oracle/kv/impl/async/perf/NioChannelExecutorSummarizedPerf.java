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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;

/**
 * Summarized channel executor perf metrics.
 */
public class NioChannelExecutorSummarizedPerf extends ObjectNodeSerializable {

    /** The field map. */
    public static final Map<String, Field<?, NioChannelExecutorSummarizedPerf>>
        FIELDS = new HashMap<>();
    /** The number of alive executors. */
    public static final String NUM_ALIVE =
        IntegerField.create(FIELDS, "numAlive", (p) -> p.numAlive);
    /**
     * The summary of responsiveness delay among the nio channel executors,
     * summarizing min/avg/mean/max among alive executors.
     */
    public static final String RESPONSIVENESS_DELAY =
        ObjectNodeField.create(
            FIELDS, "responsivenessDelaySeconds",
            (p) -> p.responsivenessDelaySeconds);
    /**
     * The summary of time metrics of nio channel executors in nanoseconds
     * including idle, io processing and task processing time summarizing
     * min/avg/mean/max among the executors.
     */
    public static final String EXECUTOR_TIME =
        ObjectNodeField.create(
            FIELDS, "executorTimeNanos",
            (p) -> p.executorTimeSummary);
    /** The field name for idle time. */
    public static final String IDLE_TIME = "idle";
    /** The field name for io processing time. */
    public static final String IO_TIME = "io";
    /** The field name for task processing time. */
    public static final String TASK_TIME = "task";
    /**
     * The summary of number of tasks metrics of nio channel executors including
     * executed tasks, submitted immediate tasks and submitted delayed tasks
     * summarizing min/avg/mean/max among the executors.
     */
    public static final String EXECUTOR_NUM_TASKS =
        ObjectNodeField.create(
            FIELDS, "executorNumTasks",
            (p) -> p.executorNumTasksSummary);
    /** The field name for number of executed tasks. */
    public static final String EXECUTED = "executed";
    /** The field name for number of submitted immediate tasks. */
    public static final String SUBMITTED_IMMEDIATE = "submittedImmediate";
    /** The field name for number of submitted delayed tasks. */
    public static final String SUBMITTED_DELAYED = "submittedDelayed";
    /**
     * The summary of handler throughput metrics of endpoint handlers in all the
     * nio channel executors including handler and channel read/write throughput
     * summarizing min/avg/mean/max among the handlers.
     */
    public static final String HANDLER_THROUGHPUT =
        ObjectNodeField.create(
            FIELDS, "handlerThroughputPerSecond",
            (p) -> p.handlerThroughputSummary);
    /** The field name for handler read throughput. */
    public static final String HANDLER_READ = "handlerRead";
    /** The field name for handler write throughput. */
    public static final String HANDLER_WRITE = "handlerWrite";
    /** The field name for channel read throughput. */
    public static final String CHANNEL_READ = "channelRead";
    /** The field name for channel write throughput. */
    public static final String CHANNEL_WRITE = "channelWrite";
    /** The field name for channel bytes read throughput. */
    public static final String CHANNEL_BYTES_READ = "channelBytesRead";
    /** The field name for channel bytes write throughput. */
    public static final String CHANNEL_BYTES_WRITE = "channelBytesWrite";
    /**
     * The summary of handler latency metrics of endpoint handlers in all the
     * nio channel executors including average and 95th handler and channel
     * read/write latency summarizing min/avg/mean/max among the handlers.
     */
    public static final String HANDLER_LATENCY =
        ObjectNodeField.create(
            FIELDS, "handlerLatencyNanos",
            (p) -> p.handlerLatencySummary);
    /** The field name for average handler read latency. */
    public static final String AVG_HANDLER_READ = "avgHandlerRead";
    /** The field name for 95th handler read latency. */
    public static final String P95_HANDLER_READ = "95thHandlerRead";
    /** The field name for average handler write latency. */
    public static final String AVG_HANDLER_WRITE = "avgHandlerWrite";
    /** The field name for 95th handler write latency. */
    public static final String P95_HANDLER_WRITE = "95thHandlerWrite";
    /** The field name for average channel read latency. */
    public static final String AVG_CHANNEL_READ = "avgChannelRead";
    /** The field name for 95th channel read latency. */
    public static final String P95_CHANNEL_READ = "95thChannelRead";
    /** The field name for average channel write latency. */
    public static final String AVG_CHANNEL_WRITE = "avgChannelWrite";
    /** The field name for 95th channel write latency. */
    public static final String P95_CHANNEL_WRITE = "95thChannelWrite";

    /* Fields */

    private final int numAlive;
    private final ObjectNode responsivenessDelaySeconds;
    private final ObjectNode executorTimeSummary;
    private final ObjectNode executorNumTasksSummary;
    private final ObjectNode handlerThroughputSummary;
    private final ObjectNode handlerLatencySummary;

    public NioChannelExecutorSummarizedPerf(
        List<NioChannelExecutorPerf> perfs)
    {
        this(
            perfs.size(),
            PerfUtil.summarize(
                perfs.stream().mapToLong(
                    (p) ->
                    TimeUnit.NANOSECONDS.toSeconds(
                        p.getResponsivenessDelayNanos()))),
            summarizeExecutorTime(perfs),
            summarizeExecutorNumTasks(perfs),
            summarizeHandlerThroughput(perfs),
            summarizeHandlerLatency(perfs));
    }

    private static ObjectNode summarizeExecutorTime(
        List<NioChannelExecutorPerf> perfs)
    {
        final ObjectNode result = JsonUtils.createObjectNode();
        addSummarizedValue(
            result, perfs, IDLE_TIME,
            (p) -> p.getIdleTimeNanos().get());
        addSummarizedValue(
            result, perfs, IO_TIME,
            (p) -> p.getIOProcessTimeNanos().get());
        addSummarizedValue(
            result, perfs, TASK_TIME,
            (p) -> p.getTaskProcessTimeNanos().get());
        return result;
    }

    private static void addSummarizedValue(
        ObjectNode obj,
        List<NioChannelExecutorPerf> perfs,
        String name,
        ToLongFunction<NioChannelExecutorPerf> mapper)
    {
        PerfUtil.addSummary(obj, name, perfs.stream().mapToLong(mapper));
    }

    private static ObjectNode summarizeExecutorNumTasks(
        List<NioChannelExecutorPerf> perfs)
    {
        final ObjectNode result = JsonUtils.createObjectNode();
        addSummarizedValue(
            result, perfs, EXECUTED,
            (p) -> p.getNumTasksExecuted().get());
        addSummarizedValue(
            result, perfs, SUBMITTED_IMMEDIATE,
            (p) -> p.getNumImmediateTasksSubmitted().get());
        addSummarizedValue(
            result, perfs, SUBMITTED_DELAYED,
            (p) -> p.getNumDelayedTasksSubmitted().get());
        return result;
    }

    private static ObjectNode summarizeHandlerThroughput(
        List<NioChannelExecutorPerf> perfs)
    {
        final ObjectNode result = JsonUtils.createObjectNode();
        addHandlerThroughput(
            result, perfs, HANDLER_READ,
            (p) -> p.getHandlerReadThroughput().getThroughputPerSecond());
        addHandlerThroughput(
            result, perfs, HANDLER_WRITE,
            (p) -> p.getHandlerWriteThroughput().getThroughputPerSecond());
        addHandlerThroughput(
            result, perfs, CHANNEL_READ,
            (p) -> p.getChannelReadThroughput().getThroughputPerSecond());
        addHandlerThroughput(
            result, perfs, CHANNEL_WRITE,
            (p) -> p.getChannelWriteThroughput().getThroughputPerSecond());
        addHandlerThroughput(
            result, perfs, CHANNEL_BYTES_READ,
            (p) ->
            p.getChannelBytesReadThroughput().getThroughputPerSecond());
        addHandlerThroughput(
            result, perfs, CHANNEL_BYTES_WRITE,
            (p) ->
            p.getChannelBytesWriteThroughput().getThroughputPerSecond());
        return result;
    }

    private static void addHandlerThroughput(
        ObjectNode obj,
        List<NioChannelExecutorPerf> perfs,
        String name,
        ToDoubleFunction<NioEndpointHandlerPerf> mapper)
    {
        PerfUtil.addSummary(
            obj, name,
            perfs.stream().flatMap((p) -> p.getHandlerPerfs().stream())
            .filter((p) -> !p.isDefault())
            .mapToDouble(mapper));
    }

    private static ObjectNode summarizeHandlerLatency(
        List<NioChannelExecutorPerf> perfs)
    {
        final ObjectNode result = JsonUtils.createObjectNode();
        addHandlerLatency(
            result, perfs, AVG_HANDLER_READ,
            (p) -> p.getHandlerReadLatency().getOperationCount() > 0,
            (p) -> p.getHandlerReadLatency().getAverage());
        addHandlerLatency(
            result, perfs, P95_HANDLER_READ,
            (p) -> p.getHandlerReadLatency().getOperationCount() > 0,
            (p) -> p.getHandlerReadLatency().getPercent95());
        addHandlerLatency(
            result, perfs, AVG_HANDLER_WRITE,
            (p) -> p.getHandlerWriteLatency().getOperationCount() > 0,
            (p) -> p.getHandlerWriteLatency().getAverage());
        addHandlerLatency(
            result, perfs, P95_HANDLER_WRITE,
            (p) -> p.getHandlerWriteLatency().getOperationCount() > 0,
            (p) -> p.getHandlerWriteLatency().getPercent95());
        addHandlerLatency(
            result, perfs, AVG_CHANNEL_READ,
            (p) -> p.getChannelReadLatency().getOperationCount() > 0,
            (p) -> p.getChannelReadLatency().getAverage());
        addHandlerLatency(
            result, perfs, P95_CHANNEL_READ,
            (p) -> p.getChannelReadLatency().getOperationCount() > 0,
            (p) -> p.getChannelReadLatency().getPercent95());
        addHandlerLatency(
            result, perfs, AVG_CHANNEL_WRITE,
            (p) -> p.getChannelWriteLatency().getOperationCount() > 0,
            (p) -> p.getChannelWriteLatency().getAverage());
        addHandlerLatency(
            result, perfs, P95_CHANNEL_WRITE,
            (p) -> p.getChannelWriteLatency().getOperationCount() > 0,
            (p) -> p.getChannelWriteLatency().getPercent95());
        return result;
    }

    private static void addHandlerLatency(
        ObjectNode obj,
        List<NioChannelExecutorPerf> perfs,
        String name,
        Predicate<NioEndpointHandlerPerf> filter,
        ToLongFunction<NioEndpointHandlerPerf> mapper)
    {
        PerfUtil.addSummary(
            obj, name,
            perfs.stream().flatMap((p) -> p.getHandlerPerfs().stream())
            .filter(filter)
            .mapToLong(mapper));
    }

    public NioChannelExecutorSummarizedPerf(ObjectNode payload) {
        this(readField(FIELDS, payload, NUM_ALIVE),
             readField(FIELDS, payload, RESPONSIVENESS_DELAY),
             readField(FIELDS, payload, EXECUTOR_TIME),
             readField(FIELDS, payload, EXECUTOR_NUM_TASKS),
             readField(FIELDS, payload, HANDLER_THROUGHPUT),
             readField(FIELDS, payload, HANDLER_LATENCY));
    }

    /** Private constructor. */
    private NioChannelExecutorSummarizedPerf(
        int numAlive,
        ObjectNode responsivenessDelayNanos,
        ObjectNode executorTimeSummary,
        ObjectNode executorNumTasksSummary,
        ObjectNode handlerThroughputSummary,
        ObjectNode handlerLatencySummary)
    {
        this.numAlive = numAlive;
        this.responsivenessDelaySeconds = responsivenessDelayNanos;
        this.executorTimeSummary = executorTimeSummary;
        this.executorNumTasksSummary = executorNumTasksSummary;
        this.handlerThroughputSummary = handlerThroughputSummary;
        this.handlerLatencySummary = handlerLatencySummary;
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }
}
