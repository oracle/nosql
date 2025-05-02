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

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;
import oracle.nosql.common.sklogger.measure.LongValue;
import oracle.nosql.common.sklogger.measure.LongValueField;

/**
 * A collection of NioChannelExecutor perf metrics.
 */
public class NioChannelExecutorPerf extends ObjectNodeSerializable {

    public static final int DEFAULT_ID = -1;

    /** The field map. */
    public static final Map<String, Field<?, NioChannelExecutorPerf>> FIELDS =
        new HashMap<>();
    /** The ID of the channel executor. The ID is assigned by a sequencer. */
    public static final String CHILD_ID =
        IntegerField.create(
            FIELDS, "childId",
            NioChannelExecutorPerf::getChildId, DEFAULT_ID);
    /**
     * The index of the channel executor. Each channel executor occupies a
     * position in a fixed-size array inside the thread pool.
     */
    public static final String CHILD_INDEX =
        IntegerField.create(
            FIELDS, "childIndex",
            NioChannelExecutorPerf::getChildIndex, DEFAULT_ID);
    /**
     * The idle time in nanos, that is, the time the channel executor spent in
     * the select loop during the last period.
     */
    public static final String IDLE_TIME_NANOS =
        LongValueField.create(
            FIELDS, "idleTimeNanos",
            NioChannelExecutorPerf::getIdleTimeNanos);
    /**
     * The io process time in nanos, that is, the time the channel executor
     * spent in read/write from/to channels and handling the callbacks during
     * the last period.
     */
    public static final String IO_PROCESS_TIME_NANOS =
        LongValueField.create(
            FIELDS, "ioProcessTimeNanos",
            NioChannelExecutorPerf::getIOProcessTimeNanos);
    /**
     * The task process time in nanos, that is, the time the channel executor
     * spent in executing submitted tasks during the last period.
     */
    public static final String TASK_PROCESS_TIME_NANOS =
        LongValueField.create(
            FIELDS, "taskProcessTimeNanos",
            NioChannelExecutorPerf::getTaskProcessTimeNanos);
    /** The number of tasks executed during the last period. */
    public static final String NUM_TASKS_EXECUTED =
        LongValueField.create(
            FIELDS, "numTasksExecuted",
            NioChannelExecutorPerf::getNumTasksExecuted);
    /** The number of immediate tasks submitted during the last period. */
    public static final String NUM_IMMEDIATE_TASKS_SUBMITTED =
        LongValueField.create(
            FIELDS, "numImmediateTasksSubmitted",
            NioChannelExecutorPerf::getNumImmediateTasksSubmitted);
    /** The number of delayed tasks submitted during the last period. */
    public static final String NUM_DELAYED_TASKS_SUBMITTED =
        LongValueField.create(
            FIELDS, "numDelayedTasksSubmitted",
            NioChannelExecutorPerf::getNumDelayedTasksSubmitted);
    /**
     * The delay in nano-seconds since the last time the executor reports
     * responsiveness.
     */
    public static final String RESPONSIVENESS_DELAY =
        LongField.create(
            FIELDS, "responsivenessDelayNanos",
            NioChannelExecutorPerf::getResponsivenessDelayNanos);
    /**
     * The time in nano-seconds since the start of the executor reports a
     * quiescent period. An executor will be killed if the quiescent period is
     * long enough.
     */
    public static final String QUIESCENT_TIME =
        LongField.create(
            FIELDS, "quiescentTimeNanos",
            NioChannelExecutorPerf::getQuiescentTimeNanos);
    /** The endpoint handler perf metrics. */
    public static final String HANDLER_PERFS =
        FilteredListField.create(
            FIELDS, "handlerPerfs",
            NioChannelExecutorPerf::getHandlerPerfs,
            NioEndpointHandlerPerf::new);
    /** The default. */
    public static final NioChannelExecutorPerf DEFAULT =
        new NioChannelExecutorPerf(
            DEFAULT_ID,
            DEFAULT_ID,
            LongValue.DEFAULT,
            LongValue.DEFAULT,
            LongValue.DEFAULT,
            LongValue.DEFAULT,
            LongValue.DEFAULT,
            LongValue.DEFAULT,
            LongField.DEFAULT,
            LongField.DEFAULT,
            FilteredListField.getDefault());

    /* Fields */

    private final int childId;
    private final int childIndex;
    private final LongValue idleTimeNanos;
    private final LongValue ioProcessTimeNanos;
    private final LongValue taskProcessTimeNanos;
    private final LongValue numTasksExecuted;
    private final LongValue numImmediateTasksSubmitted;
    private final LongValue numDelayedTasksSubmitted;
    private final long responsivenessDelayNanos;
    private final long quiescentTimeNanos;
    private final List<NioEndpointHandlerPerf> handlerPerfs;

    public NioChannelExecutorPerf(
        int childId,
        int childIndex,
        LongValue idleTimeNanos,
        LongValue ioProcessTimeNanos,
        LongValue taskProcessTimeNanos,
        LongValue numTasksExecuted,
        LongValue numImmediateTasksSubmitted,
        LongValue numDelayedTasksSubmitted,
        long responsivenessDelayNanos,
        long quiescentTimeNanos,
        List<NioEndpointHandlerPerf> handlerPerfs)
    {
        this.childId = childId;
        this.childIndex = childIndex;
        this.idleTimeNanos = idleTimeNanos;
        this.ioProcessTimeNanos = ioProcessTimeNanos;
        this.taskProcessTimeNanos = taskProcessTimeNanos;
        this.numTasksExecuted = numTasksExecuted;
        this.numImmediateTasksSubmitted = numImmediateTasksSubmitted;
        this.numDelayedTasksSubmitted = numDelayedTasksSubmitted;
        this.responsivenessDelayNanos = responsivenessDelayNanos;
        this.quiescentTimeNanos = quiescentTimeNanos;
        this.handlerPerfs = handlerPerfs;
    }

    public NioChannelExecutorPerf(JsonNode payload) {
        this(readField(FIELDS, payload, CHILD_ID),
             readField(FIELDS, payload, CHILD_INDEX),
             readField(FIELDS, payload, IDLE_TIME_NANOS),
             readField(FIELDS, payload, IO_PROCESS_TIME_NANOS),
             readField(FIELDS, payload, TASK_PROCESS_TIME_NANOS),
             readField(FIELDS, payload, NUM_TASKS_EXECUTED),
             readField(FIELDS, payload, NUM_IMMEDIATE_TASKS_SUBMITTED),
             readField(FIELDS, payload, NUM_DELAYED_TASKS_SUBMITTED),
             readField(FIELDS, payload, RESPONSIVENESS_DELAY),
             readField(FIELDS, payload, QUIESCENT_TIME),
             readField(FIELDS, payload, HANDLER_PERFS));
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }

    /* Getters */

    public int getChildId() {
        return childId;
    }

    public int getChildIndex() {
        return childIndex;
    }

    public LongValue getIdleTimeNanos() {
        return idleTimeNanos;
    }

    public LongValue getIOProcessTimeNanos() {
        return ioProcessTimeNanos;
    }

    public LongValue getTaskProcessTimeNanos() {
        return taskProcessTimeNanos;
    }

    public LongValue getNumTasksExecuted() {
        return numTasksExecuted;
    }

    public LongValue getNumImmediateTasksSubmitted() {
        return numImmediateTasksSubmitted;
    }

    public LongValue getNumDelayedTasksSubmitted() {
        return numDelayedTasksSubmitted;
    }

    public long getResponsivenessDelayNanos() {
        return responsivenessDelayNanos;
    }

    public long getQuiescentTimeNanos() {
        return quiescentTimeNanos;
    }

    public List<NioEndpointHandlerPerf> getHandlerPerfs() {
        return handlerPerfs;
    }
}

