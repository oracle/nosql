/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.async.perf;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import oracle.kv.TestBase;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable.BooleanField;
import oracle.nosql.common.jss.ObjectNodeSerializable.LongField;
import oracle.nosql.common.jss.ObjectNodeSerializable.StringField;
import oracle.nosql.common.sklogger.measure.LatencyElement;
import oracle.nosql.common.sklogger.measure.LongValue;
import oracle.nosql.common.sklogger.measure.LongValueStats;
import oracle.nosql.common.sklogger.measure.ThroughputElement;

import org.junit.Test;

/**
 * Tests the serializations of the PerfSerializable objects.
 */
public class PerfSerializationTest extends TestBase {

    private final long ONE_SECOND_IN_NANOS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Tests DialogEndpointGroupPerf.
     */
    @Test
    public void testDialogEndpointGroupPerf() throws Exception {
        final DialogEndpointGroupPerf perf =
            createDummyDialogEndpointGroupPerf();
        assertEquals(perf, new DialogEndpointGroupPerf(perf.toJson()));
    }

    private DialogEndpointGroupPerf createDummyDialogEndpointGroupPerf() {
        final long intervalStart = System.nanoTime();
        final long intervalEnd = System.nanoTime();
        final Map<String, DialogEndpointPerf> creatorPerfs = new HashMap<>();
        creatorPerfs.put(
            "creator", createDummyDialogEndpointPerf("creator", true));
        final Map<String, DialogEndpointPerf> responderPerfs = new HashMap<>();
        responderPerfs.put(
            "responder", createDummyDialogEndpointPerf("responder", true));
        final DialogResourceManagerPerf resourceManagerPerf =
            createDummyDialogResourceManagerPerf();
        final IOBufferPoolUsagePerf ioBufferPoolUsagePerf =
            createDummyIOBufferPoolUsagePerf();
        return new DialogEndpointGroupPerf(
            intervalStart, intervalEnd,
            creatorPerfs, responderPerfs,
            resourceManagerPerf, ioBufferPoolUsagePerf);
    }

    /**
     * Tests automatically filtering of DialogEndpointGroupPerf.
     *
     * The perf itself is never empty, but we can test the empty endpoint
     * perfs.
     */
    @Test
    public void testDialogEndpointGroupPerfEmpty() throws Exception {
        final DialogEndpointGroupPerf perf =
            createEmptyDialogEndpointGroupPerf();
        final ObjectNode perfJson = perf.toJson();
        assertEquals(perfJson.toString(), false, perf.isDefault());
        /*
         * Verifies that there are three fields: intervalStart, intervalEnd and
         * value.
         */
        assertEquals(perfJson.toString(), 3, perfJson.size());
        /*
         * Verifies that there are three fields: creatorPerfs, responderPerfs
         * and resourceManagerPerf.
         */
        assertEquals(perfJson.toString(), 3,
                     perfJson.get(DialogEndpointGroupPerf.VALUE).asObject()
                     .size());
        /* Verifies that each perfs has one value. */
        assertEquals(perfJson.toString(), 1,
                     perfJson
                     .get(DialogEndpointGroupPerf.VALUE)
                     .get(DialogEndpointGroupPerf.PerfValue.CREATOR_PERFS)
                     .asObject()
                     .size());
        assertEquals(perfJson.toString(), 1,
                     perfJson
                     .get(DialogEndpointGroupPerf.VALUE)
                     .get(DialogEndpointGroupPerf.PerfValue.RESPONDER_PERFS)
                     .asObject()
                     .size());
        /* Verifies that each endpoint perfs has one field: name. */
        assertEquals(perfJson.toString(), 2,
                     perfJson
                     .get(DialogEndpointGroupPerf.VALUE)
                     .get(DialogEndpointGroupPerf.PerfValue.CREATOR_PERFS)
                     .get("creator")
                     .asObject()
                     .size());
        assertEquals(perfJson.toString(), 1,
                     perfJson
                     .get(DialogEndpointGroupPerf.VALUE)
                     .get(DialogEndpointGroupPerf.PerfValue.RESPONDER_PERFS)
                     .get("responder")
                     .asObject()
                     .size());
        assertEquals(perf, new DialogEndpointGroupPerf(perf.toJson()));
    }

    private DialogEndpointGroupPerf createEmptyDialogEndpointGroupPerf() {
        final long intervalStart = System.nanoTime();
        final long intervalEnd = System.nanoTime();
        final Map<String, DialogEndpointPerf> creatorPerfs = new HashMap<>();
        creatorPerfs.put(
            "creator", createEmptyDialogEndpointPerf("creator", true));
        final Map<String, DialogEndpointPerf> responderPerfs = new HashMap<>();
        responderPerfs.put(
            "responder", createEmptyDialogEndpointPerf("responder", false));
        final DialogResourceManagerPerf resourceManagerPerf =
            createDummyDialogResourceManagerPerf();
        final IOBufferPoolUsagePerf ioBufferPoolUsagePerf =
            createEmptyIOBufferPoolUsagePerf();
        return new DialogEndpointGroupPerf(
            intervalStart, intervalEnd,
            creatorPerfs, responderPerfs,
            resourceManagerPerf, ioBufferPoolUsagePerf);
    }

    /**
     * Tests DialogEndpointPerf.
     */
    @Test
    public void testDialogEndpointPerf() throws Exception {
        final String name = "name";
        final boolean isCreator = false;
        final DialogEndpointPerf perf =
            createDummyDialogEndpointPerf(name, isCreator);
        assertEquals(perf, new DialogEndpointPerf(perf.toJson()));
    }

    private DialogEndpointPerf createDummyDialogEndpointPerf(
        String name, boolean isCreator)
    {
        final ThroughputElement.Result dialogStartThroughput =
            new ThroughputElement.Result(1, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result dialogDropThroughput =
            new ThroughputElement.Result(2, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result dialogFinishThroughput =
            new ThroughputElement.Result(3, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result dialogAbortThroughput =
            new ThroughputElement.Result(4, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result accumulatedDialogStartThroughput =
            new ThroughputElement.Result(5, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result accumulatedDialogDropThroughput =
            new ThroughputElement.Result(6, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result accumulatedDialogFinishThroughput =
            new ThroughputElement.Result(7, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result accumulatedDialogAbortThroughput =
            new ThroughputElement.Result(8, ONE_SECOND_IN_NANOS);
        final LongValueStats dialogConcurrency =
            new LongValueStats(1, 1, 1, 1, 1, 1, 0);
        final LatencyElement.Result finishedDialogLatency =
            new LatencyElement.Result(2, 2, 2, 2, 2, 2, 2, 0);
        final LatencyElement.Result abortedDialogLatency =
            new LatencyElement.Result(3, 3, 3, 3, 3, 3, 3, 0);
        final LatencyElement.Result pingLatency =
            new LatencyElement.Result(4, 4, 4, 4, 4, 4, 4, 0);
        final Map<DialogEvent.Span, LatencyElement.Result>
            dialogLatencyBreakdown = new HashMap<>();
        dialogLatencyBreakdown.put(
            DialogEvent.Span.create("INIT-WRITE"),
            new LatencyElement.Result(1, 1, 1, 1, 1, 1, 1, 0));
        final List<DialogInfoPerf> dialogInfoRecords = new ArrayList<>();
        dialogInfoRecords.add(createDummyDialogInfoPerf("id"));
        return new DialogEndpointPerf(
            name, isCreator,
            dialogStartThroughput, dialogDropThroughput,
            dialogFinishThroughput, dialogAbortThroughput,
            accumulatedDialogStartThroughput,
            accumulatedDialogDropThroughput,
            accumulatedDialogFinishThroughput,
            accumulatedDialogAbortThroughput,
            dialogConcurrency,
            finishedDialogLatency, abortedDialogLatency, pingLatency,
            dialogLatencyBreakdown, dialogInfoRecords);
    }

    /**
     * Tests automatically fitlering of DialogEndpointPerf.
     */
    @Test
    public void testDialogEndpointPerfEmpty() throws Exception {
        final String name = "name";
        final boolean isCreator = true;
        final DialogEndpointPerf perf =
            createEmptyDialogEndpointPerf(name, isCreator);
        final ObjectNode perfJson = perf.toJson();
        assertEquals(perfJson.toString(), false, perf.isDefault());
        assertEquals(perfJson.toString(), 2, perfJson.size());
        assertEquals(perf, new DialogEndpointPerf(perfJson));
    }

    private DialogEndpointPerf createEmptyDialogEndpointPerf(
        String name, boolean isCreator)
    {
        final Map<DialogEvent.Span, LatencyElement.Result>
            dialogLatencyBreakdown = new HashMap<>();
        dialogLatencyBreakdown.put(
            DialogEvent.Span.create("INIT-WRITE"),
            LatencyElement.Result.DEFAULT);
        final List<DialogInfoPerf> emptyDialogInfoRecords = new ArrayList<>();
        emptyDialogInfoRecords.add(createEmptyDialogInfoPerf());
        return new DialogEndpointPerf(
            name, isCreator,
            ThroughputElement.Result.DEFAULT, ThroughputElement.Result.DEFAULT,
            ThroughputElement.Result.DEFAULT, ThroughputElement.Result.DEFAULT,
            ThroughputElement.Result.DEFAULT, ThroughputElement.Result.DEFAULT,
            ThroughputElement.Result.DEFAULT, ThroughputElement.Result.DEFAULT,
            LongValueStats.DEFAULT,
            LatencyElement.Result.DEFAULT, LatencyElement.Result.DEFAULT,
            LatencyElement.Result.DEFAULT,
            dialogLatencyBreakdown, emptyDialogInfoRecords);
    }

    /**
     * Tests DialogEndpointSummarizedPerf.
     */
    @Test
    public void testDialogEndpointSummarizedPerf() throws Exception {
        final DialogEndpointSummarizedPerf perf =
            createDummyDialogEndpointSummarizedPerf();
        assertEquals(perf, new DialogEndpointSummarizedPerf(perf.toJson()));
    }

    private DialogEndpointSummarizedPerf
        createDummyDialogEndpointSummarizedPerf()
    {
        final Map<String, DialogEndpointPerf> creatorPerfs = new HashMap<>();
        creatorPerfs.put(
            "creator", createDummyDialogEndpointPerf("creator", true));
        final Map<String, DialogEndpointPerf> responderPerfs = new HashMap<>();
        creatorPerfs.put(
            "responder", createDummyDialogEndpointPerf("responder", false));
        return new DialogEndpointSummarizedPerf(
            creatorPerfs, responderPerfs);
    }

    /**
     * Tests automatically filtering of DialogEndpointSummarizedPerf.
     */
    @Test
    public void testDialogEndpointSummarizedPerfEmpty() throws Exception {
        final DialogEndpointSummarizedPerf perf =
            createEmptyDialogEndpointSummarizedPerf();
        final ObjectNode perfJson = perf.toJson();
        assertEquals(perfJson.toString(), true, perf.isDefault());
        assertEquals(perfJson.toString(), 0, perfJson.size());
        assertEquals(perf, new DialogEndpointSummarizedPerf(perf.toJson()));
    }

    private DialogEndpointSummarizedPerf
        createEmptyDialogEndpointSummarizedPerf()
    {
        final Map<String, DialogEndpointPerf> creatorPerfs = new HashMap<>();
        final Map<String, DialogEndpointPerf> responderPerfs = new HashMap<>();
        return new DialogEndpointSummarizedPerf(
            creatorPerfs, responderPerfs);
    }

    /**
     * Tests DialogInfoPerf.
     */
    @Test
    public void testDialogInfoPerf() throws Exception {
        final String id = "ID";
        final DialogInfoPerf perf = createDummyDialogInfoPerf(id);
        assertEquals(perf, new DialogInfoPerf(perf.toJson()));
    }

    private DialogInfoPerf createDummyDialogInfoPerf(String id) {
        final long outgoingNumMessages = 1;
        final long outgoingNumFrames = 2;
        final long outgoingNumBytes = 3;
        final long incomingNumMessages = 4;
        final long incomingNumFrames = 5;
        final long incomingNumBytes = 6;
        final boolean frameEventsDropped = true;
        final List<DialogEvent.Info> events = new ArrayList<>();
        final DialogEvent.Info event =
            new DialogEvent.Info(DialogEvent.Type.INIT, 1L);
        events.add(event);
        return new DialogInfoPerf(
            id, outgoingNumMessages, outgoingNumFrames, outgoingNumBytes,
            incomingNumMessages, incomingNumFrames, incomingNumBytes,
            frameEventsDropped, events);
    }

    /**
     * Tests automatically filtering of DialogInfoPerf.
     */
    @Test
    public void testDialogInfoPerfEmpty() throws Exception {
        final DialogInfoPerf perf = createEmptyDialogInfoPerf();
        final ObjectNode perfJson = perf.toJson();
        assertEquals(perfJson.toString(), true, perf.isDefault());
        assertEquals(perfJson.toString(), 0, perfJson.size());
        assertEquals(perf, new DialogInfoPerf(perfJson));
    }

    private DialogInfoPerf createEmptyDialogInfoPerf() {
        final String id = StringField.DEFAULT;
        final long outgoingNumMessages = LongField.DEFAULT;
        final long outgoingNumFrames = LongField.DEFAULT;
        final long outgoingNumBytes = LongField.DEFAULT;
        final long incomingNumMessages = LongField.DEFAULT;
        final long incomingNumFrames = LongField.DEFAULT;
        final long incomingNumBytes = LongField.DEFAULT;
        final boolean frameEventsDropped = BooleanField.DEFAULT;
        final List<DialogEvent.Info> events = new ArrayList<>();
        return new DialogInfoPerf(
            id, outgoingNumMessages, outgoingNumFrames, outgoingNumBytes,
            incomingNumMessages, incomingNumFrames, incomingNumBytes,
            frameEventsDropped, events);
    }

    /**
     * Tests DialogResourceManagerPerf.
     */
    @Test
    public void testDialogResourceManagerPerf() throws Exception {
        final DialogResourceManagerPerf perf =
            createDummyDialogResourceManagerPerf();
        assertEquals(perf, new DialogResourceManagerPerf(perf.toJson()));
    }

    private DialogResourceManagerPerf createDummyDialogResourceManagerPerf() {
        final double avgAvailablePermits = 512;
        final double avgAvailablePercentage = 50;
        return new DialogResourceManagerPerf(
            avgAvailablePermits, avgAvailablePercentage);
    }

    /**
     * Tests automatically filtering of DialogResourceManagerPerf.
     */
    @Test
    public void testDialogResourceManagerPerfEmpty() throws Exception {
        /* This perf is never empty. */
    }

    /**
     * Tests IOBufferPoolUsagePerf.
     */
    @Test
    public void testIOBufferPoolUsagePerf() throws Exception {
        final IOBufferPoolUsagePerf perf =
            createDummyIOBufferPoolUsagePerf();
        assertEquals(perf, new IOBufferPoolUsagePerf(perf.toJson()));
    }

    private IOBufferPoolUsagePerf createDummyIOBufferPoolUsagePerf() {
        final LongValueStats channelInputUsePercentage =
            new LongValueStats(1, 1, 1, 1, 1, 1, 0);
        final LongValueStats messageOutputUsePercentage =
            new LongValueStats(2, 2, 2, 2, 2, 2, 0);
        final LongValueStats channelOutputUsePercentage =
            new LongValueStats(3, 3, 3, 3, 3, 3, 0);
        return new IOBufferPoolUsagePerf(
            channelInputUsePercentage, messageOutputUsePercentage,
            channelOutputUsePercentage);
    }

    /**
     * Tests automatically filtering of IOBufferPoolUsagePerf.
     */
    public void testIOBufferPoolUsagePerfEmpty() throws Exception {
        final IOBufferPoolUsagePerf perf =
            createEmptyIOBufferPoolUsagePerf();
        final ObjectNode perfJson = perf.toJson();
        assertEquals(perfJson.toString(), true, perf.isDefault());
        assertEquals(perfJson.toString(), 1, perfJson.size());
        assertEquals(perf, new IOBufferPoolUsagePerf(perfJson));
    }

    private IOBufferPoolUsagePerf createEmptyIOBufferPoolUsagePerf() {
        final LongValueStats channelInputUsePercentage =
            LongValueStats.DEFAULT;
        final LongValueStats messageOutputUsePercentage =
            LongValueStats.DEFAULT;
        final LongValueStats channelOutputUsePercentage =
            LongValueStats.DEFAULT;
        return new IOBufferPoolUsagePerf(
            channelInputUsePercentage, messageOutputUsePercentage,
            channelOutputUsePercentage);
    }

    /**
     * Tests NioChannelExecutorPerf.
     */
    @Test
    public void testNioChannelExecutorPerf() throws Exception {
        final NioChannelExecutorPerf perf =
            createDummyNioChannelExecutorPerf();
        assertEquals(perf, new NioChannelExecutorPerf(perf.toJson()));
    }

    private NioChannelExecutorPerf createDummyNioChannelExecutorPerf() {
        final int childId = 0;
        final int childIndex = 0;
        final LongValue idleTimeNanos = new LongValue(1);
        final LongValue ioProcessTimeNanos = new LongValue(2);
        final LongValue taskProcessTimeNanos = new LongValue(3);
        final LongValue numTasksExecuted = new LongValue(4);
        final LongValue numImmediateTasksSubmitted = new LongValue(5);
        final LongValue numDelayedTasksSubmitted = new LongValue(6);
        final long responsivenessDelayNanos = 7 * ONE_SECOND_IN_NANOS;
        final long quiescentTimeNanos = 8 * ONE_SECOND_IN_NANOS;
        final List<NioEndpointHandlerPerf> handlerPerfs = new ArrayList<>();
        handlerPerfs.add(createDummyNioEndpointHandlerPerf());
        return new NioChannelExecutorPerf(
            childId, childIndex,
            idleTimeNanos, ioProcessTimeNanos, taskProcessTimeNanos,
            numTasksExecuted, numImmediateTasksSubmitted,
            numDelayedTasksSubmitted,
            responsivenessDelayNanos, quiescentTimeNanos,
            handlerPerfs);
    }

    /**
     * Tests automatically filtering of NioChannelExecutorPerf.
     *
     * This perf is never empty, but we can test filtering of various fields.
     */
    @Test
    public void testNioChannelExecutorPerfEmpty() throws Exception {
        final NioChannelExecutorPerf perf =
            createEmptyNioChannelExecutorPerf();
        final ObjectNode perfJson = perf.toJson();
        assertEquals(perfJson.toString(), true, perf.isDefault());
        assertEquals(perfJson.toString(), 0, perfJson.size());
        assertEquals(perf, new NioChannelExecutorPerf(perf.toJson()));
    }

    private NioChannelExecutorPerf createEmptyNioChannelExecutorPerf() {
        final int childId = NioChannelExecutorPerf.DEFAULT_ID;
        final int childIndex = NioChannelExecutorPerf.DEFAULT_ID;
        final LongValue idleTimeNanos = LongValue.DEFAULT;
        final LongValue ioProcessTimeNanos = LongValue.DEFAULT;
        final LongValue taskProcessTimeNanos = LongValue.DEFAULT;
        final LongValue numTasksExecuted = LongValue.DEFAULT;
        final LongValue numImmediateTasksSubmitted = LongValue.DEFAULT;
        final LongValue numDelayedTasksSubmitted = LongValue.DEFAULT;
        final long responsivenessDelayNanos = 0;
        final long quiescentTimeNanos = 0;
        final List<NioEndpointHandlerPerf> handlerPerfs = new ArrayList<>();
        handlerPerfs.add(createEmptyNioEndpointHandlerPerf());
        return new NioChannelExecutorPerf(
            childId, childIndex,
            idleTimeNanos, ioProcessTimeNanos, taskProcessTimeNanos,
            numTasksExecuted, numImmediateTasksSubmitted,
            numDelayedTasksSubmitted,
            responsivenessDelayNanos, quiescentTimeNanos,
            handlerPerfs);
    }

    /**
     * Tests NioChannelExecutorSummarizedPerf.
     */
    @Test
    public void testNioChannelExecutorSummarizedPerf() throws Exception {
        final NioChannelExecutorSummarizedPerf perf =
            createEmptyNioChannelExecutorSummarizedPerf();
        final ObjectNode perfJson = perf.toJson();
        assertEquals(perfJson.toString(), true, perf.isDefault());
        assertEquals(perfJson.toString(), 0, perfJson.size());
        assertEquals(
            perf, new NioChannelExecutorSummarizedPerf(perf.toJson()));
    }

    private NioChannelExecutorSummarizedPerf
        createEmptyNioChannelExecutorSummarizedPerf()
    {
        final List<NioChannelExecutorPerf> perfs = new ArrayList<>();
        return new NioChannelExecutorSummarizedPerf(perfs);
    }

    /**
     * Tests automatically filtering of NioChannelExecutorSummarizedPerf.
     */
    @Test
    public void testNioChannelExecutorSummarizedPerfEmpty() throws Exception {
        final NioChannelExecutorSummarizedPerf perf =
            createDummyNioChannelExecutorSummarizedPerf();
        assertEquals(
            perf, new NioChannelExecutorSummarizedPerf(perf.toJson()));
    }

    private NioChannelExecutorSummarizedPerf
        createDummyNioChannelExecutorSummarizedPerf()
    {
        final List<NioChannelExecutorPerf> perfs = new ArrayList<>();
        perfs.add(createDummyNioChannelExecutorPerf());
        return new NioChannelExecutorSummarizedPerf(perfs);
    }

    /**
     * Tests NioChannelThreadPoolPerf.
     */
    @Test
    public void testNioChannelThreadPoolPerf() throws Exception {
        final NioChannelThreadPoolPerf perf =
            createDummpyChannelThreadPoolPerf();
        assertEquals(perf, new NioChannelThreadPoolPerf(perf.toJson()));
    }

    private NioChannelThreadPoolPerf createDummpyChannelThreadPoolPerf() {
        final long intervalStart = System.nanoTime();
        final long intervalEnd = System.nanoTime();
        final LongValueStats numAliveExecutors =
            new LongValueStats(1, 1, 1, 1, 1, 1, 0);
        final int numUnresponsiveExecutors = 1;
        final List<NioChannelExecutorPerf> executorPerfs = new ArrayList<>();
        executorPerfs.add(createDummyNioChannelExecutorPerf());
        return new NioChannelThreadPoolPerf(
            intervalStart, intervalEnd,
            numAliveExecutors, numUnresponsiveExecutors,
            executorPerfs);
    }

    /**
     * Tests automatically filtering of NioChannelThreadPoolPerf.
     */
    @Test
    public void testNioChannelThreadPoolPerfEmpty() throws Exception {
        final NioChannelThreadPoolPerf perf =
            createEmptyChannelThreadPoolPerf();
        final ObjectNode perfJson = perf.toJson();
        assertEquals(perfJson.toString(), false, perf.isDefault());
        assertEquals(perfJson.toString(), 2, perfJson.size());
        assertEquals(perf, new NioChannelThreadPoolPerf(perf.toJson()));
    }

    private NioChannelThreadPoolPerf createEmptyChannelThreadPoolPerf() {
        final long intervalStart = System.nanoTime();
        final long intervalEnd = System.nanoTime();
        final LongValueStats numAliveExecutors = LongValueStats.DEFAULT;
        final int numUnresponsiveExecutors = 0;
        final List<NioChannelExecutorPerf> executorPerfs = new ArrayList<>();
        return new NioChannelThreadPoolPerf(
            intervalStart, intervalEnd,
            numAliveExecutors, numUnresponsiveExecutors,
            executorPerfs);
    }

    /**
     * Tests NioEndpointHandlerPerf.
     */
    @Test
    public void testNioEndpointHandlerPerf() throws Exception {
        final NioEndpointHandlerPerf perf =
            createDummyNioEndpointHandlerPerf();
        assertEquals(perf, new NioEndpointHandlerPerf(perf.toJson()));
    }

    private NioEndpointHandlerPerf createDummyNioEndpointHandlerPerf() {
        final String remoteAddress = "remote";
        final ThroughputElement.Result handlerReadThroughput =
            new ThroughputElement.Result(1, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result handlerWriteThroughput =
            new ThroughputElement.Result(2, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result channelReadThroughput =
            new ThroughputElement.Result(3, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result channelWriteThroughput =
            new ThroughputElement.Result(4, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result channelBytesReadThroughput =
            new ThroughputElement.Result(5, ONE_SECOND_IN_NANOS);
        final ThroughputElement.Result channelBytesWriteThroughput =
            new ThroughputElement.Result(6, ONE_SECOND_IN_NANOS);
        final LatencyElement.Result handlerReadLatency =
            new LatencyElement.Result(1, 1, 1, 1, 1, 1, 1, 0);
        final LatencyElement.Result handlerWriteLatency =
            new LatencyElement.Result(2, 2, 2, 2, 2, 2, 2, 0);
        final LatencyElement.Result channelReadLatency =
            new LatencyElement.Result(3, 3, 3, 3, 3, 3, 3, 0);
        final LatencyElement.Result channelWriteLatency =
            new LatencyElement.Result(4, 4, 4, 4, 4, 4, 4, 0);
        return new NioEndpointHandlerPerf(
            remoteAddress,
            handlerReadThroughput, handlerWriteThroughput,
            channelReadThroughput, channelWriteThroughput,
            channelBytesReadThroughput, channelBytesWriteThroughput,
            handlerReadLatency, handlerWriteLatency,
            channelReadLatency, channelWriteLatency);
    }

    /**
     * Tests automatically filtering of NioEndpointHandlerPerf.
     */
    @Test
    public void testNioEndpointHandlerPerfEmpty() throws Exception {
        final NioEndpointHandlerPerf perf =
            createEmptyNioEndpointHandlerPerf();
        final ObjectNode perfJson = perf.toJson();
        assertEquals(perfJson.toString(), true, perf.isDefault());
        assertEquals(perfJson.toString(), 0, perfJson.size());
        assertEquals(perf, new NioEndpointHandlerPerf(perfJson));
    }

    private NioEndpointHandlerPerf createEmptyNioEndpointHandlerPerf() {
        final String remoteAddress = StringField.DEFAULT;
        final ThroughputElement.Result handlerReadThroughput =
            ThroughputElement.Result.DEFAULT;
        final ThroughputElement.Result handlerWriteThroughput =
            ThroughputElement.Result.DEFAULT;
        final ThroughputElement.Result channelReadThroughput =
            ThroughputElement.Result.DEFAULT;
        final ThroughputElement.Result channelWriteThroughput =
            ThroughputElement.Result.DEFAULT;
        final ThroughputElement.Result channelBytesReadThroughput =
            ThroughputElement.Result.DEFAULT;
        final ThroughputElement.Result channelBytesWriteThroughput =
            ThroughputElement.Result.DEFAULT;
        final LatencyElement.Result handlerReadLatency =
            LatencyElement.Result.DEFAULT;
        final LatencyElement.Result handlerWriteLatency =
            LatencyElement.Result.DEFAULT;
        final LatencyElement.Result channelReadLatency =
            LatencyElement.Result.DEFAULT;
        final LatencyElement.Result channelWriteLatency =
            LatencyElement.Result.DEFAULT;
        return new NioEndpointHandlerPerf(
            remoteAddress,
            handlerReadThroughput, handlerWriteThroughput,
            channelReadThroughput, channelWriteThroughput,
            channelBytesReadThroughput, channelBytesWriteThroughput,
            handlerReadLatency, handlerWriteLatency,
            channelReadLatency, channelWriteLatency);
    }
}
