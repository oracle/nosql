/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import java.util.stream.Collectors;

import oracle.kv.TestBase;
import oracle.kv.impl.async.IOBufPoolTrackers.Tracker;
import oracle.kv.impl.async.IOBufPoolTrackers.TrackerImpl;
import oracle.kv.impl.async.IOBufPoolTrackers.TrackerType;
import oracle.kv.impl.async.dialog.ProtocolMesg;
import oracle.kv.impl.async.dialog.ProtocolReader;
import oracle.kv.impl.async.dialog.ProtocolWriter;
import oracle.kv.impl.async.dialog.nio.NioChannelInput;
import oracle.kv.impl.async.dialog.nio.NioChannelOutput;
import oracle.kv.impl.util.EventTrackersManager;
import oracle.kv.impl.util.FormatUtils;
import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;


/** Test the {@link IOBufPoolTrackers} class. */
public class IOBufPoolTrackersTest extends TestBase {


    private final ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor();

    @Override
    public void setUp() throws Exception {
        setupIdentityHook();
    }

    private void setupIdentityHook() {
        final AtomicInteger identitySequenceNumber = new AtomicInteger(0);
        /*
         * Use an identityHashMap since ByteBuffer equals method do not compare
         * identity
         */
        final IdentityHashMap<Object, Integer> identityMap =
            new IdentityHashMap<>();
        IOBufPoolTrackers.getIdentityHook = (object) -> {
            synchronized(identityMap) {
                if (object instanceof DummySlice) {
                    return ((DummySlice) object).getID();
                }
                return identityMap.computeIfAbsent(
                    object, (k) -> identitySequenceNumber.incrementAndGet());
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        IOBufPoolTrackers.getIdentityHook = null;
    }

    @Test
    public void testBasic() {
        final IOBufPoolTrackers trackers = new IOBufPoolTrackers(logger, executor);
        trackers.setSampleRate(1);

        final ByteBuffer buf1 = ByteBuffer.allocate(0);
        final IOBufSliceImpl slice2 = new DummySlice(2);
        final IOBufSliceImpl slice3 = new DummySlice(3);
        final IOBufSliceImpl slice4 = new DummySlice(4);
        final IOBufSliceImpl slice5 = new DummySlice(5);
        final Tracker tracker = trackers.createTracker(
            TrackerType.CHANNEL_INPUT, buf1);
        tracker.markNew(slice2, 1 /* refcnt */);
        tracker.markFork(slice2, 2 /* refcnt */, slice3);
        tracker.markNew(slice3, 1 /* refcnt */);
        tracker.markFork(slice3, 2 /* refcnt */, slice4);
        tracker.markNew(slice4, 1 /* refcnt */);
        tracker.markFork(slice2, 3 /* refcnt */, slice5);
        tracker.markNew(slice5, 1 /* refcnt */);
        final long creationTimeMillis =
            ((TrackerImpl) tracker).getCreationTimeMillis();
        ObjectNode expected = (new ExpectedTrackerResultBuilder())
            .setID(1).setRoot(2).setTrackerType("CHANNEL_INPUT")
            .setCreationTimeMillis(creationTimeMillis)
            .addSlice(2).addNew(1).addFork(3, 2).addFork(5, 3).done()
            .addSlice(5).addNew(1).done()
            .addSlice(3).addNew(1).addFork(4, 2).done()
            .addSlice(4).addNew(1).done()
            .getResult();
        assertEquals(expected, tracker.toJson());
        tracker.markFree(slice5, 0 /* refcnt */);
        tracker.markRelease(slice2, 2 /* refcnt */, slice5);
        tracker.markFree(slice4, 0 /* refcnt */);
        tracker.markRelease(slice3, 1 /* refcnt */, slice4);
        expected = (new ExpectedTrackerResultBuilder())
            .setID(1).setRoot(2).setTrackerType("CHANNEL_INPUT")
            .setCreationTimeMillis(creationTimeMillis)
            .addSlice(2).addNew(1).addFork(3, 2).addFork(5, 3)
            .addRelease(5, 2).done()
            .addSlice(3).addNew(1).addFork(4, 2).addRelease(4, 1).done()
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private class DummySlice extends IOBufSliceImpl {
        private final int id;

        public DummySlice(int id) {
            super(ByteBuffer.allocate(0),
                  IOBufPoolTrackers.createFromHeap(null),
                  "dummy");
            this.id = id;
        }

        public int getID() {
            return id;
        }

        @Override
        public void markFree() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected IOBufSliceImpl forkNewSlice(ByteBuffer buf,
                                              String forkDescription) {
            throw new UnsupportedOperationException();
        }
    }

    private class ExpectedTrackerResultBuilder {

        private final ObjectNode result = JsonUtils.createObjectNode();

        ExpectedTrackerResultBuilder setID(int id) {
            result.put("id", Integer.toString(id, 16));
            return this;
        }

        ExpectedTrackerResultBuilder setRoot(@Nullable Integer root) {
            if (root == null) {
                result.put("root", JsonUtils.createJsonNull());
            } else {
                result.put("root", Integer.toString(root, 16));
            }
            return this;
        }

        ExpectedTrackerResultBuilder setTrackerType(String type)
        {
            result.put("trackerType", type);
            return this;
        }

        ExpectedTrackerResultBuilder setCreationTimeMillis(
            long creationTimeMillis)
        {
            result.put("creationTimeMillis", creationTimeMillis);
            result.put(
                "creationTimeHuman",
                FormatUtils.formatDateTime(creationTimeMillis));
            return this;
        }

        ExpectedTrackerResultBuilder addNullSlice() {
            result.put("slices", JsonUtils.createJsonNull());
            return this;
        }

        SliceEventsBuilder addSlice(int id) {
            if (!result.has("slices")) {
                result.put("slices", JsonUtils.createArrayNode());
            }
            final ArrayNode slices =
                result.get("slices").asArray();
            for (JsonNode slice : slices) {
                final ObjectNode object = slice.asObject();
                if (object.get("id").asInt() == id) {
                    return new SliceEventsBuilder(
                        object.get("events").asArray());
                }
            }
            final ObjectNode slice = JsonUtils.createObjectNode();
            slice.put("id", Integer.toString(id, 16));
            final ArrayNode events = JsonUtils.createArrayNode();
            slice.put("events", events);
            slices.add(slice);
            return new SliceEventsBuilder(events);
        }

        ObjectNode getResult() {
            return result;
        }

        private class SliceEventsBuilder {

            private final ArrayNode events;

            private SliceEventsBuilder(ArrayNode events) {
                this.events = events;
            }

            SliceEventsBuilder addNew(int refcnt) {
                final ObjectNode event = JsonUtils.createObjectNode();
                event.put("type", "NEW");
                event.put("refcnt", refcnt);
                events.add(event);
                return this;
            }

            SliceEventsBuilder addFork(int childID, int refcnt) {
                final ObjectNode event = JsonUtils.createObjectNode();
                event.put("type", "FORK");
                event.put("refcnt", refcnt);
                event.put("child", Integer.toString(childID, 16));
                events.add(event);
                return this;
            }

            SliceEventsBuilder addRelease(int childID, int refcnt) {
                final ObjectNode event = JsonUtils.createObjectNode();
                event.put("type", "RELEASE");
                event.put("refcnt", refcnt);
                event.put("child", Integer.toString(childID, 16));
                events.add(event);
                return this;
            }

            SliceEventsBuilder addFree(int refcnt) {
                final ObjectNode event = JsonUtils.createObjectNode();
                event.put("type", "FREE");
                event.put("refcnt", refcnt);
                events.add(event);
                return this;
            }

            SliceEventsBuilder addEndpointHandlerInfo() {
                final ObjectNode event = JsonUtils.createObjectNode();
                event.put("type", "INFO");
                event.put("info", "null endpoint handler");
                events.add(event);
                return this;
            }

            SliceEventsBuilder addDialogInfo() {
                final ObjectNode event = JsonUtils.createObjectNode();
                event.put("type", "INFO");
                event.put("info", "null dialog");
                events.add(event);
                return this;
            }

            SliceEventsBuilder addInfo(String info) {
                final ObjectNode event = JsonUtils.createObjectNode();
                event.put("type", "INFO");
                event.put("info", info);
                events.add(event);
                return this;
            }

            ExpectedTrackerResultBuilder done() {
                return ExpectedTrackerResultBuilder.this;
            }

        }
    }

    @Test
    public void testChannelInput() throws Exception {
        final String trackerType = "CHANNEL_INPUT";
        final EventTrackersManager trackersManager =
            new EventTrackersManager(logger, executor);
        final IOBufPoolTrackers trackers = trackersManager.getIOBufPoolTrackers();
        trackers.setSampleRate(1);

        final NioChannelInput channelInput = new NioChannelInput(trackersManager, null);
        final Iterator<TrackerImpl> iter =
            trackers.getTrackers(TrackerType.CHANNEL_INPUT).iterator();
        final TrackerImpl slice0Tracker = iter.next();
        final TrackerImpl slice1Tracker = iter.next();
        verifyCreateChannelBufFromPool(1, 2, trackerType, slice0Tracker);
        verifyCreateChannelBufFromPool(3, 4, trackerType, slice1Tracker);

        final ByteBuffer[] buffers = channelInput.flipToChannelRead();
        final byte[] dialogFrameBytes = new byte[] {
            0x21 /* dialog frame ident */,
            0x01 /* dialog ID */,
            0x03 /* length */,
            0x44 /* D */, 0x4C /* L */, 0x47 /* G */,
        };
        fillBuffers(dialogFrameBytes, buffers);
        channelInput.flipToProtocolRead();
        final ProtocolReader protocolReader =
            new ProtocolReader(channelInput, Integer.MAX_VALUE);
        final ProtocolMesg.DialogFrame dialogFrame =
            (ProtocolMesg.DialogFrame) protocolReader.read((id) -> true);
        dialogFrame.frame.trackDialog(null);
        verifyProtocolRead(1, 2, 5, slice0Tracker);
        verifyCreateChannelBufFromPool(3, 4, trackerType, slice1Tracker);

        dialogFrame.frame.readFully(new byte[3], 0, 3);
        verifyFrameRead(1, 2, 5, slice0Tracker);
        verifyCreateChannelBufFromPool(3, 4, trackerType, slice1Tracker);

        channelInput.close();
        verifyCleanup(1, trackerType, slice0Tracker);
        verifyCleanup(3, trackerType, slice1Tracker);
        assertEquals(
            0, trackers.getTrackers(TrackerType.CHANNEL_INPUT).size());
    }

    private void verifyCreateChannelBufFromPool(
        int trackerKey,
        int rootID,
        String trackerType,
        TrackerImpl tracker)
    {
        final ObjectNode expected =
            getExpectedBuilderCreateFromPool(
                trackerKey, rootID, trackerType, tracker)
            .addSlice(rootID).addEndpointHandlerInfo().done()
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private ExpectedTrackerResultBuilder getExpectedBuilderCreateFromPool(
        int trackerKey,
        int rootID,
        String trackerType,
        TrackerImpl tracker)
    {
        return (new ExpectedTrackerResultBuilder())
            .setID(trackerKey).setRoot(rootID).setTrackerType(trackerType)
            .setCreationTimeMillis(tracker.getCreationTimeMillis())
            .addSlice(rootID).addNew(1).done();
    }

    private void fillBuffers(byte[] bytes, ByteBuffer[] buffers) {
        int i = 0;
        for (byte b : bytes) {
            while (true) {
                final ByteBuffer buf = buffers[i];
                if (buf.remaining() != 0) {
                    break;
                }
                i++;
            }
            buffers[i].put(b);
        }
    }

    private void verifyProtocolRead(int trackerKey,
                                    int rootID,
                                    int forkChildID,
                                    TrackerImpl tracker) {
        final ObjectNode expected =
            (new ExpectedTrackerResultBuilder())
            .setID(trackerKey).setRoot(rootID).setTrackerType("CHANNEL_INPUT")
            .setCreationTimeMillis(tracker.getCreationTimeMillis())
            .addSlice(rootID).addNew(1).addEndpointHandlerInfo()
            .addFork(forkChildID, 2).done()
            .addSlice(forkChildID).addNew(1).addInfo("readBytesInput")
            .addInfo("headOfMessageInput").addDialogInfo().done()
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private void verifyFrameRead(int trackerKey,
                                 int rootID,
                                 int forkChildID,
                                 TrackerImpl tracker) {
        final ObjectNode expected =
            (new ExpectedTrackerResultBuilder())
            .setID(trackerKey).setRoot(rootID).setTrackerType("CHANNEL_INPUT")
            .setCreationTimeMillis(tracker.getCreationTimeMillis())
            .addSlice(rootID).addNew(1).addEndpointHandlerInfo()
            .addFork(forkChildID, 2).addRelease(forkChildID, 1).done()
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private void verifyCleanup(int trackerKey,
                               String trackerType,
                               TrackerImpl tracker) {
        ObjectNode expected = (new ExpectedTrackerResultBuilder())
            .setID(trackerKey).setRoot(null).setTrackerType(trackerType)
            .setCreationTimeMillis(tracker.getCreationTimeMillis())
            .addNullSlice()
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    @Test
    public void testChannelOutput() throws Exception {
        final EventTrackersManager trackersManager =
            new EventTrackersManager(logger, executor);
        final IOBufPoolTrackers trackers = trackersManager.getIOBufPoolTrackers();
        trackers.setSampleRate(1);

        final NioChannelOutput channelOutput = new NioChannelOutput(trackersManager, null);
        final ProtocolWriter protocolWriter =
            new ProtocolWriter(channelOutput, Integer.MAX_VALUE);
        protocolWriter.writeProtocolVersion(1);
        final TrackerImpl sliceTracker =
            trackers.getTrackers(TrackerType.CHANNEL_OUTPUT).iterator().next();
        verifyProtocolWrite(1, 2, 3, sliceTracker);

        final NioChannelOutput.Bufs bufs = channelOutput.getBufs();
        verifyChannelWrite(1, 2, 3, sliceTracker);

        drainBufs(bufs);
        /* getBufs again to free */
        channelOutput.getBufs();
        verifyCleanup(1, "CHANNEL_OUTPUT", sliceTracker);
        assertEquals(
            0, trackers.getTrackers(TrackerType.CHANNEL_OUTPUT).size());
    }

    private void verifyProtocolWrite(int trackerKey,
                                           int rootID,
                                           int forkChildID,
                                           TrackerImpl tracker) {
        final ObjectNode expected =
            getExpectedBuilderProtocolWrite(
                trackerKey, rootID, forkChildID, tracker)
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private ExpectedTrackerResultBuilder
        getExpectedBuilderProtocolWrite(int trackerKey,
                                        int rootID,
                                        int forkChildID,
                                        TrackerImpl tracker) {
        return (new ExpectedTrackerResultBuilder())
            .setID(trackerKey).setRoot(rootID).setTrackerType("CHANNEL_OUTPUT")
            .setCreationTimeMillis(tracker.getCreationTimeMillis())
            .addSlice(rootID).addNew(1).addEndpointHandlerInfo()
            .addFork(forkChildID, 2).addFree(1).done()
            .addSlice(forkChildID).addNew(1)
            .addInfo("addToChunkOutputs")
            .addInfo("chunkAddToChnlOutput").done();
    }

    private void verifyChannelWrite(int trackerKey,
                                           int rootID,
                                           int forkChildID,
                                           TrackerImpl tracker) {
        final ObjectNode expected =
            getExpectedBuilderProtocolWrite(
                trackerKey, rootID, forkChildID, tracker)
            .addSlice(forkChildID)
            .addInfo("fetchFromChannelOutput")
            .addInfo("addToBufForSocket").done()
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private void drainBufs(NioChannelOutput.Bufs bufs) {
        for (int i = bufs.offset(); i < bufs.length(); ++i) {
            final ByteBuffer buf = bufs.array()[i];
            while (buf.hasRemaining()) {
                buf.get();
            }
        }
    }

    @Test
    public void testMessageOutput() throws Exception {
        final EventTrackersManager trackersManager =
            new EventTrackersManager(logger, executor);
        final IOBufPoolTrackers trackers = trackersManager.getIOBufPoolTrackers();
        trackers.setSampleRate(1);

        final MessageOutput messageOutput = new MessageOutput(trackersManager);
        assertEquals(1, trackers.getTrackers(TrackerType.MESSAGE_OUTPUT).size());
        final TrackerImpl slice0Tracker =
            trackers.getTrackers(TrackerType.MESSAGE_OUTPUT).iterator().next();
        verifyCreateMessageBufFromPool(1, 2, slice0Tracker);

        for (int i = 0; i < 128; ++i) {
            messageOutput.write(1);
            messageOutput.write(new byte[] { 0x01 });
        }
        assertEquals(2, trackers.getTrackers(TrackerType.MESSAGE_OUTPUT).size());
        final Iterator<TrackerImpl> iter =
            trackers.getTrackers(TrackerType.MESSAGE_OUTPUT).iterator();
        iter.next();
        final TrackerImpl slice1Tracker = iter.next();
        verifyMessageScatterBufferFilled(1, 2, 3, slice0Tracker);
        verifyCreateMessageBufFromPool(4, 5, slice1Tracker);

        final Queue<IOBufSliceList> frames =
            messageOutput.pollFrames(128, null);
        if (frames == null) {
            throw new AssertionError("frames should not be null");
        }
        verifyMessageScatterBufferPolled(1, 2, 3, 6, slice0Tracker);
        verifyMessageScatterBufferPolled(4, 5, 7, 8, slice1Tracker);

        final NioChannelOutput channelOutput =
            new NioChannelOutput(trackersManager, null);
        final ProtocolWriter protocolWriter =
            new ProtocolWriter(channelOutput, Integer.MAX_VALUE);
        protocolWriter.writeDialogStart(false /* sampled */, false /* finish */,
            true /* cont */, 1 /* typeno */, 1 /* dialogID */,
            5000 /* timeout */, frames.poll());
        while (!frames.isEmpty()) {
            final boolean last = (frames.size() == 1);
            protocolWriter.writeDialogFrame(
                last /* finish */, !last /* cont */, 1 /* dialogID */,
                frames.poll());
        }
        verifyMessageProtocolWrite(1, 2, 3, 6, slice0Tracker);
        verifyMessageProtocolWrite(4, 5, 7, 8, slice1Tracker);

        final NioChannelOutput.Bufs bufs = channelOutput.getBufs();
        verifyMessageChannelWrite(1, 2, 3, 6, slice0Tracker);
        verifyMessageChannelWrite(4, 5, 7, 8, slice1Tracker);

        drainBufs(bufs);
        /* getBufs again to free */
        channelOutput.getBufs();
        verifyCleanup(1, "MESSAGE_OUTPUT", slice0Tracker);
        verifyCleanup(4, "MESSAGE_OUTPUT", slice1Tracker);
        assertEquals(
            0, trackers.getTrackers(TrackerType.MESSAGE_OUTPUT).size());
    }

    private void verifyMessageScatterBufferFilled(
        int trackerKey,
        int rootID,
        int childID,
        TrackerImpl tracker)
    {
        final ObjectNode expected =
            getExpectedBuilderMessageScatterBufferFilled(
                trackerKey, rootID, childID, tracker)
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private ExpectedTrackerResultBuilder
        getExpectedBuilderMessageScatterBufferFilled(
        int trackerKey,
        int rootID,
        int childID,
        TrackerImpl tracker)
    {
        return getExpectedBuilderCreateFromPool(
            trackerKey, rootID, "MESSAGE_OUTPUT", tracker)
            .addSlice(rootID).addFork(childID, 2).addFree(1).done()
            .addSlice(childID).addNew(1)
            .addInfo("appendBufferedToOutput").done();
    }

    private void verifyCreateMessageBufFromPool(
        int trackerKey,
        int rootID,
        TrackerImpl tracker)
    {
        final ObjectNode expected =
            getExpectedBuilderCreateFromPool(
                trackerKey, rootID, "MESSAGE_OUTPUT", tracker)
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private void verifyMessageScatterBufferPolled(
        int trackerKey,
        int rootID,
        int bufChildID,
        int pollChildID,
        TrackerImpl tracker)
    {
        final ObjectNode expected =
            getExpectedBuilderMessageScatterBufferPolled(
                trackerKey, rootID, bufChildID, pollChildID, tracker)
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private ExpectedTrackerResultBuilder
        getExpectedBuilderMessageScatterBufferPolled(
            int trackerKey,
            int rootID,
            int bufChildID,
            int pollChildID,
            TrackerImpl tracker)
    {
        return getExpectedBuilderCreateFromPool(
            trackerKey, rootID, "MESSAGE_OUTPUT", tracker)
            .addSlice(rootID).addFork(bufChildID, 2).addFree(1).done()
            .addSlice(bufChildID).addNew(1)
            .addInfo("appendBufferedToOutput").addDialogInfo()
            .addFork(pollChildID, 2).addFree(1).done()
            .addSlice(pollChildID).addNew(1)
            .addInfo("addToFrameToPoll").addDialogInfo().done();
    }

    private void verifyMessageProtocolWrite(
        int trackerKey,
        int rootID,
        int bufChildID,
        int pollChildID,
        TrackerImpl tracker)
    {
        final ObjectNode expected =
            getExpectedBuilderMessageProtocolWrite(
                trackerKey, rootID, bufChildID, pollChildID, tracker)
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private ExpectedTrackerResultBuilder
        getExpectedBuilderMessageProtocolWrite(
            int trackerKey,
            int rootID,
            int bufChildID,
            int pollChildID,
            TrackerImpl tracker)
    {
        return getExpectedBuilderMessageScatterBufferPolled(
            trackerKey, rootID, bufChildID, pollChildID, tracker)
            .addSlice(pollChildID)
            .addInfo("addToChunkOutputs").addEndpointHandlerInfo()
            .addInfo("chunkAddToChnlOutput")
            .done();
    }

    private void verifyMessageChannelWrite(
        int trackerKey,
        int rootID,
        int bufChildID,
        int pollChildID,
        TrackerImpl tracker)
    {
        final ObjectNode expected =
            getExpectedBuilderMessageChannelWrite(
                trackerKey, rootID, bufChildID, pollChildID, tracker)
            .getResult();
        assertEquals(expected, tracker.toJson());
    }

    private ExpectedTrackerResultBuilder
        getExpectedBuilderMessageChannelWrite(
            int trackerKey,
            int rootID,
            int bufChildID,
            int pollChildID,
            TrackerImpl tracker)
    {
        return getExpectedBuilderMessageProtocolWrite(
            trackerKey, rootID, bufChildID, pollChildID, tracker)
            .addSlice(pollChildID)
            .addInfo("fetchFromChannelOutput").addInfo("addToBufForSocket")
            .done();
    }

    @Test
    public void testSample() {
        final IOBufPoolTrackers trackers = new IOBufPoolTrackers(logger, executor);
        final ByteBuffer buf = ByteBuffer.allocate(0);
        trackers.setSampleRate(2);
        trackers.setMaxNumTrackers(2);
        assertEquals(
            true,
            trackers.createTracker(TrackerType.CHANNEL_INPUT, buf).isNull());
        assertEquals(
            false,
            trackers.createTracker(TrackerType.CHANNEL_INPUT, buf).isNull());
        assertEquals(
            true,
            trackers.createTracker(TrackerType.CHANNEL_INPUT, buf).isNull());
        assertEquals(
            false,
            trackers.createTracker(TrackerType.CHANNEL_INPUT, buf).isNull());
        assertEquals(
            true,
            trackers.createTracker(TrackerType.CHANNEL_INPUT, buf).isNull());
        assertEquals(
            true,
            trackers.createTracker(TrackerType.CHANNEL_INPUT, buf).isNull());
    }

    @Test
    public void testLogging() throws Exception {
        final List<LogRecord> records = new ArrayList<>();
        final Logger testLogger = setupLogger("testLogging", records);
        final IOBufPoolTrackers trackers = new IOBufPoolTrackers(testLogger, executor);
        trackers.setSampleRate(1);
        trackers.setMaxNumTrackers(2);
        trackers.setTrackerLoggingDelayMillis(2000);
        trackers.setLoggingIntervalMillis(1000);
        trackers.scheduleLoggingTask();

        final ByteBuffer buf1 = ByteBuffer.allocate(0);
        final ByteBuffer buf2 = ByteBuffer.allocate(0);
        final IOBufSliceImpl slice3 = new DummySlice(3);
        final IOBufSliceImpl slice4 = new DummySlice(4);
        final Tracker tracker0 =
            trackers.createTracker(TrackerType.CHANNEL_INPUT, buf1);
        final Tracker tracker1 =
            trackers.createTracker(TrackerType.CHANNEL_INPUT, buf2);
        tracker0.markNew(slice3, 1 /* refcnt */);
        tracker1.markNew(slice4, 1 /* refcnt */);

        Thread.sleep(1000);
        /* Not yet treat trackers as leak, only the tracker description log */
        assertEquals(0, records.size());

        Thread.sleep(3000);
        /* Should log one or two records with two trackers */
        final String recordMessages =
            records.stream()
            .map((r) -> r.getMessage())
            .collect(Collectors.joining("\n", "\n", "\n"));
        final int recordSize = records.size();
        assertTrue(recordMessages, 1 <= recordSize);
        assertTrue(recordMessages, 2 >= recordSize);
        assertTrue(recordMessages.contains("\"id\":\"1\""));
        assertTrue(recordMessages.contains("\"id\":\"2\""));

        Thread.sleep(2000);
        /* Should not generate more records */
        assertEquals(recordSize, records.size());
    }

    private Logger setupLogger(String name, List<LogRecord> records) {
        final Logger l = Logger.getLogger(name);
        l.addHandler(new StreamHandler() {
            @Override
            public synchronized void publish(@Nullable LogRecord record) {
                records.add(record);
            }
        });
        return l;
    }

}
