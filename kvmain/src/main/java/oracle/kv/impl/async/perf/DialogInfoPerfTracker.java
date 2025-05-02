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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.impl.util.FormatUtils;

/**
 * Trackers the detailed dialog info. The tracker will be present for sampled
 * dialog contexts.
 */
public class DialogInfoPerfTracker {

    /*
     * Dialogs emit a SEND event when sending a dialog frame and a RECV event
     * when receiving. We expect most dialogs have only several frames.
     * However, there could be dialogs with huge number of frames. We start
     * dropping frame events beyond a limit to prevent these dialogs from
     * generating too many output. The default frame size is set to 2 KB. Set
     * the limit to support maximum 16 frame events which is enough for a total
     * 32KB dialog message size for both sending and receiving. Typically only
     * one of the direction will have a large amount of data.
     */
    private static final int FRAME_EVENTS_LIMIT = 16;

    private volatile String id;
    private final long initTimeNs;
    private final boolean isSampledForRecord;
    private final AtomicLong outgoingNumMessages = new AtomicLong(0);
    private final AtomicLong outgoingNumFrames = new AtomicLong(0);
    private final AtomicLong outgoingNumBytes = new AtomicLong(0);
    private final AtomicLong incomingNumMessages = new AtomicLong(0);
    private final AtomicLong incomingNumFrames = new AtomicLong(0);
    private final AtomicLong incomingNumBytes = new AtomicLong(0);

    private volatile boolean frameEventsDropped = false;
    private final List<DialogEvent.Info> events =
        Collections.synchronizedList(new ArrayList<>());

    public DialogInfoPerfTracker(long initTimeMs,
                                 long initTimeNs,
                                 boolean isSampledForRecord) {
        this.initTimeNs = initTimeNs;
        this.isSampledForRecord = isSampledForRecord;
        addEvent(
            (new DialogEvent.Info(DialogEvent.Type.INIT,
                                  initTimeNs, initTimeNs))
            .addAttribute("time", String.format(
                "%s/%s", FormatUtils.formatDateTime(initTimeMs), initTimeMs)));
    }

    public boolean isSampledForRecord() {
        return isSampledForRecord;
    }

    /* Collection methods */

    public void setId(String id) {
        this.id = id;
    }

    public void onWriteMessage(long size) {
        outgoingNumMessages.getAndIncrement();
        outgoingNumBytes.getAndAdd(size);
        addEvent((new DialogEvent.Info(DialogEvent.Type.WRITE, initTimeNs))
                 .addAttribute("size", size));
    }

    public void onSendFrame() {
        outgoingNumFrames.getAndIncrement();
        addEvent(new DialogEvent.Info(DialogEvent.Type.SEND, initTimeNs));
    }

    public void onReceiveFrame(long size) {
        incomingNumBytes.getAndAdd(size);
        incomingNumFrames.getAndIncrement();
        addEvent((new DialogEvent.Info(DialogEvent.Type.RECV, initTimeNs))
                 .addAttribute("size", size));
    }

    public void onReadMessage() {
        incomingNumMessages.getAndIncrement();
        addEvent(new DialogEvent.Info(DialogEvent.Type.READ, initTimeNs));
    }

    public void onFinish(long doneTimeNs) {
        addEvent(new DialogEvent.Info(
            DialogEvent.Type.FIN, initTimeNs, doneTimeNs));
    }

    public void onAbort(long doneTimeNs) {
        addEvent(new DialogEvent.Info(
            DialogEvent.Type.ABORT, initTimeNs, doneTimeNs));
    }

    private void addEvent(DialogEvent.Info eventInfo) {
        synchronized(events) {
            if ((eventInfo.getType().equals(DialogEvent.Type.SEND) ||
                 eventInfo.getType().equals(DialogEvent.Type.RECV)) &&
                (events.size() > FRAME_EVENTS_LIMIT)) {
                frameEventsDropped = true;
                return;
            }
            events.add(eventInfo);
        }
    }

    /* Report methods */

    /**
     * Returns the latency breakdown.
     */
    public Map<DialogEvent.Span, Long> getLatencyBreakdown() {
        final Map<DialogEvent.Span, Long> breakdown = new HashMap<>();
        final Map<DialogEvent.Span, Integer> countMap = new HashMap<>();
        final Map<DialogEvent.Span, Long> totalMap = new HashMap<>();
        for (int i = 1; i < events.size(); ++i) {
            final DialogEvent.Info prevInfo = events.get(i - 1);
            final DialogEvent.Info currInfo = events.get(i);
            final DialogEvent.Span span =
                new DialogEvent.Span(prevInfo.getType(), currInfo.getType());
            countMap.compute(span, (s, v) -> (v == null ? 0 : v) + 1);
            final long elapsedNs =
                currInfo.getElapsedNanos() - prevInfo.getElapsedNanos();
            totalMap.compute(
                span, (s, v) -> (v == null ? 0 : v) + elapsedNs);
        }
        for (DialogEvent.Span span : countMap.keySet()) {
            breakdown.put(
                span, totalMap.get(span) / countMap.get(span));
        }
        return breakdown;
    }

    /**
     * Returns the {@code DialogInfoPerf} object.
     */
    public DialogInfoPerf getDialogInfoPerf() {
        return new DialogInfoPerf(
            id,
            outgoingNumMessages.get(), outgoingNumFrames.get(),
            outgoingNumBytes.get(),
            incomingNumMessages.get(), incomingNumFrames.get(),
            incomingNumBytes.get(),
            frameEventsDropped,
            events);
    }
}
