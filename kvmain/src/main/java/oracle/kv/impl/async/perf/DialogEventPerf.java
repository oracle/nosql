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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.util.FormatUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Captures the events of a dialog.
 *
 * This class is deprected and being replaced by {@link DialogInfoPerf}.
 *
 * @deprecated since 22.3
 */
@Deprecated
public class DialogEventPerf implements Serializable {

    private static final long serialVersionUID = 1L;

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
    private final CountInfo writeInfo = new CountInfo();
    private final AtomicInteger readCount = new AtomicInteger(0);
    private final AtomicInteger sendCount = new AtomicInteger(0);
    private final CountInfo recvInfo = new CountInfo();

    private volatile boolean frameEventsDropped = false;
    private final List<EventInfo> events =
        Collections.synchronizedList(new ArrayList<>());

    public DialogEventPerf(long initTimeMs,
                           long initTimeNs,
                           boolean isSampledForRecord) {
        this.initTimeNs = initTimeNs;
        this.isSampledForRecord = isSampledForRecord;
        addEvent(new EventInfo(
            Type.INIT, "time",
            String.format(
                "%s/%s",
                FormatUtils.formatDateTime(initTimeMs),
                initTimeMs),
            initTimeNs, initTimeNs));
    }

    public boolean isSampledForRecord() {
        return isSampledForRecord;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void onWrite(long size) {
        writeInfo.update(size);
        addEvent(new EventInfo(Type.WRITE, "size", size, initTimeNs));
    }

    public void onRead() {
        readCount.getAndIncrement();
        addEvent(new EventInfo(Type.READ, null, null, initTimeNs));
    }

    public void onSend() {
        sendCount.getAndIncrement();
        addEvent(new EventInfo(Type.SEND, null, null, initTimeNs));
    }

    public void onReceive(long size) {
        recvInfo.update(size);
        addEvent(new EventInfo(Type.RECV, "size", size, initTimeNs));
    }

    public void onFinish(long doneTimeNs) {
        addEvent(new EventInfo(Type.FIN, null, null, doneTimeNs, initTimeNs));
    }

    public void onAbort(long doneTimeNs) {
        addEvent(new EventInfo(
            Type.ABORT, null, null, doneTimeNs, initTimeNs));
    }

    private void addEvent(EventInfo eventInfo) {
        synchronized(events) {
            if ((eventInfo.type.equals(Type.SEND) ||
                 eventInfo.type.equals(Type.RECV)) &&
                (events.size() > FRAME_EVENTS_LIMIT)) {
                frameEventsDropped = true;
                return;
            }
            events.add(eventInfo);
        }
    }

    public enum Type {
        INIT, WRITE, SEND, RECV, READ, FIN, ABORT
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    public JsonObject toJson() {
        final JsonObject result = new JsonObject();
        result.addProperty("id", (id == null) ? "not set" : id);
        result.add("writeInfo", writeInfo.toJson());
        result.addProperty("readCount", readCount.get());
        result.addProperty("sendCount", sendCount.get());
        result.add("recvInfo", recvInfo.toJson());
        result.add("events", getEventsJson());
        return result;
    }

    /**
     * Returns the detailed events as a json array.
     */
    private JsonArray getEventsJson() {
        final JsonArray array = new JsonArray();
        synchronized(events) {
            events.forEach((e) -> array.add(e.toJson()));
        }
        if (frameEventsDropped) {
            array.add("some events dropped");
        }
        return array;
    }

    /**
     * Returns the number of MessageOutput written.
     */
    public int getNumOutputsWritten() {
        return writeInfo.numUpdates();
    }

    /**
     * Returns a summary of the events.
     */
    public Map<EventSpan, Long> getEventLatencyNanosMap() {
        final Map<EventSpan, Long> latencyMap = new HashMap<>();
        final Map<EventSpan, Integer> spanCountMap = new HashMap<>();
        final Map<EventSpan, Long> spanLatencyTotMap = new HashMap<>();
        for (int i = 1; i < events.size(); ++i) {
            final EventInfo prevInfo = events.get(i - 1);
            final EventInfo currInfo = events.get(i);
            final EventSpan span =
                new EventSpan(prevInfo.type, currInfo.type);
            spanCountMap.compute(span, (s, v) -> (v == null ? 0 : v) + 1);
            final long elapsedNs = currInfo.elapsedNs - prevInfo.elapsedNs;
            spanLatencyTotMap.compute(
                span, (s, v) -> (v == null ? 0 : v) + elapsedNs);
        }
        for (EventSpan span : spanCountMap.keySet()) {
            latencyMap.put(
                span, spanLatencyTotMap.get(span) / spanCountMap.get(span));
        }
        return latencyMap;
    }

    /**
     * Representing a span of time between two consecutive events.
     */
    public static class EventSpan
        implements Comparable<EventSpan>, Serializable {

        private static final long serialVersionUID = 1L;

        final Type startEvent;
        final Type endEvent;

        public EventSpan(Type startEvent, Type endEvent) {
            this.startEvent = startEvent;
            this.endEvent = endEvent;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof EventSpan)) {
                return false;
            }
            final EventSpan that = (EventSpan) other;
            return (this.startEvent.equals(that.startEvent) &&
                    (this.endEvent.equals(that.endEvent)));
        }

        @Override
        public int hashCode() {
            return startEvent.hashCode() * 37 + endEvent.hashCode();
        }

        @Override
        public String toString() {
            return String.format("%s-%s", startEvent, endEvent);
        }

        @Override
        public int compareTo(EventSpan that) {
            final int startDiff = startEvent.compareTo(that.startEvent);
            return (startDiff != 0) ?
                startDiff :
                endEvent.compareTo(that.endEvent);
        }
    }

    /**
     * A count info with number of updates and total.
     *
     * Synchronized on the object for access.
     */
    private static class CountInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        private int numUpdates = 0;
        private long total = 0;

        private synchronized void update(long n) {
            numUpdates ++;
            total += n;
        }

        private synchronized int numUpdates() {
            return numUpdates;
        }

        @Override
        public String toString() {
            return toJson().toString();
        }

        public JsonObject toJson() {
            final JsonObject result = new JsonObject();
            result.addProperty("n", numUpdates);
            result.addProperty("total", total);
            return result;
        }
    }

    /**
     * A event info with a type, some info key/value pair and the elapsed time.
     */
    private static class EventInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Type type;
        private final String infoKey;
        private final Object info;
        private final long elapsedNs;

        private EventInfo(Type type,
                          String infoKey,
                          Object info,
                          long initTimeNs) {
            this(type, infoKey, info, System.nanoTime(), initTimeNs);
        }

        private EventInfo(Type type,
                          String infoKey,
                          Object info,
                          long timeNs,
                          long initTimeNs) {
            this.type = type;
            this.infoKey = infoKey;
            this.info = info;
            this.elapsedNs = timeNs - initTimeNs;
        }

        @Override
        public String toString() {
            return toJson().toString();
        }

        public JsonObject toJson() {
            final JsonObject result = new JsonObject();
            result.addProperty("type", type.toString());
            result.addProperty("elapsedNanos", elapsedNs);
            if (infoKey != null) {
                if (info instanceof Boolean) {
                    result.addProperty(infoKey, (Boolean) info);
                } else if (info instanceof Number) {
                    result.addProperty(infoKey, (Number) info);
                } else {
                    result.addProperty(infoKey, info.toString());
                }
            }
            return result;
        }
    }
}
