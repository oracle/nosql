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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.JsonSerializationUtils;
import oracle.nosql.common.jss.ObjectNodeSerializable;

/**
 * Represents the dialog-event related objects.
 */
public class DialogEvent {

    public enum Type {
        UNKNOWN, INIT, WRITE, SEND, RECV, READ, FIN, ABORT
    }

    /**
     * A event info with a type, elapsed time and some attributes.
     */
    public static class Info extends ObjectNodeSerializable {

        /** The field map. */
        public static final Map<String, Field<?, Info>> FIELDS =
            new HashMap<>();
        /** The type of the info. */
        public static final String TYPE =
            Field.create(FIELDS, "type",
                         Info::writeType,
                         Info::readType,
                         Type.UNKNOWN);
        /** The elapsed time of the event in nano-seconds. */
        public static final String ELAPSED_TIME =
            LongField.create(FIELDS, "elapsedNanos", (o) -> o.elapsedNanos);
        /** The attributes of the event. */
        public static final String ATTRIBUTES =
            ObjectNodeField.create(FIELDS, "attributes", (o) -> o.attributes);

        private static void writeType(Info info,
                                      ObjectNode payload,
                                      String name,
                                      Type defaultValue) {
            JsonSerializationUtils.writeString(
                payload, name, info.type.toString(),
                defaultValue.toString());
        }

        private static Type readType(JsonNode payload, Type defaultValue) {
            if (!payload.isString()) {
                return defaultValue;
            }
            try {
                return Type.valueOf(payload.asText());
            } catch (IllegalArgumentException e) {
                return defaultValue;
            }
        }

        private final Type type;
        private final long elapsedNanos;
        private final ObjectNode attributes;

        Info(Type type, long initTimeNs) {
            this(type, initTimeNs, System.nanoTime());
        }

        Info(Type type, long initTimeNs, long timeNs) {
            this(type, timeNs - initTimeNs, JsonUtils.createObjectNode());
        }

        Info(Type type, long elapsedNanos, ObjectNode attributes) {
            this.type = type;
            this.elapsedNanos = elapsedNanos;
            this.attributes = attributes;
        }

        Info(JsonNode payload) {
            this(readField(FIELDS, payload, TYPE),
                 readField(FIELDS, payload, ELAPSED_TIME),
                /*
                 * Cast so that the compiler can figure out the correct
                 * constructor to use because we have a collision of the
                 * constructor signatures. Without the collision, the compiler
                 * can automatically figure out what type the readField
                 * returns. A defaultValue with the correct type would be
                 * provided during run time if the payload has an incompatible
                 * data type for the field.
                 */
                 (ObjectNode) readField(FIELDS, payload, ATTRIBUTES));
        }

        Info addAttribute(String key, Object value) {
            if (value instanceof Boolean) {
                attributes.put(key, (Boolean) value);
            } else if (value instanceof Number) {
                attributes.put(key, (Number) value);
            } else {
                attributes.put(key, value.toString());
            }
            return this;
        }

        @Override
        public ObjectNode toJson() {
            return writeFields(FIELDS, this);
        }

        public Type getType() {
            return type;
        }

        public long getElapsedNanos() {
            return elapsedNanos;
        }
    }

    /**
     * Representing a span of time between two consecutive events.
     */
    public static class Span {

        final Type startEvent;
        final Type endEvent;

        public Span(Type startEvent, Type endEvent) {
            this.startEvent = startEvent;
            this.endEvent = endEvent;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Span)) {
                return false;
            }
            final Span that = (Span) other;
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

        /**
         * Creates the event span from the provided string.
         */
        public static Span create(String string) {
            final String[] parts = string.split("-");
            if (parts.length != 2) {
                return null;
            }
            try {
                return new Span(
                    Type.valueOf(parts[0]), Type.valueOf(parts[1]));
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    /**
     * Compares spans.
     */
    public static class SpanComparator implements Comparator<Span> {

        public static final SpanComparator FOR_CREATOR = new SpanComparator(true);
        public static final SpanComparator FOR_RESPONDER = new SpanComparator(false);

        private static final Map<Type, Integer> CREATOR_ORDER = new HashMap<>();
        private static final Map<Type, Integer> RESPONDER_ORDER = new HashMap<>();

        static {
            CREATOR_ORDER.put(Type.INIT, 0);
            CREATOR_ORDER.put(Type.WRITE, 1);
            CREATOR_ORDER.put(Type.SEND, 2);
            CREATOR_ORDER.put(Type.RECV, 3);
            CREATOR_ORDER.put(Type.READ, 4);
            CREATOR_ORDER.put(Type.FIN, 5);
            CREATOR_ORDER.put(Type.ABORT, 6);

            RESPONDER_ORDER.put(Type.INIT, 0);
            RESPONDER_ORDER.put(Type.RECV, 1);
            RESPONDER_ORDER.put(Type.READ, 2);
            RESPONDER_ORDER.put(Type.WRITE, 3);
            RESPONDER_ORDER.put(Type.SEND, 4);
            RESPONDER_ORDER.put(Type.FIN, 5);
            RESPONDER_ORDER.put(Type.ABORT, 6);
        }

        private final boolean isCreator;

        private SpanComparator(boolean isCreator) {
            this.isCreator = isCreator;
        }

        @Override
        public int compare(Span s1, Span s2) {
            if (isCreator) {
                if (!s1.startEvent.equals(s2.startEvent)) {
                    return CREATOR_ORDER.get(s1.startEvent) -
                        CREATOR_ORDER.get(s2.startEvent);
                }
                return CREATOR_ORDER.get(s1.endEvent) -
                    CREATOR_ORDER.get(s2.endEvent);
            }
            if (!s1.startEvent.equals(s2.startEvent)) {
                return RESPONDER_ORDER.get(s1.startEvent) -
                    RESPONDER_ORDER.get(s2.startEvent);
            }
            return RESPONDER_ORDER.get(s1.endEvent) -
                RESPONDER_ORDER.get(s2.endEvent);
        }
    }
}
