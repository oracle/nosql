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

/**
 * Detailed performance metrics of a dialog.
 */
public class DialogInfoPerf extends ObjectNodeSerializable {

    /** The field map. */
    public static final Map<String, Field<?, DialogInfoPerf>> FIELDS =
        new HashMap<>();
    /** The dialog ID, e.g., 2ec806:4b4dfb9229c127ce. */
    public static final String ID =
        StringField.create(FIELDS, "id", (o) -> o.id);
    /** The total number of outgoing messages of this dialog. */
    public static final String OUTGOING_NUM_MESSAGES =
        LongField.create(FIELDS, "outgoingNumMessages",
                         (o) -> o.outgoingNumMessages);
    /** The total number of outgoing frames of this dialog. */
    public static final String OUTGOING_NUM_FRAMES =
        LongField.create(FIELDS, "outgoingNumFrames",
                         (o) -> o.outgoingNumFrames);
    /** The total number of outgoing bytes of this dialog. */
    public static final String OUTGOING_NUM_BYTES =
        LongField.create(FIELDS, "outgoingNumBytes",
                         (o) -> o.outgoingNumBytes);
    /** The total number of incoming messages of this dialog. */
    public static final String INCOMING_NUM_MESSAGES =
        LongField.create(FIELDS, "incomingNumMessages",
                         (o) -> o.incomingNumMessages);
    /** The total number of incoming frames of this dialog. */
    public static final String INCOMING_NUM_FRAMES =
        LongField.create(FIELDS, "incomingNumFrames",
                         (o) -> o.incomingNumFrames);
    /** The total number of incoming bytes of this dialog. */
    public static final String INCOMING_NUM_BYTES =
        LongField.create(FIELDS, "incomingNumBytes",
                         (o) -> o.incomingNumBytes);
    /** The boolean value indicating whether there are frame events dropped. */
    public static final String FRAME_EVENTS_DROPPED =
        BooleanField.create(FIELDS, "frameEventsDropped",
                            (o) -> o.frameEventsDropped);
    /** The detailed events and attributes. */
    public static final String EVENTS =
        FilteredListField.create(FIELDS, "events",
                                 (o) -> o.events,
                                 DialogEvent.Info::new);

    /* Fields */

    private final String id;
    private final long outgoingNumMessages;
    private final long outgoingNumFrames;
    private final long outgoingNumBytes;
    private final long incomingNumMessages;
    private final long incomingNumFrames;
    private final long incomingNumBytes;
    private final boolean frameEventsDropped;
    private final List<DialogEvent.Info> events;

    public DialogInfoPerf(String id,
                          long outgoingNumMessages,
                          long outgoingNumFrames,
                          long outgoingNumBytes,
                          long incomingNumMessages,
                          long incomingNumFrames,
                          long incomingNumBytes,
                          boolean frameEventsDropped,
                          List<DialogEvent.Info> events) {
        this.id = id;
        this.outgoingNumMessages = outgoingNumMessages;
        this.outgoingNumFrames = outgoingNumFrames;
        this.outgoingNumBytes = outgoingNumBytes;
        this.incomingNumMessages = incomingNumMessages;
        this.incomingNumFrames = incomingNumFrames;
        this.incomingNumBytes = incomingNumBytes;
        this.frameEventsDropped = frameEventsDropped;
        this.events = events;
    }

    public DialogInfoPerf(JsonNode payload) {
        this(readField(FIELDS, payload, ID),
             readField(FIELDS, payload, OUTGOING_NUM_MESSAGES),
             readField(FIELDS, payload, OUTGOING_NUM_FRAMES),
             readField(FIELDS, payload, OUTGOING_NUM_BYTES),
             readField(FIELDS, payload, INCOMING_NUM_MESSAGES),
             readField(FIELDS, payload, INCOMING_NUM_FRAMES),
             readField(FIELDS, payload, INCOMING_NUM_BYTES),
             readField(FIELDS, payload, FRAME_EVENTS_DROPPED),
             readField(FIELDS, payload, EVENTS));
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }
}
