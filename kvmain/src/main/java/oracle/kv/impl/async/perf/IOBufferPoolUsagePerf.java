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
import java.util.Map;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;
import oracle.nosql.common.sklogger.measure.LongValueStats;
import oracle.nosql.common.sklogger.measure.LongValueStatsField;

/**
 * A collection of IO buffer pool usage perf metrics.
 */
public class IOBufferPoolUsagePerf extends ObjectNodeSerializable {

    /** The field map. */
    public static final Map<String, Field<?, IOBufferPoolUsagePerf>> FIELDS =
        new HashMap<>();
    /**
     * The pool usage percentage of channel input buffers. Channel input buffers
     * are the ones used to grab OS network bytes. See {@link
     * oracle.kv.impl.async.dialog.ChannelInput} for more detail.
     */
    public static final String CHANNEL_INPUT_USE_PERCENTAGE =
        LongValueStatsField.create(
            FIELDS, "channelInputUsePercentage",
            (o) -> o.channelInputUsePercentage);
    /**
     * The pool usage percentage of message output buffers. Message output
     * buffers are the ones used to marshall bytes from the user and async
     * request layer (e.g., primitive data of async request messages). See
     * {@link oracle.kv.impl.async.MessageOutput} for more detail.
     */
    public static final String MESSAGE_OUTPUT_USE_PERCENTAGE =
        LongValueStatsField.create(
            FIELDS, "messageOutputUsePercentage",
            (o) -> o.messageOutputUsePercentage);
    /**
     * The pool usage percentage of channel output buffers. Channel output
     * buffers are the ones used to marshall bytes that will be put into the OS
     * network bytes (e.g., dialog message header data). See {@link
     * oracle.kv.impl.async.dialog.ChannelOutput} for more detail.
     */
    public static final String CHANNEL_OUTPUT_USE_PERCENTAGE =
        LongValueStatsField.create(
            FIELDS, "channelOutputUsePercentage",
            (o) -> o.channelOutputUsePercentage);
    /** The default. */
    public static final IOBufferPoolUsagePerf DEFAULT =
        new IOBufferPoolUsagePerf(LongValueStats.DEFAULT,
                                  LongValueStats.DEFAULT,
                                  LongValueStats.DEFAULT);

    private final LongValueStats channelInputUsePercentage;
    private final LongValueStats messageOutputUsePercentage;
    private final LongValueStats channelOutputUsePercentage;

    public IOBufferPoolUsagePerf(
        LongValueStats channelInputUsePercentage,
        LongValueStats messageOutputUsePercentage,
        LongValueStats channelOutputUsePercentage)
    {
        this.channelInputUsePercentage = channelInputUsePercentage;
        this.messageOutputUsePercentage = messageOutputUsePercentage;
        this.channelOutputUsePercentage = channelOutputUsePercentage;
    }

    public IOBufferPoolUsagePerf(JsonNode payload) {
        this(readField(FIELDS, payload, CHANNEL_INPUT_USE_PERCENTAGE),
             readField(FIELDS, payload, MESSAGE_OUTPUT_USE_PERCENTAGE),
             readField(FIELDS, payload, CHANNEL_OUTPUT_USE_PERCENTAGE));
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }
}

