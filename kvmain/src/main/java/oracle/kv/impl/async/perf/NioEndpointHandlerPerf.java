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
import oracle.nosql.common.sklogger.measure.LatencyElement;
import oracle.nosql.common.sklogger.measure.ThroughputElement;

/**
 * A collection of NioEndpointHandler perf metrics.
 */
public class NioEndpointHandlerPerf extends ObjectNodeSerializable {

    /** The field map. */
    public static final Map<String, Field<?, NioEndpointHandlerPerf>> FIELDS =
        new HashMap<>();
    /** The remote network address of the endpoint. */
    public static final String REMOTE_ADDRESS =
        StringField.create(
            FIELDS, "remoteAddress",
            NioEndpointHandlerPerf::getRemoteAddress);
    /** The throughput of handler read callback invocation. */
    public static final String HANDLER_READ_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "handlerReadThroughput",
            NioEndpointHandlerPerf::getHandlerReadThroughput);
    /** The throughput of handler write callback invocation. */
    public static final String HANDLER_WRITE_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "handlerWriteThroughput",
            NioEndpointHandlerPerf::getHandlerWriteThroughput);
    /** The throughput of channel read invocation. */
    public static final String CHANNEL_READ_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "channelReadThroughput",
            NioEndpointHandlerPerf::getChannelReadThroughput);
    /** The throughput of channel write invocation. */
    public static final String CHANNEL_WRITE_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "channelWriteThroughput",
            NioEndpointHandlerPerf::getChannelWriteThroughput);
    /** The throughput of channel read number of bytes. */
    public static final String CHANNEL_BYTES_READ_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "channelBytesReadThroughput",
            NioEndpointHandlerPerf::getChannelBytesReadThroughput);
    /** The throughput of channel write number of bytes. */
    public static final String CHANNEL_BYTES_WRITE_THROUGHPUT =
        ThroughputElement.ResultField.create(
            FIELDS, "channelBytesWriteThroughput",
            NioEndpointHandlerPerf::getChannelBytesWriteThroughput);
    /** The latency of handler read callback invocation. */
    public static final String HANDLER_READ_LATENCY =
        LatencyElement.ResultField.create(
            FIELDS, "handlerReadLatency",
            NioEndpointHandlerPerf::getHandlerReadLatency);
    /** The latency of handler write callback invocation. */
    public static final String HANDLER_WRITE_LATENCY =
        LatencyElement.ResultField.create(
            FIELDS, "handlerWriteLatency",
            NioEndpointHandlerPerf::getHandlerWriteLatency);
    /** The latency of channel read invocation. */
    public static final String CHANNEL_READ_LATENCY =
        LatencyElement.ResultField.create(
            FIELDS, "channelReadLatency",
            NioEndpointHandlerPerf::getChannelReadLatency);
    /** The latency of channel write invocation. */
    public static final String CHANNEL_WRITE_LATENCY =
        LatencyElement.ResultField.create(
            FIELDS, "channelWriteLatency",
            NioEndpointHandlerPerf::getChannelWriteLatency);
    /** The default. */
    public static final NioEndpointHandlerPerf DEFAULT =
        new NioEndpointHandlerPerf(
            StringField.DEFAULT,
            ThroughputElement.Result.DEFAULT,
            ThroughputElement.Result.DEFAULT,
            ThroughputElement.Result.DEFAULT,
            ThroughputElement.Result.DEFAULT,
            ThroughputElement.Result.DEFAULT,
            ThroughputElement.Result.DEFAULT,
            LatencyElement.Result.DEFAULT,
            LatencyElement.Result.DEFAULT,
            LatencyElement.Result.DEFAULT,
            LatencyElement.Result.DEFAULT);

    /* Fields */

    private final String remoteAddress;
    private final ThroughputElement.Result handlerReadThroughput;
    private final ThroughputElement.Result handlerWriteThroughput;
    private final ThroughputElement.Result channelReadThroughput;
    private final ThroughputElement.Result channelWriteThroughput;
    private final ThroughputElement.Result channelBytesReadThroughput;
    private final ThroughputElement.Result channelBytesWriteThroughput;
    private final LatencyElement.Result handlerReadLatency;
    private final LatencyElement.Result handlerWriteLatency;
    private final LatencyElement.Result channelReadLatency;
    private final LatencyElement.Result channelWriteLatency;

    public NioEndpointHandlerPerf(
        String remoteAddress,
        ThroughputElement.Result handlerReadThroughput,
        ThroughputElement.Result handlerWriteThroughput,
        ThroughputElement.Result channelReadThroughput,
        ThroughputElement.Result channelWriteThroughput,
        ThroughputElement.Result channelBytesReadThroughput,
        ThroughputElement.Result channelBytesWriteThroughput,
        LatencyElement.Result handlerReadLatency,
        LatencyElement.Result handlerWriteLatency,
        LatencyElement.Result channelReadLatency,
        LatencyElement.Result channelWriteLatency)
    {
        this.remoteAddress = remoteAddress;
        this.handlerReadThroughput = handlerReadThroughput;
        this.handlerWriteThroughput = handlerWriteThroughput;
        this.channelReadThroughput = channelReadThroughput;
        this.channelWriteThroughput = channelWriteThroughput;
        this.channelBytesReadThroughput = channelBytesReadThroughput;
        this.channelBytesWriteThroughput = channelBytesWriteThroughput;
        this.handlerReadLatency = handlerReadLatency;
        this.handlerWriteLatency = handlerWriteLatency;
        this.channelReadLatency = channelReadLatency;
        this.channelWriteLatency = channelWriteLatency;
    }

    public NioEndpointHandlerPerf(JsonNode payload) {
        this(readField(FIELDS, payload, REMOTE_ADDRESS),
             readField(FIELDS, payload, HANDLER_READ_THROUGHPUT),
             readField(FIELDS, payload, HANDLER_WRITE_THROUGHPUT),
             readField(FIELDS, payload, CHANNEL_READ_THROUGHPUT),
             readField(FIELDS, payload, CHANNEL_WRITE_THROUGHPUT),
             readField(FIELDS, payload, CHANNEL_BYTES_READ_THROUGHPUT),
             readField(FIELDS, payload, CHANNEL_BYTES_WRITE_THROUGHPUT),
             readField(FIELDS, payload, HANDLER_READ_LATENCY),
             readField(FIELDS, payload, HANDLER_WRITE_LATENCY),
             readField(FIELDS, payload, CHANNEL_READ_LATENCY),
             readField(FIELDS, payload, CHANNEL_WRITE_LATENCY));
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }

    /* Getters */

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public ThroughputElement.Result getHandlerReadThroughput() {
        return handlerReadThroughput;
    }

    public ThroughputElement.Result getHandlerWriteThroughput() {
        return handlerWriteThroughput;
    }

    public ThroughputElement.Result getChannelReadThroughput() {
        return channelReadThroughput;
    }

    public ThroughputElement.Result getChannelWriteThroughput() {
        return channelWriteThroughput;
    }

    public ThroughputElement.Result getChannelBytesReadThroughput() {
        return channelBytesReadThroughput;
    }

    public ThroughputElement.Result getChannelBytesWriteThroughput() {
        return channelBytesWriteThroughput;
    }

    public LatencyElement.Result getHandlerReadLatency() {
        return handlerReadLatency;
    }

    public LatencyElement.Result getHandlerWriteLatency() {
        return handlerWriteLatency;
    }

    public LatencyElement.Result getChannelReadLatency() {
        return channelReadLatency;
    }

    public LatencyElement.Result getChannelWriteLatency() {
        return channelWriteLatency;
    }
}

