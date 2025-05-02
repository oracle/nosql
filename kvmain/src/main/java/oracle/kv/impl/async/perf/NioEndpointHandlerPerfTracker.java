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

import oracle.kv.impl.async.NetworkAddress;
import oracle.nosql.common.sklogger.measure.LatencyElement;
import oracle.nosql.common.sklogger.measure.ThroughputElement;

/**
 * Tracks the performance metrics of NioEndpointHandler.
 */
public class NioEndpointHandlerPerfTracker {

    /**
     * A hidden system property to specify the {@code latencySampleRate}.  We
     * do not expect the value to change. The value adjust the performance
     * impact for collecting the channel op latency stats. The default value
     * should make it sufficient that the impact is negeligible. The actual
     * value used is always the maximum pow of 2 value that is less than or
     * equal to the specified.
     */
    public static final String LATENCY_SAMPLE_RATE =
        "oracle.kv.async.perf.endpoint.handler.latency.sample.rate";
    public static final int LATENCY_SAMPLE_RATE_DEFAULT = 1024;

    /** The remote address. */
    private final NetworkAddress remoteAddress;

    /**
     * The throughput of invocations of handler read. This throughput will be
     * on the same scale with upper layer request/operation throughput.
     */
    private ThroughputElement handlerReadThroughput =
        new ThroughputElement();
    /**
     * The throughput of invocations of handler write. This throughput will be
     * on the same scale with upper layer request/operation throughput.
     */
    private ThroughputElement handlerWriteThroughput =
        new ThroughputElement();
    /**
     * The throughput of invocations of DataChannel#read. This throughput
     * will be a multiply of readThroughput since each read may do multiple
     * DataChannel read until there is no more data.
     */
    private ThroughputElement channelReadThroughput =
        new ThroughputElement();
    /**
     * The throughput of invocations of DataChannel#write. This throughput will
     * be a multiply of writeThroughput since each write may do multiple
     * DataChannel write until the network buffer is full.
     */
    private ThroughputElement channelWriteThroughput =
        new ThroughputElement();
    /**
     * The throughput of bytes read from the channel.
     */
    private ThroughputElement channelBytesReadThroughput =
        new ThroughputElement();
    /**
     * The throughput of bytes written to the channel.
     */
    private ThroughputElement channelBytesWriteThroughput =
        new ThroughputElement();
    /** The channel op latency sample rate. */
    private final int latencySampleRate =
        Integer.getInteger(LATENCY_SAMPLE_RATE,
                           LATENCY_SAMPLE_RATE_DEFAULT);
    private final long latencySampleRateHighestOneBit =
        PerfUtil.getSampleRateHighestOneBit(latencySampleRate);
    /** The latency for handler read. */
    private LatencyElement handlerReadLatency = new LatencyElement();
    /** The latency element for channel read.  */
    private LatencyElement channelReadLatency = new LatencyElement();
    /** The latency for handler write. */
    private LatencyElement handlerWriteLatency = new LatencyElement();
    /** The latency element for channel write.  */
    private LatencyElement channelWriteLatency = new LatencyElement();

    /**
     * Constructs the perf tracker.
     */
    public NioEndpointHandlerPerfTracker(NetworkAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    /**
     * Obtains the start timestamp of NioEndpointHandler#onRead. Returns the
     * current time in nanos if sampled, -1 otherwise.
     */
    public long getHandlerReadStartTimestamp() {
        if (!PerfUtil.shouldSample(
            handlerReadThroughput.getCurrentCount(),
            latencySampleRateHighestOneBit)) {
            return -1;
        }
        return System.nanoTime();
    }

    /**
     * Marks the finish of NioEndpointHandler#onRead.
     */
    public void markHandlerReadFinish(long startTimeNanos) {
        handlerReadThroughput.observe(1);
        if (startTimeNanos != -1) {
            handlerReadLatency.observe(System.nanoTime() - startTimeNanos);
        }
    }

    /**
     * Obtains the channel read start timestamp. Returns the current time in
     * nanos if sampled, -1 otherwise.
     */
    public long getChannelReadStartTimestamp() {
        if (!PerfUtil.shouldSample(
            channelReadThroughput.getCurrentCount(),
            latencySampleRateHighestOneBit)) {
            return -1;
        }
        return System.nanoTime();
    }

    /**
     * Marks the finish of channel read.
     */
    public void markChannelReadFinish(long startTimeNanos, long n) {
        channelReadThroughput.observe(1);
        channelBytesReadThroughput.observe(n);
        if (startTimeNanos != -1) {
            channelReadLatency.observe(System.nanoTime() - startTimeNanos);
        }
    }

    /**
     * Obtains the NioEndpointHandler#onWrite start timestamp. Returns the
     * current time in nanos if sampled, -1 otherwise.
     */
    public long getHandlerWriteStartTimestamp() {
        if (!PerfUtil.shouldSample(
            handlerWriteThroughput.getCurrentCount(),
            latencySampleRateHighestOneBit)) {
            return -1;
        }
        return System.nanoTime();
    }

    /**
     * Marks the finish of NioEndpointHandler#onWrite.
     */
    public void markHandlerWriteFinish(long startTimeNanos) {
        handlerWriteThroughput.observe(1);
        if (startTimeNanos != -1) {
            handlerWriteLatency.observe(System.nanoTime() - startTimeNanos);
        }
    }

    /**
     * Obtains the channel write start timestamp. Returns the current time in
     * nanos if sampled, -1 otherwise.
     */
    public long getChannelWriteStartTimestamp() {
        if (!PerfUtil.shouldSample(
            channelWriteThroughput.getCurrentCount(),
            latencySampleRateHighestOneBit)) {
            return -1;
        }
        return System.nanoTime();
    }

    /**
     * Marks the finish of channel write.
     */
    public void markChannelWriteFinish(long startTimeNanos, long n) {
        channelWriteThroughput.observe(1);
        channelBytesWriteThroughput.observe(n);
        if (startTimeNanos != -1) {
            channelWriteLatency.observe(System.nanoTime() - startTimeNanos);
        }
    }

    /**
     * Returns the endpoint handler perf since last time.
     */
    public NioEndpointHandlerPerf obtain(String watcherName, boolean clear) {
        return new NioEndpointHandlerPerf(
            remoteAddress.toString(),
            handlerReadThroughput.obtain(watcherName, clear),
            handlerWriteThroughput.obtain(watcherName, clear),
            channelReadThroughput.obtain(watcherName, clear),
            channelWriteThroughput.obtain(watcherName, clear),
            channelBytesReadThroughput.obtain(watcherName, clear),
            channelBytesWriteThroughput.obtain(watcherName, clear),
            handlerReadLatency.obtain(watcherName, clear),
            handlerWriteLatency.obtain(watcherName, clear),
            channelReadLatency.obtain(watcherName, clear),
            channelWriteLatency.obtain(watcherName, clear));
    }
}
