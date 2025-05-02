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

package oracle.kv.impl.async;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.async.perf.IOBufferPoolUsagePerf;
import oracle.kv.impl.async.perf.MetricStatsImpl;
import oracle.nosql.common.sklogger.measure.LongCappedPercentileElement;
import oracle.nosql.common.sklogger.measure.LongValueStats;

import com.google.gson.JsonObject;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A byte buffer pool to reduce the allocation pressure of input and output
 * buffers.
 *
 * The pool employs a leaking detection mechanism. For each pool we constantly
 * collect the min of the number of allocated-but-not-deallocated over a
 * period. If the min is zero, then we are sure there is no leak before the
 * time of that period, otherwise, there is a possibility of leaking and we
 * print the stats to warn.
 *
 * TODO: Currently we do not discard and gc byte buffers in the pool once they
 * are created. This is fine since we have a limited amount of buffers in the
 * pool. If this becomes a problem in the future, we will want to discard and
 * them. However, when discarded, these buffers will become tenured generation
 * garbage, which may need some thinking on the impact of gc.
 */
public class IOBufferPool {

    /* The property to enable pooling */
    private static final String BUFPOOL_DISABLED =
        "oracle.kv.async.bufpool.disabled";
    private static final boolean bufPoolDisabled =
        Boolean.getBoolean(BUFPOOL_DISABLED);

    /* Properties for setting pool buffer size */
    private static final String CHANNEL_INPUT_BUF_SIZE =
        "oracle.kv.async.bufpool.channelinput.bufsize";
    private static final int CHANNEL_INPUT_BUF_SIZE_DEFAULT = 4096;
    private static final int channelInputBufSize = Integer.getInteger(
            CHANNEL_INPUT_BUF_SIZE, CHANNEL_INPUT_BUF_SIZE_DEFAULT);

    private static final String MESSAGE_OUTPUT_BUF_SIZE =
        "oracle.kv.async.bufpool.messageoutput.bufsize";
    private static final int MESSAGE_OUTPUT_BUF_SIZE_DEFAULT = 128;
    private static final int messageOutputBufSize = Integer.getInteger(
            MESSAGE_OUTPUT_BUF_SIZE, MESSAGE_OUTPUT_BUF_SIZE_DEFAULT);

    private static final String CHANNEL_OUTPUT_BUF_SIZE =
        "oracle.kv.async.bufpool.channeloutput.bufsize";
    private static final int CHANNEL_OUTPUT_BUF_SIZE_DEFAULT = 64;
    private static final int channelOutputBufSize = Integer.getInteger(
            CHANNEL_OUTPUT_BUF_SIZE, CHANNEL_OUTPUT_BUF_SIZE_DEFAULT);

    /*
     * Properties for pool size.
     *
     * Currently, the pool will never shrink after it is expanded. We limit the
     * size of the pool with a maximum pool size, which is the min of a fixed
     * size and a certain percentage of the JVM memory.
     *
     * Alternatively, we could add a mechanism that shrinks the pool when most
     * of the free buffers are not likely to be used. However, shrinking means
     * throwing the buffers as garbage which may likely to be of long-term.
     * Therefore we will need an algorithm to make sure the shrink operation
     * will not burden the garbage collector too much which is difficult.
     * Currently we do not implement these mechansims.
     */
    private static final String IO_BUFPOOL_SIZE_HEAP_PERCENT =
        "oracle.kv.async.bufpool.size.heap.percentage";
    private static final int IO_BUFPOOL_SIZE_HEAP_PERCENT_DEFAULT = 1;
    private static final long IO_BUFPOOL_SIZE_MAX_DEFAULT = 1024 * 1024 * 1024;
    private static final int poolSizeHeapPercent = Integer.getInteger(
            IO_BUFPOOL_SIZE_HEAP_PERCENT, IO_BUFPOOL_SIZE_HEAP_PERCENT_DEFAULT);
    private static final long maxPoolBytes =
        Math.min((Runtime.getRuntime().maxMemory() * poolSizeHeapPercent) /
                 100,
                 IO_BUFPOOL_SIZE_MAX_DEFAULT);

    /* The buffer pool for all channel input */
    public static final IOBufferPool CHNL_IN_POOL =
        new IOBufferPool("Channel input", channelInputBufSize);
    /* The buffer pool for all message output */
    public static final IOBufferPool MESG_OUT_POOL =
        new IOBufferPool("Message output", messageOutputBufSize);
    /* The buffer pool for all channel output */
    public static final IOBufferPool CHNL_OUT_POOL =
        new IOBufferPool("Channel output", channelOutputBufSize);

    /* Name of the pool */
    private final String name;
    /* Size of the pool buffers */
    protected final int bufsize;
    /* Buffers that can be allocated for use */
    private final Deque<ByteBuffer> freeBufs = new ConcurrentLinkedDeque<>();
    /* Maximum amount of buffers to allocate */
    private volatile long maxPoolSize;
    /*
     * Current amount of buffers allocated that can be put in freeBufs,
     * currPoolSize <= maxPoolSize
     */
    private final AtomicInteger currPoolSize = new AtomicInteger(0);
    /*
     * The number of buffers allocated in use, i.e., not in freeBufs. Total
     * number of buffers not garbage = inUse + freeBufs.size(). Used for stats.
     */
    private final AtomicInteger inUse = new AtomicInteger(0);
    /*
     * The measurement element for the percentage of buffers in use
     * against the maximum pool size.
     */
    private final LongCappedPercentileElement inUsePercent =
        new LongCappedPercentileElement(100);

    /**
     * Constructs the pool.
     */
    protected IOBufferPool(String name, int bufsize) {
        this.name = name;
        this.bufsize = bufsize;
        this.maxPoolSize = maxPoolBytes / bufsize;
    }

    public String getName() {
        return name;
    }

    /**
     * Allocates a byte buffer from the pool.
     *
     * The byte buffer should be deallocated after use.
     *
     * @return the byte buffer, null if the pool cannot allocate any
     */
    @Nullable ByteBuffer allocPooled() {
        if (bufPoolDisabled) {
            return allocDiscarded();
        }
        final ByteBuffer buf = allocate();
        if (buf != null) {
            final int val = inUse.incrementAndGet();
            inUsePercent.observe(val * 100 / maxPoolSize);
        }
        return buf;
    }

    private @Nullable ByteBuffer allocate() {
        while (true) {
            ByteBuffer buf = freeBufs.poll();
            if (buf != null) {
                return buf;
            }
            final int size = currPoolSize.get();
            if (size >= maxPoolSize) {
                return null;
            }
            if (currPoolSize.compareAndSet(size, size + 1)) {
                return ByteBuffer.allocate(bufsize);
            }
        }
    }

    /**
     * Allocates a byte buffer from the heap.
     *
     * The byte buffer should not be deallocated after use.
     *
     * @return the byte buffer
     */
    ByteBuffer allocDiscarded() {
        return ByteBuffer.allocate(bufsize);
    }

    /**
     * Deallocates a byte buffer.
     *
     * @param buffer the byte buffer
     */
    void deallocate(ByteBuffer buffer) {
        if (bufPoolDisabled) {
            return;
        }
        buffer.clear();
        freeBufs.push(buffer);
        /* Update for leak detection */
        final int val = inUse.decrementAndGet();
        inUsePercent.observe(val * 100 / maxPoolSize);
    }

    /**
     * Returns the measurement element for the percentage of buffers in use.
     */
    public LongCappedPercentileElement getInUsePercentElement() {
        return inUsePercent;
    }

    /**
     * Returns the in use percentage of the three static pools.
     */
    public static IOBufferPoolUsagePerf obtain(
        String watcherName, boolean clear)
    {
        return new IOBufferPoolUsagePerf(
            CHNL_IN_POOL.getInUsePercentElement().obtain(watcherName, clear),
            MESG_OUT_POOL.getInUsePercentElement().obtain(watcherName, clear),
            CHNL_OUT_POOL.getInUsePercentElement().obtain(watcherName, clear));
    }

    /**
     * The in-use percent metric.
     */
    public static class InUsePercentMetrics implements Serializable {

        private static final long serialVersionUID = 1L;

        private final MetricStatsImpl chnlInResult;
        private final MetricStatsImpl mesgOutResult;
        private final MetricStatsImpl chnlOutResult;

        private InUsePercentMetrics(
            LongValueStats chnlInResult,
            LongValueStats mesgOutResult,
            LongValueStats chnlOutResult)
        {
            this.chnlInResult = new MetricStatsImpl(chnlInResult);
            this.mesgOutResult = new MetricStatsImpl(mesgOutResult);
            this.chnlOutResult = new MetricStatsImpl(chnlOutResult);
        }

        public JsonObject toJson() {
            final JsonObject result = new JsonObject();
            result.add("CHNL_IN_POOL", chnlInResult.toJson());
            result.add("MESG_OUT_POOL", mesgOutResult.toJson());
            result.add("CHNL_OUT_POOL", chnlOutResult.toJson());
            return result;
        }
    }

    /**
     * Returns the current number of allocated-but-not-deallocated buffers.
     *
     * For testing.
     */
    public int getNumInUse() {
        return inUse.get();
    }

    /**
     * Clears the number for tests.
     */
    public void clearUse() {
        inUse.set(0);
        freeBufs.clear();
    }

    /**
     * Sets the max pool size for testing.
     */
    public void setMaxPoolSize(long val) {
        if (val <= 0) {
            throw new IllegalArgumentException(
                "Max pool size must be larger than zero");
        }
        maxPoolSize = val;
    }

    public static String getSharedBufferUsageString() {
        return String.format("CHNL_IN=%s, MESG_OUT=%s, CHNL_OUT=%s",
            CHNL_IN_POOL.getNumInUse(), MESG_OUT_POOL.getNumInUse(),
            CHNL_OUT_POOL.getNumInUse());
    }
}
