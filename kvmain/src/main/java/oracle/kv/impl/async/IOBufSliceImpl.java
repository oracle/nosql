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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import oracle.kv.impl.util.EventTrackersManager;
import oracle.kv.impl.util.ObjectUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementations of {@link IOBufSlice}.
 *
 * We want different implementations for input and output to achieve memory
 * access optimization. The input logic from reading socket to MessageInput
 * reads are executed in a single thread. The output logic from MessageOutput
 * writes to socket writes can be executed in different threads. Therefore
 * input buffers can be optimized without thread-safety concerns, i.e., use
 * primitive integer instead of atomic one.
 *
 * Another implementation of the slice is the one that does not need to be
 * freed.
 */
public abstract class IOBufSliceImpl extends IOBufSliceList.Entry {

    private static final int MAX_NUM_BYTES_TOSTRING = 8;

    private final ByteBuffer buffer;
    protected final IOBufPoolTrackers.Tracker tracker;
    protected final String description;

    protected IOBufSliceImpl(ByteBuffer buffer,
                             IOBufPoolTrackers.Tracker tracker,
                             String description) {
        this.buffer = buffer;
        this.tracker = tracker;
        this.description = description;
    }

    @Override
    public ByteBuffer buf() {
        return buffer;
    }

    public IOBufPoolTrackers.Tracker getTracker() {
        return tracker;
    }

    @Override
    public IOBufSliceImpl forkAndAdvance(int len, String forkDescription) {
        if (len <= 0) {
            throw new IllegalArgumentException(
                          "Forking IOBufSlice with Invalid length: " + len);
        }
        final ByteBuffer buf = buf();
        final int advancedPos = buf.position() + len;
        final ByteBuffer childBuf = buf.duplicate();
        buf.position(advancedPos);
        childBuf.limit(advancedPos);
        return forkNewSlice(childBuf, forkDescription);
    }

    @Override
    public IOBufSliceImpl forkBackwards(int len, String forkDescription) {
        if ((len <= 0) || (len > buf().position())) {
            throw new IllegalArgumentException(
                          String.format("Forking IOBufSlice " +
                                        "with Invalid length: %d, " +
                                        "current position: %d",
                                        len, buf().position()));
        }
        final ByteBuffer buf = buf();
        final int pos = buf.position() - len;
        final int lim = buf.position();
        final ByteBuffer childBuf = buf.duplicate();
        childBuf.position(pos);
        childBuf.limit(lim);
        return forkNewSlice(childBuf, forkDescription);
    }

    /*
     * The IOBufPoolTrackers depends on IOBufSliceImpl to use identity
     * distinguish with each other and therefore, this class should not
     * override equals to anything else
     */
    @Override
    public final boolean equals(@Nullable Object other) {
        return this == other;
    }

    @Override
    public final int hashCode() {
        return System.identityHashCode(this);
    }

    /**
     * Creates a new slice with the specified byte buffer.
     *
     * @param buf the byte buffer
     * @param forkDescription the description
     * @return buffer slice
     */
    protected abstract IOBufSliceImpl forkNewSlice(ByteBuffer buf,
                                                   String forkDescription);

    /**
     * A buffer slice created from an input buffer pool.
     *
     * When the slice is freed, the underlying byte buffer is deallocated from
     * the pool.
     */
    public static class InputPoolBufSlice
        extends ThreadUnsafeRefCntPooledBufSlice {

        private final IOBufferPool pool;

        private InputPoolBufSlice(IOBufferPool pool,
                                  ByteBuffer buffer,
                                  IOBufPoolTrackers.Tracker tracker,
                                  String description) {
            super(buffer, tracker, description);
            this.pool = pool;
        }

        @Override
        protected int markFree(@Nullable PooledBufSlice childSlice) {
            final int cnt = super.markFree(childSlice);
            if (cnt == 0) {
                pool.deallocate(buf());
            }
            return cnt;
        }

        @Override
        protected @Nullable PooledBufSlice parent() {
            return null;
        }

        @Override
        protected IOBufSliceImpl forkNewSlice(ByteBuffer buffer,
                                              String forkDescription) {
            return new InputPoolForkedBufSlice(
                this, buffer, tracker, forkDescription);
        }

        /**
         * Creates a buffer slice from a input pool.
         *
         * If the pool can allocate new byte buffers, a {@link
         * InputPoolBufSlice} is created; otherwise, a {@link HeapBufSlice} is
         * created.
         */
        public static IOBufSliceImpl createFromPool(
            IOBufferPool pool,
            @Nullable EventTrackersManager trackersManager)
        {
            final ByteBuffer buffer = pool.allocPooled();
            if (buffer == null) {
                return new HeapBufSlice(pool.allocDiscarded());
            }
            return new InputPoolBufSlice(
                pool, buffer,
                IOBufPoolTrackers.createFromPool(
                    trackersManager,
                    IOBufPoolTrackers.TrackerType.CHANNEL_INPUT,
                    buffer),
                pool.getName());
        }
    }

    /**
     * A buffer slice created from an output buffer pool.
     *
     * When the slice is freed, the underlying byte buffer is deallocated from
     * the pool.
     */
    public static class OutputPoolBufSlice
        extends ThreadSafeRefCntPooledBufSlice {

        private final IOBufferPool pool;

        private OutputPoolBufSlice(IOBufferPool pool,
                                   ByteBuffer buffer,
                                   IOBufPoolTrackers.Tracker tracker,
                                   String description) {
            super(buffer, tracker, description);
            this.pool = pool;
        }

        @Override
        protected int markFree(@Nullable PooledBufSlice childSlice) {
            final int cnt = super.markFree(childSlice);
            if (cnt == 0) {
                pool.deallocate(buf());
            }
            return cnt;
        }

        @Override
        protected @Nullable PooledBufSlice parent() {
            return null;
        }

        @Override
        protected IOBufSliceImpl forkNewSlice(ByteBuffer buffer,
                                              String forkDescription) {
            return new OutputPoolForkedBufSlice(
                this, buffer, tracker, forkDescription);
        }

        /**
         * Creates a buffer slice from a input pool.
         *
         * If the pool can allocate new byte buffers, a {@link
         * OutputPoolBufSlice} is created; otherwise, a {@link HeapBufSlice} is
         * created.
         */
        public static IOBufSliceImpl createFromPool(
            IOBufferPool pool,
            IOBufPoolTrackers.TrackerType type,
            @Nullable EventTrackersManager trackersManager)
        {
            final ByteBuffer buffer = pool.allocPooled();
            if (buffer == null) {
                return new HeapBufSlice(pool.allocDiscarded());
            }
            return new OutputPoolBufSlice(
                pool, buffer,
                IOBufPoolTrackers.createFromPool(
                    trackersManager, type, buffer),
                pool.getName());
        }
    }

    /**
     * A heap allocated buffer slice that does not need to be actually freed.
     */
    public static class HeapBufSlice extends IOBufSliceImpl {

        public HeapBufSlice(ByteBuffer buffer) {
            super(buffer,
                  IOBufPoolTrackers.createFromHeap(null),
                  "heap");
        }

        @Override
        public void markFree() {
            /* do nothing */
        }

        @Override
        protected HeapBufSlice forkNewSlice(ByteBuffer buffer,
                                            String forkDescription) {
            return new HeapBufSlice(buffer);
        }

        @Override
        public String toString() {
            return BytesUtil.toString(
                       buf(), Math.min(buf().limit(), MAX_NUM_BYTES_TOSTRING));
        }

    }

    /**
     * A pooled buffer slice that needs to be freed.
     */
    private static abstract class PooledBufSlice extends IOBufSliceImpl {

        protected PooledBufSlice(ByteBuffer buffer,
                                 IOBufPoolTrackers.Tracker tracker,
                                 String description) {
            super(buffer, tracker, description);
            initRefCnt();
            tracker.markNew(this, getRefCnt());
        }

        @Override
        public IOBufSliceImpl forkAndAdvance(int len, String forkDescription) {
            final int refcnt = incRefCnt();
            final IOBufSliceImpl child =
                super.forkAndAdvance(len, forkDescription);
            tracker.markFork(this, refcnt, child);
            return child;
        }

        @Override
        public IOBufSliceImpl forkBackwards(int len, String forkDescription) {
            final int refcnt = incRefCnt();
            final IOBufSliceImpl child =
                super.forkBackwards(len, forkDescription);
            tracker.markFork(this, refcnt, child);
            return child;
        }

        @Override
        public void markFree() {
            markFree(null);
        }

        protected int markFree(@Nullable PooledBufSlice childSlice) {
            final int cnt = decRefCnt();
            if (childSlice != null) {
                tracker.markRelease(this, cnt, childSlice);
            } else {
                tracker.markFree(this, cnt);
            }
            if (cnt == 0) {
                final PooledBufSlice p = parent();
                if (p != null) {
                    p.markFree(this);
                }
            }
            return cnt;
        }

        @Override
        public String toString() {
            return String.format(
                "(id=%x parent=%x next=%x, ref=%d, buf=%x)%s",
                System.identityHashCode(this),
                System.identityHashCode(parent()),
                System.identityHashCode(next()),
                getRefCnt(),
                System.identityHashCode(buf()),
                BytesUtil.toString(
                    buf(),
                    Math.min(buf().limit(), MAX_NUM_BYTES_TOSTRING)));
        }

        /**
         * Returns the parent of the buf slice.
         *
         * @return the parent, which may be null
         */
        protected abstract @Nullable PooledBufSlice parent();

        /**
         * Initialize the reference count.
         */
        protected abstract void initRefCnt();


        /**
         * Increments the reference count.
         *
         * @return the resulted reference count
         */
        protected abstract int incRefCnt();

        /**
         * Decrements the reference count.
         *
         * @return the resulted reference count
         */
        protected abstract int decRefCnt();

        /**
         * Returns the reference count.
         */
        protected abstract int getRefCnt();
    }

    /**
     * A pooled buffer slice with thread-unsafe reference counting.
     */
    private static abstract class ThreadUnsafeRefCntPooledBufSlice
        extends PooledBufSlice {

        protected int refcnt;

        protected ThreadUnsafeRefCntPooledBufSlice(
            ByteBuffer buffer,
            IOBufPoolTrackers.Tracker tracker,
            String description)
        {
            super(buffer, tracker, description);
        }

        @Override
        protected void initRefCnt() {
            refcnt = 1;
        }

        @Override
        protected int incRefCnt() {
            return ++refcnt;
        }

        @Override
        protected int decRefCnt() {
            if (refcnt <= 0) {
                throw new IllegalStateException(
                              String.format("Invalid decRefCnt() on %s",
                                            this));
            }
            return --refcnt;
        }

        @Override
        protected int getRefCnt() {
            return refcnt;
        }
    }

    /**
     * A pooled buffer slice with thread-safe reference counting.
     */
    private static abstract class ThreadSafeRefCntPooledBufSlice
        extends PooledBufSlice {

        /*
         * Use a field updater to avoid creating an AtomicInteger for each
         * instance
         */
        private static final
            AtomicIntegerFieldUpdater<ThreadSafeRefCntPooledBufSlice>
            refCntUpdater = AtomicIntegerFieldUpdater.newUpdater(
                ThreadSafeRefCntPooledBufSlice.class, "refcnt");

        @SuppressWarnings("unused")
        protected volatile int refcnt;

        protected ThreadSafeRefCntPooledBufSlice(
            ByteBuffer buffer,
            IOBufPoolTrackers.Tracker tracker,
            String description)
        {
            super(buffer, tracker, description);
        }

        @Override
        protected void initRefCnt() {
            refCntUpdater.set(this, 1);
        }

        @Override
        protected int incRefCnt() {
            return refCntUpdater.incrementAndGet(this);
        }

        @Override
        protected int decRefCnt() {
            final int val = refCntUpdater.decrementAndGet(this);
            if (val < 0) {
                throw new IllegalStateException(
                              String.format(
                                  "Reference count value %d " +
                                  "less than zero for %s",
                                  val, getClass().getSimpleName()));
            }
            return val;
        }

        @Override
        protected int getRefCnt() {
            return refCntUpdater.get(this);
        }
    }

    /**
     * A buffer slice forked from a {@link InputPooledBufSlice} or its
     * offspring.
     */
    private static class InputPoolForkedBufSlice
        extends ThreadUnsafeRefCntPooledBufSlice {

        private final PooledBufSlice parent;

        protected InputPoolForkedBufSlice(
            PooledBufSlice parent,
            ByteBuffer buffer,
            IOBufPoolTrackers.Tracker tracker,
            String description)
        {
            super(buffer, tracker, description);
            ObjectUtil.checkNull("parent", parent);
            this.parent = parent;
        }

        @Override
        protected @Nullable PooledBufSlice parent() {
            return parent;
        }

        @Override
        protected IOBufSliceImpl forkNewSlice(ByteBuffer buffer,
                                              String forkDescription) {
            return new InputPoolForkedBufSlice(
                this, buffer, tracker, forkDescription);
        }

    }

    /**
     * A buffer slice forked from a {@link OutputPooledBufSlice} or its
     * offspring.
     */
    private static class OutputPoolForkedBufSlice
        extends ThreadSafeRefCntPooledBufSlice {

        private final PooledBufSlice parent;

        protected OutputPoolForkedBufSlice(
            PooledBufSlice parent,
            ByteBuffer buffer,
            IOBufPoolTrackers.Tracker tracker,
            String description)
        {
            super(buffer, tracker, description);
            ObjectUtil.checkNull("parent", parent);
            this.parent = parent;
        }

        @Override
        protected @Nullable PooledBufSlice parent() {
            return parent;
        }

        @Override
        protected IOBufSliceImpl forkNewSlice(ByteBuffer buffer,
                                              String forkDescription) {
            return new OutputPoolForkedBufSlice(
                this, buffer, tracker, forkDescription);
        }
    }
}
