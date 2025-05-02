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
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Consumer;

import oracle.kv.impl.util.ObjectUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A singly linked list of {@link IOBufSliceList.Entry}.
 *
 * This class uses the next pointer in the entries to thread the list for the
 * following optimization purposes:
 * - traversing the entries will not need an iterator relieving gc pressure
 * - extending one list with another will only need O(1) complexity instead of
 *   O(N) as for the java lists
 *
 * Objects of subclasses of {@link IOBufSliceList.Entry} are expected to only
 * be part of a single {@link IOBufSliceList}.
 *
 * The class methods are not thread safe.
 */
public class IOBufSliceList {

    /*
     * A sentinel node to avoid handling null pointers and hence simplify code.
     * The first node is always the sentinel node (instead of null when list is
     * empty).
     */
    private final Entry sentinel = new Sentinel();
    /* Last entry if list not empty, null if otherwise */
    private @Nullable Entry tail = null;
    private int size = 0;

    /*
     * Keeps a list of slices that has a non-NullTracker. This way we can set
     * the information of those slices in a optimized manner so that we do not
     * need to iterate through the list finding sampled slices. The list could
     * be long, e.g., when there are multiple dialogs each with messages of
     * multiple scattered data.
     */
    private @Nullable Queue<IOBufSliceImpl> nonNullTrackerSlices = null;

    /**
     * Gets the first entry, null if list is empty.
     */
    public @Nullable Entry head() {
        return sentinel.next();
    }

    /**
     * Gets the last entry, null if list is empty.
     */
    public @Nullable Entry tail() {
        return tail;
    }

    /**
     * Adds an entry to the end of the list.
     */
    public void add(Entry entry) {
        ObjectUtil.checkNull("entry", entry);
        final @Nullable Entry t = tail;
        if (t == null) {
            sentinel.next(entry);
        } else {
            t.next(entry);
        }
        entry.next(null);
        tail = entry;
        size ++;

        final IOBufSliceImpl slice = (IOBufSliceImpl) entry;
        if (!slice.getTracker().isNull()) {
            initNonNullTrackerSlices().add(slice);
        }
    }

    private Queue<IOBufSliceImpl> initNonNullTrackerSlices() {
        @Nullable Queue<IOBufSliceImpl> tracked =
            nonNullTrackerSlices;
        if (tracked != null) {
            return tracked;
        }
        tracked = new LinkedList<>();
        nonNullTrackerSlices = tracked;
        return tracked;
    }

    /**
     * Move all the slices from the other list to this list, leaving the other
     * list empty.
     */
    public void extend(
        IOBufSliceList other,
        @Nullable Consumer<IOBufSliceImpl> trackInfoSetter)
    {
        if (other.isEmpty()) {
            return;
        }
        final @Nullable Entry t = tail;
        final Entry last = (t == null) ? sentinel : t;
        last.next(other.head());
        final @Nullable Entry ot = other.tail;
        if (ot != null) {
            ot.next(null);
        }
        tail = ot;
        size += other.size;
        other.sentinel.next(null);
        other.tail = null;
        other.size = 0;

        final @Nullable Queue<IOBufSliceImpl> tracked =
            other.nonNullTrackerSlices;
        if (tracked != null) {
            if (trackInfoSetter != null) {
                tracked.forEach(trackInfoSetter);
            }
            initNonNullTrackerSlices().addAll(tracked);
            tracked.clear();
        }
    }

    /**
     * Removes and returns the first entry, or null if empty.
     */
    public @Nullable Entry poll() {
        final Entry head = head();
        if (head == null) {
            return null;
        }
        final Entry next = head.next();
        sentinel.next(next);
        head.next(null);
        if (head == tail) {
            tail = null;
        }
        size --;

        final @Nullable Queue<IOBufSliceImpl> tracked = nonNullTrackerSlices;
        if (tracked != null) {
            if (head == tracked.peek()) {
                tracked.poll();
            }
        }

        return head;
    }

    /**
     * Gets the size of the list.
     */
    public int size() {
        return size;
    }

    /**
     * Returns {@code true} if list empty.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns a slice array.
     */
    public ByteBuffer [] toBufArray() {
        ByteBuffer[] array = new ByteBuffer[size];
        if (size == 0) {
            return array;
        }
        Entry curr = head();
        for (int i = 0; i < size; ++i) {
            if (curr == null) {
                break;
            }
            array[i] = curr.buf();
            curr = curr.next();
        }
        if (curr != null) {
            throw new AssertionError(
                          String.format("Expected null, got %s", curr));
        }
        return array;
    }

    /**
     * Clears the list and frees all entries.
     */
    public void freeEntries() {
        while (true) {
            Entry entry = poll();
            if (entry == null) {
                break;
            }
            entry.markFree();
        }

        final @Nullable Queue<IOBufSliceImpl> tracked =
            nonNullTrackerSlices;
        if (tracked != null) {
            tracked.clear();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        Entry curr = head();
        while (curr != null) {
            sb.append(curr);
            sb.append("->");
            curr = curr.next();
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Sets info for all the slices with a non-NullTracker.
     */
    public void setTrackerInfo(Consumer<IOBufSliceImpl> trackInfoSetter) {
        final @Nullable Queue<IOBufSliceImpl> tracked =
            nonNullTrackerSlices;
        if (tracked != null) {
            tracked.forEach(trackInfoSetter);
        }
    }

    /**
     * An entry of the {@link IOBufSliceList}
     */
    public static abstract class Entry implements IOBufSlice {
        private @Nullable Entry next;

        Entry() {
            this(null);
        }

        Entry(@Nullable Entry next) {
            this.next = next;
        }

        /**
         * Sets the next pointer.
         *
         * @param n the next pointer
         */
        void next(@Nullable Entry n) {
            next = n;
        }

        /**
         * Gets the next pointer.
         *
         * @return the next entry, null if the next entry is null or sentinel
         */
        public @Nullable Entry next() {
            final Entry n = next;
            return (n == null) ? null : n.entryNotSentinel();
        }

        /**
         * Returns this entry unless this is a sentinel, in which case it
         * returns null.
         */
        protected @Nullable Entry entryNotSentinel() {
            return this;
        }

        /**
         * Returns {@code true} if the entry is a sentinel entry.
         */
        protected boolean isSentinel() {
            return false;
        }
    }

    /**
     * A sentinel entry of the {@link IOBufSliceList}.
     *
     * The sentinel points to the head of the list. The tail of the list points
     * to the sentinel. The sentinel points to itself if the list is empty.
     */
    private class Sentinel extends Entry {
        @Override
        public ByteBuffer buf() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IOBufSliceImpl forkAndAdvance(int len, String forkDescription) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IOBufSliceImpl forkBackwards(int len, String forkDescription) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markFree() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected @Nullable Entry entryNotSentinel() {
            return null;
        }

        @Override
        protected boolean isSentinel() {
            return true;
        }
    }
}
