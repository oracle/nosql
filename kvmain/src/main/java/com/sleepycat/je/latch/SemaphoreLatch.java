/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.latch;

/**
 * A read-write latch where "locks" (actually semaphore permits) can be
 * acquired in one thread and released in another, which is not possible with
 * the usual thread-based locks and latches.
 *
 * <p>This is for very specialized used cases. When acquiring in one thread
 * and releasing in another thread, be aware that it will be much easier to
 * create deadlocks than with the usual thread-based latch approach. With
 * thread-based latches, release() is always called in a finally block after
 * calling acquire() in the same thread, and this makes it easier to always
 * acquire locks in the same order and to ensure that locks are released.
 * Likewise, for a semaphore latch, unlike a thread-based latch, debugging
 * checks are not implemented to detect when acquire() and release() calls
 * are not balanced.</p>
 *
 * <p>Unlike a thread-based latch, a semaphore latch has a maximum number of
 * concurrent readers associated with the latch context. This means readers
 * will block to avoid exceeding the maximum. Normally a large value is
 * specified to effectively allow unlimited readers. Using large values does
 * not have a performance penalty.</p>
 */
public interface SemaphoreLatch {

    /**
     * Max value allowed for maxReaders ctor param.
     *
     * <p>It is possible that Integer.MAX_VALUE will work fine, it is
     * divided by two out of paranoia of Semaphore arithmetic bugs.</p>
     */
    int MAX_READERS = Integer.MAX_VALUE / 2;

    /**
     * Acquires one permit, blocking if all permits are held by another entity,
     * which normally only occurs when an acquireExclusive method was called.
     */
    void acquireShared();

    /**
     * Releases one permit and must be called only to balance an
     * acquireShared call.
     */
    void releaseShared();

    /**
     * Acquires all the permits, blocking until all other entity release them.
     */
    void acquireExclusive();

    /**
     * Acquires all the permits, if possible without blocking.
     */
    boolean acquireExclusiveNoWait();

    /**
     * Releases all permits and must be called only to balance an
     * acquireExclusive call.
     */
    void releaseExclusive();

    /**
     * Returns whether any permits are in use, which is true whether a shared
     * or exclusive acquire method was used.
     */
    boolean inUse();
}
