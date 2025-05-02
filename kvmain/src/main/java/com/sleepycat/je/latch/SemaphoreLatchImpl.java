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

import static com.sleepycat.je.EnvironmentFailureException.assertState;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.utilint.NotSerializable;

/** See {@link Semaphore} */
class SemaphoreLatchImpl extends Semaphore
    implements SemaphoreLatch, NotSerializable {

    private final SemaphoreLatchContext context;

    SemaphoreLatchImpl(final SemaphoreLatchContext context) {
        super(context.getLatchMaxReaders(), false);
        this.context = context;
        assertState(context.getLatchMaxReaders() <= MAX_READERS);
    }

    @Override
    public void acquireShared() {
        try {
            if (!tryAcquire(
                context.getLatchTimeoutMs(), TimeUnit.MILLISECONDS)) {
                throw LatchSupport.handleTimeout(context);
            }
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(
                context.getEnvImplForFatalException(),
                e.toString() + " name:" + context.getLatchName(),
                e);
        }
    }

    @Override
    public void releaseShared() {
        release();
    }

    @Override
    public void acquireExclusive() {
        try {
            if (!tryAcquire(
                context.getLatchMaxReaders(),
                context.getLatchTimeoutMs(), TimeUnit.MILLISECONDS)) {
                throw LatchSupport.handleTimeout(context);
            }
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(
                context.getEnvImplForFatalException(),
                e.toString() + " name:" + context.getLatchName(),
                e);
        }
    }

    @Override
    public boolean acquireExclusiveNoWait() {
        return tryAcquire(context.getLatchMaxReaders());
    }

    @Override
    public void releaseExclusive() {
        release(context.getLatchMaxReaders());
    }

    @Override
    public boolean inUse() {
        return availablePermits() < context.getLatchMaxReaders();
    }
}
