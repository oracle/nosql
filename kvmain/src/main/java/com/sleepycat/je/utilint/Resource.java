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
package com.sleepycat.je.utilint;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a resource that keeps track of the number of parties currently
 * using it, and that supports deallocation and reallocation. This class uses
 * a lock-free approach based on CAS.
 *
 * @param <T> the type of the resource
 */
public abstract class Resource<T extends Resource<T>> {

    /**
     * Use count for a deallocated resource: one that is not in use and cannot
     * be put in use until it is allocated.
     */
    private static final long DEALLOCATED = Long.MIN_VALUE;

    /**
     * Use count for the resource:
     * <ul>
     * <li> Values greater than 0 represent the number of parties currently
     * using the resource
     * <li> 0 means the resource is not currently in use, but can be put in use
     * by a call to {@link #incrementUse}
     * <li> Negative values greater than {@link #DEALLOCATED} represent
     * resources that are currently in use but that do not support new uses
     * because deallocation has been requested by a call to {@link
     * #deallocate}.  The value is the negation of the actual use count.
     * <li> {@link #DEALLOCATED} means the resource is deallocated, and cannot
     * be put into use until it is allocated by a call to {@link #allocate}
     * </ul>
     */
    private final AtomicLong useCount = new AtomicLong(DEALLOCATED);

    /**
     * Creates a deallocated resource.
     */
    protected Resource() { }

    /**
     * Called when the resource is deallocated
     */
    protected abstract void noteDeallocation();

    /**
     * Attempt to increment the number of uses of this resource, returning true
     * if successful.  The resource should only be used if this method returns
     * {@code true}.  The caller must call {@link #decrementUse} when the
     * resource is no longer in use.
     *
     * @return whether the usage was successfully incremented
     */
    public boolean incrementUse() {
        while (true) {
            final long currentUseCount = useCount.get();

            /* Check deallocating or deallocated */
            if (currentUseCount < 0) {
                if (DEBUG) {
                    addDebug("incrementUse false", currentUseCount);
                }
                return false;
            }

            if (useCount.compareAndSet(currentUseCount, currentUseCount + 1)) {
                if (DEBUG) {
                    addDebug("incrementUse true", currentUseCount);
                }
                return true;
            }
        }
    }

    /**
     * Decrements the number of uses of this resource.  This method should only
     * be called to match a corresponding call to {@link #incrementUse}.
     *
     * @throws IllegalStateException if the resource is deallocated, if
     * deallocation is underway, or if its use count is 0
     */
    public void decrementUse() {
        while (true) {
            final long currentUseCount = useCount.get();
            if (currentUseCount > 0) {
                if (useCount.compareAndSet(currentUseCount,
                    currentUseCount - 1)) {
                    if (DEBUG) {
                        addDebug("decrementUse positive", currentUseCount);
                    }
                    return;
                }
            } else if (currentUseCount == -1) {
                if (deallocate(-1)) {
                    if (DEBUG) {
                        addDebug("decrementUse deallocate", currentUseCount);
                    }
                    return;
                }
            } else if (currentUseCount == DEALLOCATED) {
                throw new IllegalStateException(
                    "The resource is already deallocated: " +
                        toString(currentUseCount));
            } else if (currentUseCount == 0) {
                throw new IllegalStateException(
                    "The resource is not in use: " +
                    toString(currentUseCount));
            } else {

                /* Using negative counts while deallocating, so count up */
                if (useCount.compareAndSet(currentUseCount,
                    currentUseCount + 1)) {
                    if (DEBUG) {
                        addDebug("decrementUse negative", currentUseCount);
                    }
                    return;
                }
            }
        }
    }

    /**
     * Deallocates this resource if its use count matches the specified value,
     * which should either be 0, when deallocating an unused resource, or -1,
     * when decrementing the last use of a resource being deallocated.  Calls
     * the deallocation handler if successful and returns true, otherwise
     * returns false.
     */
    private boolean deallocate(long currentUseCount) {
        if (!useCount.compareAndSet(currentUseCount, DEALLOCATED)) {
            return false;
        }
        if (DEBUG) {
            addDebug("deallocate", currentUseCount);
        }
        noteDeallocation();
        return true;
    }

    /**
     * Allocates this resource, which should currently be deallocated.
     *
     * @throws IllegalStateException if this resource is already allocated
     */
    public void allocate() {
        if (!useCount.compareAndSet(DEALLOCATED, 0)) {
            throw new IllegalStateException(
                "Resource is already allocated: " + toString());
        }
        if (DEBUG) {
            addDebug("allocate");
        }
    }

    /**
     * Requests that this resource be deallocated when its use count is zero.
     * This call should only be made if this resource is currently allocated
     * and not already in the process of being deallocated.
     *
     * @throws IllegalStateException if this resource is deallocated or in the
     * process of being deallocated
     */
    public void requestDeallocate() {
        while (true) {
            final long currentUseCount = useCount.get();
            if (currentUseCount == 0) {
                if (deallocate(0)) {
                    if (DEBUG) {
                        addDebug("requestDeallocate deallocate", 0);
                    }
                    return;
                }
            } else if (currentUseCount > 0) {
                if (useCount.compareAndSet(currentUseCount,
                    -currentUseCount)) {
                    if (DEBUG) {
                        addDebug("requestDeallocate in use", currentUseCount);
                    }
                    return;
                }
            } else if (currentUseCount == DEALLOCATED) {
                throw new IllegalStateException(
                    "Already deallocated: " + toString(DEALLOCATED));
            } else {
                throw new IllegalStateException(
                    "Unexpected deallocation requested: " +
                        toString(currentUseCount));
            }
        }
    }

    @Override
    public String toString() {
        return toString(useCount.get());
    }

    private String toString(long currentUseCount) {
        final StringBuilder sb = getState(currentUseCount);
        if (DEBUG) {
            addDebugInfo(sb);
        }
        return sb.toString();
    }

    private StringBuilder getState(long currentUseCount) {
        final StringBuilder sb = new StringBuilder("[Resource ");
        if (currentUseCount == DEALLOCATED) {
            sb.append("DEALLOCATED");
        } else if (currentUseCount == 0) {
            sb.append("INACTIVE");
        } else if (currentUseCount > 0) {
            sb.append("ACTIVE");
        } else {
            sb.append("DEALLOCATING");
        }
        sb.append(" useCount=").append(currentUseCount);
        addResourceInfo(sb);
        sb.append(']');
        return sb;
    }

    protected void addResourceInfo(StringBuilder sb) {
    }

    public void verifyResourceInUse() {
        if (useCount.get() == DEALLOCATED) {
            throw new IllegalStateException(
                "Resource not in use: " + this);
        }
    }

    /*
     * DEBUG
     */
    public static boolean DEBUG = false;
    private static int DEBUG_ITEMS = 10;

    private final String[] debugItems =
        DEBUG ? new String[DEBUG_ITEMS] : null;

    private int debugIndex = 0;

    private void addDebug(String label) {
        addDebug(label, useCount.get());
    }

    private void addDebug(String label, long currentUseCount) {
        String msg = label +
            ' ' + System.identityHashCode(this) +
            ' ' + getState(currentUseCount);
//        Exception e = new Exception();
//        msg += ' ' + e.getStackTrace()[0];
        debugItems[debugIndex] = msg;
        debugIndex =
            (debugIndex < DEBUG_ITEMS - 1) ? (debugIndex + 1) : 0;
    }

    private void addDebugInfo(StringBuilder sb) {
        int d = debugIndex;
        for (int i = 0; i < DEBUG_ITEMS; i += 1) {
            d = (d > 0) ? (d - 1) : DEBUG_ITEMS - 1;
            String msg = debugItems[d];
            sb.append("\n");
            sb.append(msg);
        }
    }
}
