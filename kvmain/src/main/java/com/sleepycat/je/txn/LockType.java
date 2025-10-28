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

package com.sleepycat.je.txn;

/**
 * LockType is a type safe enumeration of all lock types.  Methods on LockType
 * objects can be used to determine whether a type conflicts with another
 * type or can be upgraded to another type.
 */
public class LockType {

    /**
     * Lock types.  Indexes must be kept manually synchronized in the matrixes
     * below.
     */
    public static final LockType READ =
        new LockType(0, false, "READ", false);
    public static final LockType WRITE =
        new LockType(1, true, "WRITE", false);
    /*
     * WRITE_RMW is used for LockMode.RMW and is identical to WRITE
     * except its abort LSN is not made obsolete when the transaction
     * commits.
     */
    public static final LockType WRITE_RMW =
        new LockType(1, true, "WRITE_RMW", true);

    /**
     * NONE is used for requesting a dirty read and does not appear in the
     * conflict or upgrade matrices.
     */
    public static final LockType NONE =
        new LockType(2, false, "NONE", false);

    /**
     * Lock conflict matrix.
     * @see #getConflict
     */
    private static LockConflict[][] conflictMatrix = {
        { // READ is held and there is a request for:
            LockConflict.ALLOW,   // READ
            LockConflict.BLOCK,   // WRITE
        },
        { // WRITE is held and there is a request for:
            LockConflict.BLOCK,   // READ
            LockConflict.BLOCK,   // WRITE
        },
    };

    /**
     * Lock upgrade matrix.
     * @see #getUpgrade
     */
    private static LockUpgrade[][] upgradeMatrix = {
        { // READ is held and there is a request for:
            LockUpgrade.EXISTING,                  // READ
            LockUpgrade.WRITE_PROMOTE,             // WRITE
        },
        { // WRITE is held and there is a request for:
            LockUpgrade.EXISTING,                  // READ
            LockUpgrade.EXISTING,                  // WRITE
        },
    };

    final private int index;
    final private boolean write;
    final private String name;
    final private boolean rmw;

    /**
     * No lock types can be defined outside this class.
     */
    private LockType(int index, boolean write, String name, boolean rmw) {
        this.index = index;
        this.write = write;
        this.name = name;
        this.rmw = rmw;
    }

    /**
     * Returns true if this is a WRITE or RANGE_WRITE lock.  For RANGE_INSERT,
     * false is returned because RANGE_INSERT is used to lock the key following
     * the insertion key, not the insertion key itself.
     */
    public final boolean isWriteLock() {
        return write;
    }

    /**
     * Returns if this is a WRITE lock gotten on a read using the LockMode.RMW.
     */
    public final boolean isRMW() {
        return rmw;
    }

    /**
     * Returns the LockConfict that results when this lock type is held and the
     * given lock type is requested by another locker.
     */
    LockConflict getConflict(LockType requestedType) {
        return conflictMatrix[index][requestedType.index];
    }

    /**
     * Returns the LockUpgrade that results when this lock type is held and the
     * given lock type is requested by the same locker.
     *
     * <p>For the returned LockUpgrade object, getIllegal will never return
     * true because this method fires an assertion if getIllegal returns true.
     */
    LockUpgrade getUpgrade(LockType requestedType) {
        LockUpgrade upgrade = upgradeMatrix[index][requestedType.index];
        assert !upgrade.getIllegal() : toString() + " to " + requestedType;
        return upgrade;
    }

    @Override
    public String toString() {
        return name;
    }
}
