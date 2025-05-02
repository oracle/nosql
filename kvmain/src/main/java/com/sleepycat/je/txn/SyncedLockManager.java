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

import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.StatGroup;

/**
 * SyncedLockManager uses the synchronized keyword to implement its critical
 * sections.
 */
public class SyncedLockManager extends LockManager {

    SyncedLockManager(EnvironmentImpl envImpl) {
        super(envImpl);
    }

    @Override
    public Set<LockInfo> getOwners(Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return getOwnersInternal(
                lsn, lockTableIndex, true /*cloneLockInfo*/);
        }
    }

    @Override
    public List<LockInfo> getWaiters(Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return getWaitersInternal(lsn, lockTableIndex);
        }
    }

    @Override
    public LockType getOwnedLockType(Long lsn, Locker locker) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return getOwnedLockTypeInternal(lsn, locker, lockTableIndex);
        }
    }

    @Override
    public boolean isLockUncontended(Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return isLockUncontendedInternal(lsn, lockTableIndex);
        }
    }

    @Override
    public boolean ownsOrSharesLock(Locker locker, Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return ownsOrSharesLockInternal(locker, lsn, lockTableIndex);
        }
    }

    @Override
    Lock lookupLock(Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return lookupLockInternal(lsn, lockTableIndex);
        }
    }

    @Override
    LockAttemptResult attemptLock(Long lsn,
                                  Locker locker,
                                  LockType type,
                                  boolean nonBlockingRequest,
                                  boolean jumpAheadOfWaiters)
        throws DatabaseException {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return attemptLockInternal
                (lsn, locker, type, nonBlockingRequest, jumpAheadOfWaiters,
                 lockTableIndex);
        }
    }

    @Override
    TimeoutInfo getTimeoutInfo(
        boolean isLockNotTxnTimeout,
        Locker locker,
        long lsn,
        LockType type,
        LockGrantType grantType,
        Lock useLock,
        long timeout,
        long start,
        long now,
        DatabaseImpl database,
        Set<LockInfo> owners,
        List<LockInfo> waiters) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return getTimeoutInfoInternal(
                isLockNotTxnTimeout, locker, lsn, type, grantType, useLock,
                timeout, start, now, database, owners, waiters);
        }
    }

    @Override
    Set<Locker> releaseAndFindNotifyTargets(long lsn, Locker locker) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return releaseAndFindNotifyTargetsInternal
                (lsn, locker, lockTableIndex);
        }
    }

    @Override
    boolean demote(long lsn, Locker locker) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return demoteInternal(lsn, locker, lockTableIndex);
        }
    }

    @Override
    boolean isLocked(Long lsn) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return isLockedInternal(lsn, lockTableIndex);
        }
    }

    @Override
    boolean isOwner(Long lsn, Locker locker, LockType type) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return isOwnerInternal(lsn, locker, type, lockTableIndex);
        }
    }

    @Override
    boolean isWaiter(Long lsn, Locker locker) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return isWaiterInternal(lsn, locker, lockTableIndex);
        }
    }

    @Override
    int nWaiters(Long lsn) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return nWaitersInternal(lsn, lockTableIndex);
        }
    }

    @Override
    int nOwners(Long lsn) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return nOwnersInternal(lsn, lockTableIndex);
        }
    }

    @Override
    public Locker getWriteOwnerLocker(Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return getWriteOwnerLockerInternal(lsn, lockTableIndex);
        }
    }

    @Override
    boolean validateOwnership(Long lsn,
                              Locker locker,
                              LockType type,
                              boolean getOwnersAndWaiters,
                              boolean flushFromWaiters,
                              Set<LockInfo> owners,
                              List<LockInfo> waiters) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return validateOwnershipInternal(
                lsn, locker, type, getOwnersAndWaiters, flushFromWaiters,
                lockTableIndex, owners, waiters);
        }
    }

    @Override
    public LockAttemptResult stealLock(Long lsn,
                                          Locker locker,
                                          LockType lockType)
        throws DatabaseException {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableMutexes[lockTableIndex]) {
            return stealLockInternal(lsn, locker, lockType, lockTableIndex);
        }
    }

    @Override
    void dumpLockTable(StatGroup stats, boolean clear) {
        for (int i = 0; i < nLockTables; i++) {
            synchronized(lockTableMutexes[i]) {
                dumpLockTableInternal(stats, i, clear);
            }
        }
    }
}
