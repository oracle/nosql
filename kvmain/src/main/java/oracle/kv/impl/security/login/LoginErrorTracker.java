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

package oracle.kv.impl.security.login;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Tracks failed login attempts by users.  Errors are tracked by accessing
 * host as well as username, so attempts to access by a rogue host don't
 * prevent application access from a safe host. This does mean that a
 * password attack could make significantly more attempts if mounted as a
 * distributed attack, but we expect that customers will deploy NoSQL DB
 * with some level of network access restriction.
 */
public class LoginErrorTracker {

    /* The number of buckets in the tracked time period.  */
    private static final int BUCKET_COUNT = 20;

    /* interval over which failed login attempts are tracked (in ms) */
    private long acctErrLockoutInt;

    /* number of failed login attempts to trigger account lockout */
    private int acctErrLockoutCnt;

    /* account lockout timeout (in ms)*/
    private long acctErrLockoutTMO;

    /* start time (in ms) */
    private final long trackerStartTime;

    /* The size of each bucket (in ms) */
    private final long bucketSize;

    /* map of username:host to tracking info for that user */
    private final ConcurrentHashMap<String, UserRecord> userMap;

    private final Logger logger;

    /**
     * Construct the tracker.
     * @param accountErrorLockoutInterval the interval over which login
     *   errors are tracked.
     * @param accountErrorLockoutCount the minimum number of login errors
     *   over the tracking interval necessary to trigger account lockout.
     * @param accountErrorLockoutTimeout the length of time that an account
     *   should be locked out when the error count has been exceeded.
     */
    public LoginErrorTracker(long accountErrorLockoutInterval,
                             int accountErrorLockoutCount,
                             long accountErrorLockoutTimeout,
                             Logger logger) {

        this.acctErrLockoutInt = accountErrorLockoutInterval;
        this.acctErrLockoutCnt = accountErrorLockoutCount;
        this.acctErrLockoutTMO = accountErrorLockoutTimeout;
        this.bucketSize = acctErrLockoutInt / BUCKET_COUNT;
        this.userMap = new ConcurrentHashMap<String, UserRecord>();
        this.trackerStartTime = System.currentTimeMillis();
        this.logger = logger;
    }

    /**
     * Check whether a user account is locked out.  An account can be locked
     * on a per-host basis.
     *
     * @param username the username in question
     * @param clientHost the client host from which the request came
     * @return true if the account is locked for access from the specified host
     */
    public boolean isAccountLocked(String username, String clientHost) {

        final String userKey = userMapKey(username, clientHost);
        final UserRecord userRecord = userMap.get(userKey);
        if (userRecord == null || !userRecord.checkLockedOut()) {
            return false;
        }

        if (userRecord.tryUnlock()) {
            /* The lockout has expired */
            if (logger != null) {
                logger.info("The lockout on the account for user " + username +
                            " from " + clientHost + " has been lifted.");
            }
            return false;
        }

        /* Still locked out */
        return true;
    }

    /**
     * Report a failed login attempt by a user.
     * @param username the name of the user encountering a login error.
     * @param clientHost the host that the login attempt came from
     */
    void noteLoginError(String username, String clientHost) {

        final String userKey = userMapKey(username, clientHost);
        UserRecord userRecord = userMap.get(userKey);
        if (userRecord == null) {
            userRecord = new UserRecord();
            final UserRecord existingRecord =
                userMap.putIfAbsent(userKey, userRecord);
            if (existingRecord != null) {
                userRecord = existingRecord;
            }
        }

        if (userRecord.noteError()) {
            if (logger != null) {
                logger.info("Account for user " + username + " from " +
                            clientHost + " has been temporarily locked out " +
                            "due to excessive login errors.");
            }
        }
    }

    /**
     * Report a successful login attempt by a user.  When this occurs, we
     * clear our history of failed logins, so that repeatedly making an error
     * and then correcting it does not lock the user out.
     *
     * @param username the name of the user encountering a login error.
     * @param clientHost the host that the login attempt came from
     */
    void noteLoginSuccess(String username, String clientHost) {

        final String userKey = userMapKey(username, clientHost);
        final UserRecord userRecord = userMap.get(userKey);
        if (userRecord != null) {
            userRecord.clearErrors();
        }
    }

    /**
     * Creates a key for userMap
     */
    private String userMapKey(String username, String clientHost) {
        return username + ":" + clientHost;
    }

    /**
     * Tracks login errors by user key.
     * Errors are tracked in quantized buckets to avoid the need to
     * consume large amounts of memory.  This implies that the tracking
     * of counts vs. intervals is not exact, but doesn't need to be.
     */
    private final class UserRecord {
        /*
         * We track BUCKET_COUNT buckets of error counts, where the
         * 0'th element of the array counts the time period starting at
         * trackerStartTime + firstBucketIdx * bucketSize.  Buckets
         * 1-(BUCKET_COUNT-1) track successive time periods of length
         * bucketSize.
         */
        private final int[] errorBuckets = new int[BUCKET_COUNT];

        /*
         * The virtual index of the first error bucket relative to
         * trackerStartTime.
         */
        private int firstBucketIdx;

        /*
         * The time at which the account lockout started.  Valid only if
         * isLockedOut is true
         */
        private long lockoutStartTime;

        /*
         * Un-synchronized flag indicating whether the user account is
         * locked out.
         */
        private volatile boolean isLockedOut;

        private UserRecord() {
            firstBucketIdx = 0;
            lockoutStartTime = 0L;
            isLockedOut = false;
        }

        /**
         * Flag the user as having just encountered a login error.
         *
         * @return true if the account is newly locked out.  A return value
         * of false can happen regardless of whether the account is locked
         * out.  This only signals a transition to being locked-out.
         */
        private boolean noteError() {
            final long now = System.currentTimeMillis();
            synchronized (this) {
                final int nowBucketIdx =
                    (int) ((now - trackerStartTime) / bucketSize);
                alignBuckets(nowBucketIdx);
                if (errorBuckets[nowBucketIdx - firstBucketIdx] <
                    Integer.MAX_VALUE) {
                    errorBuckets[nowBucketIdx - firstBucketIdx] += 1;
                }

                if (errorCountsExceeded(nowBucketIdx)) {
                    /* transition to locked-out state */
                    lockoutStartTime = System.currentTimeMillis();
                    isLockedOut = true;
                }
            }
            return isLockedOut;
        }

        /**
         * Clear the login errors for the user.
         */
        private void clearErrors() {
            synchronized (this) {
                Arrays.fill(errorBuckets, 0);
            }
        }

        /**
         * Check whether the user is flagged as locked out currently.
         */
        private boolean checkLockedOut() {
            return isLockedOut;
        }

        /**
         * Check whether the user can be transitioned from locked-out to
         * not-locked-out.
         *
         * @return true if the account has been transitioned to not-locked-out.
         */
        private boolean tryUnlock() {
            final long now = System.currentTimeMillis();
            synchronized (this) {
                if (!isLockedOut) {
                    return false;
                }

                if (now - lockoutStartTime > acctErrLockoutTMO) {
                    final int nowBucketIdx =
                        (int) ((now - trackerStartTime) / bucketSize);
                    alignBuckets(nowBucketIdx);
                    if (!errorCountsExceeded(nowBucketIdx)) {
                        /* Lockout period has expired */
                        isLockedOut = false;
                        return true;
                    }
                }
            }
            return false;
        }

        /*
         * Caller must provide synchronization around calls to this method
         * and errorBuckets must be aligned.
         */
        private boolean errorCountsExceeded(final int nowBucketIdx) {
            assert (Thread.holdsLock(this));
            long errorTotal = 0;
            final int lastBucketIdx = nowBucketIdx - firstBucketIdx;
            for (int i = lastBucketIdx; i >= 0; i--) {
                errorTotal += errorBuckets[i];
            }
            return errorTotal >= acctErrLockoutCnt;
        }

        /*
         * Attempt to make nowBucketIdx be in the range firstBucketIdx -
         * firstBucketIdx+BUCKET_COUNT-1 by adjusting firstBucketIdx, and
         * shifting contents of the fixed errorBuckets array.
         * Caller must ensure single-thread access by synchronizing
         * around calls to this method.
         */
        private void alignBuckets(int nowBucketIdx) {
            assert (Thread.holdsLock(this));
            final int lastBucketIdx = firstBucketIdx + (BUCKET_COUNT - 1);

            if (nowBucketIdx > lastBucketIdx) {
                final int keepBuckets =
                    lastBucketIdx - (nowBucketIdx - BUCKET_COUNT);
                if (keepBuckets <= 0) {
                    /*
                     * None of the existing buckets are still valid.  Zero
                     * them all out.
                     */
                    Arrays.fill(errorBuckets, 0);
                    firstBucketIdx = nowBucketIdx;
                } else {
                    /*
                     * Some are re-usable - shift
                     */
                    final int loseBuckets = BUCKET_COUNT - keepBuckets;

                    /* Shift to the left */
                    for (int i = 0; i < keepBuckets; i++) {
                        errorBuckets[i] = errorBuckets[loseBuckets + i];
                    }
                    Arrays.fill(errorBuckets, keepBuckets, BUCKET_COUNT, 0);
                    firstBucketIdx += loseBuckets;
                }
            } else if (nowBucketIdx < firstBucketIdx) {
                /*
                 * This can only happen if the system clock goes dramatically
                 * backwards, but let's attempt to do something reasonable.
                 */
                final int keepBuckets =
                    BUCKET_COUNT - (firstBucketIdx - nowBucketIdx);
                if (keepBuckets <= 0) {
                    /*
                     * None of the existing buckets are still valid.  Zero
                     * them all out.
                     */
                    Arrays.fill(errorBuckets, 0);
                    firstBucketIdx = nowBucketIdx;
                } else {
                    /*
                     * Some are re-usable - shift
                     */
                    final int loseBuckets = BUCKET_COUNT - keepBuckets;

                    /* Shift to the right */
                    for (int i = 0; i < keepBuckets; i++) {
                        errorBuckets[BUCKET_COUNT - i] =
                            errorBuckets[BUCKET_COUNT - loseBuckets - i];
                    }
                    Arrays.fill(errorBuckets, 0, loseBuckets, 0);
                    firstBucketIdx = nowBucketIdx - BUCKET_COUNT + 1;
                }
            }
        }
    }
}
