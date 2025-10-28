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

package com.sleepycat.je.util.verify;

import static com.sleepycat.je.utilint.JETaskCoordinator.JE_VERIFY_BTREE_TASK;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.ExtinctionFilter.ExtinctionStatus;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryIntegrityException;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.VerifyError;
import com.sleepycat.je.VerifyError.Problem;
import com.sleepycat.je.VerifyListener;
import com.sleepycat.je.VerifySummary;
import com.sleepycat.je.cleaner.UtilizationProfile;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.InternalComparator;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeWalkerStatsAccumulator;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.RateLimitingLogger;
import com.sleepycat.je.utilint.StatsAccumulator;
import com.sleepycat.je.utilint.TaskCoordinator.Permit;

public class BtreeVerifier implements BtreeVerifyContext {
    /*
     * TODO: additional checks to make in the future.
     *  + If the verifyConfig requires to fetch LN, we need to check the full
     *    key of the in-memory LN is the same with the full key in the
     *    corresponding LNLogEntry in the log. In addition, this check may also
     *    need to be done when fetching an LN in a normal op, e.g. IN.fetchLN.
     */

    public static final VerifySummary EMPTY_SUMMARY = new Summary();

    public static final String VERIFY_PREFIX = "DB_VERIFY ";
    private final static ReadOptions NO_LOCK = new ReadOptions();
    private final static ReadOptions READ_LOCK = new ReadOptions();
    
    private static final SecondaryDBVerifier secDBVer
        = new SecondaryDBVerifier();
    /*
     * Get permit immediately, and do not have a timeout for the amount of time
     * the permit can be held.  Should only be changed for testing.
     */
    public static long PERMIT_WAIT_MS = 0;
    public static long PERMIT_TIME_TO_HOLD_MS = 0;

    static {
        NO_LOCK.setCacheMode(CacheMode.UNCHANGED);
        NO_LOCK.setLockMode(LockMode.READ_UNCOMMITTED);

        READ_LOCK.setCacheMode(CacheMode.UNCHANGED);
        READ_LOCK.setLockMode(LockMode.DEFAULT);
    }

    private final static String CORRUPTION_MSG =
        "Btree corruption was detected. ";

    /* Special values for file sizes. */
    private static final int FILE_DELETED = -1;
    private static final int FILE_OUT_OF_BOUNDS = -2;

    private final EnvironmentImpl envImpl;
    private final FileManager fileManager;
    private final Logger logger;
    private Level logLevel;
    private final RateLimitingLogger<Problem> problemLogger;
    private final RateLimitingLogger<BtreeVerifier> exceptionLogger;
    private final UtilizationProfile up;
    private final Map<Long, Integer> fileSizeCache = new HashMap<>();
    private final Map<Long, long[]> obsoleteOffsetsCache = new HashMap<>();
    private Map<Long, IN> lsnsToCheck = new HashMap<>();
    private boolean background = false;

    private volatile boolean stopVerify = false;
    private VerifyConfig btreeVerifyConfig = new VerifyConfig();

    /* DB state that can change during verification. */
    private boolean isSecondaryDb;
    private boolean checkPriToSecRefs;
    private boolean checkSecToPriRefs;
    private boolean checkForeignKeyConstraints;

    private final SecondaryAssociation offlineSecAssoc;
    private final Map<String, SecondaryConfig> keyCreatorLookup;
    private SummaryListener summary;
    private final List<VerifyError> errors = new ArrayList<>();
    private final List<VerifyError> readOnlyErrors =
        Collections.unmodifiableList(errors);

    public BtreeVerifier(final EnvironmentImpl envImpl) {
        this(envImpl, false, null, null, false, false);
    }

    public BtreeVerifier(final EnvironmentImpl envImpl,
                         final boolean disableRateLimitedLogging) {
        this(envImpl, disableRateLimitedLogging, null, null, false, false);
    }

    public BtreeVerifier(final EnvironmentImpl envImpl,
                         final boolean disableRateLimitedLogging,
                         final SecondaryAssociation secAssoc,
                         final Map<String, SecondaryConfig> keyCreatorLookup,
                         final boolean corruptVerifier,
                         final boolean offlineSecVerify) {
        this.envImpl = envImpl;
        this.fileManager = envImpl.getFileManager();
        this.logger = envImpl.getLogger();
        this.up = envImpl.getUtilizationProfile();
        if (offlineSecVerify) {
            secDBVer.runOfflineMode();
        }
        this.offlineSecAssoc = secAssoc;
        this.keyCreatorLookup = keyCreatorLookup;
        if (corruptVerifier) {
            secDBVer.runCorrupter();
        }

        /* Log problems and exceptions at most once per interval. */
        final int intervalMs = (int) TimeUnit.SECONDS.toMillis(15);
        problemLogger = new RateLimitingLogger<>(
            intervalMs,
            disableRateLimitedLogging ? 0 : Problem.values().length,
            logger,
            VERIFY_PREFIX);
        exceptionLogger = new RateLimitingLogger<>(
            intervalMs,
            disableRateLimitedLogging ? 0 : 1,
            logger,
            VERIFY_PREFIX);
    }

    
    private void logMsg(Level level, String msg) {
    	LoggerUtils.logMsg(logger, envImpl, level, VERIFY_PREFIX + msg);
    }

    @Override
    public void recordError(final VerifyError error) {
        if (envImpl.isTestMode() && error.isSevere()) {
            final EnvironmentFailureException e =
                new EnvironmentFailureException(envImpl,
                    EnvironmentFailureReason.BTREE_CORRUPTION,
                    "Severe Btree verification error occurred in test mode: " +
                    error);

            logMsg(Level.SEVERE, LoggerUtils.getStackTraceForSevereLog(e));
            throw e;
        }
        errors.add(error);
    }

    /**
     * Verifies all DBs in the environment, including ID and NAME DBs.
     * For use by {@link Environment#verify} and background verifier.
     */
    public VerifySummary verifyAll()
        throws DatabaseException {
        Long startTime = TimeSupplier.currentTimeMillis();
        summary = new SummaryListener(btreeVerifyConfig.getListener());

        /*
         * This aims to guarantee that only if DataVerifier.shutdown is
         * called, then BtreeVerifier will do nothing, including not
         * verifying the nameDatabase and mapDatabase.
         *
         * Without this, the following interleaving may appear. The premise
         * is that DataVerifier.shutdown is called immediately after
         * DataVerifier is created.
         *
         *           T1                                  Timer
         *      verifyTask is created
         *      verifyTask is scheduled
         *      DataVerifier.shutdown is called
         *         verifyTask.cancel()
         *         set stop verify flag
         *         timer.cancel()
         *         check 'task == null || !task.isRunning'
         *         Return true because !task.isRunning
         *
         *                                       Due to some reason, although
         *                                       verifyTask.cancel() and
         *                                       timer.cancel() is called,
         *                                       verifyTask can still execute
         *                                       once. So DataVerifier.shutdown
         *                                       does not achieve its target.
         *  After we add the following code, even if verifyTask can execute,
         *  it will do nothing. BtreeVerifier and DbVerifyLog will just return
         *  because now we have already set the stop flag to be true.
         */
        if (stopVerify) {
            summary.canceled = true;
            return summary;
        }

        if (!summary.onBegin()) {
            return summary;
        }

        final PrintStream out =
            (btreeVerifyConfig.getShowProgressStream() != null) ?
                btreeVerifyConfig.getShowProgressStream() :
                System.err;

        final String startMsg = "Start verify all databases";

        if (btreeVerifyConfig.getPrintInfo()) {
            out.println(startMsg);
        }
        logMsg(Level.INFO, startMsg);

        envImpl.setBtreeVerifyContext(this);
        try {
            /*
             * Verify ID and NAME databases first. During ID database
             * verification, collect the IDs of all other DBs for
             * verification further below.
             */
            final Set<DatabaseId> otherDbs = new HashSet<>();

            if (!stopVerify) {
                verifyOneDb(DbTree.ID_DB_ID, out, true, otherDbs);
            }

            if (!stopVerify) {
                verifyOneDb(DbTree.NAME_DB_ID, out, true, null);
            }

            for (final DatabaseId dbId : otherDbs) {
                if (stopVerify) {
                    break;
                }
                verifyOneDb(dbId, out, true, null);
            }

            final String stopMsg = "End verify all databases: " +
                (summary.hasErrors() ?
                    ("errors detected. " + summary) :
                    "no errors.");

            if (btreeVerifyConfig.getPrintInfo()) {
                out.println(stopMsg);
            }
            logMsg(summary.hasSevereErrors() ? logLevel : Level.INFO, stopMsg);

            if (stopVerify) {
                summary.canceled = true;
            }
            summary.onEnd(summary);
            return summary;

        } finally {
            envImpl.dbVerifyRunIncrement();
            envImpl.dbVerifyProblemsIncrementBy(summary.totalProblems());
            envImpl.dbVerifyRunTimeIncrementBy(
                TimeSupplier.currentTimeMillis() - startTime);
            envImpl.setBtreeVerifyContext(null);
        }
    }

    /**
     * For use by {@link Database#verify}.
     */
    public VerifySummary verifyDatabase(final DatabaseId dbId) {
        Long startTime = TimeSupplier.currentTimeMillis();
        summary = new SummaryListener(btreeVerifyConfig.getListener());

        if (!summary.onBegin()) {
            return summary;
        }

        envImpl.setBtreeVerifyContext(this);
        try {
            final PrintStream out =
                (btreeVerifyConfig.getShowProgressStream() != null) ?
                    btreeVerifyConfig.getShowProgressStream() :
                    System.err;

            verifyOneDb(dbId, out, false, null);

            if (stopVerify) {
                summary.canceled = true;
            }
            summary.onEnd(summary);
            return summary;

        } finally {
            envImpl.dbVerifyRunIncrement();
            envImpl.dbVerifyProblemsIncrementBy(summary.totalProblems());
            envImpl.dbVerifyRunTimeIncrementBy(
                    TimeSupplier.currentTimeMillis() - startTime);
            envImpl.setBtreeVerifyContext(null);
        }
    }

    /**
     * For use by {@link Database#getStats}.
     */
    public BtreeStats getDatabaseStats(final DatabaseId dbId) {

        summary = new SummaryListener(btreeVerifyConfig.getListener());

        envImpl.setBtreeVerifyContext(this);
        try {
            PrintStream out = btreeVerifyConfig.getShowProgressStream();
            if (out == null) {
                out = System.err;
            }

            return verifyOneDb(dbId, out, false, null);

        } finally {
            envImpl.setBtreeVerifyContext(null);
        }
    }

    /**
     * Verify one database, a batch at a time. Batches are used so that we
     * can release all DatabaseImpls between batches and allow db
     * truncate/remove operations to run.
     *
     * @param verifyAll if true, we won't log INFO messages for every database
     * to avoid cluttering the trace log.
     */
    private BtreeStats verifyOneDb(
        final DatabaseId dbId,
        final PrintStream out,
        final boolean verifyAll,
        final Set<DatabaseId> collectDbIds) {

        envImpl.checkOpen();

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        final int batchSize = btreeVerifyConfig.getBatchSize();
        final long batchDelay =
            btreeVerifyConfig.getBatchDelay(TimeUnit.MILLISECONDS);

        /* Used to verify UINs and collect BtreeStats. */
        final VerifierStatsAccumulator statsAcc = new VerifierStatsAccumulator(
            out, btreeVerifyConfig.getShowProgressInterval());

        isSecondaryDb = false;
        SecondaryDatabase secDb = null;
        Database priDb = null;
        final String dbName;

        DatabaseImpl dbImpl = getDb(dbId, null);
        if (dbImpl == null) {
            if (errors.size() > 0) {
                if (!summary.onOtherError(readOnlyErrors)) {
                    stopVerify = true;
                }
            }
            return new BtreeStats();
        }

        try {
            dbName = dbImpl.getName();

            final String startMsg = "Start verify database: " + dbName;
            if (btreeVerifyConfig.getPrintInfo()) {
                out.println(startMsg);
            }
            if (!verifyAll) {
                logMsg(Level.INFO, startMsg);
            }

            final Set<Database> referringHandles =
                dbImpl.getReferringHandles();

            for (final Database db : referringHandles) {
                priDb = db;
                if (db instanceof SecondaryDatabase) {
                    isSecondaryDb = true;
                    secDb = (SecondaryDatabase) db;
                    priDb = null;
                    break;
                }
            }
            if (secDBVer.isOfflineMode()) {
                String pattern = "p(\\d{1,5})$";
                Pattern r = Pattern.compile(pattern); 
                // verify pri to sec refs only for the partition dbs
                checkPriToSecRefs = r.matcher(dbName).find() &&
                    !isSecondaryDb &&
                    btreeVerifyConfig.getVerifySecondaries() &&
                    btreeVerifyConfig.getVerifyDataRecords();
            } else {
                checkPriToSecRefs = priDb != null &&
                    btreeVerifyConfig.getVerifySecondaries() &&
                    btreeVerifyConfig.getVerifyDataRecords();
            }

            checkSecToPriRefs = isSecondaryDb &&
                btreeVerifyConfig.getVerifySecondaries();

            checkForeignKeyConstraints = checkSecToPriRefs;
            /*
             * We need to read the record data when verifying secondaries or
             * LNs. Also for dup DBs we need the data in order to reposition
             * the cursor between batches. Note that for dup DBs the data is
             * embedded in the BIN so there is no fetch.
             */
            if (!checkSecToPriRefs &&
                !checkPriToSecRefs &&
                !dbImpl.getSortedDuplicates() &&
                !btreeVerifyConfig.getVerifyDataRecords()) {

                dataEntry.setPartial(0, 0, true);
            }
        } finally {
            envImpl.getDbTree().releaseDb(dbImpl);
        }

        try {
            Consumer<InterruptedException> handler = (e) -> {
                throw new EnvironmentFailureException(envImpl,
                    EnvironmentFailureReason.THREAD_INTERRUPTED, e);
            };
            while (true) {
                envImpl.checkOpen();

                if (stopVerify) {
                    break;
                }

                dbImpl = getDb(dbId, dbName);
                if (dbImpl == null) {
                    break;
                }

                try (Permit permit = (!background ? null
                    : envImpl.getTaskCoordinator().acquirePermit(
                        JE_VERIFY_BTREE_TASK, PERMIT_WAIT_MS,
                        PERMIT_TIME_TO_HOLD_MS, TimeUnit.MILLISECONDS,
                        handler))) {
                    if (verifyBatch(
                        dbImpl, priDb, secDb, statsAcc, keyEntry, dataEntry,
                        batchSize, collectDbIds)) {
                        break;
                    }
                } finally {
                    envImpl.getDbTree().releaseDb(dbImpl);
                }

                if (!stopVerify && batchDelay > 0) {
                    try {
                        Thread.sleep(batchDelay);
                    } catch (InterruptedException e) {
                        throw new ThreadInterruptedException(envImpl, e);
                    }
                }
            }

            if (!summary.onDatabase(dbName, readOnlyErrors)) {
                stopVerify = true;
            }

            final BtreeStats stats = new BtreeStats(statsAcc.getStats());

            if (btreeVerifyConfig.getPrintInfo()) {
                /*
                 * Intentionally use print, not println, because
                 * stats.toString() puts in a newline too.
                 */
                out.print(stats);
            }

            return stats;

        } finally {
            final String stopMsg = "End verify database: " + dbName;
            if (btreeVerifyConfig.getPrintInfo()) {
                out.println(stopMsg);
            }
            if (!verifyAll) {
                logMsg(Level.INFO, stopMsg);
            }
        }
    }

    /**
     * Calls getDb. If DB is deleted or an exception occurs, returns null.
     * If an exception occurs, adds an error before returning. If non-null is
     * returned, the caller must call releaseDb.
     *
     * @param dbName is null if this is the first lookup of this DB ID.
     */
    private DatabaseImpl getDb(final DatabaseId dbId,
                               final String dbName) {
        final DatabaseImpl dbImpl;
        try {
            dbImpl = envImpl.getDbTree().getDb(dbId);
        } catch (Exception e) {
            LoggerUtils.logMsg(
                exceptionLogger, envImpl, this, Level.SEVERE,
                LoggerUtils.getStackTraceForSevereLog(e));

            recordError(new VerifyError(
                Problem.DATABASE_ACCESS_EXCEPTION,
                "Unexpected exception looking up DB by ID, halting" +
                    " verification.  ID: " + dbId +
                    " name: " + ((dbName != null) ? dbName : "unknown") +
                    " exception: " + e.toString(),
                DbLsn.NULL_LSN));
            return null;
        }

        if (dbImpl == null || dbImpl.isDeleting()) {
            envImpl.getDbTree().releaseDb(dbImpl);
            /* Not considered an error. */
            return null;
        }

        return dbImpl;
    }

    /**
     * Verify one batch of records for the given DB.
     *
     * @return true if all records in the DB have been verified or we should
     * stop verification of the DB due to problems.
     *
     * Record Locking Strategy
     * -----------------------
     * The normal readPrimaryAfterGet implementation has a problem
     * when used in index verification: it cannot detect corruption
     * when secDirtyRead is true and the primary record is NOT_FOUND.
     * In this situation we don't have any locks, so we don't know the
     * true current state of either the primary or secondary record.
     *
     * Therefore, for the index verification, we need to lock the
     * secondary first and then use a non-blocking lock to lock
     * primary record to avoid deadlock. If we cannot lock the
     * primary record, we can just skip the verification. This is why
     * READLOCK_UNCHANGED is used when checkSecToPriRefs is true.
     *
     * If checkPriToSecRefs is true, we will first get the record
     * without acquiring a lock in this method (with NOLOCK_UNCHANGED)
     * and then try to acquire a read lock in verifyPriToSecRefs. So
     * in this method we use READLOCK_UNCHANGED only when
     * checkSecToPriRefs is true.
     *
     * When a read lock is required, a simple approach is problematic:
     *
     * 1. When requesting a read lock, it is always possible that
     *    LockConflictException will be thrown.
     *
     * 2. The (keyEntry, dataEntry) will be used to find the
     *    corresponding primary record. So dataEntry (priKey) can not
     *    be null.
     *
     * 3. We need to use nonSticky==true to avoid a deadlock when
     *    calling cursor.get(NEXT). But if cursor.get(NEXT) cannot
     *    succeed due to LockConflictException or something else, the
     *    cursorImpl will be reset, i.e. its previous location will be
     *    lost. This is not what we expect.
     *
     * The solution:
     *
     * 1. Use nonSticky==true
     *
     * 2. Use LockMode.READ_UNCOMMITTED when doing Get.NEXT. This can
     *    resolve 3 above. Because Get.NEXT will not acquire lock, if
     *    more records exist, Get.NEXT can always succeed, i.e.
     *    Get.NEXT can move to next record. So 'nonSticky==true' will
     *    not cause the cursorImpl to move to an invalid position.
     *
     * 3. Use Get.CURRENT with LockMode.DEFAULT to lock the record and
     *    read the record. This can resolve 1 above. This will acquire
     *    a READ lock on the record.
     *
     * 4. If Get.CURRENT in (3) returns null, i.e. the record may have
     *    been deleted, then we will throw an internal exception to
     *    cause the cursor to move to next slot. This will resolve 2
     *    above.
     */
    private boolean verifyBatch(
        final DatabaseImpl dbImpl,
        final Database priDb,
        final SecondaryDatabase secDb,
        final TreeWalkerStatsAccumulator statsAcc,
        final DatabaseEntry keyEntry,
        final DatabaseEntry dataEntry,
        final int batchSize,
        final Set<DatabaseId> collectDbIds) {

        final Tree tree = dbImpl.getTree();
        EnvironmentImpl.incThreadLocalReferenceCount();

        final DatabaseEntry priKey = isSecondaryDb ? dataEntry : keyEntry;
        final DatabaseEntry secKey = isSecondaryDb ? keyEntry : null;

        final Locker locker =
            LockerFactory.getInternalReadOperationLocker(envImpl);

        final Cursor cursor =
            makeCursor(dbImpl, locker, false /*retainNonTxnLocks*/);

        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);
        cursorImpl.setTreeStatsAccumulator(statsAcc);
        tree.setTreeStatsAccumulator(statsAcc);

        final Map<DatabaseId, DatabaseImpl> dbCache = new HashMap<>();
          
        try {
            /* Reposition after last record or at first record initially. */
            OperationResult result =
                findFirstRecord(dbImpl, cursor, keyEntry, dataEntry);

            if (result == null) {
                return true;
            }
            for (int recordCount = 1;; ++recordCount) {
                if (stopVerify) {
                    return true;
                }

                boolean locked = false;

                try {
                    if (collectDbIds != null) {
                        /* We're verifying the ID database. */
                        collectDbIds.add(
                            DatabaseId.fromBytes(cursorImpl.getCurrentKey()));
                    }

                    /*
                     * LN fetch errors at lower levels return an empty LN and
                     * add an error. Skip further checks in that case.
                     */
                    boolean validData = errors.isEmpty();

                    /* Verify LSNs before locking (can be expensive). */
                    if (validData &&
                        verifyLSNs(cursorImpl, priKey.getData())) {
                        /* A dangling LSN also prevents further checks. */
                        validData = false;
                    }

                    /* Need read lock to perform secondary checks. */
                    if (validData &&
                        (checkPriToSecRefs ||
                         checkSecToPriRefs ||
                         checkForeignKeyConstraints)) {
                        try {
                            if (cursor.get(
                                keyEntry, dataEntry, Get.CURRENT,
                                READ_LOCK) != null) {
                                locked = true;
                            }
                        } catch (LockConflictException e) {
                            noteLockConflict(e);
                        }
                    }
                    if (validData && locked) {
                        if (checkSecToPriRefs) {
                            secDBVer.corruptPrimaryRefs(dataEntry);
                            verifySecToPriRefs(
                                secDb, cursor, keyEntry, dataEntry);
                        }

                        if (checkForeignKeyConstraints) {
                            verifyForeignConstraint(
                                secDb, cursor, keyEntry, dataEntry, dbCache);
                        }

                        if (checkPriToSecRefs && !result.isTombstone()) {
                            verifyPriToSecRefs(
                                priDb, cursor, keyEntry, dataEntry,
                                result.getCreationTime(),
                                result.getModificationTime(),
                                result.getExpirationTime(),
                                result.getStorageSize(),
                                dbCache);
                        }
                    }
                } catch (OperationVerifyException e) {
                    /* This RuntimeException is handled in outer try block. */
                    throw e;
                } catch (RuntimeException e) {
                    System.err.println(e.getMessage());
                    LoggerUtils.logMsg(
                        exceptionLogger, envImpl, this, Level.SEVERE,
                        LoggerUtils.getStackTraceForSevereLog(e));
                    recordError(new VerifyError(
                        Problem.RECORD_ACCESS_EXCEPTION,
                        e.toString(), DbLsn.NULL_LSN));
                }

                /* Release record lock before invoking callback. */
                if (locked) {
                    locker.releaseNonTxnLocks(cursorImpl);
                }

                if (!summary.onRecord(
                    dbImpl.getName(), priKey.getData(),
                    (secKey != null) ? secKey.getData() : null,
                    readOnlyErrors)) {

                    stopVerify = true;
                    return true;
                }

                if (recordCount >= batchSize) {
                    return false;
                }

                result = cursor.get(keyEntry, dataEntry, Get.NEXT, NO_LOCK);
                if (result == null) {
                    /* All records in DB were verified. */
                    return true;
                }
            }
        } catch (OperationVerifyException e) {
            /* IN error prevents further Btree ops. */
            return true;

        } finally {
            cursorImpl.setTreeStatsAccumulator(null);
            tree.setTreeStatsAccumulator(null);
            EnvironmentImpl.decThreadLocalReferenceCount();

            cursor.close();
            locker.operationEnd();

            envImpl.getDbTree().releaseDbs(dbCache);
        }
    }

    /**
     * Positions the cursor at the first record after the given key/data.
     *
     * <p>Uses dirty-read so the record at the cursor position can be deleted
     * by another thread.</p>
     *
     * @param keyEntry is required.
     * @param dataEntry is required if DB has dups.
     * @return non-null if cursor is positioned, null if there are no more
     * records in the DB.
     */
    private static OperationResult findFirstRecord(
        final DatabaseImpl dbImpl,
        final Cursor cursor,
        final DatabaseEntry keyEntry,
        final DatabaseEntry dataEntry) {

        if (keyEntry.getData() == null) {
            /* Find the first record in this db. */
            return cursor.get(keyEntry, dataEntry, Get.FIRST, NO_LOCK);
        }

        final byte[] lastKeyBytes = keyEntry.getData();
        final byte[] lastDataBytes =
            dbImpl.getSortedDuplicates() ? dataEntry.getData() : null;

        /* Find record with key/data GTE the given key/data params. */
        final OperationResult result = cursor.get(
            keyEntry, dataEntry, Get.SEARCH_ANY_GTE, NO_LOCK);

        if (result == null) {
            /* No more records. */
            return null;
        }

        if (!Arrays.equals(lastKeyBytes, keyEntry.getData())) {
            /* Found next key. */
            return result;
        }

        if (lastDataBytes != null &&
            !Arrays.equals(lastDataBytes, dataEntry.getData())) {
            /* Found next dup. */
            return result;
        }

        /* Move past a record we have already processed. */
        return cursor.get(
            keyEntry, dataEntry, Get.NEXT, NO_LOCK);
    }

    /**
     * This method is called in StatsAccumulator.verifyNode, which means that
     * this method will execute every time it encounters one upperIN or BIN.
     *
     * <p>Because the latch is held here we only verify the basic structure of
     * all INs. LSN verification is deferred until latches are released, as
     * we're iterating records with the cursor. For UINs we accumulate their
     * LSN/keys for here for later verification. For BINs, which are always
     * latched EX, the LSN of each slot is verified as we move the cursor.</p>
     */
    private void basicBtreeVerify(final Node node) {

        verifyCommonStructure(node);
        if (!node.isUpperIN()) {
            return;
        }
        if (lsnsToCheck == null) {
            return;
        }
        final IN in = (IN) node;
        for (int i = 0; i < in.getNEntries(); i++) {
            final long lsn = in.getLsn(i);
            if (lsn == DbLsn.NULL_LSN) {
                continue;
            }
            lsnsToCheck.put(lsn, in);
        }
    }

    /**
     * Possible basic structure may contain:
     *  1. keyPrefix
     *  2. inMemorySize
     *  3. parent IN
     *  4. ordered Keys
     *  5. identifier Key and so on.
     *
     * On 1, the keyPrefix cannot be re-calculated from the full keys here,
     * since the full keys are not stored in the IN. We could get the full key
     * from the LNs, but this would be very slow.
     *
     * On 2, the inMemorySize may be slightly inaccurate, and this would not be
     * considered corruption. It is recalculated during checkpoints to account
     * for errors.
     *
     * For 3, we should verify that the node's parent is correct, i.e. the
     * parent should have a slot that refers to the child using the correct
     * key. But this has already been done in the current code:
     *      There are three places to call IN.accumulateStats, i.e. calling
     *      acc.processIN:
     *        1. Tree.getNextIN
     *        2. Tree.search
     *        3. Tree.searchSubTree
     *
     *      At these places, before calling IN.accumulateStats, the current
     *      code uses latchChildShared or latchChild to check whether the
     *      parent is right when holding the parent latch and child latch.
     *
     * For 4 and 5, we can check for corruption here.
     * For 4, whole keys need to be obtained using IN.getKey.
     * For 5, user's comparator function needs to be called if exists.
     */
    private void verifyCommonStructure(final Node node) {

        assert node.isIN();
        final IN in = (IN) node;

        verifyOrderedKeys(in);
        verifyIdentifierKey(in);
    }

    private void verifyOrderedKeys(final IN in) {

        final DatabaseImpl dbImpl = in.getDatabase();
        final InternalComparator userCompareToFcn = dbImpl.getKeyComparator();

        for (int i = 1; i < in.getNEntries(); i++) {
            final byte[] key1 = in.getKey(i);
            final byte[] key2 = in.getKey(i - 1);

            final int s = Key.compareKeys(key1, key2, userCompareToFcn);
            if (s > 0) {
                continue;
            }

            final String msg = CORRUPTION_MSG +
                "IN keys are out of order. " + in.toSafeString(i - 1, i);

            recordError(new VerifyError(
                Problem.INTERNAL_NODE_INVALID,
                msg, DbLsn.NULL_LSN));

            throw new OperationVerifyException();
        }
    }

    /**
     * Reports invalid id key if it is invalid because it is null or is not
     * present in one of the slots.
     */
    private void verifyIdentifierKey(final IN in) {

        if (in.isUpperIN() || in.isBINDelta() || in.getNEntries() == 0) {
            return;
        }

        final byte[] identifierKey = in.getIdentifierKey();
        if (identifierKey == null) {
            recordInvalidIdentifierKey(in);
            return;
        }

        /*
         * There are two problematic cases about identifierKey which are caused
         * by some errors in previous code:
         *
         * (1). The identifierKey is a prefix key due to the DupConvert bug.
         *
         *    When reading log files written by JE 4.1 or earlier, the
         *    identifier key may be incorrect because DupConvert did not
         *    convert it correctly. DupConvert converts the identifier key to
         *    a prefix key, so it will not match the complete key in any slot.
         *
         *    We should probably fix DupConvert. But even if we fix it now,
         *    it won't help users of JE 5.0 and above who have already upgraded
         *    from JE 4.1 or earlier, because DupConvert is only used when
         *    reading log files written by JE 4.1 or earlier.
         *
         *    This issue seems harmless, at least no user reports errors caused
         *    by it. So we can choose to ignore this issue. Normally, we can
         *    identify this issue by checking the end of the key for the
         *    PREFIX_ONLY value. But unfortunately this will also ignore
         *    identifier keys that happen to have the PREFIX_ONLY value at the
         *    end of a complete key(in the user's data).
         *
         *    Considering the following second issue, we choose to not check
         *    identifierKey for environments who is initially created with
         *    LogEntryType.LOG_VERSION being LT 15, where 15 is just the new
         *    log version of JE after we fix the following second issue.
         *
         * (2). The identifierKey is not in any slot due to the BIN-delta
         * mutation bug.
         *
         *     The fullBIN identifierKey may have changed when reconstituteBIN
         *     called BIN.compress. The previous code forgot to reset it. Now
         *     we fix this by resetting the identifier in BIN.mutateToFullBIN.
         *
         *     For the problematic identifierKey which is caused by the
         *     BIN-delta mutation bug, we do not have good methods to correct
         *     them. We can only detect them.
         *
         *     The problem with detecting them is that we know it is incorrect
         *     in past releases, but even when it is incorrect, we don't know
         *     the impact on the app in a particular case. It is possible that
         *     the app is working OK, even though the identifier key is
         *     incorrect. So if we detect it and the app stops working
         *     (because we invalidate the env) then we may be making things
         *     worse for the app -- this may not be what the user wants.
         *
         *  So combing above (1) and (2), we need to add a way to know the
         *  earliest log version of the env. Then we can only validate the
         *  identifierKey when this version is >= 15, where 15 is just the new
         *  log version of JE after we fix (2). See DbTree.initialLogVersion
         *  and LogEntryType.LOG_VERSION.
         */
        if (envImpl.getDbTree().getInitialLogVersion() < 15) {
            return;
        }

        final InternalComparator userCompareToFcn =
            in.getDatabase().getKeyComparator();

        for (int i = 0; i < in.getNEntries(); i++) {
            final byte[] key = in.getKey(i);
            if (Key.compareKeys(identifierKey, key, userCompareToFcn) == 0) {
                return;
            }
        }

        recordInvalidIdentifierKey(in);
    }

    private void recordInvalidIdentifierKey(final IN in) {

        final String msg = CORRUPTION_MSG +
            "IdentifierKey is null or not present in any slot. " +
            in.toSafeString();

        recordError(new VerifyError(
            Problem.INTERNAL_NODE_INVALID, msg, DbLsn.NULL_LSN));

        throw new OperationVerifyException();
    }

    /**
     * Returns whether the LSN is extinct or maybe-extinct, meaning that we
     * can't assume the LSN is active.
     *
     * <p>Should be called without holding locks or latches, to prevent
     * contention while calling the extinction filter callback.</p>
     */
    private boolean mayBeExtinct(final DatabaseImpl dbImpl,
                                 final long lsn,
                                 final byte[] priKey) {

        if (dbImpl.getSortedDuplicates()) {
            return false;
        }

        final ExtinctionStatus extinctionStatus = envImpl.getExtinctionStatus(
            dbImpl.getName(), dbImpl.getSortedDuplicates(),
            dbImpl.getDbType().isInternal(), null, priKey);

        if (extinctionStatus == ExtinctionStatus.NOT_EXTINCT) {
            return false;
        }

        /*
         * If we cannot get the extinction status then the app's metadata is
         * probably unavailable, which is an error condition. We can't take
         * further action because we don't know whether the LSN is used for
         * fetching. So just log a warning using a rate-limiting logger.
         */
        if (extinctionStatus == ExtinctionStatus.MAYBE_EXTINCT) {
            recordError(new VerifyError(
                Problem.MAYBE_EXTINCT,
                "ExtinctionStatus MAYBE_EXTINCT, could not verify LSN.", lsn));
        }

        return true;
    }

    /**
     * Returns true if a BIN slot's LSN is used for fetching the record,
     * meaning that it should exist in a data file.
     *
     * <p>This method must be called while latched and therefore it does not
     * check record extinction, which should be done for LN slots after
     * releasing the latch, using an LSN obtained while latched.</p>
     */
    private boolean isLnLsnActive(final int index, final BIN bin) {

        assert bin.isLatchOwner();

        if (envImpl.isMemOnly()) {
            return false;
        }

        final DatabaseImpl dbImpl = bin.getDatabase();

        if (dbImpl.getSortedDuplicates() ||
            dbImpl.isLNImmediatelyObsolete() ||
            bin.isEmbeddedLN(index) ||
            bin.isDefunct(index)) {
            return false;
        }
        return true;
    }

    /**
     * Returns active LSN (one that could be fetched), or NULL_LSN if the LSN
     * at the cursor slot is not active.
     */
    private long getActiveLnLsn(
        final CursorImpl cursorImpl,
        final byte[] priKey) {

        final long lsn;

        cursorImpl.latchBIN();
        try {
            final BIN bin = cursorImpl.getBIN();
            final int index = cursorImpl.getIndex();

            if (!isLnLsnActive(index, bin)) {
                return DbLsn.NULL_LSN;
            }

            lsn = bin.getLsn(index);

        } finally {
            cursorImpl.releaseBIN();
        }

        /* Check extinction after releasing latch to avoid contention. */
        if (mayBeExtinct(cursorImpl.getDb(), lsn, priKey)) {
            return DbLsn.NULL_LSN;
        }

        return lsn;
    }

    /**
     * Verifies LSN at the cursor position as well as any accumulated IN LSNs.
     *
     * @return whether cursor LSN is dangling.
     */
    private boolean verifyLSNs(final CursorImpl cursorImpl,
                               final byte[] priKey) {

        final long lnLsn = getActiveLnLsn(cursorImpl, priKey);
        boolean dangling = false;

        if (lnLsn != DbLsn.NULL_LSN) {
            dangling = verifyLSN(
                lnLsn, new CursorSlotInfo(cursorImpl));
        }
        if (dangling && btreeVerifyConfig.getRepairDataRecords()
             && !cursorImpl.getDb().isInternalDb()) {
             cursorImpl.latchBIN();
             try {
                 final BIN bin = cursorImpl.getBIN();
                 final int index = cursorImpl.getIndex();
                 bin.deleteEntry(index);
                 final String msg = "User lsn data records which were dangling"+ 
                                     " and were repaired ";
                 recordError(new VerifyError(
                             Problem.DANGLING_USER_LSN_REPAIRED, msg, lnLsn));
             } finally {
                 cursorImpl.releaseBIN();
             }
        }

        /*
         * Disable addition to lsnsToCheck (by setting to null) while we're
         * iterating over it. This prevents ConcurrentModificationException
         * in the rare case where the DB being verified is accessed by the
         * verifyLSN method. For example, we may be verifying the utilization
         * DB and read from it in verifyLSN to verify obsolete states.
         */
        final Map<Long, IN> lsns = lsnsToCheck;
        try {
            lsnsToCheck = null;
            for (final Map.Entry<Long, IN> entry : lsns.entrySet()) {
                verifyLSN(entry.getKey(), new ParentSlotInfo(entry.getValue()));
            }
        } finally {
            lsns.clear();
            lsnsToCheck = lsns;
        }

        return dangling;
    }

    private boolean verifyLSN(final long lsn,
                              final SlotInfo slotInfo) {

        final boolean dangling = verifyDanglingLSN(lsn, slotInfo);

        if (!dangling) {
            verifyObsoleteRecords(lsn, slotInfo);
            verifyReservedFileRef(lsn, slotInfo);
        }

        return dangling;
    }

    private interface SlotInfo {
        /**
         * Returns non-null Btree info if the LSN is active (will be fetched).
         * Must latch the BIN or IN to determine this. Releases the latch
         * before returning.
         */
        String checkActiveLsn(final long lsn);
        /**
         * Returns whether parent node is a BIN, meaning that LSN is for an LN.
         */
        boolean isBinNode();
    }

    /**
     * Slot info for LN LSN at cursor position.
     */
    class CursorSlotInfo implements SlotInfo {

        private final CursorImpl cursorImpl;

        CursorSlotInfo(final CursorImpl cursorImpl) {
            this.cursorImpl = cursorImpl;
        }

        @Override
        public boolean isBinNode() {
            return true;
        }

        @Override
        public String checkActiveLsn(final long lsn) {
            cursorImpl.latchBIN();
            try {
                final BIN bin = cursorImpl.getBIN();
                final int index = cursorImpl.getIndex();
                if (lsn == bin.getLsn(index) &&
                    isLnLsnActive(index, bin)) {
                    return bin.makeFetchErrorMsg("LN", null, lsn, index);
                } else {
                    return null;
                }
            } finally {
                cursorImpl.releaseBIN();
            }
        }
    }

    /**
     * Slot info for upper IN parent and child LSN.
     */
    class ParentSlotInfo implements SlotInfo {
        private final IN parent;

        ParentSlotInfo(final IN parent) {
            this.parent = parent;
        }

        @Override
        public boolean isBinNode() {
            return false;
        }

        @Override
        public String checkActiveLsn(final long lsn) {
            parent.latchSharedNoUpdateLRU();
            try {
                if (parent.getInListResident()) {
                    for (int i = 0; i < parent.getNEntries(); ++i) {
                        if (lsn == parent.getLsn(i)) {
                            return parent.makeFetchErrorMsg(
                                "IN", null, lsn, i);
                        }
                    }
                }
                return null;
            } finally {
                parent.releaseLatch();
            }
        }
    }

    /**
     * Check that LSN appears in an existing file for each IN slot.
     */
    private boolean verifyDanglingLSN(final long lsn,
                                      final SlotInfo slotInfo) {

        final long fileNum = DbLsn.getFileNumber(lsn);
        final long fileOffset = DbLsn.getFileOffset(lsn);

        /*
         * Check whether the corresponding file exist and whether the
         * LSN's offset is less than the file's length.
         */
        final int size = getFileSize(fileNum);

        if (fileOffset < size) {
            return false;
        }

        final String btreeInfo = slotInfo.checkActiveLsn(lsn);
        if (btreeInfo == null) {
            return false;
        }

        /* For security/privacy, we cannot output keys. */
        final String msg = CORRUPTION_MSG + "Active LSN " +
            (size == FILE_DELETED ?
                "file has been deleted: " :
             size == FILE_OUT_OF_BOUNDS ?
                "file is past last file number: " :
                "offset is larger than file size") +
            " fileSize=" + size + " " + btreeInfo;

        recordError(new VerifyError(
            (size == FILE_DELETED) ?
                Problem.LSN_FILE_MISSING :
                Problem.LSN_OUT_OF_BOUNDS,
            msg, lsn));

        if (slotInfo.isBinNode()) {
            return true;
        } else {
            throw new OperationVerifyException();
        }
    }

    /**
     * Returns file size or a negative value indicating whether the file was
     * deleted or larger than the last file: FILE_DELETED, FILE_OUT_OF_BOUNDS.
     */
    private int getFileSize(final long fileNum) {
        /*
         * The last file is a special case, because its totalSize is changing
         * and this file in the FileSummary is not volatile. For the last file
         * we can use getNextLsn to get the fileNum and offset of the last
         * file.
         */
        final long nextLsn = fileManager.getNextLsn();
        final long nextLsnFile = DbLsn.getFileNumber(nextLsn);

        if (fileNum > nextLsnFile) {
            return FILE_OUT_OF_BOUNDS;
        }

        if (fileNum == nextLsnFile) {
            return (int) DbLsn.getFileOffset(nextLsn);
        }

        return getAndCacheFileSize(fileNum);
    }

    /**
     * Used to get file sizes directly from the File class and cache this
     * size since files are immutable.
     *
     * <p>Note: Getting file size from UtilizationProfile is problematic
     * because it is not guaranteed to be accurate.</p>
     *
     * <p>Note that if an entry in the cache is present, we assume the file
     * exists even though it could have been deleted after we added it to the
     * cache. We chose this approach because it would be too expensive to do a
     * File.exists call for every LSN. If a file is deleted during verification
     * we may miss detection of a problem, but it will be detected during the
     * next verification run.</p>
     */
    private int getAndCacheFileSize(long fileNum) {

        final Integer sizeObj = fileSizeCache.get(fileNum);
        if (sizeObj != null) {
            return sizeObj;
        }

        final File file = new File(fileManager.getFullFileName(fileNum));
        final int size = file.exists() ? ((int) file.length()) : FILE_DELETED;
        fileSizeCache.put(fileNum, size);
        return size;
    }

    /**
     * Called when fetching an LN, which happens whenever verifyDataRecords is
     * set to true, the LN is separate (not an embedded LN or a dups DB), and
     * the LN is not already cached (which is normally true because
     * CacheMode.EVICT_LN is used in KVS).
     *
     * If this method is called from a thread that is not running verify, it
     * simply returns after checking a thread-local for null.
     */
    public static void verifyDataRecord(final LNLogEntry<?> ln,
                                        final BIN bin,
                                        final int idx) {
        final EnvironmentImpl env = bin.getEnv();
        final BtreeVerifyContext verifyContext = env.getBtreeVerifyContext();
        if (verifyContext == null) {
            return;
        }

        final long lnExp = ln.getExpirationTime();
        final long binExp = TTL.expirationToSystemTime(
                bin.getExpiration(idx), bin.isExpirationInHours());
        if (lnExp == binExp) {
            return;
        }

        verifyContext.recordError(new VerifyError(
            Problem.EXPIRATION_TIME_MISMATCH,
            "Expiration time in the BIN (" +
                TTL.formatExpirationTime(binExp) +
                ") does not match expiration time in the LN (" +
                TTL.formatExpirationTime(lnExp) +
                ") binExp=" + bin.getExpiration(idx) +
                " binHours=" + bin.isExpirationInHours() +
                " lnExp=" + ln.getExpiration() +
                " lnHours=" + ln.isExpirationInHours() +
                " binNodeId=" + bin.getNodeId() +
                " binLastLoggedLsn=" +
                DbLsn.getNoFormatString(bin.getLastLoggedLsn()) +
                " lnLsn=" + DbLsn.getNoFormatString(bin.getLsn(idx)),
            bin.getLsn(idx)));
    }

    /**
     * Checks that LSNs are not obsolete for each slot of BIN or IN.
     */
    private void verifyObsoleteRecords(final long lsn,
                                       final SlotInfo slotInfo) {

        if (!btreeVerifyConfig.getVerifyObsoleteRecords()) {
            return;
        }

        final long fileNum = DbLsn.getFileNumber(lsn);
        final long[] offsets = getObsoleteOffsets(fileNum);

        if (Arrays.binarySearch(offsets, DbLsn.getFileOffset(lsn)) < 0) {
            return;
        }

        final String btreeInfo = slotInfo.checkActiveLsn(lsn);
        if (btreeInfo == null) {
            return;
        }

        final String msg = CORRUPTION_MSG +
            "Active LSN is counted obsolete and file will be deleted: " +
            btreeInfo;
        recordError(new VerifyError(Problem.LSN_COUNTED_OBSOLETE, msg, lsn));
    }

    /**
     * Fetches and caches obsolete LSN offsets per file.
     *
     * <pre>
     * This cache may contain outdated information, since LSNs may become
     * obsolete during the verification process, and the cache is not updated.
     * This is OK because:
     *  - an obsolete LSN can never become active again, and
     *  - there is no requirement to detect corruption that occurs during the
     *    scan.
     * </pre>
     */
    private long[] getObsoleteOffsets(final long fileNum) {
        long[] offsets = obsoleteOffsetsCache.get(fileNum);
        if (offsets != null) {
            return offsets;
        }
        offsets = up.getObsoleteDetailSorted(fileNum);
        obsoleteOffsetsCache.put(fileNum, offsets);
        return offsets;
    }

    private void verifyReservedFileRef(final long lsn,
                                       final SlotInfo slotInfo) {

        final Long fileNum = DbLsn.getFileNumber(lsn);

        if (!envImpl.getFileProtector().isReservedFile(fileNum)) {
            return;
        }

        final String btreeInfo = slotInfo.checkActiveLsn(lsn);
        if (btreeInfo == null) {
            return;
        }

        final String msg = CORRUPTION_MSG +
            "Active LSN in reserved file, which will be deleted: " +
            btreeInfo;

        recordError(new VerifyError(Problem.LSN_IN_RESERVED_FILE, msg, lsn));

        if (!envImpl.isReadOnly() &&
            btreeVerifyConfig.getRepairReservedFiles()) {

            envImpl.getUtilizationProfile().reactivateReservedFile(fileNum);

            recordError(new VerifyError(
                Problem.RESERVED_FILE_REPAIRED, msg, lsn));
        }
    }

    /**
     * Log a (rate limited) message and count stat.
     */
    private void noteLockConflict(final LockConflictException e) {
        recordError(new VerifyError(
            Problem.SECONDARY_LOCK_CONFLICT,
            e.toString(), DbLsn.NULL_LSN));
    }

    /**
     * While scanning a secondary DB, check that the primary key referenced
     * by the secondary record exist in the primary DB.
     *
     * <p>When verifying index or foreign constraint, we first READ-lock the
     * secondary record (in verifyBatch) and then try to non-blocking
     * READ-lock the primary record. Using non-blocking is to avoid deadlocks,
     * since we are locking in the reverse of the usual order.</p>
     *
     * <p>If the non-blocking lock fails with LockNotAvailableException, we
     * will not be able to detect corruption and we should ignore this
     * exception and continue verification. In this case the primary record is
     * write-locked and is being modified by another thread, so it is OK to
     * skip this verification step in this case. This is a compromise.</p>
     *
     * <p>The above comments apply to verifyForeignConstraint as well.</p>
     */
    private void verifySecToPriRefs(
        final SecondaryDatabase secDb,
        final Cursor cursor,
        final DatabaseEntry key,
        final DatabaseEntry priKey) {

        assert secDb != null;
        envImpl.acquireSecondaryAssociationsReadLock();

        try {
            final SecondaryAssociation secAssoc =
                DbInternal.getSecondaryAssociation(secDb);
            if (secAssoc.isEmpty()) {
                return;
            }

            final Database priDb = secAssoc.getPrimary(priKey);
            if (priDb == null) {
                return;
            }
            /*
             * We only need to check whether the primary record exists, we
             * do not need the data.
             */
            final DatabaseEntry priData = new DatabaseEntry();
            priData.setPartial(0, 0, true);

            /*
             * Currently the secondary record is locked. In order to avoid
             * deadlock, here we use the non-blocking lock. In order to
             * release the lock on the primary record, we create a new
             * Locker to acquire the lock and release the lock in finally
             * block.
             */
            final Locker locker =
                LockerFactory.getInternalReadOperationLocker(envImpl);
            locker.setDefaultNoWait(true);

            try {
                /*
                 * Cursor.readPrimaryAfterGet may return null but this does
                 * NOT indicate index corruption, which is indicated only if
                 * OperationVerifyException is thrown.
                 */
                if (DbInternal.readPrimaryAfterGet(
                    cursor, priDb, key, priKey, priData, LockMode.DEFAULT,
                    false /*secDirtyRead*/, false /*lockPrimaryOnly*/,
                    true /*verifyOnly*/, locker, secDb, secAssoc) == null) {
                    if (secDBVer.isOfflineMode()) {
                        CursorImpl curImpl = DbInternal.getCursorImpl(cursor);
                        throw new SecondaryIntegrityException(
                                secDb, false, locker, "missing primary key",
                                secDb.getDatabaseName(),
                                priDb.getDatabaseName(), key, priKey,
                                CursorImpl.getCurrentLsn(curImpl),
                                curImpl.getExpirationTime(),
                                null);
                    }
                }
            } catch (LockNotAvailableException e) {
                noteLockConflict(e);

            } catch (SecondaryIntegrityException e) {
                noteSecondaryException(
                    Problem.PRIMARY_KEY_MISSING, e);

            } catch (IllegalStateException|OperationFailureException e) {
                /*
                 * The primary DB is likely closed or invalid. Stop
                 * verifications involving primary DB.
                 * This is not considered an error.
                 */
                checkSecToPriRefs = false;
                checkPriToSecRefs = false;

            } finally {
                /* Release primary record lock. */
                locker.operationEnd();
            }
        } finally {
            envImpl.releaseSecondaryAssociationsReadLock();
        }
    }

    /**
     * See method doc for verifySecToPriRefs.
     */
    private void verifyForeignConstraint(
        final SecondaryDatabase secDb,
        final Cursor cursor,
        final DatabaseEntry secKey,
        final DatabaseEntry priKey,
        final Map<DatabaseId, DatabaseImpl> dbCache) {

        assert secDb != null;

        final Database foreignDb =
            DbInternal.getPrivateSecondaryConfig(secDb).
                getForeignKeyDatabase();

        if (foreignDb == null) {
            return;
        }

        final DatabaseImpl foreignDbImpl =
            getRelatedDb(foreignDb, "foreign", dbCache);

        if (foreignDbImpl == null) {
            checkForeignKeyConstraints = false;
            return;
        }

        /*
         * We only need to check whether the corresponding record exists
         * in the foreign database.
         */
        /* Use the non-blocking lock. */
        final Locker locker =
            LockerFactory.getInternalReadOperationLocker(envImpl);
        locker.setDefaultNoWait(true);

        try (final Cursor foreignCursor = makeCursor(
                foreignDbImpl, locker, true /*retainNonTxnLocks*/)) {

            final OperationResult result;
            try {
                result = foreignCursor.get(
                    secKey, null, Get.SEARCH, READ_LOCK);
            } catch (LockNotAvailableException e) {
                noteLockConflict(e);
                return;
            } finally {
                locker.operationEnd();
            }

            if (result == null) {
                setSecondaryDbCorrupt(
                    Problem.FOREIGN_KEY_MISSING, secDb,
                    DbInternal.getCursorImpl(cursor).getLocker(),
                    "Secondary key does not exist in foreign database " +
                        foreignDbImpl.getName(),
                    secKey, priKey, DbInternal.getCursorImpl(cursor));
            }
        }
    }

    /**
     * While scanning a primary DB, check that all secondary keys referenced
     * by the primary record exist in the secondary DB.
     * <p>
     * This method is not called for tombstones (checked by the caller). We
     * cannot pass a tombstone to a secondary key creator.
     */
    private void verifyPriToSecRefs(
        Database priDb,
        final Cursor cursor,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final long creationTime,
        final long modificationTime,
        final long expirationTime,
        final int storageSize,
        final Map<DatabaseId, DatabaseImpl> dbCache) {
         
        if (!secDBVer.isOfflineMode()) {
            assert priDb != null;
        }
        envImpl.acquireSecondaryAssociationsReadLock();

        try {
            final SecondaryAssociation secAssoc;
            if (secDBVer.isOfflineMode()) {
                secAssoc = offlineSecAssoc;
                if (secAssoc == null || secAssoc.isEmpty()) {
                    return;
                }
                priDb = secAssoc.getPrimary(key);
                if (priDb == null) {
                    return;
                }
            } else {
                secAssoc = DbInternal.getSecondaryAssociation(priDb);
                if (secAssoc.isEmpty()) {
                    return;
                }
            }

            /*
             * If checkSecondaryKeysExist cannot find the secondary record,
             * it will throw SIE. At that time, the cursor used in
             * checkSecondaryKeysExist is not at a meaningful slot, so we get
             * the expirationTime of the corresponding primary record here
             * and then pass it to checkSecondaryKeysExist.
             */
            for (final SecondaryDatabase secDb :
                    secAssoc.getSecondaries(key)) {
                /*
                 * If the primary database is removed from the
                 * SecondaryAssociation, then we will skip checking any
                 * secondary database.
                 *
                 * Besides, if the primary database is removed from the
                 * SecondaryAssociation, secAssoc.getPrimary may throw
                 * exception.
                 */
                
                try {
                    if (secAssoc.getPrimary(key) != priDb ) {
                        return;
                    }
                } catch (Exception e) {
                    return;
                }

                /*
                 * If the secondary database is in population phase, it
                 * may be reasonable that the BtreeVerifier can not find
                 * the corresponding secondary records for the checked
                 * primary record, because the primary record has not been
                 * populated to the secondary database.
                 */
                if (secDb.isIncrementalPopulationEnabled()) {
                    continue;
                }

                if (!checkSecondaryKeysExist(
                    priDb, secDb, key, data, creationTime,
                    modificationTime, expirationTime, storageSize,
                    dbCache, secAssoc,
                    DbInternal.getCursorImpl(cursor))) {
                    if (!secDBVer.isOfflineMode()) {
                        return;
                    }
                    throw new OperationVerifyException();
                }

            }
        } finally {
            envImpl.releaseSecondaryAssociationsReadLock();
        }
    }

    /**
     * Returns false if verification should not proceed.
     */
    private boolean checkSecondaryKeysExist(
        final Database priDb,
        final SecondaryDatabase secDb,
        final DatabaseEntry priKey,
        final DatabaseEntry priData,
        final long creationTime,
        final long modificationTime,
        final long expirationTime,
        final int storageSize,
        final Map<DatabaseId, DatabaseImpl> dbCache,
        final SecondaryAssociation secAssoc,
        final CursorImpl priCursor) {
        final SecondaryConfig secondaryConfig;
        if (secDBVer.isOfflineMode()) {
            secondaryConfig = keyCreatorLookup.get(secDb.getDatabaseName());
        } else {
            secondaryConfig = DbInternal.getPrivateSecondaryConfig(secDb);
        }
        if (secondaryConfig == null) {
            return false;
        }
        final SecondaryKeyCreator keyCreator = secondaryConfig.getKeyCreator();

        final SecondaryMultiKeyCreator multiKeyCreator =
            secondaryConfig.getMultiKeyCreator();
        if (keyCreator == null && multiKeyCreator == null) {
            assert priDb.getConfig().getReadOnly();
            return true;
        }

        final DatabaseImpl secDbImpl =
            getRelatedDb(secDb, "secondary", dbCache);

        if (secDbImpl == null) {
            checkPriToSecRefs = false;
            return false;
        }

        if (keyCreator != null) {
            /* Each primary record may have a single secondary key. */
            assert multiKeyCreator == null;

            DatabaseEntry secKey = new DatabaseEntry();
            if (!keyCreator.createSecondaryKey(
                    secDb, priKey, priData, creationTime,
                    modificationTime, expirationTime, storageSize, secKey)) {
                /* This primary record has no secondary keys. */
                return true;
            }
            secDBVer.corruptSecondaryRefs(secKey);   
            checkOneSecondaryKeyExists(
                secDb, secDbImpl, priKey, secKey, priCursor, priDb, secAssoc);

            return true;
        }

        final Set<DatabaseEntry> secKeys = new HashSet<>();
        multiKeyCreator.createSecondaryKeys(secDb, priKey, priData,
                                            creationTime,
                                            modificationTime,
                                            expirationTime,
                                            storageSize,
                                            secKeys);

        if (secKeys.isEmpty()) {
            return true;
        }

        for (final DatabaseEntry secKey : secKeys) {

            if (!checkOneSecondaryKeyExists(
                secDb, secDbImpl, priKey, secKey, priCursor, priDb,
                secAssoc)) {
                return true;
            }
        }

        return true;
    }

    private boolean checkOneSecondaryKeyExists(
        final SecondaryDatabase secDb,
        final DatabaseImpl secDbImpl,
        final DatabaseEntry priKey,
        final DatabaseEntry secKey,
        final CursorImpl priCursor,
        final Database priDb,
        final SecondaryAssociation secAssoc) {
        final Locker locker =
            LockerFactory.getInternalReadOperationLocker(envImpl);

        try (final Cursor checkCursor = makeCursor(
             secDbImpl, locker, false /*retainNonTxnLocks*/)) {

            if (checkCursor.get(
                secKey, priKey, Get.SEARCH_BOTH, NO_LOCK) == null) {

                /* Same reason as that in verifyPriToSecRefs. */
                try {
                    if (secAssoc.getPrimary(priKey) != priDb ||
                        secDb.isIncrementalPopulationEnabled()) {
                        return false;
                    }
                } catch (Exception e) {
                    return false;
                }

                final String errMsg =
                    "Secondary is corrupt: the primary record contains a " +
                        "key that is not present in this secondary database.";

                setSecondaryDbCorrupt(
                    Problem.SECONDARY_KEY_MISSING, secDb, locker,
                    errMsg, secKey, priKey, priCursor);

                return false;
            }
        } finally {
            locker.operationEnd();
        }

        return true;
    }

    private void setSecondaryDbCorrupt(
        final Problem problem,
        final SecondaryDatabase secDb,
        final Locker locker,
        final String errMsg,
        final DatabaseEntry secKey,
        final DatabaseEntry priKey,
        final CursorImpl priCursor) {

        final DatabaseImpl priDb = priCursor.getDb();

        /*
         * Rather than construct an SIE here we could directly construct a
         * VerifyError. For now we're preserving the option to report the SIE
         * to the application. Also the SIE gives us a consistent error msg.
         */
        final SecondaryIntegrityException e = new SecondaryIntegrityException(
            secDb, false, locker, errMsg, secDb.getDatabaseName(),
            priDb.getName(), secKey, priKey,
            CursorImpl.getCurrentLsn(priCursor),
            priCursor.getExpirationTime(),
            EnvironmentImpl.getExtinctionStatus(priDb, priKey));

        noteSecondaryException(problem, e);
    }

    private void noteSecondaryException(
        final Problem problem,
        final SecondaryIntegrityException e) {
        recordError(new VerifyError(problem, e.getMessage(), DbLsn.NULL_LSN));
    }

    /**
     * Calls getDb using DB handle to ensure stability of DatabaseImpl while
     * we're accessing it.
     */
    private DatabaseImpl getRelatedDb(
        final Database db,
        final String relation,
        final Map<DatabaseId, DatabaseImpl> dbCache) {

        final String dbName = db.getDatabaseName();
        final DatabaseId dbId;
        try {
            dbId = DbInternal.getDbImpl(db).getId();
        } catch (IllegalStateException|OperationFailureException e) {
            /* Not considered an error. */
            return null;
        }

        envImpl.checkOpen();

        final DatabaseImpl dbImpl;
        try {
            dbImpl = envImpl.getDbTree().getDb(dbId, dbCache);
        } catch (Exception e) {
            LoggerUtils.logMsg(
                exceptionLogger, envImpl, this, Level.SEVERE,
                LoggerUtils.getStackTraceForSevereLog(e));

            recordError(new VerifyError(
                Problem.DATABASE_ACCESS_EXCEPTION,
                "Unexpected exception looking up related " + relation +
                    " DB " + dbName +
                    ", preventing further secondary verification." +
                    " dbId: " + dbId + " exception: " + e.toString(),
                DbLsn.NULL_LSN));
            return null;
        }

        if (dbImpl == null || dbImpl.isDeleting()) {
            /* Not considered an error. */
            envImpl.getDbTree().releaseDb(dbImpl);
            return null;
        }

        return dbImpl;
    }

    void setStopVerifyFlag(boolean val) {
        stopVerify = val;
    }

    boolean getStopVerifyFlag() {
        return stopVerify;
    }

    public void setBackground(boolean background) {
        this.background = background;
    }

    public void setBtreeVerifyConfig(VerifyConfig btreeVerifyConfig) {
        this.btreeVerifyConfig = btreeVerifyConfig;

        logLevel = btreeVerifyConfig.getErrorLogLevel();
    }

    private class VerifierStatsAccumulator extends StatsAccumulator {
        VerifierStatsAccumulator(
            PrintStream progressStream,
            int progressInterval) {
            super(progressStream, progressInterval);
        }

        /**
         * @throws OperationVerifyException if an error is detected, which
         * will cause the verifier to move to the next database.
         */
        @Override
        public void verifyNode(Node node) {
            basicBtreeVerify(node);
        }
    }

    /**
     * Creates a Cursor and excludes its operations from OpStats, which is
     * important because user DBs are accessed. (Internal DBs are automatically
     * excluded from OpStats.)
     */
    private Cursor makeCursor(final DatabaseImpl dbImpl,
                              final Locker locker,
                              final boolean retainNonTxnLocks) {
        final Cursor cursor =
            DbInternal.makeCursor(dbImpl, locker, null, retainNonTxnLocks);

        DbInternal.excludeFromOpStats(cursor);

        return cursor;
    }

    private VerifyError firstSevereError(final List<VerifyError> errors) {
        return errors.stream()
            .filter(VerifyError::isSevere)
            .findFirst()
            .orElse(null);
    }

    /**
     * Logs the first severe error for this record/database, if any, using the
     * rate-limited logger.
     */
    private void logError(final String dbName,
                          final byte[] priKey,
                          final byte[] secKey) {

        if (logLevel == null || errors.isEmpty()) {
            return;
        }

        final VerifyError error = firstSevereError(errors);
        if (error == null) {
            return;
        }

        final StringBuilder msg = new StringBuilder("Verify error");

        if (dbName != null) {
            msg.append(" DB=").append(dbName);
        }

        if (envImpl.getExposeUserData()) {
            if (priKey != null) {
                msg.append(" priKey=").append(Key.getNoFormatString(priKey));
            }
            if (secKey != null) {
                msg.append(" secKey=").append(Key.getNoFormatString(secKey));
            }
        }

        msg.append(" ").append(error.toString());

        LoggerUtils.logMsg(
            problemLogger, envImpl, error.getProblem(), logLevel,
            msg.toString());
    }

    private static class Summary implements VerifySummary {
        long recordsVerified;
        long recordsWithErrors;
        long otherErrors;
        boolean severeErrors;
        boolean canceled;
        final Set<Long> missingFilesReferenced = new HashSet<>();
        final Set<Long> reservedFilesReferenced = new HashSet<>();
        final Set<Long> reservedFilesRepaired = new HashSet<>();
        final Set<Long> userLSNRepaired = new HashSet<>();
        final Set<String> databasesVerified = new HashSet<>();
        final Set<String> databasesWithErrors = new HashSet<>();
        final Map<Problem, Long> problemCounts = new HashMap<>();

        @Override
        public boolean hasErrors() {
            return !problemCounts.isEmpty();
        }

        @Override
        public boolean hasSevereErrors() {
            return severeErrors;
        }

        @Override
        public boolean wasCanceled() {
            return canceled;
        }

        @Override
        public long getRecordsVerified() {
            return recordsVerified;
        }

        @Override
        public long getRecordsWithErrors() {
            return recordsWithErrors;
        }

        @Override
        public long getOtherErrors() {
            return otherErrors;
        }

        @Override
        public Set<Long> getMissingFilesReferenced() {
            return missingFilesReferenced;
        }

        @Override
        public Set<Long> getReservedFilesReferenced() {
            return reservedFilesReferenced;
        }

        @Override
        public Set<Long> getReservedFilesRepaired() {
            return reservedFilesRepaired;
        }

        @Override
        public Set<Long> getUserLSNRepaired() {
            return userLSNRepaired;
        }

        @Override
        public Set<String> getDatabasesVerified() {
            return databasesVerified;
        }

        @Override
        public Set<String> getDatabasesWithErrors() {
            return databasesWithErrors;
        }

        @Override
        public Map<Problem, Long> getProblemCounts() {
            return problemCounts;
        }

        @Override
        public String toString() {
            return "[VerifySummary" +
                " canceled=" + canceled +
                " recordsVerified=" + recordsVerified +
                " hasSevereErrors=" + severeErrors +
                " recordsWithErrors=" + recordsWithErrors +
                " databasesVerified=" + databasesVerified +
                " databasesWithErrors=" + databasesWithErrors +
                " otherErrors=" + otherErrors +
                " filesMissing=" +
                toHexString(missingFilesReferenced) +
                " reservedFilesReferenced=" +
                toHexString(reservedFilesReferenced) +
                " reservedFilesRepaired=" +
                toHexString(reservedFilesRepaired) +
                " userLSNRepaired=" +
                toHexString(userLSNRepaired) +
                " problemCounts=" + problemCounts +
                "]";
        }

        private static String toHexString(final Collection<Long>files) {
            final StringBuilder buf = new StringBuilder("[");
            boolean first = true;
            for (long f : files) {
                if (first) {
                    first = false;
                } else {
                    buf.append(", ");
                }
                buf.append("0x").append(Long.toHexString(f));
            }
            buf.append("]");
            return buf.toString();
        }

        public long totalProblems() {
            return problemCounts.values().stream()
                .mapToLong(Long::longValue).sum();
        }
    }

    /**
     * Records summary information, logs errors, calls the user-supplied
     * listener and finally clears the errors.
     */
    private class SummaryListener extends Summary implements VerifyListener {
        private final VerifyListener listener;

        SummaryListener(final VerifyListener listener) {
            this.listener = (listener != null) ? listener : noopVerifyListener;
        }

        @Override
        public boolean onBegin() {
            final boolean keepGoing = listener.onBegin();
            if (!keepGoing) {
                canceled = true;
            }
            return keepGoing;
        }

        @Override
        public void onEnd(final VerifySummary summary) {
            listener.onEnd(summary);
            secDBVer.checkVerifier(summary);
        }

        @Override
        public boolean onRecord(final String dbName,
                                final byte[] priKey,
                                final byte[] secKey,
                                final List<VerifyError> errors) {

            logError(dbName, priKey, secKey);

            ++recordsVerified;
            databasesVerified.add(dbName);

            if (!errors.isEmpty()) {
                ++recordsWithErrors;
                databasesWithErrors.add(dbName);
                if (!severeErrors && firstSevereError(errors) != null) {
                    severeErrors = true;
                }
            }

            countProblems(errors);

            final boolean keepGoing = listener.onRecord(
                dbName, priKey, secKey, errors);
            BtreeVerifier.this.errors.clear();

            if (!keepGoing) {
                canceled = true;
            }
            return keepGoing;
        }

        @Override
        public boolean onDatabase(final String dbName,
                                  final List<VerifyError> errors) {

            logError(dbName, null, null);

            databasesVerified.add(dbName);

            if (!errors.isEmpty()) {
                databasesWithErrors.add(dbName);
            }

            countProblems(errors);

            final boolean keepGoing = listener.onDatabase(dbName, errors);
            BtreeVerifier.this.errors.clear();

            if (!keepGoing) {
                canceled = true;
            }
            return keepGoing;
        }

        @Override
        public boolean onOtherError(final List<VerifyError> errors) {
            assert !errors.isEmpty();

            logError(null, null, null);

            ++otherErrors;

            countProblems(errors);

            final boolean keepGoing = listener.onOtherError(errors);
            BtreeVerifier.this.errors.clear();

            if (!keepGoing) {
                canceled = true;
            }
            return keepGoing;
        }

        private void countProblems(final List<VerifyError> errors) {

            for (final VerifyError error : errors) {

                problemCounts.compute(
                    error.getProblem(),
                    (k, v) -> (v == null) ? 1 : (v + 1));

                final long fileNum = error.getLsnFile();
                if (fileNum >= 0) {
                    switch (error.getProblem()) {
                    case LSN_FILE_MISSING:
                        missingFilesReferenced.add(fileNum);
                        break;
                    case LSN_IN_RESERVED_FILE:
                        reservedFilesReferenced.add(fileNum);
                        break;
                    case RESERVED_FILE_REPAIRED:
                        reservedFilesRepaired.add(fileNum);
                        break;
                    case DANGLING_USER_LSN_REPAIRED:
                        userLSNRepaired.add(error.getLsn());
                    default:
                        /* Other problems are not collected. */
                    }
                }
            }
        }
    }

    /**
     * Listener that does nothing. Used to avoid checking listener variables
     * for null.
     */
    private static final VerifyListener noopVerifyListener =
        new VerifyListener() {

        @Override
        public boolean onBegin() {
            return true;
        }

        @Override
        public void onEnd(final VerifySummary summary) {
        }

        @Override
        public boolean onRecord(final String dbName,
                                final byte[] priKey,
                                final byte[] secKey,
                                final List<VerifyError> errors) {
            return true;
        }

        @Override
        public boolean onDatabase(final String dbName,
                                  final List<VerifyError> errors) {
            return true;
        }

        @Override
        public boolean onOtherError(final List<VerifyError> errors) {
            return true;
        }
    };

    /**
     * A corruption class added to corrupt the secondary and primary references
     * 
     */
    private static final class SecondaryDBVerifier {
        private boolean run;
        private boolean offlineMode;
        SecondaryDBVerifier() {
            run = false;
            offlineMode = false;
        }

        public void runCorrupter() {
            run = true;
        }

        public void runOfflineMode() {
            offlineMode = true;
        }

        public boolean isOfflineMode() {
            return offlineMode;
        }

        private void corruptData(DatabaseEntry entry) {
            byte[] bytearray = entry.getData();
            bytearray[bytearray.length - 2] = (byte)0;
            entry.setData(bytearray);
        }

        public void corruptPrimaryRefs(DatabaseEntry primaryKey) {
            if (!run) {
               return;
            }
            int randomNum = ThreadLocalRandom.current().nextInt(1, 11);
            if (randomNum % 10 != 0) {
                return;
            }
            corruptData(primaryKey); 
        }

        public void corruptSecondaryRefs(DatabaseEntry secondaryKey) {
            if (!run) {
                return;
            }
            int randomNum = ThreadLocalRandom.current().nextInt(1, 11);
            if (randomNum % 10 != 0) {
                return;
            }
            corruptData(secondaryKey);
        }
        public void checkVerifier(final VerifySummary summary) {
            if (!run) {
                return;
            }
            assert summary.getProblemCounts().keySet().
                contains(Problem.PRIMARY_KEY_MISSING);
            assert summary.getProblemCounts().keySet().
                contains(Problem.SECONDARY_KEY_MISSING);
        }
    };
}
