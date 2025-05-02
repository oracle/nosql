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

package com.sleepycat.je.cleaner;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.SortedLSNTreeWalker;
import com.sleepycat.je.dbi.SortedLSNTreeWalker.TreeNodeProcessor;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.UtilizationFileReader;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Verify cleaner data structures
 */
public class VerifyUtils {

    private static final boolean DEBUG = false;

    /**
     * Compare the LSNs referenced by a given Database to the lsns held
     * in the utilization profile. Assumes that the database and
     * environment is quiescent, and that there is no current cleaner
     * activity.
     */
    public static void checkLsns(Database db)
        throws DatabaseException {

        checkLsns(DbInternal.getDbImpl(db), System.out);
    }

    /**
     * Compare the lsns referenced by a given Database to the lsns held
     * in the utilization profile. Assumes that the database and
     * environment is quiescent, and that there is no current cleaner
     * activity.
     */
    private static void checkLsns(DatabaseImpl dbImpl,
                                  PrintStream out)
        throws DatabaseException {

        /* Get all the LSNs in the database. */
        GatherLSNs gatherLsns = new GatherLSNs();
        long rootLsn = dbImpl.getTree().getRootLsn();
        List<DatabaseException> savedExceptions = new ArrayList<>();

        SortedLSNTreeWalker walker =
            new SortedLSNTreeWalker(new DatabaseImpl[] { dbImpl },
                                    new long[] { rootLsn },
                                    gatherLsns, savedExceptions, null);
        walker.walk();

        /* Print out any exceptions seen during the walk. */
        if (savedExceptions.size() > 0) {
            out.println(savedExceptions.size() +
                        " problems seen during tree walk for checkLsns");
            for (DatabaseException savedException : savedExceptions) {
                out.println("  " + savedException);
            }
        }

        Set<Long> lsnsInTree = gatherLsns.getLsns();
        if (rootLsn != DbLsn.NULL_LSN) {
            lsnsInTree.add(rootLsn);
        }

        /* Get all the files used by this database. */
        Iterator<Long> iter = lsnsInTree.iterator();
        Set<Long> fileNums = new HashSet<>();

        while (iter.hasNext()) {
            long lsn = iter.next();
            fileNums.add(DbLsn.getFileNumber(lsn));
        }

        /* Gather up the obsolete LSNs in these file summary LNs. */
        iter = fileNums.iterator();
        Set<Long> obsoleteLsns = new HashSet<>();
        EnvironmentImpl envImpl = dbImpl.getEnv();
        UtilizationProfile profile = envImpl.getUtilizationProfile();

        while (iter.hasNext()) {
            Long fileNum = iter.next();

            PackedOffsets obsoleteOffsets = profile.getObsoleteDetailPacked(
                fileNum, false /*logUpdate*/, null,
                false /*obsoleteBeforeCkpt*/);
            PackedOffsets.Iterator obsoleteIter = obsoleteOffsets.iterator();
            while (obsoleteIter.hasNext()) {
                long offset = obsoleteIter.next();
                Long oneLsn = DbLsn.makeLsn(fileNum, offset);
                obsoleteLsns.add(oneLsn);
                if (DEBUG) {
                    out.println("Adding 0x" + Long.toHexString(oneLsn));
                }
            }
        }

        /* Check than none the LSNs in the tree is in the UP. */
        boolean error = false;
        iter = lsnsInTree.iterator();
        while (iter.hasNext()) {
            Long lsn = iter.next();
            if (obsoleteLsns.contains(lsn)) {
                out.println("Obsolete LSN set contains valid LSN " +
                            DbLsn.getNoFormatString(lsn));
                error = true;
            }
        }

        /*
         * Check that none of the LSNs in the file summary LN is in the
         * tree.
         */
        iter = obsoleteLsns.iterator();
        while (iter.hasNext()) {
            Long lsn = iter.next();
            if (lsnsInTree.contains(lsn)) {
                out.println("Tree contains obsolete LSN " +
                            DbLsn.getNoFormatString(lsn));
                error = true;
            }
        }

        if (error) {
            throw new EnvironmentFailureException
                (envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                 "Lsn mismatch");
        }

        if (savedExceptions.size() > 0) {
            throw new EnvironmentFailureException
                (envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                 "Sorted LSN Walk problem");
        }
    }

    private static class GatherLSNs implements TreeNodeProcessor {
        private final Set<Long> lsns = new HashSet<>();

        @Override
        public void processLSN(long childLSN,
                               LogEntryType childType,
                               Node ignore,
                               byte[] ignore2,
                               int ignore3,
                               boolean ignore4) {
            if (childLSN != DbLsn.NULL_LSN) {
                lsns.add(childLSN);
            }
        }

        public Set<Long> getLsns() {
            return lsns;
        }

        @Override
        public void noteMemoryExceeded() {
        }
    }

    /**
     * Compare utilization as calculated by UtilizationProfile to utilization
     * as calculated by UtilizationFileReader.  Also check that per-database
     * and per-file utilization match.
     *
     * @throws EnvironmentFailureException if there are mismatches
     */
    public static void verifyUtilization(EnvironmentImpl envImpl,
                                         boolean expectAccurateObsoleteLNCount,
                                         boolean expectAccurateObsoleteLNSize)
        throws DatabaseException {

        Map<Long,FileSummary> profileMap = envImpl.getCleaner()
            .getUtilizationProfile()
            .getFileSummaryMap(true);

        /* Flush the log before reading. */
        envImpl.getLogManager().flushNoSync();

        /* Create per-file map of recalculated utilization info. */
        Map<Long,FileSummary> recalcMap =
            UtilizationFileReader.calcFileSummaryMap(envImpl);

        /*
         * Loop through each file in the per-file profile, checking it against
         * the recalculated map and database derived maps.
         */
        for (Map.Entry<Long, FileSummary> entry : profileMap.entrySet()) {
            Long file = entry.getKey();
            String fileStr = "0x" + Long.toHexString(file);
            FileSummary profileSummary = entry.getValue();
            FileSummary recalcSummary = recalcMap.remove(file);
            checkTrue(fileStr, recalcSummary != null);
            /*
            if (expectAccurateObsoleteLNCount &&
                expectAccurateObsoleteLNSize &&
                profileSummary.obsoleteLNSize !=
                recalcSummary.getObsoleteLNSize()) {
                System.out.println("file=" + file);
                System.out.println("profile=" + profileSummary);
                System.out.println("recalc=" + recalcSummary);
            }
            //*/
            /*
            if (expectAccurateObsoleteLNCount &&
                profileSummary.obsoleteLNCount !=
                recalcSummary.obsoleteLNCount) {
                System.out.println("file=" + file);
                System.out.println("profile=" + profileSummary);
                System.out.println("recalc=" + recalcSummary);
            }
            //*/
            /*
            if (recalcSummary.totalCount !=
                    profileSummary.totalCount) {
                System.out.println("file=" + file);
                System.out.println("profile=" + profileSummary);
                System.out.println("recalc=" + recalcSummary);
            }
            //*/
            checkEquals(fileStr,
                recalcSummary.totalCount, profileSummary.totalCount);
            checkEquals(fileStr,
                recalcSummary.totalSize, profileSummary.totalSize);
            checkEquals(fileStr,
                recalcSummary.totalINCount, profileSummary.totalINCount);
            checkEquals(fileStr,
                recalcSummary.totalINSize, profileSummary.totalINSize);
            checkEquals(fileStr,
                recalcSummary.totalLNCount, profileSummary.totalLNCount);
            checkEquals(fileStr,
                recalcSummary.totalLNSize, profileSummary.totalLNSize);

            /*
             * Currently we cannot verify obsolete INs because
             * UtilizationFileReader does not count them accurately.
             */
            final boolean DEBUG = false;
            if (DEBUG) {
                checkEquals(fileStr,
                    recalcSummary.obsoleteINCount,
                    profileSummary.obsoleteINCount);
            }

            /*
             * The obsolete LN count/size is inaccurate when a deleted LN is
             * not counted properly by recovery because its parent INs were
             * flushed and the obsolete LN was not found in the tree.
             */
            if (expectAccurateObsoleteLNCount) {
                checkEquals(fileStr,
                    recalcSummary.obsoleteLNCount,
                    profileSummary.obsoleteLNCount);

                /*
                 * The obsolete LN size is inaccurate when a tree walk is
                 * performed for truncate/remove or an abortLsn is counted by
                 * recovery.
                 */
                if (expectAccurateObsoleteLNSize) {
                    checkEquals(fileStr,
                        recalcSummary.getObsoleteLNSize(),
                        profileSummary.obsoleteLNSize);
                }
            }
        }
        checkTrue(recalcMap.toString(), recalcMap.isEmpty());
    }

    private static void checkTrue(String errorMessage, boolean checkIsTrue) {
        if (!checkIsTrue) {
            throw EnvironmentFailureException.unexpectedState(errorMessage);
        }
    }

    private static void checkEquals(String errorMessage,
                                    long expect,
                                    long actual) {
        if (expect != actual) {
            throw EnvironmentFailureException.unexpectedState(
                errorMessage + " expected=" + expect + " actual=" + actual);
        }
    }
}
