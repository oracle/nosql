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

package com.sleepycat.je;

import java.util.Map;
import java.util.Set;

import com.sleepycat.je.VerifyError.Problem;

/**
 * Summarizes information about a verification. Returned by verify methods and
 * passed to {@link VerifyListener#onEnd}.
 *
 * <p>Note that all databases are verified when the entire Environment is
 * verified, including internal JE databases. An error in an internal JE
 * database is likely to be severe since it could prevent HA capabilities
 * from working, prevent access to other databases, etc. Internal databases
 * can be easily identified using {@link Database#isInternalDatabaseName}.</p>
 *
 * @see Environment#verify(VerifyConfig)
 * @see Database#verify(VerifyConfig)
 */
public interface VerifySummary {

    /**
     * Whether any errors were detected.
     */
    boolean hasErrors();

    /**
     * Whether any {@link VerifyError#isSevere severe} errors were detected.
     */
    boolean hasSevereErrors();

    /**
     * Whether a {@link VerifyListener} method returned false to cancel the
     * verification.
     */
    boolean wasCanceled();

    /**
     * Number of records verified, which is the number of calls to
     * {@link VerifyListener#onRecord}.
     */
    long getRecordsVerified();

    /**
     * Number of records with errors, which is the number of calls to
     * {@link VerifyListener#onRecord} with a non-empty errors list.
     */
    long getRecordsWithErrors();

    /**
     * Set of databases that were verified, which are the databases passed to
     * {@link VerifyListener#onDatabase}.
     */
    Set<String> getDatabasesVerified();

    /**
     * Set of databases that were verified incompletely due to errors,
     * which are the databases passed to {@link VerifyListener#onDatabase}
     * with a non-empty errors list.
     */
    Set<String> getDatabasesWithErrors();

    /**
     * Number of times that an error has occurred that was not associated
     * with a specific database, which is the number of calls to
     * {@link VerifyListener#onOtherError}.
     */
    long getOtherErrors();

    /**
     * Set of missing files that were referenced by active data.
     * These files represent {@link Problem#LSN_FILE_MISSING}.
     */
    Set<Long> getMissingFilesReferenced();

    /**
     * Set of reserved files that were referenced by active data.
     * These files represent {@link Problem#LSN_IN_RESERVED_FILE}.
     */
    Set<Long> getReservedFilesReferenced();

    /**
     * Set of reserved files that were referenced by active data and have
     * been repaired by reactivating them.
     * These files represent {@link Problem#RESERVED_FILE_REPAIRED}.
     */
    Set<Long> getReservedFilesRepaired();

    /**
     * Set of lsn that were referenced by btree but dangling  and have
     * been repaired by removing them from tree.
     * These files represent {@link Problem#DANGLING_USER_LSN_REPAIRED}.
     */
    Set<Long> getUserLSNRepaired();

    /**
     * Map of problems to the number of VerifyErrors reported for each problem.
     */
    Map<Problem, Long> getProblemCounts();
}
