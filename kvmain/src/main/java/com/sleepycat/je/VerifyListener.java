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

import java.util.List;

import com.sleepycat.je.VerifyError.Problem;

/**
 * Can be used during Btree verification to collect information in addition to
 * what is provided by {@link VerifySummary}, to monitor progress, and to
 * cancel verification prematurely.
 *
 * <p>Note that all databases are verified when the entire Environment is
 * verified, including internal JE databases. An error in an internal JE
 * database is likely to be severe since it could prevent HA capabilities
 * from working, prevent access to other databases, etc. Internal databases
 * can be easily identified using {@link Database#isInternalDatabaseName}.</p>
 *
 * @see VerifyConfig#setListener(VerifyListener)
 */
public interface VerifyListener {

    /**
     * Called before verification starts.
     *
     * @return false to cancel verification, in which case {@link #onEnd} is
     * not called.
     */
    boolean onBegin();

    /**
     * Called when verification ends for any reason.
     *
     * @param summary is the same object returned by the verify method.
     */
    void onEnd(VerifySummary summary);

    /**
     * Called after verifying a record.
     *
     * @param dbName the database name.
     *
     * @param priKey the primary key. If the database being verified is a
     * secondary database, this is the key of the associated primary record.
     *
     * @param secKey the record's secondary key, or null if the database
     * being verified is not a secondary database.
     *
     * @param errors the errors detected for this record, or an empty list if
     * there were none.
     *
     * @return false to cancel verification, in which case {@link #onEnd} is
     * called.
     */
    boolean onRecord(String dbName,
                     byte[] priKey,
                     byte[] secKey,
                     List<VerifyError> errors);

    /**
     * Called after verifying a database.
     *
     * <p>Database verification may be aborted when certain problems are
     * detected, such as {@link Problem#INTERNAL_NODE_INVALID}. Each
     * Problem constant that causes verification of a database to abort is
     * documented as such.</p>
     *
     * @param dbName the database name.
     *
     * @param errors the errors detected that caused database verification to
     * be aborted, or an empty list if database verification finished normally
     * or verification was canceled.
     *
     * @return false to cancel verification, in which case {@link #onEnd} is
     * called.
     */
    boolean onDatabase(String dbName,
                       List<VerifyError> errors);

    /**
     * Called when an error is detected that is not associated with a specific
     * database.
     *
     * @param errors is a non-empty list containing the errors.
     *
     * @return false to cancel verification, in which case {@link #onEnd} is
     * called.
     */
    boolean onOtherError(List<VerifyError> errors);
}
