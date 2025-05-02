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

import java.util.Properties;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.VerifyError;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.RestoreMarker;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.utilint.LoggerUtils;

public class VerifierUtils {

    private static final String EXCEPTION_KEY = "ex";

    private static final String RESTORE_REQUIRED_MSG =
        "The environment may not be opened due to persistent data " +
            "corruption that was detected earlier. The marker file " +
            "(7fffffff.jdb) may be deleted to allow recovery, but " +
            "this is normally unsafe and not recommended. " +
            "Original exception:\n";

    /**
     * Return an exception that can be thrown by the caller.
     *
     * <p>If {@link EnvironmentConfig#LOG_CHECKSUM_FATAL} is true and the env
     * is not read-only, a restore marker file is created from the
     * origException.</p>
     *
     * @param origException the exception contains the properties that are
     * stored to the marker file.
     *
     * @return a {@link OperationVerifyException} if the verifier is running,
     * else an {@link EnvironmentFailureException}. When an EFE is thrown it
     * will invalidate the environment.
     */
    public static DatabaseException handleChecksumError(
        final long lsn,
        final Throwable origException,
        final EnvironmentImpl envImpl) {

        final String checksumMsg = "Persistent corruption detected: " +
            origException.toString();

        /*
         * Do not throw EFE or create the marker file when the error is
         * detected by the Btree verifier.
         */
        final BtreeVerifyContext btreeVerifyContext =
            envImpl.getBtreeVerifyContext();

        if (btreeVerifyContext != null) {
            btreeVerifyContext.recordError(new VerifyError(
                VerifyError.Problem.CHECKSUM_ERROR, checksumMsg, lsn));
            return new OperationVerifyException();
        }

        final RestoreRequired.FailureType failureType =
            RestoreRequired.FailureType.LOG_CHECKSUM;

        String markerFileError = "";

        /*
         * If env is read-only (for example when using the DbVerify command
         * line) we cannot create the marker file, but we should still create
         * and return an invalidating EFE indicating persistent corruption.
         */
        if (envImpl.isChecksumErrorFatal() && !envImpl.isReadOnly()) {
            final Properties props = new Properties();

            props.setProperty(
                EXCEPTION_KEY, origException.toString() + "\n" +
                    LoggerUtils.getStackTrace(origException));
            boolean enable = envImpl.getConfigManager().getBoolean(
                EnvironmentParams.RESTOREMARKER_ENABLE);
            final RestoreMarker restoreMarker = new RestoreMarker(
                envImpl.getFileManager(), envImpl.getLogManager(), enable);

            try {
                restoreMarker.createMarkerFile(failureType, props);
            } catch (RestoreMarker.FileCreationException e) {
                markerFileError = " " + e.getMessage();
            }
        }

        return new EnvironmentFailureException(
            envImpl, EnvironmentFailureReason.LOG_CHECKSUM,
            checksumMsg + markerFileError, origException);
    }

    /*
     * Get a message referencing the original data corruption exception.
     */
    public static String getRestoreRequiredMessage(
        RestoreRequired restoreRequired) {

        Properties p = restoreRequired.getProperties();
        return RESTORE_REQUIRED_MSG + p.get(EXCEPTION_KEY);
    }
}
