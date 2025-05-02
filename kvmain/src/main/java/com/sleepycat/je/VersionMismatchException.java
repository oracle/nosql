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

import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Thrown by the Environment constructor when an environment cannot be
 * opened because the version of the existing log is not compatible with the
 * version of JE that is running.  This occurs when a later version of JE was
 * used to create the log, or when the log was written using JE 4 or earlier.
 *
 * <p><strong>Warning:</strong> This exception should be handled when more than
 * one version of JE may be used to access an environment.</p>
 *
 * @since 4.0
 */
public class VersionMismatchException extends EnvironmentFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public VersionMismatchException(EnvironmentImpl envImpl, String message) {
        super(envImpl, EnvironmentFailureReason.VERSION_MISMATCH, message);
    }

    /** 
     * Only for use by wrapSelf methods.
     */
    private VersionMismatchException(String message,
                                     EnvironmentFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public EnvironmentFailureException wrapSelf(
        String msg,
        EnvironmentFailureException clonedCause) {

        return new VersionMismatchException(msg, clonedCause);
    }
}
