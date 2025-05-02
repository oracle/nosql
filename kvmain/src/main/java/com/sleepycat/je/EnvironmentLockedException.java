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
 * Thrown by the {@link Environment} constructor when an environment cannot be
 * opened for write access because another process has the same environment
 * open for write access.
 *
 * <p><strong>Warning:</strong> This exception should be handled when an
 * environment is opened by more than one process.</p>
 */
public class EnvironmentLockedException extends EnvironmentFailureException {

    private static final long serialVersionUID = 629594964L;

    /** 
     * For internal use only.
     * @hidden 
     */
    public EnvironmentLockedException(EnvironmentImpl envImpl,
                                      String message) {
        super(envImpl, EnvironmentFailureReason.ENV_LOCKED, message);
    }

    /** 
     * Only for use by wrapSelf methods.
     */
    private EnvironmentLockedException(String message,
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

        return new EnvironmentLockedException(msg, clonedCause);
    }
}
