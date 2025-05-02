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
 * Thrown by the {@link Environment} constructor when {@code EnvironmentConfig
 * AllowCreate} property is false (environment creation is not permitted), but
 * there are no log files in the environment directory.
 *
 * @since 4.0
 */
public class EnvironmentNotFoundException extends EnvironmentFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public EnvironmentNotFoundException(EnvironmentImpl envImpl,
                                        String message) {
        super(envImpl, EnvironmentFailureReason.ENV_NOT_FOUND, message);
    }

    /** 
     * Only for use by wrapSelf methods.
     */
    private EnvironmentNotFoundException(String message,
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

        return new EnvironmentNotFoundException(msg, clonedCause);
    }
}
