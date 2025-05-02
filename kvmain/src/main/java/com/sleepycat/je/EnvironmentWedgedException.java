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
 * Thrown by the {@link Environment#close()} when the current process must be
 * shut down and restarted before re-opening the Environment.
 * <p>
 * If during close(), a badly behaved internal thread cannot be stopped,
 * then the JVM process must be stopped and restarted. The close() method first
 * attempts a soft shutdown of each thread. If that fails to stop the thread,
 * it is interrupted. If that fails to stop the thread, because it never
 * becomes interruptible, then {@code EnvironmentWedgedException} is thrown by
 * close(), after performing as much of the normal shutdown process as
 * possible. Before this exception is thrown, a full thread dump is logged, to
 * aid in debugging.
 * <p>
 * Note that prior to calling close(), if JE attempts to shut down an internal
 * thread and it cannot be shut down, the Environment will be {@link
 * Environment#isValid() invalidated}, also causing an {@code
 * EnvironmentWedgedException} to be thrown. In this case (as in all other
 * cases where an {@link EnvironmentFailureException} is thrown and the
 * Environment is invalidated), the application should call {@link
 * Environment#close()}. The close() method will throw {@code
 * EnvironmentWedgedException} in this case, as described above.
 * <p>
 * If the application fails to restart the process when this exception is
 * thrown, it is likely that re-opening the Environment will not be possible,
 * or will result in unpredictable behavior. This is because the thread that
 * stopped may be holding a resource that is needed by the newly opened
 * Environment.
 *
 * @since 7.1
 */
public class EnvironmentWedgedException extends EnvironmentFailureException {

    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public EnvironmentWedgedException(EnvironmentImpl envImpl,
                                      String message) {
        super(envImpl, EnvironmentFailureReason.WEDGED, message);
    }

    /**
     * Only for use by wrapSelf methods.
     */
    private EnvironmentWedgedException(String message,
                                       EnvironmentFailureException cause) {
        super(message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public EnvironmentWedgedException wrapSelf(
        String msg,
        EnvironmentFailureException clonedCause) {

        return new EnvironmentWedgedException(msg, clonedCause);
    }
}
