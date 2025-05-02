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

package oracle.kv.impl.fault;

/**
 * Informs a process level handler that the fault is at a more global system
 * level and restarting the process is unlikely to result in forward progress
 * until an administrator takes some form of corrective action.
 * <p>
 * The handler for this exception is expected to exit with the
 * {@link ProcessExitCode#NO_RESTART} exit code. The SNA or a shell script
 * monitoring the process then does its best to draw the administrator's
 * attention to the problem so that the root cause of the fault can be
 * addressed. Once the problem has been addressed the process can be restarted
 * by the administrator.
 * <p>
 * Since the fault handling design requires that this exception be used
 * entirely within a process, it's not Serializable.
 *
 * @see ProcessFaultException
 */
@SuppressWarnings("serial")
public class SystemFaultException extends RuntimeException {
    /**
     * Constructor to wrap a fault and indicate that it's a System level
     * failure that's not restartable.
     *
     * @param msg a message further explaining the error
     * @param e the exception being wrapped
     */
    public SystemFaultException(String msg, Exception e) {
        super(msg, e);
        assert e != null;
    }

    /**
     * The process exit code indicating that the process must be restarted.
     *
     * @return the process exit code
     */
    public ProcessExitCode getExitCode() {
        return ProcessExitCode.NO_RESTART;
    }
}
