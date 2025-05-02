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
 * Informs a process level handler that the process can recover from the fault
 * by being restarted.
 *
 * <p>
 * The handler for this exception is expected to exit with the
 * {@link ProcessExitCode#RESTART} exit code. The SNA or a shell script which
 * takes appropriate action based upon the processes's exit code ten restarts
 * the process.
 * <p>
 * Since the fault handling design requires that this exception be used
 * entirely within a process, it's not Serializable.
 *
 * @see SystemFaultException
 */
@SuppressWarnings("serial")
public class ProcessFaultException extends RuntimeException {

    /**
     * Constructor to wrap a fault and indicate that it's restartable.
     *
     * @param msg a message further explaining the error
     * @param fault the exception being wrapped
     */
    public ProcessFaultException(String msg, Exception fault) {
        super(msg, fault);
        assert fault != null;
    }

    @Override
    public synchronized RuntimeException getCause() {
        return (RuntimeException)super.getCause();
    }

    /**
     * The process exit code indicating that the process must be restarted.
     *
     * @return the process exit code
     */
    public ProcessExitCode getExitCode() {
        return ProcessExitCode.RESTART;
    }
}
