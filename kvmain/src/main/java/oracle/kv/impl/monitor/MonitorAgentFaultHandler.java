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

package oracle.kv.impl.monitor;

import java.util.logging.Logger;

import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.ServiceStatusTracker;

/**
 * Monitor agents should never cause process exit. Faults should be recorded
 * as well as possible, although an error found from monitoring may indicate
 * a general error in the monitoring system.
 */
public class MonitorAgentFaultHandler extends ProcessFaultHandler {

    public MonitorAgentFaultHandler(
        Logger logger, ServiceStatusTracker serviceStatusTracker)
    {
        super(logger, ProcessExitCode.RESTART, serviceStatusTracker);
    }

    /**
     * Do nothing, don't shutdown.
     */
    @Override
    protected void queueShutdownInternal(Throwable fault,
                                         ProcessExitCode exitCode) {
        /* do nothing. */
    }

    /**
     * Wrap it inside an AdminFaultException, if it isn't already an
     * InternalFaultException. The fault is an InternalFaultException when it
     * originated at a different service, and is just passed through.
     */
    @Override
    protected RuntimeException getThrowException(RuntimeException fault) {
        if (fault instanceof InternalFaultException) {
            return fault;
        }
        if (fault instanceof ClientAccessException) {
            return ((ClientAccessException)fault).getCause();
        }
        return new MonitorAgentFaultException(fault);
    }

    /**
     * Monitor agents should never shuts down, so all exit codes are eaten in
     * this method. If this is a problem with a true exit code, we do log
     * the problem.
     */
    @Override
    public ProcessExitCode getExitCode(RuntimeException fault) {

        /* This is a pass-through exception, which was logged elsewhere */
        if (fault instanceof InternalFaultException) {
            return null;
        }

        /*
         * Report the error, but don't return an exit code. We don't want the
         * process to exit, but we do want to leave some information about the
         * problem. Also write this to stderr, in case the logging system has
         * failed, say due to out of disk,
         */
        String msg =
            "Exception encountered. but process will remain active: " + fault;
        logger.severe(msg);
        if (!EmbeddedMode.isEmbedded()) {
            System.err.println(msg);
        }
        return null;
    }

    private static class MonitorAgentFaultException
        extends InternalFaultException {
        private static final long serialVersionUID = 1L;

        MonitorAgentFaultException(Throwable cause) {
            super(cause);
        }
    }
}
