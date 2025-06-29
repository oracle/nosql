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

package oracle.kv.impl.admin;

import java.util.function.Supplier;
import java.util.logging.Logger;

import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;

/**
 * The fault handler for the AdminService.
 *
 * - IllegalCommandException is a subclass of OperationFaultException used to
 * indicate user error, such as a bad parameter, or a user-provoked illegal
 * plan transition. It is thrown synchronously, in direct response to a user
 * action. IllegalCommandException is used rather than the standard
 * java.lang.IllegalArgumentException or java.lang.IllegalStateException to let
 * us distinguish between user problems and IllegalExceptions reported from
 * other Java libraries or other kv components which indicate that there has
 * been a logic error. The CLI/GUI should indicate to the user that the action
 * which incurred the IllegalCommandException should be corrected and retried
 *
 * - OperationFaultException is thrown when an administrative command fails due
 * to some transient situation or resource problem, such as a
 * RejectedExecutionException from lack of threads, or a network problem, lack
 * of open ports, timeout. There is no sense of permanent corruption or
 * unexpected state; the command can be repeated safely and may succeed at a
 * later try.
 *
 * - NonfatalAssertionException indicate that an assertion-style condition has
 * failed, but that the problem is not deemed severe enough to cause the Admin
 * to exit and restart. Instead, the problem is logged and noted through the
 * monitoring system.
 *
 * All other Java exceptions cause the Admin to exit and restart.
 * IllegalStateException is the usual choice for when finding that an assertion
 * style condition has been violated and should cause the Admin to exit.
 */
public class AdminServiceFaultHandler extends ProcessFaultHandler {
    final AdminService owner;
    /** The fault that results in the process shutting down. */
    private Throwable shutdownFault;
    /** Tells whether to suppress printing to stdout in getExitCode. */
    private boolean suppressPrinting = false;

    /**
     * Creates a fault handler for the AdminService
     *
     * @param logger the logger associated with the service
     */
    public AdminServiceFaultHandler(Logger logger, AdminService owner) {
        super(logger, ProcessExitCode.RESTART,
              owner.getServiceStatusTracker());
        this.owner = owner;
    }

    /**
     * Initiates an async shutdown in a separate thread. Note that there is no
     * way to really guarantee, that the process does not exit before the call
     * completes. From a practical standpoint this is a very unlikely event.
     */
    @Override
    protected void queueShutdownInternal(Throwable fault,
                                         ProcessExitCode exitCode) {
        shutdownFault = fault;
        if (!owner.getUsingThreads()) {
            final ServiceStatus status =
                (ProcessExitCode.NO_RESTART == exitCode) ?
                ServiceStatus.ERROR_NO_RESTART :
                ServiceStatus.ERROR_RESTARTING;
            owner.update(status, fault != null ? fault.toString() : "fault");
            haltAfterWait(HALT_AFTER_WAIT_MS, fault, exitCode);
        }
        new AsyncShutdownThread(exitCode).start();
    }

    /**
     * Wrap it inside an AdminFaultException, if it isn't already an
     * InternalFaultException or ClientAccessException or a
     * WrappedClientException of some sort.
     *
     * The fault is an InternalFaultException when it originated at a different
     * service, and is just passed through.
     */
    @Override
    protected RuntimeException getThrowException(RuntimeException fault) {
        if (fault instanceof InternalFaultException) {
            return fault;
        }

        if (fault instanceof ClientAccessException) {
            /*
             * This is a security exception generated by the client.
             * Unwrap it so that the client sees it in its original form.
             */
            return ((ClientAccessException) fault).getCause();
        }

        if (fault instanceof WrappedClientException) {
            /*
             * Client exceptions are are passed through to the client.
             */
            return fault;
        }

        return getAdminFaultException(fault);
    }

    /**
     * Wraps a RuntimeException as an AdminFaultException.
     */
    private RuntimeException getAdminFaultException(RuntimeException fault) {
        if (fault instanceof CommandFaultException) {
            return AdminFaultException.wrapCommandFault(
                (CommandFaultException) fault);
        }
        if (fault instanceof DatabaseException ||
            fault instanceof Admin.DBOperationFailedException) {
           return new AdminFaultException(fault, fault.getMessage(),
                                          ErrorMessage.NOSQL_5300,
                                          CommandResult.NO_CLEANUP_JOBS);
        }
        if (fault instanceof NonfatalAssertionException) {
            return new AdminFaultException(fault, fault.getMessage(),
                                           ErrorMessage.NOSQL_5500,
                                           CommandResult.NO_CLEANUP_JOBS);
        }
        return new AdminFaultException(fault);
    }

    /**
     * Decide whether to exit based on the Exception being handled.  If it is a
     * NonFatalAssertionException, log severe but do not exit.  Unrecognized
     * exceptions cause the process to exit.  Note: OperationFaultException
     * handling does not take this path at all; such are handled in
     * ProcessFaultHandler.rethrow.
     */
    @Override
    public ProcessExitCode getExitCode(RuntimeException fault) {

        final ProcessExitCode exitCode = super.getExitCode(fault);

        /* This is a pass-through exception, which was logged elsewhere */
        if (fault instanceof InternalFaultException) {
            return null;
        }

        /* This is a pass-through exception, which was logged elsewhere */
        if (fault instanceof WrappedClientException) {
            return null;
        }

        /* If the admin is already trying to shut down, let it. */
        if (owner.getAdmin() == null || owner.getAdmin().isClosing()) {
            return exitCode;
        }

        /*
         * Report the error as severe in any case.
         */
        String msg =
            "Unanticipated exception encountered: " +
            LoggerUtils.getStackTrace(fault);
        logger.severe(msg);

        if (fault instanceof NonfatalAssertionException) {
            /* The fault had a localized effect; does not require shutdown. */
            return null;
        }

        if (fault instanceof UnsupportedOperationException) {
            /* The fault indicate a call of the incompatible method. It does
             * not require the admin service to shutdown */
            return null;
        }

        if (fault instanceof EnvironmentFailureException &&
            ((EnvironmentFailureException) fault).isCorrupted()) {

            /* Do not restart if environment is corrupted */
            return ProcessExitCode.getNoRestart(fault);
        }

        /* The fault is fatal to the Admin; it should shut down and restart. */
        if (!suppressPrinting && !EmbeddedMode.isEmbedded()) {
            System.err.println(msg);
        }
        return exitCode;
    }

    /**
     * If a CommandFaultException wraps another exception, it needs to get a
     * correct error code based on that exception.
     */
    @Override
    protected ProcessExitCode internalGetExitCode(RuntimeException fault) {
        if (fault instanceof IllegalCommandException) {
            return super.internalGetExitCode(fault);
        }
        if (fault instanceof CommandFaultException &&
            fault.getCause() != null) {
            final Throwable t = fault.getCause();
            if (t instanceof RuntimeException) {
                return super.internalGetExitCode((RuntimeException) t);
            }
            /*
             * We do not encourage to wrap an error in CommandFaultException.
             * Following check is done just in case.
             */
            if (t instanceof Error) {
                return ProcessExitCode.getRestart(t);
            }
        }
        return super.internalGetExitCode(fault);
    }

    private class AsyncShutdownThread extends Thread {
        final ProcessExitCode exitCode;

        AsyncShutdownThread(ProcessExitCode exitCode) {
            super();
            this.exitCode = exitCode;
            /* Don't hold up the exit. */
            setDaemon(true);
        }

        @Override
        public void run() {
            Thread.yield();
            /*
             * The exception should already have been logged, but print it here
             * to stderr as a last resource in case the logging system has
             * itself failed, say because we have run out of disk space.
             */
            if (!suppressPrinting && !EmbeddedMode.isEmbedded()) {
                System.err.println("Process exiting due to fault");
                if (shutdownFault != null) {
                    shutdownFault.printStackTrace(System.err);
                }
            }

            /**
             * Do not exit if running in a thread context.
             */
            if (!owner.getUsingThreads()) {

                final ServiceStatus status =
                    (ProcessExitCode.NO_RESTART == exitCode) ?
                    ServiceStatus.ERROR_NO_RESTART :
                    ServiceStatus.ERROR_RESTARTING;
                owner.update(status,
                             shutdownFault != null ?
                             "exception: " + shutdownFault.toString() :
                             "fault");

                /**
                 * Flush logs and stderr.
                 */
                LoggerUtils.closeAllHandlers();
                System.err.flush();
                System.exit(exitCode.getValue());
            }
        }
    }

    public void setSuppressPrinting() {
        suppressPrinting = true;
    }

    /**
     * Each command may need to wrap a RuntimeException to a
     * CommandFaultException, in order to describe this exception with standard
     * error message and related information, so that clients can generate
     * correct result reports based on it. The following classes that extend
     * Supplier and Runnable provide convenient patterns to do this.
     */

    public static abstract class SimpleCommandOp <R>
        implements Supplier<R> {

        @Override
        public R get() {
            try {
                return perform();
            } catch (RuntimeException e) {
                throw getWrappedException(e);
            }
        }

        /**
         * Get a wrapped RuntimeException in case of need.
         */
        protected abstract RuntimeException
            getWrappedException(RuntimeException e);

        protected abstract R perform();
    }

    public static abstract class SimpleCommandProc implements Runnable {
        @Override
        public void run() {
            try {
                perform();
            } catch (RuntimeException e) {
                throw getWrappedException(e);
            }
        }

        /**
         * Get a wrapped RuntimeException in case of need.
         */
        protected abstract RuntimeException
            getWrappedException(RuntimeException e);

        protected abstract void perform();
    }
}
