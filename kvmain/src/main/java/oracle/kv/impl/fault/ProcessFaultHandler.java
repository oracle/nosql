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

import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.whenComplete;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.impl.api.table.TableVersionException;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryRuntimeException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.RollbackProhibitedException;

/**
 * The process level fault handler (ProcessFaultHandler) and its
 * application-specific subclasses intercept all RuntimeExceptions and perform
 * the following basic tasks:
 * <ol>
 * <li>
 * Log the fault.</li>
 * <li>
 * Update monitoring data. Note that we currently do not have a coordination
 * mechanism in place to ensure that the monitored data is pulled from the
 * process before it exits. TODO</li>
 * <li>
 * Initiates a shutdown of the process if the exception is not a
 * OperationFaultException.</li>
 * </ol>
 * <p>
 * If fault is detected in an RMI service thread, the exception is re-thrown.
 * There is a certain level of coordination that is required between the RMI
 * service call and the shutdown to ensure that the process does not exit
 * before the RMI call is completed. If the fault is detected in a web service
 * request, similar considerations apply. The error indication must be sent to
 * the caller before the process exits.
 * <p>
 * Shutting down the faulting request with a specific and explicit failure
 * indicator is desirable but not required behavior. It permits the request
 * initiator to log the cause of the fault at the client thus enabling better
 * fault analysis. In the absence of an explicit failure indicator, the broken
 * tcp connection would result in an IOException on the client instead causing
 * the request to be aborted in any case.
 * <p>
 * The faulting process exits with an appropriate exit code to indicate whether
 * the SNA should attempt to restart it. If the SNA itself is the process in
 * question, then init.d will attempt to restart it.
 */
public abstract class ProcessFaultHandler
    implements AsyncEndpointGroupFaultHandler {

    /**
     * The number of milliseconds to wait for System.exit to perform an orderly
     * shutdown before performing a halt, which skips any shutdown hooks.
     */
    protected static final long HALT_AFTER_WAIT_MS = 60_000;

    /**
     * The default process exit code used for a fault that's not a subclass of
     * {@link SystemFaultException} or a {@link ProcessFaultException}.
     */
    private final ProcessExitCode defaultExitCode;

    protected Logger logger;

    /** The service status tracker to notify of status changes. */
    private final ServiceStatusTracker statusTracker;

    /**
     * Whether a shutdown has been requested, to make sure the shutdown only
     * occurs once.
     */
    private final AtomicBoolean shutdownRequested = new AtomicBoolean();

    public ProcessFaultHandler(Logger logger,
                               ProcessExitCode defaultExitCode,
                               ServiceStatusTracker statusTracker) {
        this.logger = logger;
        this.defaultExitCode = defaultExitCode;
        this.statusTracker = checkNull("statusTracker", statusTracker);
    }

    public ProcessExitCode getDefaultExitCode() {
        return defaultExitCode;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void execute(Runnable r) {
        try {
            r.run();
        } catch (RuntimeException re) {
            rethrow(re);
        } catch (Error e) {
            rethrow(e);
        }
    }

    /**
     * Executes the operation inside a process level fault handler that forces
     * a shutdown if the operation throws a RuntimeException.
     * <p>
     * Note that any changes in the bodies of these methods must be
     * appropriately replicated in the next three methods that are variants of
     * this one.
     *
     * @param <R> The return type associated with the operation
     * @param <E> The type of exception thrown by the operation
     * @param operation the operation whose faults are to be handled
     * @return the return value when the operation is successful
     */
    public <R, E extends Exception> R execute(Operation<R, E> operation)
        throws E {

        try {
            return operation.execute();
        } catch (RuntimeException re) {
            rethrow(re);
        } catch (Error e) {
            rethrow(e);
        }
        assert false : " code not reachable ";
        return null;
    }

    /**
     * An overloading of the above method for simple operations that do not
     * throw exceptions.
     */
    public <R> R execute(Supplier<R> operation) {

        try {
            return operation.get();
        } catch (RuntimeException re) {
            rethrow(re);
        } catch (Error e) {
            rethrow(e);
        }

        assert false : " code not reachable ";
        return null;
    }

    /**
     * An overloading of the above method for Procedures.
     */
    public <E extends Exception> void execute(Procedure<E> proc)
        throws E {

        try {
            proc.execute();
        } catch (RuntimeException re) {
            rethrow(re);
        } catch (Error e) {
            rethrow(e);
        }
    }

    /**
     * Execute an operation that produces a future and handle any exceptions
     * thrown.
     */
    public <R> CompletableFuture<R>
        executeFuture(Supplier<CompletableFuture<R>> proc)
    {
        CompletableFuture<R> future = null;
        try {
            future = proc.get();
        } catch (Throwable t) {
            future = failedFuture(t);
        }
        return whenComplete(
            future,
            (r, e) -> {
                if (e instanceof RuntimeException) {
                    rethrow((RuntimeException) e);
                } else if (e instanceof Error) {
                    rethrow((Error) e);
                } else if (e != null) {
                    throw new AssertionError("Not reached");
                }
            });
    }

    /**
     * Performs the processing required around an Error before re-throwing it.
     */
    public void rethrow(Error error) {

        try {
            if (logger != null) {
                logger.log(Level.SEVERE, "Process exiting with error", error);
            }
            throw error;
        } finally {
            /*
             * Queue shutdown after the logging for the throw has completed.
             */
            queueShutdown(error, ProcessExitCode.RESTART);
        }
    }

    /**
     * Performs the processing required around a RuntimeException before
     * re-throwing it.
     */
    public void rethrow(final RuntimeException requestException)
        throws FaultException {

        final ProcessExitCode exitCode = internalGetExitCode(requestException);
        try {
            if (logger != null) {
                if (exitCode != null) {
                    logger.log(Level.SEVERE, "Process exiting",
                               requestException);
                } else {
                    /* Reduce logging output for errors that are not severe */
                    if (logger.isLoggable(Level.FINE)) {
                        final String msg =
                            "Process fault handler handled exception: " +
                            requestException.getClass().getName() +
                            " Exception message: " +
                            requestException.getMessage();
                        logger.fine(msg);
                    }
                }
            }
            throw getThrowException(requestException);
        } finally {
            /*
             * Queue shutdown after the logging for the throw has completed.
             */
            if (exitCode != null) {
                /* Must be the very last thing that's done. */
                queueShutdown(requestException, exitCode);
            }
        }
    }

    protected ProcessExitCode internalGetExitCode(RuntimeException e) {
        ProcessExitCode exitCode;
        try {
            /* Throw to dispatch based upon the exception type. */
            throw e;
        } catch (ProcessFaultException rfe) {
            exitCode = rfe.getExitCode();
        } catch (SystemFaultException sfe) {
            exitCode = sfe.getExitCode();
        } catch (OperationFaultException nfe) {
            exitCode = null; /* Don't exit the process. */
        } catch (ClientAccessException cae) {
            exitCode = null; /* Don't exit the process. */
        } catch (KVSecurityException kvse) {
            exitCode = null; /* Don't exit the process. */
        } catch (SessionAccessException sae) {
            exitCode = null; /* Don't exit the process. */
        } catch (QueryException qe) {
            exitCode = null; /* Don't exit the process. */
        } catch (QueryStateException qse) {
            exitCode = null; /* Don't exit the process. */
        } catch (QueryRuntimeException qre) {
            exitCode = null; /* Don't exit the process. */
        } catch (TableVersionException tve) {
            exitCode = null; /* Don't exit the process. */
        } catch (RollbackProhibitedException rpe) {
            /*
             * Exit and disable process restart. Since manual admin intervention
             * is required restart is not going to fix the problem and may
             * result in the process being caught in a restart loop,
             * overwriting useful information in the logs.
             */
            exitCode = ProcessExitCode.getNoRestart(rpe);
        } catch (RuntimeException re) {
            exitCode = getExitCode(re);
        }
        return exitCode;
    }

    /**
     * Returns the exception that will be thrown out of the handler. Subclasses
     * of this class can override this method to wrap exceptions appropriately.
     *
     * @param requestException the runtime exception that was actually
     * encountered while processing a request.
     *
     * @return the exception to be thrown
     */
    protected RuntimeException
        getThrowException(RuntimeException requestException) {

        if (requestException instanceof ClientAccessException) {
            /*
             * This is a security exception generated by the client.
             * Unwrap it so that the client sees it in its orginal form.
             */
            throw (RuntimeException) requestException.getCause();
        }

        return requestException;
    }

    /**
     * Determines whether the process should exit due to the runtime exception
     * and the exit code it should use upon exit. This implementation returns
     * the default exit code unless requestException implements InjectedFault,
     * in which case it returns ProcessExitCode#INJECTED_RESTART.
     *
     * @param requestException the runtime exception that was actually
     * encountered while processing a request. Note that it may be different
     * from the one that is actually thrown out of the handler; the latter is
     * determined by {@link #getThrowException(RuntimeException)}.
     *
     * @return null if the process should not exit. A non null value if it
     * should.
     */
    protected ProcessExitCode getExitCode(RuntimeException requestException) {
        return requestException instanceof InjectedFault ?
            ProcessExitCode.INJECTED_FAULT_RESTART :
            defaultExitCode;
    }

    /**
     * Initiates a process shutdown request. The method is simply a wrapper
     * around queueShutdownInternal which does the real work. The method
     * handles any exceptions encountered during shutdown by exiting promptly.
     *
     * OOMEs get special attention and try to avoid any allocation. Note that
     * OOMEs come from multiple sources depending on the type of memory (thread
     * (/etc/security/limits.conf nproc setting), heap, perm space, etc.) that
     * was exhausted. The java APIs aren't sufficient to distinguish amongst
     * these possibilities and navigate a safe path to communicate the real
     * cause of the problem. So in an OOME situation, we do a quick exit with
     * no logging of this problem to prevent cascading failures. A
     * distinguished process exit code ({@link ProcessExitCode#RESTART_OOME})
     * is used so that the SNA can log the OOME instead. Note that this
     * handling is mainly useful in non-heap OOME situations, since the process
     * typically becomes unresponsive (due to frequent GCs), long before an
     * OOME is thrown. Monitoring the free memory explicitly is a better way to
     * deal with heap exhaustion instead of waiting for an OOME.
     *
     * @param requestException the runtime exception that was actually
     * encountered while processing a request.
     *
     * @param exitCode the exitCode to be used by the process.
     */
    public final void queueShutdown(Throwable requestException,
                                    ProcessExitCode exitCode) {

        if (!shutdownRequested.compareAndSet(false, true)) {
            return;
        }

        boolean immediateExit = true;
        if (requestException instanceof OutOfMemoryError) {
            exitCode = ProcessExitCode.RESTART_OOME;
        } else {
            /* Try for a clean process shutdown */
            try {
                queueShutdownInternal(requestException, exitCode);
                immediateExit = false;
            } catch (OutOfMemoryError ome) {
                exitCode = ProcessExitCode.RESTART_OOME;
            } catch (Throwable t) {
                if (logger != null) {
                    logger.log(Level.SEVERE, "Process exiting",
                               requestException);
                    logger.log(Level.SEVERE, "Error during shutdown", t);
                }
            }
        }

        if (immediateExit) {
            if (TestStatus.isActive()) {
                /*
                 * Throw  an exception in the test environment, don't exit the
                 * test.
                 */
                throw new IllegalStateException("exit", requestException);
            }
            /* Exit immediately */
            halt(requestException, exitCode);
        }
    }

    /**
     * Queues a shutdown in a process-specific way. To ensure correct
     * handling of exceptions, this method must only be invoked via
     * queueShutdown().
     *
     * @param requestException the runtime exception that was actually
     * encountered while processing a request.
     *
     * @param exitCode the exitCode to be used by the process
     */
    protected abstract void queueShutdownInternal(Throwable requestException,
                                                  ProcessExitCode exitCode);

    /**
     * Create a thread that waits for the specified number of milliseconds and
     * then halts the JVM with the specified exit code. If an exception occurs
     * while attempting to create and start the thread, then halt immediately.
     * Use this method before creating a thread that calls System.exit to
     * perform an orderly shutdown so that the JVM can be halted if the exit
     * fails to complete in the specified amount of time.
     */
    protected void haltAfterWait(long waitMs,
                                 Throwable exception,
                                 ProcessExitCode exitCode) {
        try {
            final Thread t = new Thread("KV_Exit_Monitor") {
                @Override
                public void run() {
                    try {
                        Thread.sleep(waitMs);
                    } catch (InterruptedException e) {
                        return;
                    }
                    halt(exception, exitCode);
                }
            };
            t.setDaemon(true);
            t.start();
        } catch (Throwable t) {
            halt(exception, exitCode);
        }
    }

    /** Halt the JVM. Using separate method to support testing. */
    void halt(Throwable exception, ProcessExitCode exitCode) {
        final ServiceStatus serviceStatus;
        switch (exitCode) {
        case RESTART:
        case RESTART_OOME:
            serviceStatus = ServiceStatus.ERROR_RESTARTING;
            break;
        case NO_RESTART:
            serviceStatus = ServiceStatus.ERROR_NO_RESTART;
            break;
        case INJECTED_FAULT_RESTART:
            serviceStatus = ServiceStatus.INJECTED_FAULT_RESTARTING;
            break;
        case INJECTED_FAULT_NO_RESTART:
            serviceStatus = ServiceStatus.INJECTED_FAULT_NO_RESTART;
            break;
        default:
            /*
             * That should cover everything, but better supply a default here
             * just in case
             */
            serviceStatus = ServiceStatus.ERROR_RESTARTING;
            break;
        }
        statusTracker.update(serviceStatus, "exception: " + exception);
        LoggerUtils.closeAllHandlers();
        System.err.flush();
        Runtime.getRuntime().halt(exitCode.getValue());
    }

    /*
     * The proliferation of differently named interfaces below is due to the
     * lack of overloading on Generic parameters in Java.
     */

    /**
     * The interface to be implemented by the operation whose process level
     * faults are to be handled.
     *
     * @param <R> the Result type associated with the Operation
     * @param <E> the Exception type associated with the execution of the
     * operation.
     */
    public interface Operation<R, E extends Exception> {
        R execute() throws E;
    }

    /**
     * A variant to simplify the handling of operations that do not throw
     * exceptions.
     *
     * @param <R> the Result type associated with the Operation
     */
    public interface SimpleOperation<R> extends Supplier<R> {
        R execute();
        @Override
        default R get() {
            return execute();
        }
    }

    /**
     * A variant for procedural operations that does not return values.
     *
     * @param <E> the Exception type associated with the execution of the
     * operation.
     */
    public interface Procedure<E extends Exception> {
        void execute() throws E;
    }

    /**
     * A variant for procedural operations that does not throw exceptions.
     */
    public interface SimpleProcedure extends Runnable {
        void execute();
        @Override
        default void run() {
            execute();
        }
    }
}
