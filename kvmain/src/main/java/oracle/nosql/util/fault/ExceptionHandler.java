/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util.fault;

/**
 * Base class handles exceptions of request and process domains. Every
 * exception is translated and mapped to a POJO {@link ErrorResponse} using
 * the response model defined in RFC7807. After response generates,
 * process-level faults is handled with process cleanup and exit, which is
 * implemented by subclass of this handler.
 */
public abstract class ExceptionHandler {

    /* Flag that if ErrorResponse includes debug info like stack trace */
    protected boolean includeDebugInfo = false;

    /**
     * This method translates the exception argument to an {@link ErrorResponse}
     * that contains error information defined with RFC7807. It's used to build
     * HTTP response by each service component.
     *
     * If the exception is an unexpected error or process-level fault, it
     * triggers process-level cleanup and exits the process via the
     * queueShutdown() method. Process cleanup and exit typically (this may not
     * always be feasible) follow the generation of a response, so that the
     * requesting application is aware of the reason for the request failure.
     * Such a forced process exit always results in a non-zero process exit
     * code. Subclasses can override getProcessExitCode() to return specific
     * exit codes which can be used by the process level handler to take
     * appropriate action, typically, a process restart.
     *
     * This method translates RequestFault and UnsupportedOperationException,
     * other exceptions are all translated to a {@link ErrorResponse} with
     * UNKNWON_INTERNAL_ERROR. If subclasses need to handle other exceptions,
     * override the method handleUnknownException.
     */
    public ErrorResponse handleException(Exception ex) {
        ProcessExitCode processExitCode = null;
        ErrorResponse errorResponse = null;
        try {
            if (ex instanceof RequestFault) {
                final RequestFault rf = (RequestFault)ex;

                /*
                 * RequestFault must have error defined, if not, return
                 * an error response with UNKNOWN_INTERNAL_ERROR, and set
                 * process exit code to trigger a process cleanup and exit.
                 */
                if (rf.getError() == null) {
                    processExitCode = ProcessExitCode.RESTART;
                    errorResponse = buildErrorResponse(ex);
                } else {
                    errorResponse = ErrorResponse.build(
                        rf.getError(), ex, includeDebugInfo);
                }
            } else if (ex instanceof UnsupportedOperationException) {
                errorResponse = ErrorResponse.build(
                    ErrorCode.UNSUPPORTED_OPERATION, ex, includeDebugInfo);
            } else {
                /*
                 * Other exceptions are considered as unexpected, if subclasses
                 * don't override this method to handle their cases.
                 */
                errorResponse = handleUnknownException(ex);
            }
            return errorResponse;
        } catch (Exception e) {
            /*
             * Unexpected exception, return an error response with
             * UNKNOWN_INTERNAL_ERROR and set process exit code to trigger
             * a process cleanup and exit.
             */
            processExitCode = ProcessExitCode.RESTART;
            return buildErrorResponse(e);
        } finally {
            if (processExitCode == null) {
                processExitCode = getProcessExitCode(ex);
            }
            if (processExitCode != null) {
                /*
                 * TODO:
                 * Coordinating so that the process exits only after the
                 * response is sent out, is tricky and is primarily useful for
                 * debugging at the client end.
                 */
                queueShutdown(ex, processExitCode);
            }
        }
    }

    /**
     * The method is reserved for subclasses to resolve their exceptions, which
     * are not RequestFault or UnsupportedOperationException. If not override,
     * all other exceptions are translated to a HTTP response with error
     * UNKNOWN_INTERNAL_ERROR.
     */
    protected ErrorResponse handleUnknownException(Exception ex) {
        return buildErrorResponse(ex);
    }

    /**
     * Returns the process exit code of exception, if not a process-level
     * fault return zero. If not override, it returns process exit code of
     * ProcessFault and 200 for other unexpected exception.
     */
    protected ProcessExitCode getProcessExitCode(Exception ex) {
        if (ex instanceof ProcessFault) {
            final ProcessFault pf = (ProcessFault)ex;
            return pf.getExitCode();
        } else if (ex instanceof RequestFault ||
                   ex instanceof UnsupportedOperationException) {
            return null;
        } else {
            return ProcessExitCode.RESTART;
        }
    }

    /**
     * Build an ErrorResponse with UNKNOWN_INTERNAL_ERROR error.
     */
    private ErrorResponse buildErrorResponse(Exception ex) {
        return ErrorResponse.build(
            ErrorCode.UNKNOWN_INTERNAL_ERROR, ex, includeDebugInfo);
    }

    /*
     * For testing.
     */
    protected void includeDebugInfo() {
        this.includeDebugInfo = true;
    }

    /**
     * Queues an asynchronous shutdown in a process-specific way. To ensure
     * the response is generated before the process is terminated, this method
     * must be invoked after the response generation.
     *
     * @param exception the exception that cause process exit
     */
    protected abstract void queueShutdown(Exception exception,
                                          ProcessExitCode exitCode);
}
