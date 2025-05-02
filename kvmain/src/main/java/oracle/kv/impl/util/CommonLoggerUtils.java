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

package oracle.kv.impl.util;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.LogManager;

/**
 * Logger utilities common to both client and server side.
 */
public class CommonLoggerUtils {

    /**
     * Get the value of a specified Logger property.
     */
    public static String getLoggerProperty(String property) {
        LogManager mgr = LogManager.getLogManager();
        return mgr.getProperty(property);
    }

    /**
     * Utility method to return a String version of a stack trace
     */
    public static String getStackTrace(Throwable t) {
        if (t == null) {
            return "";
        }

        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        stackTrace = stackTrace.replaceAll("&lt", "<");
        stackTrace = stackTrace.replaceAll("&gt", ">");

        return stackTrace;
    }

    /**
     * Return the stack trace for the specified thread as a string.
     *
     * @param thread the thread whose stack trace should be returned
     * @return the stack trace string
     */
    public static String getStackTrace(Thread thread) {
        final StringBuilder sb = new StringBuilder();
        getStackTrace(thread, sb);
        return sb.toString();
    }

    /**
     * Append the stack trace for the specified thread to the specified
     * StringBuilder.
     *
     * @param thread the thread whose stack should be passed to the string
     * builder
     * @param sb the string builder
     */
    public static void getStackTrace(Thread thread, StringBuilder sb) {
        boolean first = true;
        for (final StackTraceElement element : thread.getStackTrace()) {
            if (!first) {
                sb.append("\n\t");
            } else {
                first = false;
            }
            sb.append(element.toString());
        }
    }

    /**
     * Modify the stack trace of the specified exception to append the current
     * call stack.  Call this method to include information about the current
     * stack when rethrowing an exception that was originally thrown and caught
     * in another thread.  Note that this method will have no effect on
     * exceptions that are not writable.
     *
     * @param exception the exception
     */
    public static void appendCurrentStack(Throwable exception) {
        checkNull("exception", exception);
        final StackTraceElement[] existing = exception.getStackTrace();
        final StackTraceElement[] current = new Throwable().getStackTrace();
        final StackTraceElement[] updated =
            new StackTraceElement[existing.length + current.length];
        System.arraycopy(existing, 0, updated, 0, existing.length);
        System.arraycopy(current, 0, updated, existing.length, current.length);
        exception.setStackTrace(updated);
    }

    /**
     * Return a string that includes the fully qualified class name and message
     * of the specified exception. Use this method to include information about
     * an exception in the message of another exception, for example to add
     * information about a cause to the message of the enclosing exception. It
     * is better to use this method than use the message because some
     * exceptions have empty or minimal messages and expect the exception class
     * to identify the problem. It is also important to use this method rather
     * than concatenating the exception directly into the message because some
     * exception toString methods (for example FaultException) include stack
     * traces. Stack traces should not be included in exception messages
     * because that can expose them to non-direct driver users, which is a
     * potential security risk.
     *
     * @param exception the exception
     * @return a string describing the exception
     */
    public static String exceptionString(Throwable exception) {
        return exception.getClass().getName() + ": " + exception.getMessage();
    }
}
