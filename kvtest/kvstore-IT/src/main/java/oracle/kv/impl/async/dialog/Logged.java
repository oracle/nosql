/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog;

import static org.junit.Assert.fail;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;
import java.util.logging.Level;

import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A base class that can log info.
 *
 * Errors are always buffered in a queue so that it can be printed out later,
 * say, when an error occurs. Messages are buffered in the queue if enabled.
 * Messages and errors are always logged.
 */
public class Logged {

    /* Start time. */
    private static volatile long T0 = System.nanoTime();

    /* Logging queues */
    private final Queue<String> messages =
        new ConcurrentLinkedQueue<String>();
    private final Queue<ErrorAndTime> errors =
        new ConcurrentLinkedQueue<ErrorAndTime>();

    private final Logger logger;
    private final Level logLevel;
    private final boolean queueMesgs;

    public Logged(boolean queueMesgs) {
        this(Logged.class.getName(), Level.FINE, queueMesgs);
    }

    public Logged(String name, Level logLevel, boolean queueMesgs) {
        this.logger = Logger.getLogger(name);
        this.logLevel = logLevel;
        this.queueMesgs = queueMesgs;
    }

    /**
     * Resets T0.
     */
    public static void resetT0() {
        T0 = System.nanoTime();
    }

    /**
     * Returns the time in nanoseconds since start time T0.
     */
    public static long now() {
        return System.nanoTime() - T0;
    }

    protected void logMesg(String mesg) {
        if (queueMesgs || logger.isLoggable((logLevel))) {
            final long timeNs = now();
            final String toLog = String.format("now=%.2f ms, %s",
                                               timeNs / 1.0e6, mesg);
            if (queueMesgs) {
                messages.add(toLog);
            }
            if (logger.isLoggable(logLevel)) {
                logger.log(logLevel, "now={0} ms, mesg={1}",
                           new Object[] {timeNs / 1.0e6, mesg});
            }

        }
    }

    protected void logError(Throwable throwable) {
        long timeNs = now();
        errors.add(new ErrorAndTime(throwable, timeNs));
        if (logger.isLoggable(logLevel)) {
            logger.log(logLevel, "now={0} ms, error={1}",
                    new Object[] {
                        timeNs / 1.0e6,
                        LoggerUtils.getStackTrace(throwable)});
        }
    }

    public void checkExceptions(Class<?>... allowed) {
        for (ErrorAndTime entry : errors) {
            boolean isAllowed = false;
            for (Class<?> cls : allowed) {
                if (cls.isInstance(entry.error)) {
                    isAllowed = true;
                    break;
                }
            }
            if (!isAllowed) {
                fail("Exception not allowed: "
                        + LoggerUtils.getStackTrace(entry.error));
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("errors=").append("[\n");
        for (ErrorAndTime entry : errors) {
            sb.append(String.format("now=%.2f ms, %s",
                                    entry.timeNs / 1.0e6,
                                    LoggerUtils.getStackTrace(entry.error))).
                append("\n");
        }
        sb.append("]\n");
        sb.append("messages=").append("[\n");
        for (String mesg : messages) {
            sb.append(mesg).append("\n");
        }
        sb.append("]");
        return sb.toString();
    }

    private class ErrorAndTime {
        Throwable error;
        long timeNs;
        ErrorAndTime(Throwable error, long timeNs) {
            this.error = error;
            this.timeNs = timeNs;
        }
    }
}
