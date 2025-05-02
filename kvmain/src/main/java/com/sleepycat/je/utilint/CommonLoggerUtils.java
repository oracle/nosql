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

package com.sleepycat.je.utilint;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * A Logger utility that can be used on both the KV client and server sides.
 *
 * The KV store client jar should not contain JE environment references. Any
 * client and server side common code should use this utility class instead of
 * the {@link LoggerUtils} class.
 */
public class CommonLoggerUtils {

    /** Return the stack trace of the caller, for debugging. */
    public static String getStackTrace() {
        Exception e = new Exception();
        return getStackTrace(e);
    }

    /** Return a String version of a stack trace */
    public static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        assert debugStackTrace(t, sw);
        String stackTrace = sw.toString();
        stackTrace = stackTrace.replaceAll("&lt", "<");
        stackTrace = stackTrace.replaceAll("&gt", ">");

        return stackTrace;
    }

    /*
     * For security reasons, you cannot do a printStackTrace() in a
     * production environment, so only call this as part of an assert.
     */
    private static boolean debugStackTrace(Throwable t, StringWriter sw) {
        t.printStackTrace(new PrintWriter(sw));
        return true;
    }

    public static String getStackTraceForSevereLog(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        stackTrace = stackTrace.replaceAll("&lt", "<");
        stackTrace = stackTrace.replaceAll("&gt", ">");

        return stackTrace;
    }
}
