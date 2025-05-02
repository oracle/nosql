/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.logging.Level;

/**
 * Base class for tests that need an {@link UncaughtExceptionHandler}.
 */
public abstract class UncaughtExceptionTestBase extends TestBase
        implements UncaughtExceptionHandler {

    protected volatile Throwable uncaughtException;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        uncaughtException = null;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (uncaughtException != null) {
            throw new IllegalStateException(
                "Uncaught exception: " + uncaughtException, uncaughtException);
        }
    }

    @Override
    public void uncaughtException(Thread thread, Throwable exception) {
        final String msg =
            "Uncaught exception in thread " + thread + ": " + exception;
        synchronized (System.err) {
            System.err.println(msg);
            exception.printStackTrace();
        }
        uncaughtException = exception;
        logger.log(Level.SEVERE, msg, exception);
    }
}
