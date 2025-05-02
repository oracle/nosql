/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the number of milliseconds after which a test class should
 * timeout. If the annotation is not specified, test classes timeout by default
 * after {@value TestBase#DEFAULT_CLASS_TIMEOUT_MILLIS} milliseconds. <p>
 *
 * Add this annotation to tests that run long enough that they might reach the
 * default timeout under normal circumstances. Currently, with the default
 * timeout set to 1200 seconds (20 minutes), let's plan to increase the timeout
 * for tests that typically take more than 700 seconds (about 12 minutes) in
 * the standard Jenkins runs, setting the timeout to roughly double the typical
 * run time. <p>
 *
 * To make adjustments for running unit tests on a slow system, see {@link
 * TestBase#TEST_CLASS_TIMEOUT_MULTIPLIER}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TestClassTimeoutMillis {

    /**
     * Returns the test class timeout in milliseconds.
     */
    long value();
}
