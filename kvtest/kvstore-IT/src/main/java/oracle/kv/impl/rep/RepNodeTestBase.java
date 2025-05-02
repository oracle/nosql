/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.rep;

import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The base class for RepNode unit tests. It supplies the utility methods used
 * by subclasses when setting up and tearing down the RepNode component.
 */
public class RepNodeTestBase extends UncaughtExceptionTestBase {

    protected final int timeoutMs = 5000;

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        /*
         * Release logging files explicitly, because these tests create
         * components below the RepNodeService. The file handler close is
         * in the RepNodeService shutdown, which is not being called.
         */
        LoggerUtils.closeAllHandlers();
    }
}
