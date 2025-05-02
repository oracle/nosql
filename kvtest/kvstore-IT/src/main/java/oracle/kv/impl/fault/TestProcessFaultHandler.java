/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.fault;

import java.util.logging.Logger;

import oracle.kv.impl.util.ServiceStatusTracker;

/**
 * A abstract process fault handler that supplies a dummy ServiceStatusTracker,
 * for testing.
 */
public abstract class TestProcessFaultHandler extends ProcessFaultHandler {
    protected TestProcessFaultHandler(Logger logger,
                                      ProcessExitCode exitCode)
    {
        super(logger, exitCode, new ServiceStatusTracker(logger));
    }
}
