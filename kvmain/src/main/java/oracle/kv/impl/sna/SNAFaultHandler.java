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

package oracle.kv.impl.sna;

import java.util.logging.Logger;

import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.util.ServiceStatusTracker;

/**
 * Specializes the ProcessFaultHandler so that all thrown exceptions are
 * wrapped inside a SNAFaultException unless they originated with one of its
 * managed services.
 *
 */
public class SNAFaultHandler extends ProcessFaultHandler {

    public SNAFaultHandler(StorageNodeAgentImpl sna) {
        this(sna.getLogger(),
             sna.getStorageNodeAgent().getServiceStatusTracker());
    }

    public SNAFaultHandler(Logger logger,
                           ServiceStatusTracker serviceStatusTracker) {
        super(logger, null /* defaultExitCode */, serviceStatusTracker);
    }

    /**
     * This method should never be called.
     */
    @Override
    protected void queueShutdownInternal(Throwable fault,
                                         ProcessExitCode exitCode) {

        throw new UnsupportedOperationException
            ("Method not implemented: " + "queueShutdown", fault);
    }

    /**
     * Wrap it inside a SNAFaultException.
     */
    @Override
    protected RuntimeException getThrowException(RuntimeException fault) {
        if (fault instanceof ClientAccessException) {
            return ((ClientAccessException) fault).getCause();
        }

        /* If the fault originated at another service, don't wrap it. */
        return((fault instanceof InternalFaultException) ?
                fault : new SNAFaultException(fault));
    }
}
