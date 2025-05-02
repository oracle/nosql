/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;

import java.util.logging.Logger;

import oracle.kv.impl.fault.FaultHandlerTestBase;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultHandler;

/**
 * Test the RN Admin fault handler
 */
public class RepNodeAdminFaultHandlerTest extends FaultHandlerTestBase {

    @Override
    public ProcessFaultHandler createPFH(ProcessExitCode exitCode) {
        return new PFH(logger, exitCode);
    }

    @Override
    public void validateFault(RuntimeException expected,
                       RuntimeException rfe) {
        assertEquals(expected.getClass().getName(),
                     ((RepNodeAdminFaultException)rfe).getFaultClassName());
    }

    private static class PFH extends
        RepNodeAdminFaultHandler implements TestExitCode  {

        private ProcessExitCode exitCode= null;

        public PFH(Logger logger, ProcessExitCode defaultExitCode) {
            super(logger, defaultExitCode);
        }

        @Override
        public void queueShutdownInternal(Throwable fault,
                                  @SuppressWarnings("hiding")
                                  ProcessExitCode exitCode) {

            this.exitCode = exitCode;
        }

        @Override
        public ProcessExitCode getTestExitCode() {
            return exitCode;
        }
    }
}
