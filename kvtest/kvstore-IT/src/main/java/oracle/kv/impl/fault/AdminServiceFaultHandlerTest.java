/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.fault;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminService;
import oracle.kv.impl.admin.AdminServiceFaultHandler;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.util.ErrorMessage;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;

public class AdminServiceFaultHandlerTest extends FaultHandlerTestBase {

    @Override
    protected ProcessFaultHandler createPFH(ProcessExitCode exitCode) {
        final AdminService adminService = new AdminService(false);
        return new ASFH(logger, adminService);
    }

    @Override
    protected void validateFault(RuntimeException expected,
                                 RuntimeException rfe) {
        /*
         * Compare the exception message only,  since the call stack is
         * different by caller
         */
        try {
            throw expected;
        } catch (InternalFaultException ie) {
            assertEquals(ie.getMessage(), rfe.getMessage());
        } catch (ClientAccessException cae) {
            assertEquals(cae.getCause().getMessage(), rfe.getMessage());
        } catch (CommandFaultException cfe) {
            assertEquals(AdminFaultException.wrapCommandFault(cfe).
                             getMessage(),
                         rfe.getMessage());
        } catch (RuntimeException re) {
            assertEquals(new AdminFaultException(re).getMessage(),
                         rfe.getMessage());
        }
    }

    @Test
    public void testCommandFaultWrapper() {

        final CommandFaultException[] cmdFaults = new CommandFaultException[] {
            wrapAsCommandFault(new IllegalStateException("test")),
            wrapAsCommandFault(new OperationFaultException("test")),
            wrapAsCommandFault(
                new ProcessFaultException("test",
                                          new NullPointerException("test"))),
            wrapAsCommandFault(
                new SystemFaultException("test",
                                         new NullPointerException("test"))),
            new CommandFaultException("test", ErrorMessage.NOSQL_5500, null)
        };

        final ProcessExitCode[] exitCodes = new ProcessExitCode[] {
            ProcessExitCode.RESTART,
            null,
            ProcessExitCode.RESTART,
            ProcessExitCode.NO_RESTART,
            null
        };

        for (int i = 0; i < cmdFaults.length; i++) {
            checkSimpleRFE(cmdFaults[i], exitCodes[i]);
            checkRFE(cmdFaults[i], exitCodes[i]);
            checkProcRFE(cmdFaults[i], exitCodes[i]);
            checkSimpleProcRFE(cmdFaults[i], exitCodes[i]);
        }
    }

    @Test
    public void testSpecialExceptionErrorCode() {
        RuntimeException fault = new NonfatalAssertionException("test");
        testSpecialException(fault,
                             new CommandFails(fault.getMessage(),
                                              ErrorMessage.NOSQL_5500,
                                              CommandResult.NO_CLEANUP_JOBS));
        fault = new Admin.DBOperationFailedException("test");
        testSpecialException(fault,
                             new CommandFails(fault.getMessage(),
                                              ErrorMessage.NOSQL_5300,
                                              CommandResult.NO_CLEANUP_JOBS));
        fault = new TestDBException("test");
        testSpecialException(fault,
                             new CommandFails(fault.getMessage(),
                                              ErrorMessage.NOSQL_5300,
                                              CommandResult.NO_CLEANUP_JOBS));
    }

    private void testSpecialException(RuntimeException fault,
                                      CommandResult result) {
        ProcessFaultHandler pfh =  createPFH(null);
        try {
            pfh.execute(new RTESimpleOp(fault));
            fail("expected exception");
        } catch (AdminFaultException rfe) {
            assertEquals(rfe.getCommandResult().getErrorCode(),
                         result.getErrorCode());
            assertEquals(rfe.getCommandResult().getDescription(),
                         result.getDescription());
            assertArrayEquals(rfe.getCommandResult().getCleanupJobs(),
                    result.getCleanupJobs());
        }
    }

    private static CommandFaultException
        wrapAsCommandFault(RuntimeException re) {
        return new CommandFaultException("test", re, ErrorMessage.NOSQL_5500,
                                         null);
    }

    private static class ASFH extends AdminServiceFaultHandler
        implements TestExitCode  {

        private ProcessExitCode exitCode= null;

        public ASFH(Logger logger, AdminService adminService) {
            super(logger, adminService);
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

    private static class TestDBException extends DatabaseException {

        private static final long serialVersionUID = 1L;

        public TestDBException(String message) {
            super(message);
        }
    }
}
