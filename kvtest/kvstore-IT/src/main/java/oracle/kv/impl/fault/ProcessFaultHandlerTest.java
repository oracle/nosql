/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.fault;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.junit.Test;

/**
 * Tests for process fault handler methods.
 */
public class ProcessFaultHandlerTest extends FaultHandlerTestBase {

    @Override
    protected ProcessFaultHandler createPFH(ProcessExitCode exitCode) {
        return new PFH(logger, exitCode);
    }

    /**
     * Test handling of OOME in request itself.
     */
    @Test
    public void testOOMEInRequest() {

        /* Prevent test process from exiting */
        ProcessFaultHandler pfh =  createPFH(ProcessExitCode.RESTART);
        OutOfMemoryError error = new OutOfMemoryError("oome") ;
        try {
            pfh.execute(new ErrorSimpleOp(error));
            fail("expected exception");
        } catch (IllegalStateException ise) {
            /* The real process would have exited with a return code. */
            assertEquals("exit", ise.getMessage());
            assertEquals(error, ise.getCause());
        }
    }

    /**
     * Test handling of OOME encountered during process exits
     */
    @Test
    public void testOOMEInExceptionHandling() {

        /* Prevent test process from exiting */
        ProcessFaultHandler pfh =
            new OOMEPFH(logger, ProcessExitCode.RESTART);
        Error error = new UnknownError();
        try {
            pfh.execute(new ErrorSimpleOp(error));
            fail("expected exception");
        } catch (IllegalStateException ise) {
            /* The real process would have exited with the return code. */
            assertEquals("exit", ise.getMessage());
            assertEquals(error, ise.getCause());
        }
    }

    /**
     * Test that shutdowns only call exit once and that we call halt if the
     * exit hangs. [KVSTORE-405]
     */
    @Test
    public void testShutdown() throws Exception {
        final HaltOnExitHangPFH pfh =
            new HaltOnExitHangPFH(logger, ProcessExitCode.RESTART);
        final RuntimeException except1 = new RuntimeException();

        /* Inject two failures that should request shutdowns */
        try {
            pfh.execute(new RTESimpleOp(except1));
            fail("Expected exception");
        } catch (RuntimeException e) {
            assertEquals(except1, e);
        }
        final RuntimeException except2 = new RuntimeException();
        try {
            pfh.execute(new RTESimpleOp(except2));
            fail("Expected exception");
        } catch (RuntimeException e) {
            assertEquals(except2, e);
        }

        /* Shutdown hangs, so expect one call to halt */
        assertTrue(pfh.haltComplete.tryAcquire(4, SECONDS));
        assertEquals(1, pfh.haltCount.get());

        /* Only actually done one shutdown */
        assertEquals(1, pfh.shutdownCount.get());
    }

    @Override
    protected void checkSimpleRFE(RuntimeException re,
                                  final ProcessExitCode exitCode) {
        ProcessFaultHandler pfh =  createPFH(exitCode);
        try {
            pfh.execute(new RTESimpleOp(re));
            fail("expected exception");
        } catch (RuntimeException e) {
            /* Expected exception */
            assertEquals(re, e);
        }
        assertEquals(exitCode, ((TestExitCode)pfh).getTestExitCode());
    }

    @Override
    public void validateFault(RuntimeException expected,
                              RuntimeException rfe) {
        assertEquals(expected, rfe);
    }

    private static class PFH extends TestProcessFaultHandler
            implements TestExitCode {

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

    private static class OOMEPFH extends PFH  {

        public OOMEPFH(Logger logger, ProcessExitCode defaultExitCode) {
            super(logger, defaultExitCode);
        }

        @Override
        public void queueShutdownInternal(Throwable fault,
                                          ProcessExitCode exitCode) {
            super.queueShutdownInternal(fault, exitCode);
            throw new OutOfMemoryError("test");
        }
    }

    private static class HaltOnExitHangPFH extends PFH {
        final AtomicInteger shutdownCount = new AtomicInteger();
        final AtomicInteger haltCount = new AtomicInteger();
        final Semaphore haltComplete = new Semaphore(0);

        HaltOnExitHangPFH(Logger logger, ProcessExitCode defaultExitCode) {
            super(logger, defaultExitCode);
        }

        @Override
        public void queueShutdownInternal(Throwable fault,
                                          ProcessExitCode exitCode) {
            super.queueShutdownInternal(fault, exitCode);
            shutdownCount.getAndIncrement();

            /*
             * This implementation does not actually do a shutdown, so that is
             * similar to one where the System.exit call hangs. The
             * haltAfterWait method should call halt.
             */
            haltAfterWait(1000, fault, exitCode);
        }
        @Override
        void halt(Throwable exception, ProcessExitCode exitCode) {
            haltCount.getAndIncrement();
            haltComplete.release();
        }
    }
}
