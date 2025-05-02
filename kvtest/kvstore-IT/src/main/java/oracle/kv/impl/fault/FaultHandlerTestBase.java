/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.fault;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import oracle.kv.TestBase;
import oracle.kv.impl.fault.ProcessFaultHandler.Operation;
import oracle.kv.impl.fault.ProcessFaultHandler.Procedure;
import oracle.kv.impl.fault.ProcessFaultHandler.SimpleOperation;
import oracle.kv.impl.fault.ProcessFaultHandler.SimpleProcedure;

import org.junit.Test;

/**
 * Base class for process fault handler tests
 */
public abstract class FaultHandlerTestBase extends TestBase {

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /**
     * The following sequence of methods verifies that a runtime fault
     * exception is as expected.
     */

    protected abstract ProcessFaultHandler createPFH(ProcessExitCode exitCode);

    @Test
    public void testExecuteOp() {
        final ProcessExitCode exitCode = ProcessExitCode.RESTART;
        ProcessFaultHandler pfh =  createPFH(exitCode);
        try {
            assertEquals(Integer.MIN_VALUE,
                         pfh.execute(new Op(false)).intValue());
            assertNull(((TestExitCode)pfh).getTestExitCode());
        } catch (TestException e) {
            fail(e.getMessage());
        }

        pfh = createPFH(exitCode);
        try {
            pfh.execute(new Op(true));
            fail("expected exception");
        } catch (FaultHandlerTestBase.TestException e) {
            /* Expected exception */
            assertEquals(null, ((TestExitCode)pfh).getTestExitCode());
        }

        checkRFE(new IllegalStateException("test"),
                 ProcessExitCode.RESTART);

        checkRFE(new OperationFaultException("test"), null);

        checkRFE(new ProcessFaultException("test",
                                           new NullPointerException("test")),
                 ProcessExitCode.RESTART);

        checkRFE(new SystemFaultException("test",
                                          new NullPointerException("test")),
                 ProcessExitCode.NO_RESTART);
    }

    @Test
    public void testExecuteSimpleOperation() {
        final ProcessExitCode exitCode = ProcessExitCode.RESTART;
        ProcessFaultHandler pfh =  createPFH(exitCode);
        assertEquals(Integer.MIN_VALUE,
                     pfh.execute(new SimpleOp()).intValue());
        assertNull(((TestExitCode)pfh).getTestExitCode());

        checkSimpleRFE(new IllegalStateException("test"),
                       ProcessExitCode.RESTART);

        checkSimpleRFE(new OperationFaultException("test"), null);

        checkSimpleRFE(new ProcessFaultException("test",
                                           new NullPointerException("test")),
                                           ProcessExitCode.RESTART);

        checkSimpleRFE(new SystemFaultException("test",
                                          new NullPointerException("test")),
                                          ProcessExitCode.NO_RESTART);
    }

    @Test
    public void testExecuteProc() {
        final ProcessExitCode exitCode = ProcessExitCode.RESTART;
        ProcessFaultHandler pfh =  createPFH(exitCode);
        try {
            pfh.execute(new Proc(false));
            assertNull(((TestExitCode)pfh).getTestExitCode());
        } catch (FaultHandlerTestBase.TestException e) {
            fail(e.getMessage());
        }

        pfh =  createPFH(exitCode);
        try {
            pfh.execute(new Proc(true));
            fail("expected exception");
        } catch (FaultHandlerTestBase.TestException e) {
            /* Expected exception */
            assertEquals(null, ((TestExitCode)pfh).getTestExitCode());
        }

        checkProcRFE(new IllegalStateException("test"),
                     ProcessExitCode.RESTART);

        checkProcRFE(new OperationFaultException("test"), null);

        checkProcRFE(new ProcessFaultException("test",
                                           new NullPointerException("test")),
                     ProcessExitCode.RESTART);

        checkProcRFE(new SystemFaultException("test",
                                          new NullPointerException("test")),
                     ProcessExitCode.NO_RESTART);
    }

    @Test
    public void testExecuteSimpleProc() {
        final ProcessExitCode exitCode = ProcessExitCode.RESTART;
        ProcessFaultHandler pfh =  createPFH(exitCode);
        pfh.execute(new SimpleProc());
        assertNull(((TestExitCode)pfh).getTestExitCode());

        checkSimpleProcRFE(new IllegalStateException("test"),
                       ProcessExitCode.RESTART);

        checkSimpleProcRFE(new OperationFaultException("test"), null);

        checkSimpleProcRFE(new ProcessFaultException("test",
                                           new NullPointerException("test")),
                                           ProcessExitCode.RESTART);

        checkSimpleProcRFE(new SystemFaultException("test",
                                                    new NullPointerException("test")),
                                                    ProcessExitCode.NO_RESTART);
    }

    protected abstract void validateFault(RuntimeException expected,
                                          RuntimeException rfe);

    protected void checkSimpleRFE(RuntimeException re,
                                  final ProcessExitCode exitCode) {
        ProcessFaultHandler pfh =  createPFH(exitCode);
        try {
            pfh.execute(new RTESimpleOp(re));
            fail("expected exception");
        } catch (RuntimeException rfe) {
            validateFault(re, rfe);
        }
        assertEquals(exitCode, ((TestExitCode)pfh).getTestExitCode());
    }

    protected void checkRFE(RuntimeException re,
                            final ProcessExitCode exitCode) {
        ProcessFaultHandler pfh =  createPFH(exitCode);
        try {
            pfh.execute(new RTEOp(re));
            fail("expected exception");
        } catch (RuntimeException rfe) {
            validateFault(re, rfe);
        } catch (TestException e) {
            fail(e.getMessage());
        }
        assertEquals(exitCode,((TestExitCode)pfh).getTestExitCode());
    }

    protected void checkProcRFE(RuntimeException re,
                                final ProcessExitCode exitCode) {
        ProcessFaultHandler pfh =  createPFH(exitCode);
        try {
            pfh.execute(new RTEProc(re));
            fail("expected exception");
        } catch (RuntimeException rfe) {
            validateFault(re, rfe);
        } catch (TestException e) {
            fail(e.getMessage());
        }
        assertEquals(exitCode, ((TestExitCode)pfh).getTestExitCode());
    }

    protected void checkSimpleProcRFE(RuntimeException re,
                                      final ProcessExitCode exitCode) {
        ProcessFaultHandler pfh =  createPFH(exitCode);
        try {
            pfh.execute(new RTESimpleProc(re));
            fail("expected exception");
        } catch (RuntimeException rfe) {
            validateFault(re, rfe);
        }
        assertEquals(exitCode, ((TestExitCode)pfh).getTestExitCode());
    }

    @SuppressWarnings("serial")
    static protected class TestException extends Exception{}

    protected class Op implements Operation<Integer, TestException> {
        private final boolean throwException;

        Op(boolean throwException) {
            super();
            this.throwException = throwException;
        }

        @Override
        public Integer execute()
            throws FaultHandlerTestBase.TestException {
            if (throwException) {
                throw new FaultHandlerTestBase.TestException();
            }
            return Integer.MIN_VALUE;
        }
    }

    protected class Proc implements Procedure<TestException> {

        private final boolean throwException;

        Proc(boolean throwException) {
            super();
            this.throwException = throwException;
        }

        @Override
        public void execute() throws TestException {
            if (throwException) {
                throw new TestException();
            }
            return ;
        }
    }

    protected class SimpleProc implements SimpleProcedure {
        @Override
        public void execute() {
            return ;
        }
    }

    protected class RTESimpleProc implements SimpleProcedure {

        private final RuntimeException rte;

        public RTESimpleProc(RuntimeException rte) {
            this.rte = rte;
        }
        @Override
        public void execute() {
            throw rte;
        }
    }

    protected class SimpleOp implements SimpleOperation<Integer> {

        @Override
        public Integer execute() {
            return Integer.MIN_VALUE;
        }
    }

    protected class RTESimpleOp implements SimpleOperation<Integer> {

        private final RuntimeException rte;

        public RTESimpleOp(RuntimeException rte) {
            this.rte = rte;
        }
        @Override
        public Integer execute() {
            throw rte;
        }
    }

    protected class ErrorSimpleOp implements SimpleOperation<Integer> {

        private final Error e;

        public ErrorSimpleOp(Error e) {
            this.e = e;
        }

        @Override
        public Integer execute() {
            throw e;
        }
    }

    protected class RTEOp implements Operation<Integer, FaultHandlerTestBase.TestException> {

        private final RuntimeException rte;

        public RTEOp(RuntimeException rte) {
            this.rte = rte;
        }
        @Override
        public Integer execute() throws FaultHandlerTestBase.TestException {
            throw rte;
        }
    }

    protected class RTEProc implements Procedure<FaultHandlerTestBase.TestException> {

        private final RuntimeException rte;

        public RTEProc(RuntimeException rte) {
            this.rte = rte;
        }
        @Override
        public void execute() throws FaultHandlerTestBase.TestException {
            throw rte;
        }
    }

    /**
     * Interface implemented by test fault handlers
     */
    public interface TestExitCode {
        ProcessExitCode getTestExitCode();
    }
}
