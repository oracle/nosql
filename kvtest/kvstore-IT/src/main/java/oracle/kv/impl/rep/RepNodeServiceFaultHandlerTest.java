/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;
import java.util.Date;

import oracle.kv.FaultException;
import oracle.kv.impl.fault.FaultHandlerTestBase;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.ServiceFaultHandler;
import oracle.kv.impl.util.FormatUtils;

/**
 * Tests the RNService fault handler
 */
public class RepNodeServiceFaultHandlerTest extends FaultHandlerTestBase {

    /**
     * Should be false. Will cause a process exit if set to true. Only set to
     * true for custom process hand testing in a sandbox.
     */
    static private boolean customProcessExit = false;

    @Override
    public ProcessFaultHandler createPFH(ProcessExitCode exitCode) {
        return new PFH(logger, exitCode);
    }

    @Override
    public void validateFault(RuntimeException expected,
                       RuntimeException rfe) {
        assertEquals(expected.getClass().getName(),
                     ((FaultException)rfe).getFaultClassName());

        //testing fault time
        String faultTime = rfe.toString();
        faultTime = faultTime.substring(faultTime.indexOf("on [") + 4);
        faultTime = faultTime.substring(0, faultTime.indexOf("]")).
                                       replace(" UTC","");
        String currentTime =
            FormatUtils.formatDateTimeMillis(System.currentTimeMillis()).
            replace(" UTC","");
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try {
            Date fDate = df.parse(faultTime);
            Date sDate = df.parse(currentTime);
            long diffInMillies = Math.abs(sDate.getTime() - fDate.getTime());
            assertTrue(TimeUnit.MILLISECONDS
                    .toMinutes(diffInMillies) <= 2);
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception: " + e, e);
        }
    }

    private static class PFH extends
        ServiceFaultHandler implements TestExitCode  {

        private ProcessExitCode exitCode= null;

        public PFH(Logger logger, ProcessExitCode defaultExitCode) {
            super(logger, defaultExitCode);
        }

        @Override
        public void queueShutdownInternal(Throwable fault,
                                          @SuppressWarnings("hiding")
                                          ProcessExitCode exitCode) {
            if (customProcessExit) {
                /* Only for hand-process exit testing  */
                super.queueShutdownInternal(fault, exitCode);
            }
            this.exitCode = exitCode;
        }

        @Override
        public ProcessExitCode getTestExitCode() {
            return exitCode;
        }
    }
}
