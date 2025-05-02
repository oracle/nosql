/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.TestUtils.fastSerialize;
import static oracle.kv.util.TestUtils.checkAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;

import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializeExceptionUtil;
import oracle.kv.impl.util.SerializeExceptionUtilTest;

import org.junit.Test;

public class FaultExceptionTest extends TestBase {

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        FaultException.testNoCurrentInMessage = false;
    }

    /**
     * Test that FaultExceptions can be serialized even if they are created
     * with isRemote=false.
     */
    @Test
    public void testSerializeRemote() throws Exception {
        FaultException fe = new FaultException("main message", null, false);
        FaultException deserialized = fastSerialize(fe);
        String remoteStackTrace = deserialized.getRemoteStackTrace();
        assertTrue("Remote stack trace should contain" +
                   " 'testSerializeRemote': " + remoteStackTrace,
                   remoteStackTrace.contains("testSerializeRemote"));

        fe = new FaultException("main message", null, true);
        deserialized = fastSerialize(fe);
        remoteStackTrace = deserialized.getRemoteStackTrace();
        assertTrue("Remote stack trace should contain" +
                   " 'testSerializeRemote': " + remoteStackTrace,
                   remoteStackTrace.contains("testSerializeRemote"));

        fe = new FaultException(
            "main message", new RuntimeException("cause message"), false);
        deserialized = fastSerialize(fe);
        remoteStackTrace = deserialized.getRemoteStackTrace();
        assertTrue("Remote stack trace should contain" +
                   " 'testSerializeRemote': " + remoteStackTrace,
                   remoteStackTrace.contains("testSerializeRemote"));
        assertTrue("Remote stack trace should contain 'cause message': " +
                   remoteStackTrace,
                   remoteStackTrace.contains("cause message"));

        fe = new FaultException(
            "main message", new RuntimeException("cause message"), true);
        deserialized = fastSerialize(fe);
        remoteStackTrace = deserialized.getRemoteStackTrace();
        assertTrue("Remote stack trace should contain" +
                   " 'testSerializeRemote': " + remoteStackTrace,
                   remoteStackTrace.contains("testSerializeRemote"));
        assertTrue("Remote stack trace should contain 'cause message': " +
                   remoteStackTrace,
                   remoteStackTrace.contains("cause message"));
    }

    /**
     * Test that setting the stack trace changes the remote stack trace as
     * well.
     */
    @Test
    public void testSetStackTrace() {
        FaultException.testNoCurrentInMessage = true;
        FaultException fe = new FaultException("msg", null, true);
        fe.setStackTrace(
            new StackTraceElement[] {
                new StackTraceElement("a", "b", "c", 10)
            });
        assertEquals("oracle.kv.FaultException: msg\n" +
                     "Fault class name: oracle.kv.FaultException\n" +
                     "\tat a.b(c:10)\n",
                     fe.getRemoteStackTrace());

    }

    @Test
    public void testSerialVersion() throws Exception {

        /*
         * Don't include current time or version in the fault exception message
         * so we get reproducible results
         */
        FaultException.testNoCurrentInMessage = true;

        /* Use a fixed stack for the same reason */
        final StackTraceElement[] stack = {
            new StackTraceElement("cl1", "meth1", "file1", 42),
            new StackTraceElement("cl2", "meth2", "file2", 77)
        };

        final Exception exception = new Exception();
        exception.setStackTrace(stack);

        final FaultException faultNullCauseRemote =
            new FaultException("msg1", null /* cause */, true /* isRemote */);
        faultNullCauseRemote.setStackTrace(stack);
        faultNullCauseRemote.setResourceId(new RepNodeId(1, 2));

        final FaultException faultCauseRemote =
            new FaultException(exception.getMessage(), exception,
                               true /* isRemote */);
        faultCauseRemote.setStackTrace(stack);

        final FaultException faultNullCauseNotRemote =
            new FaultException("msg1", null /* cause */, false /* isRemote */);
        faultNullCauseNotRemote.setStackTrace(stack);

        final FaultException faultCauseNotRemote =
            new FaultException(exception.getMessage(),
                               exception, false /* isRemote */);
        faultCauseNotRemote.setStackTrace(stack);
        faultCauseNotRemote.setResourceId(new RepNodeId(3, 1));

        checkAll(
            Stream.of(
                serialVersionChecker(
                    faultNullCauseRemote,
                    SerialVersion.MINIMUM, 0x9fc513e9ee04ec2eL),
                serialVersionChecker(
                    faultCauseRemote,
                    SerialVersion.MINIMUM, 0x67cc0c7767c577fL),
                serialVersionChecker(
                    faultNullCauseNotRemote,
                    SerialVersion.MINIMUM, 0x21a787432aa8fb8bL),
                serialVersionChecker(
                    faultCauseNotRemote,
                    SerialVersion.MINIMUM, 0x8533beb3758d081eL))
            .map(c -> c.writer(SerializeExceptionUtil::writeException))
            .map(c -> c.reader(SerializeExceptionUtil::readException))
            .map(c -> c.equalsChecker(
                     SerializeExceptionUtilTest::equalExceptions)));
    }
}
