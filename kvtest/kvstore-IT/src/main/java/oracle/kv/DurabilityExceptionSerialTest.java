/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static java.util.Collections.singleton;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;

import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializeExceptionUtilTest;

import org.junit.Test;

/** Test serial version compatibility */
public class DurabilityExceptionSerialTest extends TestBase {

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        FaultException.testNoCurrentInMessage = false;
    }

    @Test
    public void testSerialVersion() throws Exception {

        /*
         * Don't include current time or version in the fault exception message
         * so we get reproducible results
         */
        FaultException.testNoCurrentInMessage = true;

        /* Use a fixed stack trace in exceptions for same reason */
        final StackTraceElement[] stack = {
            new StackTraceElement("cl", "meth", "file", 22)
        };
        final RuntimeException re = new RuntimeException();
        re.setStackTrace(stack);
        final DurabilityException de =
            new DurabilityException(re, ReplicaAckPolicy.ALL, 2,
                                    singleton("replica1"));
        de.setStackTrace(stack);

        serialVersionChecker(de,
                             SerialVersion.MINIMUM,
                             0x4a38a6399ac44ee0L)
            .equalsChecker(SerializeExceptionUtilTest::equalExceptions)
            .check();
    }
}
