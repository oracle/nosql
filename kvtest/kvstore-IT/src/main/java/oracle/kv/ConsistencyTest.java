/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * Simple tests of the Consistency class.
 */
public class ConsistencyTest extends TestBase {

    @SuppressWarnings("deprecation")
    @Test
    public void testToString() {
        testToString(Consistency.NONE_REQUIRED, "Consistency.NoneRequired");
        testToString(Consistency.ABSOLUTE, "Consistency.Absolute");
        testToString(Consistency.NONE_REQUIRED_NO_MASTER,
                     "Consistency.NoneRequiredNoMaster");

        final Random random = new Random();
        final UUID repGroupUuid = UUID.randomUUID();
        final long repGroupVlsn = Math.abs(random.nextLong());
        final long timeout = Math.abs(random.nextInt());
        testToString(new Consistency.Version(
                         new Version(repGroupUuid, repGroupVlsn),
                         timeout, TimeUnit.MILLISECONDS),
                     "Consistency.Version version=" +
                     "<Version repGroupUuid=%s" +
                     " vlsn=%d repNodeId=null lsn=0x0/0x0>",
                     repGroupUuid, repGroupVlsn);

        final long permissibleLag = Math.abs(random.nextInt());
        testToString(new Consistency.Time(
                         permissibleLag, TimeUnit.MILLISECONDS,
                         timeout, TimeUnit.MILLISECONDS),
                     "Consistency.Time permissibleLag=%d",
                     permissibleLag);
    }

    private static void testToString(Object object,
                                     String format,
                                     Object... formatArgs) {
        assertEquals(String.format(format, formatArgs), object.toString());
    }
}
