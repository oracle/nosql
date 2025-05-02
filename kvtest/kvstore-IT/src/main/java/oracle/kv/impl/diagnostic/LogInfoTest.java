/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import oracle.kv.TestBase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests LogInfo.
 */
public class LogInfoTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    @Test
    public void testBasic() throws ParseException {
        LogInfo logInfo =
                new LogInfo("2014-04-12 11:11:11.111 UTC info content");
        assertEquals(logInfo.getTimestampString(),
                     "2014-04-12 11:11:11.111 UTC");

        SimpleDateFormat dateFormat =
                new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = dateFormat.parse("2014-04-12 11:11:11.111");
        assertEquals(logInfo.getTimestamp(), date);

        assertEquals(logInfo.toString(),
                     "2014-04-12 11:11:11.111 UTC info content");


        LogInfo logInfo1 =
                new LogInfo("2014-04-12 11:11:11.111 UTC info content");
        assertTrue(logInfo.equals(logInfo1));

        /*
         * Test class LogInfoComparator, the log item should be sorted by the
         * order of time stamp in ascend
         */
        LogInfo.LogInfoComparator comparator = new LogInfo.LogInfoComparator();
        assertEquals(comparator.compare(logInfo, logInfo1), 0);

        LogInfo logInfo2 =
                new LogInfo("2014-04-12 11:11:11.110 UTC info content");
        assertEquals(comparator.compare(logInfo, logInfo2), 1);

        LogInfo logInfo3 =
                new LogInfo("2014-04-12 11:11:11.112 UTC info content");
        assertEquals(comparator.compare(logInfo, logInfo3), -1);


    }
}
