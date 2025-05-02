/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;


import java.io.File;

import oracle.kv.TestBase;
import oracle.kv.impl.diagnostic.LogFileInfo.LogFileType;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests LogFileInfo.
 */
public class LogFileInfoTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    @Test
    public void testBasic() {
        /* Test getter methods */
        File file = new File("rg1-rn1_0.log");
        LogFileInfo logFileInfo = new LogFileInfo(file, LogFileType.RN);
        assertEquals(logFileInfo.getNodeID(), 1);
        assertEquals(logFileInfo.getFileSequenceID(), 0);
        assertEquals(logFileInfo.getNodeName(), "rg1-rn1");
        assertEquals(logFileInfo.getFilePath(), file.getAbsolutePath());

        file = new File("sn3_5.log");
        logFileInfo = new LogFileInfo(file, LogFileType.ALL);
        assertEquals(logFileInfo.getNodeID(), -1);
        assertEquals(logFileInfo.getFileSequenceID(), -1);
        assertEquals(logFileInfo.getNodeName(), "all");
        assertEquals(logFileInfo.getFilePath(), file.getAbsolutePath());

        file = new File("admin3_4.log");
        logFileInfo = new LogFileInfo(file, LogFileType.ADMIN);
        assertEquals(logFileInfo.getNodeID(), 3);
        assertEquals(logFileInfo.getFileSequenceID(), 4);
        assertEquals(logFileInfo.getNodeName(), "admin3");
        assertEquals(logFileInfo.getFilePath(), file.getAbsolutePath());

        file = new File("admin3_4.log");
        LogFileInfo logFileInfo1 = new LogFileInfo(file, LogFileType.ADMIN);

        file = new File("admin3_3.log");
        LogFileInfo logFileInfo2 = new LogFileInfo(file, LogFileType.ADMIN);

        file = new File("admin3_5.log");
        LogFileInfo logFileInfo3 = new LogFileInfo(file, LogFileType.ADMIN);

        LogFileInfo.LogFileInfoComparator comparator =
                new LogFileInfo.LogFileInfoComparator();

        /*
         * Test class LogFileInfoComparator, the log file should be sorted by
         * the order of file sequence ID in descend
         */
        assertEquals(comparator.compare(logFileInfo, logFileInfo1), 0);
        assertEquals(comparator.compare(logFileInfo, logFileInfo2), -1);
        assertEquals(comparator.compare(logFileInfo, logFileInfo3), 1);
    }
}
