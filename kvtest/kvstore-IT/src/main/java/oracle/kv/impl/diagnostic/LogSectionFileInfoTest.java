/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Tests LogSectionFileInfo.
 */
public class LogSectionFileInfoTest extends TestBase {

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
        File file = new File(File.separator + "outputdir1" + File.separator +
                             "file.tmp.log");
        List<String> list = new ArrayList<String>();
        list.add("2014-04-12 11:11:11.111 UTC");
        list.add("2014-04-12 11:11:12.111 UTC");
        list.add("2014-04-12 11:11:13.111 UTC");
        /* Test getter methods */
        LogSectionFileInfo sectionFileInfo = new LogSectionFileInfo(file, list);
        assertEquals(sectionFileInfo.getFileName(), "file.tmp.log");
        assertEquals(sectionFileInfo.getFilePath(), File.separator +
                     "outputdir1" + File.separator + "file.tmp.log");

        /* Test log item in log section file */
        LogInfo logInfo = new LogInfo("2014-04-12 11:11:11.111 UTC");
        assertTrue(sectionFileInfo.getFirst().equals(logInfo));
        assertTrue(sectionFileInfo.pop().equals(logInfo));
        /* Only a section in section log file, so there is no another section */
        assertFalse(sectionFileInfo.isEmpty());
    }
}
