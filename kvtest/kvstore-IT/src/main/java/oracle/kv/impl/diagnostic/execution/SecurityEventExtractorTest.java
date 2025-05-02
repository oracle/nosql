/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic.execution;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Tests SecurityEventExtractor.
 */
public class SecurityEventExtractorTest extends TestBase {

    private File directory;
    private File subDirectory;
    private File logDirectory;
    private File logFile1;
    private File logFile2;
    private File logFile3;
    private File logFile4;

    @Override
    public void setUp() throws Exception {
        /* Create directory and files for this test */
        directory = new File("SecurityEventExtractorTestDir");
        if (!directory.exists()) {
            directory.mkdir();
        }

        subDirectory = new File(directory, "subDir");
        if (!subDirectory.exists()) {
            subDirectory.mkdir();
        }

        logDirectory = new File(subDirectory, "log");
        if (!logDirectory.exists()) {
            logDirectory.mkdir();
        }

        logFile1 = new File(logDirectory, "admin0_1.log");
        if (!logFile1.exists()) {
            logFile1.createNewFile();
        }
        /* Write security events for this log file */
        BufferedWriter bw = new BufferedWriter(new FileWriter(logFile1));
        bw.write("2014-07-30 07:35:48.737 UTC SEC_INFO [admin0] KVAuditInfo X");
        bw.newLine();
        bw.write("2014-07-30 07:35:49.737 UTC SEC_INFO [admin0] KVAuditInfo X");
        bw.newLine();
        bw.close();

        logFile2 = new File(logDirectory, "sn1_1.log");
        if (!logFile2.exists()) {
            logFile2.createNewFile();
        }
        /* Write security events for this log file */
        bw = new BufferedWriter(new FileWriter(logFile2));
        bw.write("2014-07-30 07:35:46.737 UTC SEC_INFO [sn1] KVAuditInfo X");
        bw.newLine();
        bw.write("2014-07-30 07:35:47.737 UTC SEC_INFO [sn1] KVAuditInfo X");
        bw.newLine();
        bw.close();

        logFile3 = new File(logDirectory, "rg1_rn0_1.log");
        if (!logFile3.exists()) {
            logFile3.createNewFile();
        }
        /* Write security events for this log file */
        bw = new BufferedWriter(new FileWriter(logFile3));
        bw.write("2014-07-30 07:35:44.737 UTC " +
                        "SEC_INFO [rg1_rn0] KVAuditInfo X");
        bw.newLine();
        bw.write("2014-07-30 07:35:45.737 UTC " +
                        "SEC_INFO [rg1_rn0] KVAuditInfo X");
        bw.newLine();
        bw.close();

        logFile4 = new File(logDirectory, "mystore_7.log");
        if (!logFile4.exists()) {
            logFile4.createNewFile();
        }
        /* Write security events for this log file */
        bw = new BufferedWriter(new FileWriter(logFile4));
        bw.write("2014-07-30 07:35:46.737 UTC SEC_INFO [sn1] KVAuditInfo X");
        bw.newLine();
        bw.write("2014-07-30 07:35:47.737 UTC SEC_INFO [sn1] KVAuditInfo X");
        bw.newLine();
        bw.close();

    }

    @Override
    public void tearDown() throws Exception {
        /* Delete temporary directory and files */
        if (logFile1.exists()) {
            logFile1.delete();
        }

        if (logFile2.exists()) {
            logFile2.delete();
        }

        if (logFile3.exists()) {
            logFile3.delete();
        }

        if (logFile4.exists()) {
            logFile4.delete();
        }

        if (logDirectory.exists()) {
            logDirectory.delete();
        }

        if (subDirectory.exists()) {
            subDirectory.delete();
        }

        if (directory.exists()) {
            directory.delete();
        }
    }

    @Test
    public void testBasic() throws Exception {
        /* Extract security events from log files */
        SecurityEventExtractor extractor = new SecurityEventExtractor("5000");
        extractor.execute(directory.getAbsolutePath());

        /* Check whether the security event file is generated or not */
        File file = new File("5000_securityevent.tmp");

        assertTrue(file.exists());

        /* Check the content of the security event file is expected */
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        line = br.readLine();
        assertEquals(line, "2014-07-30 07:35:44.737 UTC " +
                        "SEC_INFO [rg1_rn0] KVAuditInfo X");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:35:45.737 UTC " +
                        "SEC_INFO [rg1_rn0] KVAuditInfo X");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:35:46.737 UTC " +
                        "SEC_INFO [sn1] KVAuditInfo X");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:35:47.737 UTC " +
                        "SEC_INFO [sn1] KVAuditInfo X");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:35:48.737 UTC " +
                        "SEC_INFO [admin0] KVAuditInfo X");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:35:49.737 UTC " +
                        "SEC_INFO [admin0] KVAuditInfo X");

        line = br.readLine();
        assertTrue(line == null);

        br.close();

        /* Delete temporary file */
        file.delete();
    }
}
