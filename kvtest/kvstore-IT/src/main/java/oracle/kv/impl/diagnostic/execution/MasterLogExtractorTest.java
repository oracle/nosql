/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic.execution;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import oracle.kv.TestBase;
import oracle.kv.impl.diagnostic.LogFileInfo.LogFileType;

import org.junit.Test;

/**
 * Tests MasterLogExtractor.
 */
public class MasterLogExtractorTest extends TestBase {

    private File directory;
    private File subDirectory;
    private File logDirectory;

    @Override
    public void setUp()
        throws Exception {
        /* Create directory and files for this test */
        directory = new File("MasterLogExtractorDir");
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
    }

    @Override
    public void tearDown()
        throws Exception {
        /* Delete temporary directory and files */

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
    public void testSingleAdmin() throws Exception {
        /* Generate log files and fill them with log item */
        File file1 = new File(logDirectory, "admin1_2.log");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file1));
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin1] Initializing " +
                    "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin1] JE: Master " +
                    "changed to 1");
        bw.newLine();
        bw.close();

        File file2 = new File(logDirectory, "admin1_1.log");
        bw = new BufferedWriter(new FileWriter(file2));
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin1] JE: " +
                    "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.189 UTC INFO [admin1] JE: Election " +
                    "finished. Elapsed time: 12043ms");
        bw.newLine();
        bw.close();

        File file3 = new File(logDirectory, "admin1_0.log");
        bw = new BufferedWriter(new FileWriter(file3));
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin1] JE: " +
                    "Master changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.997 UTC INFO [admin1] JE: Election " +
                    "finished. Elapsed time: 44158ms");
        bw.newLine();
        bw.close();

        /* Extract master log items of all log files */
        MasterLogExtractor extractor =
                new MasterLogExtractor(LogFileType.ADMIN, "xxx");
        extractor.execute(directory.getAbsolutePath());

        /* Check whether the master log section file exists or not */
        File file = new File("xxx_admin1_masterlog.tmp");
        assertTrue(file.exists());

        /* Check whether the content of the generated file is expected */
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        line = br.readLine();
        assertEquals(line, "2014-07-30 07:34:55.838 UTC INFO [admin1] " +
                        "Initializing Admin for store: mystore");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:34:56.332 UTC INFO [admin1] JE: " +
                        "Master changed to 1");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:48:35.996 UTC INFO [admin1] JE: " +
                        "Master changed to 1");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:48:35.997 UTC INFO [admin1] JE: " +
                        "Election finished. Elapsed time: 44158ms");

        line = br.readLine();
        assertTrue(line == null);

        br.close();

        /* Delete temporary files */
        file1.delete();
        file2.delete();
        file3.delete();
        file.delete();
    }

    @Test
    public void testMultipleAdmins() throws Exception {
        /* Generate log files for multiple admins and fill them with log item */
        File file1 = new File(logDirectory, "admin1_0.log");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file1));
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin1] Initializing " +
                        "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin1] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin1] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin1] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        File file2 = new File(logDirectory, "admin2_0.log");
        bw = new BufferedWriter(new FileWriter(file2));
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin2] Initializing " +
                "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin2] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin2] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin2] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        File file3 = new File(logDirectory, "admin3_0.log");
        bw = new BufferedWriter(new FileWriter(file3));
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin3] Initializing " +
                        "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin3] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin3] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin3] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        /* Extract master log items of all log files */
        MasterLogExtractor extractor =
                new MasterLogExtractor(LogFileType.ADMIN, "xxx");
        extractor.execute(directory.getAbsolutePath());

        /*
         * Two files are generated after extraction, then check whether the
         * two files exist and the content in them is expected or not
         */
        File file = new File("xxx_admin1_masterlog.tmp");
        assertTrue(file.exists());

        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        line = br.readLine();
        assertEquals(line, "2014-07-30 07:34:55.838 UTC INFO [admin1] " +
                        "Initializing Admin for store: mystore");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:34:56.332 UTC INFO [admin1] JE: " +
                        "Master changed to 1");

        line = br.readLine();
        assertEquals(line, "2014-07-30 07:48:35.996 UTC INFO [admin1] JE: " +
                        "Master changed to 1");

        line = br.readLine();
        assertTrue(line == null);

        br.close();
        file.delete();

        file = new File("xxx_admin3_masterlog.tmp");
        assertTrue(file.exists());

        br = new BufferedReader(new FileReader(file));
        line = br.readLine();
        assertEquals(line, "2014-07-30 07:42:57.188 UTC INFO [admin3] JE: " +
                        "Master changed to 3");

        line = br.readLine();
        assertTrue(line == null);

        br.close();
        file.delete();

        file = new File("xxx_admin2_masterlog.tmp");
        assertFalse(file.exists());

        /* Delete temporary files */
        file1.delete();
        file2.delete();
        file3.delete();
        file.delete();
    }
}
