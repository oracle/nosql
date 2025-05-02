/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static java.util.Arrays.asList;
import static oracle.kv.util.CompressingFileHandler.DATE_PATTERN;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.BasicCompressingFileHandler.MatchInfo;
import oracle.kv.util.CompressingFileHandler.FileInfo;

import org.junit.Test;

/** Tests for the CompressingFileHandler class */
public class CompressingFileHandlerTest
        extends BasicCompressingFileHandlerTestBase<CompressingFileHandler> {

    @Test
    public void testConstructorComputedLimit() throws Exception {
        final File dir = TestUtils.getTestDir();
        handler = createFileHandler(
            dir + "/a_%g", 5_000_000, 20, true /* extraLogging */);
        assertEquals(25_641_025, handler.getComputedLimit());
        assertEquals(5_000_000, handler.getLimit());
        assertEquals(20, handler.getCount());
    }

    @Test
    public void testComputeLimit() {
        assertEquals(25_641_025,
                     CompressingFileHandler.computeLimit(5_000_000, 20));
        assertEquals(5_128_205,
                     CompressingFileHandler.computeLimit(1_000_000, 20));
        assertEquals(17_241_379,
                     CompressingFileHandler.computeLimit(5_000_000, 10));
        assertEquals(Integer.MAX_VALUE,
                     CompressingFileHandler.computeLimit(1_000_000_000, 10));
        assertEquals(Integer.MAX_VALUE,
                     CompressingFileHandler.computeLimit(
                         Integer.MAX_VALUE, 10));
    }

    @Test
    public void testGetRegex() {
        Pattern pattern =
            CompressingFileHandler.getRegex("/tmp/admin1_%g.log");

        Matcher matcher = pattern.matcher("/tmp/admin1_2.log");
        assertTrue(matcher.matches());
        assertEquals("2", matcher.group(1));

        matcher = pattern.matcher("/tmp/admin1_20220518-163652.log");
        assertTrue(matcher.matches());
        assertEquals("20220518-163652", matcher.group(1));

        matcher = pattern.matcher("/tmp/admin1_20220518-163652-4.log");
        assertTrue(matcher.matches());
        assertEquals("20220518-163652-4", matcher.group(1));

        matcher = pattern.matcher("/tmp/admin1_20220518-163652-123.log");
        assertTrue(matcher.matches());
        assertEquals("20220518-163652-123", matcher.group(1));
    }

    @Test
    public void testDatePattern() {
        Matcher matcher = DATE_PATTERN.matcher("19700001-000001");
        assertTrue(matcher.matches());
        assertEquals("19700001-000001", matcher.group(1));
        assertEquals(null, matcher.group(2));

        matcher = DATE_PATTERN.matcher("19700001-000001-1");
        assertTrue(matcher.matches());
        assertEquals("19700001-000001", matcher.group(1));
        assertEquals("1", matcher.group(2));
    }

    @Test
    public void testCompareMatchInfo() {
        Pattern pattern =
            CompressingFileHandler.getRegex("/tmp/a_%g.log");

        /* Lower version numbers are higher */
        testCompareMatchInfo(1, pattern, "/tmp/a_0.log", "/tmp/a_1.log");
        testCompareMatchInfo(-1, pattern, "/tmp/a_10.log", "/tmp/a_9.log");
        testCompareMatchInfo(0, pattern, "/tmp/a_21.log", "/tmp/a_21.log");

        /* Compare non-integers in reverse dictionary order */
        testCompareMatchInfo(1, pattern,
                             "/tmp/a_12345678901234567890.log",
                             "/tmp/a_222222222222222222.log");

        /* Number is higher than date */
        testCompareMatchInfo(1, pattern,
                             "/tmp/a_9.log",
                             "/tmp/a_20220519-133846.log");
        testCompareMatchInfo(-1, pattern,
                             "/tmp/a_20220519-133846.log",
                             "/tmp/a_1.log");

        /* Lower date values are older */
        testCompareMatchInfo(1, pattern,
                             "/tmp/a_20220519-133200.log",
                             "/tmp/a_20220519-133100.log");
        testCompareMatchInfo(0, pattern,
                             "/tmp/a_20220519-133100.log",
                             "/tmp/a_20220519-133100.log");
        testCompareMatchInfo(-1, pattern,
                             "/tmp/a_20220519-133000.log",
                             "/tmp/a_20220519-133100.log");

        /* Date is duplicate date is higher */
        testCompareMatchInfo(1, pattern,
                             "/tmp/a_20220519-133200-1.log",
                             "/tmp/a_20220519-133200.log");
        testCompareMatchInfo(-1, pattern,
                             "/tmp/a_20220519-133200.log",
                             "/tmp/a_20220519-133200-10.log");

        /* Higher duplicates are higher */
        testCompareMatchInfo(1, pattern,
                             "/tmp/a_20220519-133200-10.log",
                             "/tmp/a_20220519-133200-2.log");
        testCompareMatchInfo(0, pattern,
                             "/tmp/a_20220519-133200-6.log",
                             "/tmp/a_20220519-133200-6.log");
        testCompareMatchInfo(-1, pattern,
                             "/tmp/a_20220519-133200-1.log",
                             "/tmp/a_20220519-133200-7.log");

        /* Version number is higher than duplicate */
        testCompareMatchInfo(1, pattern,
                             "/tmp/a_3.log",
                             "/tmp/a_20220519-133200-1.log");
        testCompareMatchInfo(-1, pattern,
                             "/tmp/a_20220519-133200-2.log",
                             "/tmp/a_1.log");

        /* Duplicate is higher than no duplicate */
        testCompareMatchInfo(1, pattern,
                             "/tmp/a_20220519-133200-1.log",
                             "/tmp/a_20220519-133200.log");
        testCompareMatchInfo(-1, pattern,
                             "/tmp/a_20220519-133200.log",
                             "/tmp/a_20220519-133200-20.log");

        /* Compare when the unique value is too large to be a valid int */
        testCompareMatchInfo(-1, pattern,
                             "/tmp/a_20220519-133200-1234567890123.log",
                             "/tmp/a_20220519-133200-13.log");
    }

    private void testCompareMatchInfo(int expected,
                                      Pattern pattern,
                                      String path1,
                                      String path2) {
        Matcher matcher1 = pattern.matcher(path1);
        assertTrue(matcher1.matches());
        Matcher matcher2 = pattern.matcher(path2);
        assertTrue(matcher2.matches());
        assertEquals("Comparing " + path1 + " and " + path2,
                     Integer.signum(expected),
                     Integer.signum(
                         new MatchInfo(Paths.get(path1), matcher1).compareTo(
                             new MatchInfo(Paths.get(path2), matcher2))));
        assertEquals("Comparing " + path2 + " and " + path1,
                     -Integer.signum(expected),
                     Integer.signum(
                         new MatchInfo(Paths.get(path2), matcher2).compareTo(
                             new MatchInfo(Paths.get(path1), matcher1))));
    }

    @Test
    public void testMatchInfoToString() {
        Pattern pattern = CompressingFileHandler.getRegex("/tmp/a_%g.log");
        String path = "/tmp/a_1.log";
        Matcher matcher = pattern.matcher(path);
        assertEquals("MatchInfo[path=" + path + " matcher=" + matcher + "]",
                     new MatchInfo(Paths.get(path), matcher).toString());
    }

    @Test
    public void testCompareFileInfo() {
        Pattern pattern =
            CompressingFileHandler.getRegex("/tmp/a_%g.log");

        testCompareFileInfo(-1, pattern,
                            "/tmp/a_20220520-000001.log",
                            "/tmp/a_20220520-000002.log");
        testCompareFileInfo(0, pattern,
                            "/tmp/a_20220520-000001.log",
                            "/tmp/a_20220520-000001.log");
        testCompareFileInfo(1, pattern,
                            "/tmp/a_20220520-0000010.log",
                            "/tmp/a_20220520-000001.log");

        testCompareFileInfo(-1, pattern,
                            "/tmp/a_20220520-000001.log",
                            "/tmp/a_20220520-000001-1.log");
        testCompareFileInfo(1, pattern,
                            "/tmp/a_20220520-000001-19.log",
                            "/tmp/a_20220520-000001.log");

        testCompareFileInfo(-1, pattern,
                            "/tmp/a_20220520-000001-1.log",
                            "/tmp/a_20220520-000001-9.log");
        testCompareFileInfo(0, pattern,
                            "/tmp/a_20220520-000001-3.log",
                            "/tmp/a_20220520-000001-3.log");
        testCompareFileInfo(1, pattern,
                            "/tmp/a_20220520-000001-30.log",
                            "/tmp/a_20220520-000001-3.log");
    }

    private void testCompareFileInfo(int expected,
                                     Pattern pattern,
                                     String path1,
                                     String path2) {
        Matcher matcher1 = pattern.matcher(path1);
        assertTrue(matcher1.matches());
        String version1 = matcher1.group(1);
        Matcher dateMatcher1 = DATE_PATTERN.matcher(version1);
        assertTrue(dateMatcher1.matches());
        String creationTime1 = dateMatcher1.group(1);
        String unique1 = dateMatcher1.group(2);

        Matcher matcher2 = pattern.matcher(path2);
        assertTrue(matcher2.matches());
        String version2 = matcher2.group(1);
        Matcher dateMatcher2 = DATE_PATTERN.matcher(version2);
        assertTrue(dateMatcher2.matches());
        String creationTime2 = dateMatcher2.group(1);
        String unique2 = dateMatcher2.group(2);

        assertEquals(
            "Comparing " + path1 + " and " + path2,
            Integer.signum(expected),
            Integer.signum(
                new FileInfo(Paths.get(path1), version1, creationTime1,
                             unique1)
                .compareTo(
                    new FileInfo(Paths.get(path2), version2, creationTime2,
                                 unique2))));
        assertEquals(
            "Comparing " + path2 + " and " + path1,
            -Integer.signum(expected),
            Integer.signum(
                new FileInfo(Paths.get(path2), version2, creationTime2,
                             unique2)
                .compareTo(
                    new FileInfo(Paths.get(path1), version1, creationTime1,
                                 unique1))));
    }

    @Test
    public void testFileInfoToString() {
        assertEquals("FileInfo[path=foo_20220523-150000-1.log" +
                     " version=20220523-150000-1]",
                     new FileInfo(Paths.get("foo_20220523-150000-1.log"),
                                  "20220523-150000-1", "20220523-150000",
                                  "1")
                     .toString());
    }

    @Test
    public void testGetCreationTime() throws Exception {
        final TestCompressingFileHandler testHandler =
            new TestCompressingFileHandler("a_%g.log");
        handler = testHandler;

        Path path =
            Paths.get(CompressingFileHandlerOps.directory).resolve("a_1.log");
        testHandler.init("a_1.log", 1000);
        assertEquals(1000, testHandler.getCreationTime(path));

        testHandler.init("a_1.log", 1000);
        testHandler.ops.creationTimeFail = 1;
        long time = testHandler.getCreationTime(path);
        assertTrue("Found time: " + time,
                   Math.abs(time - System.currentTimeMillis()) < 10000);
    }

    @Test
    public void testCheckFiles() throws Exception {
        final TestCompressingFileHandler testHandler =
            new TestCompressingFileHandler("a_%g.log");
        handler = testHandler;

        /* No files */
        testHandler.init();
        testHandler.check();

        /* No matching files */
        testHandler.init("b_0.log", 2000,
                         "b_1.log", 1000,
                         "abc_0.log", 2000,
                         "abc_1.log", 1000);
        testHandler.check();

        /* No rotation */
        testHandler.init("a_0.log", 1000);
        testHandler.check();

        /* First rotation */
        testHandler.init("a_0.log", 2000,
                         "a_1.log", 1000);
        testHandler.check("mv a_1.log a_19700101-000001.log.tmp",
                          "gz a_19700101-000001.log.tmp",
                          "rm a_19700101-000001.log.tmp");

        /* First rotation, died before starting compression */
        testHandler.init("a_0.log", 2000,
                         "a_19700101-000001.log.tmp", 1000);
        testHandler.check("gz a_19700101-000001.log.tmp",
                          "rm a_19700101-000001.log.tmp");

        /* First rotation, died before finishing compression */
        testHandler.init("a_0.log", 2000,
                         "a_19700101-000001.log.tmp", 1000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check("rm a_19700101-000001.log.gz",
                          "gz a_19700101-000001.log.tmp",
                          "rm a_19700101-000001.log.tmp");

        /* First rotation, compression done */
        testHandler.init("a_0.log", 2000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check();

        /* Second rotation */
        testHandler.init("a_0.log", 3000,
                         "a_1.log", 2000,
                         "a_19700101-000001.log", 1000);
        testHandler.check("mv a_1.log a_19700101-000002.log.tmp",
                          "gz a_19700101-000002.log.tmp",
                          "rm a_19700101-000002.log.tmp");

        /* Second rotation, duplicate time */
        testHandler.init("a_0.log", 2000,
                         "a_1.log", 1000,
                         "a_19700101-000001.log", 1000);
        testHandler.check("mv a_1.log a_19700101-000001-1.log.tmp",
                          "gz a_19700101-000001-1.log.tmp",
                          "rm a_19700101-000001-1.log.tmp");

        /* Second rotation, duplicate time, died before starting compression */
        testHandler.init("a_0.log", 2000,
                         "a_19700101-000001-1.log.tmp", 1000,
                         "a_19700101-000001.log", 1000);
        testHandler.check("gz a_19700101-000001-1.log.tmp",
                          "rm a_19700101-000001-1.log.tmp");

        /*
         * Second rotation, duplicate time, died before finishing compression
         */
        testHandler.init("a_0.log", 2000,
                         "a_19700101-000001-1.log.tmp", 1000,
                         "a_19700101-000001-1.log.gz", 1000,
                         "a_19700101-000001.log", 1000);
        testHandler.check("rm a_19700101-000001-1.log.gz",
                          "gz a_19700101-000001-1.log.tmp",
                          "rm a_19700101-000001-1.log.tmp");

        /* Second rotation, duplicate time, compression completed */
        testHandler.init("a_0.log", 2000,
                         "a_19700101-000001-1.log.gz", 1000,
                         "a_19700101-000001.log", 1000);
        testHandler.check();

        /* Multiple duplicate times */
        testHandler.init("a_0.log", 2000,
                         "a_1.log", 1000,
                         "a_2.log", 1000,
                         "a_3.log", 1000);
        testHandler.check("mv a_3.log a_19700101-000001.log.tmp",
                          "mv a_2.log a_19700101-000001-1.log.tmp",
                          "mv a_1.log a_19700101-000001-2.log.tmp",
                          "gz a_19700101-000001.log.tmp",
                          "rm a_19700101-000001.log.tmp",
                          "gz a_19700101-000001-1.log.tmp",
                          "rm a_19700101-000001-1.log.tmp",
                          "gz a_19700101-000001-2.log.tmp",
                          "rm a_19700101-000001-2.log.tmp");

        /* Many duplicate times */
        testHandler.init("a_0.log", 2000,
                         "a_1.log", 1000,
                         "a_2.log", 1000,
                         "a_3.log", 1000,
                         "a_4.log", 1000,
                         "a_5.log", 1000,
                         "a_6.log", 1000,
                         "a_7.log", 1000,
                         "a_8.log", 1000,
                         "a_9.log", 1000,
                         "a_10.log", 1000,
                         "a_11.log", 1000);
        testHandler.check(0,
                          "mv a_11.log a_19700101-000001.log.tmp",
                          "mv a_10.log a_19700101-000001-1.log.tmp",
                          "mv a_9.log a_19700101-000001-2.log.tmp",
                          "mv a_8.log a_19700101-000001-3.log.tmp",
                          "mv a_7.log a_19700101-000001-4.log.tmp",
                          "mv a_6.log a_19700101-000001-5.log.tmp",
                          "mv a_5.log a_19700101-000001-6.log.tmp",
                          "mv a_4.log a_19700101-000001-7.log.tmp",
                          "mv a_3.log a_19700101-000001-8.log.tmp",
                          "mv a_2.log a_19700101-000001-9.log.tmp",
                          "mv a_1.log a_19700101-000001-10.log.tmp",
                          "gz a_19700101-000001.log.tmp",
                          "rm a_19700101-000001.log.tmp",
                          "gz a_19700101-000001-1.log.tmp",
                          "rm a_19700101-000001-1.log.tmp",
                          "gz a_19700101-000001-2.log.tmp",
                          "rm a_19700101-000001-2.log.tmp",
                          "gz a_19700101-000001-3.log.tmp",
                          "rm a_19700101-000001-3.log.tmp",
                          "gz a_19700101-000001-4.log.tmp",
                          "rm a_19700101-000001-4.log.tmp",
                          "gz a_19700101-000001-5.log.tmp",
                          "rm a_19700101-000001-5.log.tmp",
                          "gz a_19700101-000001-6.log.tmp",
                          "rm a_19700101-000001-6.log.tmp",
                          "gz a_19700101-000001-7.log.tmp",
                          "rm a_19700101-000001-7.log.tmp",
                          "gz a_19700101-000001-8.log.tmp",
                          "rm a_19700101-000001-8.log.tmp",
                          "gz a_19700101-000001-9.log.tmp",
                          "rm a_19700101-000001-9.log.tmp",
                          "gz a_19700101-000001-10.log.tmp",
                          "rm a_19700101-000001-10.log.tmp");

        /* Ignore hand decompressed files */
        testHandler.init("a_0.log", 2000,
                         "a_19700101-000001.log", 1000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check();

        /* Ignore if only hand decompressed files */
        testHandler.init("a_0.log", 3000,
                         "a_19700101-000002.log", 2000,
                         "a_19700101-000001.log", 1000);
        testHandler.check();

        /* Started after multiple rotated files */
        testHandler.init("a_0.log", 4000,
                         "a_1.log", 3000,
                         "a_2.log", 2000,
                         "a_3.log", 1000);
        testHandler.check("mv a_3.log a_19700101-000001.log.tmp",
                          "mv a_2.log a_19700101-000002.log.tmp",
                          "mv a_1.log a_19700101-000003.log.tmp",
                          "gz a_19700101-000001.log.tmp",
                          "rm a_19700101-000001.log.tmp",
                          "gz a_19700101-000002.log.tmp",
                          "rm a_19700101-000002.log.tmp",
                          "gz a_19700101-000003.log.tmp",
                          "rm a_19700101-000003.log.tmp");

        /* Don't delete files if at count */
        testHandler.init("a_0.log", 20000,
                         "a_1.log", 19000,
                         "a_19700101-000018.log.gz", 18000,
                         "a_19700101-000017.log.gz", 17000,
                         "a_19700101-000016.log.gz", 16000,
                         "a_19700101-000015.log.gz", 15000,
                         "a_19700101-000014.log.gz", 14000,
                         "a_19700101-000013.log.gz", 13000,
                         "a_19700101-000012.log.gz", 12000,
                         "a_19700101-000011.log.gz", 11000,
                         "a_19700101-000010.log.gz", 10000,
                         "a_19700101-000009.log.gz", 9000,
                         "a_19700101-000008.log.gz", 8000,
                         "a_19700101-000007.log.gz", 7000,
                         "a_19700101-000006.log.gz", 6000,
                         "a_19700101-000005.log.gz", 5000,
                         "a_19700101-000004.log.gz", 4000,
                         "a_19700101-000003.log.gz", 3000,
                         "a_19700101-000002.log.gz", 2000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check("mv a_1.log a_19700101-000019.log.tmp",
                          "gz a_19700101-000019.log.tmp",
                          "rm a_19700101-000019.log.tmp");

        /* Delete files beyond count */
        testHandler.init("a_0.log", 21000,
                         "a_1.log", 20000,
                         "a_19700101-000019.log.gz", 19000,
                         "a_19700101-000018.log.gz", 18000,
                         "a_19700101-000017.log.gz", 17000,
                         "a_19700101-000016.log.gz", 16000,
                         "a_19700101-000015.log.gz", 15000,
                         "a_19700101-000014.log.gz", 14000,
                         "a_19700101-000013.log.gz", 13000,
                         "a_19700101-000012.log.gz", 12000,
                         "a_19700101-000011.log.gz", 11000,
                         "a_19700101-000010.log.gz", 10000,
                         "a_19700101-000009.log.gz", 9000,
                         "a_19700101-000008.log.gz", 8000,
                         "a_19700101-000007.log.gz", 7000,
                         "a_19700101-000006.log.gz", 6000,
                         "a_19700101-000005.log.gz", 5000,
                         "a_19700101-000004.log.gz", 4000,
                         "a_19700101-000003.log.gz", 3000,
                         "a_19700101-000002.log.gz", 2000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check("mv a_1.log a_19700101-000020.log.tmp",
                          "rm a_19700101-000001.log.gz",
                          "gz a_19700101-000020.log.tmp",
                          "rm a_19700101-000020.log.tmp");

        /* Test exception when reading directory */
        testHandler.init("a_0.log", 2000,
                         "a_1.log", 1000);
        testHandler.ops.newDirectoryStreamFail = 1;
        testHandler.check(1);

        /* Test exception when renaming a file */
        testHandler.init("a_0.log", 2000,
                         "a_1.log", 1000);
        testHandler.ops.renameFileFail = 1;
        testHandler.check(1,
                          "mv fail a_1.log a_19700101-000001.log.tmp");

        /* Test exception when deleting file after compression */
        testHandler.init("a_0.log", 2000,
                         "a_1.log", 1000);
        testHandler.ops.deleteFileFail = 1;
        testHandler.check(1,
                          "mv a_1.log a_19700101-000001.log.tmp",
                          "gz a_19700101-000001.log.tmp",
                          "rm fail a_19700101-000001.log.tmp");

        /* Test exception when deleting excess file */
        testHandler.init("a_0.log", 21000,
                         "a_19700101-000020.log.gz", 20000,
                         "a_19700101-000019.log.gz", 19000,
                         "a_19700101-000018.log.gz", 18000,
                         "a_19700101-000017.log.gz", 17000,
                         "a_19700101-000016.log.gz", 16000,
                         "a_19700101-000015.log.gz", 15000,
                         "a_19700101-000014.log.gz", 14000,
                         "a_19700101-000013.log.gz", 13000,
                         "a_19700101-000012.log.gz", 12000,
                         "a_19700101-000011.log.gz", 11000,
                         "a_19700101-000010.log.gz", 10000,
                         "a_19700101-000009.log.gz", 9000,
                         "a_19700101-000008.log.gz", 8000,
                         "a_19700101-000007.log.gz", 7000,
                         "a_19700101-000006.log.gz", 6000,
                         "a_19700101-000005.log.gz", 5000,
                         "a_19700101-000004.log.gz", 4000,
                         "a_19700101-000003.log.gz", 3000,
                         "a_19700101-000002.log.gz", 2000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.ops.deleteFileFail = 1;
        testHandler.check(1,
                          "rm fail a_19700101-000001.log.gz");

        /* Delete temp file that is past the limit */
        testHandler.init("a_0.log", 21000,
                         "a_1.log", 20000,
                         "a_2.log", 19000,
                         "a_3.log", 18000,
                         "a_4.log", 17000,
                         "a_5.log", 16000,
                         "a_6.log", 15000,
                         "a_7.log", 14000,
                         "a_8.log", 13000,
                         "a_9.log", 12000,
                         "a_10.log", 11000,
                         "a_11.log", 10000,
                         "a_12.log", 9000,
                         "a_13.log", 8000,
                         "a_14.log", 7000,
                         "a_15.log", 6000,
                         "a_16.log", 5000,
                         "a_17.log", 4000,
                         "a_18.log", 3000,
                         "a_19.log", 2000,
                         "a_19700101-000001.log.tmp", 1000);
        testHandler.check("mv a_19.log a_19700101-000002.log.tmp",
                          "mv a_18.log a_19700101-000003.log.tmp",
                          "mv a_17.log a_19700101-000004.log.tmp",
                          "mv a_16.log a_19700101-000005.log.tmp",
                          "mv a_15.log a_19700101-000006.log.tmp",
                          "mv a_14.log a_19700101-000007.log.tmp",
                          "mv a_13.log a_19700101-000008.log.tmp",
                          "mv a_12.log a_19700101-000009.log.tmp",
                          "mv a_11.log a_19700101-000010.log.tmp",
                          "mv a_10.log a_19700101-000011.log.tmp",
                          "mv a_9.log a_19700101-000012.log.tmp",
                          "mv a_8.log a_19700101-000013.log.tmp",
                          "mv a_7.log a_19700101-000014.log.tmp",
                          "mv a_6.log a_19700101-000015.log.tmp",
                          "mv a_5.log a_19700101-000016.log.tmp",
                          "mv a_4.log a_19700101-000017.log.tmp",
                          "mv a_3.log a_19700101-000018.log.tmp",
                          "mv a_2.log a_19700101-000019.log.tmp",
                          "mv a_1.log a_19700101-000020.log.tmp",
                          "rm a_19700101-000001.log.tmp",
                          "gz a_19700101-000002.log.tmp",
                          "rm a_19700101-000002.log.tmp",
                          "gz a_19700101-000003.log.tmp",
                          "rm a_19700101-000003.log.tmp",
                          "gz a_19700101-000004.log.tmp",
                          "rm a_19700101-000004.log.tmp",
                          "gz a_19700101-000005.log.tmp",
                          "rm a_19700101-000005.log.tmp",
                          "gz a_19700101-000006.log.tmp",
                          "rm a_19700101-000006.log.tmp",
                          "gz a_19700101-000007.log.tmp",
                          "rm a_19700101-000007.log.tmp",
                          "gz a_19700101-000008.log.tmp",
                          "rm a_19700101-000008.log.tmp",
                          "gz a_19700101-000009.log.tmp",
                          "rm a_19700101-000009.log.tmp",
                          "gz a_19700101-000010.log.tmp",
                          "rm a_19700101-000010.log.tmp",
                          "gz a_19700101-000011.log.tmp",
                          "rm a_19700101-000011.log.tmp",
                          "gz a_19700101-000012.log.tmp",
                          "rm a_19700101-000012.log.tmp",
                          "gz a_19700101-000013.log.tmp",
                          "rm a_19700101-000013.log.tmp",
                          "gz a_19700101-000014.log.tmp",
                          "rm a_19700101-000014.log.tmp",
                          "gz a_19700101-000015.log.tmp",
                          "rm a_19700101-000015.log.tmp",
                          "gz a_19700101-000016.log.tmp",
                          "rm a_19700101-000016.log.tmp",
                          "gz a_19700101-000017.log.tmp",
                          "rm a_19700101-000017.log.tmp",
                          "gz a_19700101-000018.log.tmp",
                          "rm a_19700101-000018.log.tmp",
                          "gz a_19700101-000019.log.tmp",
                          "rm a_19700101-000019.log.tmp",
                          "gz a_19700101-000020.log.tmp",
                          "rm a_19700101-000020.log.tmp");
    }

    @Test
    public void testCreateCompressedFile() throws Exception {
        final Path dir =
            Files.createTempDirectory(TestUtils.getTestDir().toPath(), "dir");
        handler = new CompressingFileHandler(
            dir + "/a_%g.log", 1_000_000, 10, true /* extraLogging */);

        assertFalse(
            handler.createCompressedFile(
                Paths.get("/directory/not/found/file.log"),
                dir.resolve("file.log.gz")));
        assertFalse(Files.exists(dir.resolve("file.log.gz")));

        assertFalse(
            handler.createCompressedFile(
                dir.resolve("file-not-found.log"),
                dir.resolve("file-not-found.log.gz")));
        assertFalse(Files.exists(dir.resolve("file-not-found.log.gz")));

        Path original = dir.resolve("foo.log");
        Path compressed = Paths.get(original + ".gz");
        List<String> text = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            text.add("The quick brown fox jumped over the lazy dog: " + i);
        }
        Files.write(original, text);

        tearDowns.add(() -> resetPerms(dir));
        Files.setPosixFilePermissions(
            dir, PosixFilePermissions.fromString("---------"));
        assertFalse(handler.createCompressedFile(original, compressed));
        resetPerms(dir);
        assertFalse(Files.exists(compressed));

        tearDowns.add(() -> resetPerms(original));
        Files.setPosixFilePermissions(
            original, PosixFilePermissions.fromString("---------"));
        assertFalse(handler.createCompressedFile(original, compressed));
        assertFalse(Files.exists(compressed));
        resetPerms(original);

        assertTrue(handler.createCompressedFile(original, compressed));
        try (InputStream originalIn = Files.newInputStream(original);
             InputStream compressedIn = new GZIPInputStream(
                 Files.newInputStream(compressed))) {
            checkStreamsEqual(originalIn, compressedIn);
        }
        assertEquals(1, handler.getCompressedCount());
        final double avgCompression = handler.getAverageCompression();
        assertTrue("Average compression was " + avgCompression +
                   ", should be less than 0.25",
                   avgCompression < 0.25);

        tearDowns.add(() -> resetPerms(compressed));
        Files.setPosixFilePermissions(
            compressed, PosixFilePermissions.fromString("---------"));
        assertFalse(handler.createCompressedFile(original, compressed));
        resetPerms(compressed);

        handler.close();
        Files.delete(compressed);
        handler = new CompressingFileHandler(
            dir + "/a_%g.log", 1_000_000, 10, true /* extraLogging */) {
            @Override
            long getFileSize(Path path) throws IOException {
                throw new IOException("Injected exception");
            }
        };
        assertTrue(handler.createCompressedFile(original, compressed));
        assertEquals(1, handler.getUnexpectedCount());
    }

    @Test
    public void testCompressAll() throws Exception {
        final Path dir =
            Files.createTempDirectory(TestUtils.getTestDir().toPath(), "dir");
        handler = new CompressingFileHandler(
            dir + "/a_%g.log", 1000, 5, true /* extraLogging */);

        checkException(() -> handler.compressAll(
                           asList(dir.resolve("a_1.log.gz"))),
                       IllegalStateException.class,
                       "Attempt to compress file with wrong suffix");

        handler.compressAll(asList(dir.resolve("not-found.log.tmp")));

        /* Test compressing after close */
        handler.close();
        handler.compressAll(asList(dir.resolve("a_1.log.tmp")));
    }

    @Test
    public void testHandler() throws Exception {
        final Path dir =
            Files.createTempDirectory(TestUtils.getTestDir().toPath(), "dir");
        handler = new CompressingFileHandler(
            dir + "/a_%g.log", 1000, 5, true /* extraLogging */);
        Logger testLogger = Logger.getLogger("testlogger");
        testLogger.setUseParentHandlers(false);
        testLogger.setLevel(Level.INFO);
        handler.setLevel(Level.INFO);
        testLogger.addHandler(handler);

        final String lorem =
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed" +
            " do eiusmod tempor incididunt ut labore et dolore magna aliqua." +
            " Ut enim ad minim veniam, quis nostrud exercitation ullamco" +
            " laboris nisi ut aliquip ex ea commodo consequat. Duis aute" +
            " irure dolor in reprehenderit in voluptate velit esse cillum" +
            " dolore eu fugiat nulla pariatur.";


        for (int i = 0; i < 200; i++) {
            testLogger.info(i + ": " + lorem);
        }

        assertEquals(0, handler.getUnexpectedCount());
        assertTrue("Compressed count: " + handler.getCompressedCount(),
                   handler.getCompressedCount() > 4);
        assertTrue("Deleted count: " + handler.getDeletedCount(),
                   handler.getDeletedCount() > 1);
        assertTrue("Average compression: " + handler.getAverageCompression(),
                   handler.getAverageCompression() < 0.5);

        /*
         * Make the compressed log files read-only and then try again to test
         * unexpected exception handling
         */
        try (final DirectoryStream<Path> stream =
             Files.newDirectoryStream(dir, "*.gz")) {
            for (final Path p : stream) {
                Files.setPosixFilePermissions(
                    p, PosixFilePermissions.fromString("r--r--r--"));
            }
        }

        for (int i = 0; i < 100; i++) {
            testLogger.info(i + ": " + lorem);
        }

        assertEquals(0, handler.getUnexpectedCount());
        assertTrue("Compressed count: " + handler.getCompressedCount(),
                   handler.getCompressedCount() > 4);
        assertTrue("Deleted count: " + handler.getDeletedCount(),
                   handler.getDeletedCount() > 1);
        assertTrue("Average compression: " + handler.getAverageCompression(),
                   handler.getAverageCompression() < 0.5);
    }

    /* Other classes and methods */

    @Override
    protected CompressingFileHandler createFileHandler(
        String pattern, int limit, int count, boolean extraLogging)
        throws IOException
    {
        return new CompressingFileHandler(pattern, limit, count, extraLogging);
    }

    /**
     * A subclass of CompressingFileHandler used to test the checkFiles method.
     * Uses CompressingFileHandlerOps to simulate and record file system
     * operations.
     */
    private static class TestCompressingFileHandler
            extends CompressingFileHandler {

        final CompressingFileHandlerOps ops = new CompressingFileHandlerOps();

        TestCompressingFileHandler(String pattern) throws IOException {
            super(CompressingFileHandlerOps.directory + "/" + pattern,
                  5_000_000, 20, true /* extraLogging */);
        }

        /**
         * Initialize for a new test, specifying files and associated creation
         * times in milliseconds.
         */
        void init(Object... filesAndCreationTimes) {
            ops.init(filesAndCreationTimes);
            resetUnexpectedCount();
        }

        /**
         * Call checkFiles and check that the specified expected operations,
         * and no unexpected operations, are performed.
         */
        void check(String... expectedOps) {
            check(0, expectedOps);
        }

        /**
         * Call checkFiles and check that specified number of unexpected
         * operations, and the specified expected operations, are performed.
         */
        void check(int unexpectedCount, String... expectedOps) {
            checkFiles();
            ops.checkOps(expectedOps);
            assertEquals(unexpectedCount, getUnexpectedCount());
        }

        /** Return the files specified in init. */
        @Override
        DirectoryStream<Path> newDirectoryStream() throws IOException {

            /*
             * If ops is null, we're still in the constructor, so no directory
             * contents yet.
             */
            if (ops == null) {
                return new DirectoryStream<Path>() {
                    @Override
                    public Iterator<Path> iterator() {
                        return Arrays.<Path>asList().iterator();
                    }
                    @Override
                    public void close() { }
                };
            }
            return ops.newDirectoryStream();
        }

        @Override
        long getCreationTimeInternal(Path path) throws IOException {
            return ops.getCreationTimeInternal(path);
        }

        /** Perform compression immediately */
        @Override
        void scheduleCompression(Collection<Path> toCompress) {
            compressAll(toCompress);
        }

        @Override
        void createCompressedFileInternal(Path path, Path compressed)
            throws IOException
        {
            ops.createCompressedFileInternal(path, compressed);
        }

        @Override
        void deleteFileInternal(Path path) throws IOException {
            ops.deleteFileInternal(path);
        }

        @Override
        void renameFileInternal(Path source, Path target) throws IOException {
            ops.renameFileInternal(source, target);
        }

        @Override
        long getFileSize(Path path) throws IOException {
            return ops.getFileSize(path);
        }
    }

    /**
     * Reset the permissions on the file associated with the specified path so
     * that it is read/write/execute for the current user.
     */
    private static void resetPerms(Path path) {
        try {
            Files.setPosixFilePermissions(
                path, PosixFilePermissions.fromString("rwx------"));
        } catch (IOException e) {
        }
    }

    private static void checkStreamsEqual(InputStream x, InputStream y)
        throws IOException
    {
        final byte[] bufx = new byte[512];
        final byte[] bufy = new byte[512];

        while (true) {
            int countx = x.read(bufx);
            if (countx == -1) {
                if (y.read() != -1) {
                    throw new EOFException("EOF for first stream");
                }
                break;
            }
            int pos = 0;
            int count = countx;
            do {
                int county = y.read(bufy, pos, count);
                if (county == -1) {
                    throw new EOFException("EOF for second stream");
                }
                count -= county;
            } while (count > 0);
            for (int i = 0; i < bufx.length; i++) {
                assertSame(bufx[i], bufy[i]);
            }
        }
    }
}
