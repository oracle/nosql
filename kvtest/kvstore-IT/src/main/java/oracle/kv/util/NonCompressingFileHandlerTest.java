/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

/** Tests for the NonCompressingFileHandler class */
public class NonCompressingFileHandlerTest
        extends BasicCompressingFileHandlerTestBase<NonCompressingFileHandler>
{
    @Test
    public void testCheckFiles() throws Exception {
        final TestNonCompressingFileHandler testHandler =
            new TestNonCompressingFileHandler("a_%g.log");
        handler = testHandler;

        /* No files */
        testHandler.init();
        testHandler.check(false);

        /* No matching files */
        testHandler.init("b_0.log", 2000,
                         "b_1.log", 1000,
                         "abc_0.log", 2000,
                         "abc_1.log", 1000);
        testHandler.check(false);

        /* No compressed files */
        testHandler.init("a_0.log", 1000);
        testHandler.check(false);

        /* Too many files, but no date format */
        testHandler.init("a_0.log", 7000,
                         "a_1.log", 6000,
                         "a_2.log", 5000,
                         "a_3.log", 4000,
                         "a_4.log", 3000,
                         "a_5.log", 2000);
        testHandler.check(false);

        /* One extra file */
        testHandler.init("a_0.log", 7000,
                         "a_1.log", 6000,
                         "a_2.log", 5000,
                         "a_3.log", 4000,
                         "a_19700101-000003.log.gz", 3000,
                         "a_19700101-000002.log.gz", 2000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check(true,
                          "rm a_19700101-000001.log.gz",
                          "rm a_19700101-000002.log.gz");

        /* Delete partially compressed files even if below count */
        testHandler.init("a_0.log", 2000,
                         "a_19700101-000001.log.tmp", 1000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check(true,
                          "rm a_19700101-000001.log.gz");

        /* Delete partially compressed files if above count */
        testHandler.init("a_0.log", 2000,
                         "a_1.log", 3000,
                         "a_2.log", 4000,
                         "a_3.log", 5000,
                         "a_19700101-000001.log.tmp", 1000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check(true,
                          "rm a_19700101-000001.log.gz");

        /*
         * Only delete duplicates, make sure they don't affect count. And
         * ignore unknown suffix.
         */
        testHandler.init("a_0.log", 4000,
                         "a_1.log", 5000,
                         "a_19700101-000003.log.tmp", 3000,
                         "a_19700101-000002.log.tmp", 2000,
                         "a_19700101-000002.log.gz", 2000,
                         "a_19700101-000002.log.unknown", 2000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check(true,
                          "rm a_19700101-000002.log.gz");

        /* Test unique suffixes */
        testHandler.init("a_0.log", 4000,
                         "a_1.log", 5000,
                         "a_19700101-000003-2.log.tmp", 3000,
                         "a_19700101-000003-1.log.tmp", 3000,
                         "a_19700101-000003.log.tmp", 3000,
                         "a_19700101-000002.log.gz", 2000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check(true,
                          "rm a_19700101-000001.log.gz",
                          "rm a_19700101-000002.log.gz");

        /* Delete compressed file, still extra non-rotated files after */
        testHandler.init("a_0.log", 7000,
                         "a_1.log", 6000,
                         "a_2.log", 5000,
                         "a_3.log", 4000,
                         "a_4.log", 3000,
                         "a_5.log", 2000,
                         "a_19700101-000001.log.gz", 1000);
        testHandler.check(true,
                          "rm a_19700101-000001.log.gz");

        /* Test exception when reading directory */
        testHandler.init("a_0.log", 2000,
                         "a_1.log", 1000);
        testHandler.ops.newDirectoryStreamFail = 1;
        testHandler.check(true, 1);
    }

    /* Other classes and methods */

    @Override
    protected NonCompressingFileHandler createFileHandler(
        String pattern, int limit, int count, boolean extraLogging)
        throws IOException
    {
        return new NonCompressingFileHandler(pattern, limit, count,
                                             extraLogging);
    }

    /**
     * A subclass of NonCompressingFileHandler used to test the checkFiles
     * method.
     */
    private static class TestNonCompressingFileHandler
            extends NonCompressingFileHandler {

        final CompressingFileHandlerOps ops = new CompressingFileHandlerOps();

        TestNonCompressingFileHandler(String pattern) throws IOException {
            super(CompressingFileHandlerOps.directory + "/" + pattern,
                  5_000_000, 5, true /* extraLogging */);
        }

        /**
         * Initialize for a new test, specifying files and associated creation
         * times in milliseconds.
         */
        void init(Object... filesAndCreationTimes) {
            ops.init(filesAndCreationTimes);
            resetUnexpectedCount();
            setChecksEnabled();
        }

        /**
         * Call checkFiles, check that the specified expected operations, and
         * no unexpected operations, are performed, and that checks are enabled
         * afterwards as expected.
         */
        void check(boolean checksEnabledAfter, String... expectedOps) {
            check(checksEnabledAfter, 0, expectedOps);
        }

        /**
         * Call checkFiles, check that specified number of unexpected
         * operations, and the specified expected operations, are performed,
         * and that checks are enabled afters as expected.
         */
        void check(boolean checksEnabledAfter,
                   int unexpectedCount,
                   String... expectedOps) {
            checkFiles();
            ops.checkOps(expectedOps);
            assertEquals("unexpected count",
                         unexpectedCount, getUnexpectedCount());
            assertEquals("checks enabled afterwards",
                         checksEnabledAfter, getChecksEnabled());
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

        @Override
        void deleteFileInternal(Path path) throws IOException {
            ops.deleteFileInternal(path);
        }

        @Override
        long getFileSize(Path path) throws IOException {
            return ops.getFileSize(path);
        }
    }
}
