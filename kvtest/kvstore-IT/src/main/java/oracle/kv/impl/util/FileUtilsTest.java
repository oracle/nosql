/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.util.FileUtils.computeSha256Hash;
import static oracle.kv.impl.util.FileUtils.getFormattedFileModTime;
import static oracle.kv.impl.util.FileUtils.readFileWithMaxSize;
import static oracle.kv.impl.util.FormatUtils.formatDateTimeMillis;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Test for oracle.kv.impl.util.FileUtils.
 */
public class FileUtilsTest extends TestBase {

    public final static String DEST_DIR = "testdestdir";

    /*
     * Verify that deleteDirectory works.
     */
    @Test
    public void testDeleteDirectory() throws IOException {
        File testDir = TestUtils.getTestDir();

        File topDir = new File(testDir, "topDir");
        topDir.mkdir();
        File subDir = new File(topDir, "subdir");
        subDir.mkdir();
        File topDirMember = new File(topDir, "topDirMember");
        topDirMember.createNewFile();
        File subDirMember = new File(subDir, "subDirmember");
        subDirMember.createNewFile();

        assertTrue(topDir.isDirectory() && topDirMember.isFile() &&
                   subDir.isDirectory() && subDirMember.isFile());

        /* Access failure produces false return value from deleteDirectory */
        topDirMember.setReadOnly();
        subDir.setReadOnly();
        subDirMember.setReadOnly();
        assertFalse(FileUtils.deleteDirectory(topDir));

        topDirMember.setReadable(true);
        subDir.setReadable(true);
        subDir.setWritable(true); /* allow planting of symlink, below */
        subDirMember.setReadable(true);

        /* Does not follow symbolic links */
        File linkTarget = new File(testDir, "linkTarget");
        linkTarget.mkdir();
        File linkTargetMember = new File(linkTarget, "linkTargetMember");
        linkTargetMember.createNewFile();
        File link = new File(subDir, "link");
        Files.createSymbolicLink(link.toPath(), linkTarget.toPath());
        assertTrue(link.exists() && Files.isSymbolicLink(link.toPath()));

        /* Recursive delete should succeed */
        assertTrue(FileUtils.deleteDirectory(topDir));

        assertFalse(topDir.exists() || topDirMember.exists() ||
                    subDir.exists() || subDirMember.exists() || link.exists());

        /* But should not have descended into the linked dir */
        assertTrue(linkTargetMember.exists());

        linkTarget.delete();
    }

    @Test
    public void testComputeSha256HashFile() throws IOException {
        final File testDir = TestUtils.getTestDir();
        final File file = new File(testDir, "testfile");
        tearDowns.add(() -> {
                file.setWritable(true);
                file.delete();
            });

        assertEquals(null,
                     computeSha256Hash(new File(testDir, "no-such-file")));
        assertEquals(null,
                     computeSha256Hash(new File("unknown-directory",
                                                "no-such-file")));

        Files.createFile(file.toPath());
        String hash =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assertEquals(hash, computeSha256Hash(file));

        Files.writeString(file.toPath(), "abc");
        hash =
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assertEquals(hash, computeSha256Hash(file));

        file.setReadable(false);
        checkException(() -> computeSha256Hash(file),
                       AccessDeniedException.class);
    }

    @Test
    public void testComputeSha256HashConsumer() throws IOException {

        {
            ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
            computeSha256Hash(bais, is -> assertEquals(-1, is.read()));
        }

        final ByteArrayInputStream bais =
            new ByteArrayInputStream(new byte[] { 1, 3, 5 });
        computeSha256Hash(bais, is -> {
                assertEquals(1, is.read());
                assertEquals(3, is.read());
                assertEquals(5, is.read());
                assertEquals(-1, is.read());
            });

        bais.close();
        computeSha256Hash(bais, is -> assertEquals(-1, is.read()));

        checkException(
            () -> computeSha256Hash(bais,
                                    is -> {
                                        throw new IOException("abc");
                                    }),
            IOException.class,
            "abc");

        checkException(
            () -> computeSha256Hash(bais,
                                    is -> {
                                        throw new Exception("def");
                                    }),
            Exception.class,
            "def");
    }

    @Test
    public void testGetFormattedFileModTime() throws IOException {
        final File testDir = TestUtils.getTestDir();
        final File file = new File(testDir, "testfile");
        tearDowns.add(() -> {
                file.setWritable(true);
                file.delete();
            });

        assertEquals(null,
                     getFormattedFileModTime(
                         new File(testDir, "no-such-file")));
        assertEquals(null,
                     getFormattedFileModTime(
                         new File("unknown-directory", "no-such-file")));

        Files.createFile(file.toPath());
        String time = formatDateTimeMillis(file.lastModified());
        assertEquals(time, getFormattedFileModTime(file));

        file.setLastModified(0);
        assertEquals(null, getFormattedFileModTime(file));

        Files.setLastModifiedTime(
            file.toPath(), FileTime.from(-10000, TimeUnit.MILLISECONDS));
        assertEquals(null, getFormattedFileModTime(file));

    }

    @Test
    public void testReadFileWithMaxSize() throws IOException {
        final File testDir = TestUtils.getTestDir();
        final File file = new File(testDir, "testfile");
        tearDowns.add(() -> {
                file.setWritable(true);
                file.delete();
            });

        final AtomicBoolean wasTruncated = new AtomicBoolean();
        checkException(() -> readFileWithMaxSize(
                           new File(testDir, "no-such-file"), 1, wasTruncated),
                       FileNotFoundException.class);
        assertFalse(wasTruncated.get());
        checkException(() -> readFileWithMaxSize(
                           new File("unknown-directory", "no-such-file"), 1,
                           wasTruncated),
                       FileNotFoundException.class);
        assertFalse(wasTruncated.get());

        Files.createFile(file.toPath());
        assertEquals("", readFileWithMaxSize(file, 1000, wasTruncated));
        assertFalse(wasTruncated.get());

        Files.writeString(file.toPath(), "abc");
        assertEquals("abc", readFileWithMaxSize(file, 1000, wasTruncated));
        assertFalse(wasTruncated.get());
        assertEquals("ab", readFileWithMaxSize(file, 2, wasTruncated));
        assertTrue(wasTruncated.get());
        wasTruncated.set(false);
        assertEquals("", readFileWithMaxSize(file, 0, wasTruncated));
        assertTrue(wasTruncated.get());
        wasTruncated.set(false);

        file.setReadable(false);
        checkException(() -> readFileWithMaxSize(file, 1000, wasTruncated),
                       FileNotFoundException.class);
        assertFalse(wasTruncated.get());
    }
}
