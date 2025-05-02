/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.util.TestUtils;

/**
 * A class that simulates and records CompressingFileHandler and
 * NonCompressingFileHandler file system operations to simplify testing.
 */
class CompressingFileHandlerOps {
    /*
     * Put '\l' in the pathname to simulate a substring that appears in a
     * Windows pathname that was causing trouble because was '\l' not a valid
     * regex escape.
     */
    static final String directory = TestUtils.getTestDir() + "\\l";
    static {
        new File(directory).mkdir();
    }
    private final Map<Path, Integer> files = new HashMap<>();
    private final List<String> operations = new ArrayList<>();

    /*
     * If the ...Fail values are non-zero, cause the operation fail when called
     * for the associated operation count
     */
    int newDirectoryStreamFail;
    private int newDirectoryStreamCount;
    int deleteFileFail;
    private int deleteFileCount;
    int renameFileFail;
    private int renameFileCount;
    int creationTimeFail;
    private int creationTimeCount;

    /**
     * Initialize for a new test, specifying pairs of file (String) and the
     * associated creation times (int) in milliseconds.
     */
    void init(Object... filesAndCreationTimes) {
        assertEquals(0, filesAndCreationTimes.length % 2);
        files.clear();
        operations.clear();
        for (int i = 0; i < filesAndCreationTimes.length; i += 2) {
            assertTrue("Value should be a String: " + filesAndCreationTimes[i],
                       filesAndCreationTimes[i] instanceof String);
            assertEquals(null,
                         files.put(Paths.get(directory + "/" +
                                             filesAndCreationTimes[i]),
                                   (Integer) filesAndCreationTimes[i+1]));
        }
        newDirectoryStreamFail = 0;
        newDirectoryStreamCount = 0;
        deleteFileFail = 0;
        deleteFileCount = 0;
        renameFileFail = 0;
        renameFileCount = 0;
        creationTimeFail = 0;
        creationTimeCount = 0;
    }

    /**
     * Check that the expected operations were performed.
     */
    void checkOps(String... expectedOps) {
        assertEquals(asList(expectedOps), operations);
    }

    /** Return the files specified in init. */
    DirectoryStream<Path> newDirectoryStream() throws IOException {
        if (++newDirectoryStreamCount == newDirectoryStreamFail) {
            throw new IOException(
                "Injected exception at count " + newDirectoryStreamCount);
        }
        return new DirectoryStream<Path>() {
            @Override
            public Iterator<Path> iterator() {
                return (files == null) ?
                    Arrays.<Path>asList().iterator() :
                    files.keySet().iterator();
            }
            @Override
            public void close() { }
        };
    }

    /** Return the time specified in init. */
    long getCreationTimeInternal(Path path) throws IOException {
        if (++creationTimeCount == creationTimeFail) {
            throw new IOException(
                "Injected exception at count creationTimeCount");
        }
        if (!files.containsKey(path)) {
            throw new FileNotFoundException("File not found: " + path);
        }
        return files.get(path).longValue();
    }

    /**
     * Check that the compressed file has the expected name and record the
     * operation as "gz <filename>".
     */
    void createCompressedFileInternal(Path path, Path compressed)
        throws IOException
    {
        final String pathString = path.toString();
        assertEquals(pathString.replace(".tmp", ".gz"),
                     compressed.toString());
        final boolean fail = !files.containsKey(path);
        operations.add("gz " + (fail ? "fail " : "") + path.getFileName());

        if (fail) {
            throw new FileNotFoundException("File not found: " + path);
        }
        files.put(compressed, files.get(path));
    }

    /** Record the operation as "rm <filename>". */
    void deleteFileInternal(Path path) throws IOException {
        final boolean fail = (++deleteFileCount == deleteFileFail);
        operations.add("rm " + (fail ? "fail " : "") + path.getFileName());
        if (fail) {
            throw new IOException(
                "Injected exception at count " + deleteFileCount);
        }
        files.remove(path);
    }

    /** Record the operation as "mv <filename> <filename>". */
    void renameFileInternal(Path source, Path target) throws IOException {
        final boolean failInject = (++renameFileCount == renameFileFail);
        final boolean failSource = !files.containsKey(source);
        final boolean failTarget = files.containsKey(target);
        final boolean fail = failInject || failSource || failTarget;
        operations.add("mv " + (fail ? "fail " : "") +
                       source.getFileName() + " " + target.getFileName());
        if (failInject) {
            throw new IOException(
                "Injected exception at count " + renameFileCount);
        }
        if (failSource) {
            throw new FileNotFoundException("File not found: " + source);
        }
        if (failTarget) {
            throw new IOException("File already exists: " + target);
        }
        files.put(target, files.get(source));
        files.remove(source);
    }

    /** Return a value to avoid an error */
    long getFileSize(Path path) throws IOException {
        if (!files.containsKey(path)) {
            throw new FileNotFoundException("File not found: " + path);
        }
        return 0;
    }
}
