/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package com.sleepycat.je.util;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;

import com.sleepycat.je.BackupArchiveLocation;
import com.sleepycat.je.BackupFSArchiveCopy;
import com.sleepycat.je.BackupFileCopy;
import com.sleepycat.je.BackupFileLocation;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.dbi.BackupManager;
import com.sleepycat.je.dbi.SnapshotManifest;
import com.sleepycat.je.dbi.SnapshotManifest.LogFileInfo;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.NotSerializable;

/**
 * @hidden For internal use: automatic backups
 *
 * CheckSnapshot checks the validity of an archived backup snapshot.
 *
 * <pre>
 * usage: java { com.sleepycat.je.util.CheckSnapshot |
 *               -jar je-&lt;version&gt;.jar CheckSnapshot }
 *         -m &lt;file&gt;    # the manifest of the snapshot to check
 *         -c &lt;file&gt;    # the copy class configuration
 *         -l &lt;file&gt;    # the location class configuration
 *         [-C &lt;class&gt;] # the copy class
 *         [-L &lt;class&gt;] # the location class
 *         [-V]         # print version
 * </pre>
 */
public class CheckSnapshot {

    /* TODO: Add this class to JarMain when we make it public */

    private static final String USAGE =
        "usage: " + CmdUtil.getJavaCommand(CheckSnapshot.class) + "\n" +
        "       -m <file>    # the manifest of the snapshot to check\n" +
        "       -c <file>    # the copy class configuration\n" +
        "       -l <file>    # the location class configuration\n" +
        "       [-C <class>] # the copy class\n" +
        "       [-L <class>] # the location class\n" +
        "       [-V]         # print version";

    private SnapshotManifest manifest;
    private BackupFileCopy backupCopy;
    private BackupArchiveLocation backupLocation;

    /**
     * Creates a CheckSnapshot object for checking the validity of an archived
     * backup snapshot.
     *
     * @param manifest the manifest of the snapshot to check
     * @param backupCopy used to check files in the backup archive
     * @param backupLocation used to identify the location of files in the
     * backup archive
     */
    public CheckSnapshot(final SnapshotManifest manifest,
                         final BackupFileCopy backupCopy,
                         final BackupArchiveLocation backupLocation) {
        this.manifest = requireNonNull(manifest);
        this.backupCopy = requireNonNull(backupCopy);
        this.backupLocation = requireNonNull(backupLocation);
    }

    private CheckSnapshot() { }

    /**
     * Checks the validity of an archived backup snapshot. Prints the results
     * of the check to standard output, and errors and usage to standard error.
     * Exits with 0 on success and a non-zero value if there are failures.
     *
     * @param argv the arguments
     * @see #USAGE
     */
    public static void main(final String[] argv) {
        try {
            mainInternal(System.out, argv);
        } catch (UsageException e) {
            System.err.println(e.getMessage());
        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Checks the validity of an archived backup snapshot, for testing. Prints
     * the results of the check to specified stream. Throws an exception if
     * there are failures or if a usage message should be printed.
     *
     * @param argv the arguments
     * @throws UsageException if a usage message should be printed
     * @throws RuntimeException if the check fails
     * @see #USAGE
     */
    static void mainInternal(final PrintStream out, final String... argv) {
        final CheckSnapshot checkSnapshot = new CheckSnapshot();
        checkSnapshot.parseArgs(argv);
        final SnapshotManifest manifest = checkSnapshot.manifest;
        out.println("Checking snapshot " + manifest.getSnapshot() +
                    " for node " + manifest.getNodeName());
        out.println("Snapshot is " +
                    (manifest.getIsComplete() ? "complete" : "not complete"));
        out.println(checkSnapshot.check());
        out.println("Check passed");
    }

    private void printUsage(String msg) {
        if (msg == null) {
            throw new UsageException(USAGE);
        }
        throw new RuntimeException(msg + "\n" + USAGE);
    }

    @SuppressWarnings("serial")
    static class UsageException extends RuntimeException
        implements NotSerializable {

        UsageException(final String msg) {
            super(msg);
        }
    }

    private void parseArgs(final String[] argv) {
        int argc = 0;
        int nArgs = argv.length;

        if (nArgs == 0) {
            printUsage(null);
        }

        Path manifestPath = null;
        Path copyConfig = null;
        Path locationConfig = null;
        String copyClass = null;
        String locationClass = null;

        while (argc < nArgs) {
            final String thisArg = argv[argc++];
            if (thisArg.equals("-m")) {
                if (argc < nArgs) {
                    manifestPath = Paths.get(argv[argc++]);
                } else {
                    printUsage("-m requires an argument");
                }
            } else if (thisArg.equals("-c")) {
                if (argc < nArgs) {
                    copyConfig = Paths.get(argv[argc++]);
                } else {
                    printUsage("-c requires an argument");
                }
            } else if (thisArg.equals("-l")) {
                if (argc < nArgs) {
                    locationConfig = Paths.get(argv[argc++]);
                } else {
                    printUsage("-l requires an argument");
                }
            } else if (thisArg.equals("-C")) {
                if (argc < nArgs) {
                    copyClass = argv[argc++];
                } else {
                    printUsage("-C requires an argument");
                }
            } else if (thisArg.equals("-L")) {
                if (argc < nArgs) {
                    locationClass = argv[argc++];
                } else {
                    printUsage("-L requires an argument");
                }
            } else if (thisArg.equals("-V")) {
                throw new UsageException(JEVersion.CURRENT_VERSION.toString());
            } else {
                printUsage("Unknown argument: " + thisArg);
            }
        }

        if (manifestPath == null) {
            printUsage("-m is a required argument");
        }
        if (copyConfig == null) {
            printUsage("-c is a required argument");
        }
        if (locationConfig == null) {
            printUsage("-l is a required argument");
        }
        try {
            manifest = readManifest(manifestPath);
        } catch (IOException e) {
            throw new RuntimeException(
                "Problem reading manifest file " + manifestPath + ": " + e);
        }
        try {
            backupCopy = (copyClass != null) ?
                BackupManager.getImplementationInstance(
                    BackupFileCopy.class, copyClass) :
                new BackupFSArchiveCopy();
        } catch (RuntimeException e) {
            throw new RuntimeException("Problem with copy class " +
                                       copyClass + ": " + e);
        }
        try {
            backupCopy.initialize(copyConfig.toFile());
        } catch (Exception e) {
            throw new RuntimeException(
                "Problem initializing copy class from " + copyConfig + ": " +
                e);
        }
        try {
            backupLocation = (locationClass != null) ?
                BackupManager.getImplementationInstance(
                    BackupArchiveLocation.class, locationClass) :
                new BackupFileLocation();
        } catch (RuntimeException e) {
            throw new RuntimeException("Problem with location class " +
                                       locationClass + ": " + e);
        }
        try {
            backupLocation.initialize(manifest.getNodeName(),
                                      locationConfig.toFile());
        } catch (Exception e) {
            throw new RuntimeException(
                "Problem initializing location class from " + locationConfig +
                ": " + e);
        }
    }

    /**
     * Checks the validity of the snapshot specified in the constructor.
     *
     * @return status information from the check
     * @throws RuntimeException if the check fails
     */
    public String check() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream out = new PrintStream(baos);
        boolean ok = true;
        if (!verifyFiles("snapshot", manifest.getSnapshotFiles(), out)) {
            ok = false;
        }
        if (!verifyFiles("erased", manifest.getErasedFiles(), out)) {
            ok = false;
        }
        final String msg;
        try {
            msg = baos.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unexpected exception: " + e);
        }
        if (!ok) {
            throw new RuntimeException(msg);
        }
        return msg;
    }

    /**
     * Returns the manifest stored in the specified file.
     *
     * @param manifestPath the path of the manifest file
     * @return the manifest
     * @throws IOException if there is an I/O error, including if the manifest
     * file is not found
     */
    public static SnapshotManifest readManifest(final Path manifestPath)
        throws IOException {

        return SnapshotManifest.deserialize(Files.readAllBytes(manifestPath));
    }

    private boolean verifyFiles(final String type,
                                final Map<String, LogFileInfo> files,
                                final PrintStream out) {
        int copyCount = 0;
        boolean ok = true;
        for (final Entry<String, LogFileInfo> entry : files.entrySet()) {
            final LogFileInfo info = entry.getValue();
            if (!info.getIsCopied()) {
                continue;
            }
            copyCount++;
            final String logFile = info.getSnapshot() + "/" + entry.getKey();
            final URL url = backupLocation.getArchiveLocation(logFile);
            final String archiveChecksum;
            try {
                archiveChecksum =
                    BackupManager.checksumToHex(backupCopy.checksum(url));
            } catch (InterruptedException|IOException e) {
                out.println("Error: Problem reading checksum for " + type +
                            " log file " + logFile +
                            ": " + e);
                ok = false;
                continue;
            }
            if (!info.getChecksum().equals(archiveChecksum)) {
                out.println("Error: Incorrect checksum for " + type +
                            " log file " + logFile +
                            ": expected " +  info.getChecksum()+
                            ", found " + archiveChecksum);
                ok = false;
            }
        }
        out.println("Copied " + type + " log files: " + copyCount);
        return ok;
    }
}
