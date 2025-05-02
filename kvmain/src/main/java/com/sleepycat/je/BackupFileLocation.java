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

package com.sleepycat.je;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;

/**
 * @hidden For internal use: automatic backups
 *
 * An implementation of {@link BackupArchiveLocation} that provides local file
 * locations for copying backup files. This class is suitable for use with the
 * {@link BackupFSArchiveCopy} class.
 */
public class BackupFileLocation implements BackupArchiveLocation {

    /* The name of the node associated with the backup. */
    private volatile String nodeName;

    /** The pathname of the base directory, ending in "/". */
    private volatile String baseDirectory;

    /**
     * Creates an instance of this class.
     */
    public BackupFileLocation() { }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation uses the configuration argument as pathname of
     * the base directory for all archive files.
     *
     * @param configFile the absolute pathname of the base directory
     * @throws IllegalArgumentException if the argument is not an absolute
     * pathname
     */
    @Override
    public synchronized void initialize(@SuppressWarnings("hiding")
                                        final String nodeName,
                                        final File configFile) {
        requireNonNull(nodeName, "nodeName arg must not be null");
        requireNonNull(configFile, "configFile arg must not be null");
        if (baseDirectory != null) {
            throw new IllegalStateException("Already initialized");
        }
        final String configFileString = configFile.getPath();
        if (!Paths.get(configFileString).isAbsolute()) {
            throw new IllegalArgumentException(
                "The configuration argument for BackupFileLocation must be" +
                " an absolute pathname: " + configFile);
        }
        this.nodeName = nodeName;
        baseDirectory = configFileString.endsWith("/") ?
            configFileString :
            configFileString + "/";
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation a {@code file:} URL rooted in the directory
     * specified by the argument passed to the constructor, followed by a
     * subdirectory for the node name, and then the relative archive path.
     */
    @Override
    public URL getArchiveLocation(final String relativeArchivePath) {
        requireNonNull(relativeArchivePath,
                       "relativeArchivePath arg must not be null");
        final String file = relativeArchivePath.startsWith("/") ?
            baseDirectory + nodeName + relativeArchivePath :
            baseDirectory + nodeName + "/" + relativeArchivePath;
        try {
            return new URL("file", null, file);
        } catch (MalformedURLException e) {
            throw new IllegalStateException(
                "Unexpected malformed file URL for: " + file, e);
        }
    }
}
