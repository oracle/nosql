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

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * @hidden For internal use: automatic backups
 *
 * Interface used by Backup to determine archive locations for copying backup
 * files.
 *
 * <p>Unexpected exceptions thrown when backup calls methods on an
 * implementation of this interface will cause the backup facility to be
 * shutdown, but will leave the environment open. {@link InterruptedException}
 * and {@link IOException} are expected exceptions, while {@link
 * IllegalArgumentException}, other runtime exceptions, and errors are treated
 * as unexpected.
 */
public interface BackupArchiveLocation {

    /**
     * The initialize method should be invoked exactly once after the creation
     * of the object via its no arguments constructor.
     *
     * @param nodeName the node name of the environment associated with the
     * backup
     * @param configFile a pathname used to initialize this object. It could,
     * for example, be the directory under which all files should be archived,
     * or it could be the name of a properties file that contains values that
     * will affect the behavior of the implementation. The format and meaning
     * of the config file is private to the implementation.
     *
     * @throws InterruptedException if the initialize operation is interrupted
     * @throws IllegalArgumentException if the configuration was invalid
     * @throws IOException if some irrecoverable I/O issue was encountered
     * during initialization
     */
    void initialize(String nodeName, File configFile)
        throws InterruptedException, IOException;

    /**
     * Returns a URL that represents the location where a backup file should be
     * stored. The result should be unique for a given environment and
     * backup path.
     *
     * @param relativeArchivePath the path of the backup file
     * @return the archive URL location
     */
    URL getArchiveLocation(String relativeArchivePath);
}
