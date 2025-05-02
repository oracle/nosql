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

package com.sleepycat.je.log;

import com.sleepycat.je.DatabaseException;

/**
 * FileHandleSource is a file source built on top of a cached file handle.
 */
class FileHandleSource extends FileSource {

    private FileHandle fileHandle;

    FileHandleSource(FileHandle fileHandle,
                     FileManager fileManager) {
        super(fileHandle.getFile(), fileManager,
              fileHandle.getFileNum(), fileHandle.getLogVersion());
        this.fileHandle = fileHandle;
    }

    /**
     * @see LogSource#release
     */
    @Override
    public void release()
        throws DatabaseException {

        fileHandle.release();
    }

    @Override
    public int getLogVersion() {
        return fileHandle.getLogVersion();
    }
}
