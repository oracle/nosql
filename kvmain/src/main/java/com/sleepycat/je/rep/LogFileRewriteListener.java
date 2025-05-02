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

package com.sleepycat.je.rep;

import java.io.File;
import java.util.Set;

/**
 * @hidden
 * A notification callback interface to warn the user that JE is about to
 * modify previously written log files as part of sync-up rollback.
 *
 * @see RollbackException
 */
public interface LogFileRewriteListener {

    /**
     * @hidden
     * Notifies the user that JE is about to modify previously written log
     * files.
     *
     * @param files the log files that will be modified.
     */
    public void rewriteLogFiles(Set<File> files);
}
