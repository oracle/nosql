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

package com.sleepycat.je.rep.impl.networkRestore;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.utilint.NotSerializable;


/**
 * FileTransferException exception will be thrown out when GetFileTask thread
 * or LogFileFeeder thread is interrupted by other kinds of exceptions.
 */
@SuppressWarnings("serial")
public class FileTransferException extends DatabaseException
    implements NotSerializable {

    public FileTransferException(String message, Throwable t) {
        super(message, t);
    }
}
