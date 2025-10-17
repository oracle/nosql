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

import com.sleepycat.je.utilint.NotSerializable;

import java.io.IOException;

/**
 * Thrown to indicate that an error happened in the reading of channel
 * at {@link com.sleepycat.je.rep.utilint.BinaryProtocol}.
 */
@SuppressWarnings("serial")
public class BinaryProtocolException extends IOException
        implements NotSerializable {

    public BinaryProtocolException(String message) {
        super(message);
    }
}
