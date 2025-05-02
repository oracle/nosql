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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.utilint.NotSerializable;

/**
 * SyncUpFailedException will be thrown out during feeder-replica syncup.
 * During syncup, feeder/replica may spend too much time reading its log
 * files backwards to locate a potential matchpoint and channel is idle
 * because of no read activity. If the channel is detected to be closed,
 * syncup will be terminated and will throw a SyncUpFailedException.
 */

public class SyncUpFailedException extends DatabaseException
    implements NotSerializable {
    public SyncUpFailedException(String message) {
        super(message);
    }
}