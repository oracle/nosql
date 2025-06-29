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

package com.sleepycat.je.rep.impl;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.rep.ReplicationMutableConfig;

/**
 * Implemented by observers of mutable rep config changes.
 */
public interface RepEnvConfigObserver {

    /**
     * Notifies the observer that one or more mutable rep properties have been
     * changed.
     */
    void repEnvConfigUpdate(RepConfigManager configMgr,
                            ReplicationMutableConfig newConfig)
        throws DatabaseException;
}
