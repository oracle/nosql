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

package com.sleepycat.je.rep.stream;

/**
 * Interface that represents the filter change requested by client and would
 * be sent to feeder over the wire during streaming. It is to be implemented
 * by the client that uses the subscription api.
 */
public interface FeederFilterChange {

    /**
     * Returns the filter change request id
     *
     * @return the filter change request id
     */
    String getReqId();
}
