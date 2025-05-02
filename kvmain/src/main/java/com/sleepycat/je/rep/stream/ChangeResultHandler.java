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
 * Interface that processes feeder filter change result. Like
 * {@link FeederFilterChange}, the client should implement this interface to
 * process the filter change result returned from feeder.
 */
public interface ChangeResultHandler {

    /**
     * Called when feeder returns a feeder change result.
     *
     * @param result the result of a filter change
     */
    void onResult(FeederFilterChangeResult result);
}

