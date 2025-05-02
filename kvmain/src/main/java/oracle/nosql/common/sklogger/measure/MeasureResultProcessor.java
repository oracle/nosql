/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.sklogger.measure;

import oracle.nosql.common.jss.JsonSerializable;

/**
 * Processes a measure result.
 */
public interface MeasureResultProcessor {

    /**
     * Prepare before processing a batch of results.
     */
    void prepare(long obtainTimeMillis);

    /**
     * Processes an obtained result.
     */
    void process(JsonSerializable result);

    /**
     * Processes an occurred error.
     */
    default void process(Throwable t) {
    }

    /**
     * Finish after processing a batch of results.
     */
    default void finish() {
    }
}
