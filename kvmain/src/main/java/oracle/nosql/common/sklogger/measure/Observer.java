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

/**
 * Observes system states.
 *
 * <p>Implementation needs to consider the thread-safety and performance impact
 * of this method.
 */
public interface Observer<S> {

    /**
     * Observes a new state.
     */
    void observe(S newState);
}

