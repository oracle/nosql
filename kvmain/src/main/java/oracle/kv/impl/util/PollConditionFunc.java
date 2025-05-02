/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.util;

/**
 * Object represents a functional interface of polling condition.
 */
@FunctionalInterface
public interface PollConditionFunc {
    /**
     * Checks the polling condition, returning true if the required condition
     * has been reached.
     *
     * @return whether the condition has been reached
     */
    boolean condition();
}
