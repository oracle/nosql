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

package oracle.kv.table;

import oracle.kv.impl.api.table.FieldValueImpl;

/**
 * IdentityDef represents an immutable metadata object used to represent
 * the properties of an identity column in a table.
 *
 * @since 4.6
 */
public interface SequenceDef {

    /**
     * Create a deep copy of this object.
     *
     * @return a new copy
     */
    SequenceDef clone();

    /**
     * Returns true if start value is explicitly set.
     */
    boolean isSetStartValue();

    /**
     * Get the start value of generator.
     */
    FieldValueImpl getStartValue();

    /**
     * Returns true if increment value is explicitly set.
     */
    boolean isSetIncrementValue();

    /**
     * Get the increment value of generator.
     */
    FieldValueImpl getIncrementValue();

    /**
     * Returns true if max value is explicitly set.
     */
    boolean isSetMaxValue();

    /**
     * Get the max value of generator.
     */
    FieldValueImpl getMaxValue();

    /**
     * Returns true if min value is explicitly set.
     */
    boolean isSetMinValue();

    /**
     * Get the min value of generator.
     */
    FieldValueImpl getMinValue();

    /**
     * Returns true if cache is explicitly set.
     */
    boolean isSetCacheValue();

    /**
     * Get the cache value of generator.
     */
    FieldValueImpl getCacheValue();

    /**
     * Returns true if cycle is explicitly set.
     */
    boolean isSetCycle();

    /**
     * Get the configuration of whether to generate sequence on null.
     */
    boolean getCycle();
}
