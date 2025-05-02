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

/**
 * IntegerValue extends {@link FieldValue} to represent an integer.
 *
 * @since 3.0
 */
public interface IntegerValue extends FieldValue {

    /**
     * Returns the integer value of the object.
     *
     * @return the integer value
     */
    int get();

    /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public IntegerValue clone();
}



