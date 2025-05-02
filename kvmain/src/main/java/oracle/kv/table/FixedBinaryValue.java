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
 * FixedBinaryValue extends {@link FieldValue} to represent a fixed-size byte
 * array.
 *
 * @since 3.0
 */
public interface FixedBinaryValue extends FieldValue {

    /**
     * Returns the {@link FixedBinaryDef} instance that defines this value.
     *
     * @return the FixedBinaryDef
     */
    @Override
    FixedBinaryDef getDefinition();

    /**
     * Get the byte array value.
     *
     * @return the byte array value
     */
    byte[] get();

    /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public FixedBinaryValue clone();

    /**
     * Returns a String representation of the value using a Base64 encoding.
     *
     * @return a String representation of the value
     */
    @Override
    public String toString();
}
