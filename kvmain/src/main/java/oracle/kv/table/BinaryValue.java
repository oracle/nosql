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
 * BinaryValue extends {@link FieldValue} to represent a byte array
 * value.
 *
 * @since 3.0
 */
public interface BinaryValue extends FieldValue {

    /**
     * Returns the byte array value.
     *
     * @return the byte array value
     */
    byte[] get();

    /**
     * Returns 0 if the two values are equal in terms of length and byte
     * content, otherwise it returns -1.
     *
     * @return 0 if the values are equal, -1 otherwise
     */
    @Override
    public int compareTo(FieldValue otherValue);

    /**
     * Create a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public BinaryValue clone();

    /**
     * Returns a String representation of the value using a Base64 encoding.
     *
     * @return a String representation of the value
     */
    @Override
    public String toString();
}
