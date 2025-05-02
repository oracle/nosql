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
 * FixedBinaryDef is an extension of {@link FieldDef} to encapsulate a fixed
 * length binary value.
 *
 * @since 3.0
 */
public interface FixedBinaryDef extends FieldDef {

    /**
     * @return the size of the binary value in bytes
     */
    int getSize();

    /**
     * @return the name of the type.  A name is required for
     * serialization.
     */
    String getName();

    /**
     * @return a deep copy of this object
     */
    @Override
    public FixedBinaryDef clone();

    /**
     * Creates a FixedBinaryValue instance from a String.  The String must be a
     * Base64 encoded string returned from {@link FixedBinaryValue#toString}.
     *
     * @return a String representation of the value
     *
     * @throws IllegalArgumentException if the string cannot be decoded
     */
    public FixedBinaryValue fromString(String encodedString);
}
