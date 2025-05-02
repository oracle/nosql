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
 * BinaryDef is an extension of {@link FieldDef} to encapsulate a Binary type.
 * There is no instance-specific state at this time.
 *
 * @since 3.0
 */
public interface BinaryDef extends FieldDef {

    /**
     * @return a deep copy of this object.
     */
    @Override
    public BinaryDef clone();

    /**
     * Creates a BinaryValue instance from a String.  The String must be a
     * Base64 encoded string returned from {@link BinaryValue#toString}.
     *
     * @return a String representation of the value
     *
     * @throws IllegalArgumentException if the string cannot be decoded
     */
    public BinaryValue fromString(String encodedString);
}
