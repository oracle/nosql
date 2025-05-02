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
 * EnumValue extends {@link FieldValue} to represent a single value in an
 * enumeration.  Enumeration values are represented as strings.
 *
 * @since 3.0
 */
public interface EnumValue extends FieldValue {

    /**
     * Returns the {@link EnumDef} instance that defines this value.
     *
     * @return the EnumDef
     */
    @Override
    EnumDef getDefinition();

    /**
     * Gets the string value of the enumeration.
     *
     * @return the string value of the EnumValue
     */
    String get();

    /**
     * Returns the index of the value in the enumeration definition.  This is
     * used for sort order when used in keys and index keys.
     *
     * @return the index of the value of this object in the enumeration
     * definition returned by {@link #getDefinition}
     */
    int getIndex();

    /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public EnumValue clone();
}



