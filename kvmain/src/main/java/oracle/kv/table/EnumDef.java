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
 * EnumDef is a extension of {@link FieldDef} to support an enumeration type.
 * Valid members of an enumeration are represented as an array of strings.
 * A value instance of EnumDef is represented by a single string, which
 * must exist in the set of valid strings.
 *
 * @since 3.0
 */
public interface EnumDef extends FieldDef {

    /**
     * @return the legal values for the enumeration
     */
    String[] getValues();

    /**
     * @return the name of the enumeration.  Unlike most FieldDef instances
     * enumerations require a name to be included in the schema.
     */
    String getName();

    /**
     * @return a deep copy of this object
     */
    @Override
    public EnumDef clone();
}
