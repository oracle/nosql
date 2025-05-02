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
 * IntegerDef is an extension of {@link FieldDef} to encapsulate an Integer.
 * It adds a minimum and maximum value range and type.  Minimum and maximum
 * are inclusive.
 *
 * @since 3.0
 */
public interface IntegerDef extends FieldDef {

    /**
     * @return the minimum value for the instance if defined, otherwise null
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on Integer types. A storage size argument can be specified on
     * an Integer type when used in a primary key.
     */
    @Deprecated
    Integer getMin();

    /**
     * @return the maximum value for the instance if defined, otherwise null
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on Integer types. A storage size argument can be specified on
     * an Integer type when used in a primary key.
     */
    @Deprecated
    Integer getMax();

    /**
     * @return a deep copy of this object
     */
    @Override
    public IntegerDef clone();
}
