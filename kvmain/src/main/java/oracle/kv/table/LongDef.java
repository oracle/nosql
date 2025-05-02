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
 * LongDef is an extension of {@link FieldDef} to encapsulate a Long.  It adds
 * a minimum and maximum value range and type.  Minimum and maximum are always
 * inclusive.
 *
 * @since 3.0
 */
public interface LongDef extends FieldDef {

    /**
     * @return the minimum value for the instance if defined, otherwise null
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on Long types.
     */
    @Deprecated
    Long getMin();

    /**
     * @return the maximum value for the instance if defined, otherwise null
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on Long types.
     */
    @Deprecated
    Long getMax();

    /**
     * @return a deep copy of this object
     */
    @Override
    public LongDef clone();
}
