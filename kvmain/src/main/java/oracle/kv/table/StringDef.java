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
 * StringDef is an extension of {@link FieldDef} to encapsulate a String.
 * It adds a minimum and maximum value range and type.
 * <p>
 * Comparisons to minimum and maxium values are done using
 * {@link String#compareTo}.
 *
 * @since 3.0
 */
public interface StringDef extends FieldDef {

    /**
     * Get the minimum value for the string.
     *
     * @return the minimum value for the instance if defined, otherwise null
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on String types.
     */
    @Deprecated
    String getMin();

    /**
     * Get the maximum value for the string.
     *
     * @return the maximum value for the instance if defined, otherwise null
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on String types.
     */
    @Deprecated
    String getMax();

    /**
     * @return true if the minimum is inclusive.  This value is only relevant
     * if {@link #getMin} returns a non-null value.
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on String types.
     */
    @Deprecated
    boolean isMinInclusive();

    /**
     * @return true if the maximum is inclusive.  This value is only relevant
     * if {@link #getMax} returns a non-null value.
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on String types.
     */
    @Deprecated
    boolean isMaxInclusive();

    /**
     * @return a deep copy of this object
     */
    @Override
    public StringDef clone();
}
