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

import java.util.List;

/**
 * PrimaryKey is a specialization of Row to represent a primary key used to
 * access records in a table using the {@link TableAPI}.  It may contain all or
 * part of a primary key for its associated table.  If a PrimaryKey is
 * partially filled the fields must be set in order of significance, as defined
 * by the list returned by {@link #getFields}.  If an attempt is made to set a
 * field that is not part of the primary key IllegalArgumentException is
 * thrown.
 *<p>
 * PrimaryKey objects are constructed using
 * {@link Table#createPrimaryKey createPrimaryKey}.
 *
 * @since 3.0
 */
public interface PrimaryKey extends Row {

    /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public PrimaryKey clone();

    /**
     * Returns an unmodifiable list of fields, in key order, that
     * comprise this key.  This method returns the same list as {@link
     * Table#getPrimaryKey} for this instance's Table.
     *
     * @return the fields
     *
     * @since 3.0.6
     * @deprecated as of 4.2 Please use the equivalent getFieldNames() method.
     */
    @Deprecated
    @Override
    List<String> getFields();
}
