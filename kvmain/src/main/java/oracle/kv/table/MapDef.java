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
 * MapDef is an extension of {@link FieldDef} to define an unordered map, where
 * all entries are constrained to a single type. Map keys are case-sensitive.
 *
 * @since 3.0
 */
public interface MapDef extends FieldDef {

    /**
     * @return the definition of the type of object stored in the map
     */
    FieldDef getElement();

    /**
     * @return a deep copy of this object
     */
    @Override
    public MapDef clone();

    /**
     * @hidden
     * For internal use only.
     * @return the definition type of the key
     */
    FieldDef getKeyDefinition();
}
