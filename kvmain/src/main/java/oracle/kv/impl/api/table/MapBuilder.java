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

package oracle.kv.impl.api.table;

import oracle.kv.table.MapDef;

/**
 * MapBuilder
 */
public class MapBuilder extends CollectionBuilder {

    MapBuilder(String description) {
        super(description);
    }

    MapBuilder() {
    }

    @Override
    public String getBuilderType() {
        return "Map";
    }

    @Override
    public MapDef build() {
        if (field == null) {
            throw new IllegalArgumentException
                ("Map has no field and cannot be built");
        }
        return FieldDefFactory.createMapDef((FieldDefImpl)field, description);
    }

    /*
     * Create a JSON representation of the map field
     **/
    public String toJsonString(boolean pretty) {
        MapDefImpl tmp = FieldDefFactory.createMapDef((FieldDefImpl)field,
                                                      description);
        return tmp.toJsonString(pretty);
    }
}
