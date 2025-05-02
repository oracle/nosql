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

import oracle.kv.table.ArrayDef;

/**
 * ArrayBuilder
 */
public class ArrayBuilder extends CollectionBuilder {

    ArrayBuilder(String description) {
        super(description);
    }

    ArrayBuilder() {
    }

    @Override
    public String getBuilderType() {
        return "Array";
    }

    @Override
    public ArrayDef build() {
        if (field == null) {
            throw new IllegalArgumentException
                ("Array has no field and cannot be built");
        }
        return FieldDefFactory.createArrayDef((FieldDefImpl)field, description);
    }

    /*
     * Create a JSON representation of the array field
     **/
    public String toJsonString(boolean pretty) {
        ArrayDefImpl tmp = FieldDefFactory.createArrayDef((FieldDefImpl)field,
                                                          description);
        return tmp.toJsonString(pretty);
    }
}
