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

import oracle.kv.table.FieldDef;

/**
 * Common builder for map and array.
 */
class CollectionBuilder extends TableBuilderBase {
    protected String description;
    protected FieldDef field;

    protected CollectionBuilder(String description) {
        this.description = description;
    }

    protected CollectionBuilder() {
        description = null;
    }

    @Override
    public TableBuilderBase addField(final FieldDef field1) {
        if (this.field != null) {
            throw new IllegalArgumentException
                ("Field for collection is already defined");
        }
        this.field = field1;
        return this;
    }

    @Override
    public TableBuilderBase setDescription(final String description) {
        this.description = description;
        return this;
    }

    @Override
    public boolean isCollectionBuilder() {
        return true;
    }
}
