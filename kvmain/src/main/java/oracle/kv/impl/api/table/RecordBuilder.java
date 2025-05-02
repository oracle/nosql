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
import oracle.kv.table.RecordDef;

/*
 * Record builder
 */
public class RecordBuilder extends TableBuilderBase {
    private final String name;
    private String description;

    RecordBuilder(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public RecordBuilder(String name) {
        this.name = name;
    }

    @Override
    public String getBuilderType() {
        return "Record";
    }

    @Override
    public RecordDef build() {
        /*
         * Allow null name for anonymous Records
         */
        if (name == null) {
            return new RecordDefImpl(fields, description);
        }
        return new RecordDefImpl(name, fields, description);
    }

    @Override
    public TableBuilderBase addField(String name1, FieldDef def) {
        if (name1 == null) {
            throw new IllegalArgumentException
                ("Record fields must have names");
        }
        return super.addField(name1, def);
    }

    @Override
    public TableBuilderBase setDescription(final String description) {
        this.description = description;
        return this;
    }

    /*
     * Create a JSON representation of the record field
     **/
    public String toJsonString(boolean pretty) {
        RecordDefImpl tmp = new RecordDefImpl(name, fields, description);
        return tmp.toJsonString(pretty);
    }
}
