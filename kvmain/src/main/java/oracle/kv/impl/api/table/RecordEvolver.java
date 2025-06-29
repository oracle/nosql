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


/**
 * RecordEvolver is used to evolve Records within a table.  It is similar
 * to TableEvolver except that it works directly on the map values and does not
 * use the top-level table.
 */
class RecordEvolver extends TableBuilderBase {
    private final RecordDefImpl record;

    RecordEvolver(final RecordDefImpl record) {
        super(record.getFieldMap());
        this.record = record;
    }

    /**
     * Accessors
     */
    public RecordDefImpl getRecord() {
        return record;
    }
}
