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

import oracle.kv.Version;
import oracle.kv.table.Table;

/*
 * A class used to deserialize RowImpl instance from bytes.
 */
class RowReaderImpl extends FieldValueReaderImpl<RowImpl> {

    RowReaderImpl(RowImpl row) {
        super(row);
    }

    @Override
    public RowImpl getValue() {
        return (RowImpl)value;
    }

    @Override
    public Table getTable() {
        return getValue().getTable();
    }

    @Override
    public void setTableVersion(int tableVersion) {
        getValue().setTableVersion(tableVersion);
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        getValue().setExpirationTime(expirationTime);
    }

    @Override
    public void setVersion(Version version) {
        getValue().setVersion(version);
    }

    @Override
    public void setRegionId(int regionId) {
        getValue().setRegionId(regionId);
    }

    @Override
    public void setModificationTime(long modificationTime) {
        getValue().setModificationTime(modificationTime);
    }

    @Override
    public void setTombstone(boolean isTombstone) {
        getValue().setTombstone(isTombstone);
    }
}
