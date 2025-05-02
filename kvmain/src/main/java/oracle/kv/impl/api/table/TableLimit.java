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

import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * A change to table limits.
 */
class TableLimit extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String tableName;
    private final TableLimits limits;

    TableLimit(TableImpl table, int seqNum) {
        super(seqNum);
        assert table.isTop();
        tableName = table.getFullName();
        namespace = table.getInternalNamespace();
        limits = table.getTableLimits();
    }

    TableLimit(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        tableName = readString(in, serialVersion);
        namespace = readString(in, serialVersion);
        limits = readFastExternalOrNull(in, serialVersion, TableLimits::new);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, tableName);
        writeString(out, serialVersion, namespace);
        writeFastExternalOrNull(out, serialVersion, limits);
    }

    @Override
    TableImpl apply(TableMetadata md) {
        final TableImpl table = md.getTable(namespace, tableName, true);
        if (table == null) {
            return null;
        }
        table.setTableLimits(limits);
        return table;
    }

    @Override
    ChangeType getChangeType() {
        return StandardChangeType.TABLE_LIMIT;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof TableLimit)) {
            return false;
        }
        final TableLimit other = (TableLimit) obj;
        return Objects.equals(namespace, other.namespace) &&
            Objects.equals(tableName, other.tableName) &&
            Objects.equals(limits, other.limits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            namespace,
                            tableName,
                            limits);
    }
}
