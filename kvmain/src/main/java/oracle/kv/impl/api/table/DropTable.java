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

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Removes a table (or marks it for deletion).
 */
class DropTable extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final String namespace;
    private final boolean markForDelete;

    DropTable(String namespace, String tableName,
              boolean markForDelete, int seqNum) {
        super(seqNum);
        this.tableName = tableName;
        this.namespace = namespace;
        this.markForDelete = markForDelete;
    }

    DropTable(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        tableName = readString(in, serialVersion);
        namespace = readString(in, serialVersion);
        markForDelete = in.readBoolean();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, tableName);
        writeString(out, serialVersion, namespace);
        out.writeBoolean(markForDelete);
    }

    @Override
    TableImpl apply(TableMetadata md) {
        return md.removeTable(namespace, tableName, markForDelete);
    }

    @Override
    ChangeType getChangeType() {
        return StandardChangeType.DROP_TABLE;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof DropTable)) {
            return false;
        }
        final DropTable other = (DropTable) obj;
        return Objects.equals(namespace, other.namespace) &&
            Objects.equals(tableName, other.tableName) &&
            (markForDelete == other.markForDelete);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            namespace,
                            tableName,
                            markForDelete);
    }
}
