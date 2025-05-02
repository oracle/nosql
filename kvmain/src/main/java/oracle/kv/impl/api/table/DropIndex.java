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
 *
 */
class DropIndex extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String indexName;
    private final String tableName;
    private final String namespace;

    DropIndex(String namespace,
              String indexName,
              String tableName,
              int seqNum) {
        super(seqNum);
        this.indexName = indexName;
        this.tableName = tableName;
        this.namespace = namespace;
    }

    DropIndex(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        indexName = readString(in, serialVersion);
        tableName = readString(in, serialVersion);
        namespace = readString(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, indexName);
        writeString(out, serialVersion, tableName);
        writeString(out, serialVersion, namespace);
    }

    @Override
    TableImpl apply(TableMetadata md) {
        return md.removeIndex(namespace, indexName, tableName);
    }

    @Override
    ChangeType getChangeType() {
        return StandardChangeType.DROP_INDEX;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof DropIndex)) {
            return false;
        }
        final DropIndex other = (DropIndex) obj;
        return Objects.equals(namespace, other.namespace) &&
            Objects.equals(indexName, other.indexName) &&
            Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            namespace,
                            indexName,
                            tableName);
    }
}
