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
 * A TableMetadata Change for dropNamespace.
 */
class RemoveNamespaceChange extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String namespace;

    RemoveNamespaceChange(String namespace, int seqNum) {
        super(seqNum);
        this.namespace = namespace;
    }

    RemoveNamespaceChange(DataInput in, short serialVersion)
        throws IOException
    {
        super(in, serialVersion);
        namespace = readString(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, namespace);
    }

    @Override
    TableImpl apply(TableMetadata md) {
        md.dropNamespace(namespace);
        return null;
    }

    @Override
    ChangeType getChangeType() {
        return StandardChangeType.REMOVE_NAMESPACE_CHANGE;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof RemoveNamespaceChange)) {
            return false;
        }
        final RemoveNamespaceChange other = (RemoveNamespaceChange) obj;
        return Objects.equals(namespace, other.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), namespace);
    }
}
