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

import oracle.kv.impl.security.ResourceOwner;

/**
 * A TableMetadata Change for createNamespace.
 */
class AddNamespaceChange extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final ResourceOwner owner;

    AddNamespaceChange(String namespace, ResourceOwner owner, int seqNum) {
        super(seqNum);
        this.namespace = namespace;
        this.owner = owner;
    }

    AddNamespaceChange(DataInput in, short serialVersion)
        throws IOException
    {
        super(in, serialVersion);
        namespace = readString(in, serialVersion);
        owner = readFastExternalOrNull(in, serialVersion, ResourceOwner::new);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, namespace);
        writeFastExternalOrNull(out, serialVersion, owner);
    }

    @Override
    TableImpl apply(TableMetadata md) {
        md.createNamespace(namespace, owner);
        return null;
    }

    @Override
    ChangeType getChangeType() {
        return StandardChangeType.ADD_NAMESPACE_CHANGE;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof AddNamespaceChange)) {
            return false;
        }
        final AddNamespaceChange other = (AddNamespaceChange) obj;
        return Objects.equals(namespace, other.namespace) &&
            Objects.equals(owner, other.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), namespace, owner);
    }
}
