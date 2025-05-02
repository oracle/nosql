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

package oracle.kv.impl.security.util;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Kerberos principal instance name of a storage node.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class SNKrbInstance implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1L;

    private final String instanceName;
    private final int snId;

    public SNKrbInstance(String instanceName, int storageNodeId) {
        checkNull("instanceName", instanceName);
        this.instanceName = instanceName;
        this.snId = storageNodeId;
    }

    /**
     * FastExternalizable constructor.
     */
    public SNKrbInstance(DataInput in, short serialVersion)
        throws IOException {

        instanceName = readNonNullString(in, serialVersion);
        snId = in.readInt();
    }

    public String getInstanceName() {
        return this.instanceName;
    }

    public int getStorageNodeId() {
        return snId;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullString non-null String})
     *      {@link #getInstanceName instanceName}
     * <li> ({@link DataOutput#writeInt int}) {@link #getStorageNodeId snId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeNonNullString(out, serialVersion, instanceName);
        out.writeInt(snId);
    }
}
