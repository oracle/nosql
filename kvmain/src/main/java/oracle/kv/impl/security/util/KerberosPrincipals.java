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

import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Encapsulates Kerberos principal instance names for re-authentication of
 * RepNodeLoginManager. Kerberos authentication requires client to provide
 * appropriate service principal name, which including service, instance
 * and realm in NoSQL system. All server nodes must be in the same realm and
 * use the same service name, so only pass instance names to client.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class KerberosPrincipals implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1L;

    private final SNKrbInstance[] instanceNames;

    public KerberosPrincipals(final SNKrbInstance[] instanceNames) {
        if ((instanceNames == null) || (instanceNames.length == 0)) {
            this.instanceNames = null;
        } else {
            this.instanceNames = instanceNames;
        }
    }

    /**
     * FastExternalizable constructor.
     */
    public KerberosPrincipals(DataInput in, short serialVersion)
        throws IOException {

        final boolean hasInstances = in.readBoolean();
        if (hasInstances) {
            final int len = readNonNullSequenceLength(in);
            this.instanceNames = new SNKrbInstance[len];
            for (int i = 0; i < len; i++) {
                this.instanceNames[i] =
                    new SNKrbInstance(in, serialVersion);
            }
        } else {
            instanceNames = null;
        }
    }

    public SNKrbInstance[] getSNInstanceNames() {
        return this.instanceNames;
    }

    /**
     * Return instance name of given storage node. If it does not exists, return
     * null.
     */
    public String getInstanceName(final StorageNode sn) {
        for (SNKrbInstance snKrb : getSNInstanceNames()) {
            if (sn.getStorageNodeId().getStorageNodeId() ==
                snKrb.getStorageNodeId()) {
                return snKrb.getInstanceName();
            }
        }
        return null;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link DataOutput#writeBoolean boolean}) <i>whether instanceNames
     *      is present</i>
     * <li> ({@link SerializationUtil#writeNonNullArray non-null array}) {@link
     *      #getSNInstanceNames instanceNames}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        if (instanceNames != null && instanceNames.length != 0) {
            out.writeBoolean(true);
            writeNonNullArray(out, serialVersion, instanceNames);
        } else {
            out.writeBoolean(false);
        }
    }
}
