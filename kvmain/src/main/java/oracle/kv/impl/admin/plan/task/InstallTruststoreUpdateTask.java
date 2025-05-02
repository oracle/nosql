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

package oracle.kv.impl.admin.plan.task;

import java.rmi.RemoteException;

import oracle.kv.impl.admin.plan.UpdateTlsCredentialsPlan;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI.CredentialHashes;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.NonNullByDefault;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Install the truststore update, if present, and update the client truststore.
 *
 * <p>This task replaces the installed truststore with the truststore update,
 * if the update file is present and different from the installed file, and
 * then removes both the keystore and truststore updates, if present. It
 * performs the replacement by copying the truststore update to a file with the
 * name of the installed truststore with a ".tmp" suffix and then renames it
 * atomically to the installed truststore. The temporary file needs to be
 * located in the security directory, rather in a temporary directory, because
 * atomic renames are only supported within the same file system device.
 *
 * @see SecurityUtils#installTruststoreUpdate
 */
@NonNullByDefault
public class InstallTruststoreUpdateTask
        extends SNRetryTask<UpdateTlsCredentialsPlan> {

    private static final long serialVersionUID = 1L;

    public InstallTruststoreUpdateTask(UpdateTlsCredentialsPlan plan,
                                       StorageNodeId snId) {
        super(plan, snId);
    }

    @Override
    protected @Nullable String doSNTask(StorageNodeAgentAPI snApi)
        throws RemoteException
    {
        final CredentialHashes hashes = plan.getCredentialHashes(snId);
        return snApi.installTruststoreUpdate(hashes.truststore.hash);
    }
}
