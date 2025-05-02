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
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.NonNullByDefault;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Installs the keystore update, if present.
 *
 * <p>This task replaces the installed keystore with the keystore update if
 * present and different from the installed keystore. To update the file
 * atomically, it copies the keystore update to a file with the name of the
 * installed keystore with a ".tmp" suffix, and then renames it atomically to
 * the installed keystore. The temporary file needs to be located in the
 * security directory, rather in a temporary directory, because atomic renames
 * are only supported within the same file system device.
 *
 * @see SecurityUtils#installKeystoreUpdate
 */
@NonNullByDefault
public class InstallKeystoreUpdateTask
        extends SNRetryTask<UpdateTlsCredentialsPlan> {

    private static final long serialVersionUID = 1L;

    public InstallKeystoreUpdateTask(UpdateTlsCredentialsPlan plan,
                                     StorageNodeId snId) {
        super(plan, snId);
    }

    @Override
    protected @Nullable String doSNTask(StorageNodeAgentAPI snApi)
        throws RemoteException
    {
        return snApi.installKeystoreUpdate(
            plan.getCredentialHashes(snId).keystore.hash);
    }
}
