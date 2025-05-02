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
 * Verify credential updates for updates on an SN.
 *
 * <p>This task modifies the associated plan object to contain the hashes of
 * the update files if any, but has no other side effects.
 *
 * @see SecurityUtils#verifyTlsCredentialUpdates
 */
@NonNullByDefault
public class VerifyTlsCredentialUpdatesTask
        extends SNRetryTask<UpdateTlsCredentialsPlan> {

    private static final long serialVersionUID = 1L;

    public VerifyTlsCredentialUpdatesTask(UpdateTlsCredentialsPlan plan,
                                          StorageNodeId snId) {
        super(plan, snId);
    }

    @Override
    protected @Nullable String doSNTask(StorageNodeAgentAPI snApi)
        throws RemoteException
    {
        final CredentialHashes hashes =
            snApi.verifyTlsCredentialUpdates(plan.isForce());
        plan.setCredentialHashes(snId, hashes);
        return null;
    }
}
