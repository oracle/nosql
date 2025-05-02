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

import oracle.kv.impl.admin.plan.AbstractPlanWithTopology;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.NonNullByDefault;

/**
 * Retrieves updates to TLS credentials for a storage node, calling the
 * retrieve command if one is installed.
 *
 * <p>The only side effects are any produced by the retrieve command. The
 * retrieve command should be written to allow it to be run multiple times in
 * cases where an UpdateTlsCredentialsPlan is retried. It is OK if it retrieves
 * notices and retrieves newly updated credentials on each run.
 *
 * @see StorageNodeAgent#retrieveTlsCredentials
 */
@NonNullByDefault
public class RetrieveTlsCredentialsTask
        extends SNRetryTask<AbstractPlanWithTopology> {

    private static final long serialVersionUID = 1L;

    public RetrieveTlsCredentialsTask(AbstractPlanWithTopology plan,
                                      StorageNodeId snId) {
        super(plan, snId);
    }

    @Override
    protected String doSNTask(StorageNodeAgentAPI snApi)
        throws RemoteException
    {
        return snApi.retrieveTlsCredentials();
    }
}
