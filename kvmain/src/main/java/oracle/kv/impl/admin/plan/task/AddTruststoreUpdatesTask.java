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
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.NonNullByDefault;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Modifies the installed truststore to add entries from the truststore update,
 * if present, that are not found in the installed truststore, and updates the
 * client truststore.
 *
 * <p>This task modifies the installed truststore as needed, adding any new
 * entries found in the truststore update. To install the updated file
 * atomically, it saves the updated truststore to a file with the name of the
 * installed truststore with a ".tmp" suffix, and then renames it atomically to
 * the installed truststore. The temporary file needs to be located in the
 * security directory, rather in a temporary directory, because atomic renames
 * are only supported for the same file system device. This task has no side
 * effects if it finds that the truststore update has already been installed.
 *
 * @see SecurityUtils#addTruststoreUpdates
 */
@NonNullByDefault
public class AddTruststoreUpdatesTask
        extends SNRetryTask<AbstractPlanWithTopology> {

    private static final long serialVersionUID = 1L;

    public AddTruststoreUpdatesTask(AbstractPlanWithTopology plan,
                                    StorageNodeId snId) {
        super(plan, snId);
    }

    @Override
    protected @Nullable String doSNTask(StorageNodeAgentAPI snApi)
        throws RemoteException
    {
        return snApi.addTruststoreUpdates();
    }
}
