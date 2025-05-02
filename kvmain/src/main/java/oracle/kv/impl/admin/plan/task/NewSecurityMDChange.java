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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;

/**
 * Send a simple newSecurityMDChange call to the CommandServiceAPI to notify
 * new security metadata change.
 *
 * This task should be executed right after updating security metadata task.
 */
public class NewSecurityMDChange extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected final AdminId adminId;
    private final AbstractPlan plan;

    public NewSecurityMDChange(AbstractPlan plan, AdminId adminId) {
        this.plan = plan;
        this.adminId = adminId;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        final Logger logger = plan.getLogger();
        final Parameters parameters = admin.getCurrentParameters();
        final SecurityMetadata md =
            admin.getMetadata(SecurityMetadata.class, MetadataType.SECURITY);
        if (md == null) {
            throw new IllegalCommandException("No security metadata");
        }
        final int latestSequenceNum = md.getSequenceNumber();
        logger.log(Level.FINE,
                  "{0} notify target of security metadata change " +
                  "whose seq number is {1}",
                  new Object[]{this, latestSequenceNum});

        try {
            final AdminParams current = parameters.get(adminId);
            final StorageNodeId snid = current.getStorageNodeId();
            final StorageNodeParams snp = parameters.get(snid);
            final CommandServiceAPI cs =
                ServiceUtils.waitForAdmin(snp.getHostname(),
                                          snp.getRegistryPort(),
                                          plan.getLoginManager(),
                                          40,
                                          ServiceStatus.RUNNING,
                                          plan.getLogger());
            cs.newSecurityMDChange();
        } catch (NotBoundException notbound) {
            logger.log(Level.INFO,
                       "{0} {1} cannot be contacted when notifying about " +
                       "security metadata change: {2}",
                       new Object[]{this, adminId, notbound});
            throw notbound;
        } catch (RemoteException e) {
            logger.log(Level.SEVERE,
                       "{0} attempting to notify {1} about " +
                       "security metadata change: {2}",
                       new Object[]{this, adminId, e});
            throw e;
        }
        return State.SUCCEEDED;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" cause ").append(adminId)
                       .append(" to apply the latest security metadata change");
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    /**
     * Returns true if this NewSecurityMDChange will end up call the same
     * admin to notify the security metadata change. Checks that admin Id
     * are the same.
     */
    @Override
    public boolean logicalCompare(Task t) {
        if (this == t) {
            return true;
        }

        if (t == null) {
            return false;
        }

        if (getClass() != t.getClass()) {
            return false;
        }
        NewSecurityMDChange other = (NewSecurityMDChange) t;

        return adminId.equals(other.adminId);
    }
}
