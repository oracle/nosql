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
import java.util.HashSet;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.TopologyCheck;
import oracle.kv.impl.admin.TopologyCheck.JEHAInfo;
import oracle.kv.impl.admin.TopologyCheck.OkayRemedy;
import oracle.kv.impl.admin.TopologyCheck.Remedy;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.MigrateSNPlan;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Do topology verification before a MigrateSNPlan. We hope to detect any
 * topology inconsistencies and prevent cascading topology errors.
 *
 * Also check the version of the new SN, to prevent the inadvertent downgrade
 * of a RN version.
 */
public class VerifyBeforeMigrate extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final StorageNodeId oldSN;
    private final StorageNodeId newSN;

    public VerifyBeforeMigrate(AbstractPlan plan,
                               StorageNodeId oldSN,
                               StorageNodeId newSN) {
        this.plan = plan;
        this.oldSN = oldSN;
        this.newSN = newSN;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return (plan instanceof MigrateSNPlan) ?
            ((MigrateSNPlan) plan).isForce() :
            false;
    }

    /**
     * Check that the rep groups for all the RNs that are involved in the move
     * have quorum. If they don't, the migrate will fail.
     */
    @Override
    public State doWork() throws Exception {

        final Admin admin = plan.getAdmin();
        final Parameters params = admin.getCurrentParameters();
        final Topology topo = admin.getCurrentTopology();

        /*
         * Check that the new SN is at a version compatible with that of the
         * Admin. Ideally we'd check the destination SN is a version that is
         * greater than or equal to the source SN, but the source SN is down
         * and unavailable. Instead, check that the destination SN is a version
         * that is >= the Admin version. If the Admin is at an older version
         * that the source SN, this check will be too lenient. If the Admin has
         * is at a newer version than the source SN, this will be too
         * stringent. Nevertheless, it's worthwhile making the attempt. If the
         * check is insufficient, the migrated RNs and Admins will not come up
         * on the new SN, so the user will see the issue, though at a less
         * convenient time.
         */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, admin.getLoginManager(), plan.getLogger());

        final String errorMsg =
            newSN + " cannot be contacted. Please ensure " +
            "that it is deployed and running before attempting to migrate " +
            "to this storage node: ";

        KVVersion newVersion = null;
        try {
            final StorageNodeAgentAPI newSNA =
                    regUtils.getStorageNodeAgent(newSN);
            newVersion = newSNA.ping().getKVVersion();

        } catch (RemoteException | NotBoundException e) {
            throw new OperationFaultException(errorMsg + e);
        }

        if (VersionUtil.compareMinorVersion(KVVersion.CURRENT_VERSION,
                                            newVersion) > 0) {
            throw new OperationFaultException
                ("Cannot migrate " + oldSN + " to " + newSN +
                 " because " + newSN + " is at older version " + newVersion +
                 ". Please upgrade " + newSN +
                 " to a version that is equal or greater than " +
                 KVVersion.CURRENT_VERSION);
        }

        /*
         * Find all the RNs that are purported to live on either the old
         * SN or the newSN. We need to check both old and new because the
         * migration might be re-executed, and the changes might already
         * have been partially carried out.
         */
        final Set<RepNodeId> hostedRNs = new HashSet<>();
        for (RepNodeParams rnp : params.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(oldSN) ||
                rnp.getStorageNodeId().equals(newSN)) {
                hostedRNs.add(rnp.getRepNodeId());
            }
        }

        HealthCheck.create(plan.getAdmin(), toString(), hostedRNs).await();

        /*
         * Make sure that the RNs involved are consistent in their topology,
         * config.xml, and JE HA addresses. However, we can only check the
         * new SN, because the old one may very well be down. If the migrate
         * has never executed, this RN will still be on the old SN and there
         * won't be anything to check on the new SN, but we will verify the
         * JEHA location.
         */
        final TopologyCheck checker = new TopologyCheck(this.toString(),
                                                        plan.getLogger(),
                                                        topo, params);

        for (RepNodeId rnId : hostedRNs) {
            final Remedy remedy =
                checker.checkLocation(admin, newSN, rnId,
                                      false, /* calledByDeployNewRN */
                                      true /* makeRNEnabled */,
                                      null /* oldSNId */,
                                      null /* storageDirectory */);
            /* TODO: Consider applying the remedy instead of failing. */
            if (!remedy.isOkay()) {
                throw new OperationFaultException
                    (rnId + " has inconsistent location metadata. Please " +
                     "run plan repair-topology: " + remedy);
            }

            final JEHAInfo jeHAInfo = ((OkayRemedy) remedy).getJEHAInfo();
            if (jeHAInfo != null) {
                final StorageNodeId currentHost = jeHAInfo.getSNId();
                if (!currentHost.equals(oldSN) &&
                    !currentHost.equals(newSN)) {

                    // TODO: Run tests to see if this situation occurs, and,
                    // if so, try moving the check into checkRNLocation
                    throw new OperationFaultException
                        (rnId + " has inconsistent location metadata" +
                         " and is living on " + currentHost  +
                         " rather than " + oldSN + " or " + newSN +
                         "Please run plan repair-topology");
                }
            }
        }

        final Set<ArbNodeId> hostedANs = new HashSet<>();
        for (ArbNodeParams anp : params.getArbNodeParams()) {
            if (anp.getStorageNodeId().equals(oldSN) ||
                anp.getStorageNodeId().equals(newSN)) {
                hostedANs.add(anp.getArbNodeId());
            }
        }

        for (ArbNodeId anId : hostedANs) {
            final Remedy remedy =
                checker.checkLocation(admin, newSN, anId,
                                      false /* calledByDeployNewRN */,
                                      true /* makeRNEnabled */,
                                      null /* oldSNId */);
            if (!remedy.isOkay()) {
                throw new OperationFaultException
                    (anId + " has inconsistent location metadata. Please " +
                     "run plan repair-topology : " + remedy);
            }

            final JEHAInfo jeHAInfo = ((OkayRemedy) remedy).getJEHAInfo();
            if (jeHAInfo != null) {
                final StorageNodeId currentHost = jeHAInfo.getSNId();
                if (!currentHost.equals(oldSN) &&
                    !currentHost.equals(newSN)) {

                    // TODO: Run tests to see if this situation occurs, and,
                    // if so, try moving the check into checkRNLocation
                    throw new OperationFaultException
                        (anId + " has inconsistent location metadata" +
                         " and is living on " + currentHost  +
                         " rather than " + oldSN + " or " + newSN +
                         "Please run plan repair-topology");
                }
            }
        }

        AdminId adminToCheck = null;
        for (AdminParams ap : params.getAdminParams()) {
            if (ap.getStorageNodeId().equals(oldSN) ||
                ap.getStorageNodeId().equals(newSN)) {
                adminToCheck = ap.getAdminId();
            }
        }

        if (adminToCheck != null) {
            final Remedy remedy =
                checker.checkAdminMove(admin, adminToCheck, oldSN, newSN);
            /* TODO: Consider applying the remedy instead of failing. */
            if (!remedy.isOkay()) {
                throw new OperationFaultException(
                    adminToCheck + " has inconsistent location metadata." +
                    " Please run plan repair-topology: " + remedy);
            }

            final JEHAInfo jeHAInfo = ((OkayRemedy) remedy).getJEHAInfo();
            if (jeHAInfo != null) {
                final StorageNodeId currentHost = jeHAInfo.getSNId();
                if (!currentHost.equals(oldSN) &&
                    !currentHost.equals(newSN)) {

                    // TODO: Run tests to see if this situation occurs, and,
                    // if so, try moving the check into checkRNLocation
                    throw new OperationFaultException
                        (adminToCheck + " has inconsistent location metadata" +
                         " and is living on " + currentHost  +
                         " rather than " + oldSN + " or " + newSN +
                         "Please run plan repair-topology");
                }
            }
        }

        return Task.State.SUCCEEDED;
    }
}
