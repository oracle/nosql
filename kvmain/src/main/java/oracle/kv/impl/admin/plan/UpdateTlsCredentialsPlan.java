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

package oracle.kv.impl.admin.plan;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.task.AddTruststoreUpdatesTask;
import oracle.kv.impl.admin.plan.task.CheckTlsCredentialsConsistencyTask;
import oracle.kv.impl.admin.plan.task.InstallKeystoreUpdateTask;
import oracle.kv.impl.admin.plan.task.InstallTruststoreUpdateTask;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.RetrieveTlsCredentialsTask;
import oracle.kv.impl.admin.plan.task.VerifyTlsCredentialUpdatesTask;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.sna.StorageNodeAgentAPI.CredentialHashes;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;

/** Retrieve and install updates to TLS credentials. */
public class UpdateTlsCredentialsPlan extends AbstractPlanWithTopology {

    private static final long serialVersionUID = 1L;

    private final boolean retrieve;
    private final boolean install;
    private final Set<StorageNodeId> snIds;
    private final Map<StorageNodeId, CredentialHashes> snCredentialHashes =
        Collections.synchronizedMap(new HashMap<>());
    private volatile boolean isForce;

    UpdateTlsCredentialsPlan(String planName,
                             Planner planner,
                             Topology topology,
                             boolean retrieve,
                             boolean install) {
        super(getPlanName(planName, retrieve, install), planner, topology);
        if (!retrieve && !install) {
            throw new IllegalArgumentException(
                "At least one of retrieve and install needs to be true");
        }
        this.retrieve = retrieve;
        this.install = install;

        checkSecureStore();

        final StorageNodeMap storageNodes = topology.getStorageNodeMap();
        /* Snapshot of the current set of SNs so we can detect changes */
        snIds = new HashSet<>(storageNodes.getAllIds());

        if (retrieve) {
            final ParallelBundle retrieveTasks = new ParallelBundle();
            snIds.forEach(snId -> retrieveTasks.addTask(
                              new RetrieveTlsCredentialsTask(this, snId)));
            addTask(retrieveTasks);
        }

        final ParallelBundle verifyTasks = new ParallelBundle();
        snIds.forEach(snId -> verifyTasks.addTask(
                          new VerifyTlsCredentialUpdatesTask(this, snId)));
        addTask(verifyTasks);
        addTask(new CheckTlsCredentialsConsistencyTask(this));

        if (install) {
            final ParallelBundle addTruststoreTasks = new ParallelBundle();
            snIds.forEach(snId -> addTruststoreTasks.addTask(
                              new AddTruststoreUpdatesTask(this, snId)));
            addTask(addTruststoreTasks);

            final ParallelBundle installKeystoreTasks = new ParallelBundle();
            snIds.forEach(snId -> installKeystoreTasks.addTask(
                              new InstallKeystoreUpdateTask(this, snId)));
            addTask(installKeystoreTasks);

            final ParallelBundle installTruststoreTasks = new ParallelBundle();
            snIds.forEach(snId -> installTruststoreTasks.addTask(
                              new InstallTruststoreUpdateTask(this, snId)));
            addTask(installTruststoreTasks);
        }
    }

    private void checkSecureStore() {
        final AdminServiceParams params = getAdmin().getParams();
        if ((params == null) ||
            (params.getSecurityParams() == null) ||
            !params.getSecurityParams().isSecure()) {
            throw new IllegalCommandException(
                "Cannot update TLS credentials for a non-secure store");
        }
    }

    /** Record information about credentials for the specified SN. */
    public void setCredentialHashes(StorageNodeId snId,
                                    CredentialHashes hashes) {
        snCredentialHashes.put(snId, hashes);
    }

    public CredentialHashes getCredentialHashes(StorageNodeId snId) {
        return snCredentialHashes.get(snId);
    }

    public Map<StorageNodeId, CredentialHashes> getAllCredentialHashes() {
        return Collections.unmodifiableMap(snCredentialHashes);
    }

    public boolean isForce() {
        return isForce;
    }

    /**
     * Acquire the elasticity lock to prevent topology changes while the plan
     * is running.
     */
    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        planner.lockElasticity(getId(), getName());
    }

    /** Fail if the set of SNs has changed since the plan was created. */
    @Override
    public void preExecuteCheck(boolean force, Logger plannerlogger) {
        checkSecureStore();
        final Topology currentTopo = planner.getAdmin().getCurrentTopology();
        if (!snIds.equals(currentTopo.getStorageNodeMap().getAllIds())) {
            throw new IllegalCommandException(
                "Plan cannot be run because the set of storage nodes has" +
                " changed since the plan was created");
        }
        isForce = force;
    }

    /** Clear snCredentialHashes for a new run. */
    @Override
    synchronized PlanRun startNewRun() {
        snCredentialHashes.clear();
        return super.startNewRun();
    }

    private static String getPlanName(String planName,
                                      boolean retrieve,
                                      boolean install) {
        if (planName != null) {
            return planName;
        }
        return "Update TLS credentials" +
            (!retrieve ? " (install-only)" :
             !install ? " (retrieve-only)" :
             "");
    }

    @Override
    public String getDefaultName() {
        return getPlanName(null, retrieve, install);
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
