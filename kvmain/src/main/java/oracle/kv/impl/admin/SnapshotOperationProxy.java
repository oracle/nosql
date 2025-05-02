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

package oracle.kv.impl.admin;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Snapshot.SnapResult;
import oracle.kv.impl.admin.Snapshot.SnapResultSummary;
import oracle.kv.impl.admin.Snapshot.SnapshotOperation;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * When security is enabled clients cannot directly invoke methods on the SNA
 * because SNA methods are internal used only in a secure configuration. This
 * class act as a proxy in this case. It is designed to reside on an internal
 * KVStore component (e.g., Admin), and helps to perform snapshot operations on
 * SNA with the component's InternalLoginManager.
 */
public class SnapshotOperationProxy {

    private final Admin admin;
    private final Topology topo;
    private final RegistryUtils ru;
    private final Logger logger;

    /**
     * if set this hook is called in the midst of snapshot execute task
     * after successfully grabbing the elasticity lock to deliberatey block
     * the task to test concurent snapshot execute tasks
     */
    public static volatile TestHook<?> faultHook;

    public SnapshotOperationProxy(final Admin admin) {
        assert admin != null;
        this.admin = admin;
        topo = admin.getCurrentTopology();
        logger = admin.getLogger();
        ru = new RegistryUtils(topo, admin.getLoginManager(), logger);
    }

    /**
     * Returns a specific Storage Node Agent.
     */
    private StorageNodeAgentAPI getStorageNodeAgent(StorageNodeId snid) {
        Exception e = null;
        logger.fine("Trying to contact storage node " + snid +
                    " for snapshot operation");
        try {
            return ru.getStorageNodeAgent(snid);
        } catch (RemoteException re) {
            e = re;
        } catch (NotBoundException nbe) {
            e = nbe;
        }
        final String errMsg =
            "Cannot contact storage node " + snid + ": " + e.getMessage();
        logger.fine(errMsg);
        throw new SNAUnavailableException(errMsg);
    }

    /**
     * Returns any Storage Node Agent.
     */
    private StorageNodeAgentAPI getStorageNodeAgent() {
        SNAUnavailableException e = null;
        final List<StorageNode> storageNodes = topo.getSortedStorageNodes();
        StorageNodeAgentAPI snai = null;
        for (StorageNode sn : storageNodes) {
            try {
                snai = getStorageNodeAgent(sn.getResourceId());
                logger.info("Snapshot operation using storage node: " +
                            sn.getResourceId());
                break;
            } catch (SNAUnavailableException snaue) {
                e = snaue;
            }
        }
        if (snai == null && e != null) {
            throw e;
        }
        return snai;
    }

    /**
     * Common code to perform the same task on all services and wait for
     * results.  Because storage nodes can be down put the possibility of
     * connection-related exceptions inside the task execution vs creation.
     */
     public SnapResultSummary executeSnapshotTasks(SnapshotOperation sop,
                                                   String sname,
                                                   DatacenterId dcId) {
        SnapshotUtils utils = new SnapshotUtils(logger);
        utils.resetOperation(sop);
        ExecutorService threadPool = null;
        try {
            /**
             * Only create snapshot command requires a lock, delete snapshot
             * command doesn't require a lock.
             */
            if(sop.equals(SnapshotOperation.CREATE)) {
                try {
                    admin.getPlanner().lockElasticityForCommand("Snapshot");
                } catch (PlanLocksHeldException e) {
                    throw new IllegalCommandException(e.getMessage());
                }
            }

            assert TestHookExecute.doHookIfSet(faultHook, null);

            List<RepNode> repNodes = topo.getSortedRepNodes();
            threadPool = utils.createThreadPool();
            List<Future<SnapResult>> taskResults = utils.createFutureList();

            /**
             * Do admins first
             */
            Parameters p = admin.getCurrentParameters();
            for (AdminId id : p.getAdminIds()) {
                AdminParams ap = p.get(id);
                StorageNode sn = topo.get(ap.getStorageNodeId());
                if (dcId != null && !sn.getDatacenterId().equals(dcId)) {
                    continue;
                }
                logger.info("Creating task (" + sop + ") for " + id);
                taskResults.add
                    (threadPool.submit
                        (new SnapshotTask(sop, sname, sn.getResourceId(), id)));
            }

            for (RepNode rn : repNodes) {
                StorageNode sn = topo.get(rn.getStorageNodeId());
                if (dcId != null && !sn.getDatacenterId().equals(dcId)) {
                    continue;
                }
                logger.info("Creating task (" + sop + ") for " + rn);
                taskResults.add
                    (threadPool.submit
                        (new SnapshotTask(sop, sname, sn.getResourceId(),
                                          rn.getResourceId())));
            }

            for (StorageNode sn : topo.getSortedStorageNodes()) {
                if (dcId != null && !sn.getDatacenterId().equals(dcId)) {
                    continue;
                }
                logger.info(
                    "Creating snapshot config task (" + sop + ") for " +
                    sn.toString());
                taskResults.add
                    (threadPool.submit
                        (new SnapshotConfigTask(
                            sop, sname, sn.getResourceId())));
            }

            return utils.waitForResults(topo, taskResults);
        } finally {
            if(sop.equals(SnapshotOperation.CREATE)) {
                admin.getPlanner().clearLocksForCommand();
            }
            if (threadPool != null) {
                threadPool.shutdown();
            }
        }
    }

    /**
     * Returns an array of names of snapshots. If no storage node id is
     * specified, this will choose an arbitrary storage node and ask it for its
     * list under the assumption that each SN should have the same snapshots.
     * Try all storage nodes until an available one is found.
     *
     * @param snid id of the storage node. If null, an arbitrary storage node
     * will be chosen.
     * @return an array of snapshot names
     */
    public String[] listSnapshots(StorageNodeId snid) {
        /*
         * If storage node is chosen arbitrarily, its id will have been logged
         * in getStorageNodeAgent() before, so we can make it empty here.
         */
        final String snStr = (snid != null) ? snid.toString() : "";
        logger.info("List snapshots from storage node " + snStr);

        try {
            /* Get an arbitrary storage node if snid is null */
            final StorageNodeAgentAPI snai = (snid == null) ?
                                             getStorageNodeAgent() :
                                             getStorageNodeAgent(snid);
            return snai.listSnapshots();
        } catch (Exception e) {
            logger.info("Failed to list shapshots from storage node " + snStr +
                        ": " + e);
            throw new IllegalCommandException(
                "Cannot list snapshots from storage node " + snStr + ": " +
                e.getMessage(), e);
        }
    }

    /**
     * Helps to call the snapshot methods on SNA according to specified
     * operation types and storage node id. The result of operation will be
     * encapsulated and returned as a {@link SnapResult} instance.
     *
     * @param sop snapshot operation
     * @param snid id of storage node on which the snapshot will be done
     * @param rid the resource that is the snapshot target, a RepNodeId or
     * AdminId
     * @param sname name of snapshot
     * @return operation result as a SnapResult instance
     */
    public SnapResult executeSnapshotOp(SnapshotOperation sop,
                                        StorageNodeId snid,
                                        ResourceId rid,
                                        String sname) {
        try {
            final StorageNodeAgentAPI snai = getStorageNodeAgent(snid);
            if (sop == SnapshotOperation.CREATE) {
                if (rid instanceof RepNodeId) {
                    snai.createSnapshot((RepNodeId) rid, sname);
                } else {
                    snai.createSnapshot((AdminId) rid, sname);
                }
            } else if (sop == SnapshotOperation.REMOVE) {
                if (rid instanceof RepNodeId) {
                    snai.removeSnapshot((RepNodeId) rid, sname);
                } else {
                    snai.removeSnapshot((AdminId) rid, sname);
                }
            } else {
                if (rid instanceof RepNodeId) {
                    snai.removeAllSnapshots((RepNodeId) rid);
                } else {
                    snai.removeAllSnapshots((AdminId) rid);
                }
            }
            logger.info("Snapshot operation " + sop + " of " + sname + " on " +
                        snid + " for " + rid + " succeeded.");
            return new SnapResult(sop, true, rid, null, "Succeeded");
        } catch (Exception e) {
            logger.info("Snapshot operation " + sop + " of " + sname +
                        " failed on " + snid + " for " + rid + ": " +
                        e.getMessage());

            /*
             * The exception may not be serializable, so we wrap it in a
             * SnapshotFaultException.
             */
            return new SnapResult(sop, false, rid,
                                  new SnapshotFaultException(e), "Failed");
        }
    }

    /**
     * Snapshot configuration operation run on specific SN. Backup or remove
     * the named snapshot of configuration under specified SN.
     * @param sop specify operation type for SN configurations.
     * @param snid id of specific SN.
     * @param sname name of snapshot.
     */
    public SnapResult executeSnapshotConfigOp(SnapshotOperation sop,
                                              StorageNodeId snid,
                                              String sname) {
        try {
            final StorageNodeAgentAPI snai = getStorageNodeAgent(snid);
            if (sop == SnapshotOperation.CREATE) {
                snai.createSnapshotConfig(sname);
            } else if (sop == SnapshotOperation.REMOVE) {
                snai.removeSnapshotConfig(sname);
            } else {
                snai.removeAllSnapshotConfigs();
            }
            logger.info("Snapshot config operation " + sop + " of " + sname +
                        " on " + snid + " succeeded.");
            return new SnapResult(sop, true, snid, null, "Succeeded");
        } catch (Exception e) {
            logger.info("Snapshot config operation " + sop + " of " + sname +
                        " failed on " + snid + ": " +
                        new SnapshotFaultException(e));
            return new SnapResult(sop, false, snid,
                                  e, "Failed");
        }
    }

    /**
     * Encapsulate a task to snapshot a single managed service, either RepNode
     * or Admin.
     */
    private class SnapshotTask implements Callable<SnapResult> {
        final SnapshotOperation sop;
        final String sname;
        final StorageNodeId snid;
        final ResourceId rid;

        SnapshotTask(final SnapshotOperation sop,
                     final String sname,
                     final StorageNodeId snid,
                     final ResourceId rid) {
            this.sop = sop;
            this.sname = sname;
            this.snid = snid;
            this.rid = rid;
        }

        @Override
        public SnapResult call() {
            return executeSnapshotOp(sop, snid, rid, sname);
        }
    }

    /**
     * Encapsulate a task to snapshot configurations of a SN.
     */
    private class SnapshotConfigTask implements Callable<SnapResult> {
        final SnapshotOperation sop;
        final String sname;
        final StorageNodeId snid;

        SnapshotConfigTask(final SnapshotOperation sop,
                           final String sname,
                           final StorageNodeId snid) {
            this.sop = sop;
            this.sname = sname;
            this.snid = snid;
        }

        @Override
        public SnapResult call() {
            return executeSnapshotConfigOp(sop, snid, sname);
        }
    }

    /**
     * Signals that a StorageNodeAgent is unavailable. This extends
     * {@link NonfatalAssertionException} because it will not crash the Admin.
     */
    private static class SNAUnavailableException
        extends NonfatalAssertionException {
        private static final long serialVersionUID = 1L;

        private SNAUnavailableException(String msg) {
            super(msg);
        }
    }

    /**
     * Subclass of InternalFaultException used to indicate that the fault
     * originated in calling the snapshot operation on SNA.
     */
    private static class SnapshotFaultException
        extends InternalFaultException {
        private static final long serialVersionUID = 1L;

        private SnapshotFaultException(Throwable cause) {
            super(cause);
        }
    }
}
