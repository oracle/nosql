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

import java.io.PrintStream;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A class to encapsulate store-wide snapshot creation and management.
 * Model for snapshots:
 * -- Snapshots are run for all nodes, masters and replicas
 * -- Snapshots will fail for disabled or not-running services
 *
 * Off-node backups of snapshots is done via direct administrative copies of
 * files associated with a particular snapshot.  In general the snapshot from
 * the nodes acting as master at the time of the snapshot should be used.
 *
 * Operations supported include:
 * createSnapshot(name)
 * listSnapshots()
 * removeSnapshot(name)
 * removeAllSnapshots()
 *
 * The methods, with the exception of createSnapshot() are void.  Results are
 * determined by calling methods after the operation returns.  The general
 * pattern is:
 *   Snapshot snapshot = new Snapshot(...);
 *   snapshot.operation();
 *   if (!snapshot.succeeded()) {
 *     ...problems...
 *     List&lt;SnapResult&gt; failures = snapshot.getFailures();
 *     ... process failures ...
 *   }
 *
 * The SnapResult object contains the ResourceId of the failed node along with
 * any exception or additional information that may be useful.  It is also
 * possible to get a list of successes.
 *
 * The name passed into createSnapshot() will be appended to the current date
 * and time to make the actual snapshot name used, which is the one returned
 * by listSnapshots() and used for removeSnapshot().
 *
 */

public class Snapshot {

    private final Topology topo;
    private final CommandServiceAPI cs;
    private final SnapshotUtils utils;
    private SnapResultSummary snapResultSummary;
    private SnapshotOperation op;

    public enum SnapshotOperation {
        CREATE, REMOVE, REMOVEALL
    }

    public Snapshot(CommandServiceAPI cs,
                    boolean verboseOutput,
                    PrintStream output)
        throws RemoteException {
        this.cs = cs;
        topo = cs.getTopology();
        utils = new SnapshotUtils(verboseOutput, output);
    }

    /**
     * Results handling.
     */

    /**
     * Indicates whether the operation succeeded on all nodes in the Topology.
     */
    public boolean succeeded() {
        return snapResultSummary.getSucceeded();
    }

    /**
     * Return true if at least one node in each replication group (including
     * the admin) returned success.  If there was a failure the caller must
     * use other methods to report the problems.
     */
    public boolean getQuorumSucceeded() {
        return snapResultSummary.getQuorumSucceeded();
    }

    /**
     * Return the list of SnapResult objects for which the operation succeeded
     */
    public List<SnapResult> getSuccesses() {
        return snapResultSummary.getSuccesses();
    }

    /**
     * Return the list of SnapResult objects for which the operation failed
     */
    public List<SnapResult> getFailures() {
        return snapResultSummary.getFailures();
    }

    /**
     * Return the list of SnapResult of snapshot configuration operations
     * which are succeeded.
     */
    public List<SnapResult> getConfigSuccesses() {
        return snapResultSummary.getConfigSuccesses();
    }

    /**
     * Return the list of SnapResult of snapshot configuration operations
     * which are failed.
     */
    public List<SnapResult> getConfigFailures() {
        return snapResultSummary.getConfigFailures();
    }

    public SnapshotOperation getOperation() {
        return op;
    }

    /**
     * Create a snapshot on all Storage Nodes in the topology.  The actual
     * snapshot name is the current time/date concatenated with the name
     * parameter.
     *
     * @param name the suffix to use for the snapshot name
     *
     * @return the generated snapshot name
     */
    public String createSnapshot(String name)
        throws RemoteException {
        return createSnapshot(name, null);
    }

    /**
     * Create a snapshot on all Storage Nodes in the specified zone.
     * The actual snapshot name is the current time/date concatenated with the
     * name parameter.
     * Operation applies to all zones if dcId is null.
     *
     * @param name the suffix to use for the snapshot name
     * @param dcId the datacenterId
     *
     * @return the generated snapshot name
     */
    public String createSnapshot(String name, DatacenterId dcId)
        throws RemoteException {

        String snapName = makeSnapshotName(name);
        utils.verbose("Start create snapshot " + snapName);
        resetOperation(SnapshotOperation.CREATE);
        snapResultSummary = cs.executeSnapshotOperation(op, snapName, dcId);
        utils.verbose("Complete create snapshot " + snapName);
        return snapName;
    }

    /**
     * Remove a snapshot from all Storage Nodes.  Each Storage Node will remove
     * it from any managed services.
     *
     * @param name the full name of the snapshot, including date time that was
     * generated by createSnapshot
     *
     */
    public void removeSnapshot(String name)
        throws RemoteException {
        removeSnapshot(name, null);
    }

    /**
     * Remove a snapshot from all Storage Nodes belonging to the given zone.
     * Each Storage Node will remove it from any managed services.
     * Operation applies to all zones if dcId is null.
     *
     * @param name the full name of the snapshot, including date time that was
     * generated by createSnapshot
     * @param dcId the datacenterId
     */
    public void removeSnapshot(String name, DatacenterId dcId)
        throws RemoteException {

        utils.verbose("Start remove snapshot " + name);
        resetOperation(SnapshotOperation.REMOVE);
        snapResultSummary = cs.executeSnapshotOperation(op, name, dcId);
        utils.verbose("Complete remove snapshot " + name);
    }

    /**
     * Remove all known snapshots unconditionally.
     */
    public void removeAllSnapshots()
        throws Exception {

        removeAllSnapshots(null);
    }

    /**
     * Remove all known snapshots in a given zone. If null is supplied for dcId
     * all the known spanshots are removed unconditionally.
     */
    public void removeAllSnapshots(DatacenterId dcId)
        throws Exception {

        utils.verbose("Start remove all snapshots");
        resetOperation(SnapshotOperation.REMOVEALL);
        snapResultSummary = cs.executeSnapshotOperation(op, null, dcId);
        utils.verbose("Complete remove all snapshots");
    }

    /**
     * Return an array of names of snapshots.  For now this will choose an
     * arbitrary Storage Node and ask it for its list under the assumption
     * that each SN should have the same snapshots.  Try all storage nodes
     * if any are not available.
     */
    public String[] listSnapshots()
        throws RemoteException {

        return cs.listSnapshots(null /* StorageNodeId */);
    }

    /**
     * A variant of listSnapshots that takes a specific StorageNodeId as the
     * target node.
     */
    public String[] listSnapshots(StorageNodeId snid)
        throws RemoteException {

        StorageNode sn = topo.get(snid);
        if (sn == null) {
            throw new IllegalArgumentException
                ("No Storage Node found with id " + snid);
        }
        utils.verbose("Listing snapshots from Storage Node: " + snid);
        return cs.listSnapshots(snid);
    }

    private void resetOperation(SnapshotOperation newOp) {
        op = newOp;
        utils.resetOperation(newOp);
        snapResultSummary = null;
    }

    private String makeSnapshotName(String name) {
        Date now = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyMMdd-HHmmss");
        return format.format(now) + "-" + name;
    }

    /**
     * The SnapResult object contains the ResourceId of the failed node along
     * with any exception or additional information that may be useful.
     */
    public static class SnapResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean result;
        private final ResourceId service;
        private final Exception ex;
        private final String message;
        private final SnapshotOperation op;

        protected SnapResult(final SnapshotOperation op,
                             final boolean result,
                             final ResourceId service,
                             final Exception ex,
                             final String message) {
            this.op = op;
            this.result = result;
            this.service = service;
            this.ex = ex;

            /**
             * TODO: should message include stack trace in the exception case?
             */
            if (ex != null) {
                this.message = message + ", Exception: " + ex;
            } else {
                this.message = message;
            }
        }

        public boolean getSucceeded() {
            return result;
        }

        public ResourceId getService() {
            return service;
        }

        public String getMessage() {
            return message;
        }

        public Exception getException() {
            return ex;
        }

        public String getExceptionStackTrace() {
            if (ex != null) {
                return LoggerUtils.getStackTrace(ex);
            }
            return "";
        }

        @Override
        public String toString() {
            return "Operation " + op + " on " + service +
                (result ? " succeeded" : " failed") + ": " + message;
        }
    }

    /**
     * Encapsulate the results of a distributed snapshot operation.
     */
    public static class SnapResultSummary implements Serializable {
        private static final long serialVersionUID = 1L;

        /*
         * Indicate success of snapshot data files.
         */
        private final List<SnapResult> success;

        /*
         * Indicate failure of snapshot data files.
         */
        private final List<SnapResult> failure;

        private final boolean allSucceeded;
        private final boolean quorumSucceeded;
        private final List<SnapResult> configSuccess;
        private final List<SnapResult> configFailure;

        public SnapResultSummary(final List<SnapResult> success,
                                 final List<SnapResult> failure,
                                 final boolean allSucceeded,
                                 final boolean quorumSucceeded,
                                 final List<SnapResult> configSuccess,
                                 final List<SnapResult> configFailure) {
            this.success = success;
            this.failure = failure;
            this.allSucceeded = allSucceeded;
            this.quorumSucceeded = quorumSucceeded;
            this.configSuccess = configSuccess;
            this.configFailure = configFailure;
        }

        public List<SnapResult> getSuccesses() {
            return success;
        }

        public List<SnapResult> getFailures() {
            return failure;
        }

        public boolean getSucceeded() {
            return allSucceeded;
        }

        public boolean getQuorumSucceeded() {
            return quorumSucceeded;
        }

        public List<SnapResult> getConfigSuccesses() {
            if (configSuccess == null) {
                return Collections.emptyList();
            }
            return configSuccess;
        }

        public List<SnapResult> getConfigFailures() {
            if (configFailure == null) {
                return Collections.emptyList();
            }
            return configFailure;
        }

    }
}
