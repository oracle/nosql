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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Snapshot.SnapResult;
import oracle.kv.impl.admin.Snapshot.SnapResultSummary;
import oracle.kv.impl.admin.Snapshot.SnapshotOperation;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepGroupMap;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;

class SnapshotUtils {

    private Logger logger;
    private boolean verboseOutput;
    private PrintStream output;
    private SnapshotOperation sop;
    private ExecutorService threadPool;
    /**
     * Results handling
     */
    private List<SnapResult> success;
    private List<SnapResult> failure;
    private boolean allSucceeded = true;
    private boolean quorumSucceeded = true;

    SnapshotUtils (Logger logger) {
        this.logger = logger;
    }

    SnapshotUtils(boolean verboseOutput, PrintStream output) {
        this.verboseOutput = verboseOutput;
        this.output = output;
    }

    void resetOperation(SnapshotOperation newOp) {
        sop = newOp;
        allSucceeded = true;
        quorumSucceeded = true;
        success = null;
        failure = null;
    }

    ExecutorService createThreadPool() {
        if (threadPool == null) {
            threadPool = Executors.newCachedThreadPool
                (new KVThreadFactory("Snapshot", null));
        }
        return threadPool;
    }

    List<Future<SnapResult>> createFutureList() {
        return new ArrayList<Future<SnapResult>>();
    }

    /**
     * Walk the list of tasks aggregating results.  Because the SnapshotTask
     * will not throw an exception it should not be possible to get an
     * exception from the Future other than InterruptedException, but it needs
     * to be handled because of the Future.get() contract.
     */
    SnapResultSummary waitForResults(
        Topology topo, List<Future<SnapResult>> taskResults) {
        success = new ArrayList<SnapResult>();
        failure = new ArrayList<SnapResult>();
        List<SnapResult> configSuccess = new ArrayList<SnapResult>();
        List<SnapResult> configFailure = new ArrayList<SnapResult>();

        for (Future<SnapResult> result : taskResults) {
            SnapResult snapResult = null;
            try {
                snapResult = result.get();
            } catch (InterruptedException e) {
                snapResult = new SnapResult(sop, false, null, e, "Interrupted");
            } catch (ExecutionException e) {
                snapResult = new SnapResult(sop, false, null, e, "Fail");
            }
            verbose("Task result (" + sop + "): " + snapResult);

            /* Update snapshot config results */
            if (snapResult.getService() instanceof StorageNodeId) {
                if (snapResult.getSucceeded()) {
                    configSuccess.add(snapResult);
                } else {
                    configFailure.add(snapResult);
                }
                continue;
            }

            if (snapResult.getSucceeded()) {
                success.add(snapResult);
            } else {
                allSucceeded = false;
                failure.add(snapResult);
            }
        }

        verbose("Operation " + sop + ", Successful nodes: " + success.size() +
                ", failed nodes: " + failure.size());

        /**
         * Perform additional analysis.
         */
        processResults(topo);
        return new SnapResultSummary(success, failure, allSucceeded,
            quorumSucceeded, configSuccess, configFailure);
    }

    void verbose(String msg) {
        if (verboseOutput) {
            message(msg);
        } else if (logger != null) {
            logger.fine(msg);
        }
    }

    private void message(String msg) {
        if (output != null) {
            output.println(msg);
        } else if (logger != null) {
            logger.info(msg);
        }
    }

    /**
     * Figure out if the operation was overall a success or failure based on
     * getting valid returns from enough nodes.  This isn't the most efficient
     * algorithm but it's simple.
     * o create a HashSet<RepGroupId> and populate it from the successes
     * o walk the topology and make sure that there is an entry in the set for
     * each RepGroup.
     * o note the failures.
     */
    private void processResults(Topology topo) {
        if (allSucceeded) {
            verbose("Operation " + sop + " succeeded for all nodes");
            quorumSucceeded = true;
            return;
        }

        Set<RepGroupId> repGroups = new HashSet<RepGroupId>();
        boolean adminOK = false;
        for (SnapResult res : success) {
            ResourceId rid = res.getService();
            if (rid != null) {
                if (rid instanceof RepNodeId) {
                    RepGroupId rgid =
                        new RepGroupId(((RepNodeId)rid).getGroupId());
                    repGroups.add(rgid);
                } else {
                    adminOK = true;
                }
            }
        }

        if (!adminOK) {
            quorumSucceeded = false;
        }
        RepGroupMap rgm = topo.getRepGroupMap();
        for (RepGroup rg : rgm.getAll()) {
            if (!repGroups.contains(rg.getResourceId())) {
                message("Operation " + sop + " did not succeed for shard " + rg);
                quorumSucceeded = false;
            } else {
                verbose("Operation " + sop + " succeeded for shard " + rg);
            }
        }
    }
}
