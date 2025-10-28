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

package oracle.kv.impl.api.ops;

import java.time.temporal.ChronoUnit;
import java.time.Clock;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import oracle.kv.MetadataNotFoundException;
import oracle.kv.PrepareQueryException;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.Result.QueryResult;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.QueryRuntimeException;
import oracle.kv.impl.query.runtime.PartitionUnionIter;
import oracle.kv.impl.query.runtime.PartitionUnionIter.PartitionedResults;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.PlanIter.PlanIterKind;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.query.runtime.server.ServerIterFactoryImpl;
import oracle.kv.impl.rep.PartitionManager;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.migration.generation.PartitionGenerationTable;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.query.ExecuteOptions;

import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.Database;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link TableQuery}.
 */
public class TableQueryHandler extends InternalOperationHandler<TableQuery> {

    public static double WAIT_ELASTICITY_MASTER_PERCENT = 0.75;
    public static double WAIT_ELASTICITY_REPLICA_PERCENT = 0.25;
    public static int WAIT_MS_FOR_ELASTICITY = 100;

    TableQueryHandler(OperationHandler handler, OpCode opCode) {
        super(handler, opCode, TableQuery.class);
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(TableQuery op) {
        /*
         * Checks the basic privilege for authentication here, and leave the
         * keyspace checking and the table access checking in
         * {@code verifyTableAccess()}.
         */
        return SystemPrivilege.usrviewPrivList;
    }

    /**
     * Returns a TableMetadataHelper instance available on this node.
     */
    private TableMetadataHelper getMetadataHelper() {

        final TableMetadata md =
            (TableMetadata) getRepNode().getMetadata(MetadataType.TABLE);

        String msg = null;
        if (md == null) {
            msg = "Query execution unable to get metadata from " +
                "RN: " + getRepNode().getRepNodeId();
        } else if (md.isEmpty()) {
            msg = "Metadata not initialised with tables at RN: " +
                getRepNode().getRepNodeId();
        }
        if (msg != null) {
            getLogger().warning(msg);

            throw new MetadataNotFoundException(msg,
                operationHandler.getTableMetadataSeqNum());
        }
        return md;
    }

    @Override
    Result execute(TableQuery op,
                   Transaction txn,
                   PartitionId partitionId) {

        TableMetadataHelper mdHelper = getMetadataHelper();
        ExecuteOptions options = new ExecuteOptions()
            .setRegionId(op.getLocalRegionId())
            .setDoTombstone(op.doTombstone())
            .setRowMetadata(op.getRowMetadata());

        /*
         * Save the ResumeInfo before execution.
         *
         * The ResumeInfo can be modified during execution, and the
         * query request might need to be forwarded to another RN to
         * re-execute in some cases(e.g. master transfer). In those
         * cases, the ResumeInfo should be reverted to its previous
         * value before execution.
         */
        ResumeInfo savedResumeInfo = new ResumeInfo(op.getResumeInfo());

        QueryResult result;
        String trace = null;

        try {
            do {
                RuntimeControlBlock rcb =
                    new RuntimeControlBlock(getLogger(), mdHelper, options, op,
                        this, new ServerIterFactoryImpl(txn, operationHandler));

                if (trace != null) {
                    rcb.setTrace(trace);
                }

                result = executeQueryPlan(op, rcb, partitionId, savedResumeInfo);

                if (result == null) {
                    trace = rcb.getTrace();
                }
            } while (result == null);

            return result;
        } catch (Throwable t) {
            /*
             * Restore the ResumeInfo to the previous value before execution, so
             * that it can be re-executed on other RN if needed.
             *
             * We don't know which error will cause the request to be forwarded,
             * just always revert the ResumeInfo, it should be harmless.
             */
            op.setResumeInfo(savedResumeInfo);
            throw t;
        }
    }

    private QueryResult executeQueryPlan(
        TableQuery op,
        RuntimeControlBlock rcb,
        PartitionId pid,
        ResumeInfo savedResumeInfo) {

        RepNode rn = getRepNode();

        TopologyManager topoManager = rn.getTopologyManager();

        ResumeInfo ri = op.getResumeInfo();
        ri.setRCB(rcb);
        ri.setCurrentPid(pid.getPartitionId());

        Topology baseTopo = rcb.getBaseTopo();
        int baseTopoNum = ri.getBaseTopoNum();

        int batchSize = op.getBatchSize();
        PlanIter queryPlan = op.getQueryPlan();
        boolean inSortPhase1 = false;
        PartitionUnionIter partUnionIter = null;
        Throwable exception = null;
        boolean gotResult = false;
        boolean more = false;
        List<FieldValueImpl> results = new ArrayList<FieldValueImpl>(batchSize);
        int[] pids = null;
        int[] numResultsPerPid = null;
        ResumeInfo[] resumeInfos = null;

        boolean isAllShardsQuery = false;
        boolean isSinglePartitionQuery = false;
        boolean isNormalScan = false;

        String batchName = null;
        if (rcb.getTraceLevel() >= 1) {
            batchName = (Clock.systemUTC().instant().truncatedTo(ChronoUnit.MILLIS) +
                         " " + rn.getRepNodeId().getFullName());
        }

        if (op.getOpCode() == OpCode.QUERY_MULTI_PARTITION) {
            if (queryPlan.getKind() == PlanIterKind.PARTITION_UNION) {
                partUnionIter = (PartitionUnionIter)queryPlan;
                inSortPhase1 = ri.isInSortPhase1();
            } else if (queryPlan.getKind() == PlanIterKind.GROUP &&
                       queryPlan.getInputIter().getKind() ==
                       PlanIterKind.PARTITION_UNION) {
                partUnionIter = (PartitionUnionIter)queryPlan.getInputIter();
                assert(!partUnionIter.doesSort());
            }
        } else if (op.getOpCode() == OpCode.QUERY_MULTI_SHARD) {
            isAllShardsQuery = true;
        } else {
            isSinglePartitionQuery = true;
        }

        try {
            queryPlan.open(rcb);

            if (inSortPhase1 && partUnionIter != null) {

                partUnionIter.next(rcb);

                PartitionedResults res = partUnionIter.getPartitionedResults(rcb);
                results = res.results;
                pids = res.pids;
                numResultsPerPid = res.numResultsPerPid;
                resumeInfos = res.resumeInfos;
                ri = rcb.getResumeInfo();

            } else {
                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("TableQueryHandler: Executing query on " +
                              pid + " with baseTopoNum " + baseTopoNum +
                              " and ResumeInfo :\n" + ri);
                    if (rcb.getTraceLevel() >= 4) {
                        rcb.trace("Batch size: " + batchSize +
                                  " timeout: " + op.getTimeout());
                        rcb.trace(queryPlan.display(true));
                    }
                }

                if (baseTopoNum >= 0) {
                    if (isAllShardsQuery) {

                        if (baseTopo == null) {
                            baseTopo = getBaseTopo(rcb, baseTopoNum);
                        }

                        isNormalScan = (ri.getVirtualScanPid() < 0);

                        if (isNormalScan) {
                            handlePartitionMigrations(rcb);
                        } else {
                            ensureMigratedPartition(rcb);
                        }
                    } else if (isSinglePartitionQuery && rcb.usesIndex()) {

                        topoManager.addQuery(rcb);

                        if (!checkPartitionIsHere(rcb, pid)) {
                            if (rcb.getTraceLevel() >= 1) {
                                rcb.trace(true,
                                          "Partition " + pid.getPartitionId() +
                                          " is not found here. " +
                                          "Throwing RNUnavailableException");
                            }
                            throw new RNUnavailableException(
                                " Partition " + pid.getPartitionId() +
                                " is not found in this RN");
                        }
                    }
                }

                while (true) {

                    gotResult = queryPlan.next(rcb);

                    if (!gotResult) {
                        break;
                    }

                    addResult(rcb, ri, queryPlan, results);

                    if (batchSize > 0 && results.size() >= batchSize) {

                        if (rcb.getTraceLevel() >= 1) {
                            rcb.trace("TableQueryHandler: query needs to " +
                                      "suspend because it has reached the " +
                                      "batch size. Num results = " +
                                      results.size());
                        }
                        rcb.setNeedToSuspend(true);
                    }
                }

                if (rcb.needToSuspend()) {
                    byte[] primResumeKey = ri.getPrimResumeKey(0);
                    byte[] secResumeKey = ri.getSecResumeKey(0);

                    if (primResumeKey != null || secResumeKey != null) {
                        more = true;
                    } else {
                        more = false;
                    }
                } else {
                    more = false;
                }

                ri.setNumResultsComputed(results.size());

                if (!more) {
                    ri.setCurrentIndexRange(0, 0);
                }

                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("TableQueryHandler: Produced a batch of " +
                              results.size() + " results on " + pid +
                              " number of KB read = " + op.getReadKB() +
                              " more results = " + more + " reachedLimit = " +
                              rcb.getReachedLimit() + " needToSuspend = " +
                              rcb.needToSuspend());

                    if (rcb.getTraceLevel() >= 2) {
                        rcb.trace("Resume Info =\n" + ri.toString());
                    }
                }
            }

        } catch (LockConflictException lce) {
            /* let the caller handle this */
            exception = lce;
            throw lce;
        } catch (QueryException qe) {
            getLogger().fine("Query execution failed: " + qe);
            /*
             * Turn this into a wrapped IllegalArgumentException so that it can
             * be passed to the client.
             */
            exception = qe;
            throw qe.getWrappedIllegalArgument();
        } catch (QueryStateException qse) {
            exception = qse;
            /* This exception indicates a bug or problem in the engine. Wrap it
             * into one that can be thrown to the client. Specifically, a
             * WrappedClientException is thrown (which is a RuntimeException) */
            getLogger().warning(qse.toString());
            qse.throwClientException();
        } catch (IllegalArgumentException e) {
            exception = e;
            throw new WrappedClientException(e);
        } catch (PrepareQueryException pqe) {
            exception = pqe;
            throw new WrappedClientException(pqe);
        } catch (RNUnavailableException rnu) {
            throw rnu;
        } catch (RuntimeException re) {
            exception = re;
            /*
             * RuntimeException should not be caught here. REs should be
             * propagated up to the request handler as many are explicitly
             * handled there. The issue is that the query code can throw REs
             * which it should catch and turn into something specific (such as
             * an IAE). Until that time this needs to remain to avoid the
             * RN restarting due to some minor query issue. The request handler
             * will check to see if the cause of the QueryRuntimeException is
             * handled. If it isn't, it will rethrow the QueryRuntimeException,
             * which the ServiceFaultHandler will handle by sending the cause
             * the client rather than restarting.
             *
             * Detect NullPointerException and log it as SEVERE. NPEs
             * should not be considered user errors.
             */
            if (re instanceof NullPointerException) {
                getLogger().severe("NullPointerException during query: " +
                                   re);
            }
            throw new QueryRuntimeException(re);
        } catch (Throwable t) {
            exception = t;
            throw t;
        } finally {

            if (exception != null &&
                rcb.getTraceLevel() >= 1 &&
                !rcb.doLogFileTracing()) {

                rcb.trace(true, "QUERY TRACE:\n" + rcb.getTrace() +
                          "\n" + CommonLoggerUtils.getStackTrace(exception));
            }

            if (baseTopoNum >= 0) {
                if (isAllShardsQuery || isSinglePartitionQuery) {
                    topoManager.removeQuery(rcb);
                }
            }

            try {
                queryPlan.close(rcb);
            } catch (RuntimeException re) {
                if (exception == null) {
                    throw new QueryRuntimeException(re);
                }
            }
        }

        boolean repeatBatch = false;

        if (baseTopoNum >= 0) {
            if (isAllShardsQuery) {

                if (isNormalScan) {
                    checkForRestoredPartitions(rcb);
                }

                repeatBatch = checkForMigratedPartitions(rcb, isNormalScan);

            } else if (isSinglePartitionQuery && rcb.usesIndex()) {

                ArrayList<PartitionId> migratedPartitions = rcb.getMigratedPartitions();
                for (PartitionId pid2 : migratedPartitions) {
                    if (pid2.getPartitionId() == pid.getPartitionId()) {
                        rcb.trace(true, "partition " + pid.getPartitionId() +
                                  " migrated during index scan. " +
                                  "Throwing RNUnavailableException");
                        throw new RNUnavailableException(
                            " Partition " + pid.getPartitionId() +
                            " migrated during index scan");
                    }
                }
            }
        }

        ri.addReadKB(op.getReadKB());

        String batchTrace = null;
        if (rcb.getTraceLevel() >= 1) {
            batchTrace = rcb.getTrace();
        }

        if (!repeatBatch) {
            return new QueryResult(getOpCode(),
                                   op.getReadKB(),
                                   op.getWriteKB(),
                                   results,
                                   op.getResultDef(),
                                   op.mayReturnNULL(),
                                   more,
                                   ri,
                                   rcb.getReachedLimit(),
                                   pids,
                                   numResultsPerPid,
                                   resumeInfos,
                                   batchName,
                                   batchTrace);
        }

        /* The batch has to aborted and repeated. So, throw away the results
         * produced during the batch. If the batch has reached the consumption
         * limit, it has to be repeated by the driver. In this case, an empty
         * result and the savedRI are sent back to the driver (via the proxy).
         * Otherwise, the batch is repeated locally. */
        results.clear();

        savedResumeInfo.addReadKB(op.getReadKB());

        if (rcb.getReachedLimit()) {

            if (rcb.getTraceLevel() >= 1) {
                rcb.trace("Repeating query batch from the proxy");
                rcb.trace(true, "Repeating query batch from the proxy");
            }

            return new QueryResult(getOpCode(),
                                   op.getReadKB(),
                                   op.getWriteKB(),
                                   results,
                                   op.getResultDef(),
                                   op.mayReturnNULL(),
                                   more,
                                   savedResumeInfo,
                                   rcb.getReachedLimit(),
                                   pids,
                                   numResultsPerPid,
                                   resumeInfos,
                                   batchName,
                                   batchTrace);
        }

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("Repeating query batch locally");
            rcb.trace(true, "Repeating query batch locally. Current trace =\n" +
                      rcb.getTrace());
        }
        op.setResumeInfo(new ResumeInfo(savedResumeInfo));
        return null;
    }

    private void addResult(
        RuntimeControlBlock rcb,
        ResumeInfo ri,
        PlanIter queryPlan,
        List<FieldValueImpl> results) {

        FieldValueImpl res = rcb.getRegVal(queryPlan.getResultReg());
        if (res.isTuple()) {
            res = ((TupleValue)res).toRecord();
        }

        if (rcb.getTraceLevel() >= 1) {
            if (ri.getCurrentPid() > 0) {
                rcb.trace("TableQueryHandler: Produced result on " +
                          "partition " + ri.getCurrentPid() + " :\n" + res);
            } else {
                rcb.trace("TableQueryHandler: Produced result :\n" + res);
            }
        }

        results.add(res);
    }

    public PartitionId[] getShardPids() {
        Set<PartitionId> pids =
            new TreeSet<PartitionId>(getRepNode().getPartitions());
        return pids.toArray(new PartitionId[pids.size()]);
    }

    public int getNumPartitions() {
        return getRepNode().getTopology().getNumPartitions();
    }

    private Topology getBaseTopo(RuntimeControlBlock rcb, int baseTopoNum) {

        RepNode rn = getRepNode();
        TopologyManager topoManager = rn.getTopologyManager();
        Topology currTopo = topoManager.getTopology();
        Topology baseTopo;

        if (currTopo.getSequenceNumber() == baseTopoNum) {
            baseTopo = currTopo;
        } else {
            try {
                baseTopo = topoManager.getTopology(
                    rn.getKVStore(), baseTopoNum, rcb.getRemainingTimeOrZero());
                if (baseTopo == null) {
                    throw new QueryStateException(
                        "Failed to read base topology with "
                        + "sequence number " + baseTopoNum
                        + ", got null result");
                }
             } catch (Throwable cause) {
                 throw new QueryStateException(
                     "Failed to read base topology with " +
                     "sequence number " + baseTopoNum,
                     cause);
             }
        }

        rcb.setBaseTopo(baseTopo);
        return baseTopo;
    }

    /*
     * This method is called for base-shard scans only, before query execution
     * starts. It does the following:
     * - registers the RCB with the MigrationManager so that the query will
     *   receive notifications about partition migrations and migration
     *   failures.
     * - finds partitions that have moved away from this shard since
     *   the last query batch, and updates the ResumeInfo (the VSM)
     *   accordingly.
     * - checks if any partitions that were already known to have moved away
     *   are found back in this shard (due to migration failure at the target
     *   shard). If any such partitions are found, an exception is thrown.
     */
    private void handlePartitionMigrations(RuntimeControlBlock rcb) {

        RepNode rn = getRepNode();
        TopologyManager topoManager = rn.getTopologyManager();
        int sid = rn.getRepNodeId().getGroupId();

        ResumeInfo ri = rcb.getResumeInfo();
        int baseTopoNum = ri.getBaseTopoNum();
        Topology baseTopo = rcb.getBaseTopo();
        List<Integer> newMigratedPartitions = new ArrayList<Integer>();
        List<Integer> targetShards = new ArrayList<Integer>();

        topoManager.addQuery(rcb);

        waitServerStateUpdated(rcb, topoManager, rn.getPartitionManager(),
                               baseTopo);

        /* Check that partitions that were found (in previous batches) to
         * have migrated out of this shard have not returned to this shard
         * (due to failures). To see why this is needed, consider the
         * following scenario:
         * Both the current and the previous batch scan the same shard. At
         * the start of the previous batch, partition P was found to have
         * migrated. In between the 2 batches, P returns to this shard. If we
         * allow the scan in this batch to proceed, we will generate duplicate
         * results when we execute the virtual-shard scan for P.
         *
         * Note: If this RN is a REPLICA, the partition may be still here due
         * to a delay in the replication of the MigrationDB. So, we wait a
         * little before throwing the exception. */
        Set<Integer> migratedPartitions = ri.getMigratedPartitions();

        if (migratedPartitions != null) {

            for (int pid : migratedPartitions) {
                PartitionId PID = new PartitionId(pid);
                boolean success = true;
                long waitTime = 0;

                while (true) {
                    Topology localTopo = topoManager.getLocalTopology();
                    RepGroupId currSidForPid = localTopo.getRepGroupId(PID);
                    RepGroupId origSidForPid = baseTopo.getRepGroupId(PID);
                    if (sid == currSidForPid.getGroupId() &&
                        sid == origSidForPid.getGroupId()) {

                        try {
                            Thread.sleep(WAIT_MS_FOR_ELASTICITY);
                            waitTime += WAIT_MS_FOR_ELASTICITY;
                            if (waitTime > (rcb.getTimeoutMs() *
                                            WAIT_ELASTICITY_REPLICA_PERCENT)) {
                                success = false;
                                break;
                            }
                        } catch (InterruptedException e) {
                        }
                    }
                    break;
                }

                if (!success) {
                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("Partition " + pid + " found back in shard " +
                                  sid + " after having moved away from it.");
                    }

                    if (rn.isMaster()) {
                        throw new QueryStateException(
                            "Partition " + pid + " found back in shard " +
                            sid + " after having moved away from it.");
                    }

                    throw new RNUnavailableException(
                        "Partition " + pid + " was supposed to have " +
                        "migrated out of shard " + sid + " but it is still here",
                        true);
                }
            }
        }

        final boolean updated = topoManager.callWithObjectLock(() -> {
            Topology currTopo = topoManager.getTopology();
            Topology localTopo = topoManager.getLocalTopology();

            if (baseTopoNum == currTopo.getSequenceNumber()
                && (localTopo.getSequenceNumber()
                    == currTopo.getSequenceNumber())
                && (!localTopo.appliedLocalizationChange())) {
                return false;
            }

            /* Find the base partitions of this shard that are not known
             * already to have migrated. Among these partitions find the ones
             * that have migrated out of this shard since the end of the
             * previous batch and update the ri.theVSM accordingly. */
            List<PartitionId> expectedPartitions = baseTopo.
                getPartitionsInShard(sid, migratedPartitions);

            for (PartitionId pid : expectedPartitions) {
                RepGroupId sidForPid = localTopo.getRepGroupId(pid);
                if (sid != sidForPid.getGroupId()) {
                    newMigratedPartitions.add(pid.getPartitionId());
                    targetShards.add(sidForPid.getGroupId());
                }
            }
            return true;
        });

        if (!updated) {
            return;
        }

        if (!newMigratedPartitions.isEmpty()) {

            int resumePid = -1;

            if (ri.getPrimResumeKey(0) != null) {
                resumePid = rn.getPartitionId(ri.getPrimResumeKey(0)).
                                    getPartitionId();
            }

            for (int i = 0; i < newMigratedPartitions.size(); ++i) {
                int pid2 = newMigratedPartitions.get(i);
                int sid2 = targetShards.get(i);
                ri.addVirtualScan(resumePid, pid2, sid2);
            }
        }
    }

    /**
     * Waits until that
     * (1) the topology T (could be either local or official) is updated to one
     * that is after the base topology;
     * (2) all partitions in T on this RN has the partition DB handle opened
     * and updated in PartitionManager;
     * (3) all partitions in T on this RN has the partition generation opened.
     */
    private void waitServerStateUpdated(
        RuntimeControlBlock rcb,
        TopologyManager topoManager,
        PartitionManager partitionManager,
        Topology baseTopo) {

        RepNode rn = getRepNode();

        Topology targetTopo = topoManager.callWithObjectLock(() -> {
                final Topology localTopo = topoManager.getLocalTopology();
                return (localTopo.getSequenceNumber() >=
                        baseTopo.getSequenceNumber() ?
                        localTopo :
                        baseTopo);
            });

        long waitTime =
            (long)(rn.isMaster() ?
                   rcb.getTimeoutMs() * WAIT_ELASTICITY_MASTER_PERCENT :
                   rcb.getTimeoutMs() * WAIT_ELASTICITY_REPLICA_PERCENT);
        Topology updated = null;
        try {
            updated = partitionManager.awaitUpdate(targetTopo, waitTime);
        } catch (InterruptedException e) {
            throw new QueryStateException(
                "Interrupted while waiting for RN state updated.");
        }

        if (updated != null) {
            final Topology t = updated;
            getLogger().fine(
                "Server updated to topology (" + t.getSequenceNumber() +
                ", " + t.getLocalizationNumber() + ")");
            return;
        }

        if (rn.isMaster()) {
            throw new QueryStateException(
                "Wait timeout at master " +
                "for server state updated past sequence number " +
                targetTopo.getSequenceNumber());
        }

        throw new RNUnavailableException(
            "Wait timeout at replica " +
            "for server state updated past sequence number " +
            targetTopo.getSequenceNumber());
    }

    /*
     * This method is called for virtual-shard scans only, before query execution
     * starts. It checks whether the partition that is supposed to have migrated
     * to this shard is indeed here. If the partition is missing, the method
     * waits for some time, and if it is still missing, it throws an exception.
     *
     * The partition may be missing either due to a delay in updating the
     * local topology or the PartitionManager, or due to migration failure.
     */
    private void ensureMigratedPartition(RuntimeControlBlock rcb) {

        RepNode rn = getRepNode();
        TopologyManager topoManager = rn.getTopologyManager();
        int sid = rn.getRepNodeId().getGroupId();

        ResumeInfo ri = rcb.getResumeInfo();
        PartitionId pid = new PartitionId(ri.getVirtualScanPid());

        topoManager.addQuery(rcb);

        if (!checkPartitionIsHere(rcb, pid)) {
            if (rcb.getTraceLevel() >= 0) {
                rcb.trace("Partition " + pid + " was supposed to have " +
                          "migrated to shard " + sid + " but is not found here");
            }
            if (rn.isMaster()) {
                throw new QueryStateException(
                    "Partition " + pid + " was supposed to have " +
                    "migrated to shard " + sid + " but is not found here");
            }
            throw new RNUnavailableException(
                "Partition " + pid + " was supposed to have " +
                "migrated to shard " + sid + " but is not found here", true);
        }
    }

    private boolean checkPartitionIsHere(
        RuntimeControlBlock rcb,
        PartitionId pid) {

        RepNode rn = getRepNode();
        PartitionManager partManager = rn.getPartitionManager();
        TopologyManager topoManager = rn.getTopologyManager();
        PartitionGenerationTable partitionGenerationTable = rn.getPartGenTable();
        int sid = rn.getRepNodeId().getGroupId();
        long waitTime = 0;

        while (true) {

            final boolean done = topoManager.callWithObjectLock(() -> {

                Topology localTopo = topoManager.getLocalTopology();
                RepGroupId sidForPid = localTopo.getRepGroupId(pid);

                if (sid == sidForPid.getGroupId()) {
                    /*
                     * Checks that the partition DB is open and the associated
                     * generation is ready. We must check the DB first, then
                     * the generation since isPartitionOpen always return true
                     * if there is no migration ever.
                     */
                    Database db = partManager.getPartitionDB(pid);
                    if (db != null &&
                        db.getEnvironment() != null &&
                        db.getEnvironment().isValid() &&
                        partitionGenerationTable.isPartitionOpen(pid)) {

                        return true;
                    }

                    if (rcb.getTraceLevel() >= 2) {
                        rcb.trace("Partition " + pid.getPartitionId() +
                                  " not found in PartitionManager. " +
                                  "Putting query to sleep");
                    }
                } else if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Partition " + pid.getPartitionId() +
                              " not found in local topology. " +
                              "Putting query to sleep");
                }
                return false;
            });

            if (done) {
                return true;
            }

            try {
                Thread.sleep(WAIT_MS_FOR_ELASTICITY);
                waitTime += WAIT_MS_FOR_ELASTICITY;
                if (rn.isMaster()) {
                    if (waitTime >
                        (rcb.getTimeoutMs() * WAIT_ELASTICITY_MASTER_PERCENT)) {
                        break;
                    }
                } else if (waitTime >
                           (rcb.getTimeoutMs() * WAIT_ELASTICITY_REPLICA_PERCENT)) {
                    break;
                }
            } catch (InterruptedException e) {
            }
            continue;
        }

        return false;
    }

    /*
     * Method called at the end of a batch to check if any partitions moved
     * out of this shard during the batch. If this is normal shard scan, the
     * method returns true, and as a result, the batch will be aborted and
     * then repeated. If this is a virtual shard scan, an exception is thrown
     * (this can happen if a query span more than one elasticity ops, which
     * is not supported by the query-elasticity algorithm)
     */
    private boolean checkForMigratedPartitions(
        RuntimeControlBlock rcb,
        boolean isNormalScan) {

        int sid = getRepNode().getRepNodeId().getGroupId();
        Topology baseTopo = rcb.getBaseTopo();
        ArrayList<PartitionId> migratedPartitions = rcb.getMigratedPartitions();

        if (isNormalScan) {
            for (PartitionId pid : migratedPartitions) {
                if (baseTopo.getRepGroupId(pid).getGroupId() == sid) {
                    return true;
                }
            }
        } else {
            int pid2 = rcb.getResumeInfo().getVirtualScanPid();
            for (PartitionId pid : migratedPartitions) {
                if (pid2 == pid.getPartitionId()) {
                    QueryStateException qse = new QueryStateException(
                        "Partition " + pid.getPartitionId() +
                        " migrated again out of its target shard " + sid);
                    getLogger().info(qse.toString());
                    qse.throwClientException();
                }
            }
        }

        return false;
    }

    /*
     * Method called at the end of a batch to check if a partition that
     * moved out of this shard was returned to this shard during the batch,
     * because of a failure at the target shard. If so, an exception is thrown.
     *
     * The method is called for normal shard scans only. For virtual-shard
     * scans it is not needed: Before the scan starts, we check that partition
     * is here. So, for the partition to be restored, it must have moved out
     * first. But the move-out is also registered during the scan, and an
     * exception will be thrown after the end of the scan in this case (
     * in method checkForMigratedPartitions() above).
     */
    private void checkForRestoredPartitions(RuntimeControlBlock rcb) {

        int sid = getRepNode().getRepNodeId().getGroupId();
        Topology baseTopo = rcb.getBaseTopo();
        ArrayList<PartitionId> restoredPartitions = rcb.getRestoredPartitions();

        if (restoredPartitions.isEmpty()) {
            return;
        }

        for (PartitionId pid : restoredPartitions) {
            if (baseTopo.getRepGroupId(pid).getGroupId() == sid) {
                QueryStateException qse = new QueryStateException(
                        "Partition " + pid.getPartitionId() +
                        "found back in shard " + sid +
                        " after having moved away from it.");
                getLogger().info(qse.toString());
                qse.throwClientException();
            }
        }
    }
}
