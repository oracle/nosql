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

package oracle.kv.impl.query.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.topo.PartitionId;

/*
 * PartitionUnionIter is used for ALL_PARTITION queries in the cloud. For
 * such queries, the PartitionUnionIter is always the root iterator of the
 * server-side query plan. The algorithm used to execute such queries is
 * the following:
 *
 * A. Simple queries (no order-by, group-by)
 *
 * 1. Assume batch i starts execution at the driver.
 *
 * 2. The driver sends a request to the proxy. The continuation key (CKi) of
 * this request specifies (a) a partition Pi to be processed at some RN, (b)
 * the resume key RKi for Pi, and (c) a bitmap BMi with one bit per partition.
 * A bit is ON iff the corresponding partition was fully processed (all the
 * results from that partition were produced) during a previous batch (or
 * batches). Such a partition is said to be "done" (it will not be visited
 * again in this query). If i = 1 (the very first batch), Pi = 1, RKi is
 * null, and the bitmap has all its bits OFF.
 *
 * 3. The proxy (via its local RCV iter) forwards the request to an RN
 * containing Pi.
 *
 * 4. At the RN, the root PlanIter is a PartitionUnionIter. CKi is
 * represented by a ResumeInfo. ResumeInfo.theCurrentPid and
 * ResumeInfo.thePartitionsBitmap store Pi and BMi, respectively.
 *
 * 5. The PartitionUnionIter starts by asking the local topology for the ids
 * of all the local partitions. It stores this in theReqIds field.
 *
 * 6. Then, it starts executing the query sequentially on each not-done local
 * partition. The 1st partition to be processed in this step is Pi. Every
 * time a partition is fully processed, it is marked as done in the BMi
 * before moving on to the next partition. This step continues until the
 * batch limit is reached or all results from all local not-done partitions
 * are produced.
 *
 * 7. A new continuation key, CK(i+1), is constructed to be used for the next
 * batch. If the batch limit was reached during step 6, P(i+1) is set to the
 * partition that was being processed when the limit was reached and RK(i+1)
 * is set to the resume key of that partition. Otherwise P(i+1) is set to -1.
 * In both cases, BM(i+1) is BMi with the updates (if any) done to it during
 * step 6.
 *
 * 8. CK(i+1) and any results produced during step 6 are sent back to the proxy.
 *
 * 9. If the batch limit was reached during step 6, the proxy simply sends
 * CK(i+1) and the results back to driver, which returns them to the app.
 *
 * 10. If the batch limit was not reached, the proxy looks at BM(i+1) for a
 * not-done partition and sends a request to an RN that hosts that partition.
 * The batch limit for this request is set to the remaining limit. At the
 * RN this request is processed the same way as in steps 4-8.
 *
 * 11. Step 10 repeats until the full batch limit is consumed or all query
 * results have been produced (i.e., all bits in BM(i+1) are ON). The
 * proxy accumulates the query results produced by each proxy-RN request.
 * When done, it sends these results together with CK(i+1) to the driver.
 *
 * B. Sorting/grouping queries
 *
 * Sorting queries are executed in 2 phases. In phase 1, we make sure that the
 * driver caches at least one result per partition, except from partitions that
 * do not contain any results at all. During this phase, a request sent by the
 * driver to the store may access more than one partitions (as explained in
 * more detail below). Let R be the set of partitions that have at least one
 * result. At the end of phase 1, the driver will have a result queue and a
 * continuation key per R partition. In phase 2, the query driver merge-sorts
 * the locally cached partition results. When all cached results from some
 * partition are consumed, the driver requests more results from that partition
 * only (only a single partition is accessed during a phase 2 batch).
 *
 * A phase-1 batch is processed as follows:
 *
 * 1. The driver sends a request to the proxy. The continuation key (CKi) of
 * this request specifies (a) a flag PH1i telling whether the query is in
 * phase 1, (b) a partition Pi to be processed at some RN, (c) the resume key
 * RKi for Pi, and (d) a bitmap BMi with one bit per partition. A bit is ON iff
 * the corresponding partition is "done". In this case, "done" means that during
 * a previous batch, at least one result was generated from the partition or it
 * was found that the partition does not have any results at all. A "done"
 * partition will not be visited again during phase 1. If i = 1, phase 1 is
 * true, Pi = 1, RKi is null, and the bitmap has all its bits OFF.
 *
 * 2. The proxy forwards the request to an RN containing Pi.
 *
 * 3. At the RN, the query processor uses the local topology info and the BMi
 * to construct a set S of local, not-done partitions. It also constructs a
 * CKij for each S partition Pij, except Pi (whose CK is CKi). In each CKij,
 * the partition id is Pij, RKij is null, BMij is BMi, and PH1ij is true.
 *
 * 4. Then, it starts executing the query in a round-robin fashion on each
 * partition in S. The 1st partition to be processed in this step is Pi. Every
 * time a result is produced from a partition Pij, or Pij is found to have no
 * more results, Pij is marked as done in the BMi, before moving on to the next
 * partition. Furthermore, if Pij has no more results, CKij is set to null and
 * Pij is removed from S. This step continues until the batch limit is reached
 * or all results from all S partitions are produced.
 *
 * 5. A new continuation key, CK(i+1), is constructed to be used for the next
 * batch. Two cases are distinguished (a) If the batch limit was reached before
 * all S partitions were visited at least once during step 4, CK(i+1) is set to
 * the CKij of the partition that was being processed when the limit was reached.
 * (b) Otherwise P(i+1) is set to -1, RK(i+1) is set to null, and PH1(i+1) is
 * set to false. In both cases, BM(i+1) is BMi with the updates (if any) done to
 * it during step 4.
 *
 * 6. CK(i+1), all the CKij's, and any results produced during step 4 are sent
 * back to the proxy. The results are grouped per partition.
 *
 * 7. If case (5a) is true, the proxy simply sends CK(i+1), the CKij's and the
 * results to the driver.
 *
 * 8. If case (5b) is true, the proxy must determine if phase 1 is finished and
 * if not what P(i+1) should be. It does so by looking into BM(i+1) for a
 * partition that is not done yet. If one is found, P(i+1) is set to that
 * partition and PH1(i+1) is set to true. Otherwise, P(i+1) is left to -1 and
 * PH1(i+1) to false. Then, the proxy sends sends CK(i+1), the CKij's and the
 * results to the driver. Notice that in this case it may be that the batch
 * limit was not reached at the RN. However, for simplicity, the proxy will not
 * attempt to send another request to some RN.
 *
 * 9. The driver caches the received results and CKij's for the associated
 * partitions. If PH1(i+1) is true, the driver will initiate the next phse-1
 * batch using CK(i+1). Otherwise, it enters phase 2.
 *
 *
 * Handling topology changes:
 *
 * What happens if a partition moves to another shard while it is being
 * processed?. In this case, an ISE will be raised (because the partition DB
 * will be closed). The ISE will be caught and handled by the RequestHandler.
 * There are 2 cases: (a) The partition is the original partition Pi (the one
 * specified by the Request.partitionId field). In this case, the RequestHandler
 * will forward the Request to the RN that now stores partition Pi, where it will
 * be re-executed from the beginning. (b) The partition is not the original one.
 * The RequestHandler will again try to forward the Request to an RN that stores
 * the partition specified by Request.partitionId. The PartitionUnionIter cannot
 * change Request.partitionId, so it will still "point" to the original partition
 * Pi, not the current one. Unless Pi has also moved, RequestHandler will handle
 * this by throwing an RNUnavailableException, which will be caught by the
 * RequestDispatcher at the proxy. The RequestDispatcher will then retry the
 * Request from the beginning.
 */
public class PartitionUnionIter extends PlanIter {

    public static class PartitionedResults {

        public List<FieldValueImpl> results;
        public int[] pids;
        public int[] numResultsPerPid;
        public ResumeInfo[] resumeInfos;
    }

    public static class PartitionUnionState extends PlanIterState {

        /*
         * Pi : the id of the partition that was initally requested.
         */
        int theInitPid;

        /*
         * The id of the partition that is being processed currently
         */
        int thePid;

        /*
         * The pids of all partitions stored in this RN.
         */
        PartitionId[] theRepPids;

        /*
         * If the query is in sorting phase 1, it stores a ResumeInfo for each
         * of the local partitions that is not "done".
         */
        ResumeInfo[] theResumeInfos;

        /*
         * If the query is in sorting phase 1, it stores the results produced
         * for each of the local partitions.
         */
        List<FieldValueImpl>[] theResults;

        /*
         * Total number of results in theResults
         */
        int theNumResults;

        /*
         * If the query is in sorting phase 1, it stores the number of local
         * partitions that produced at least one result.
         */
        int theNumPartitionsWithResults;

        /*
         * Index into theRepPids, theResumeInfos, and theResults.
         */
        int thePidIdx;

        public PartitionUnionState(
            RuntimeControlBlock rcb,
            PartitionUnionIter iter) {

            super();
            ResumeInfo ri = rcb.getResumeInfo();
            theInitPid = ri.getCurrentPid();
            thePid = theInitPid;
            thePidIdx = -1;
            theRepPids = rcb.getQueryHandler().getShardPids();

            if (iter.theDoesSort) {
                if (ri.isInSortPhase1()) {
                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("PartitionUnionIter in sorting phase 1");
                    }
                } else {
                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("PartitionUnionIter in sorting phase 2");
                    }
                }
            }

            if (rcb.getTraceLevel() >= 3) {
                StringBuffer sb = new StringBuffer();
                sb.append("local partitions = {");
                for (PartitionId pid : theRepPids) {
                    sb.append(pid).append(", ");
                }
                sb.append("}");
                rcb.trace(sb.toString());
            }

            if (rcb.getTraceLevel() >= 1) {
                rcb.trace("PartitionUnionIter: Executing query on initial " +
                          "partition " + theInitPid);
            }
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            thePid = theInitPid;
            thePidIdx = -1;
        }
    }

    private final PlanIter theInputIter;

    private final boolean theDoesSort;

    public PartitionUnionIter(Expr e, int resultReg, PlanIter input) {
        super(e, resultReg);
        theInputIter = input;
        theDoesSort = e.getQCB().hasSort();
    }

    PartitionUnionIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInputIter = deserializeIter(in, serialVersion);
        theDoesSort = in.readBoolean();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theInputIter, out, serialVersion);
        out.writeBoolean(theDoesSort);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.PARTITION_UNION;
    }

    public boolean doesSort() {
        return theDoesSort;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PartitionUnionState(rcb, this));
        theInputIter.open(rcb);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInputIter.close(rcb);
        state.close();
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
        theInputIter.reset(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PartitionUnionState state =
            (PartitionUnionState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (theDoesSort) {
            return sortingNext(rcb, state);
        }

        return simpleNext(rcb, state);
    }

    /*
     * Note: If in sort phase 1, sortingNext() will be called exactly once
     * during the liftime of this PartitionUnionIter. This invocation will
     * compute all results for the current batch.
     */
    @SuppressWarnings("unchecked")
    public boolean sortingNext(
        RuntimeControlBlock rcb,
        PartitionUnionState state) {

        ResumeInfo initRI =  rcb.getResumeInfo();

        if (!initRI.isInSortPhase1()) {

            boolean more = theInputIter.next(rcb);

            if (!more) {
                state.done();
                return false;
            }

            return true;
        }

        BitSet partitionsBitmap = initRI.getPartitionsBitmap();

        int batchSize = rcb.getBatchSize();
        int pid;
        ResumeInfo ri;
        int numPartitions = rcb.getQueryHandler().getNumPartitions();
        int numDonePartitions = 0;
        boolean[] donePartitions = new boolean[state.theRepPids.length];
        state.theResumeInfos = new ResumeInfo[state.theRepPids.length];
        @SuppressWarnings({"rawtypes", "all"})
        final List<FieldValueImpl>[] results =
            new ArrayList[state.theRepPids.length];
         state.theResults = results;

        /*
         * For each local partition: if during a previous batch we got a result
         * from it or it was found to have no results at all, mark it as "done",
         * so that it will not be processed again in the current batch.
         *
         * Also, make thePidIdx point to the slot that contains theInitPid, so
         * that processing of this batch starts with theInitPid partition.
         *
         * Note: If theInitPid partition has moved, throw ISE so that the whole
         * Request is forwarded to an RN that now hosts that partition.
         */
        for (int i = 0; i < state.theRepPids.length; ++i) {

            pid = state.theRepPids[i].getPartitionId();

            if (pid == state.theInitPid) {
                assert(!partitionsBitmap.get(pid));
                state.thePidIdx = i;
                state.theResumeInfos[i] = initRI;
                donePartitions[i] = false;
            } else {
                if (partitionsBitmap.get(pid)) {
                    ++numDonePartitions;
                    donePartitions[i] = true;
                } else {
                    donePartitions[i] = false;
                }
                state.theResumeInfos[i] = null;
            }
        }

        if (state.thePidIdx < 0) {
            throw new IllegalStateException(
                "Partition " + state.theInitPid + " has moved");
        }

        pid = state.theInitPid;
        ri = initRI;

        /*
         * Run the query on each not-done local partition, in a round-robin
         * fashion, until the batch limit is reached or all results from all
         * local partitions have been produced.
         */
        while(true) {

            if (rcb.getTraceLevel() >= 1) {
                rcb.trace("PartitionUnionIter in sort phase 1: Executing " +
                          "query on partition " + pid);
                rcb.trace("Resume Info " + ri.hashCode() + " :\n" + ri);
            }

            if (initRI.getTotalReadKB() == 0 &&
                rcb.getQueryOp().getReadKB() == 0 &&
                partitionsBitmap.cardinality() == numPartitions - 1) {

                rcb.getQueryOp().setEmptyReadFactor(1);

                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("PartitionUnionIter: set empty read factor to 1");
                }
            }

            boolean more = theInputIter.next(rcb);

            if (more) {

                FieldValueImpl res;

                if (state.theResults[state.thePidIdx] == null) {
                    ++state.theNumPartitionsWithResults;
                    state.theResults[state.thePidIdx] =
                        new ArrayList<FieldValueImpl>();
                }

                res = addResult(rcb, theInputIter,
                                state.theResults[state.thePidIdx]);

                ++state.theNumResults;
                if (!partitionsBitmap.get(pid)) {
                    ++numDonePartitions;
                    partitionsBitmap.set(pid);
                }

                ri.setIsInSortPhase1(false);
                ri.incNumResultsComputed();

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("PartitionUnionIter in sort phase 1: " +
                              "Produced result from partition " + pid +
                              " :\n" + res);
                }

                if (ri.getPrimResumeKey(0) == null) {
                    state.theResumeInfos[state.thePidIdx] = null;
                    donePartitions[state.thePidIdx] = true;
                }
            } else {
                if (!rcb.getReachedLimit()) {

                    if (rcb.getTraceLevel() >= 2) {
                        rcb.trace("PartitionUnionIter in sort phase 1: " +
                                  "no more results from partition " + pid);
                    }

                    if (!partitionsBitmap.get(pid)) {
                        ++numDonePartitions;
                        partitionsBitmap.set(pid);
                    }
                    state.theResumeInfos[state.thePidIdx] = null;
                    donePartitions[state.thePidIdx] = true;
                }
            }

            if (ri.getGBTuple() != null) {
                boolean needToSuspend = rcb.needToSuspend();
                rcb.setNeedToSuspend(true);
                boolean more1 = theInputIter.next(rcb);
                assert(ri.getGBTuple() == null);
                if (!needToSuspend) {
                    rcb.setNeedToSuspend(false);
                }

                if (more1) {
                    if (ri.getPrimResumeKey(0) == null) {
                        ri.incNumResultsComputed();
                    }

                    FieldValueImpl res =
                        addResult(rcb, theInputIter,
                                  state.theResults[state.thePidIdx]);

                    ++state.theNumResults;

                    if (rcb.getTraceLevel() >= 2) {
                        rcb.trace("PartitionUnionIter in sort phase 1: " +
                                  "Produced group result from partition " + pid +
                                  " :\n" + res);
                        rcb.trace("PartitionUnionIter in sort phase 1: " +
                                  "reached limit = " + rcb.getReachedLimit());
                    }
                }

                if (ri.getPrimResumeKey(0) == null) {
                    state.theResumeInfos[state.thePidIdx] = null;
                    donePartitions[state.thePidIdx] = true;
                }
            }

            /*
             * Stop fetching records when one of the following conditions is
             * met:
             *   - Reached the size or batch limit
             *   - For inner join queries, all local partitions have been
             *     processed. This is because the theInputIter.reset(rcb)
             *     before fetch from next partition will clear the resume
             *     keys of inner tables.
             *
             * If all local partitions are done, create an empty ResumeInfo
             * object to signal the end of sort phase 1.
             * The proxy will then use the partitions bitmap in ResumeInfo to
             * locate the next partition to process.
             */
            boolean allPartitionsDone =
                (numDonePartitions == state.theRepPids.length);
            if (rcb.getReachedLimit() ||
                (batchSize > 0 &&
                 state.theNumResults >= batchSize &&
                 !rcb.cannotSuspend())  ||
                 (allPartitionsDone &&
                  ri.numTables() > 1 &&
                  !rcb.cannotSuspend())) {

                if (allPartitionsDone) {
                    ri = new ResumeInfo(rcb);
                    rcb.setResumeInfo(ri);
                    ri.setPartitionsBitmap(partitionsBitmap);
                    ri.setIsInSortPhase1(false);
                }

                state.done();
                return true;
            }

            if (batchSize > 0 && state.theNumResults >= batchSize) {
                rcb.setNeedToSuspend(true);
            }

            if (more && rcb.cannotSuspend()) {
                continue;
            }

            /*
             * Move to the next local partition, skipping any partitions that
             * do no have any more results.
             */

            rcb.resetCannotSuspend();
            rcb.setNeedToSuspend(false);

            int mark = state.thePidIdx;

            while (true) {
                ++state.thePidIdx;
                if (state.thePidIdx == state.theRepPids.length) {
                    state.thePidIdx = 0;
                }

                /*
                 * If partition has no more results or it was processed during
                 * an earlier batch, skip it.
                 */
                if (donePartitions[state.thePidIdx]) {

                    if (rcb.getTraceLevel() >= 3) {
                        rcb.trace("PartitionUnionIter in sort phase 1: " +
                                  "skipping done partition " +
                                  state.theRepPids[state.thePidIdx].getPartitionId());
                    }

                    /*
                     * If none of the local partitions has any more results,
                     * create an empty ResumeInfo to be returned to the proxy.
                     * The proxy will use the partitions bitmap in this
                     * ResumeInfo to find the next partition to be processed.
                     */
                    if (state.thePidIdx == mark) {

                        if (rcb.getTraceLevel() >= 1) {
                            rcb.trace("PartitionUnionIter in sort phase 1: " +
                                      "no more results from local partitions");
                        }

                        ri = new ResumeInfo(rcb);
                        rcb.setResumeInfo(ri);
                        ri.setPartitionsBitmap(partitionsBitmap);
                        ri.setIsInSortPhase1(false);
                        state.done();
                        return true;
                    }

                    continue;
                }

                /* Prepare the query plan for execution on the chosen partition */
                pid = state.theRepPids[state.thePidIdx].getPartitionId();
                ri = state.theResumeInfos[state.thePidIdx];
                if (ri == null) {
                    ri = new ResumeInfo(rcb);
                    ri.setCurrentPid(pid);
                    ri.setPartitionsBitmap(partitionsBitmap);
                    ri.setIsInSortPhase1(true);
                    state.theResumeInfos[state.thePidIdx] = ri;
                }
                rcb.setResumeInfo(ri);
                theInputIter.reset(rcb);

                break;
            }
        }
    }

    private FieldValueImpl addResult(RuntimeControlBlock rcb,
                                     PlanIter queryPlan,
                                     List<FieldValueImpl> results) {

        FieldValueImpl res = rcb.getRegVal(queryPlan.getResultReg());
        if (res.isTuple()) {
            res = ((TupleValue)res).toRecord();
        }
        results.add(res);
        return res;
    }

    public PartitionedResults getPartitionedResults(RuntimeControlBlock rcb) {

        PartitionUnionState state =
            (PartitionUnionState)rcb.getState(theStatePos);

        PartitionedResults res = new PartitionedResults();

        res.pids = new int[state.theNumPartitionsWithResults];
        res.numResultsPerPid = new int[state.theNumPartitionsWithResults];
        res.resumeInfos = new ResumeInfo[state.theNumPartitionsWithResults];
        res.results = new ArrayList<FieldValueImpl>(state.theNumResults);
        int p = 0;

        for (int i = 0; i < state.theRepPids.length; ++i) {

            if (state.theResults[i] == null) {
                continue;
            }

            res.pids[p] = state.theRepPids[i].getPartitionId();
            res.numResultsPerPid[p] = state.theResults[i].size();
            res.resumeInfos[p] = state.theResumeInfos[i];

            for (int j = 0; j < state.theResults[i].size(); ++j) {
                res.results.add(state.theResults[i].get(j));
            }

            ++p;
        }

        if (rcb.getTraceLevel() >= 1) {
            StringBuffer sb = new StringBuffer();
            sb.append("PartitionUnionIter: Produced a batch of " +
                      res.results.size() + " results during sort phase 1 " +
                      " number of KB read = " + rcb.getQueryOp().getReadKB());
            sb.append("\n[pid, num results] = { ");
            for (int i = 0; i < state.theNumPartitionsWithResults; ++i) {
                sb.append("[").append(res.pids[i]).append(", ").
                append(res.numResultsPerPid[i]).append("] ");
            }
            sb.append("}");

            rcb.trace(sb.toString());

            rcb.trace("Global ResumeInfo =\n" + rcb.getResumeInfo().toString());
        }

        return res;
    }

    public boolean simpleNext(
        RuntimeControlBlock rcb,
        PartitionUnionState state) {

        if (rcb.needToSuspend() && !rcb.cannotSuspend()) {
            state.done();
            return false;
        }

        ResumeInfo ri =  rcb.getResumeInfo();

        while(true) {

            boolean more = theInputIter.next(rcb);

            if (!more) {

                if (rcb.needToSuspend()) {
                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("PartitionUnionIter: next() returning false " +
                                  "because batch limit was reached");
                    }
                    state.done();
                    return false;
                }

                moveToNextPartition(rcb, state);

                if (state.isDone()) {
                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("PartitionUnionIter: next() returning false " +
                                  "because all partitions were processed");
                    }
                    return false;
                }

                continue;
            }

            /*
             * We have a result. If this is the last result from the current
             * partition, mark the current partition as done and move to the
             * next one, if any.
             */
            if (ri.getPrimResumeKey(0) == null) {
                moveToNextPartition(rcb, state);
            }

            return true;
        }
    }

    private void moveToNextPartition(
        RuntimeControlBlock rcb,
        PartitionUnionState state) {

        ResumeInfo ri =  rcb.getResumeInfo();
        BitSet partitionsBitmap = ri.getPartitionsBitmap();
        partitionsBitmap.set(state.thePid);

        while (true) {
            ++state.thePidIdx;

            if (state.thePidIdx == state.theRepPids.length) {
                state.done();
                ri.setCurrentPid(-1);
                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("PartitionUnionIter: no more partitions to process");
                }
                return;
            }

            int pid = state.theRepPids[state.thePidIdx].getPartitionId();

            if (partitionsBitmap.get(pid)) {
                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("PartitionUnionIter: skipped partition " + pid);
                }
                continue;
            }

            state.thePid = pid;
            ri.setCurrentPid(pid);

            theInputIter.reset(rcb);
            rcb.getResumeInfo().reset();

            if (rcb.getTraceLevel() >= 1) {
                rcb.trace("PartitionUnionIter: Executing query on partition " + pid);
                rcb.trace("KBs read = " + rcb.getQueryOp().getReadKB());
            }

            break;
        }

        return;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"sorting\" : ").append(theDoesSort).append(",\n");

        displayInputIter(sb, formatter, verbose, theInputIter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof PartitionUnionIter)) {
            return false;
        }
        final PartitionUnionIter other = (PartitionUnionIter) obj;
        return Objects.equals(theInputIter, other.theInputIter) &&
            (theDoesSort == other.theDoesSort);
    }
}
