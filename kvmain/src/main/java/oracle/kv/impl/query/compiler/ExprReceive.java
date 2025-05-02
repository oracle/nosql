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

package oracle.kv.impl.query.compiler;


import java.util.ArrayList;

import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.types.ExprType;

/**
 * This expr is used to mark the boundaries between parts of the query that
 * execute on different "machines". The receive expr itself executes at a
 * "client machine" and its child subplan executes at a "server machine".
 * The child subplan may actually be replicated on several server machines,
 * in which case the receive expr acts as a UNION ALL expr, collecting and
 * propagating the results it receives from its children. Furthermore, the
 * receive expr may perform a merge-sort over its inputs (if the inputs
 * return sorted results).
 *
 * Receive exprs are always created as parents of the BaseTable exprs in the
 * exprs graph, After their creation, Receive exprs are pulled-up as far as
 * they can go. All this is done by the Distributer class.
 *
 * theInput:
 * The expr producing the input to this receive expr.
 *
 * theShardKey:
 * For single-partition queries, it stores the value of the shard key that is
 * specified in the WHERE clause. Notice that the value of a shard-key column
 * may not be known at compile time, because it is specified by an external
 * variable (or more genrally, an expression involing external variables). In
 * this case, theShardKey will store a dummy value for such a column, and
 * theShardKeyExternals will store the expression that will be evaluated
 * during runtime (see ReceiveIter.open()) to compute the actual value for
 * the shard-key column.
 *
 * theShardKeyExternals:
 * If the query specified a shard key, then for each shard-key column it stores
 * either null or an expression that will be evaluated during runtime to compute
 * the value of the shard-key column. Null is stored for shard-key columns who
 * value is known at compile time (i.e. it is specified by a literal value).
 * 
 * theEliminateIndexDups:
 * Whether or not to eliminate index dups. These are duplicate results that
 * may be generated during the scan of a multikey (array/map) index.
 *
 * thePrimKeyPositions:
 * The positions of the primary key columns in the RecordValues received
 * from the servers. This is non-null only if the ReceiveIter must do
 * elimination of index duplicates.
 */
public class ExprReceive extends Expr {

    private Expr theInput;

    private int[] theSortFieldPositions;

    private SortSpec[] theSortSpecs;

    private DistributionKind theDistributionKind;

    private PrimaryKeyImpl theShardKey;

    private int thePid = -1;

    private int theMinPid = -1;

    private int theMaxPid = -1;

    private int theMinSid = -1;

    private int theMaxSid = -1;

    private ArrayList<Expr> thePartitionsBindExprs;

    private ArrayList<Expr> theShardsBindExprs;

    private ArrayList<Expr> theShardKeyExternals;

    private boolean theEliminateIndexDups;

    private ArrayList<Integer> thePrimKeyPositions;

    private boolean theIsUpdate;

    private ArrayList<ExprSFW> theJoinBranches;

    public ExprReceive(QueryControlBlock qcb, StaticContext sctx) {
        super(qcb, sctx, ExprKind.RECEIVE, null);
        qcb.setHasReceiveIter();
    }

    @Override
    int getNumChildren() {
        return 1;
    }

    @Override
    Expr getInput() {
        return theInput;
    }

    public void setInput(Expr newExpr, boolean destroy) {

        newExpr.addParent(this);
        if (theInput != null) {
            theInput.removeParent(this, destroy);
        }
        theInput = newExpr;
        theType = computeType();
        computeDistributionKind();
        setLocation(newExpr.getLocation());
    }

    private void computeDistributionKind() {

        if (theInput.getKind() != ExprKind.BASE_TABLE) {
            return;
        }

        ExprBaseTable tableExpr = (ExprBaseTable)theInput;

        IndexImpl index = tableExpr.getIndex();

        if (tableExpr.isSinglePartition()) {

            theDistributionKind = DistributionKind.SINGLE_PARTITION;

            theShardKey = tableExpr.getShardKey();
            thePid = tableExpr.getPartitionId();

            if (tableExpr.getShardKeyBindExprs() != null) {
                theShardKeyExternals =
                    new ArrayList<Expr>(tableExpr.getShardKeyBindExprs());
            }

            theMinSid = tableExpr.getMinShard();
            theMaxSid = tableExpr.getMaxShard();
            theShardsBindExprs = tableExpr.getShardIdBindExprs();
            thePartitionsBindExprs = tableExpr.getPartitionIdBindExprs();

        } else if (index != null) {
            theDistributionKind = DistributionKind.ALL_SHARDS;
            theMinSid = tableExpr.getMinShard();
            theMaxSid = tableExpr.getMaxShard();
            theShardsBindExprs = tableExpr.getShardIdBindExprs();
        } else {
            theDistributionKind = DistributionKind.ALL_PARTITIONS;
            theMinPid = tableExpr.getMinPartition();
            theMaxPid = tableExpr.getMaxPartition();
            theMinSid = tableExpr.getMinShard();
            theMaxSid = tableExpr.getMaxShard();
            thePartitionsBindExprs = tableExpr.getPartitionIdBindExprs();
            theShardsBindExprs = tableExpr.getShardIdBindExprs();
        }
    }

    DistributionKind getDistributionKind() {
        return theDistributionKind;
    }

    void setDistributionKind(DistributionKind v) {
        theDistributionKind = v;
    }

    PrimaryKeyImpl getShardKey() {
        return theShardKey;
    }

    void copySinglePartitionInfo(ExprReceive rcv) {
        theShardKey = rcv.theShardKey;
        theShardKeyExternals = rcv.theShardKeyExternals;
        thePid = rcv.thePid;
        thePartitionsBindExprs = rcv.thePartitionsBindExprs;
    }

    ArrayList<Expr> getShardKeyExternals() {
        return theShardKeyExternals;
    }

    public int getPartitionId() {
        return thePid;
    }

    public int getMinPartition() {
        return theMinPid;
    }

    public int getMaxPartition() {
        return theMaxPid;
    }

    ArrayList<Expr> getPartitionsBindExprs() {
        return thePartitionsBindExprs;
    }

    public int getMinShard() {
        return theMinSid;
    }

    public int getMaxShard() {
        return theMaxSid;
    }

    ArrayList<Expr> getShardsBindExprs() {
        return theShardsBindExprs;
    }

    void addSort(int[] sortExprPositions, SortSpec[] specs) {
        theSortFieldPositions = sortExprPositions;
        theSortSpecs = specs;
        theType = computeType();
    }

    void reverseSortDirection() {

        if (theSortSpecs == null) {
            throw new QueryStateException("Unexpected call");
        }

        for (SortSpec spec : theSortSpecs) {
            spec.theIsDesc = true;
            spec.theNullsFirst = true;
        }
    }

    int[] getSortFieldPositions() {
        return theSortFieldPositions;
    }

    SortSpec[] getSortSpecs() {
        return theSortSpecs;
    }

    void setEliminateIndexDups(boolean v) {
        theEliminateIndexDups = v;
    }

    boolean getEliminateIndexDups() {
        return theEliminateIndexDups;
    }

    void setIsUpdate(boolean v) {
        theIsUpdate = v;
    }

    boolean getIsUpdate() {
        return theIsUpdate;
    }

    void addPrimKeyPositions(int[] positions) {

        if (thePrimKeyPositions == null) {
            thePrimKeyPositions = new ArrayList<Integer>(positions.length);
            for (int i = 0; i < positions.length; ++i) {
                thePrimKeyPositions.add(positions[i]);
            }
        } else {
            for (int i = 0; i < positions.length; ++i) {
                if (!thePrimKeyPositions.contains(positions[i])) {
                    thePrimKeyPositions.add(positions[i]);
                }
            }
        }

        theType = computeType();
    }

    int[] getPrimKeyPositions() {
        if (thePrimKeyPositions == null) {
            return null;
        }
        int[] res = new int[thePrimKeyPositions.size()];
        for (int i = 0; i < thePrimKeyPositions.size(); ++i) {
            res[i] = thePrimKeyPositions.get(i);
        }
        return res;
    }

    void addJoinBranch(ExprSFW branch) {
        if (theJoinBranches == null) {
            theJoinBranches = new ArrayList<>();
        }
        theJoinBranches.add(branch);
    }

    ArrayList<ExprSFW> getJoinBranches() {
        return theJoinBranches;
    }
 
    @Override
    ExprType computeType() {
        return theInput.getType();
    }

    @Override
    public boolean mayReturnNULL() {
        return theInput.mayReturnNULL();
    }

    @Override
    boolean mayReturnEmpty() {
        return true;
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        formatter.indent(sb);
        sb.append("DistributionKind : ").append(theDistributionKind);
        sb.append(",\n");
        if (theShardKey != null) {
            formatter.indent(sb);
            sb.append("PrimaryKey :").append(theShardKey);
            sb.append(",\n");
        }
        if (theSortFieldPositions != null) {
            formatter.indent(sb);
            sb.append("Sort Field Positions : ").append(theSortFieldPositions);
            sb.append(",\n");
        }
        if (thePrimKeyPositions != null) {
            formatter.indent(sb);
            sb.append("Primary Key Positions : ");
            for (int i = 0; i < thePrimKeyPositions.size(); ++i) {
                sb.append(thePrimKeyPositions.get(i));
                if (i < thePrimKeyPositions.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append(",\n");
        }
        theInput.display(sb, formatter);
    }
}
