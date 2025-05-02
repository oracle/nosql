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
import java.util.Comparator;
import java.util.List;

import oracle.kv.Direction;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.ExprInOp.In3BindInfo;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.FieldRange;


/**
 * ExprBaseTable is an internal expression representing a single table or a
 * NESTED TABLES clause appearing in a FROM clause of a query. A NESTED TABLES
 * clause consists of a target table and a number of ancestor and/or descendant
 * tables of the target table.
 *
 * Evaluation of this expr returns a set of rows. In the single-table case, the
 * rows are the rows of the referenced table and they are returned as tuples,
 * i.e., if the rows of the table consist of N columns, the expression returns
 * N values per row (stored in a TupleValue). In the NESTED TABLES case, the
 * returned rows are composite rows constructed by the left-outer-joins among
 * the participating tables. Each composite row has N columns, where N is the
 * number of tables in the NESTED TABLES. The value of each column is a row
 * from the associated table or NULL if there is no row from the associated
 * table that matches with the other table rows in the composite row. The
 * N values per composite row are stored in a TupleValue,
 *
 * theTargetTable:
 * The target table
 *
 * theTables:
 * Stores the TableImpl for each referenced table. In the NESTED TABLES case,
 * the tables are ordered in the order that they would be encountered during
 * a depth-first traversal of the table hierarchy tree (so, the ancestor
 * tables are first followed by the target table, and then the descendandant
 * tables). The position of each table is this array serves as the id of the
 * table in the query context.
 *
 * theAliases:
 * Mirrors theTables and stores the alias used for each table.
 *
 * theNumAncestors:
 * The number of ancestor tables.
 *
 * theNumDescendants:
 * The number of descendant tables.
 *
 * theTablePreds:
 * Mirrors theTables and stores the condition expr, if any, that must be applied
 * on each table row during the evaluation of this ExprBaseTable. For the target
 * table, the condition is an index-filtering pred (i.e., can be evaluated by
 * index columns only) that has been pushed down from the WHERE clause. For each
 * non-target table, the condition is the ON predicate that appears in the
 * NESTED TABLES clause next to that table; it may be an index-filtering pred
 * or not.
 *
 * theUsesCoveringIndex:
 * For each table, it says whether the index used to access the table is
 * covering or not.
 *
 * theIndexStorageSizeCalls:
 * List containing all the invocations of index_storage_size() in the query
 */
public class ExprBaseTable extends Expr {

    static class JoinComparator implements Comparator<ExprBaseTable> {

        @Override
        public int compare(ExprBaseTable v1, ExprBaseTable v2) {
            return (v1.thePosInJoin - v2.thePosInJoin);
        }
    }

    static class IndexHint {

        IndexImpl theIndex; // null means the primary index
        boolean theForce;

        IndexHint(IndexImpl index, boolean force) {
            theIndex = index;
            theForce = force;
        }

        @Override
        public String toString() {

            String name = (theIndex == null ? "primary" : theIndex.getName());

            if (theForce) {
                return "FORCE_INDEX(" + name + ")";
            }

            return "PREFER_INDEX(" + name + ")";
        }
    }

    static final JoinComparator theJoinComparator = new JoinComparator();

    private TableImpl theTargetTable;

    private ArrayList<TableImpl> theTables = new ArrayList<TableImpl>(1);

    private ArrayList<String> theAliases = new ArrayList<String>(1);

    private int theNumAncestors;

    private int theNumDescendants;

    private Expr[] theTablePreds;

    private Direction theDirection = Direction.FORWARD;

    private IndexImpl theIndex;

    private ArrayList<RecordValueImpl> theIndexKeys;

    private ArrayList<FieldRange> theRanges;

    private ArrayList<Expr> theBindExprs;

    private int[][] theBindKeysMap;

    private ArrayList<In3BindInfo> theIn3BindInfos;

    private boolean theHaveINstartstopPreds;

    private PrimaryKeyImpl theShardKey;

    private ArrayList<Expr> theShardKeyBindExprs;

    private int theMinPartition = -1;

    private int theMaxPartition = -1;

    private int theMinShard = -1;

    private int theMaxShard = -1;

    private ArrayList<Expr> thePartitionIdBindExprs;

    private ArrayList<Expr> theShardIdBindExprs;

    private boolean[] theUsesCoveringIndex;

    private List<IndexHint> theIndexHints = null;

    private IndexHint theForceIndexHint = null;

    private boolean theEliminateIndexDups;

    private boolean theIsUpdate;

    private boolean theIsDelete;

    private ArrayList<ExprFuncCall> theIndexStorageSizeCalls;

    private int thePosInJoin = -1;

    public ExprBaseTable(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location) {

        super(qcb, sctx, ExprKind.BASE_TABLE, location);
    }

    public void addTable(
        TableImpl table,
        String alias,
        boolean isAncestor,
        boolean isDescendant,
        QueryException.Location loc) {

        theTables.add(table);
        theAliases.add(alias);

        if (isAncestor) {
            if (!theTargetTable.isAncestor(table)) {
                throw new QueryException(
                    "Table " + table.getFullName() + " is not an ancestor " +
                    "of target table " + theTargetTable.getFullName(), loc);
            }

            ++theNumAncestors;

        } else if (isDescendant) {

            if (!table.isAncestor(theTargetTable)) {
                throw new QueryException(
                    "Table " + table.getFullName() + " is not a descendant " +
                    "of target table " + theTargetTable.getFullName(), loc);
            }

            ++theNumDescendants;

        } else {
            theTargetTable = table;
        }
    }

    void setSortedTables(
        ArrayList<TableImpl> sortedTables,
        ArrayList<String> sortedAliases) {

        theTables = sortedTables;
        theAliases = sortedAliases;

        computeType();
    }

    void finalizeTables() {

        int numTables = theTables.size();

        theUsesCoveringIndex = new boolean[numTables];
        theTablePreds = new Expr[numTables];

        for (int i = 0; i < numTables; ++ i) {
            theUsesCoveringIndex[i] = false;
            theTablePreds[i] = null;
        }

        assert(theType == null);
        computeType();
    }

    int getNumTables() {
        return theTables.size();
    }

    int getNumAncestors() {
        return theNumAncestors;
    }

    int getNumDescendants() {
        return theNumDescendants;
    }

    boolean hasNestedTables() {
        return (theNumAncestors > 0 || theNumDescendants > 0);
    }

    boolean isDescendant(int tablePos) {
        return tablePos > theNumAncestors + 1;
    }

    TableImpl getTargetTable() {
        return theTargetTable;
    }

    int getTargetTablePos() {
        return theNumAncestors;
    }

    @Override
    ArrayList<TableImpl> getTables() {
        return theTables;
    }

    TableImpl getTable(int pos) {
        return theTables.get(pos);
    }

    int getTablePos(TableImpl table) {

        for (int i = 0; i < theTables.size(); ++i) {
            if (theTables.get(i).getId() == table.getId()) {
                return i;
            }
        }

        return -1;
    }

    boolean isJsonCollection() {
        return (theTables.size() == 1 && theTables.get(0).isJsonCollection());
    }

    void setTablePred(int tablePos, Expr pred, boolean destroy) {

        pred = ExprPromote.create(null, pred, TypeManager.BOOLEAN_QSTN());
        pred.addParent(this);

        if (theTablePreds[tablePos] != null) {
            theTablePreds[tablePos].removeParent(this, destroy);
        }

        theTablePreds[tablePos] = pred;
    }

    Expr getTablePred(int tablePos) {
        return theTablePreds[tablePos];
    }

    void removeTablePred(int tablePos, boolean destroy) {
        theTablePreds[tablePos].removeParent(this, destroy);
        theTablePreds[tablePos] = null;
    }

    ArrayList<String> getAliases() {
        return theAliases;
    }

    TableImpl getTableForAlias(String alias) {

        for (int i = 0; i < theTables.size(); ++i) {
            if (theAliases.get(i).equals(alias))
                return theTables.get(i);
        }

        throw new QueryStateException(
            "Could not find table for alias " + alias);
    }

    void setPosInJoin(int v) {
        thePosInJoin = v;
    }

    int getPosInJoin() {
        return thePosInJoin;
    }

    IndexImpl getIndex() {
        return theIndex;
    }

    /*
     * The children of an ExprbaseTable are the ON/filtering conditions
     * associated with each table. We assume that the number of children
     * is equal to the number of tables, even if some or all of the table
     * preds may be null. The ExprIterator skips over the null preds.
     */
    @Override
    int getNumChildren() {
        return (theNumAncestors + theNumDescendants + 1);
    }

    Direction getDirection() {
        return theDirection;
    }

    void setDirection(Direction dir) {
        theDirection = dir;
    }

    ArrayList<RecordValueImpl> getIndexKeys() {
        return theIndexKeys;
    }

    void addIndexKeys(
        int tablePos,
        IndexImpl index,
        ArrayList<RecordValueImpl> keys,
        ArrayList<FieldRange> ranges,
        boolean isCoveringIndex) {

        /* Set theIndexKeys and theRanges only if it is the target table */
        if (tablePos == theNumAncestors) {
            theIndexKeys = keys;
            theRanges = ranges;
            theIndex = index;
        }
        theUsesCoveringIndex[tablePos] = isCoveringIndex;
    }

    void removeIndexKeys() {
        theIndexKeys = null;
    }

    ArrayList<FieldRange> getRanges() {
        return theRanges;
    }

    boolean[] getUsesCoveringIndex() {
        return theUsesCoveringIndex;
    }

    boolean targetTableUsesCoveringIndex() {
        return theUsesCoveringIndex[theNumAncestors];
    }

    void setHaveINstartstopPreds() {
        theHaveINstartstopPreds = true;
    }

    public boolean getHaveINstartstopPreds() {
        return theHaveINstartstopPreds;
    }

    void setBindKeys(
        ArrayList<ArrayList<Expr>> bindKeys,
        ArrayList<In3BindInfo> in3bis) {

        assert(theBindExprs == null);
        assert(theIndexKeys != null);
        assert(bindKeys.size() == theIndexKeys.size());

        theIn3BindInfos = (in3bis.isEmpty() ? null : in3bis);

        int numKeys = theIndexKeys.size();

        /* We transform the given bindKeys into theBindExprs and theBindKeysMap.
         * theBindExprs contains the *unique* exprs found in the bindKeys.
         * theBindKeysMap mirrors the bindKeys and replaces each expr in the
         * bindKeys with the position of that expr inside theBindExprs (or -1
         * if the corresponding entry in the bindKeys is null. */
        theBindExprs = new ArrayList<Expr>();
        theBindKeysMap = new int[numKeys][];

        for (int k = 0; k < numKeys; ++k) {

            ArrayList<Expr> bindKey = bindKeys.get(k);
            RecordValueImpl key = theIndexKeys.get(k);

            int numPushedFields = key.size();
            if (theRanges != null && theRanges.get(k) != null ) {
                numPushedFields += 2;
            }

            boolean haveExternals = false;

            theBindKeysMap[k] = new int[numPushedFields];

            for (int f = 0; f < numPushedFields; ++f) {

                Expr e = bindKey.get(f);

                if (e == null) {
                    theBindKeysMap[k][f] = -1;
                } else {
                    haveExternals = true;
                   int pos = theBindExprs.indexOf(e);
                   if (pos < 0) {
                       theBindExprs.add(e);
                       theBindKeysMap[k][f] =
                           theBindExprs.size() - 1;
                   } else {
                       theBindKeysMap[k][f] = pos;
                   }
                }
            }

            if (!haveExternals) {
                theBindKeysMap[k] = null;
            }
        }
    }

    ArrayList<Expr> getBindExprs() {
        return theBindExprs;
    }

    public int[][] getBindKeysMap() {
        return theBindKeysMap;
    }

    public ArrayList<In3BindInfo> getIn3BindInfos() {
        return theIn3BindInfos;
    }

    void addShardKey(PrimaryKeyImpl key) {
        theShardKey = key;
    }

    PrimaryKeyImpl getShardKey() {
        return theShardKey;
    }

    void setShardKeyBindExprs(ArrayList<Expr> v) {
        assert(theShardKeyBindExprs == null);
        theShardKeyBindExprs = v;
    }

    ArrayList<Expr> getShardKeyBindExprs() {
        return theShardKeyBindExprs;
    }

    int getMinPartition() {
        return theMinPartition;
    }

    int getMaxPartition() {
        return theMaxPartition;
    }

    int getMinShard() {
        return theMinShard;
    }

    int getMaxShard() {
        return theMaxShard;
    }

    void setMinContainer(int c, boolean isPartition) {
        if (isPartition) {
            theMinPartition = c;
        } else {
            theMinShard = c;
        }
    }

    void setMaxContainer(int c, boolean isPartition) {
        if (isPartition) {
            theMaxPartition = c;
        } else {
            theMaxShard = c;
        }
    }

    void setContainerIdBindExprs(ArrayList<Expr> v, boolean forPartitions) {
        if (forPartitions) {
            assert(thePartitionIdBindExprs == null);
            thePartitionIdBindExprs = v;
        } else {
            assert(theShardIdBindExprs == null);
            theShardIdBindExprs = v;
        }
    }

    ArrayList<Expr> getPartitionIdBindExprs() {
        return thePartitionIdBindExprs;
    }

    ArrayList<Expr> getShardIdBindExprs() {
        return theShardIdBindExprs;
    }

    boolean isSinglePartition() {

        if (theShardKey != null) {

            if (theShardKeyBindExprs == null) {
                return true;
            }

            boolean singlePart = true;

            for (Expr e : theShardKeyBindExprs) {
                if (e != null && ConstKind.isJoinConst(e)) {
                    singlePart = false;
                    break;
                }
            }

            if (singlePart) {
                return true;
            }
        }

        if (thePartitionIdBindExprs == null) {
            return (theMinPartition > 0 &&
                    theMinPartition == theMaxPartition);
        }

        return (thePartitionIdBindExprs.size() == 1 &&
                ConstKind.isRuntimeConst(thePartitionIdBindExprs.get(0)));
    }

    int getPartitionId() {
        if (theMinPartition > 0 && theMinPartition == theMaxPartition) {
            return theMinPartition;
        }
        return -1;
    }

    /**
     * If index is null, we are checking whether the ptimary index
     * has been specified in a hint/
     */
    boolean isIndexHint(IndexImpl index) {

        if (theIndexHints == null) {
            return false;
        }

        for (IndexHint hint : theIndexHints) {
            if (hint.theIndex == index) {
                return true;
            }
        }

        return false;
    }

    IndexHint getForceIndexHint() {
        return theForceIndexHint;
    }

    /**
     * If index is null, it means the hint is about the primary index
     */
    void addIndexHint(IndexImpl index, boolean force, Location loc) {

        if (theIndexHints == null) {
            theIndexHints = new ArrayList<IndexHint>();
        }

        IndexHint hint = new IndexHint(index, force);

        if (!containsHint(theIndexHints, hint) ) {
            theIndexHints.add(hint);
        }

        if (force) {
            if (theForceIndexHint != null &&
                !index.getName().equals(theForceIndexHint.theIndex.getName())) {
                throw new QueryException(
                    "Cannot have more than one FORCE_INDEX hints", loc);
            }

            theForceIndexHint = hint;
        }
    }

    private static boolean containsHint(
        List<IndexHint> indexHints,
        IndexHint hint) {
        for (IndexHint h : indexHints) {
            if (h.theIndex == null && hint.theIndex == null ||
                h.theIndex.getName().equals(hint.theIndex.getName())) {
                return true;
            }
        }
        return false;
    }

    void addIndexStorageSizeCall(ExprFuncCall fncall) {

        if (theIndexStorageSizeCalls == null) {
            theIndexStorageSizeCalls = new ArrayList<ExprFuncCall>();
        }

        theIndexStorageSizeCalls.add(fncall);
    }

    ArrayList<ExprFuncCall> getIndexStorageSizeCalls() {
        return theIndexStorageSizeCalls;
    }

    boolean isIndexStorageSizeCallForIndex(IndexImpl index) {

        if (theIndexStorageSizeCalls == null) {
            return false;
        }

        for (ExprFuncCall fncall : theIndexStorageSizeCalls) {
            ExprVar var = (ExprVar)fncall.getArg(0);
            TableImpl table = var.getTable();
            String idxName = ((ExprConst)fncall.getArg(1)).getString();
            if (table.getId() == index.getTable().getId() &&
                index.getName().equals(idxName)) {
                return true;
            }
        }

        return false;
    }

    void setEliminateIndexDups() {
        theEliminateIndexDups = true;
    }

    boolean getEliminateIndexDups() {
        return theEliminateIndexDups;
    }

    void setIsUpdate() {
        theIsUpdate = true;
    }

    boolean getIsUpdate() {
        return theIsUpdate;
    }

    void setIsDelete() {
        theIsDelete = true;
    }

    boolean getIsDelete() {
        return theIsDelete;
    }

    @Override
    ExprType computeType() {

        if (theType != null) {
            return theType;
        }

        if (theTables.size() == 1) {
            if (theTargetTable.isJsonCollection()) {
                theType = TypeManager.createType(
                    FieldDefImpl.Constants.mapJsonDef,
                    Quantifier.STAR);
            } else {
                theType = TypeManager.createTableRecordType(theTargetTable,
                                                            Quantifier.STAR);
            }
            return theType;
        }

        FieldMap unionMap = new FieldMap();

        for (int i = 0; i < theTables.size(); ++i) {

            TableImpl table = theTables.get(i);
            FieldDefImpl rowDef = (table.isJsonCollection() ?
                                   FieldDefImpl.Constants.mapJsonDef :
                                   table.getRowDef());
            String fname = theAliases.get(i);

            unionMap.put(fname, rowDef, true, /*nullable*/
                         null/*defaultValue*/);
        }

        RecordDefImpl unionDef =
            FieldDefFactory.createRecordDef(unionMap, "fromDef");

        theType = TypeManager.createType(unionDef, Quantifier.STAR);

        return theType;
    }

    @Override
    public boolean mayReturnNULL() {
        return false;
    }

    @Override
    boolean mayReturnEmpty() {
        return true;
    }

    @Override
    void display(StringBuilder sb, DisplayFormatter formatter) {

        formatter.indent(sb);
        sb.append("TABLE");
        displayContent(sb, formatter);
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        int numRanges = (theRanges != null ? theRanges.size() : 0);

        sb.append("\n");
        formatter.indent(sb);
        sb.append("[\n");

        formatter.incIndent();
        formatter.indent(sb);
        sb.append(theTargetTable.getName());

        if (theIndex == null) {

            if (theUsesCoveringIndex[theNumAncestors]) {
                sb.append(" via covering primary index");
            } else {
                sb.append(" via primary index");
            }
        } else {
            if (theUsesCoveringIndex[theNumAncestors]) {
                sb.append(" via covering index ");
            } else {
                sb.append(" via index ");
            }
            theIndex.getName();
            if (theEliminateIndexDups) {
                sb.append(" with duplicate elimination");
            }
        }

        for (int i = 0; i < numRanges; ++i) {
            sb.append("\n");
            formatter.indent(sb);
            sb.append("KEY: ");
            sb.append(theIndexKeys.get(i));
            sb.append("\n");
            formatter.indent(sb);
            sb.append("RANGE: ");
            sb.append(theRanges.get(i));
        }

        if (theNumAncestors > 0) {
            sb.append("\n");
            formatter.indent(sb);
            sb.append("Ancestors :\n");
            for (int i = 0; i < theNumAncestors; ++i) {
                formatter.indent(sb);
                sb.append(theTables.get(i).getFullName());
                if (theUsesCoveringIndex[i]) {
                    sb.append(" via covering primary index");
                } else {
                    sb.append(" via primary index");
                }
                sb.append("\n");
            }
        }

        if (theNumDescendants > 0) {
            sb.append("\n");
            formatter.indent(sb);
            sb.append("Descendants :\n");
            for (int i = theNumAncestors + 1; i < theTables.size(); ++i) {
                formatter.indent(sb);
                sb.append(theTables.get(i).getFullName());
                if (theUsesCoveringIndex[i]) {
                    sb.append(" via covering primary index");
                } else {
                    sb.append(" via primary index");
                }
                sb.append("\n");
            }
        }

        if (theBindExprs != null) {
            sb.append("\n");
            formatter.indent(sb);
            sb.append("PUSHED EXTERNAL EXPRS: ");
            for (Expr expr : theBindExprs) {
                sb.append("\n");
                if (expr == null) {
                    formatter.indent(sb);
                    sb.append("null");
                } else {
                    expr.display(sb, formatter);
                }
            }
        }

        if (theTablePreds != null) {

            if (theTablePreds[theNumAncestors] != null) {
                sb.append("\n\n");
                formatter.indent(sb);
                sb.append("Filtering Predicate:\n");
                theTablePreds[theNumAncestors].display(sb, formatter);
            }

            for (int i = 0; i < theTables.size(); ++i) {

                if (i == theNumAncestors || theTablePreds[i] == null) {
                    continue;
                }

                sb.append("\n\n");
                formatter.indent(sb);
                sb.append("ON Predicate for table ").
                   append(theTables.get(i).getFullName()).
                   append(":\n");
                theTablePreds[i].display(sb, formatter);
            }
        }

        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("]");
    }
}
