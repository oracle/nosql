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

package oracle.kv.impl.query.runtime.server;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DoubleValueImpl;
import oracle.kv.impl.api.table.EmptyValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.Geometry;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexKeyImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.JsonCollectionRowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.query.compiler.ExprInOp.In3BindInfo;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.SortSpec;
import oracle.kv.impl.query.runtime.BaseTableIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.PlanIterState;
import oracle.kv.impl.query.runtime.PlanIterState.StateEnum;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.query.runtime.SortIter;
import oracle.kv.impl.query.runtime.server.TableScannerFactory.TableScanner;
import oracle.kv.impl.query.runtime.server.TableScannerFactory.AncestorScanner;
import oracle.kv.impl.query.runtime.server.TableScannerFactory.SizeLimitException;
import oracle.kv.impl.query.runtime.server.TableScannerFactory.TimeoutException;
import oracle.kv.impl.util.Pair;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.PrepareQueryException;

/**
 * ServerTableIter is used at runtime only, to execute a table scan (via the
 * primary or a secondary index) in the server context. This class exists so
 * that it can include code that uses server-only classes that are not available
 * on the client side. It is created by BaseTableIter during its open() call.
 * The BaseTableIter serves as a proxy for this implementation.
 *
 * Given that ServerTableIter is created during open(), it does not need to
 * store its dynamic state separately; the ServerTableIter itself is "state".
 * So, ServerTableIter uses just the basic PlanIterState.
 *
 * ServerTableIter implements access to a single table as well as the
 * left-outer-join semantics of the NESTED TABLES clause.
 *
 * Some terminology for the NESTED TABLES case:
 *
 * - An "A" table is a table appearing in the ANCESTORS part of the NESTED
 *   TABLES.
 * - A "D" table is a table appearing in the DESCENDANTS part of the NESTED
 *   TABLES, or the target table.
 * - The "join tree" is a projection of the table hierarchy tree that contains
 *   the tables specified in the NESTED TABLES. For example, consider the
 *   following table hierarchy:
 *
 *                  A
 *               /  |  \
 *              /   |   \
 *            B     F     G
 *          /  \         /  \
 *         C    E       H    J
 *        /                   \
 *       D                     K
 *
 *  and this FROM clause: FROM NESTED TABLES(A descendants(B, D, J)). Then the
 *  join tree is:
 *
 *                   A
 *                 /   \
 *               B      J
 *              /
 *             D
 *
 *   Here is another join tree from the same table hierarchy:
 *
 *                   A
 *                 /   \
 *               B      K
 *              /
 *             C
 *
 *   We will refer to the above join trees as "sample tree1" and "sample
 *   tree2" and use them as examples when we explain the NESTED TABLES
 *   algorithm below.
 *
 * - In this implementation, the terms "parent table" and "child table" will
 *   refer to parent/child  tables in the context of the join tree, not the
 *   table hierarchy tree. Similarly, the ancestor/descendant relationship is
 *   defined here with respect to the join tree.
 *
 * theFactory:
 * See javadoc for TableScannerFactory.
 *
 * theTables:
 * An array containing a TableImpl instance for each of the tables accessed
 * by "this". The position of the TableImpls within this array server as
 * local table ids. The tables are sorted in the order that they would be
 * encountered in a depth-first traversal of the table hierarchy.
 *
 * theTargetTable:
 * The target table of a NESTED TABLES clause, or the single table referenced
 * in the FROM clause.
 *
 * theTargetTablePos:
 * The position of the target table in theTables (it's equal to theNumAncestors)
 *
 * theLockIndexEntries:
 * This boolean field is used to implement a small optimization for the
 * following case: (a) "this" accesses a single table, and (b) the index used
 * to access the table is covering, and (c) there is no filtering predicate.
 * In this case, we should be locking the index entries during the scan, rather
 * than doing dirty reads and then locking the row (which, in case of secondary
 * indexes, involves a lookup of the primary index). Locking the index entries
 * upfront is ok because all of these entries need to be returned. In this case,
 * theLockIndexEntries will be set to true.
 *
 * Note that in case of a covering index with filtering, we do dirty reads as
 * we scan the index. If an index entry survives the filtering, then we lock
 * that entry (but not the associated table row). We don't need to recheck the
 * filtering predicate because as long as the JE cursor is on that entry, its
 * key cannot change, it can only be marked as deleted, in which case the lock
 * call will fail and the entry will be skipped.
 *
 * theRTPrimKeys:
 * theRTSecKeys:
 * theRTRanges:
 * These 3 fields are the runtime versions of thePrimKeys, theSecKeys, and
 * theRanges fields of BaseTableIter. theRTPrimKeys and theRTSecKeys are
 * instances of PrimaryKeyImpl and IndexKeyImpl respectively (instead of
 * RecordValueImpl). Furthermore, any external-variable placeholders that
 * may appear in the compile-time versions of these 3 fields have been
 * replaced with actual values in the runtime versions.
 *
 * theIndexEntryDef:
 *
 * theAlwaysFalse:
 *
 * theJoinAncestors:
 * For each D-table T, the D-tables that are ancestors of T in the join tree.
 *
 * theJoinLeaves:
 * For each table, true/false if the table is/is-not a leaf in the join tree.
 *
 * theLinearJoin:
 * True if the join tree is a lineer path. If so, then if a table T in the path
 * does not satisfy the join or the ON predicates, we can skip all rows from
 * tables under T by using the max-key-components optimization (see
 * MultiTableOperationHandler.keyInTargetTable()).
 *
 * theScanner:
 * The scanner used to access the rows of the D tables. It scans either the
 * primary index (which contains the rows of all tables), or a secondary index
 * of the target table (in which case, the target table is the only D table).
 * (see javadoc of TableScannerFactory). The row that theScanner is currently
 * positioned on is called the "current row", and its containing table is the
 * "current table".
 *
 * theJoinPathLength:
 * The current length of theJoinPath.
 *
 * theJoinPath:
 * A linear path in the join tree containing D-table rows and/or keys that
 * have been scanned already, but for which we don't know yet whether they will
 * participate in a result or not. All the rows in the path satisfy the join
 * and ON preds, so theJoinPath (together with theAncestorsPath) is essentially
 * storing a partial potential result.
 *
 * If the current row satisfies the join and ON preds, it is added to
 * theJoinPath. If the current table is a leaf in the join tree, theJoinPath
 * holds an actual result. We call this a "current result", because it
 * contains the current row (as well as rows from all the ancestors of the
 * current table) and is generated during the processing of the current row.
 * But theJoinPath may store an actual result even if its leaf node is not a
 * join-tree leaf; this is the case when we know (based on the position of the
 * current row relative to the rows in theJoinPath) that there are no rows that
 * may join with the leaf node in theJoinPath. We call this a "delayed result",
 * because it is generated after its participating rows have already been
 * scanned and processed. For example, consider the join tree shown above, and
 * let theJoinPath contain rows RA and RB from tables A and B respectively. If
 * the current row RJ is from table J, RB cannot match with any following rows,
 * and if RB did not also match with any preceding rows, the delayed result
 * [RA, RB, NULL, NULL] must be generated.
 *
 * theAncestorsPath:
 * It stores the A-table rows/keys for the current target-table row (the row at
 * the root of theJoinPath). Note that we don't keep track of the length of this
 * path, because all the A-rows are added together, so the path is always
 * "full".
 *
 * theSavedResult:
 * When theJoinPath has a delayed result, we cannot return that result to the
 * caller immediatelly, because we have to process the current row first (which
 * will not participate in theJoinPath result). In this case theSavedResult is
 * used to store theJoinPath result until we can actually return this result.
 * We have to do this because processing the current row may modify theJoinPath
 * (and the tuple registers).
 *
 * theNextTable:
 * Used to take advantage of the max-key-components optimization. When non-null
 * we know that no rows with more primary key components than those of
 * theNextTable will participate in the next result. So we can skip such rows.
 *
 * theHaveResult:
 * If true, the previous invocation of next() produced 2 results: a delayed one
 * and a "normal" one. Only the delayed result was returned, so in the current
 * invocation of next() we must return the normal result, which is stored in
 * thejoinPath. Here is an example that shows how 2 results may be produced:
 *
 * NESTED TABLES(A descendatns(B, C, G)).
 *
 * During a next() invocation, we get a row RG from G and theJoinPath contains
 * rows RA and RB from A abd B respectivelly. The RG row will cause a delayed
 * result to be produced: (RA, RB, NULL, NULL). However, (RA, NULL, NULL, RG)
 * is also a result.
 *
 * theProducedDelayedResult
 * Set to true when a delayed result is produced, and reset to false at the
 * start of the next nestedTablesNext() invocation. When set to true, we also
 * set theCannotSuspendDueToDelayedResult in the RCB, if this TableIter is not
 * in the last join branch. We do this to avoid missing results when a NESTED
 * TABLES is inner-joined with another NESTED TABLES or another table. For
 * example, consider a NESTED TABLES N that is joined with another table B.
 * Assume a delayed result is produced by N for row RN1. At this point, the
 * table scanner of N has moved past RN1, to RN2. If the query is allowed to
 * suspend while RN1 is joined with its matching rows in B, then the next query
 * batch will resume with RN2, thus missing any additional results from RN1.
 * When theProducedDelayedResult, the RCB.theCannotSuspendDueToDelayedResult
 * is also reset. Notice that the RCB.theCannotSuspendDueToDelayedResult is
 * an integer, instead of a boolean, because it may be (re)set by multiple
 * NESTED TABLES.
 *
 * theInResume:
 * True while the ServerTableIter is in resume.
 *
 * thePrimKeysSet:
 * Used for duplicate row elimination.
 *
 * theExceededSizeLimit:
 * This is needed for all-partition cloud queries in sorting phase 1 (see
 * javadoc of PartititonUnionIter for details about how all-partition cloud
 * queries are executed). Initially, this.theExceededSizeLimit is false.
 * During reset(), we save theScanner.theExceededSizeLimit flag in
 * this.theExceededSizeLimit and then we pass the saved flag to the constructor
 * of the next scanner, which is created during the 1st next() after the
 * reset().
 */
public class ServerTableIter extends BaseTableIter {

    private static class KeyRangePair {

        RecordValueImpl key;
        FieldRange range;

        KeyRangePair(RecordValueImpl key, FieldRange range) {
            this.key = key;
            this.range = range;
        }
    }

    private class KeyCompareFunction implements Comparator<RecordValueImpl> {

        int[] theSortFieldPositions;
        SortSpec[] theSortSpecs;

        KeyCompareFunction() {
            RecordValueImpl key = theRTKeys[0];
            int numFields = key.size();
            theSortFieldPositions = new int[numFields];
            theSortSpecs = new SortSpec[numFields];
            boolean isDesc = (theDirection == Direction.REVERSE);
            boolean nullsFirst = isDesc;
            SortSpec spec = new SortSpec(isDesc, nullsFirst);

            for (int i = 0; i < numFields; ++i) {
                theSortFieldPositions[i] = i;
                theSortSpecs[i] = spec;
            }
        }

        @Override
        public int compare(RecordValueImpl v1, RecordValueImpl v2) {
            return SortIter.compareRecords(v1, v2, theSortFieldPositions,
                                           theSortSpecs);
        }
    }

    private class KeyRangeCompareFunction implements Comparator<KeyRangePair> {

        int[] theSortFieldPositions;
        SortSpec[] theSortSpecs;

        KeyRangeCompareFunction() {
            RecordValueImpl key = theRTKeys[0];
            int numFields = key.size();
            theSortFieldPositions = new int[numFields];
            theSortSpecs = new SortSpec[numFields];
            boolean isDesc = (theDirection == Direction.REVERSE);
            boolean nullsFirst = isDesc;
            SortSpec spec = new SortSpec(isDesc, nullsFirst);

            for (int i = 0; i < numFields; ++i) {
                theSortFieldPositions[i] = i;
                theSortSpecs[i] = spec;
            }
        }

        @Override
        public int compare(KeyRangePair v1, KeyRangePair v2) {
            return SortIter.compareRecords(v1.key, v2.key,
                                           theSortFieldPositions,
                                           theSortSpecs);
        }
    }

    private class IndexScanId {

        RuntimeControlBlock rcb;
        int scan;

        IndexScanId(RuntimeControlBlock rcb, int scan) {
            this.rcb = rcb;
            this.scan = scan;
        }

        @Override
        public boolean equals(Object o) {

            RecordValueImpl key1 = theRTKeys[scan];
            RecordValueImpl key2 = theRTKeys[((IndexScanId)o).scan];

            if (! key1.equals(key2)) {
                return false;
            }

            FieldRange range1 = theRTRanges[scan];
            FieldRange range2 = theRTRanges[((IndexScanId)o).scan];

            if (range1 == null) {
                if (range2 == null) {
                    return true;
                }
                return false;
            } else if (range2 == null) {
                return false;
            }

            if (range1.getStart() != null) {
                if (!range1.getStart().equals(range2.getStart())) {
                    return false;
                }
            } else if (range2.getStart() != null) {
                return false;
            }

            if (range1.getEnd() != null) {
                if (!range1.getEnd().equals(range2.getEnd())) {
                    return false;
                }
            } else if (range2.getEnd() != null) {
                return false;
            }

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("Found duplicate index scan:\n" +
                          "key1 = " + key1 + " key2 = " + key2 +
                          "\nrange1 = " + range1 +
                          "\nrange2 = " + range2);
            }
            return true;
        }

        @Override
        public int hashCode() {
            return theRTKeys[scan].hashCode();
        }
    }

    private static class JoinPathNode {

        private int tablePos = -1;

        private byte[] primKeyBytes;

        private FieldValueImpl row;

        boolean matched;

        void reset(int pos, FieldValueImpl inRow, byte[] pk) {
            this.tablePos = pos;
            this.row = inRow;
            this.primKeyBytes = pk;
            this.matched = false;
        }

        @Override
        public String toString() {
            return ("tablePos = " + tablePos + " matched = " + matched +
                    " Row = \n" + row);
        }
    }

    static final FieldValueImpl theNULL = NullValueImpl.getInstance();

    /*
     * The following fields are created during the construction of "this"
     * and remain constant afterwards.
     */

    private final TableScannerFactory theFactory;

    private final TableImpl[] theTables;

    private final TableImpl theTargetTable;

    private final int theTargetTablePos;

    private final IndexImpl theIndex;

    private final boolean theLockIndexEntries;

    private RecordValueImpl[] theRTKeys;

    private FieldRange[] theRTRanges;

    protected RecordDefImpl theIndexEntryDef;

    private boolean theAlwaysFalse;

    private boolean[] theAlwaysFalseScans;

    private int[][] theJoinAncestors;

    private boolean[] theJoinLeaves;

    private boolean theLinearJoin;

    /*
     * The following fields store dynamic state used during the operation
     * of this iter.
     */

    private TableScanner theScanner;

    private int theJoinPathLength;

    private JoinPathNode[] theJoinPath;

    private byte[] theJoinPathSecKey;

    private JoinPathNode[] theAncestorsPath;

    private FieldValueImpl[] theSavedResult;

    private TableImpl theNextTable;

    private boolean theProducedDelayedResult;

    private boolean theHaveResult;

    private boolean theInResume;

    private boolean theExceededSizeLimit;

    ServerTableIter(
        RuntimeControlBlock rcb,
        ServerIterFactoryImpl opCtx,
        BaseTableIter parent) {

        super(parent);

        theFactory = new TableScannerFactory(rcb,
                                             opCtx.getTxn(),
                                             opCtx.getOperationHandler());

        TableMetadataHelper md =  rcb.getMetadataHelper();

        int numTables = theTableNames.length;

        theTables = new TableImpl[numTables];

        for (int i = 0; i < theTableNames.length; ++i) {
            theTables[i] = md.getTable(theNamespace, theTableNames[i]);

            if (theTables[i] == null) {
                String name = NameUtils.makeQualifiedName(theNamespace,
                                                          theTableNames[i]);
                throw new IllegalArgumentException(
                      "Table " + name + " does not exist");
            }

            if (theTableIds != null && theTables[i].getId() != theTableIds[i]) {
                String name = NameUtils.makeQualifiedName(theNamespace,
                                                          theTableNames[i]);
                throw new IllegalArgumentException(
                      "Table " + name + " does not have the expected id: " +
                      theTableIds[i]);
            }

            if (theTableVersions != null &&
                theTables[i].getTableVersion() != theTableVersions[i]) {
                String name = NameUtils.makeQualifiedName(theNamespace,
                                                          theTableNames[i]);
                throw new PrepareQueryException(
                      "The query must be prepared again because table " +
                      name + " has been altered.");
            }
        }

        theTargetTable = theTables[theNumAncestors];
        theTargetTablePos = theNumAncestors;

        if (theIndexName != null) {
            theIndex = (IndexImpl)theTargetTable.getIndex(theIndexName);
            if (theIndex == null) {
                throw new IllegalArgumentException(
                      "Index " + theIndexName + " does not exist");
            }

            if (theIndexId > 0 && theIndex.getId() != theIndexId) {
                throw new PrepareQueryException(
                      "The query must be prepared again because index " +
                      theIndexName + " does not have the expected id: " +
                      theIndexId);
            }
        } else {
            theIndex = null;
        }

        theLockIndexEntries = (numTables == 1 &&
                               theUsesCoveringIndex[0] &&
                               thePredIters[0] == null);

        if (thePushedExternals != null) {
            oldComputeScanBoundatries(rcb);
        } else {
            computeIndexScans(rcb);
        }

        initJoins();
    }

    private void computeIndexScans(RuntimeControlBlock rcb) {

        int geoFieldPos = -1;
        int numCompileScans; // the num of scans determined during compilation

        if (theIndex != null) {
            geoFieldPos = theIndex.getGeoFieldPos();
            numCompileScans = theSecKeys.length;
        } else {
            numCompileScans = thePrimKeys.length;
        }

        /* If the query has no external variables, the runtime scans are the
         * compile-time scans. */
        if (theExternalKeysIters == null) {
            initIndexScans(numCompileScans, numCompileScans);
            sortIndexScans();
            return;
        }

        int numRuntimeScans = numCompileScans;
        Object[] bindVals = new Object[theExternalKeysIters.length];

        PlanIter geomIter = null;
        PlanIter distIter = null;
        Geometry geom = null;
        double dist = -1;
        List<Pair<String,String>> geoRanges = null;
        List<String> geoKeys = null;

        /* Find among theExternalKeysIters the iters that compute a
         * geometry or a geo distance for a pushed geo pred */
        if (geoFieldPos >= 0) {
            int[] bindmap = theExternalKeysMap[0];
            if (bindmap != null && bindmap.length >= geoFieldPos) {
                if (bindmap[geoFieldPos] >= 0) {
                    geomIter = theExternalKeysIters[bindmap[geoFieldPos]];
                }
                if (bindmap[geoFieldPos+1] >= 0) {
                    distIter = theExternalKeysIters[bindmap[geoFieldPos+1]];
                }
            }
        }

        int[] in3Iters = null;

        /* Find among theExternalKeysIters the iters that compute the RHS
         * of an IN3 operator. The in3Iters array mirrors theExternalKeysIters.
         * Let K = in3Iters[i]. If K >= 0, the i-th iter in theExternalKeysIters
         * computes the RHS expr of an IN3 operator and K is the position of the
         * that IN3 op within theIn3BindInfos. */
        if (theIn3BindInfos != null) {

            in3Iters = new int[theExternalKeysIters.length];
            for (int i = 0; i < theExternalKeysIters.length; ++i) {
                in3Iters[i] = -1;
            }

            int[] bindmap = theExternalKeysMap[0];

            for (int i = 0; i < theIn3BindInfos.length; ++i) {
                In3BindInfo in3bi = theIn3BindInfos[i];
                int in3field = in3bi.theIndexFieldPositions[0];
                in3Iters[bindmap[in3field]] = i;
                in3bi.theRHSIter = bindmap[in3field];
            }
        }

        /* Compute all the bind values */
        for (int i = 0; i < theExternalKeysIters.length; ++i) {

            FieldValueImpl bindval = null;
            PlanIter iter = theExternalKeysIters[i];
            iter.open(rcb);
            boolean more = iter.next(rcb);
            if (more) {
                bindval = rcb.getRegVal(iter.getResultReg());
            }

            if (rcb.getTraceLevel() >= 4) {
                if (bindval == null) {
                    rcb.trace("Bind iter " + i + ": Computed no values");
                } else if (bindval.isNull()) {
                    rcb.trace("Bind iter " + i + ": Computed NULL bind value");
                } else {
                    rcb.trace("Bind iter " + i + ": Computed bind value: " +
                              bindval);
                }
            }

            if (in3Iters != null && in3Iters[i] >= 0) {
                ArrayList<FieldValueImpl> bindvals = new ArrayList<>();

                bindVals[i] = bindvals;
                if (bindval != null) {
                    bindvals.add(bindval);
                }

                while (iter.next(rcb)) {
                    bindval = rcb.getRegVal(iter.getResultReg());
                    bindvals.add(bindval);

                    if (rcb.getTraceLevel() >= 4) {
                        if (bindval.isNull()) {
                            rcb.trace("Bind iter " + i +
                                      ": Computed NULL bind value");
                        } else {
                            rcb.trace("Bind iter " + i +
                                      ": Computed bind value: " + bindval);
                        }
                    }
                }

                if (bindvals.size() < theIn3BindInfos[in3Iters[i]].theNumComps) {
                    theAlwaysFalse = true;
                    return;
                }

                numRuntimeScans *= (bindvals.size() /
                                    theIn3BindInfos[in3Iters[i]].theNumComps);

            } else {
                if (bindval == null) {
                    bindval = EmptyValueImpl.getInstance();
                }

                bindVals[i] = bindval;

                if (iter.next(rcb)) {
                    throw new QueryStateException(
                       "Pushed external expression returns more than one items");
                }

                if (iter == geomIter) {
                    geom = CompilerAPI.getGeoUtils().castAsGeometry(bindval);

                    if (geom == null) {
                        throw new QueryException(
                            "The right operand of a geo search function is " +
                            "not a valid geometry.", geomIter.getLocation());
                    }
                }

                if (iter == distIter) {
                    dist = ((DoubleValueImpl)bindval).get();

                    if (dist <= 0) {
                        theAlwaysFalse = true;
                        return;
                    }
                }
            }
        }

        if (theIndex != null && geom != null) {
            geoRanges = CompilerAPI.getGeoUtils().
                        ranges(geom, dist, rcb.getExecuteOptions());

            int numGeoRanges = geoRanges.size();

            if (theIndex.isGeometryIndex()) {
                geoKeys = CompilerAPI.getGeoUtils().keys(geoRanges);
                if (geoKeys != null) {
                    numGeoRanges += geoKeys.size();
                }
            }

            if (rcb.getTraceLevel() >= 4) {
                rcb.trace("Num Compile Scans = " + numCompileScans +
                          " Num Runtime Scans = " + numRuntimeScans +
                          " Num geo ranges = " + numGeoRanges);
            }

            numRuntimeScans = numRuntimeScans * numGeoRanges;
        }

        /* Intialize the runtime index scans with the compile time scans */
        initIndexScans(numCompileScans, numRuntimeScans);

        /* Bind the computed values into the runtime index scan, generating
         * new runtime index scans along the way, if necessary */
        bindIndexScans(rcb, theTargetTable, theIndex, geoFieldPos,
                       numCompileScans, numRuntimeScans,
                       bindVals, geoRanges, geoKeys);
        if (theAlwaysFalse) {
            return;
        }
        sortIndexScans();
        eliminateDuplicateIndexScans(rcb);
    }

    private void initIndexScans(
        int numCompileScans,
        int numRuntimeScans) {

        theRTRanges = new FieldRange[numRuntimeScans];
        theRTKeys = new RecordValueImpl[numRuntimeScans];
        theAlwaysFalseScans = new boolean[numRuntimeScans];

        for (int s = 0; s < numRuntimeScans; ++s) {
            theAlwaysFalseScans[s] = false;
        }

        if (theIndex != null) {
            for (int s = 0; s < numCompileScans; ++s) {
                theRTKeys[s] = theIndex.
                    createIndexKeyFromFlattenedRecord(theSecKeys[s]);
                theRTRanges[s] = theRanges[s];
            }
            theIndexEntryDef = theIndex.getIndexEntryDef();
        } else {
            for (int s = 0; s < numCompileScans; ++s) {
                theRTKeys[s] = theTargetTable.createPrimaryKey(thePrimKeys[s]);
                theRTRanges[s] = theRanges[s];
            }
            theIndexEntryDef = theTargetTable.getRowDef();
        }
    }

    private void bindIndexScans(
        RuntimeControlBlock rcb,
        TableImpl table,
        IndexImpl index,
        int geoFieldPos,
        int numCompileScans,
        int numRuntimeScans,
        Object[] bindVals,
        List<Pair<String,String>> geoRanges,
        List<String> geoKeys) {

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("Num Compile Scans = " + numCompileScans +
                      " Num Runtime Scans = " + numRuntimeScans);
        }

        int numFields = (index == null ?
                         table.getPrimaryKeySize() :
                         index.numFields()); // does not include pk columns

        int numScans = numCompileScans;
        int currNumScans = numScans;
        FieldValueImpl val;
        boolean[] processedFields = null;
        boolean haveFalseScans = false;

        if (theIn3BindInfos != null) {
            processedFields = new boolean[numFields];
            for (int i = 0; i < numFields; ++i) {
                processedFields[i] = false;
            }
        }

        int[] scanMap = new int[numRuntimeScans];
        for (int s = 0; s < numCompileScans; ++s) {
            scanMap[s] = s;
        }

        for (int field = 0; field < numFields; ++field) {

            if (processedFields != null && processedFields[field]) {
                continue;
            }

            boolean pushed = false;

            for (int scan = 0; scan < numScans; ++scan) {

                RecordValueImpl key = theRTKeys[scan];
                FieldRange range = theRTRanges[scan];

                if (rcb.getTraceLevel() >= 4) {
                    rcb.trace("Binding field " + field + " in scan " +
                              scan + " key = " + key + " range = " + range);
                }

                if ((field == key.size() && range == null) ||
                    field > key.size()) {
                    if (rcb.getTraceLevel() >= 4) {
                        rcb.trace("Nothing pushed on field");
                    }
                    continue;
                }

                pushed = true;
                int[] bindMap = theExternalKeysMap[scanMap[scan]];

                if (bindMap == null ||
                    (field < key.size() && bindMap[field] < 0) ||
                     (bindMap[field] < 0 && bindMap[field+1] < 0)) {

                    if (rcb.getTraceLevel() >= 4) {
                        rcb.trace("Nothing to bind");
                    }
                    continue;
                }

                In3BindInfo in3bi = isIn3Field(field);

                if (in3bi != null &&
                    processedFields != null /* just to eliminate warning*/) {

                    ArrayList<?> vals = (ArrayList<?>)(bindVals[bindMap[field]]);

                    int[] in3fields = in3bi.theIndexFieldPositions;

                    for (int i = 0; i < in3fields.length; ++i) {
                        processedFields[in3fields[i]] = true;
                    }

                    int numNewScans = vals.size() / in3bi.theNumComps;

                    for (int scan2 = 0; scan2 < numNewScans; ++scan2) {

                        if (scan2 != 0) {
                            key = key.clone();
                            if (range != null) {
                                range = range.clone();
                            }
                            theRTKeys[currNumScans] = key;
                            theRTRanges[currNumScans] = range;
                            scanMap[currNumScans] = scanMap[scan];
                            ++currNumScans;
                        }

                        for (int inpos = 0; inpos < in3fields.length; ++inpos) {

                            val = (FieldValueImpl)
                                  vals.get(scan2 * in3bi.theNumComps +
                                           in3bi.thePushedComps[inpos]);

                            try {
                                key.put(in3fields[inpos], val);
                            } catch (IllegalArgumentException e) {
                                Type ftype = key.getFieldDef(in3fields[inpos]).
                                             getType();
                                throw new QueryException(
                                    "Type mismatch in IN operator.\n" +
                                    "LHS type: " + ftype +
                                    " RHS type: " + val.getType(),
                                    theExternalKeysIters[in3bi.theRHSIter].
                                    getLocation());
                            }
                        }

                        if (rcb.getTraceLevel() >= 4) {
                            rcb.trace("Bound key " + key);
                        }
                    }

                } else if (field < key.size()) {

                    val = (FieldValueImpl)bindVals[bindMap[field]];

                    /* Comparison of a primary key column with NULL is always
                     * false. The bind val may be null in the case of an inner
                     * join where the left join operand is a NESTED TABLES. */
                    if (index == null && val.isNull()) {
                        theAlwaysFalseScans[scan] = true;
                        haveFalseScans = true;
                        if (rcb.getTraceLevel() >= 4) {
                            rcb.trace("Scan is always false");
                        }
                        continue;
                    }

                    if (!val.isNull()) {
                        val = castBindVal(table, index, field, val,
                                          FuncCode.OP_EQ, scan);
                    }
                    bindVals[bindMap[field]] = val;

                    if (theAlwaysFalseScans[scan]) {
                        haveFalseScans = true;
                        continue;
                    }

                    key.put(field, val);

                    if (rcb.getTraceLevel() >= 4) {
                        rcb.trace("Bound key " + key);
                    }

                } else if (index != null && geoFieldPos >= 0) {

                    assert(geoFieldPos == key.size());

                    String pathName = index.getFieldName(geoFieldPos);
                    FieldDefImpl rangeDef = FieldDefImpl.Constants.stringDef;

                    for (int i = 0; i < geoRanges.size(); ++i) {
                        Pair<String,String> grange = geoRanges.get(i);
                        FieldRange frange = new FieldRange(pathName, rangeDef, 0);
                        frange.setStart(grange.first(), true);
                        frange.setEnd(grange.second(), true);

                        if (i == 0) {
                            theRTRanges[scan] = frange;
                        } else {
                            theRTKeys[currNumScans] = key.clone();
                            theRTRanges[currNumScans] = frange;
                            ++currNumScans;
                        }
                    }

                    if (geoKeys != null) {
                        for (int i = 0; i < geoKeys.size(); ++i) {
                            theRTRanges[currNumScans] = null;
                            theRTKeys[currNumScans] = key.clone();
                            theRTKeys[currNumScans].put(geoFieldPos, geoKeys.get(i));
                            ++currNumScans;
                        }
                    }

                } else {
                    if (bindMap[field] >= 0) {
                        val = (FieldValueImpl)bindVals[bindMap[field]];
                        if (val.isNull()) {
                            theAlwaysFalseScans[scan] = true;
                            haveFalseScans = true;
                            continue;
                        }
                        val = castBindVal(table, index, field, val,
                                          FuncCode.OP_GE, scan);
                        bindVals[bindMap[field]] = val;
                        range.setStart(val, range.getStartInclusive(), false);
                    }

                    if (bindMap[field+1] >= 0) {
                        val = (FieldValueImpl)bindVals[bindMap[field+1]];
                        if (val != null && val.isNull()) {
                            theAlwaysFalseScans[scan] = true;
                            haveFalseScans = true;
                            continue;
                        }
                        val = castBindVal(table, index, field, val,
                                          FuncCode.OP_LE, scan);
                        bindVals[bindMap[field+1]] = val;
                        range.setEnd(val, range.getEndInclusive(), false);
                    }

                    if (theAlwaysFalseScans[scan]) {
                        haveFalseScans = true;
                        continue;
                    }

                    if (!range.check()) {
                        theAlwaysFalseScans[scan] = true;
                        haveFalseScans = true;
                        continue;
                    }

                    if (range.getStart() == null && range.getEnd() == null) {
                        theRTRanges[scan] = null;
                    }
                }
            }

            if (!pushed) {
                break;
            }

            numScans = currNumScans;
        }

        if (haveFalseScans) {
            eliminateAlwaysFalseIndexScans();
            if (theRTKeys.length == 0) {
                theAlwaysFalse = true;
                return;
            }
        }
    }

    private In3BindInfo isIn3Field(int field) {

        if (theIn3BindInfos == null) {
            return null;
        }

        for (int i = 0; i < theIn3BindInfos.length; ++i) {
            In3BindInfo in3bi = theIn3BindInfos[i];
            for (int j = 0; j < in3bi.theIndexFieldPositions.length; ++j) {
                if (in3bi.theIndexFieldPositions[j] == field) {
                    return in3bi;
                }
            }
        }

        return null;
    }

    private FieldValueImpl castBindVal(
        TableImpl table,
        IndexImpl index,
        int field,
        FieldValueImpl val,
        FuncCode compOp,
        int scan) {

        FieldValueImpl newVal = BaseTableIter.
            castValueToIndexKey(table, index, field, val, compOp);

        if (newVal != val) {

            if (newVal == BooleanValueImpl.falseValue) {
                theAlwaysFalseScans[scan] = true;
                return null;
            }

            if (newVal == BooleanValueImpl.trueValue) {
                val = null;
            } else {
                val = newVal;
            }
        }

        return val;
    }

    private void sortIndexScans() {

        if (theRTKeys.length == 1 || !theHaveINstartstopPreds) {
            return;
        }

        if (theRTRanges == null) {
            Arrays.sort(theRTKeys, new KeyCompareFunction());
            return;
        }

        KeyRangePair[] keyRanges = new KeyRangePair[theRTKeys.length];
        for (int i = 0; i < theRTKeys.length; ++i) {
            keyRanges[i] = new KeyRangePair(theRTKeys[i], theRTRanges[i]);
        }

        Arrays.sort(keyRanges, new KeyRangeCompareFunction());

         for (int i = 0; i < theRTKeys.length; ++i) {
             theRTKeys[i] = keyRanges[i].key;
             theRTRanges[i] =  keyRanges[i].range;
         }
    }

    private void eliminateDuplicateIndexScans(RuntimeControlBlock rcb) {

        if (theRTKeys.length == 1 || !theHaveINstartstopPreds) {
            return;
        }

        HashSet<IndexScanId> scansSet =
            new HashSet<IndexScanId>(theRTKeys.length);

        for (int i = 0; i < theRTKeys.length; ++i) {

            boolean dup = !scansSet.add(new IndexScanId(rcb, i));

            if (dup) {
                theRTKeys[i] = null;
                theRTRanges[i] = null;
            }
        }
    }

    private void eliminateAlwaysFalseIndexScans() {

        ArrayList<RecordValueImpl> scanKeys = new ArrayList<>(theRTKeys.length);
        ArrayList<FieldRange> scanRanges = new ArrayList<>(theRTKeys.length);

        for (int i = 0; i < theRTKeys.length; ++i) {
            if (!theAlwaysFalseScans[i]) {
                scanKeys.add(theRTKeys[i]);
                scanRanges.add(theRTRanges[i]);
            }
        }

        theRTKeys = scanKeys.toArray(new RecordValueImpl[scanKeys.size()]);
        theRTRanges = scanRanges.toArray(new FieldRange[scanRanges.size()]);
    }

    private void oldComputeScanBoundatries(RuntimeControlBlock rcb) {

        TableImpl table = theTargetTable;
        IndexImpl index = null;
        int geoFieldPos = -1;

        if (theIndexName != null) {
            index = (IndexImpl)table.getIndex(theIndexName);
            geoFieldPos = index.getGeoFieldPos();
        }

        FieldValueImpl val;
        int numRanges = (index != null ? theSecKeys.length : thePrimKeys.length);

        if (index != null &&
            theSecKeys[0].size() == geoFieldPos &&
            thePushedExternals[0].length > 0 &&
            thePushedExternals[0][geoFieldPos] != null) {

            oldComputeGeoKeys(rcb);
            return;
        }

        theRTKeys = new RecordValueImpl[numRanges];
        theRTRanges = new FieldRange[numRanges];

        if (index != null) {
            for (int k = 0; k < numRanges; ++k) {
                theRTKeys[k] = index.
                    createIndexKeyFromFlattenedRecord(theSecKeys[k]);
            }
            theIndexEntryDef = index.getIndexEntryDef();
        } else {
            for (int k = 0; k < numRanges; ++k) {
                theRTKeys[k] = table.createPrimaryKey(thePrimKeys[k]);
            }
            theIndexEntryDef = table.getRowDef();
        }

        for (int k = 0; k < numRanges; ++k) {

            PlanIter[] pushedExternals = thePushedExternals[k];

            if (pushedExternals == null ||
                pushedExternals.length == 0) {
                theRTRanges[k] = theRanges[k];
                continue;
            }

            int size = pushedExternals.length;

            // Compute external expressions in the current FieldRange
            if (theRanges[k] != null) {

                FieldRange range = theRanges[k].clone();

                PlanIter lowIter = pushedExternals[size-2];
                PlanIter highIter = pushedExternals[size-1];

                size -= 2;

                if (lowIter != null) {

                    val = oldComputeExternalKey(rcb, lowIter, table, index,
                                                size, FuncCode.OP_GE);
                    if (theAlwaysFalse) {
                        return;
                    }

                    range.setStart(val, range.getStartInclusive(), false);
                }

                if (highIter != null) {

                    val = oldComputeExternalKey(rcb, highIter, table, index,
                                                size, FuncCode.OP_LE);
                    if (theAlwaysFalse) {
                        return;
                    }

                    range.setEnd(val, range.getEndInclusive(), false);
                }

                if (!range.check()) {
                    theAlwaysFalse = true;
                    return;
                }

                if (range.getStart() != null || range.getEnd() != null) {
                    theRTRanges[k] = range;
                } else {
                    theRTRanges[k] = null;
                }

            } else {
                theRTRanges[k] = null;
            }

            oldComputeExternalKeys(rcb, table, index, size, k, pushedExternals);

            if (theAlwaysFalse) {
                return;
            }
        }
    }

    private void oldComputeGeoKeys(RuntimeControlBlock rcb) {

        if (theSecKeys.length != 1) {
            throw new QueryStateException("");
        }

        TableImpl table = theTargetTable;
        IndexImpl index = (IndexImpl)table.getIndex(theIndexName);
        int geoFieldPos = index.getGeoFieldPos();

        Geometry geom = null;
        double dist = -1;
        FieldValueImpl geomVal;
        FieldValueImpl distVal;
        PlanIter geomIter = thePushedExternals[0][geoFieldPos];
        PlanIter distIter = thePushedExternals[0][geoFieldPos + 1];

        geomVal = oldComputeExternalKey(rcb, geomIter, table, index,
                                        geoFieldPos, FuncCode.OP_GE);

        geom = CompilerAPI.getGeoUtils().castAsGeometry(geomVal);

        if (distIter != null) {
            distVal = oldComputeExternalKey(rcb, distIter, table, index,
                                            geoFieldPos, FuncCode.OP_GE);
            dist = ((DoubleValueImpl)distVal).get();
        }

        if (geom == null || (distIter != null && dist <= 0)) {
            theAlwaysFalse = true;
            return;
        }

        List<Pair<String,String>> ranges =
            CompilerAPI.getGeoUtils().
            ranges(geom, dist, rcb.getExecuteOptions());

        int numRanges = ranges.size();
        int numKeys = 0;

        List<String> keys = null;

        if (index.isGeometryIndex()) {
            keys = CompilerAPI.getGeoUtils().keys(ranges);
            numKeys = keys.size();
        }

        theRTKeys = new RecordValueImpl[numRanges + numKeys];
        theRTRanges = new FieldRange[numRanges + numKeys];

        IndexKeyImpl ikey = index.
            createIndexKeyFromFlattenedRecord(theSecKeys[0]);

        String pathName = index.getFieldName(geoFieldPos);
        FieldDefImpl rangeDef = FieldDefImpl.Constants.stringDef;

        for (int i = 0; i < numRanges; ++i) {
            Pair<String,String> range = ranges.get(i);
            FieldRange frange = new FieldRange(pathName, rangeDef, 0);
            frange.setStart(range.first(), true);
            frange.setEnd(range.second(), true);
            theRTRanges[i] = frange;
            theRTKeys[i] = ikey.clone();

            oldComputeExternalKeys(rcb, table, index, geoFieldPos,
                                   i, thePushedExternals[0]);
        }

        if (keys != null) {
            for (int i = numRanges, j = 0; j < numKeys; ++i, ++j) {
                theRTRanges[i] = null;
                theRTKeys[i] = ikey.clone();
                theRTKeys[i].put(geoFieldPos, keys.get(j));

                oldComputeExternalKeys(rcb, table, index, geoFieldPos,
                                       i, thePushedExternals[0]);
            }
        }

        return;
    }

    /*
     * Compute external expressions in each component of the current
     * primary or secondary key
     */
    void oldComputeExternalKeys(
        RuntimeControlBlock rcb,
        TableImpl table,
        IndexImpl index,
        int keySize,
        int rangeIdx,
        PlanIter[] pushedExternals) {

        for (int i = 0; i < keySize; ++i) {

            PlanIter extIter = pushedExternals[i];

            if (extIter == null) {
                continue;
            }

            FieldValueImpl val = oldComputeExternalKey(rcb, extIter, table, index,
                                                       i, FuncCode.OP_EQ);
            if (theAlwaysFalse) {
                return;
            }

            theRTKeys[rangeIdx].put(i, val);
        }
    }

    private FieldValueImpl oldComputeExternalKey(
        RuntimeControlBlock rcb,
        PlanIter iter,
        TableImpl table,
        IndexImpl index,
        int keyPos,
        FuncCode compOp) {

        iter.open(rcb);
        iter.next(rcb);
        FieldValueImpl val = rcb.getRegVal(iter.getResultReg());

        if (iter.next(rcb)) {
            throw new QueryStateException(
                "Pushed external expression returns more than one items");
        }

        iter.close(rcb);

        if (index != null &&
            index.isGeoIndex() &&
            keyPos == index.getGeoFieldPos()) {
            return val;
        }

        FieldValueImpl newVal = BaseTableIter.castValueToIndexKey(
            table, index, keyPos, val, compOp);

        if (newVal != val) {

            if (newVal == BooleanValueImpl.falseValue) {
                theAlwaysFalse = true;
                return newVal;
            }

            if (newVal == BooleanValueImpl.trueValue) {
                val = null;
            } else {
                val = newVal;
            }
        }

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("Computed external key: " + val);
        }

        return val;
    }

    private void initJoins() {

        int numTables = theTableNames.length;

        theJoinLeaves = new boolean[numTables];
        theJoinAncestors = new int[numTables][];
        theJoinAncestors[theTargetTablePos] = new int[0];
        theJoinLeaves[theTargetTablePos] = true;

        for (int i = theTargetTablePos + 1; i < numTables; ++i) {

            TableImpl table = theTables[i];
            theJoinLeaves[i] = true;

            int j;
            for (j = i-1; j >= theTargetTablePos; --j) {

                if (TableImpl.isAncestorOf(table, theTables[j])) {
                    theJoinLeaves[j] = false;
                    int numAncestorsOfParent = theJoinAncestors[j].length;
                    theJoinAncestors[i] = new int[1 + numAncestorsOfParent];
                    for (int k = 0; k < numAncestorsOfParent; ++k) {
                        theJoinAncestors[i][k] = theJoinAncestors[j][k];
                    }
                    theJoinAncestors[i][numAncestorsOfParent] = j;
                    break;
                }
            }

            assert(j >= theTargetTablePos);
        }

        int numLeaves = 0;

        for (int i = theTargetTablePos; i < numTables && numLeaves < 2; ++i) {
            if (theJoinLeaves[i]) {
                ++numLeaves;
            }
        }

        if (numLeaves == 1) {
            theLinearJoin = true;
        } else {
            theLinearJoin = false;
        }

        if (theNumDescendants > 0 || theNumAncestors > 0) {

            theSavedResult = new FieldValueImpl[numTables];
            theJoinPathLength = 0;
            theJoinPath = new JoinPathNode[theNumDescendants + 1];

            for (int i = 0; i <= theNumDescendants; ++i) {
                theJoinPath[i] = new JoinPathNode();
            }
        } else {
            theSavedResult = null;
            theJoinPath = null;
        }

        if (theNumAncestors > 0) {
            theAncestorsPath = new JoinPathNode[theNumAncestors];
            for (int i = 0; i < theNumAncestors; ++i) {
                theAncestorsPath[i] = new JoinPathNode();
            }
        } else {
            theAncestorsPath = null;
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion) {
        throwCannotCall("writeFastExternal");
    }

   private int findTablePos(TableImpl table) {

        for (int i = 0; i < theTables.length; ++i) {
            if (theTables[i] == table) {
                return i;
            }
        }

        throw new QueryStateException(
            "Key does not belong to any table specified in " +
            " a NESTED TABLE clause. Row:\n" + theScanner.getIndexRow());
    }

    private int getJoinParent(int table) {
        return theJoinAncestors[table][theJoinAncestors[table].length - 1];
    }

    @Override
    public int[] getTupleRegs() {
        throwCannotCall("getTupleRegs");
        return null;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        TableIterState state = new TableIterState(this);
        rcb.setState(theStatePos, state);

        rcb.getResumeInfo().ensureTableRI(thePosInJoin);

        if (theTupleRegs != null) {
            TupleValue tuple = new TupleValue((RecordDefImpl)theTypeDef,
                                              rcb.getRegisters(),
                                              theTupleRegs);

            if (theTableNames.length == 1) {
                tuple.setTableAndIndex(theTargetTable, theIndex, false);
            }
            rcb.setRegVal(theResultReg, tuple);
        }

        if (theIndexTupleRegs != null) {
            TupleValue idxtuple = new TupleValue(theIndexEntryDef,
                                                 rcb.getRegisters(),
                                                 theIndexTupleRegs);
            idxtuple.setTableAndIndex(theTargetTable, theIndex, true);

            rcb.setRegVal(theIndexResultReg, idxtuple);

            if (rcb.getTraceLevel() >= 4) {
                rcb.trace("Set result register for index var: " +
                          theIndexResultReg);
            }
        }

        for (int i = 0; i < thePredIters.length; ++ i) {
            if (thePredIters[i] != null) {
                thePredIters[i].open(rcb);
            }
        }

        if (theAlwaysFalse) {
            state.done();
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);

        if (theScanner != null) {
            theExceededSizeLimit = theScanner.exceededSizeLimit();
            theScanner.close();
            theScanner = null;
        }

        theJoinPathLength = 0;
        theHaveResult = false;
        theNextTable = null;

        for (int i = 0; i < thePredIters.length; ++ i) {
            if (thePredIters[i] != null) {
                thePredIters[i].reset(rcb);
            }
        }

        if (thePosInJoin > 0) {

            rcb.getResumeInfo().reset(thePosInJoin);

            if (theExternalKeysIters != null) {

                theAlwaysFalse = false;

                for (PlanIter iter : theExternalKeysIters) {
                    iter.reset(rcb);
                }
                computeIndexScans(rcb);
            }
        }
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state == null) {
            return;
        }

        state.close();

        if (theScanner != null) {
            theScanner.close();
            theScanner = null;
        }

        for (int i = 0; i < thePredIters.length; ++ i) {
            if (thePredIters[i] != null) {
                thePredIters[i].close(rcb);
            }
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (theScanner == null) {

            if (theAlwaysFalse) {
                state.done();
                return false;
            }

            /*
             * This call will return an index scanner if secKey is not null,
             * otherwise it returns a primary key scanner.
             */
            theScanner = theFactory.getTableScanner(
                    rcb.getResumeInfo().getCurrentPid(),
                    theDirection,
                    thePosInJoin,
                    theTables,
                    theNumAncestors,
                    theIndex,
                    theRTKeys,
                    theRTRanges,
                    theIsUpdate,
                    theLockIndexEntries,
                    theUsesCoveringIndex,
                    theExceededSizeLimit,
                    theVersion);
        }

        boolean more;

        if (theNumDescendants > 0 || theNumAncestors > 0) {

            if (theHaveResult) {
                produceResult(rcb);
                theHaveResult = false;
                rcb.getResumeInfo().setMoveAfterResumeKey(thePosInJoin, true);
                more = true;
            } else if (rcb.needToSuspend()) {
                if (rcb.getTraceLevel() >= 2) {
                    trace(rcb, "query can now suspend");
                }
                rcb.resetCannotSuspend();
                state.done();
                return false;
            } else {
                more = nestedTablesNext(rcb, state);
            }

            if (more) {
                /*
                 * Populate the regs with the results. Do the target table
                 * first. If it's using a covering secondary index, populate
                 * theIndexTupleRegs with the fields of the index entry.
                 * Otherwise populate theTupleRegs.
                 */
                if (theUsesCoveringIndex[theTargetTablePos] &&
                    thePrimKeys == null) {
                    RecordValueImpl idxRow = (RecordValueImpl)
                        theSavedResult[theTargetTablePos];
                    for (int i = 0; i < idxRow.getNumFields(); ++i) {
                        rcb.setRegVal(theIndexTupleRegs[i], idxRow.get(i));
                    }
                } else {
                    rcb.setRegVal(theTupleRegs[theTargetTablePos],
                                  theSavedResult[theTargetTablePos]);
                }

                for (int i = 0; i < theTables.length; ++i) {
                    if (i == theTargetTablePos) {
                        continue;
                    }
                    rcb.setRegVal(theTupleRegs[i], theSavedResult[i]);
                }
            }

            return more;
        }

        if (rcb.needToSuspend()) {
            if (rcb.getTraceLevel() >= 2) {
                trace(rcb, "query can now suspend");
            }
            rcb.resetCannotSuspend();
            state.done();
            return false;
        }

        return simpleNext(rcb, state);
    }

    public boolean simpleNext(RuntimeControlBlock rcb, PlanIterState state) {

        RecordValueImpl indexRow = null;
        ResumeInfo ri = rcb.getResumeInfo();

        if (state.isOpen()) {
            state.setState(StateEnum.RUNNING);
        }

        PlanIter filterIter = getTargetTablePred();

        int[] tupleRegs;
        int resultReg;
        if (theIndexTupleRegs != null) {
            tupleRegs = theIndexTupleRegs;
            resultReg = theIndexResultReg;
        } else {
            tupleRegs = theTupleRegs;
            resultReg = theResultReg;
        }

        try {
            boolean more = theScanner.next(null);

            while (more) {

                if (filterIter != null) {

                    indexRow = theScanner.getIndexRow();

                    /* If jsonCollection, add the primary key fields to
                     * the map and set the result to the map, which
                     * is MapValueImpl. */
                    if (indexRow instanceof JsonCollectionRowImpl) {
                        JsonCollectionRowImpl srow =
                            (JsonCollectionRowImpl) indexRow;

                        srow.addPrimKeyAndPropertyFields(
                            theScanner.expirationTime(),
                            theScanner.creationTime(),
                            theScanner.modificationTime(),
                            theScanner.partitionId(),
                            (theIndex == null ?
                             theScanner.rowStorageSize() : 0),
                            theScanner.indexStorageSize());
                        rcb.setRegVal(theResultReg, srow.getJsonCollectionMap());

                    } else {
                        /* Populate the appropriate registers */
                        for (int i = 0; i < indexRow.getNumFields(); ++i) {
                            rcb.setRegVal(tupleRegs[i], indexRow.get(i));
                        }

                        TupleValue tv = (TupleValue)rcb.getRegVal(resultReg);
                        tv.setExpirationTime(theScanner.expirationTime());
                        tv.setModificationTime(theScanner.modificationTime());
                        tv.setCreationTime(theScanner.creationTime());
                        tv.setPartition(theScanner.partitionId());
                        tv.setIndexStorageSize(theScanner.indexStorageSize());

                        /*
                         * Don't get the row size if the query is using a secondary
                         * index, because (a) doing so will access the LN and
                         * (b) it's not necessary as the row_storage_size() function
                         * cannot appear in an index-filtering predicate for a
                         * secondary index.
                         */
                        if (theIndex == null) {
                            tv.setStorageSize(theScanner.rowStorageSize());
                        }
                    }

                    /* Compute the filter condition */
                    boolean match = filterIter.next(rcb);

                    if (match) {
                        FieldValueImpl val =
                            rcb.getRegVal(filterIter.getResultReg());
                        match = (val.isNull() ? false : val.getBoolean());
                    }

                    filterIter.reset(rcb);

                    if (!match) {
                        if (rcb.getTraceLevel() >= 2) {
                            trace(rcb, "Filtered-out index row :\n " + indexRow);
                        }

                        more = theScanner.next(null);
                        continue;
                    }
                }

                if (theUsesCoveringIndex[0]) {
                    if (theLockIndexEntries || theScanner.lockIndexRow()) {

                        if (indexRow == null) {
                            indexRow = theScanner.getIndexRow();
                        }

                        if (indexRow instanceof JsonCollectionRowImpl) {
                            JsonCollectionRowImpl srow =
                                (JsonCollectionRowImpl) indexRow;

                            srow.addPrimKeyAndPropertyFields(
                                theScanner.expirationTime(),
                                theScanner.creationTime(),
                                theScanner.modificationTime(),
                                theScanner.partitionId(),
                                theScanner.rowStorageSize(),
                                theScanner.indexStorageSize());
                            rcb.setRegVal(theResultReg,
                                          srow.getJsonCollectionMap());
                        } else {
                            for (int i = 0; i < indexRow.getNumFields(); ++i) {
                                rcb.setRegVal(tupleRegs[i], indexRow.get(i));
                            }

                            TupleValue tv = (TupleValue)rcb.getRegVal(resultReg);
                            tv.setExpirationTime(theScanner.expirationTime());
                            tv.setCreationTime(theScanner.creationTime());
                            tv.setModificationTime(theScanner.modificationTime());
                            tv.setPartition(theScanner.partitionId());
                            tv.setIndexStorageSize(theScanner.indexStorageSize());
                            if (theIndex == null) {
                                tv.setStorageSize(theScanner.rowStorageSize());
                            }

                            if (rcb.getTraceLevel() >= 4) {
                                trace(rcb, "ServerTableIter: row storage size = " +
                                      tv.getStorageSize());
                                trace(rcb, "ServerTableIter: index storage size = " +
                                      tv.getIndexStorageSize());
                                trace(rcb, "ServerTableIter: partition id = " +
                                      tv.getPartition());
                            }
                        }

                        return true;
                    }
                } else {

                    if (theIndexTupleRegs != null) {
                        indexRow = theScanner.getIndexRow();
                        for (int i = 0; i < indexRow.getNumFields(); ++i) {
                            rcb.setRegVal(theIndexTupleRegs[i], indexRow.get(i));
                        }
                    }

                    RowImpl tableRow = theScanner.getTableRow();

                    if (tableRow != null) {

                        if (rcb.getTraceLevel() >= 3) {
                            trace(rcb, "Produced row: " + tableRow);
                        }

                        if (!theIsUpdate) {
                            /*
                             * If jsonCollection, add the primary key fields to
                             * the map and set the result to the map, which
                             * is MapValueImpl.
                             */
                            if (tableRow instanceof JsonCollectionRowImpl) {
                                JsonCollectionRowImpl srow =
                                    (JsonCollectionRowImpl) tableRow;
                                srow.addPrimKeyAndPropertyFields(
                                    theScanner.indexStorageSize());
                                rcb.setRegVal(theResultReg,
                                              srow.copyJsonCollectionMap());
                                return true;
                            }

                            for (int i = 0; i < tableRow.getNumFields(); ++i) {
                                rcb.setRegVal(theTupleRegs[i], tableRow.get(i));
                            }

                            TupleValue tv = (TupleValue)
                                rcb.getRegVal(theResultReg);
                            tv.setExpirationTime(tableRow.getExpirationTime());
                            tv.setModificationTime(tableRow.getLastModificationTime());
                            tv.setCreationTime(tableRow.getCreationTime());
                            tv.setPartition(tableRow.getPartition());
                            tv.setStorageSize(tableRow.getStorageSize());
                            tv.setIndexStorageSize(theScanner.indexStorageSize());
                            tv.setVersion(tableRow.getVersion());
                            tv.setRowMetadata(tableRow.getRowMetadata());
                        } else {
                            rcb.setRegVal(theResultReg, tableRow);
                        }

                        return true;
                    } else if (rcb.getTraceLevel() >= 2) {
                        trace(rcb, "Failed to access table row for" +
                              " index row");
                    }
                }

                more = theScanner.next(null);
                indexRow = null;
            }
        } catch (SizeLimitException sle) {

            if (rcb.getTraceLevel() >= 2) {
                trace(rcb, "Suspending due to SizeLimitException. MaxReadKB = " +
                          rcb.getMaxReadKB() + " CurrentMaxReadKB = " +
                          rcb.getCurrentMaxReadKB());
            }

            /* rcb.setReachedLimit() sets the needToSuspend flag as well */
            rcb.setReachedLimit();
            ri.setMoveAfterResumeKey(thePosInJoin, sle.getAfterReadEntry());

        } catch (TimeoutException e) {

            /* Note: the reachedLimit flag has been set already (see
             * rcb.checkItemout()) */
            if (rcb.getTraceLevel() >= 1) {
                trace(rcb, "Suspending due to TimeoutException");
            }
        }

        state.done();
        return false;
    }

    private boolean nestedTablesNext(
        RuntimeControlBlock rcb,
        PlanIterState state) {

        ResumeInfo ri = rcb.getResumeInfo();
        TableImpl table;
        boolean more;

        try {
            while (true) {

                if (thePosInJoin < rcb.getNumJoinBranches() - 1 &&
                    theProducedDelayedResult) {

                    if (rcb.getTraceLevel() >= 1) {
                        trace(rcb, "resetting theCannotSuspendDueToDelayedResult");
                    }
                    rcb.resetCannotSuspend2();
                }
                theProducedDelayedResult = false;

                /*
                 * Step 1: Get the next row to consider. Let R be this row and
                 * T be its containing table.
                 */
                if (state.isOpen()) {
                    state.setState(StateEnum.RUNNING);

                    if (ri.getPrimResumeKey(thePosInJoin) != null) {
                        more = resume(rcb);
                    } else {
                        more = theScanner.next(theTargetTable);
                    }
                } else {
                    more = theScanner.next(theNextTable);
                }

                if (!more) {
                    break;
                }

                theNextTable = null;
                boolean match = true;

                table = theScanner.getTable();
                int tablePos = findTablePos(table);

                if (rcb.getTraceLevel() >= 2) {
                    trace(rcb, "Current index row for table " +
                          table.getFullName() + " : \n" +
                          theScanner.getIndexRow());
                }

                /*
                 * Step 2: theJoinPath is empty. If T is not the target table,
                 * we can skip R and all subsequent rows until we get the next
                 * target-table row. Otherwise, if T is the target table, then
                 * (a) If there are any A tables, get the A rows associated
                 *     with R.
                 * (b) Apply the filtering pred, if any, on R. If pred not
                 *     satisfied skip R and all rows until next target-table
                 *     row. Else add R to theJoinPath and if there are no
                 *     other D tables, produce a result and return true.
                 *     Else, continue with next row.
                 *
                 * Note: we get the A rows before applying the filter pred
                 * in order to avoid deadlock. Otherwise, if the pred is
                 * satisfied, R will be locked when we go to get its A rows,
                 * so we will be holding 2 locks at a time (acquired in
                 * reverse order). TODO: optimize for the case when the
                 * filtering pred is not satisfied? To do this we need a JE
                 * api to release the lock held by a cursor, without closing
                 * the cursor.
                 */
                if (theJoinPathLength == 0) {

                    if (tablePos != theTargetTablePos) {
                        theNextTable = theTargetTable;
                        continue;
                    }

                    if (theNumAncestors > 0) {
                        if (theScanner.exceededSizeLimit()) {
                            throw new SizeLimitException();
                        }
                        addAncestorValues(rcb, theScanner.getPrimKeyBytes());
                    }

                    match = addRowToJoinPath(rcb, theScanner, theJoinPath,
                                             0, tablePos);

                    if (!match) {
                        theNextTable = table;
                    } else if (theJoinLeaves[tablePos]) {
                        produceResult(rcb);
                        return true;
                    }

                    continue;
                }

                /*
                 * Step 3: Find the closest ancestor to T that is in theJoinPath.
                 * Let AT be this table. Notice that if T is in theJoinPath
                 * already, AT is T itself. ancJPP is the position of AT within
                 * theJoinPath.
                 */
                int ancJPP = getAncestorPosInJoinPath(tablePos);
                int ancTablePos = theJoinPath[ancJPP].tablePos;
                TableImpl ancTable = theTables[ancTablePos];

                if (rcb.getTraceLevel() >= 3) {
                    trace(rcb, "Join path ancestor for table " +
                          table.getFullName() + " is table " +
                          ancTable.getFullName() +
                          ", at join path pos = " + ancJPP +
                          ". Ancestor node = " + theJoinPath[ancJPP]);
                }

                /*
                 * Step 4: Produce delayed result, if necessary. Let L be the
                 * leaf node in the join path. If T is not a proper descendant
                 * of L table, the L row cannot match with R or any row after R,
                 * so if L has not matched with any preceding rows, theJoinPath
                 * stores a delayed result. We save this result until we are
                 * done processing R and then return it to the caller. To handle
                 * the case where this result turns out to be the last result of
                 * the current batch, we set the MoveAfterResumeKey flag to
                 * false so that the next batch will start with row R.
                 */
                if ((ancJPP < theJoinPathLength-1 || ancTablePos == tablePos) &&
                    !theJoinPath[theJoinPathLength-1].matched) {
                    produceResult(rcb);
                    theProducedDelayedResult = true;
                    if (thePosInJoin < rcb.getNumJoinBranches() - 1) {
                        if (rcb.getTraceLevel() >= 1) {
                            trace(rcb, "1. Cannot suspend due to delayed " +
                                  "result in join");
                        }
                        rcb.setCannotSuspend2();
                    }
                    ri.setMoveAfterResumeKey(thePosInJoin, false);
                }

                /*
                 * Step 5: If T is not a child of any table in theJoinPath, R
                 * does not join with any table, so we skip it. Furthermore,
                 * we truncate theJoinPath by throwing away any nodes below
                 * ancJPP, since these nodes cannot match with any row after R.
                 */
                if (tablePos != ancTablePos &&
                    getJoinParent(tablePos) != ancTablePos) {

                    theJoinPathLength = ancJPP + 1;

                    if (theProducedDelayedResult) {
                        return true;
                    }

                    continue;
                }

                /*
                 * Step 6: If T is a child of a table P in theJoinPath (T itself
                 * may or may not be in the join path) we check whether R and
                 * the P row satisfy the join predicate.
                 */
                if (tablePos != theTargetTablePos) {
                    match = doJoin(rcb,
                                   ancJPP - (ancTablePos == tablePos ? 1 : 0));
                }

                /*
                 * Step 7: If the join failed, R is skipped. However, we must
                 * first check for a delayed result. For example, consider our
                 * sample tree1, let theJoinPath contain rows RA and RB, and
                 * the current row be RD (R = RD, T = D). RD does not match
                 * with RB, and neither will any row after RD, so, if RB has
                 * not been matched already, we have a delayed result. This
                 * case is not captured in step 4 above.
                 *
                 * If T is a child of the target table, the fact that R does
                 * not match with the current target-table row means that we
                 * have moved past that row and all its descendant rows, so
                 * we can skip all rows until the next target-table row.
                 * Otherwise, if the join tree is just a linear path, we can
                 * skip all rows under T.
                 *
                 * Note: If the join is not linear, we cannot skip any rows
                 * using the max-key-components optimization. To see why,
                 * consider sample join tree2. Assume that we have a C row that
                 * does not match with the B row in theJoinPath. If we skip all
                 * rows having more key components than C, we may skip a K row
                 * that matches with the current A row in theJoinPath.
                 */
                if (!match) {

                    if (getJoinParent(tablePos) == theTargetTablePos) {
                        theNextTable = theTargetTable;
                    } else if (theLinearJoin) {
                        theNextTable = table;
                    }

                    if (!theJoinPath[theJoinPathLength-1].matched) {
                        produceResult(rcb);
                        theProducedDelayedResult = true;
                        if (thePosInJoin < rcb.getNumJoinBranches() - 1) {
                            if (rcb.getTraceLevel() >= 1) {
                                trace(rcb, "2. Cannot suspend due to delayed " +
                                      "result in join");
                            }
                            rcb.setCannotSuspend2();
                        }
                    }

                    if (theProducedDelayedResult) {
                        return true;
                    }

                    continue;
                }

                /*
                 * Step 8: If T is the target table, add the matching ancestor
                 * rows. As before, in order to avoid deadlocks, this is done
                 * before locking R and adding it to theJoinPath.
                 */
                if (tablePos == theTargetTablePos && theNumAncestors > 0) {
                    if (theScanner.exceededSizeLimit()) {
                        throw new SizeLimitException();
                    }
                    addAncestorValues(rcb, theScanner.getPrimKeyBytes());
                }

                /*
                 * Step 9: Apply the ON/filtering pred, if any, on R. If the pred
                 * is not satisfied skip R, after returning any delayed result.
                 * Otherwise, lock and add R to theJoinPath.
                 *
                 * We can apply the max-key-components optimization to skip
                 * more rows than R if the join is linear, or T is the target
                 * table.
                 */
                match = addRowToJoinPath(rcb, theScanner, theJoinPath,
                                         ancJPP +
                                         (ancTablePos == tablePos ? 0 : 1),
                                         tablePos);
                if (!match) {

                    if (tablePos == theTargetTablePos || theLinearJoin) {
                        theNextTable = table;
                    }

                    if (theProducedDelayedResult) {
                        return true;
                    }
                    continue;
                }

                /*
                 * Step 10: At this point R has been added to theJoinPath. If
                 * T is a leaf of the join tree, we have a "current" result,
                 * which we can return immediately, unless a delayed result
                 * was also recognized during the current nestedTablesNext()
                 * invocation. In the later case, we return the delayed result
                 * here and set theHaveResult to true so that in the next
                 * invocation of nestedTablesNext() we will return the cached
                 * "current" result without moving the scanner at all.
                 */
                if (theJoinLeaves[tablePos]) {
                    assert(tablePos ==
                           theJoinPath[theJoinPathLength-1].tablePos);
                    if (!theProducedDelayedResult) {
                        produceResult(rcb);
                    } else {
                        theHaveResult = true;
                        ri.setMoveAfterResumeKey(thePosInJoin, false);
                    }

                    return true;
                }

                /*
                 * Step 11: Return any delayed result.
                 */
                if (theProducedDelayedResult) {
                    return true;
                }
            }

        } catch (SizeLimitException sle) {

            if (rcb.getTraceLevel() >= 2) {
                trace(rcb, "Suspending due to SizeLimitException. theInResume = " +
                      theInResume + " producedDelayedResult = " +
                      theProducedDelayedResult +
                      " moveAfterResumeKey = " + sle.getAfterReadEntry());
            }

            rcb.setReachedLimit();
            state.done();

            /*
             * If the SLE occurred after a delayed result was recognized,
             * return that result now.
             */
            if (theProducedDelayedResult) {
                return true;
            }

            /*
             * If the SLE occurred while in resume(), don't change the resume
             * info (we will resume with the same continuation key). Otherwise,
             * save the new join path and set theMoveAfterResumeKey to false
             * so that we will resume with the current row R.
             */
            if (!theInResume) {
                ri.setMoveAfterResumeKey(thePosInJoin, sle.getAfterReadEntry());
                saveJoinPath(rcb);
            }

            return false;

        } catch (TimeoutException e) {

            /* Note: the reachedLimit flag has been set already (see
             * rcb.checkItemout()) */
            if (rcb.getTraceLevel() >= 2) {
                trace(rcb, "Suspending due to TimeoutException");
            }

            state.done();

            if (theProducedDelayedResult) {
                return true;
            }

            if (!theInResume) {
                saveJoinPath(rcb);
            }

            return false;
        }

        /*
         * We are done. However, anything that remains in theJoinPath is a
         * delayed result that must be returned now.
         */
        state.done();

        if (theJoinPathLength > 0 &&
            !theJoinPath[theJoinPathLength-1].matched) {

            theProducedDelayedResult = true;
            if (thePosInJoin < rcb.getNumJoinBranches() - 1) {
                if (rcb.getTraceLevel() >= 1) {
                    trace(rcb, "3. Cannot suspend due to delayed result in join");
                }
                rcb.setCannotSuspend2();
            }
            produceResult(rcb);
            return true;
        }

        return false;
    }

    private int getAncestorPosInJoinPath(int tablePos) {

        for (int i = theJoinPathLength - 1; i >= 0; --i) {

            int joinTablePos = theJoinPath[i].tablePos;

            if (tablePos == joinTablePos) {
                return i;
            }

            int[] ancestors = theJoinAncestors[tablePos];
            for (int j = ancestors.length - 1; j >= 0; --j) {
                if (ancestors[j] == joinTablePos) {
                    return i;
                }
            }
        }

        throw new QueryStateException(
            "Table does not have an ancestor in the join path. Table: " +
            theTables[tablePos].getFullName() + "\njoin path length = " +
            theJoinPathLength);
    }

    /**
     * Join the current row with the row in the jpp position of theJoinPath.
     * Return true if the rows match; false otherwise.
     */
    private boolean doJoin(RuntimeControlBlock rcb, int jpp) {

        TableImpl outerTable = theTables[theJoinPath[jpp].tablePos];

        byte[] outerKey = theJoinPath[jpp].primKeyBytes;
        byte[] innerKey = theScanner.getPrimKeyBytes();
        innerKey = Key.getPrefixKey(innerKey, outerTable.getNumKeyComponents());

        if (rcb.getTraceLevel() >= 2) {
            trace(rcb, "Join at path pos " + jpp +
                  "\nouter row:\n" + theJoinPath[jpp].row +
                  "\ninner row:\n" + theScanner.getIndexRow());
        }

        boolean res = Arrays.equals(innerKey, outerKey);

        if (!res && rcb.getTraceLevel() >= 2) {
            trace(rcb, "Join failed");
        }

        return res;
    }

    /**
     * Add a row/key to a given path, which is either theJoinPath or
     * theAncestorsPath. The row is added only if it can be locked successfully
     * and it satisfies the ON/filtering pred associated with its table, if
     * any. The row/key is added at a given position in the path, and in the
     * case of theJoinPath, any nodes after that position are removed from the
     * path.
     */
    private boolean addRowToJoinPath(
        RuntimeControlBlock rcb,
        TableScanner scanner,
        JoinPathNode[] path,
        int pathPos,
        int tablePos) throws SizeLimitException {

        boolean lockedRow = false;
        RecordValueImpl indexRow = null;

        /*
         * tableRow will actually be set to indexRow, if the index is covering.
         * Otherwise, it will be set to a true table row (i.e., a RowImpl).
         */
        RecordValueImpl tableRow = null;

        if (thePredIters.length > 0 && thePredIters[tablePos] != null) {

            /*
             * Populate the appropriate registers for evaluating the pred.
             */
            if (tablePos == theTargetTablePos) {

                indexRow = scanner.getIndexRow();

                if (thePrimKeys != null) {
                    rcb.setRegVal(theTupleRegs[tablePos], indexRow);
                } else {
                    for (int i = 0; i < indexRow.getNumFields(); ++i) {
                        rcb.setRegVal(theIndexTupleRegs[i], indexRow.get(i));
                    }
                }

            } else {

                if (theUsesCoveringIndex[tablePos]) {
                    if (scanner.lockIndexRow()) {
                        indexRow = scanner.getIndexRow();
                        tableRow = indexRow;
                        lockedRow = true;
                    }
                } else {
                    tableRow = scanner.getTableRow();
                    if (tableRow != null) {
                        lockedRow = true;
                    }
                }

                if (!lockedRow) {
                    return false;
                }

                if (rcb.getTraceLevel() >= 2) {
                    trace(rcb, "Evaluating ON predicate on : " + tableRow);
                }

                for (int i = 0; i < theNumAncestors; ++i) {
                    rcb.setRegVal(theTupleRegs[i], theAncestorsPath[i].row);
                }

                if (path == theJoinPath && theJoinPathLength > 0) {

                    if (thePrimKeys == null &&
                        theUsesCoveringIndex[theTargetTablePos]) {

                        RecordValueImpl targetIndexRow = (RecordValueImpl)
                            theJoinPath[0].row;

                        for (int i = 0; i < targetIndexRow.getNumFields(); ++i) {
                            rcb.setRegVal(theIndexTupleRegs[i],
                                          targetIndexRow.get(i));
                        }
                    } else {
                        rcb.setRegVal(theTupleRegs[theTargetTablePos],
                                      theJoinPath[0].row);
                    }

                    for (int i = 1; i < theJoinPathLength; ++i) {
                        rcb.setRegVal(theTupleRegs[theJoinPath[i].tablePos],
                                      theJoinPath[i].row);
                    }
                }

                rcb.setRegVal(theTupleRegs[tablePos], tableRow);
            }

            /*
             * Evaluate the pred. Return false if not satified.
             */
            PlanIter predIter = thePredIters[tablePos];

            boolean match = predIter.next(rcb);

            if (match) {
                FieldValueImpl val = rcb.getRegVal(predIter.getResultReg());
                match = (val.isNull() ? false : val.getBoolean());
            }

            predIter.reset(rcb);

            if (!match) {
                if (rcb.getTraceLevel() >= 2) {
                    trace(rcb, "ON predicate failed on table : " +
                          theTableNames[tablePos]);
                }
                return false;
            }
        }

        /*
         * Lock and read the row, if not done already
         */
        if (!lockedRow) {
            if (theUsesCoveringIndex[tablePos]) {
                if (scanner.lockIndexRow()) {
                    if (indexRow == null) {
                        indexRow = scanner.getIndexRow();
                    }
                    tableRow = indexRow;
                    lockedRow = true;
                }
            } else {
                tableRow = scanner.getTableRow();
                if (tableRow != null) {
                    lockedRow = true;
                }
            }

            if (!lockedRow) {
                return false;
            }
        }

        /*
         * Add row to the given path.
         */
        path[pathPos].reset(tablePos, tableRow, scanner.getPrimKeyBytes());

        if (path == theJoinPath) {

            theJoinPathLength = pathPos + 1;

            if (pathPos > 0) {
                path[pathPos - 1].matched = true;
            }

            if (tablePos == theTargetTablePos &&
                theIndexName != null) {
                theJoinPathSecKey = scanner.getSecKeyBytes();
            }
        }

        if (rcb.getTraceLevel() >= 2) {
            String pathstr = (path == theJoinPath ?
                              "join path" :
                              "ancestors path");
            trace(rcb, "Added node to " + pathstr + " at position : " + pathPos +
                      ". Node : " + path[pathPos]);
        }

        return true;
    }

    private void produceResult(RuntimeControlBlock rcb) {

        for (int i = 0; i < theNumAncestors; ++i) {
            theSavedResult[i] = theAncestorsPath[i].row;

            if (rcb.getTraceLevel() >= 2) {
                trace(rcb, "Saved anestor path row: " + theAncestorsPath[i].row);
            }
        }

        for (int i = theNumAncestors; i < theTables.length; ++i) {
            theSavedResult[i] = theNULL;
        }

        theJoinPath[theJoinPathLength-1].matched = true;

        saveJoinPath(rcb);

        /* Remove the last node from theJoinPath; it's not needed any more */
        --theJoinPathLength;
    }

    /**
     * Save info from the join path to the RCB, to be used as resume info.
     */
    private void saveJoinPath(RuntimeControlBlock rcb) {

        int[] joinPathTables = null;
        byte[] joinPathLastKey = null;
        boolean joinPathMatched = true;

        if (theJoinPathLength > 0) {

            joinPathTables = new int[theJoinPathLength];
            joinPathLastKey = theJoinPath[theJoinPathLength-1].primKeyBytes;
            joinPathMatched = theJoinPath[theJoinPathLength-1].matched;

            for (int i = 0; i < theJoinPathLength; ++i) {

                int table = theJoinPath[i].tablePos;

                theSavedResult[table] = theJoinPath[i].row;

                joinPathTables[i] = table;

                if (rcb.getTraceLevel() >= 2) {
                    trace(rcb, "Saved join path row: " + theJoinPath[i].row);
                }
            }
        }

        rcb.getResumeInfo().setJoinPath(thePosInJoin,
                                        joinPathTables,
                                        joinPathLastKey,
                                        theJoinPathSecKey,
                                        joinPathMatched);
    }

    private void addAncestorValues(
        RuntimeControlBlock rcb,
        byte[] targetKey) throws SizeLimitException {

        boolean match = false;

        AncestorScanner ancScanner =
            theFactory.getAncestorScanner(theScanner.getOp(), !theInResume);

        for (int i = 0; i < theNumAncestors; ++i) {

            TableImpl ancTable = theTables[i];

            byte[] ancKey =
                Key.getPrefixKey(targetKey, ancTable.getNumKeyComponents());

            if (rcb.getTraceLevel() >= 2) {
                trace(rcb, "Adding ancestor row for ancestor table " +
                      ancTable.getFullName() + " at pos " + i);
            }

            if (theAncestorsPath[i].tablePos >= 0 &&
                Arrays.equals(ancKey, theAncestorsPath[i].primKeyBytes)) {

                if (rcb.getTraceLevel() >= 2) {
                    trace(rcb, "Ancestor row is already in ancestors path " +
                          ". Ancestor index row =\n" + theAncestorsPath[i].row);
                }

                continue;
            }

            try {
                ancScanner.init(ancTable, null, ancKey, null);

                boolean more = ancScanner.next(null);

                if (rcb.getTraceLevel() >= 2) {
                    trace(rcb, "Ancestor row retrieved. " +
                          "Ancestor index row =\n" + ancScanner.getIndexRow());
                }

                if (!more) {
                    if (rcb.getTraceLevel() >= 2) {
                        trace(rcb, "Join failed");
                    }
                    theAncestorsPath[i].reset(i, theNULL, null);
                    continue;
                }

                match = addRowToJoinPath(rcb, ancScanner,
                                         theAncestorsPath,
                                         i,
                                         i);
                if (!match) {
                    if (rcb.getTraceLevel() >= 2) {
                        trace(rcb, "ON predicate failed on ancestor row");
                    }
                    theAncestorsPath[i].reset(i, theNULL, null);
                }
            } finally {
                ancScanner.close();
            }
        }
    }

    private boolean resume(RuntimeControlBlock rcb)
        throws SizeLimitException, TimeoutException {

        if (rcb.getTraceLevel() >= 2) {
            trace(rcb, "RESUME STARTS");
        }

        theInResume = true;

        ResumeInfo ri = rcb.getResumeInfo();

        int[] joinPathTables = ri.getJoinPathTables(thePosInJoin);
        byte[] joinPathKey = ri.getJoinPathKey(thePosInJoin);
        int joinPathLen;
        TableImpl nextTable = null;

        if (joinPathKey == null) {
            if (rcb.getTraceLevel() >= 2) {
                trace(rcb, "RESUME DONE with null joinPathKey");
            }

            /*
             * If the next() call below throws an SLE without moving
             * the cursor at all, we should resume with exactly the same
             * resume info. So, don't set theInResume to false in this case.
             */
            boolean more = false;
            byte[] primResumeKey = ri.getPrimResumeKey(thePosInJoin);
            byte[] descResumeKey = ri.getDescResumeKey(thePosInJoin);

            try {
                more = theScanner.next(theTables[theTargetTablePos]);
            } catch (SizeLimitException | TimeoutException e) {

                if (!Arrays.equals(primResumeKey, ri.getPrimResumeKey(thePosInJoin)) ||
                    !Arrays.equals(descResumeKey, ri.getDescResumeKey(thePosInJoin))) {
                    theInResume = false;
                }
                throw e;
            }

            theInResume = false;
            return more;
        }

        /* Don't charge cost during resume */
        AncestorScanner ancScanner =
            theFactory.getAncestorScanner(theScanner.getOp(),
                                          false /* chargeCost */);

        joinPathLen = joinPathTables.length;
        joinPathKey = ri.getJoinPathKey(thePosInJoin);

        byte[] prevPrimKey = null;

        for (int i = 0; i < joinPathLen; ++i) {

            int tablePos = joinPathTables[i];
            TableImpl table = theTables[tablePos];
            IndexImpl index = null;
            byte[] primKey = null;
            byte[] secKey = null;

            primKey = Key.getPrefixKey(joinPathKey,
                                       table.getNumKeyComponents());

            if (i == 0 && theIndexName != null) {
                index = (IndexImpl)table.getIndex(theIndexName);
                secKey = ri.getJoinPathSecKey(thePosInJoin);
            }

            ancScanner.init(table, index, primKey, secKey);

            try {
                boolean more = ancScanner.next(null);

                if (rcb.getTraceLevel() >= 2) {
                    trace(rcb, "Got index row:\n" + ancScanner.getIndexRow() +
                          "\nat join path pos " + i);
                }

                if (!more ||
                    !addRowToJoinPath(rcb, ancScanner, theJoinPath,
                                      i, tablePos)) {

                    if (rcb.getTraceLevel() >= 2) {
                        trace(rcb, "Could not find ancestor row for table " +
                              table.getFullName() + " at join path pos " +
                              i);
                    }

                    if (tablePos == theTargetTablePos || theLinearJoin) {
                        nextTable = table;
                    }

                    if (theIndex != null) {
                        ri.setDescResumeKey(thePosInJoin, prevPrimKey);
                    }

                    break;
                }

                if (tablePos == theTargetTablePos && theNumAncestors > 0) {
                    addAncestorValues(rcb, ancScanner.getPrimKeyBytes());
                }
            } finally {
                ancScanner.close();
            }

            theJoinPath[joinPathLen-1].matched = ri.getJoinPathMatched(thePosInJoin);
            prevPrimKey = primKey;
        }

        if (rcb.getTraceLevel() >= 2) {
            trace(rcb, "RESUME DONE");
        }

        boolean more = false;
        byte[] primResumeKey = ri.getPrimResumeKey(thePosInJoin);
        byte[] descResumeKey = ri.getDescResumeKey(thePosInJoin);

        try {
            more = theScanner.next(nextTable);

        } catch (SizeLimitException | TimeoutException e) {

            if (!Arrays.equals(primResumeKey, ri.getPrimResumeKey(thePosInJoin)) ||
                !Arrays.equals(descResumeKey, ri.getDescResumeKey(thePosInJoin))) {
                theInResume = false;
            }
            throw e;
        }

        theInResume = false;
        return more;
    }

    private void throwCannotCall(String method) {
        throw new QueryStateException(
            "ServerTableIter: " + method + " cannot be called");
    }

    private void trace(RuntimeControlBlock rcb, String msg) {
        rcb.trace("TableIter in join pos " + thePosInJoin + ": " + msg);
    }
}
