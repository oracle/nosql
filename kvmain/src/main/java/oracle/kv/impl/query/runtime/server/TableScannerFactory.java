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

import static oracle.kv.impl.api.ops.InternalOperationHandler.MIN_READ;
import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;

import java.util.HashSet;
import java.util.logging.Level;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.IndexKeysIterateHandler;
import oracle.kv.impl.api.ops.IndexScanner;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.InternalOperationHandler;
import oracle.kv.impl.api.ops.MultiGetTableKeysHandler;
import oracle.kv.impl.api.ops.MultiTableOperationHandler;
import oracle.kv.impl.api.ops.MultiTableOperationHandler.OperationTableInfo;
import oracle.kv.impl.api.ops.OperationHandler;
import oracle.kv.impl.api.ops.Scanner;
import oracle.kv.impl.api.ops.TableQuery;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexKeyImpl;
import oracle.kv.impl.api.table.IndexRange;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.table.FieldRange;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

/**
 * This class serves 2 purposes:
 * (a) It stores some operational context that cannot be stored in the RCB
 *     because the associated java classes are server-only code.
 * (b) Serves as a factory for 2 classes (PrimaryTableScanner and
 *     SecondaryTableScanner) that perform scans over the primary and a
 *     secondary index, respectively.
 *
 * The scanner classes provide a common api to ServerTableIter. Both classes
 * scan their associated index using dirty reads and key-only access and make
 * the scanned index entries available to the ServerTableIter via the
 * nextIndexRow() api. Then, if needed, the ServerTableIter can lock and get
 * the full table row via the currentTableRow() method. This 2-step table
 * access allows filtering based on index entry fields to be done before
 * fetching the full rows, thus avoiding fetching rows that do not survive
 * the filtering.
 *
 * An instance of this class is created in the ServerTableIter constructor,
 * before the server-side query operation starts.
 */
public class TableScannerFactory {

    /**
     * A test hook after the scanner obtained the next record but before it does
     * anything with it.
     */
    public static volatile TestHook<PrimaryTableScannerHookObject>
        primaryTableScanHookObtainedNext =
        null;

    /**
     * Object for the primaryTableScanHookObject.
     */
    public static class PrimaryTableScannerHookObject {
        public final TableQuery op;
        public final MultiGetTableKeysHandler handler;

        public PrimaryTableScannerHookObject(TableQuery op,
                                             MultiGetTableKeysHandler handler)
        {
            this.op = op;
            this.handler = handler;
        }
    }

    /*
     * The interface for scanners returned by this factory.
     */
    public interface TableScanner {

        public InternalOperation getOp();

        /**
         * Returns the table associated with the current index entry. The
         * result is valid only if this method is called after nextIndexRow()
         * has been called.
         */
        public TableImpl getTable();

        /**
         * Moves the cursor to the next index entry of the index scan. It does
         * don eserialize the entry. Returns true if a next entry is found;
         * false otherwise.
         *
         * Normally, forTable is null and has no effect. forTable may be not
         * null only for the primary table scanner and only if the
         * ServerTableIter implements a NESTED TABLES clause. In this case,
         * the method returns the next primary index entry whose number of
         * key components is no more than those of the forTable (or null if
         * no such index entry).
         */
        public boolean next(TableImpl forTable)
            throws SizeLimitException, TimeoutException;

        /**
         * Returns the deserialized current index entry. The result is valid
         * only if this method is called after next() has been called.
         */
        public RecordValueImpl getIndexRow();

        /**
         * Locks the current index entry. This interface may only be called
         * after next() has returned true.
         * @return true if the enry is locked, false if it cannot be locked,
         * which means that the entry has been deleted.
         */
        public boolean lockIndexRow();

        /**
         * Locks and returns the full row associated with the "current" key.
         * This interface may only be called after next() has returned true.
         * @return the complete row or null if the row has been deleted.
         */
        public RowImpl getTableRow() throws SizeLimitException;

        /**
         * Returns the current primary key in binary format. The result is valid
         * only if this method is called after next() has been called.
         */
        public byte[] getPrimKeyBytes();

        /**
         * Returns the current secondary key in binary format, if the scanner
         * scans a secondary index, or null otherwise. The result is valid only
         * if this method is called after next() has been called.
         */
        public byte[] getSecKeyBytes();

        /**
         * Return the expiration time of the current row. The result is valid
         * only if this method is called after currentTableRow() has been
         * called.
         */
        public long expirationTime();

        public long modificationTime();

        public int partitionId();

        public int rowStorageSize();

        public int indexStorageSize();

        /**
         * Return the version of the current row. The result is valid only if
         * this method is called after currentTableRow() has been called.
         */
        public Version rowVersion();

        /**
         * Closes the scanner. This must be called to avoid resource leaks.
         */
        public void close();

        public boolean exceededSizeLimit();
    }

    private final RuntimeControlBlock theRCB;

    private ResumeInfo theResumeInfo;

    private final Transaction theTxn;

    private PartitionId thePid;

    private final OperationHandler theHandlersManager;

    private final RepNode theRN;

    private final int theShardId;

    public TableScannerFactory(
        final RuntimeControlBlock rcb,
        final Transaction txn,
        final OperationHandler oh) {

        theRCB = rcb;
        theResumeInfo = rcb.getResumeInfo();
        theTxn = txn;
        theHandlersManager = oh;
        theRN = oh.getRepNode();
        theShardId = theRN.getRepNodeId().getGroupId();
    }

    /*
     * Returns a TableScanner. This is an index scanner if indexKey is not null,
     * otherwise it is a primary key scanner. In both cases the object must be
     * closed to avoid leaking resources and/or leaving records locked.
     */
    public TableScanner getTableScanner(
        int pid,
        Direction dir,
        int posInJoin,
        TableImpl[] tables,
        int numAncestors,
        IndexImpl index,
        RecordValueImpl[] indexKeys,
        FieldRange[] ranges,
        boolean isUpdate,
        boolean lockIndexEntries,
        boolean[] usesCoveringIndex,
        boolean exceededSizeLimit,
        @SuppressWarnings("unused")
        short version) {

        if (pid < 0) {
            pid = theRCB.getJoinPid(); 
        }

        thePid = new PartitionId(pid);

        if (index != null) {

            if (tables.length > numAncestors + 1) {
                return new CompositeTableScanner(posInJoin,
                                                 dir,
                                                 tables,
                                                 numAncestors,
                                                 indexKeys,
                                                 ranges,
                                                 usesCoveringIndex,
                                                 exceededSizeLimit);
            }

            return new SecondaryTableScanner(posInJoin,
                                             dir,
                                             tables,
                                             numAncestors,
                                             index,
                                             indexKeys,
                                             ranges,
                                             lockIndexEntries,
                                             usesCoveringIndex[numAncestors],
                                             exceededSizeLimit);
        }

        return new PrimaryTableScanner(posInJoin,
                                       dir,
                                       isUpdate,
                                       tables,
                                       numAncestors,
                                       indexKeys,
                                       ranges,
                                       lockIndexEntries,
                                       false, /* isComposite */
                                       usesCoveringIndex,
                                       exceededSizeLimit);
    }

    AncestorScanner getAncestorScanner(InternalOperation op,
                                       boolean chargeCost) {
        return new AncestorScanner(op, chargeCost);
    }

    /**
     * This is a "scanner" that is actually used to do an exact key lookup.
     * It is used by the ServerTaleIter to retrieve the ancestor keys and/or
     * rows for the current target-table row. It is also used during resume
     * to read the rows in the resume join path.
     *
     * Modeling it as a scanner allows some code to be reused in
     * ServerTableIter.
     *
     * To avoid adding complexity to join implementation, don't suspend in
     * middle of fetching ancestors key/row even if exceeded the size limit.
     *
     * theOp:
     * This is the InternalOperation used by the "main" scanner of the
     * ServerTableIter (either a PrimaryTableScanner or SecondaryTableScanner).
     * We put a reference in "this" so that read consumption is tracked in
     * one place only.
     */
    class AncestorScanner implements TableScanner {

        InternalOperation theOp;

        TableImpl theTable;

        IndexImpl theIndex;

        byte[] thePrimKey;

        byte[] theSecKey;

        Database theDB;

        Cursor theCursor;

        SecondaryDatabase theSecDB;

        SecondaryCursor theSecCursor;

        OperationResult theGetResult;

        boolean theMoreElements;

        RowImpl theTableRow;

        RecordValueImpl theIndexRow;

        final DatabaseEntry theKeyEntry;

        final DatabaseEntry theNoDataEntry;

        final DatabaseEntry theDataEntry;

        /*
         * If true, charge the read cost. Otherwise, don't charge. This is
         * the case used by ServerTableIterator.resume() when constructs the
         * join path during resume.
         */
        final boolean theChargeCost;

        AncestorScanner(InternalOperation op, boolean chargeCost) {

            theOp = op;
            theChargeCost = chargeCost;

            if (thePid.getPartitionId() >= 0) {
                theDB = theHandlersManager.getRepNode().getPartitionDB(thePid);
            }

            theKeyEntry = new DatabaseEntry();
            theDataEntry = new DatabaseEntry();
            theNoDataEntry = new DatabaseEntry();
            theNoDataEntry.setPartial(0, 0, true);
        }

        void init(TableImpl table, IndexImpl index, byte[] pkey, byte[] ikey) {

            theTable = table;
            theIndex = index;
            thePrimKey = pkey;
            theSecKey = ikey;
            theMoreElements = true;
            theTableRow = null;
            theIndexRow = null;

            if (index == null) {

                if (theDB == null) {
                    theDB = theHandlersManager.getRepNode().getPartitionDB(pkey);
                }

                theCursor = theDB.openCursor(theTxn, CURSOR_DEFAULT);

            } else {

                if (theSecDB == null) {
                    theSecDB = theHandlersManager.getRepNode().
                               getIndexDB(table.getInternalNamespace(),
                                          index.getName(),
                                          table.getFullName());
                }

                theSecCursor = theSecDB.openCursor(theTxn, CURSOR_DEFAULT);
            }
        }

        @Override
        public InternalOperation getOp() {
            return theOp;
        }

        @Override
        public TableImpl getTable() {
            return theTable;
        }

        @Override
        public byte[] getPrimKeyBytes() {
            return thePrimKey;
        }

        @Override
        public byte[] getSecKeyBytes() {
            return theSecKey;
        }

        @Override
        public long expirationTime() {
            return theGetResult.getExpirationTime();
        }

        @Override
        public long modificationTime() {
            return theGetResult.getModificationTime();
        }

        @Override
        public int partitionId() {
            if (thePid.getPartitionId() >= 0) {
                return thePid.getPartitionId();
            }
            return theHandlersManager.getRepNode().
                getPartitionId(thePrimKey).getPartitionId();
        }

        @Override
        public int rowStorageSize() {
            Cursor cursor = (theIndex == null ? theCursor : theSecCursor);
            return InternalOperationHandler.getStorageSize(cursor);
        }

        @Override
        public int indexStorageSize() {
            return -1;
        }

        @Override
        public Version rowVersion() {
            Cursor cursor = (theIndex == null ? theCursor : theSecCursor);
            return theHandlersManager.getVersion(cursor);
        }

        @Override
        public boolean exceededSizeLimit() {
            return false;
        }

        @Override
        public void close() {
            if (theCursor != null) {
                TxnUtil.close(theCursor);
            }
            if (theSecCursor != null) {
                TxnUtil.close(theSecCursor);
            }
        }

        @Override
        public RecordValueImpl getIndexRow() {
            return (theIndex != null ? theIndexRow : theTableRow);
        }

        @Override
        public boolean lockIndexRow() {
            return true;
        }

        @SuppressWarnings("resource")
        @Override
        public RowImpl getTableRow() throws SizeLimitException {

            Cursor cursor = (theIndex == null ? theCursor : theSecCursor);

            OperationResult result =
                cursor.get(theKeyEntry, theDataEntry, Get.CURRENT,
                           LockMode.READ_UNCOMMITTED.toReadOptions().
                               clone().setExcludeTombstones(true));

            if (result == null) {
                return null;
            }

            if (theChargeCost) {
                /*
                 * The key cost has been charged during next(TableImpl),
                 * subtracts it from current record size.
                 */
                int storageSize = rowStorageSize() - MIN_READ;
                if (storageSize > 0) {
                    theOp.addReadBytes(storageSize);
                }
            }

            byte[] data = theDataEntry.getData();

            if (theIndex == null) {
                return theTable.initRowFromValueBytes(theTableRow,
                                                      data,
                                                      expirationTime(),
                                                      modificationTime(),
                                                      rowVersion(),
                                                      partitionId(),
                                                      theShardId,
                                                      rowStorageSize());
            }

            theTableRow = theTable.createRow();
            if (!theTable.initRowFromKeyValueBytes(thePrimKey,
                                                   data,
                                                   expirationTime(),
                                                   modificationTime(),
                                                   rowVersion(),
                                                   partitionId(),
                                                   theShardId,
                                                   rowStorageSize(),
                                                   theTableRow)) {
                return null;
            }

            return theTableRow;
        }

        @Override
        public boolean next(TableImpl forTable) throws SizeLimitException {

            if (!theMoreElements) {
                theTableRow = null;
                theIndexRow = null;
                return false;
            }

            theMoreElements = false;

            if (theIndex == null) {

                if (theRCB.getTraceLevel() >= 2) {
                    theRCB.trace("Searching for anc key : " +
                                 PlanIter.printKey(thePrimKey));
                }

                theKeyEntry.setData(thePrimKey);

                theGetResult = theCursor.get(theKeyEntry, theNoDataEntry,
                                             Get.SEARCH,
                                             InternalOperationHandler.
                                                 DEFAULT_EXCLUDE_TOMBSTONES);
                if (theGetResult == null) {
                    return false;
                }

                if (theChargeCost) {
                    /* Charge the cost if found the key */
                    theOp.addMinReadCharge();
                }

                TableImpl table = theTable.findTargetTable(thePrimKey);

                if (table == null) {

                    /*
                     * This should not be possible unless there is a non-table key
                     * in the btree.
                     */
                    String msg = "Key is not in a table: "  +
                                 UserDataControl.displayKey(thePrimKey);
                    theHandlersManager.getLogger().log(Level.INFO, msg);
                    return false;
                }

                if (theTable.getId() != table.getId()) {
                    return false;
                }

                theTableRow = theTable.createRow();

                if (!theTable.initRowFromKeyBytes(thePrimKey, -1, /*initPos*/
                                                  theTableRow)) {
                    theTableRow = null;
                    return false;
                }

                if (theRCB.getTraceLevel() >= 3) {
                    theRCB.trace("Produced key row : " + theTableRow);
                }
            } else {

                theKeyEntry.setData(theSecKey);
                theDataEntry.setData(thePrimKey);

                theGetResult = theSecCursor.get(
                                   theKeyEntry,
                                   theDataEntry,
                                   theNoDataEntry,
                                   Get.SEARCH_BOTH,
                                   InternalOperationHandler.
                                       DEFAULT_EXCLUDE_TOMBSTONES);

                if (theGetResult == null) {
                    return false;
                }

                if (theChargeCost) {
                    /* Charge the cost if found the key */
                    theOp.addMinReadCharge();
                }

                theIndexRow = theIndex.getIndexEntryDef().createRecord();

                theIndex.rowFromIndexEntry(theIndexRow, thePrimKey, theSecKey);

                if (theRCB.getTraceLevel() >= 3) {
                    theRCB.trace("Produced key row : " + theIndexRow);
                }
            }

            return true;
        }
    }

    /*
     * theScanner:
     * The underlying Scanner used by PrimaryTableScanner. It uses
     * DIRTY_READ_ALL lockmode (unless theLockIndexEntries is true) and does
     * a key-only scan.
     *
     * theTableRow:
     * A RowImpl where the current binary primary-index key is deserialized
     * into. If the full record is needed by the query, the associated LN will
     * also be deserialized into this RowImpl.
     *
     * theDataEntry:
     * Used to retrieve the LN associated with the current index key.
     *
     * theChargeForResumeKey:
     * theChargeForResumeRow:
     * Whether to charge read units during the resume of the current batch
     * (i.e. during the first call to this.next() and, if the index is not
     * covering, this.getTableRow()). This will be false if the batch must
     * resume on the resume key/row, rather than after it. In this case, 
     * during resume we will read the last key/row of the previous batch
     * again and we don't want to charge for reading this key/row. Avoiding
     * this double-charge is good practice, but it also prevents a potential
     * infinite loop for inner join queries. For example, consider a join
     * between table A and B, where both tables are scanned via their primary
     * index and the index is not covering. Let RA and RB be the last rows
     * read during the previous batch. When the current batch starts, the
     * scanner for A will resume on RA. The scanner for B may resume on RB
     * or the next row. If B resumes on RB and we charge for re-reading RA
     * and RB, we may reach the read limit after we read the RB key, but
     * before we read the actual row. In this case, the current batch will
     * suspend without having made any forward progress, and the same will
     * happen with all subsequent batches.
     *
     * Notice that without inner joins, the scanner will always make forward
     * progress during resume, even if we do always charge for the reads done
     * during resume. This is because:
     * - the read limit will always be >= 1 at resume time, so we can read
     *   a key without exceeding the limit. The key that gets read is either
     *   the resume key (if ResumeInfo.theMoveAfterResumeKey is false), or
     *   the next key. In the later case we have made progress. Otherwise:
     * - if the index is covering, the next this.next() call will read the
     *   next key and set the resume to that key, so we have made progress.
     * - If the index is not covering, we read the resume row next, and then
     *   check if we have exceeded the limit. If so, we don't throw the SLE
     *   immediatelly, instead we defer it until the resume row is processed
     *   by the rest of the query execution plan (potentially generating a
     *   query result) and the next this.next() call is made. Furthermore, we
     *   set ResumeInfo.theMoveAfterResumeKey to true. So, even though this
     *   batch reads only the resume row, the next batch will read the next row. 
     */
    private class PrimaryTableScanner implements TableScanner {

        final int thePosInJoin;

        final Direction theDirection;

        final boolean theIsUpdate;

        final TableImpl[] theTables;

        final boolean[] theCoveringIndexes;

        final int theNumAncestors;

        final boolean theHasDescendants;

        final TargetTables theTargetTables;

        final RecordValueImpl[] theKeys;

        final FieldRange[] theRanges;

        final TableQuery theOp;

        final MultiGetTableKeysHandler theOpHandler;

        final boolean theLockIndexEntries;

        final DatabaseEntry theDataEntry;

        int theCurrentIndexRange;

        OperationTableInfo theTableInfo;

        Scanner theScanner;

        boolean theMoreElements;

        byte[] theBinaryPrimKey;

        TableImpl theTable;

        RowImpl theTableRow;

        boolean theExceededSizeLimit;

        boolean theChargeForResumeKey;

        boolean theChargeForResumeRow;

        PrimaryTableScanner(
            int posInJoin,
            Direction dir,
            boolean isUpdate,
            TableImpl[] tables,
            int numAncestors,
            RecordValueImpl[] keys,
            FieldRange[] ranges,
            boolean lockIndexEntries,
            boolean isComposite,
            boolean[] coveringIndexes,
            boolean exceededSizeLimit) {

            thePosInJoin = posInJoin;
            theDirection = dir;
            theIsUpdate = isUpdate;

            theTables = tables;
            theCoveringIndexes = coveringIndexes;
            theNumAncestors = numAncestors;
            theHasDescendants = (tables.length > numAncestors + 1);

            theTargetTables =  new TargetTables(tables, numAncestors);

            theKeys = keys;
            theRanges = ranges;

            theOp = theRCB.getQueryOp();
            theOpHandler = (MultiGetTableKeysHandler)
                theHandlersManager.getHandler(OpCode.MULTI_GET_TABLE_KEYS);

            theLockIndexEntries = lockIndexEntries;
            theDataEntry = new DatabaseEntry();

            theTable = tables[numAncestors];

            theResumeInfo = theRCB.getResumeInfo();

            theCurrentIndexRange = theResumeInfo.
                                   getCurrentIndexRange(thePosInJoin);

            theChargeForResumeKey =
                (theResumeInfo.getMoveAfterResumeKey(thePosInJoin) &&
                 theResumeInfo.getMoveJoinAfterResumeKey(thePosInJoin)) ||
                 theResumeInfo.getPrimResumeKey(thePosInJoin) == null;

            theChargeForResumeRow =
                (theResumeInfo.getMoveJoinAfterResumeKey(thePosInJoin) ||
                 theResumeInfo.getPrimResumeKey(thePosInJoin) == null);

            theMoreElements = true;

            theTableRow = theTable.createRow();

            theExceededSizeLimit = exceededSizeLimit;

            if (!isComposite) {
                initIndexRange();
            }
        }

        void initIndexRange() {

            if (theScanner != null) {
                theScanner.close();
            }

            PrimaryKey key = (PrimaryKey)theKeys[theCurrentIndexRange];
            FieldRange range = theRanges[theCurrentIndexRange];

            TableKey tableKey = TableKey.createKey(theTable, key, true);
            KeyRange tableRange = TableAPIImpl.createKeyRange(range, true);
            assert(tableKey != null);

            theOpHandler.verifyTableAccess(theTargetTables,
                                           tableKey.getKeyBytes());

            boolean moveAfterResumeKey = 
                (theResumeInfo.getMoveAfterResumeKey(thePosInJoin) &&
                 theResumeInfo.getMoveJoinAfterResumeKey(thePosInJoin));

            if (theRCB.getTraceLevel() >= 2) {
                theRCB.trace("Initializing primary index scan " +
                             theCurrentIndexRange +
                             " in " + thePid +
                             "\nkey = " + key +
                             "\nwith resume key: " +
                             PlanIter.printKey(theResumeInfo.
                                               getPrimResumeKey(thePosInJoin)) +
                             "\nmove after resume key = " +
                             moveAfterResumeKey +
                             ", op readKB = " + theOp.getReadKB() +
                             ", theLockIndexEntries = " + theLockIndexEntries);
            }

            theTableInfo = new OperationTableInfo();
            theTableInfo.setTopLevelTable(theTable.getTopLevelTable());

            theOpHandler.initTableLists(theTargetTables,
                                        theTableInfo,
                                        theTxn,
                                        theDirection,
                                        theResumeInfo.
                                        getPrimResumeKey(thePosInJoin));
            /*
             * Create a key-only scanner using dirty reads. This means that in
             * order to use the record, it must be locked, and if the data is
             * required, it must be fetched.
             */

            theScanner = new Scanner(
                theOp,
                theTxn,
                thePid,
                theOpHandler.getRepNode(),
                tableKey.getKeyBytes(),
                tableKey.getMajorKeyComplete(),
                tableRange,
                Depth.PARENT_AND_DESCENDANTS,
                theDirection,
                theResumeInfo.getPrimResumeKey(thePosInJoin),
                moveAfterResumeKey,
                CURSOR_DEFAULT,
                (theIsUpdate ?
                 LockMode.RMW :
                 (theLockIndexEntries ?
                  LockMode.DEFAULT :
                  LockMode.READ_UNCOMMITTED_ALL)),
                true); /* use a key-only scanner; fetch data in the "next" call */

            /*
             * Disable charging the cost of reading key in the scanner, see more
             * details on charging key read cost in next(TableImpl).
             */
            theScanner.setChargeKeyRead(false);
            /*
             * The Scanner performs key-only scan, the key read cost will be
             * charged during key scan, set SubtractKeyReadCost to true to
             * subtract the key read cost when charge the row read cost.
             */
            theScanner.setSubtractKeyReadCost(true);
        }

        @Override
        public InternalOperation getOp() {
            return theOp;
        }

        @Override
        public TableImpl getTable() {
            return theTable;
        }

        @Override
        public byte[] getPrimKeyBytes() {
            return theBinaryPrimKey;
        }

        @Override
        public byte[] getSecKeyBytes() {
            return null;
        }

        @Override
        public long expirationTime() {
            return theScanner.getExpirationTime();
        }

        @Override
        public long modificationTime() {
            return theScanner.getModificationTime();
        }

        @Override
        public int partitionId() {
            return thePid.getPartitionId();
        }

        @Override
        public int rowStorageSize() {
            return InternalOperationHandler.
                   getStorageSize(theScanner.getCursor());
        }

        @Override
        public int indexStorageSize() {
            return -1;
        }

        @Override
        public Version rowVersion() {
            return theHandlersManager.getVersion(theScanner.getCursor());
        }

        @Override
        public boolean exceededSizeLimit() {
            return theExceededSizeLimit;
        }

        @Override
        public void close() {

            if (theScanner != null) {
                theScanner.close();
            }
            theTableRow = null;
            theBinaryPrimKey = null;
        }

        @Override
        public RecordValueImpl getIndexRow() {

            if (theMoreElements) {
                return theTableRow;
            }

            return null;
        }

        @Override
        public boolean lockIndexRow() {
            return theScanner.getCurrent();
        }

        @Override
        public RowImpl getTableRow() throws SizeLimitException {

            if (!theScanner.
                 getLockedData(theDataEntry,
                               false,
                               theChargeForResumeRow/*chargeRowRead*/)) {

                if (theRCB.getTraceLevel() >= 3) {
                    theRCB.trace("Failed to lock index row: " + theTableRow);
                }

                theChargeForResumeRow = true;
                return null;
            }

            /*
             * Check with size limit. If the current read cost exceeds the size
             * limit, return the current data entry already-fetched and defer
             * throwing SizeLimitException until move to next index row.
             */
            try {
                checkSizeLimit(theRCB, getOp());
            } catch (SizeLimitException ex) {
                assert !theExceededSizeLimit;
                theExceededSizeLimit = true;
            }

            theChargeForResumeRow = true;

            return theTable.initRowFromValueBytes(theTableRow,
                                                  theDataEntry.getData(),
                                                  expirationTime(),
                                                  modificationTime(),
                                                  rowVersion(),
                                                  partitionId(),
                                                  theShardId,
                                                  rowStorageSize());

        }

        @Override
        public boolean next(TableImpl forTable)
            throws SizeLimitException, TimeoutException {

            /* Throw deferred SLE */
            if (theExceededSizeLimit) {
                throw new SizeLimitException(true /* afterReadEntry */);
            }

            /*
             * The cost of reading key is disabled in the scanner, the cost of
             * reading key will be charged after key check by keyInTargetTable()
             *   - match > 0, valid key, charge min. read.
             *   - match < 0, no key found, charge empty read.
             *   - match = 0, invalid key for the target table and continue
             *                to next key, no charge.
             */
            while (theCurrentIndexRange < theKeys.length) {

                while (theMoreElements) {

                    if (theRCB.checkTimeout()) {
                        throw new TimeoutException();
                    }

                    if (!theScanner.next()) {
                        break;
                    }

                    theBinaryPrimKey = theScanner.getKey().getData();
                    theResumeInfo.setPrimResumeKey(thePosInJoin, theBinaryPrimKey);

                    if (theRCB.getTraceLevel() >= 3) {
                        theRCB.trace("Produced binary index entry in " +
                                     thePid + " : " +
                                     PlanIter.printKey(theBinaryPrimKey) +
                                     " op read KB = " + theOp.getReadKB());
                    }

                    if (theRCB.getTraceLevel() >= 3 && forTable != null) {
                        theRCB.trace("moveToNextIndexEntry for table " +
                                     forTable.getFullName());
                    }

                    assert TestHookExecute.doHookIfSet(
                        primaryTableScanHookObtainedNext,
                        new PrimaryTableScannerHookObject(theOp, theOpHandler));

                    int match = MultiTableOperationHandler.
                        keyInTargetTable(theHandlersManager.getLogger(),
                                         theOp,
                                         theTargetTables,
                                         theTableInfo,
                                         (forTable != null ?
                                          forTable.getNumKeyComponents() :
                                          -1),
                                         theScanner,
                                         theScanner.getLockMode(),
                                         false /* chargeReadCost */);

                    theBinaryPrimKey = theScanner.getKey().getData();
                    theResumeInfo.setPrimResumeKey(thePosInJoin, theBinaryPrimKey);
                    theTable = theTableInfo.getCurrentTable();

                    if (match <= 0) {
                        if (match < 0) {
                            theMoreElements = false;
                            /* charge empty read, if nothing has been read before */
                            theOp.addEmptyReadCharge();
                        }

                        if (theRCB.getTraceLevel() >= 3) {
                            theRCB.trace("Skipping non-matching binary index entry in " +
                                         thePid + " : " +
                                         PlanIter.printKey(theBinaryPrimKey));
                        }
                        continue;
                    }

                    if (theScanner.isTombstone()) {
                        if (theRCB.getTraceLevel() >= 3) {
                            theRCB.trace("Skipping tombstone binary index entry in " +
                                         thePid + " : " +
                                         PlanIter.printKey(theBinaryPrimKey));
                        }
                        continue;
                    }

                    /* Charge min. read for reading the matched key */
                    if (theChargeForResumeKey) {
                        if (theRCB.getTraceLevel() >= 4) {
                            theRCB.trace("Charging key read. Current op read KB = " +
                                         theOp.getReadKB());
                        }
                        theOp.addMinReadCharge();
                    }

                    if (theTargetTables.hasChildTables() ||
                        theTargetTables.hasAncestorTables() ||
                        theTable.isJsonCollection()) {
                        theTableRow = theTable.createRow();
                    }

                    /* if the current read caused the size limit to be exceeded,
                     * but the index is covering, then defer throwing
                     * SizeLimitException until the next call to next(). */
                    try {
                        checkSizeLimit(theRCB, theOp);
                    } catch (SizeLimitException sle) {
                        if (!usesCoveringIndex()) {
                            throw sle;
                        }
                        assert !theExceededSizeLimit;
                        theExceededSizeLimit = true;
                    }

                    if (!theTable.initRowFromKeyBytes(theBinaryPrimKey,
                                                      -1, /*initPos*/
                                                      theTableRow)) {
                        continue;
                    }

                    if (theRCB.getTraceLevel() >= 3) {
                        theRCB.trace("Produced key row in partition " +
                                     thePid + " : " + theTableRow);
                    }

                    theChargeForResumeKey = true;
                    theResumeInfo.setMoveAfterResumeKey(thePosInJoin, true);
                    return true;
                }

                ++theCurrentIndexRange;

                while (theCurrentIndexRange < theKeys.length) {
                    if (theKeys[theCurrentIndexRange] == null) {
                        ++theCurrentIndexRange;
                        continue;
                    }
                    theResumeInfo.setCurrentIndexRange(thePosInJoin,
                                                       theCurrentIndexRange);
                    theResumeInfo.setPrimResumeKey(thePosInJoin, null);
                    theMoreElements = true;
                    initIndexRange();
                    break;
                }
            }

            theResumeInfo.setPrimResumeKey(thePosInJoin, null);
            theResumeInfo.setCurrentIndexRange(thePosInJoin, 0);
            theMoreElements = false;
            return false;
        }

        protected boolean usesCoveringIndex() {

            if (!theHasDescendants) {
                return theCoveringIndexes[theNumAncestors];
            }

            for (int i = 0; i < theTables.length; ++i) {
                if (theTables[i] == theTable) {
                    return theCoveringIndexes[i];
                }
            }

            assert(false);
            return true;
        }
    }

    /**
     * theScanner:
     * The underlying IndexScanner used by SecondaryTableScanner. It uses
     * DIRTY_READ_ALL lockmode and does a key-only scan.
     *
     * theTableRow:
     * The table Row that stores the record pointed to by the current index
     * entry (the one that the scanner is positioned on).
     *
     * theDataEntry:
     * A DataEntry used to retrieve the data portion of the record pointed to
     * by the current index entry.
     *
     * theExceededSizeLimit:
     * Set to true when the current read cost exceeds the max ReadKB limit
     *
     * theChargeForResumeRow:
     * See comment for same fields in PrimaryTableScanner.
     */
    private class SecondaryTableScanner implements TableScanner {

        final int thePosInJoin;

        final TableImpl theTable;

        final IndexImpl theIndex;

        final TargetTables theTargetTables;

        final boolean theLockIndexEntries;

        final boolean theUsesCoveringIndex;

        final Direction theDirection;

        final RecordValueImpl[] theKeys;

        final FieldRange[] theRanges;

        final IndexKeysIterateHandler theOpHandler;

        TableQuery theOp;

        int theCurrentIndexRange;

        IndexScanner theScanner;

        boolean theMoreElements;

        byte[] theBinaryPrimKey;

        byte[] theBinaryIndexKey;

        int theIndexEntryStorageSize;

        final RecordValueImpl theIndexRow;

        RowImpl theTableRow;

        final DatabaseEntry theDataEntry;

        private boolean theExceededSizeLimit;

        private HashSet<PartitionId> theTargetPartitions;

        private PartitionId theCurrentRowPid;

        private boolean theIsVirtualScan;

        boolean theChargeForResumeRow;

        SecondaryTableScanner(
            int posInJoin,
            Direction dir,
            TableImpl[] tables,
            int numAncestors,
            IndexImpl index,
            RecordValueImpl[] keys,
            FieldRange[] ranges,
            boolean lockIndexEntries,
            boolean usesCoveringIndex,
            boolean exceededSizeLimit) {

            thePosInJoin = posInJoin;
            theIndex = index;
            theTable = theIndex.getTable();

            if (tables == null) {
                tables = new TableImpl[1];
                tables[0] = theTable;
            }

            assert(theTable == tables[numAncestors]);
            theTargetTables =  new TargetTables(tables, numAncestors);
            theLockIndexEntries = lockIndexEntries;
            theUsesCoveringIndex = usesCoveringIndex;
            theDirection = dir;
            theKeys = keys;
            theRanges = ranges;

            theOp = theRCB.getQueryOp();
            theOpHandler = (IndexKeysIterateHandler)
                theHandlersManager.getHandler(OpCode.INDEX_KEYS_ITERATE);

            theCurrentIndexRange = theResumeInfo.
                                   getCurrentIndexRange(thePosInJoin);

            theChargeForResumeRow =
                (theResumeInfo.getMoveJoinAfterResumeKey(thePosInJoin) ||
                 theResumeInfo.getPrimResumeKey(thePosInJoin) == null);

            theMoreElements = true;
            theDataEntry = new DatabaseEntry();

            theExceededSizeLimit = exceededSizeLimit;

            initIndexRange();

            theTableRow = theTable.createRow();
            theIndexRow = theIndex.getIndexEntryDef().createRecord();

            ResumeInfo ri = theRCB.getResumeInfo();
            Topology baseTopo = theRCB.getBaseTopo();

            if (ri.getVirtualScanPid() > 0) {
                theIsVirtualScan = true;
                theTargetPartitions = new HashSet<>();
                theTargetPartitions.add(new PartitionId(ri.getVirtualScanPid()));
                if (theRCB.getTraceLevel() >= 3) {
                    theRCB.trace("Added PID " + ri.getVirtualScanPid() +
                                 " for virtual shard to theTargetPartitions");
                }
            } else if (thePid.getPartitionId() > 0) {
                theTargetPartitions = new HashSet<>();
                theTargetPartitions.add(thePid);
            } else if (baseTopo != null) {
                theTargetPartitions =
                    new HashSet<>(baseTopo.getPartitionsInShard(theShardId,
                                                                null));
            }
        }

        void initIndexRange() {

            if (theScanner != null) {
                theScanner.close();
            }

            theOpHandler.verifyTableAccess(theTargetTables);

            IndexKeyImpl key = (IndexKeyImpl)theKeys[theCurrentIndexRange];
            FieldRange range = theRanges[theCurrentIndexRange];
            boolean geomRange = (key.size() == theIndex.getGeoFieldPos());
            byte[] secResumeKey = theResumeInfo.getSecResumeKey(thePosInJoin);
            byte[] primResumeKey = theResumeInfo.getPrimResumeKey(thePosInJoin);

            boolean moveAfterResumeKey = 
                (theResumeInfo.getMoveAfterResumeKey(thePosInJoin) &&
                 theResumeInfo.getMoveJoinAfterResumeKey(thePosInJoin));

            if (theRCB.getTraceLevel() >= 3) {
                theRCB.trace("Initializing index scan " +
                             theCurrentIndexRange +
                             " in " + thePid +
                             "\nKey = " + key + "\nRange = " + range +
                             "\nwith resumeKey:\n" +
                             PlanIter.printByteArray(secResumeKey) + "\n" +
                             PlanIter.printByteArray(primResumeKey) +
                             "\nmove after resume key = " +
                             moveAfterResumeKey +
                             ", op readKB = " + theOp.getReadKB() +
                             "\ngeom range = " + geomRange);
            }

            assert(!geomRange || range != null);

            /*
             * Create an IndexOperation for a single target table
             */
            IndexRange indexRange = new IndexRange(key, range, theDirection,
                                                   geomRange);

            if (geomRange && range.getStart().equals(range.getEnd())) {
                assert(indexRange.getPrefixKey() != null);
                assert(indexRange.getEndKey() == null);
            }

            /*
             * Create a key-only scanner using dirty reads. This means that
             * in order to use the record, it must be locked, and if the data
             * is required, it must be fetched.
             */
            theScanner = new IndexScanner(
                theOp,
                theTxn,
                theOpHandler.getSecondaryDatabase(
                    theTable.getInternalNamespace(),
                    theTable.getFullName(),
                    theIndex.getName()),
                theIndex,
                indexRange,
                secResumeKey,
                primResumeKey,
                moveAfterResumeKey,
                CURSOR_DEFAULT,
                (theLockIndexEntries ?
                 LockMode.DEFAULT :
                 LockMode.READ_UNCOMMITTED_ALL),
                true /*keyOnly*/);

            /* The Scanner performs key-only scan, and charges the key read cost
             * during the scan. By setting SubtractKeyReadCost to true, the key
             * read cost will be subtracted from the row read cost if and when
             * the row is actually accessed. */
            theScanner.setSubtractKeyReadCost(true);

            theScanner.setRCB(theRCB);
        }

        @Override
        public InternalOperation getOp() {
            return theOp;
        }

        @Override
        public TableImpl getTable() {
            return theTable;
        }

        @Override
        public byte[] getPrimKeyBytes() {
            return theBinaryPrimKey;
        }

        @Override
        public byte[] getSecKeyBytes() {
            return theBinaryIndexKey;
        }

        @Override
        public long expirationTime() {
            return theScanner.getExpirationTime();
        }

        @Override
        public long modificationTime() {
            return theScanner.getModificationTime();
        }

        @Override
        public int partitionId() {
            return theHandlersManager.getRepNode().
                getPartitionId(theBinaryPrimKey).getPartitionId();
        }

        @Override
        public int rowStorageSize() {
            if (!theScanner.getLockedData(theDataEntry, true)) {
                assert(false);
                return 0;
            }
            return InternalOperationHandler.
                   getStorageSize(theScanner.getCursor());
        }

        @Override
        public int indexStorageSize() {
            return theIndexEntryStorageSize;
        }

        @Override
        public Version rowVersion() {
            return theHandlersManager.getVersion(theScanner.getCursor());
        }

        @Override
        public boolean exceededSizeLimit() {
            return theExceededSizeLimit;
        }

        @Override
        public void close() {

            if (theScanner != null) {
                theScanner.close();
            }
        }

        @Override
        public RecordValueImpl getIndexRow() {

            if (theMoreElements) {
                theIndex.rowFromIndexEntry(theIndexRow,
                                           theBinaryPrimKey,
                                           theBinaryIndexKey);

                if (theRCB.getTraceLevel() >= 1) {
                    theRCB.trace("Produced index row in " +
                                 theCurrentRowPid + " : " + theIndexRow);
                }

                return theIndexRow;
            }

            return null;
        }

        @Override
        public boolean lockIndexRow() {
            return theScanner.lockIndexEntry();
        }

        @Override
        public RowImpl getTableRow() throws SizeLimitException {

            if (theRCB.getTraceLevel() >= 1) {
                theRCB.trace("Reading row with theChargeForResumeRow = " +
                             theChargeForResumeRow);
            }

            if (!theScanner.
                 getLockedData(theDataEntry,
                               theChargeForResumeRow/*chargeRowRead*/)) {

                theChargeForResumeRow = true;
                return null;
            }

            /*
             * Check with size limit. If the current read cost exceeds the size
             * limit, return the current data entry already-fetched and defer
             * throwing SizeLimitException until move to next index row.
             */
            try {
                checkSizeLimit(theRCB, theOp);
            } catch(SizeLimitException sle) {
                assert !theExceededSizeLimit;
                theExceededSizeLimit = true;
            }

            byte[] data = theDataEntry.getData();

            if (data == null || data.length == 0) {
                /*
                 * A key-only row, no data to fetch. However, the table may
                 * have evolved and it now contains non-prim-key columns as
                 * well. So, we must fill the missing columns with their
                 * default values.
                 */
                if  (theTable.getRowDef().getNumFields() ==
                     theTable.getPrimaryKeySize()) {

                    throw new QueryStateException(
                        "currentRow() should never be called on a key-only " +
                        "table, because the index should be a covering one");
                }
            }

            int storageSize = InternalOperationHandler.
                              getStorageSize(theScanner.getCursor());

            if (!theTable.initRowFromKeyValueBytes(theBinaryPrimKey,
                                                   data,
                                                   expirationTime(),
                                                   modificationTime(),
                                                   rowVersion(),
                                                   partitionId(),
                                                   theShardId,
                                                   storageSize,
                                                   theTableRow)) {
                theChargeForResumeRow = true;
                return null;
            }

            theChargeForResumeRow = true;
            return theTableRow;
        }

        @Override
        public boolean next(TableImpl forTable)
            throws SizeLimitException, TimeoutException {

            /* Throw deferred SLE */
            if (theExceededSizeLimit) {
                throw new SizeLimitException(true /* afterReadEntry */);
            }

            while (theCurrentIndexRange < theKeys.length) {

                while (theMoreElements) {

                    if (theRCB.checkTimeout()) {
                        throw new TimeoutException();
                    }

                    if (!theScanner.next()) {
                        break;
                    }

                    DatabaseEntry indexKey = theScanner.getIndexKey();
                    DatabaseEntry primaryKey = theScanner.getPrimaryKey();
                    assert(indexKey != null && primaryKey != null);

                    theBinaryPrimKey = primaryKey.getData();
                    theBinaryIndexKey = indexKey.getData();
                    theIndexEntryStorageSize = InternalOperationHandler.
                        getStorageSize(theScanner.getCursor());

                    theResumeInfo.setPrimResumeKey(thePosInJoin, theBinaryPrimKey);
                    theResumeInfo.setSecResumeKey(thePosInJoin, theBinaryIndexKey);
                    /*
                     * if the current read caused the size limit to be exceeded,
                     * but the index is covering, then defer throwing
                     * SizeLimitException until the next call to next().
                     */
                    try {
                        checkSizeLimit(theRCB, theOp);
                    } catch (SizeLimitException sle) {
                        if (!theUsesCoveringIndex) {
                            throw sle;
                        }
                        assert !theExceededSizeLimit;
                        theExceededSizeLimit = true;
                    }

                    theCurrentRowPid = theRN.getPartitionId(theBinaryPrimKey);
                    theRCB.setJoinPid(theCurrentRowPid.getPartitionId());

                    if (theTargetPartitions != null &&
                        !theTargetPartitions.contains(theCurrentRowPid)) {

                        if (theRCB.getTraceLevel() >= 1) {
                            theRCB.trace("Skipping index row in " +
                                         theCurrentRowPid + ". Partition is not among " +
                                         "the target partitions");
                        }

                        if (theExceededSizeLimit) {
                            throw new SizeLimitException();
                        }
                        continue;
                    }

                    /* If this is a normal scan, check that the partition has
                     * migrated out of this shard. If it has, skip this entry.
                     * The partition may have migrated away, but the
                     * PartitionManager has not been updated yet to reflect
                     * the migration, and as a result, the index scan (at the
                     * JE level) will still return entries pointing to the
                     * migrated partition. */
                    if (!theIsVirtualScan &&
                        theResumeInfo.isMigratedPartition(theCurrentRowPid)) {
                        if (theRCB.getTraceLevel() >= 1) {
                            theRCB.trace("Skipping index row in " +
                                         theCurrentRowPid +
                                         ". Partition has migrated " +
                                         "out of this shard");
                        }

                        if (theExceededSizeLimit) {
                            throw new SizeLimitException();
                        }
                        continue;
                    }

                    if (theRCB.getTraceLevel() >= 4) {
                        theRCB.trace("Produced index row in " + theCurrentRowPid);
                    }

                    if (theTargetTables.hasAncestorTables()) {
                        theTableRow = theTable.createRow();
                    }

                    theResumeInfo.setMoveAfterResumeKey(thePosInJoin, true);
                    return true;
                }

                ++theCurrentIndexRange;

                while (theCurrentIndexRange < theKeys.length) {
                    if (theKeys[theCurrentIndexRange] == null) {
                        ++theCurrentIndexRange;
                        continue;
                    }
                    theResumeInfo.setCurrentIndexRange(thePosInJoin,
                                                       theCurrentIndexRange);
                    theResumeInfo.setPrimResumeKey(thePosInJoin, null);
                    theResumeInfo.setSecResumeKey(thePosInJoin, null);
                    initIndexRange();
                    break;
                }
            }

            theResumeInfo.setPrimResumeKey(thePosInJoin, null);
            theResumeInfo.setSecResumeKey(thePosInJoin, null);
            theResumeInfo.setCurrentIndexRange(thePosInJoin, 0);
            theMoreElements = false;
            return false;
        }
    }

    /**
     * A table scanner that scans the target table via a secondary index and
     * the descendant tables via a primary index scan.
     */
    private class CompositeTableScanner extends PrimaryTableScanner {

        final IndexImpl theIndex;

        final TableImpl theTargetTable;

        final RecordValueImpl[] theSecKeys;

        final IndexKeysIterateHandler theSecOpHandler;

        IndexScanner theSecScanner;

        byte[] theBinaryIndexKey;

        RecordValueImpl theIndexRow;

        int theIndexEntryStorageSize;

        private HashSet<PartitionId> theTargetPartitions;

        private PartitionId theCurrentRowPid;

        private boolean theIsVirtualScan;

        CompositeTableScanner(
            int posInJoin,
            Direction dir,
            TableImpl[] tables,
            int numAncestors,
            RecordValueImpl[] keys,
            FieldRange[] ranges,
            boolean[] coveringIndexes,
            boolean exceededSizeLimit) {

            super(posInJoin,
                  dir,
                  false, /* isUpdate */
                  tables,
                  numAncestors,
                  null,
                  ranges,
                  false, /*lockIndexEntries*/
                  true, /*isComposite*/
                  coveringIndexes,
                  exceededSizeLimit);

            theIndex = (IndexImpl) ((IndexKey)keys[0]).getIndex();
            theTargetTable = tables[numAncestors];
            theSecKeys = keys;

            theSecOpHandler = (IndexKeysIterateHandler)
                theHandlersManager.getHandler(OpCode.INDEX_KEYS_ITERATE);

            initIndexRange();

            ResumeInfo ri = theRCB.getResumeInfo();
            Topology baseTopo = theRCB.getBaseTopo();

            if (ri.getVirtualScanPid() > 0) {
                theIsVirtualScan = true;
                theTargetPartitions = new HashSet<>();
                theTargetPartitions.add(new PartitionId(ri.getVirtualScanPid()));
                if (theRCB.getTraceLevel() >= 2) {
                    theRCB.trace("Added PID " + ri.getVirtualScanPid() +
                                 " for virtual shard to theTargetPartitions");
                }
            } else if (thePid.getPartitionId() > 0) {
                theTargetPartitions = new HashSet<>();
                theTargetPartitions.add(thePid);
            } else {
                theTargetPartitions =
                    new HashSet<>(baseTopo.getPartitionsInShard(theShardId,
                                                                null));
            }
        }

        @Override
        void initIndexRange() {

            if (theSecScanner != null) {
                theSecScanner.close();
            }

            if (theScanner != null) {
                theScanner.close();
            }

            theSecOpHandler.verifyTableAccess(theTargetTables);

            IndexKeyImpl key = (IndexKeyImpl)theSecKeys[theCurrentIndexRange];
            FieldRange range = theRanges[theCurrentIndexRange];
            byte[] secResumeKey = theResumeInfo.getSecResumeKey(thePosInJoin);
            byte[] primResumeKey = theResumeInfo.getPrimResumeKey(thePosInJoin);

            if (theRCB.getTraceLevel() >= 3) {
                theRCB.trace("Initializing composite index scan " +
                             theCurrentIndexRange +
                             "\nKey = " + key +
                             "\nRange = " + range +
                             "\nwith resume key:\n" +
                             PlanIter.printByteArray(secResumeKey) + "\n" +
                             PlanIter.printByteArray(primResumeKey) +
                             "\nmove after resume key = " +
                             theResumeInfo.getMoveAfterResumeKey(thePosInJoin) +
                             ", op readKB = " + theOp.getReadKB());
            }

            /*
             * Create an IndexOperation for a single target table
             */
            IndexRange indexRange = new IndexRange(key, range, theDirection);

            theTableInfo = new OperationTableInfo();
            theTableInfo.setTopLevelTable(theTable.getTopLevelTable());

            /*
             * Create a key-only scanner using dirty reads. This means that
             * in order to use the record, it must be locked, and if the data
             * is required, it must be fetched.
             */
            theSecScanner = new IndexScanner(
                theOp,
                theTxn,
                theSecOpHandler.getSecondaryDatabase(
                    theTable.getInternalNamespace(),
                    theTable.getFullName(),
                    theIndex.getName()),
                theIndex,
                indexRange,
                theResumeInfo.getSecResumeKey(thePosInJoin),
                theResumeInfo.getPrimResumeKey(thePosInJoin),
                false, /*moveAfterResumeKey()*/
                CURSOR_DEFAULT,
                LockMode.READ_UNCOMMITTED_ALL,
                true /*keyOnly*/);

            theSecScanner.setRCB(theRCB);
        }

        @Override
        public byte[] getSecKeyBytes() {
            return theBinaryIndexKey;
        }

        @Override
        public int indexStorageSize() {
            return theIndexEntryStorageSize;
        }

        @Override
        public void close() {

            super.close();

            if (theSecScanner != null) {
                theSecScanner.close();
            }
        }

        @Override
        public RecordValueImpl getIndexRow() {

            if (!theMoreElements) {
                return null;
            }

            if (theTable.getId() == theTargetTable.getId()) {

                if (theIndexRow == null) {

                    /*
                     * Must create a new index row each time, because a ref to
                     * the returned index row may be stored in the join path.
                     */
                    theIndexRow = theIndex.getIndexEntryDef().createRecord();

                    theIndex.rowFromIndexEntry(theIndexRow,
                                               theBinaryPrimKey,
                                               theBinaryIndexKey);

                }

                return theIndexRow;
            }

            return theTableRow;
        }

        @Override
        public boolean next(TableImpl forTable)
            throws SizeLimitException, TimeoutException {

            /* Throw deferred SLE */
            if (theExceededSizeLimit) {
                throw new SizeLimitException(true /* afterReadEntry */);
            }

            while (theCurrentIndexRange < theSecKeys.length) {

                if (moveToNextEntryInCurrentScan(forTable)) {
                    return true;
                }

                ++theCurrentIndexRange;

                while (theCurrentIndexRange < theSecKeys.length) {
                    if (theSecKeys[theCurrentIndexRange] == null) {
                        ++theCurrentIndexRange;
                        continue;
                    }
                    theResumeInfo.setCurrentIndexRange(thePosInJoin,
                                                       theCurrentIndexRange);
                    theResumeInfo.setPrimResumeKey(thePosInJoin, null);
                    theResumeInfo.setSecResumeKey(thePosInJoin, null);
                    theResumeInfo.setDescResumeKey(thePosInJoin, null);
                    theMoreElements = true;
                    initIndexRange();
                    break;
                }
            }

            theResumeInfo.setPrimResumeKey(thePosInJoin, null);
            theResumeInfo.setSecResumeKey(thePosInJoin, null);
            theResumeInfo.setDescResumeKey(thePosInJoin, null);
            theResumeInfo.setCurrentIndexRange(thePosInJoin, 0);
            theMoreElements = false;
            return false;
        }

        private boolean moveToNextEntryInCurrentScan(TableImpl forTable)
            throws SizeLimitException, TimeoutException {

            boolean newTargetTableRow = false;

            while (true) {

                while (theScanner == null) {

                    theMoreElements = theSecScanner.next();

                    if (!theMoreElements) {
                        break;
                    }

                    theBinaryIndexKey = theSecScanner.getIndexKey().getData();
                    theBinaryPrimKey = theSecScanner.getPrimaryKey().getData();
                    theResumeInfo.setPrimResumeKey(thePosInJoin, theBinaryPrimKey);
                    theResumeInfo.setSecResumeKey(thePosInJoin, theBinaryIndexKey);

                    try {
                        checkSizeLimit(theRCB, theOp);
                    } catch (SizeLimitException sle) {
                        if (!usesCoveringIndex()) {
                            throw sle;
                        }
                        assert !theExceededSizeLimit;
                        theExceededSizeLimit = true;
                    }

                    if (!thePid.isNull()) {
                        PartitionId pid = theHandlersManager.getRepNode().
                            getPartitionId(theBinaryPrimKey);

                        if (pid.getPartitionId() != thePid.getPartitionId()) {

                            if (theRCB.getTraceLevel() >= 3) {
                                theRCB.trace("Skipping index row with pid: " +
                                             pid + " Target pid = " + thePid);
                            }

                            if (theExceededSizeLimit) {
                                throw new SizeLimitException();
                            }
                            continue;
                        }
                    }

                    theCurrentRowPid = theRN.getPartitionId(theBinaryPrimKey);

                    if (theTargetPartitions != null &&
                        !theTargetPartitions.contains(theCurrentRowPid)) {

                        if (theRCB.getTraceLevel() >= 3) {
                            theRCB.trace("Skipping index row in " +
                                         theCurrentRowPid +
                                         ". Partition is not among " +
                                         "the target partitions");
                        }

                        if (theExceededSizeLimit) {
                            throw new SizeLimitException();
                        }
                        continue;
                    }

                    if (!theIsVirtualScan &&
                        theResumeInfo.isMigratedPartition(theCurrentRowPid)) {
                        if (theRCB.getTraceLevel() >= 3) {
                            theRCB.trace("Skipping index row in " +
                                         theCurrentRowPid +
                                         ". Partition has migrated " +
                                         "out of this shard");
                        }

                        if (theExceededSizeLimit) {
                            throw new SizeLimitException();
                        }
                        continue;
                    }

                    theIndexRow = null;
                    newTargetTableRow = true;

                    if (theRCB.getTraceLevel() >= 2) {
                        theRCB.trace("Produced index row : " + getIndexRow() +
                                     " with binary key " +
                                     PlanIter.printByteArray(theBinaryIndexKey));

                    }

                    theIndexEntryStorageSize = InternalOperationHandler.
                        getStorageSize(theSecScanner.getCursor());

                    theOpHandler.initTableLists(
                            theTargetTables,
                            theTableInfo,
                            theTxn,
                            Direction.FORWARD,
                            theResumeInfo.getDescResumeKey(thePosInJoin));

                    theScanner = new Scanner(
                            theOp,
                            theTxn,
                            theCurrentRowPid,
                            theOpHandler.getRepNode(),
                            theBinaryPrimKey, /*parentKey*/
                            true, /* MajorKeyComplete */
                            null, /*range*/
                            Depth.PARENT_AND_DESCENDANTS,
                            Direction.FORWARD,
                            theResumeInfo.getDescResumeKey(thePosInJoin), /*resumeKey*/
                            theResumeInfo.getMoveAfterResumeKey(thePosInJoin),
                            CURSOR_DEFAULT,
                            LockMode.READ_UNCOMMITTED_ALL,
                            true/*keyonly*/);

                    /*
                     * Disable charging the cost of reading primary key in the
                     * scanner, charge the key read cost from primary index in
                     * moveToNextEntryInCurrentScan(TableImpl).
                     */
                    theScanner.setChargeKeyRead(false);

                    /*
                     * The Scanner performs key-only scan, the key read cost
                     * will be charged during key scan, set subtractKeyReadCost
                     * to true to subtract the key read cost when charge the
                     * row read cost.
                     */
                    theScanner.setSubtractKeyReadCost(true);
                }

                while (theMoreElements) {

                    if (theRCB.checkTimeout()) {
                        throw new TimeoutException();
                    }

                    if (!theScanner.next()) {
                        break;
                    }

                    theBinaryPrimKey = theScanner.getKey().getData();
                    theResumeInfo.setDescResumeKey(thePosInJoin, theBinaryPrimKey);

                    if (theRCB.getTraceLevel() >= 3) {
                        theRCB.trace("Produced binary index entry in " +
                                     thePid + " : " +
                                     PlanIter.printKey(theBinaryPrimKey) +
                                     " op read KB = " + theOp.getReadKB());
                    }

                    if (theRCB.getTraceLevel() >= 3 && forTable != null) {
                        theRCB.trace("moveToNextIndexEntry for table " +
                                     forTable.getFullName());
                    }

                    int match = MultiGetTableKeysHandler.
                        keyInTargetTable(theHandlersManager.getLogger(),
                                         theOp,
                                         theTargetTables,
                                         theTableInfo,
                                         (forTable != null ?
                                          forTable.getNumKeyComponents() :
                                          -1),
                                         theScanner,
                                         theScanner.getLockMode(),
                                         false /* chargeReadCost */);

                    theBinaryPrimKey = theScanner.getKey().getData();
                    theResumeInfo.setDescResumeKey(thePosInJoin, theBinaryPrimKey);
                    theTable = theTableInfo.getCurrentTable();

                    if (match <= 0) {
                        if (match < 0) {
                            /* charge empty read, if nothing has been read before */
                            theOp.addEmptyReadCharge();
                            break;
                        }

                        if (theRCB.getTraceLevel() >= 3) {
                            theRCB.trace("Skipping non-matching binary index entry in " +
                                         thePid + " : " +
                                         PlanIter.printKey(theBinaryPrimKey));
                        }
                        continue;
                    }

                    /* The cost of reading the key of target table has been
                     * charged during theSecScanner.next(), so here don't charge
                     * the key of target table. */
                    if (!newTargetTableRow && theChargeForResumeKey) {
                        if (theRCB.getTraceLevel() >= 4) {
                            theRCB.trace("Charging key read. Current op read KB = " +
                                         theOp.getReadKB());
                        }
                        theOp.addMinReadCharge();
                    }

                    if (theScanner.isTombstone()) {
                        if (theRCB.getTraceLevel() >= 3) {
                            theRCB.trace("Skipping tombstone binary index entry in " +
                                         thePid + " : " +
                                         PlanIter.printKey(theBinaryPrimKey));
                        }
                        continue;
                    }

                    theTableRow = theTable.createRow();

                    /* if the current read caused the size limit to be exceeded,
                     * but the index is covering, then defer throwing
                     * SizeLimitException until the next call to next(). */
                    try {
                        checkSizeLimit(theRCB, theOp);
                    } catch (SizeLimitException sle) {
                        if (!usesCoveringIndex()) {
                            throw sle;
                        }
                        theExceededSizeLimit = true;
                    }

                    if (!theTable.initRowFromKeyBytes(theBinaryPrimKey,
                                                      -1, /*initPos*/
                                                      theTableRow)) {
                        continue;
                    }

                    if (theRCB.getTraceLevel() >= 2) {
                        theRCB.trace("Produced prim index row : " + theTableRow);
                    }

                    if (newTargetTableRow) {
                        newTargetTableRow = false;

                        /* Lock (but not read) the target-table row and check
                         * if the sec-index entry still points to this row. If
                         * not, move to the next sec-index entry. */
                        if (!theSecScanner.getCurrent()) {
                            theScanner.close();
                            theScanner = null;

                            return moveToNextEntryInCurrentScan(forTable);
                        }
                    }

                    theChargeForResumeKey = true;
                    theResumeInfo.setMoveAfterResumeKey(thePosInJoin, true);
                    return true;
                }

                if (theMoreElements) {
                    theScanner.close();
                    theScanner = null;
                    theResumeInfo.setDescResumeKey(thePosInJoin, null);
                    theTable = theTargetTable;
                    continue;
                }

                break;
            }

            return false;
        }
    }

    /**
     * Check if current read KB exceeds the read size limit or not, throw
     * SizeLimitException if true.
     */
    private static void checkSizeLimit(
        RuntimeControlBlock rcb,
        InternalOperation op) throws SizeLimitException {

        checkSizeLimit(rcb, op, 0);
    }

    /**
     * Called just before or just after a byte-reading op. If just before,
     * incBytes is the number of bytes that will be read by the op. In this
     * case, the method checks if reading the incBytes will cause the read
     * limit to be exceeded. If just after, incBytes will be 0 and the bytes
     * read by op will have been tallied into the op already. In this case
     * the method checks if the read limit has been exceeded. In both cases,
     * if the limit will be or has been exceeded, the method throws
     * SizeLimitException.
     */
    private static void checkSizeLimit(
        RuntimeControlBlock rcb,
        InternalOperation op,
        int incBytes) throws SizeLimitException {

        if (!rcb.getUseBytesLimit()) {
            return;
        }

        int incKB = (incBytes > 0 ? op.getReadKBToAdd(incBytes) : 0);

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Checking Size Limit. Current size = " +
                      op.getReadKB() + " additional bytes = " + incKB +
                      " Max Size = " + rcb.getCurrentMaxReadKB());
        }

        if (op.getReadKB() + incKB > rcb.getCurrentMaxReadKB()) {
            if (!rcb.cannotSuspend2()) {
                throw new SizeLimitException();
            }
        }
    }

    /**
     * A utility exception used to indicate that the readKB of a operation
     * exceeds the size limit.
     */
    @SuppressWarnings("serial")
    static class SizeLimitException extends Exception {

        /* Ture if the exception is throw after read the current entry */
        private boolean afterReadEntry;

        SizeLimitException() {
            this(false);
        }

        SizeLimitException(boolean afterReadEntry) {
            this.afterReadEntry = afterReadEntry;
        }

        boolean getAfterReadEntry() {
            return afterReadEntry;
        }
    }

    @SuppressWarnings("serial")
    static class TimeoutException extends Exception {
    }
}
