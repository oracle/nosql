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

import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.ops.PutHandler;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.TableQuery;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.IntegerValueImpl;
import oracle.kv.impl.api.table.JsonCollectionRowImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.ExprUpdateRow;
import oracle.kv.impl.query.runtime.InsertRowIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.PlanIterState;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.query.runtime.UpdateFieldIter;
import oracle.kv.impl.query.runtime.UpdateRowIter;
import oracle.kv.impl.rep.table.TableManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.table.TimeToLive;

/**
 *
 */
public class ServerUpdateRowIter extends UpdateRowIter {

    static IntegerValueImpl zero =
        FieldDefImpl.Constants.integerDef.createInteger(0);

    ServerIterFactoryImpl theOpContext;

    PutHandler thePutHandler;

    /* The runtime version of theAllIndexes. Instead of just the index names,
     * it stores the names of the je dbs that store the indexes */
    String[] theRTAllIndexes;

    ServerUpdateRowIter(UpdateRowIter parent) {
        super(parent);
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new UpdateRowState(this));

        theInputIter.open(rcb);
        for (PlanIter updIter : theUpdateOps) {
            updIter.open(rcb);
        }

        if (theTTLIter != null) {
            theTTLIter.open(rcb);
        }

        theOpContext = (ServerIterFactoryImpl)rcb.getServerIterFactory();
        thePutHandler = new PutHandler(theOpContext.getOperationHandler());

        if (theAllIndexes != null) {
            theRTAllIndexes = new String[theAllIndexes.length];
            for (int i = 0; i < theAllIndexes.length; ++i) {
                theRTAllIndexes[i] = TableManager.
                                     createDbName(theNamespace,
                                                  theAllIndexes[i],
                                                  theTableName);
            }
        }
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        theInputIter.close(rcb);
        for (PlanIter updIter : theUpdateOps) {
            updIter.close(rcb);
        }

        if (theTTLIter != null) {
            theTTLIter.close(rcb);
        }

        state.close();
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInputIter.reset(rcb);
        for (PlanIter updIter : theUpdateOps) {
            updIter.reset(rcb);
        }

        if (theTTLIter != null) {
            theTTLIter.reset(rcb);
        }

        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theInputIter.next(rcb);

        if (!more) {

            if (rcb.getReachedLimit()) {
                throw new QueryException(
                    "Query cannot be executed further because the " +
                    "computation of a single result consumes " +
                    "more bytes than the maximum allowed.",
                    theLocation);
            }

            if (theHasReturningClause) {
                state.done();
                return false;
            }
            rcb.setRegVal(theResultReg, zero);
            state.done();
            return true;
        }

        PartitionId pid = new PartitionId(rcb.getResumeInfo().getCurrentPid());
        TableQuery op = rcb.getQueryOp();

        RowImpl row = null;
        Put put = null;
        Result res = null;
        int numUpdated = 0;

        do {
            int inputReg = theInputIter.getResultReg();
            FieldValueImpl inVal = rcb.getRegVal(inputReg);

            if (!(inVal instanceof RowImpl)) {
                throw new QueryStateException(
                    "Update statement expected a row, but got this field " +
                    "value:\n" + inVal);
            }

            row = (RowImpl)inVal;
            row.setRowMetadata(rcb.getRowMetadata());

            put = createPut(row, rcb);

            if (put != null) {
                /*
                 * When update rows with shard key only, check if exceeded the
                 * max number of rows that can be updated in one query
                 */
                if (!theIsCompletePrimaryKey &&
                    numUpdated == rcb.getUpdateLimit()) {
                    if (rcb.getTraceLevel() >= 2) {
                        rcb.trace("Update query reached updateLimit: " +
                                  numUpdated);
                    }
                    throw new QueryException("Update query failed because the " +
                        "number of rows to be updated exceeded the limit of " +
                        rcb.getUpdateLimit());
                }

                /* Check if exceeded write size limit */
                if (op.getCurrentMaxWriteKB() > 0 &&
                    op.getWriteKB() > op.getCurrentMaxWriteKB()) {

                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("Update query exceeded maxWriteKB: " +
                                  op.getWriteKB() + " > " +
                                  op.getCurrentMaxWriteKB());
                    }
                    throw new QueryException("Update query failed due to " +
                        "the current data written in this query exceeded " +
                        "the limit(KB): [actual=" + op.getWriteKB() +
                        ", limit=" + op.getCurrentMaxWriteKB() + "]");
                }

                /*
                 * Configures the resource tracker of Put op with the tracker
                 * of TableQuery.
                 */
                put.setResourceTracker(op);

                res = thePutHandler.execute(put, theOpContext.getTxn(), pid);

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Updated the row with primary key: " +
                              row.createPrimaryKey());
                }

                op.addWriteKB(res.getWriteKB());
                numUpdated++;

                if (theHasReturningClause) {
                    if (res != null) {
                        row.setVersion(res.getNewVersion());
                    }
                }
            }

            /*
             * No need to get next entry if complete primary key is provided,
             * because at most only one row will be updated in this case.
             */
            if (theIsCompletePrimaryKey) {
                break;
            }

            more = theInputIter.next(rcb);

            /* Check if exceeded read size limit */
            if (rcb.getReachedLimit()) {
                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("Update query exceeded maxReadKB: " +
                              op.getReadKB() + " > " +
                              op.getCurrentMaxReadKB());
                }
                throw new QueryException("Update query failed due to " +
                    "the current data read in this query exceeded the " +
                    "limit: [actual=" + op.getReadKB() +
                    ", limit=" + op.getCurrentMaxReadKB() + "]");
            }

        } while (more);

        rcb.getResumeInfo().setPrimResumeKey(0, null);

        if (theHasReturningClause) {
            /*
             * Returning clause is allowed only if completed primary key is
             * specified
             */
            assert(theIsCompletePrimaryKey);

            if (row instanceof JsonCollectionRowImpl) {
                JsonCollectionRowImpl srow = (JsonCollectionRowImpl)row;
                srow.addPrimKeyAndPropertyFields(0);
                rcb.setRegVal(theResultReg, srow.getJsonCollectionMap());
            } else {
                rcb.setRegVal(theResultReg, row);
            }
        } else {
            RecordValueImpl retval =
                ExprUpdateRow.theNumRowsUpdatedType.createRecord();
            retval.put(0, numUpdated);
            rcb.setRegVal(theResultReg, retval);
        }

        state.done();
        return true;
    }

    private Put createPut(RowImpl row, RuntimeControlBlock rcb) {
        boolean updated = false;
        boolean updateTTL = false;
        long exptime = row.getExpirationTime();
        TableImpl table = row.getTableImpl();

        for (PlanIter updFieldIter : theUpdateOps) {

            ((UpdateFieldIter)updFieldIter).setRow(rcb, row);

            if (updFieldIter.next(rcb)) {
                updated = true;
            }
            updFieldIter.reset(rcb);
        }

        TimeToLive ttlObj = InsertRowIter.setRowExpTime(rcb, row,
                                                        theUpdateTTL,
                                                        theTTLIter,
                                                        theTTLUnit,
                                                        theLocation);

        if (ttlObj != null || (theUpdateTTL && exptime > 0)) {
            updated = true;
            updateTTL = true;
            if (theTTLIter != null) {
                theTTLIter.reset(rcb);
            }
        }

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("Row after update =\n" + row);
        }

        if (!updated) {
            return null;
        }

        KVStore kvstore = thePutHandler.getOperationHandler().
            getRepNode().getKVStore();

        if (table.hasIdentityColumn()) {
            RecordDefImpl rowDef = table.getRowDef();
            int idCol = table.getIdentityColumn();
            FieldValueImpl userValue = row.get(idCol);
            boolean isOnNull = table.isIdentityOnNull();
            /*fill the sequence number only if identity column is updated to
             * NULL and the SG type is by default on null*/
            if (isOnNull && userValue.isNull()) {
                FieldValueImpl generatedValue =
                    ((KVStoreImpl)kvstore).getIdentityNextValue(table,
                        rowDef.getFieldDef(idCol), 0, userValue, idCol);
                if (generatedValue != null) {
                    row.putInternal(idCol, generatedValue, false);
                }
            }
        }

        Key rowkey = row.getPrimaryKey(false/*allowPartial*/);
        Value rowval = table.createValue(row, false);

        KeySerializer keySerializer =
            KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        byte[] keybytes = keySerializer.toByteArray(rowkey);

        return new Put(OpCode.PUT,
                       keybytes,
                       rowval,
                       ReturnValueVersion.Choice.NONE,
                       row.getTableImpl().getId(),
                       ttlObj,
                       updateTTL,
                       true,
                       theRTAllIndexes,
                       theAllIndexIds,
                       theIndexesToUpdate);
    }
}
