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

import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.impl.api.ops.Delete;
import oracle.kv.impl.api.ops.DeleteHandler;
import oracle.kv.impl.api.ops.TableQuery;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.runtime.PlanIterState;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.query.runtime.DeleteRowIter;
import oracle.kv.impl.topo.PartitionId;

/**
 *
 */
public class ServerDeleteRowIter extends DeleteRowIter {

    ServerIterFactoryImpl theOpContext;

    DeleteHandler theOpHandler;

    TableQuery theQueryOp;

    TableImpl theTable;

    PrimaryKeyImpl thePrimKey;

    long theNumDeleted;

    public ServerDeleteRowIter(
        RuntimeControlBlock rcb,
        DeleteRowIter parent) {

        super(parent);

        TableMetadataHelper md =  rcb.getMetadataHelper();

        theTable = md.getTable(theNamespace, theTableName);
        thePrimKey = theTable.createPrimaryKey();
        theQueryOp = rcb.getQueryOp();
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new DeleteRowState(this));
        theInput.open(rcb);

        theOpContext = (ServerIterFactoryImpl)rcb.getServerIterFactory();
        theOpHandler = new DeleteHandler(theOpContext.getOperationHandler());
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        theNumDeleted = 0;
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInput.close(rcb);

        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (theHasReturningClause) {

            if (rcb.getReachedLimit()) {
                state.done();
                return false;
            }

            boolean more = theInput.next(rcb);

            if (!more) {
                state.done();
                return false;
            }

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            thePrimKey.clear();

            if (val.isTuple()) {
                TupleValue tuple = (TupleValue)val;
                for (int i = 0; i < thePrimKey.getNumFields(); ++i) {
                    thePrimKey.putInternal(i, tuple.get(thePrimKeyPositions[i]));
                }
                rcb.setRegVal(theResultReg, tuple.toRecord());
            } else {
                thePrimKey.putInternal(0, val);
                rcb.setRegVal(theResultReg, val);
            }

            deleteRow(rcb);
            return true;
        }

        int numDeleted = 0;

        boolean more = theInput.next(rcb);

        while (more) {

            TupleValue primkey =
                (TupleValue)rcb.getRegVal(theInput.getResultReg());

            thePrimKey.clear();

            for (int i = 0; i < primkey.getNumFields(); ++i) {
                thePrimKey.putInternal(i, primkey.get(i));
            }

            deleteRow(rcb);
            ++theNumDeleted;
            ++numDeleted;

            if (numDeleted == rcb.getDeleteLimit()) {
                rcb.setNeedToSuspend(true);
            }

            if (rcb.needToSuspend()) {
                break;
            }

            more = theInput.next(rcb);
        }

        FieldValueImpl numDeletedVal =
            FieldDefImpl.Constants.longDef.createLong(theNumDeleted);

        RecordValueImpl record = theResultType.createRecord();
        record.put(0, numDeletedVal);

        rcb.setRegVal(theResultReg, record);

        state.done();
        return true;
    }

    private void deleteRow(RuntimeControlBlock rcb) {

        TableKey key = TableKey.createKey(theTable, thePrimKey,
                                          false/*allowPartial*/);

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Primary key of row to delete: " + thePrimKey);
        }

        ResumeInfo ri = rcb.getResumeInfo();
        PartitionId pid;

        if (ri.getCurrentPid() >= 0) {
            pid = new PartitionId(ri.getCurrentPid());
        } else {
            byte[] pk = thePrimKey.getPrimaryKey(false).toByteArray();
            pid = theOpContext.getOperationHandler().
                  getRepNode().getPartitionId(pk);
        }

        Delete op = new Delete(key.getKeyBytes(), Choice.NONE, theTable.getId(),
                               rcb.doTombstone(), rcb.getRowMetadata());
        /*
         * Configure the resource tracker of Delete op with the
         * tracker of TableQuery.
         */
        op.setResourceTracker(theQueryOp);

        theOpHandler.execute(op, theOpContext.getTxn(), pid);

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Deleted row with primary key: " + thePrimKey);
        }

        if (rcb.getUseBytesLimit()) {

            theQueryOp.addReadKB(op.getReadKB());
            theQueryOp.addWriteKB(op.getWriteKB());

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Delete: checking byte limit: readKB = " +
                          theQueryOp.getReadKB() + " writeKB = " +
                          theQueryOp.getWriteKB());
            }

            if (theQueryOp.getWriteKB() >= theQueryOp.getCurrentMaxWriteKB()) {

                rcb.setReachedLimit();

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Delete reached maxWriteKB: " +
                              theQueryOp.getWriteKB() + " >= " +
                              theQueryOp.getCurrentMaxWriteKB());
                }
            } else if (theQueryOp.getReadKB() >=
                       theQueryOp.getCurrentMaxReadKB()) {

                rcb.setReachedLimit();

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Delete reached maxReadKB: " +
                              theQueryOp.getReadKB() + " >= " +
                              theQueryOp.getCurrentMaxReadKB());
                }
            }
        }
    }
}
