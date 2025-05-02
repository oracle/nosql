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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_14;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.query.QueryPublisher;
import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.EmptyValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.LongValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.ExprSFW;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;

/**
 * SFWIter evaluates a SELECT-FROM-WHERE query block. The iterator produces
 * a sequence of FieldValues. If the SFW expr does implicit record construction
 * (i.e., the SELECT clause contains more than 1 expr, or a single expr with an
 * AS clause), then the sequence consists of TupleValues. In fact, in this case,
 * a single TupleValue is allocated (during the open() method) and placed in
 * this.theResultReg. The TupleValue points to the registers that hold the
 * field values of the tuple. These registers are filled-in during each next()
 * call. The names of the tuple columns are statically known (i.e., they don't
 * need to be computed during runtime), and as a result, they are not placed
 * in registers; instead they are available via method calls.
 *
 * theFromIters:
 *
 * theFromVarNames:
 *
 * theWhereIter:
 *
 * theColumnIters:
 *
 * theColumnNames:
 *
 * theNumGBColumns:
 * see ExprSFW.theNumGroupByExprs
 * Introduced in v18.1
 *
 * theDoNullOnEmpty:
 * see ExprSFW.theDoNullOnEmpty.
 * Introduced in v4.4
 *
 * theTupleRegs:
 *
 * theTypeDefinition:
 * The type of the data returned by this iterator. This will always be a
 * RecordDefImpl, but to support older drivers, we allow FieldDefImpl
 */
public class SFWIter extends PlanIter {

    public static class SFWIterState extends PlanIterState {

        private int theNumBoundVars;

        private long theOffset;

        private long theLimit;

        private long theNumResults;

        private FieldValueImpl[] theGBTuple;

        private FieldValueImpl[] theSwapGBTuple;

        private boolean theHaveGBTuple;

        private boolean[] theFromItersFirstCall;

        private boolean theDoOldStyleGrouping;

        SFWIterState(RuntimeControlBlock rcb, SFWIter iter) {

            theFromItersFirstCall = new boolean[iter.theFromIters.length];
            for (int i = 0; i < theFromItersFirstCall.length; ++i) {
                theFromItersFirstCall[i] = true;
            }

            theDoOldStyleGrouping =
                (rcb.getCompilerVersion() < QUERY_VERSION_14 &&
                 rcb.isServerRCB() &&
                 rcb.getQueryOp().getOpCode() == OpCode.QUERY_SINGLE_PARTITION);
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theNumBoundVars = 0;
            theNumResults = 0;
            theHaveGBTuple = false;
            theGBTuple = null;
            theSwapGBTuple = null;
        }
    }

    private final PlanIter[] theFromIters;

    private final String[][] theFromVarNames;

    private final PlanIter theWhereIter;

    private final PlanIter[] theColumnIters;

    private final String[] theColumnNames;

    private final int theNumGBColumns;

    private final boolean theIsGroupingForDistinct;

    private final boolean theDoNullOnEmpty;

    private final int[] theTupleRegs;

    private final FieldDefImpl theTypeDefinition;

    private final PlanIter theOffsetIter;

    private final PlanIter theLimitIter;

    public SFWIter(
        ExprSFW e,
        int resultReg,
        int[] tupleRegs,
        PlanIter[] fromIters,
        String[][] fromVarNames,
        PlanIter whereIter,
        PlanIter[] columnIters,
        String[] columnNames,
        PlanIter offsetIter,
        PlanIter limitIter,
        boolean forCloud) {

        super(e, resultReg, forCloud);
        theFromIters = fromIters;
        theFromVarNames = fromVarNames;
        theWhereIter = whereIter;
        theColumnIters = columnIters;
        theColumnNames = columnNames;
        theNumGBColumns = e.getNumGroupExprs();
        theIsGroupingForDistinct = e.isGroupingForDistinct();
        theDoNullOnEmpty = e.doNullOnEmpty();
        theTupleRegs = tupleRegs;
        theTypeDefinition = e.getType().getDef();
        theOffsetIter = offsetIter;
        theLimitIter = limitIter;
    }

    SFWIter(DataInput in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theTypeDefinition = (FieldDefImpl)deserializeFieldDef(in, serialVersion);
        theTupleRegs = deserializeIntArray(in, serialVersion);
        theColumnNames = deserializeStringArray(in, serialVersion);
        theColumnIters = deserializeIters(in, serialVersion);

        theNumGBColumns = in.readInt();

        theFromIters = deserializeIters(in, serialVersion);
        theFromVarNames = new String[theFromIters.length][];

        for (int i = 0; i < theFromIters.length; ++i) {
            theFromVarNames[i] =
                PlanIter.deserializeStringArray(in, serialVersion);
        }
        theWhereIter = deserializeIter(in, serialVersion);
        theOffsetIter = deserializeIter(in, serialVersion);
        theLimitIter = deserializeIter(in, serialVersion);

        theDoNullOnEmpty = in.readBoolean();

        theIsGroupingForDistinct = in.readBoolean();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        if (theIsCloudDriverIter) {
            assert(theFromIters.length == 1);
            theFromIters[0].writeFastExternal(out, serialVersion);
            return;
        }

        super.writeFastExternal(out, serialVersion);

        serializeFieldDef(theTypeDefinition, out, serialVersion);
        serializeIntArray(theTupleRegs, out, serialVersion);
        serializeStringArray(theColumnNames, out, serialVersion);

        serializeIters(theColumnIters, out, serialVersion);

        out.writeInt(theNumGBColumns);

        serializeIters(theFromIters, out, serialVersion);

        for (int i = 0; i < theFromIters.length; ++i) {
            PlanIter.serializeStringArray(
                theFromVarNames[i], out, serialVersion);
        }

        serializeIter(theWhereIter, out, serialVersion);
        serializeIter(theOffsetIter, out, serialVersion);
        serializeIter(theLimitIter, out, serialVersion);

        out.writeBoolean(theDoNullOnEmpty);

        out.writeBoolean(theIsGroupingForDistinct);
    }

    @Override
    public void writeForCloud(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        assert(theIsCloudDriverIter);
        assert(theFromIters.length == 1);

        writeForCloudCommon(out, driverVersion);

        CloudSerializer.writeStringArray(theColumnNames, out);
        out.writeInt(theNumGBColumns);
        CloudSerializer.writeString(theFromVarNames[0][0], out);
        out.writeBoolean(theTupleRegs == null);

        CloudSerializer.writeIters(theColumnIters, out, driverVersion, valWriter);
        CloudSerializer.writeIter(theFromIters[0], out, driverVersion, valWriter);
        CloudSerializer.writeIter(theOffsetIter, out, driverVersion, valWriter);
        CloudSerializer.writeIter(theLimitIter, out, driverVersion, valWriter);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.SELECT;
    }

    @Override
    public int[] getTupleRegs() {
        return theTupleRegs;
    }

    public int getNumColumns() {
        return theColumnNames.length;
    }

    public String getColumnName(int i) {
        return theColumnNames[i];
    }

    @Override
    public void setPublisher(
        RuntimeControlBlock rcb,
        QueryPublisher pub) {

        for (final PlanIter iter : theFromIters) {
            iter.setPublisher(rcb, pub);
        }
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        SFWIterState state = new SFWIterState(rcb, this);

        rcb.setState(theStatePos, state);

        if (theTupleRegs != null) {

            TupleValue tuple = new TupleValue((RecordDefImpl)theTypeDefinition,
                                              rcb.getRegisters(),
                                              theTupleRegs);

            rcb.setRegVal(theResultReg, tuple);
        }

        for (int i = 0; i < theFromIters.length; ++i) {
            theFromIters[i].open(rcb);
        }

        if (theWhereIter != null) {
            theWhereIter.open(rcb);
        }

        for (PlanIter columnIter : theColumnIters) {
            columnIter.open(rcb);
        }

        computeOffsetLimit(rcb);

        if (theNumGBColumns >= 0) {
            if (rcb.isServerRCB()) {
                initAggrValues(rcb, state);
            } else {
                state.theGBTuple = new FieldValueImpl[theColumnIters.length];
                state.theSwapGBTuple = new FieldValueImpl[theColumnIters.length];
            }
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        SFWIterState state = (SFWIterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (int i = 0; i < theFromIters.length; ++i) {
            theFromIters[i].reset(rcb);
        }

        if (theWhereIter != null) {
            theWhereIter.reset(rcb);
        }

        for (PlanIter columnIter : theColumnIters) {
            columnIter.reset(rcb);
        }

        if (theOffsetIter != null) {
            theOffsetIter.reset(rcb);
        }

        if (theLimitIter != null) {
            theLimitIter.reset(rcb);
        }

        state.reset(this);

        if (theNumGBColumns >= 0) {
            initAggrValues(rcb, state);
        }

        computeOffsetLimit(rcb);
    }

    private void initAggrValues(RuntimeControlBlock rcb, SFWIterState state) {

        if (!state.theDoOldStyleGrouping) {

            state.theGBTuple = new FieldValueImpl[theColumnIters.length];
            state.theSwapGBTuple = new FieldValueImpl[theColumnIters.length];

            for (int i = theNumGBColumns; i < state.theGBTuple.length; ++i) {
                theColumnIters[i].initAggrValue(rcb, null);
            }
        } else {
            ResumeInfo ri = rcb.getResumeInfo();

            if (ri.getGBTuple() != null) {
                state.theGBTuple = ri.getGBTuple();
                state.theHaveGBTuple = true;
            } else {
                state.theGBTuple = new FieldValueImpl[theColumnIters.length];
            }

            state.theSwapGBTuple = new FieldValueImpl[theColumnIters.length];

            if (state.theHaveGBTuple && rcb.getTraceLevel() >= 2) {
                rcb.trace("SFW Received resume GB tuple:");
                for (int i = 0; i < state.theGBTuple.length; ++i) {
                    rcb.trace("Val " + i + " = " + state.theGBTuple[i]);
                }
            }

            /* Init the iterators computing the aggr functions */
            for (int i = theNumGBColumns; i < state.theGBTuple.length; ++i) {
                theColumnIters[i].initAggrValue(rcb, state.theGBTuple[i]);
            }
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        return nextInternal(rcb, false /* localOnly */);
    }

    @Override
    public boolean nextLocal(RuntimeControlBlock rcb) {
        return nextInternal(rcb, true /* localOnly */);
    }

    private boolean nextInternal(RuntimeControlBlock rcb, boolean localOnly) {

        SFWIterState state = (SFWIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (state.theNumResults >= state.theLimit) {

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("SFW: reached LIMIT clause " + state.theLimit);
            }
            state.done();
            theFromIters[0].reset(rcb);
            ResumeInfo ri = rcb.getResumeInfo();
            ri.setPrimResumeKey(0, null);
            ri.setSecResumeKey(0, null);
            if (!state.theDoOldStyleGrouping) {
                ri.setGBTuple(null);
            }
            return false;
        }

        /* while loop for skipping offset results */
        while (true) {

            boolean more = computeNextResult(rcb, state, localOnly);

            if (!more) {
                return false;
            }

            /*
             * Even though we have a result, the state may be DONE. This is the
             * case when the result is the last group tuple in a grouping SFW.
             * In this case, if we have not reached the offset yet, we should
             * ignore this result and return false.
             */
            if (state.isDone() && state.theOffset > 0) {
                if (!state.theDoOldStyleGrouping) {
                    rcb.getResumeInfo().setGBTuple(null);
                }
                return false;
            }

            if (state.theOffset == 0) {
                ++state.theNumResults;
                break;
            }

            --state.theOffset;
        }

        return true;
    }

    boolean computeNextResult(
        RuntimeControlBlock rcb,
        SFWIterState state,
        boolean localOnly) {

        /* while loop for group by */
        while (true) {

            if (theWhereIter != null) {

                boolean whereValue = true;

                do {
                    if (!getNextFROMTuple(rcb, state, localOnly)) {

                        if (theNumGBColumns >= 0) {
                            return produceLastGroup(rcb, state, localOnly);
                        }

                        return false;
                    }

                    boolean more = theWhereIter.next(rcb);

                    if (!more) {
                        whereValue = false;
                    } else {
                        FieldValueImpl val =
                            rcb.getRegVal(theWhereIter.getResultReg());

                        whereValue = (val.isNull() ? false : val.getBoolean());
                    }

                    theWhereIter.reset(rcb);

                } while (whereValue == false);

            } else {
                if (!getNextFROMTuple(rcb, state, localOnly)) {

                    if (theNumGBColumns >= 0) {
                        return produceLastGroup(rcb, state, localOnly);
                    }

                    return false;
                }
            }

            /*
             * Compute the exprs in the SELECT list. If this is a grouping
             * SFW, compute only the group-by columns. However, skip this
             * computation if this is not a grouping SFW and it has an offset
             * that has not been reached yet.
             */

            if (theNumGBColumns < 0 && state.theOffset > 0) {
                return true;
            }

            int numCols = (theNumGBColumns >= 0 ?
                           theNumGBColumns :
                           theColumnIters.length);
            int i = 0;

            /*
             * Starting with 19.3, theTupleRegs == null iff it's a SELECT *
             * query. In this case, theNumGBColumns will be < 0, which means
             * numCols > 0. Futhermore, the next() call on theColumnIters[0]
             * will always return true.
             *
             * Before 19.3, theTupleRegs can be null also when the SELECT
             * clause contains a single expr without an AS clause.
             */
            if (numCols > 0 && theTupleRegs == null) {

                assert(theNumGBColumns < 0);

                boolean more = theColumnIters[0].next(rcb);
                assert(more);

                if (!more) {
                    rcb.setRegVal(theResultReg,
                                  (theDoNullOnEmpty ?
                                   NullValueImpl.getInstance() :
                                   EmptyValueImpl.getInstance()));
                } else {
                    i = 1;
                }

                /*
                 * theResultReg is the same as theColumnIters[0].theResultReg,
                 * so no need to set this.theResultReg.
                 */
                theColumnIters[0].reset(rcb);

                if (theNumGBColumns < 0) {
                    return true;
                }

            } else {

                for (i = 0; i < numCols; ++i) {

                    PlanIter columnIter = theColumnIters[i];
                    boolean more = columnIter.next(rcb);

                    if (!more) {

                        /* Skip the curent input row if any of the gb exprs
                           return empty result on this row */
                        if (theNumGBColumns > 0 && !theIsGroupingForDistinct) {
                            columnIter.reset(rcb);
                            break;
                        }

                        FieldValueImpl value = (theDoNullOnEmpty ?
                                                NullValueImpl.getInstance() :
                                                EmptyValueImpl.getInstance());

                        if (rcb.getTraceLevel() >= 4) {
                            rcb.trace("Value for SFW column " + i +
                                      " = " + value);
                        }
                        rcb.setRegVal(theTupleRegs[i], value);
                    } else {
                        /*
                         * theTupleRegs[i] is the same as
                         * theColumnIters[i].theResultReg, so no need to set
                         * theTupleRegs[i], unless the value stored there is
                         * another TupleValue, in which case we convert it to
                         * a record so that we don't have to deal with nested
                         * TupleValues.
                         */
                        FieldValueImpl value =
                            rcb.getRegVal(columnIter.getResultReg());

                        if (rcb.getTraceLevel() >= 4) {
                            rcb.trace("Value for SFW column " + i +
                                      " = " + value);
                        }

                        if (value.isTuple()) {
                            value = ((TupleValue)value).toRecord();
                            rcb.setRegVal(theTupleRegs[i], value);
                        }
                    }

                    /* the column iterators need to be reset for the next call
                     * to next */
                    columnIter.reset(rcb);
                }
            }

            if (i < numCols) {
                continue;
            }

            if (theNumGBColumns < 0) {
                break;
            }

            if (groupInputTuple(rcb, state)) {
                break;
            }
        }

        return true;
    }

    private boolean getNextFROMTuple(
        RuntimeControlBlock rcb,
        SFWIterState state,
        boolean localOnly) {

        while (0 <= state.theNumBoundVars &&
               state.theNumBoundVars < theFromIters.length) {

            PlanIter fromIter = theFromIters[state.theNumBoundVars];
            boolean hasNext;
            boolean isFirstCall =
                state.theFromItersFirstCall[state.theNumBoundVars];

            if (localOnly && state.theNumBoundVars == 0) {

                hasNext = fromIter.nextLocal(rcb);

                if (!hasNext) {
                    if (theFromIters[0].isDone(rcb)) {
                        state.done();
                    }
                    return false;
                }
            } else {
                hasNext = fromIter.next(rcb);
            }

            if (!hasNext) {

                if (rcb.getTraceLevel() >= 3 && theFromIters.length > 1) {
                    rcb.trace("No Value for FROM clause " +
                              state.theNumBoundVars);
                }

                if (state.theNumBoundVars > 0 && isFirstCall) {

                    if (rcb.getTraceLevel() >= 3) {
                        rcb.trace("First call for FROM clause " +
                                  state.theNumBoundVars);
                    }
                    rcb.setRegVal(fromIter.theResultReg,
                                  EmptyValueImpl.getInstance());
                    state.theFromItersFirstCall[state.theNumBoundVars] = false;
                    ++state.theNumBoundVars;
                } else {
                    state.theFromItersFirstCall[state.theNumBoundVars] = true;
                    if (state.theNumBoundVars > 0) {
                        fromIter.reset(rcb);
                    }
                    --state.theNumBoundVars;
                }

                if (rcb.isServerRCB() && rcb.getReachedLimit()) {

                    if (state.theDoOldStyleGrouping &&
                        theNumGBColumns >= 0 &&
                        state.theHaveGBTuple) {

                        int numCols = theColumnIters.length;

                        for (int i = theNumGBColumns; i < numCols; ++i) {
                            state.theGBTuple[i] =
                                theColumnIters[i].getAggrValue(rcb, false);
                        }

                        rcb.getResumeInfo().setGBTuple(state.theGBTuple);
                    }

                    if (theOffsetIter != null) {
                         rcb.getResumeInfo().setOffset(state.theOffset);
                    }
                }
            } else {
                if (rcb.getTraceLevel() >= 3 && theFromIters.length > 1) {
                    rcb.trace("Value for FROM clause " + state.theNumBoundVars +
                              " = " + rcb.getRegVal(fromIter.theResultReg));
                }

                state.theFromItersFirstCall[state.theNumBoundVars] = false;
                ++state.theNumBoundVars;
            }
        }

        if (state.theNumBoundVars < 0) {
            rcb.resetCannotSuspend();
            state.done();
            return false;
        }

        assert(state.theNumBoundVars == theFromIters.length);

        if (theFromIters.length > 1) {
            rcb.setCannotSuspend();
        }

        --state.theNumBoundVars;
        return true;
    }

    /*
     * This method checks whether the current input tuple (a) starts the
     * first group, i.e. it is the very 1st tuple in the input stream, or
     * (b) belongs to the current group, or (c) starts a new group otherwise.
     * The method returns true in case (c), indicating that an output tuple
     * is ready to be returned to the consumer of this SFW. Otherwise, false
     * is returned.
     */
    boolean groupInputTuple(RuntimeControlBlock rcb, SFWIterState state) {

        int numCols = theColumnIters.length;

        /*
         * If this is the very first input tuple, start the first group and
         * go back to compute next input tuple.
         */
        if (!state.theHaveGBTuple) {

            for (int i = 0; i < theNumGBColumns; ++i) {
                state.theGBTuple[i] = rcb.getRegVal(
                    theColumnIters[i].getResultReg());
            }

            for (int i = theNumGBColumns; i < numCols; ++i) {
                theColumnIters[i].next(rcb);
                theColumnIters[i].reset(rcb);
            }

            state.theHaveGBTuple = true;

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Started first group:");
                traceCurrentGroup(rcb, state);
            }

            return false;
        }

        /*
         * Compare the current input tuple with the current group tuple.
         */
        int j;
        for (j = 0; j < theNumGBColumns; ++j) {
            FieldValueImpl newval = rcb.getRegVal(
                theColumnIters[j].getResultReg());
            FieldValueImpl curval = state.theGBTuple[j];
            if (!newval.equal(curval)) {
                break;
            }
        }

        /*
         * If the input tuple is in current group, update the aggregate
         * functions and go back to compute the next input tuple.
         */
        if (j == theNumGBColumns) {

            for (int i = theNumGBColumns; i < numCols; ++i) {
                theColumnIters[i].next(rcb);
                theColumnIters[i].reset(rcb);
            }

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Input tuple belongs to current group:");
                traceCurrentGroup(rcb, state);
            }

            return false;
        }

        /*
         * Input tuple starts new group. We must finish up the current group,
         * produce a result (output tuple) from it, and init the new group.
         * Specifically:
         *
         * 1. Get the final aggregate values for the current group and store
         *    them in theGBTuple.
         * 2. Save the input tuple in theSwapGBTuple (because the
         *    result regs that are now storing the input tuple
         *    will be used to store the output tuple).
         * 3. Swap theGBTuple with theSwapGBTuple.
         * 4. Aggregate the non-gb expressions for the new group.
         * 5. Move the finished group tuple from theSwapGBTuple to the
         *    result regs of theColumnIters.
         * 6. Put a ref to the new group tuple in the RCB to be sent back
         *    to the client as part of the resume info, if the output tuple
         *    produced here happens to be the last one in the batch.
         */

        for (int i = theNumGBColumns; i < numCols; ++i) {
            state.theGBTuple[i] = theColumnIters[i].getAggrValue(rcb, true);
        }

        for (int i = 0; i < theNumGBColumns; ++i) {
            state.theSwapGBTuple[i] = rcb.getRegVal(
                    theColumnIters[i].getResultReg());
        }

        FieldValueImpl[] temp = state.theSwapGBTuple;
        state.theSwapGBTuple = state.theGBTuple;
        state.theGBTuple = temp;

        for (int i = theNumGBColumns; i < numCols; ++i) {
            theColumnIters[i].next(rcb);
            theColumnIters[i].reset(rcb);
            state.theGBTuple[i] = theColumnIters[i].getAggrValue(rcb, false);
        }

        for (int i = 0; i < numCols; ++i) {
            rcb.setRegVal(theColumnIters[i].getResultReg(),
                          state.theSwapGBTuple[i]);
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Started new group:");
            traceCurrentGroup(rcb, state);
        }

        if (state.theDoOldStyleGrouping) {
            rcb.getResumeInfo().setGBTuple(state.theGBTuple);
        } else {
            /* Set a dummy gb tuple. It is needed by the PartitionUnionIter to
             * check if there is a cached group, in which case it must consume
             * this cached gropu before moving to the next partition. */
            rcb.getResumeInfo().setGBTuple(new FieldValueImpl[0]);
        }

        return true;
    }

    boolean produceLastGroup(
        RuntimeControlBlock rcb,
        SFWIterState state,
        boolean localOnly) {

        ResumeInfo ri = rcb.getResumeInfo();

        if (rcb.isServerRCB()) {

            if (state.theDoOldStyleGrouping && rcb.getReachedLimit()) {
                return false;
            }

            if (!state.theHaveGBTuple) {

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("produceLastGroup: no cached group:");
                    traceCurrentGroup(rcb, state);
                }

                if (theNumGBColumns > 0) {
                    if (!state.isDone()) {
                        state.done();
                    }
                    return false;
                }

                for (int i = 0; i < theColumnIters.length; ++i) {

                    PlanIter aggrIter = theColumnIters[i];

                    if (aggrIter.getKind() == PlanIterKind.FUNC_COUNT ||
                        aggrIter.getKind() == PlanIterKind.FUNC_COUNT_STAR) {
                        rcb.setRegVal(aggrIter.getResultReg(), LongValueImpl.ZERO);
                    } else if (aggrIter.getKind() == PlanIterKind.FUNC_COLLECT) {
                        ArrayDefImpl def = ((FuncCollectIter)aggrIter).getArrayDef();
                        rcb.setRegVal(aggrIter.getResultReg(), def.createArray());
                    } else {
                        rcb.setRegVal(aggrIter.getResultReg(),
                                      NullValueImpl.getInstance());
                    }
                }

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("produceLastGroup: Produced empty group:");
                    traceCurrentGroup(rcb, state);
                }

                ri.setGBTuple(null);

                /* If this is a partial group, don't count it in the number of
                 * results computed by this batch */
                if (ri.getPrimResumeKey(0) != null) {
                    ri.setNumResultsComputed(-1);
                }

                return true;
            }

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Produced last group:");
                traceCurrentGroup(rcb, state);
            }

            for (int i = 0; i < theNumGBColumns; ++i) {
                rcb.setRegVal(theColumnIters[i].getResultReg(),
                              state.theGBTuple[i]);
            }

            for (int i = theNumGBColumns; i < theColumnIters.length; ++i) {
                rcb.setRegVal(theColumnIters[i].getResultReg(),
                              theColumnIters[i].getAggrValue(rcb, true));
            }

            ri.setGBTuple(null);
            if (!state.isDone()) {
                state.done();
            }

            /* If this is a partial group, don't count it in the number of
             * results computed by this batch */
            if (ri.getPrimResumeKey(0) != null) {
                ri.setNumResultsComputed(-1);
            }

            return true;
        }

        if (localOnly && !theFromIters[0].isDone(rcb)) {
            return false;
        }

        if (!state.theHaveGBTuple) {
            return false;
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Produced last group:");
            traceCurrentGroup(rcb, state);
        }

        for (int i = 0; i < theNumGBColumns; ++i) {
            rcb.setRegVal(theColumnIters[i].getResultReg(),
                          state.theGBTuple[i]);
        }

        for (int i = theNumGBColumns; i < theColumnIters.length; ++i) {
            rcb.setRegVal(theColumnIters[i].getResultReg(),
                          theColumnIters[i].getAggrValue(rcb, true));
        }

        return true;
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        SFWIterState state = (SFWIterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (int i = 0; i < theFromIters.length; ++i) {
            theFromIters[i].close(rcb);
        }

        if (theWhereIter != null) {
            theWhereIter.close(rcb);
        }

        for (PlanIter columnIter : theColumnIters) {
            columnIter.close(rcb);
        }

        if (theOffsetIter != null) {
            theOffsetIter.close(rcb);
        }

        if (theLimitIter != null) {
            theLimitIter.close(rcb);
        }

        state.close();
    }

    @Override
    public Throwable getCloseException(RuntimeControlBlock rcb) {
        return theFromIters[0].getCloseException(rcb);
    }

    private void computeOffsetLimit(RuntimeControlBlock rcb) {

        SFWIterState state = (SFWIterState)rcb.getState(theStatePos);

        long offset = 0;
        long limit = -1;

        if (theOffsetIter != null) {
            theOffsetIter.open(rcb);
            theOffsetIter.next(rcb);
            FieldValueImpl val = rcb.getRegVal(theOffsetIter.getResultReg());
            offset = val.getLong();

            if (offset < 0) {
                throw new QueryException(
                   "Offset can not be a negative number",
                    theOffsetIter.theLocation);
            }

            if (offset > Integer.MAX_VALUE) {
                throw new QueryException(
                   "Offset can not be greater than Integer.MAX_VALUE",
                    theOffsetIter.theLocation);
            }
        }

        if (theLimitIter != null) {
            theLimitIter.open(rcb);
            theLimitIter.next(rcb);
            FieldValueImpl val = rcb.getRegVal(theLimitIter.getResultReg());
            limit = val.getLong();

            if (limit < 0) {
                throw new QueryException(
                    "Limit can not be a negative number",
                    theLimitIter.theLocation);
            }

            if (limit > Integer.MAX_VALUE) {
                throw new QueryException(
                   "Limit can not be greater than Integer.MAX_VALUE",
                    theOffsetIter.theLocation);
            }
        }

        ResumeInfo ri = rcb.getResumeInfo();
        long numResultsComputed = 0;

        if (ri != null) {
            numResultsComputed = ri.getNumResultsComputed();
        }

        if (limit < 0) {
            limit = Long.MAX_VALUE;
        } else {
            limit -= numResultsComputed;
        }

        if (numResultsComputed > 0) {
            offset = 0;
        } else if (rcb.isServerRCB() &&
                   theOffsetIter != null &&
                   ri != null &&
                   ri.getOffset() >= 0) {
            offset = ri.getOffset();
        }

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("offset = " + offset + " limit = " + limit);
        }

        state.theOffset = offset;
        state.theLimit = limit;
    }

    @Override
    public Map<String, String> getRNTraces(RuntimeControlBlock rcb) {
        return theFromIters[0].getRNTraces(rcb);
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        for (int i = 0; i < theFromIters.length; ++i) {
            formatter.indent(sb);
            sb.append("\"FROM\" :\n");
            theFromIters[i].display(sb, formatter, verbose);

            sb.append(",\n");
            formatter.indent(sb);
            if (theFromVarNames[i].length == 1) {
                sb.append("\"FROM variable\" : \"");
                sb.append(theFromVarNames[i][0]).append("\"");
            } else {
                sb.append("\"FROM variables\" : [");
                for (int j = 0; j < theFromVarNames[i].length; ++j ) {
                    String vname = theFromVarNames[i][j];
                    sb.append("\"").append(vname).append("\"");
                    if (j < theFromVarNames[i].length - 1) {
                        sb.append(", ");
                    }
                }
                sb.append("]");
            }
            sb.append(",\n");
        }

        if (theWhereIter != null) {
            formatter.indent(sb);
            sb.append("\"WHERE\" : \n");
            theWhereIter.display(sb, formatter, verbose);
            sb.append(",\n");
        }

        if (theNumGBColumns >= 0) {
            formatter.indent(sb);
            sb.append("\"GROUP BY\" : ");
            if (theNumGBColumns == 0) {
                sb.append("\"No grouping expressions\"");
            } else if (theNumGBColumns == 1) {
                sb.append("\"Grouping by the first expression " +
                          "in the SELECT list\"");
            } else {
                sb.append("\"Grouping by the first " + theNumGBColumns +
                          " expressions in the SELECT list\"");
            }
            sb.append(",\n");
            if (verbose) {
                formatter.indent(sb);
                sb.append("\"grouping for distinct\" : ");
                sb.append(theIsGroupingForDistinct);
                sb.append(",\n");
            }
        }

        formatter.indent(sb);
        sb.append("\"SELECT expressions\" : [\n");

        formatter.incIndent();
        for (int i = 0; i < theColumnIters.length; ++i) {
            formatter.indent(sb);
            sb.append("{\n");
            formatter.incIndent();
            formatter.indent(sb);
            sb.append("\"field name\" : \"");
            sb.append(theColumnNames[i]);
            sb.append("\",\n");
            formatter.indent(sb);
            sb.append("\"field expression\" : \n");
            theColumnIters[i].display(sb, formatter, verbose);
            formatter.decIndent();
            sb.append("\n");
            formatter.indent(sb);
            sb.append("}");
            if (i < theColumnIters.length - 1) {
                sb.append(",\n");
            }
        }
        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("]");

        if (verbose) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"convert EMPTY to NULL\" : ");
            sb.append(theDoNullOnEmpty);
        }

        if (theOffsetIter != null) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"OFFSET\" :\n");
            theOffsetIter.display(sb, formatter, verbose);
        }

        if (theLimitIter != null) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"LIMIT\" :\n");
            theLimitIter.display(sb, formatter, verbose);
        }
    }

    void traceCurrentGroup(RuntimeControlBlock rcb, SFWIterState state) {

        for (int i = 0; i < theNumGBColumns; ++i) {
            rcb.trace("Val " + i + " = " + state.theGBTuple[i]);
        }

        for (int i = theNumGBColumns; i < theColumnIters.length; ++i) {
            rcb.trace("Val " + i + " = " + theColumnIters[i].getAggrValue(rcb, false));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof SFWIter)) {
            return false;
        }
        final SFWIter other = (SFWIter) obj;
        return Arrays.equals(theFromIters, other.theFromIters) &&
            Arrays.deepEquals(theFromVarNames, other.theFromVarNames) &&
            Objects.equals(theWhereIter, other.theWhereIter) &&
            Arrays.equals(theColumnIters, other.theColumnIters) &&
            Arrays.equals(theColumnNames, other.theColumnNames) &&
            (theNumGBColumns == other.theNumGBColumns) &&
            (theIsGroupingForDistinct == other.theIsGroupingForDistinct) &&
            (theDoNullOnEmpty == other.theDoNullOnEmpty) &&
            Arrays.equals(theTupleRegs, other.theTupleRegs) &&
            Objects.equals(theTypeDefinition, other.theTypeDefinition) &&
            Objects.equals(theOffsetIter, other.theOffsetIter) &&
            Objects.equals(theLimitIter, other.theLimitIter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theFromIters,
                            Arrays.deepHashCode(theFromVarNames),
                            theWhereIter,
                            theColumnIters,
                            theColumnNames,
                            theNumGBColumns,
                            theIsGroupingForDistinct,
                            theDoNullOnEmpty,
                            theTupleRegs,
                            theTypeDefinition,
                            theOffsetIter,
                            theLimitIter);
    }
}
