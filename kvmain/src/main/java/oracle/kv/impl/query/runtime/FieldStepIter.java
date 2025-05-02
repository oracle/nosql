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
import java.util.Map;
import java.util.Objects;
import java.util.Stack;

import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.EmptyValueImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.JsonCollectionRowImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;

import com.fasterxml.jackson.core.io.CharTypes;

/**
 *
 */
public class FieldStepIter extends PlanIter {

    static private class ArrayAndPos {

        ArrayValueImpl theArray;
        int thePos;

        ArrayAndPos(ArrayValueImpl array) {
            theArray = array;
            thePos = 0;
        }
    }

    static private class FieldStepState extends PlanIterState {

        final boolean theHasTupleInput;

        Stack<ArrayAndPos> theArrays;

        FieldValueImpl theCtxItem = null;

        String theFieldName;

        int theFieldPos;

        FieldStepState(FieldStepIter iter) {
            super();
            theArrays = new Stack<ArrayAndPos>();
            theFieldName = iter.theFieldName;
            theFieldPos = iter.theFieldPos;
            theHasTupleInput = iter.theInputIter.producesTuples();
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            if (theArrays != null) {
                theArrays.clear();
            }
            theCtxItem = null;
            theFieldName = ((FieldStepIter)iter).theFieldName;
            theFieldPos = ((FieldStepIter)iter).theFieldPos;
        }

        @Override
        public void close() {
            super.close();
            theArrays = null;
            theCtxItem = null;
            theFieldName = null;
        }
    }

    private final PlanIter theInputIter;

    private final PlanIter theFieldNameIter;

    private final String theFieldName;

    private final int theFieldPos;

    private final int theCtxItemReg;

    public FieldStepIter(
        Expr e,
        int resultReg,
        PlanIter inputIter,
        PlanIter fieldNameIter,
        String fieldName,
        int fieldPos,
        int ctxItemReg,
        boolean forCloud) {

        super(e, resultReg, forCloud);
        theInputIter = inputIter;
        theFieldNameIter = fieldNameIter;
        theFieldName = fieldName;
        theFieldPos = fieldPos;
        theCtxItemReg = ctxItemReg;
    }

    /**
     * FastExternalizable constructor.
     */
    FieldStepIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);

        theCtxItemReg = in.readInt();
        theFieldPos = in.readInt();
        theInputIter = deserializeIter(in, serialVersion);
        theFieldName = SerializationUtil.readString(in, serialVersion);

        boolean fieldNameIterExists = in.readBoolean();
        if (fieldNameIterExists) {
            theFieldNameIter = deserializeIter(in, serialVersion);
        } else {
            theFieldNameIter = null;
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);

        out.writeInt(theCtxItemReg);
        out.writeInt(theFieldPos);
        serializeIter(theInputIter, out, serialVersion);
        SerializationUtil.writeString(out, serialVersion, theFieldName);

        if (theFieldNameIter != null) {
            out.writeBoolean(true);
            serializeIter(theFieldNameIter, out, serialVersion);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void writeForCloud(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        assert(theIsCloudDriverIter);
        writeForCloudCommon(out, driverVersion);
        theInputIter.writeForCloud(out, driverVersion, valWriter);
        CloudSerializer.writeString(theFieldName, out);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FIELD_STEP;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new FieldStepState(this));
        theInputIter.open(rcb);
        if (theFieldNameIter != null) {
            theFieldNameIter.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        FieldStepState state = (FieldStepState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        int inputReg = theInputIter.getResultReg();

        /*
         * theFieldPos is > 0 when the field name is known at compile time and
         * the input expr is also known at compile time to return records of
         * known type. If, in addition, the input iterator returns its results
         * as a tuple, there is nothing to do: The result has already been
         * stored in a register R by the input iter and theResultReg of this
         * iter points to R.
         */
        if (theFieldPos >= 0 && state.theHasTupleInput) {
            assert(theFieldName != null);
            assert(theInputIter.getTupleRegs() != null);

            boolean more = theInputIter.next(rcb);
            FieldValueImpl val = rcb.getRegVal(theResultReg);

            /*
             * Note: the EMPTY value may show up if "this" is being evaluated
             * within an index-fitering pred.
             */
            if (!more || val == EmptyValueImpl.getInstance()) {
                state.done();
                return false;
            }

            state.theCtxItem = rcb.getRegVal(inputReg);
            return true;
        }

        /*
         * Compute the field name expression once here, if it does not
         * depend on the ctx item and has not been computed already.
         */
        if (theFieldName == null && theCtxItemReg < 0) {

            computeFieldName(rcb, state, false);

            if (state.theFieldName == null) {
                state.done();
                return false;
            }
        }

        while (true) {

            FieldValueImpl ctxItem = null;
            Type ctxItemKind;
            FieldValueImpl result;

            /*
             * Compute the next context item; either from the input iter or
             * from the top stacked array, if any.
             * Note: the EMPTY value may show up if "this" is being evaluated
             * within an index-fitering pred.
             */
            if (state.theArrays.isEmpty()) {

                boolean more = theInputIter.next(rcb);

                ctxItem = rcb.getRegVal(inputReg);

                if (!more || ctxItem == EmptyValueImpl.getInstance()) {
                    state.done();
                    return false;
                }

                ctxItem = rcb.getRegVal(inputReg);

                if (ctxItem.isAtomic()) {
                    continue;
                }

                if (ctxItem.isNull()) {
                    rcb.setRegVal(theResultReg, ctxItem);
                    state.theCtxItem = ctxItem;
                    return true;
                }

                ctxItemKind = ctxItem.getType();

            } else {
                ArrayAndPos arrayCtx = state.theArrays.peek();
                ArrayValueImpl array = arrayCtx.theArray;

                ctxItem = array.getElement(arrayCtx.thePos);
                ctxItemKind = ctxItem.getType();

                ++arrayCtx.thePos;
                if (arrayCtx.thePos >= array.size()) {
                    state.theArrays.pop();
                }

               if (ctxItem.isAtomic()) {
                    continue;
                }
            }

            /*
             * We have a candidate ctx item. If it is an array, stack the
             * array and repeat the loop to get a real ctx item.
             */
            if (ctxItemKind == Type.ARRAY) {
                ArrayValueImpl array = (ArrayValueImpl)ctxItem;
                if (array.size() > 0) {
                    ArrayAndPos arrayCtx = new ArrayAndPos(array);
                    state.theArrays.push(arrayCtx);
                }
                continue;
            } else if (ctxItemKind != Type.RECORD && ctxItemKind != Type.MAP) {
                throw new QueryException(
                    "Input value in field step has invalid type.\n" +
                    "Expected a complex type. Actual type is:\n" +
                    ctxItem.getDefinition(), getLocation());
            }

            /*
             * We really have the ctx item now (it's not an array). Bind the $$
             * var and compute the field name again, if it depends on $$. If
             * there is no field name, repeat the loop to get the next ctx item.
             */
            state.theCtxItem = ctxItem;

            if (theCtxItemReg >= 0) {

                computeFieldName(rcb, state, true);

                if (state.theFieldName == null) {
                    continue;
                }
            }

            /*
             * Return the value of the specified field in the ctx item.
             */
            if (ctxItemKind == Type.RECORD) {

                if (ctxItem.isTuple()) {
                    TupleValue tuple = (TupleValue)ctxItem;
                    state.theFieldPos = (theFieldPos >= 0 ?
                                         theFieldPos :
                                         tuple.getFieldPos(state.theFieldName));
                    result = tuple.get(state.theFieldPos);
                } else if (ctxItem instanceof JsonCollectionRowImpl) {
                    JsonCollectionRowImpl jsonRow = (JsonCollectionRowImpl)ctxItem;
                    result = jsonRow.get(state.theFieldName);
                } else {
                    RecordValueImpl rec = (RecordValueImpl)ctxItem;
                    state.theFieldPos = (theFieldPos >= 0 ?
                                         theFieldPos :
                                         rec.getFieldPos(state.theFieldName));
                    result = rec.get(state.theFieldPos);
                }

                if (result == null) {
                    if (ctxItem instanceof JsonCollectionRowImpl) {
                        continue;
                    }
                    throw new QueryException(
                        "There is no field named " + state.theFieldName +
                        " in record\n" + ctxItem +
                        "with type\n" + ctxItem.getDefinition(), getLocation());
                }

            } else {
                /*
                 * JSON collection case. JSON collections return a map
                 * representing a row and that map includes primary key
                 * fields, which should be handled in a case-insensitive
                 * manner, which MAP does not, by default. If this is
                 * a primary key field, as indicated by theFieldPos >= 0,
                 * use a case-insensitive get on the map
                 */
                assert(ctxItemKind == Type.MAP);
                MapValueImpl map = (MapValueImpl)ctxItem;
                result = (theFieldPos < 0 ? map.get(state.theFieldName) :
                          mapGetCaseInsensitive(map, state.theFieldName));
                if (result == null) {
                    continue;
                }
            }

            rcb.setRegVal(theResultReg, result);
            return true;
        }
    }

    /*
     * This is used to implement a case-insensitive get for primary
     * key fields which are case-insensitive. This is needed in case
     * a user uses a case different from the table definition in a
     * query.
     */
    private static FieldValueImpl mapGetCaseInsensitive(MapValueImpl map,
                                                        String fieldName) {
        FieldValueImpl value = map.get(fieldName);
        if (value != null) {
            return value;
        }

        final String lname = fieldName.toLowerCase();
        /* do case insensitive search of keys */
        for (Map.Entry<String, FieldValue> entry :
                 map.getFieldsInternal().entrySet()) {
            String current = entry.getKey().toLowerCase();
            if (current.equals(lname)) {
                return (FieldValueImpl) entry.getValue();
            }
        }
        return null;
    }

    void computeFieldName(
        RuntimeControlBlock rcb,
        FieldStepState state,
        boolean reset) {

        if (reset) {
            theFieldNameIter.reset(rcb);
        }

        if (theCtxItemReg >= 0) {
            rcb.setRegVal(theCtxItemReg, state.theCtxItem);
        }

        boolean more = theFieldNameIter.next(rcb);

        if (!more) {
            state.theFieldName = null;
            return;
        }

        int nameReg = theFieldNameIter.getResultReg();
        FieldValueImpl name = rcb.getRegVal(nameReg);

        if (name.isNull()) {
            state.theFieldName = null;
        } else {
            state.theFieldName = name.getString();
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInputIter.reset(rcb);
        if (theFieldNameIter != null) {
            theFieldNameIter.reset(rcb);
        }
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInputIter.close(rcb);
        if (theFieldNameIter != null) {
            theFieldNameIter.close(rcb);
        }

        state.close();
    }

    @Override
    void getParentItemContext(RuntimeControlBlock rcb, ParentItemContext ctx) {
        FieldStepState state = (FieldStepState)rcb.getState(theStatePos);
        ctx.theParentItem = state.theCtxItem;
        ctx.theTargetPos = state.theFieldPos;
        ctx.theTargetKey = state.theFieldName;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"field name\" : ");
        if (theFieldNameIter != null) {
            sb.append("\n");
            formatter.indent(sb);
            theFieldNameIter.display(sb, formatter, verbose);
            sb.append(",\n");
        } else {
            sb.append("\"");
            CharTypes.appendQuoted(sb, theFieldName);
            sb.append("\",\n");
        }

        if (verbose && theFieldPos >= 0) {
            formatter.indent(sb);
            sb.append("\"field position\" : ").append(theFieldPos);
            sb.append(",\n");
        }

        if (verbose && theCtxItemReg >= 0) {
            formatter.indent(sb);
            sb.append("\"register for $ variable\" : ").append(theCtxItemReg);
            sb.append(",\n");
        }

        displayInputIter(sb, formatter, verbose, theInputIter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FieldStepIter)) {
            return false;
        }
        final FieldStepIter other = (FieldStepIter) obj;
        return Objects.equals(theInputIter, other.theInputIter) &&
            Objects.equals(theFieldNameIter, other.theFieldNameIter) &&
            Objects.equals(theFieldName, other.theFieldName) &&
            (theFieldPos == other.theFieldPos) &&
            (theCtxItemReg == other.theCtxItemReg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theInputIter,
                            theFieldNameIter,
                            theFieldName,
                            theFieldPos,
                            theCtxItemReg);
    }
}
