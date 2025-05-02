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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FuncAnyOp;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.CompOpIter.CompResult;

/**
 * Iterator to implement the "any" comparison operators {@literal (=any, !=any,
 * <any, <=any, >any, and >=any)}. These operators have existential semantics:
 *
 * boolean AnyOp(any*, any*)
 *
 * Note: This implementation assumes no knowledge of the types of the items in
 * the 2 sequences. Each sequence may contain items of any type, including items
 * of different types. As a result, the implementation is not very efficient
 * when both sequences have more than one items, because it simply performs a
 * nested loop over the 2 sequences. Better implementations are possible when
 * we have type info. For example, if all the items in both sequences are of
 * the same type, then {@literal <any, <=any, >any, and >=any} can be
 * implemented by computing the min and max of each sequence and comparing
 * these min/max values. TODO: implement "specialized" versions of AnyOpIter
 * that make use of the static types of the input sequences.
 */
public class AnyOpIter extends PlanIter {

    static private class AnyOpIterState extends PlanIterState {

        final CompResult theResult = new CompResult();

        boolean theHaveNull;

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theResult.clear();
            theHaveNull = false;
        }
    }

    private static final int theChunkSize = 10;

    private final FuncCode theAnyCode;

    private final FuncCode theCompCode;

    private final PlanIter theLeftOp;

    private final PlanIter theRightOp;

    public AnyOpIter(
        Expr e,
        int resultReg,
        FuncCode code,
        PlanIter[] argIters) {

        super(e, resultReg);
        theAnyCode = checkNull("code", code);
        theCompCode = FuncAnyOp.anyToComp(code);
        assert(argIters.length == 2);
        theLeftOp = argIters[0];
        theRightOp = argIters[1];
    }

    /**
     * TupleSerialization constructor.
     */
    AnyOpIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);

        short ordinal = readOrdinal(in, FuncCode.VALUES_COUNT);
        theAnyCode = FuncCode.valueOf(ordinal);
        theCompCode = FuncAnyOp.anyToComp(theAnyCode);
        theLeftOp = deserializeIter(in, serialVersion);
        theRightOp = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theAnyCode.ordinal());
        serializeIter(theLeftOp, out, serialVersion);
        serializeIter(theRightOp, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ANY_COMPARE;
    }

    @Override
    FuncCode getFuncCode() {
        return theAnyCode;
    }


    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new AnyOpIterState());
        theLeftOp.open(rcb);
        theRightOp.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        AnyOpIterState state = (AnyOpIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        FieldValueImpl lv1;
        FieldValueImpl lv2;
        FieldValueImpl rv1;
        FieldValueImpl rv2;
        boolean more;

        /*
         * Optimize for the case where at least one of the input sequences
         * contains at most one item.
         */

        more = theLeftOp.next(rcb);
        if (!more) {
            done(rcb, state, false);
            return true;
        }

        more = theRightOp.next(rcb);
        if (!more) {
            done(rcb, state, false);
            return true;
        }

        /*
         * Both the left and the right side have at least 1 item.
         * Compare the 1st items from each side now.
         */
        lv1 = rcb.getRegVal(theLeftOp.getResultReg());
        rv1 = rcb.getRegVal(theRightOp.getResultReg());

        if (compare(rcb, state, lv1, rv1)) {
            done(rcb, state, true);
            return true;
        }

        if (lv1.isTuple()) {
            lv1 = ((TupleValue)lv1).toRecord();
        }

        more = theLeftOp.next(rcb);

        if (!more) {

            /* Left size has exactly one item */
            while (theRightOp.next(rcb)) {

                rv2 = rcb.getRegVal(theRightOp.getResultReg());

                if (compare(rcb, state, lv1, rv2)) {
                    done(rcb, state, true);
                    return true;
                }
            }

            done(rcb, state, false);
            return true;
        }

        /* Left size has more than one item */
        lv2 = rcb.getRegVal(theLeftOp.getResultReg());

        if (rv1.isTuple()) {
            rv1 = ((TupleValue)rv1).toRecord();
        }

        more = theRightOp.next(rcb);

        if (!more) {

            /* Right size has exactly one item */
            if (compare(rcb, state, lv2, rv1)) {
               done(rcb, state, true);
               return true;
           }

           while (theLeftOp.next(rcb)) {

                lv2 = rcb.getRegVal(theLeftOp.getResultReg());

                if (compare(rcb, state, lv2, rv1)) {
                    done(rcb, state, true);
                    return true;
                }
           }

           done(rcb, state, false);
           return true;
        }

        rv2 = rcb.getRegVal(theRightOp.getResultReg());

        /*
         * Handle the general case. Compute a chunk of items from the left
         * side, and then compare these items with all of the items from
         * the right side. Repeat until a left item compares true with a
         * right item or until there are no more left items. The right-side
         * subplan is reset at the end of each left chunk (i.e., the right-
         * side subplan is re-computed fully for each left chunk).
         *
         * TODO consider chunking the right side as well
         */

        /*
         * lv2 and rv2 have not yet been compared with each other and with
         * lv1/rv1. Do these comparisons now, before handling the reset of
         * the sequences.
         */
        if (compare(rcb, state, lv1, rv2) ||
            compare(rcb, state, lv2, rv1) ||
            compare(rcb, state, lv2, rv2)) {
            done(rcb, state, true);
            return true;
        }

        ArrayList<FieldValueImpl> lchunk =
            new ArrayList<FieldValueImpl>(theChunkSize);

        lchunk.add(lv1);
        lchunk.add(lv2);
        theRightOp.reset(rcb);

        while (true) {

            while (lchunk.size() < theChunkSize && theLeftOp.next(rcb)) {
                lv1 = rcb.getRegVal(theLeftOp.getResultReg());
                lchunk.add(lv1);
            }

            while (theRightOp.next(rcb)) {

                rv1 = rcb.getRegVal(theRightOp.getResultReg());

                for (int i = 0; i < lchunk.size(); ++i) {

                    lv1 = lchunk.get(i);

                    if (compare(rcb, state, lv1, rv1)) {
                        done(rcb, state, true);
                        return true;
                    }
                }
            }

            if (lchunk.size() == theChunkSize) {
                theRightOp.reset(rcb);
                lchunk.clear();
                continue;
            }

            break;
        }

        done(rcb, state, false);
        return true;
    }

    private void done(
        RuntimeControlBlock rcb,
        AnyOpIterState state,
        boolean result) {

        FieldValueImpl res =
            (state.theHaveNull && !result ?
             NullValueImpl.getInstance()  :
             FieldDefImpl.Constants.booleanDef.createBoolean(result));
        rcb.setRegVal(theResultReg, res);
        state.done();
    }

    private boolean compare(
        RuntimeControlBlock rcb,
        AnyOpIterState state,
        FieldValueImpl v0,
        FieldValueImpl v1) {

        state.theResult.clear();

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("anyOp: comparing\n" + v0 + "\nwith\n" + v1);
        }

        CompOpIter.compare(rcb,
                           v0,
                           v1,
                           theCompCode,
                           false, // forSort
                           state.theResult,
                           getLocation());

        if (state.theResult.haveNull) {
            state.theHaveNull = true;
            return false;
        }

        if (state.theResult.incompatible) {
            return false;
        }

        int comp = state.theResult.comp;

        switch (theCompCode) {
        case OP_EQ:
            return (comp == 0);
        case OP_NEQ:
            return (comp != 0);
        case OP_GT:
            return (comp > 0);
        case OP_GE:
            return (comp >= 0);
        case OP_LT:
            return (comp < 0);
        case OP_LE:
            return (comp <= 0);
        default:
            assert(false);
            return false;
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theLeftOp.reset(rcb);
        theRightOp.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {
        theLeftOp.close(rcb);
        theRightOp.close(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.close();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"left operand\" :\n");
        theLeftOp.display(sb, formatter, verbose);
        sb.append(",\n");
        formatter.indent(sb);
        sb.append("\"right operand\" :\n");
        theRightOp.display(sb, formatter, verbose);
    }

    @Override
    void displayName(StringBuilder sb) {

        switch (theAnyCode) {
        case OP_GT_ANY:
            sb.append("ANY_GREATER_THAN");
            break;
        case OP_GE_ANY:
            sb.append("ANY_GREATER_OR_EQUAL");
            break;
        case OP_LT_ANY:
            sb.append("ANY_LESS_THAN");
            break;
        case OP_LE_ANY:
            sb.append("ANY_LESS_OR_EQUAL");
            break;
        case OP_EQ_ANY:
            sb.append("ANY_EQUAL");
            break;
        case OP_NEQ_ANY:
            sb.append("ANY_NOT_EQUAL");
            break;
        default:
            break;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof AnyOpIter)) {
            return false;
        }
        final AnyOpIter other = (AnyOpIter) obj;
        return (theAnyCode == other.theAnyCode) &&
            (theCompCode == other.theCompCode) &&
            Objects.equals(theLeftOp, other.theLeftOp) &&
            Objects.equals(theRightOp, other.theRightOp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theAnyCode,
                            theCompCode,
                            theLeftOp,
                            theRightOp);
    }
}
