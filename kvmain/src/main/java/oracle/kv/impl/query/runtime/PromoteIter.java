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
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

/**
 * The main purpose of the PromoteIter is to do type checking and type
 * promotion on the arguments of function-call expressions (ExprFnCall).
 * Its semantics are as follows:
 *
 * a. Check that the cardinality of the input set conforms with the quantifier
 *    of the target type.
 *
 * b. Check that each value on the input set is a subtype of the target type,
 *    or is promotable to the target type. If so, the promotion is performed
 *    via casting. The following promotions are allowed:
 *    - Integer to Float or Double
 *    - Long to Float or Double
 *
 * c. Raise an error if either of the above checks fail.
 *
 * d. Pass on to the parent expression each input value (or the corresponding
 *    promoted value) that passes the type checks.
 *
 * Inputs:
 *   Zero or more values of any type.
 *
 * Result:
 * Either an error or th input values promoted to the target type.
 */
public class PromoteIter extends PlanIter {

    private final PlanIter theInputIter;

    private final ExprType theTargetType;

    public PromoteIter(
        Expr e,
        int resultReg,
        PlanIter inputIter,
        ExprType type) {

        super(e, resultReg);
        theInputIter = inputIter;
        theTargetType = type;
    }

    /**
     * FastExternalizable constructor.
     */
    PromoteIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInputIter = deserializeIter(in, serialVersion);
        theTargetType = deserializeExprType(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theInputIter, out, serialVersion);
        serializeExprType(theTargetType, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.PROMOTE;
    }

    @Override
    public int[] getTupleRegs() {
        return theInputIter.getTupleRegs();
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        theInputIter.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        Quantifier quant = theTargetType.getQuantifier();

        boolean more = theInputIter.next(rcb);

        if (!more) {
            if (quant == Quantifier.ONE || quant == Quantifier.PLUS) {
                throw new QueryException(
                    "Empty result set cannot be promoted to type " +
                    theTargetType, getLocation());
            }

            state.done();
            return false;
        }

        if (quant == Quantifier.ONE || quant == Quantifier.QSTN) {
            if (theInputIter.next(rcb)) {
                throw new QueryException(
                    "Result set with more than one item cannot be promoted " +
                    " to type " + theTargetType, getLocation());
            }

            promoteValue(rcb);
            state.done();
            return true;
        }

        promoteValue(rcb);
        return true;
    }

    private void promoteValue(RuntimeControlBlock rcb) {

        int inputReg = theInputIter.getResultReg();
        FieldValueImpl inValue = rcb.getRegVal(inputReg);
        FieldValueImpl retValue;

        if (inValue.isNull()) {
            retValue = NullValueImpl.getInstance();
        } else {
            retValue = TypeManager.promote(inValue, theTargetType);

            if (retValue == null) {
                throw new QueryException(
                    "Cannot promote item of type :\n" +
                    inValue.getDefinition() + "\nto type :\n" +
                    theTargetType, getLocation());
            }
        }

        if (rcb.getTraceLevel() >= 3) {
            rcb.trace("Promoted value\n" + inValue + "\nto value\n" + retValue);
        }

        rcb.setRegVal(theResultReg, retValue);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInputIter.reset(rcb);
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
        state.close();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"target type\" : ");
        theTargetType.getDef().displayAsJson(sb, formatter, verbose);
        sb.append(",\n");
        displayInputIter(sb, formatter, verbose, theInputIter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof PromoteIter)) {
            return false;
        }
        final PromoteIter other = (PromoteIter) obj;
        return Objects.equals(theInputIter, other.theInputIter) &&
            Objects.equals(theTargetType, other.theTargetType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theInputIter, theTargetType);
    }
}
