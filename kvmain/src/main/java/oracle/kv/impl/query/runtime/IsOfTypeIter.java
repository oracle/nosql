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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.table.FieldDef;

/**
 * The main purpose of the IsOfTypeIter is to do type checking on the
 * expression.
 * Its semantics are as follows:
 *
 * a. If NOT is specified the result is negated.
 *
 * b. First quantifiers are checked. If input has:
 *   - 0 items: only ?, * quantifiers are valid
 *   - 1 item: all ONE, ?, *, + quantifiers are valid
 *   - 2 items: only *, + quantifiers are valid
 *
 * c. Check the input value with each of the types in the list and their
 * subtypes. If ONLY flag is specified subtypes are not checked.
 *
 * d. Given expr: $a IS OF TYPE ( t1, t2, ...) it is equivalent to:
 *      $a IS OF TYPE ( t1 ) OR $a IS OF TYPE (t2) OR ...
 *    And given $a as ( a1, a2, ...), $a IS OF TYPE (t) is equivalent with:
 *      a1 IS OF TYPE (t)  AND  a2 IS OF TYPE(t) AND ...
 *    Note: this applies to SQL NULL values too, because AND and OR
 *    expressions handle NULL values.
 *
 * Inputs:
 *   - one input value of any type.
 *
 * Result:
 *   a boolean value.
 */
public class IsOfTypeIter extends PlanIter {

    private static class IsOfTypeIterState extends PlanIterState {

        boolean[] theValidTypes;

        IsOfTypeIterState(IsOfTypeIter iter) {
            theValidTypes = new boolean[iter.theTargetTypes.size()];
            for (int i = 0; i < theValidTypes.length; i++) {
                theValidTypes[i] = true;
            }
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            for (int i = 0; i < theValidTypes.length; i++) {
                theValidTypes[i] = true;
            }
        }
    }

    private final PlanIter theInputIter;

    private final boolean theNotFlag;

    private final List<FieldDef> theTargetTypes;

    private final List<ExprType.Quantifier> theTargetQuantifiers;

    private final List<Boolean> theOnlyTargetFlags;

    public IsOfTypeIter(
        Expr e,
        int resultReg,
        PlanIter inputIter,
        boolean notFlag,
        List<FieldDef> targetTypes,
        List<ExprType.Quantifier> targetQuantifiers,
        List<Boolean> onlyTargetFlags) {

        super(e, resultReg);
        theInputIter = inputIter;
        assert targetTypes != null && targetQuantifiers != null &&
            onlyTargetFlags != null &&
            targetTypes.size() == targetQuantifiers.size() &&
            targetTypes.size() == onlyTargetFlags.size();
        theNotFlag = notFlag;
        theTargetTypes = targetTypes;
        theTargetQuantifiers = targetQuantifiers;
        theOnlyTargetFlags = onlyTargetFlags;
    }

    /**
     * FastExternalizable constructor.
     */
    IsOfTypeIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInputIter = deserializeIter(in, serialVersion);
        theNotFlag = in.readBoolean();
        int size = readPositiveInt(in);
        theTargetTypes = new ArrayList<FieldDef>(size);
        theTargetQuantifiers = new ArrayList<ExprType.Quantifier>(size);
        theOnlyTargetFlags = new ArrayList<Boolean>(size);
        for (int i = 0; i < size; i++) {
            theTargetTypes.add(deserializeFieldDef(in, serialVersion));
            theTargetQuantifiers.add(deserializeQuantifier(in, serialVersion));
            theOnlyTargetFlags.add(in.readBoolean());
        }
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
        out.writeBoolean(theNotFlag);
        int size = theTargetTypes.size();
        out.writeInt(size);
        for(int i = 0; i < size; i++) {
            serializeFieldDef(theTargetTypes.get(i), out, serialVersion);
            serializeQuantifier(theTargetQuantifiers.get(i), out, serialVersion);
            out.writeBoolean(theOnlyTargetFlags.get(i));
        }
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.IS_OF_TYPE;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new IsOfTypeIterState(this));
        theInputIter.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        IsOfTypeIterState state = (IsOfTypeIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        int targetTypesSize = theTargetTypes.size();

        int inputSize = 0;
        boolean seenNull = false;
        int validTypesCounter = targetTypesSize;

        while (true) {
            boolean hasNext = theInputIter.next(rcb);

            if (!hasNext) {
                // no items remaining in the input

                if (inputSize == 0) {
                    boolean result = false;
                    for (int i = 0; i < targetTypesSize; i++) {
                        ExprType.Quantifier quantifier =
                            theTargetQuantifiers.get(i);

                        if (quantifier == ExprType.Quantifier.QSTN ||
                            quantifier == ExprType.Quantifier.STAR) {

                            result = true;
                            break;
                        }
                    }

                    FieldValueImpl retValue =
                        BooleanValueImpl.create(theNotFlag ? !result : result);
                    rcb.setRegVal(theResultReg, retValue);

                    state.done();
                    return true;
                }

                if (seenNull && validTypesCounter > 0) {
                    FieldValueImpl retValue = NullValueImpl.getInstance();
                    rcb.setRegVal(theResultReg, retValue);
                    state.done();
                    return true;
                }

                boolean result = (validTypesCounter > 0);

                FieldValueImpl retValue =
                    BooleanValueImpl.create(theNotFlag ? !result : result);
                rcb.setRegVal(theResultReg, retValue);
                state.done();
                return true;
            }

            inputSize++;

            int inputReg = theInputIter.getResultReg();
            FieldValueImpl inValue = rcb.getRegVal(inputReg);

            if (inValue.isNull()) {
                seenNull = true;
                continue;
            }

            FieldDefImpl valDef = inValue.getDefinition();

            for (int i = 0; i < targetTypesSize; i++) {

                if (!state.theValidTypes[i]) {
                    continue;
                }

                ExprType.Quantifier quantifier = theTargetQuantifiers.get(i);

                if (inputSize >= 2 &&
                    (quantifier == ExprType.Quantifier.QSTN ||
                     quantifier == ExprType.Quantifier.ONE)) {

                    state.theValidTypes[i] = false;
                    validTypesCounter--;

                    continue;
                }

                FieldDef targetType = theTargetTypes.get(i);
                boolean onlyFlag = theOnlyTargetFlags.get(i);
                boolean isOfType;

                if (onlyFlag) {
                    isOfType = targetType.equals(valDef);
                } else {
                    isOfType = valDef.isSubtype((FieldDefImpl) targetType);
                }

                if (!isOfType) {
                    state.theValidTypes[i] = false;
                    validTypesCounter--;
                }
            }

            if (validTypesCounter == 0) {
                boolean result = false;
                FieldValueImpl retValue =
                    BooleanValueImpl
                    .create(theNotFlag ? !result : result);
                rcb.setRegVal(theResultReg, retValue);
                state.done();
                return true;
            }
        }
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
    void displayName(StringBuilder sb) {
        sb.append("IS_");
        if (theNotFlag) {
            sb.append("NOT_");
        }
        sb.append("OF_TYPE");
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"target types\" : [\n");
        formatter.incIndent();

        for(int i = 0; i < theTargetTypes.size(); i++) {

            FieldDefImpl type = (FieldDefImpl)theTargetTypes.get(i);

            formatter.indent(sb);
            sb.append("{\n");
            formatter.indent(sb);
            sb.append("\"type\" : ");
            type.displayAsJson(sb, formatter, verbose);
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"quantifier\" : \"");
            sb.append(theTargetQuantifiers.get(i)).append("\",\n");
            formatter.indent(sb);
            sb.append("\"only\" : ");
            sb.append(theOnlyTargetFlags.get(i));
            sb.append("\n");
            formatter.indent(sb);
            sb.append("}");
            if (i < theTargetTypes.size() - 1) {
                sb.append(",\n");
            }
        }

        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("],\n");

        displayInputIter(sb, formatter, verbose, theInputIter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof IsOfTypeIter)) {
            return false;
        }
        final IsOfTypeIter other = (IsOfTypeIter) obj;
        return Objects.equals(theInputIter, other.theInputIter) &&
            (theNotFlag == other.theNotFlag) &&
            Objects.equals(theTargetTypes, other.theTargetTypes) &&
            Objects.equals(theTargetQuantifiers, other.theTargetQuantifiers) &&
            Objects.equals(theOnlyTargetFlags, other.theOnlyTargetFlags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theInputIter,
                            theNotFlag,
                            theTargetTypes,
                            theTargetQuantifiers,
                            theOnlyTargetFlags);
    }
}
