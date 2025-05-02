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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

public class MathIter extends PlanIter {
    private static final FieldValueImpl PI_VALUE =
            FieldDefImpl.Constants.doubleDef.createDouble(Math.PI);
    private static final FieldValueImpl E_VALUE =
            FieldDefImpl.Constants.doubleDef.createDouble(Math.E);

    /* maximum decimal places for round and trunc */
    private static final int RND_MAX_PLACES = 30;

    private final FunctionLib.FuncCode theCode;
    private final PlanIter[] theArgs;

    public MathIter(Expr e,
                    int resultReg,
                    FunctionLib.FuncCode code,
                    PlanIter[] argIters) {
        super(e, resultReg);
        theCode = checkNull("code", code);
        theArgs = argIters;

        switch (theCode) {
            case FN_MATH_E:
            case FN_MATH_PI:
            case FN_MATH_RAND:
                assert argIters.length == 0;
                break;
            case FN_MATH_ABS:
            case FN_MATH_ACOS:
            case FN_MATH_ASIN:
            case FN_MATH_ATAN:
            case FN_MATH_CEIL:
            case FN_MATH_COS:
            case FN_MATH_COT:
            case FN_MATH_DEGREES:
            case FN_MATH_EXP:
            case FN_MATH_FLOOR:
            case FN_MATH_LN:
            case FN_MATH_LOG10:
            case FN_MATH_RADIANS:
            case FN_MATH_SIGN:
            case FN_MATH_SIN:
            case FN_MATH_SQRT:
            case FN_MATH_TAN:
                assert argIters.length == 1;
                break;
            case FN_MATH_ATAN2:
            case FN_MATH_LOG:
            case FN_MATH_POWER:
                assert argIters.length == 2;
                break;
            case FN_MATH_ROUND:
            case FN_MATH_TRUNC:
                assert argIters.length >= 1 && argIters.length <= 2;
                break;
            default:
                break;
        }
    }

    /**
     * FastExternalizable constructor.
     */

    MathIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, FunctionLib.FuncCode.VALUES_COUNT);
        theCode = FunctionLib.FuncCode.valueOf(ordinal);
        theArgs = deserializeIters(in, serialVersion);
    }


    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theCode.ordinal());
        serializeIters(theArgs, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_MATH;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        for (PlanIter arg : theArgs) {
            arg.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }
        FieldValueImpl res;
        switch (theCode) {
            case FN_MATH_E: {
                res = E_VALUE;
                break;
            }
            case FN_MATH_PI: {
                res = PI_VALUE;
                break;
            }
            case FN_MATH_RAND: {
                res = FieldDefImpl.Constants.doubleDef.createDouble(
                        ThreadLocalRandom.current().nextDouble());
                break;
            }
            case FN_MATH_ABS: {
                PlanIter argIter = theArgs[0];
                boolean opNext = argIter.next(rcb);
                if (!opNext) {
                    state.done();
                    return false;
                }
                FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());
                assert (argValue != null);
                res = getAbsOfValue(argValue);
                break;
            }
            case FN_MATH_CEIL: {
                PlanIter argIter = theArgs[0];
                boolean opNext = argIter.next(rcb);
                if (!opNext) {
                    state.done();
                    return false;
                }
                FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());
                assert (argValue != null);
                res = getCeilOfValue(argValue);
                break;
            }
            case FN_MATH_FLOOR: {
                PlanIter argIter = theArgs[0];
                boolean opNext = argIter.next(rcb);
                if (!opNext) {
                    state.done();
                    return false;
                }
                FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());
                assert (argValue != null);
                res = getFloorOfValue(argValue);
                break;
            }
            case FN_MATH_ACOS:
            case FN_MATH_ASIN:
            case FN_MATH_ATAN:
            case FN_MATH_COS:
            case FN_MATH_COT:
            case FN_MATH_DEGREES:
            case FN_MATH_EXP:
            case FN_MATH_LN:
            case FN_MATH_LOG10:
            case FN_MATH_RADIANS:
            case FN_MATH_SIGN:
            case FN_MATH_SIN:
            case FN_MATH_SQRT:
            case FN_MATH_TAN: {
                PlanIter argIter = theArgs[0];
                boolean opNext = argIter.next(rcb);
                if (!opNext) {
                    state.done();
                    return false;
                }
                FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());
                assert (argValue != null);
                res = getValueSingleOperand(theCode, argValue);
                break;
            }
            case FN_MATH_ATAN2: {
                PlanIter argIterLeft = theArgs[0];
                PlanIter argIterRight = theArgs[1];
                boolean opLeft = argIterLeft.next(rcb);
                boolean opRight = argIterRight.next(rcb);
                if (!opLeft || !opRight) {
                    state.done();
                    return false;
                }
                FieldValueImpl argValueLeft =
                        rcb.getRegVal(argIterLeft.getResultReg());
                FieldValueImpl argValueRight =
                        rcb.getRegVal(argIterRight.getResultReg());
                assert (argValueLeft != null && argValueRight != null);
                res = getTan2Value(argValueLeft, argValueRight);
                break;
            }
            case FN_MATH_LOG: {
                PlanIter argIterLeft = theArgs[0];
                PlanIter argIterRight = theArgs[1];
                boolean opLeft = argIterLeft.next(rcb);
                boolean opRight = argIterRight.next(rcb);
                if (!opLeft || !opRight) {
                    state.done();
                    return false;
                }
                FieldValueImpl argValueLeft =
                        rcb.getRegVal(argIterLeft.getResultReg());
                FieldValueImpl argValueRight =
                        rcb.getRegVal(argIterRight.getResultReg());
                assert (argValueLeft != null && argValueRight != null);
                res = getLogValue(argValueLeft, argValueRight);
                break;
            }
            case FN_MATH_POWER: {
                PlanIter argIterLeft = theArgs[0];
                PlanIter argIterRight = theArgs[1];
                boolean opLeft = argIterLeft.next(rcb);
                boolean opRight = argIterRight.next(rcb);
                if (!opLeft || !opRight) {
                    state.done();
                    return false;

                }
                FieldValueImpl argValueLeft =
                        rcb.getRegVal(argIterLeft.getResultReg());
                FieldValueImpl argValueRight =
                        rcb.getRegVal(argIterRight.getResultReg());
                assert (argValueLeft != null && argValueRight != null);
                res = getPowerValue(argValueLeft, argValueRight);
                break;
            }
            case FN_MATH_ROUND: {
                FieldValueImpl argLeft;
                FieldValueImpl argRight;

                if (theArgs.length == 2) {
                    PlanIter argIterLeft = theArgs[0];
                    PlanIter argIterRight = theArgs[1];

                    boolean opLeft = argIterLeft.next(rcb);
                    boolean opRight = argIterRight.next(rcb);
                    if (!opLeft || !opRight) {
                        state.done();
                        return false;
                    }
                    argLeft = rcb.getRegVal(argIterLeft.getResultReg());
                    argRight = rcb.getRegVal(argIterRight.getResultReg());
                    assert (argLeft != null && argRight != null);
                } else {
                    PlanIter argIterLeft = theArgs[0];
                    boolean opLeft = argIterLeft.next(rcb);
                    if (!opLeft) {
                        state.done();
                        return false;
                    }
                    argLeft = rcb.getRegVal(argIterLeft.getResultReg());
                    argRight = FieldDefImpl.Constants.doubleDef.createDouble(0);
                }
                res = getRoundValue(argLeft, argRight);
                break;
            }
            case FN_MATH_TRUNC: {
                FieldValueImpl argLeft;
                FieldValueImpl argRight;

                if (theArgs.length == 2) {
                    PlanIter argIterLeft = theArgs[0];
                    PlanIter argIterRight = theArgs[1];

                    boolean opLeft = argIterLeft.next(rcb);
                    boolean opRight = argIterRight.next(rcb);
                    if (!opLeft || !opRight) {
                        state.done();
                        return false;
                    }
                    argLeft = rcb.getRegVal(argIterLeft.getResultReg());
                    argRight = rcb.getRegVal(argIterRight.getResultReg());
                    assert (argLeft != null && argRight != null);
                } else {
                    PlanIter argIterLeft = theArgs[0];
                    boolean opLeft = argIterLeft.next(rcb);
                    if (!opLeft) {
                        state.done();
                        return false;
                    }
                    argLeft = rcb.getRegVal(argIterLeft.getResultReg());
                    argRight = FieldDefImpl.Constants.doubleDef.createDouble(0);
                }
                res = getTruncValue(argLeft, argRight);
                break;
            }
            default: {
                res = NullValueImpl.getInstance();
                break;
            }
        }
        rcb.setRegVal(theResultReg, res);
        state.done();
        return true;
    }

    private FieldValueImpl getValueSingleOperand(FunctionLib.FuncCode code,
                                                 FieldValueImpl argValue) {
        if (!argValue.isNumeric()) {
            return NullValueImpl.getInstance();
        }
        switch (code) {
            case FN_MATH_ACOS: {
                double dRes = Math.acos(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_ATAN: {
                double dRes = Math.atan(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_ASIN: {
                double dRes = Math.asin(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_COS: {
                double dRes = Math.cos(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_COT: {
                double dRes = 1.0d / Math.tan(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_DEGREES: {
                double dRes = Math.toDegrees(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_EXP: {
                double dRes = Math.exp(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_LN: {
                double dRes = Math.log(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_LOG10: {
                double dRes = Math.log10(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_RADIANS: {
                double dRes = Math.toRadians(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_ROUND: {
                long lRes = Math.round(argValue.castAsDouble());
                return FieldDefImpl.Constants.longDef.createLong(lRes);
            }
            case FN_MATH_SIGN: {
                double dRes = Math.signum(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_SIN: {
                double dRes = Math.sin(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_SQRT: {
                double dRes = Math.sqrt(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            case FN_MATH_TAN: {
                double dRes = Math.tan(argValue.castAsDouble());
                return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            }
            default:
                // should never hit
                assert (false);
        }
        return NullValueImpl.getInstance();
    }

    private FieldValueImpl getAbsOfValue(FieldValueImpl argValue) {
        if (!argValue.isNumeric()) {
            return NullValueImpl.getInstance();
        }
        FieldValueImpl res;
        /*
         * For Integer and Long MIN_VALUES Math.abs returns the same value
         */
        switch (argValue.getType()) {
            case INTEGER:
                int iRes = Math.abs(argValue.getInt());
                res = FieldDefImpl.Constants.integerDef.createInteger(iRes);
                break;
            case LONG:
                long lRes = Math.abs(argValue.getLong());
                res = FieldDefImpl.Constants.longDef.createLong(lRes);
                break;
            case FLOAT:
                float fRes = Math.abs(argValue.getFloat());
                res = FieldDefImpl.Constants.floatDef.createFloat(fRes);
                break;
            case DOUBLE:
                double dRes = Math.abs(argValue.getDouble());
                res = FieldDefImpl.Constants.doubleDef.createDouble(dRes);
                break;
            case NUMBER:
                BigDecimal nRes = argValue.getDecimal().abs();
                res = FieldDefImpl.Constants.numberDef.createNumber(nRes);
                break;
            default:
                res = NullValueImpl.getInstance();
                break;
        }
        return res;
    }

    private FieldValueImpl getCeilOfValue(FieldValueImpl argValue) {
        if (!argValue.isNumeric()) {
            return NullValueImpl.getInstance();
        }
        FieldValueImpl res;
        switch (argValue.getType()) {
            case INTEGER:
                int iRes = argValue.getInt();
                res = FieldDefImpl.Constants.integerDef.createInteger(iRes);
                break;
            case LONG:
                long lRes = argValue.getLong();
                res = FieldDefImpl.Constants.longDef.createLong(lRes);
                break;
            case FLOAT:
                double fRes = Math.ceil(argValue.castAsDouble());
                res = FieldDefImpl.Constants.floatDef.createFloat((float) fRes);
                break;
            case DOUBLE:
                double dRes = Math.ceil(argValue.castAsDouble());
                res = FieldDefImpl.Constants.doubleDef.createDouble(dRes);
                break;
            case NUMBER:
                BigDecimal nRes = argValue.getDecimal().setScale(0,
                        RoundingMode.CEILING);
                res = FieldDefImpl.Constants.numberDef.createNumber(nRes);
                break;
            default:
                res = NullValueImpl.getInstance();
                break;
        }
        return res;
    }

    private FieldValueImpl getFloorOfValue(FieldValueImpl argValue) {
        if (!argValue.isNumeric()) {
            return NullValueImpl.getInstance();
        }
        FieldValueImpl res;
        switch (argValue.getType()) {
            case INTEGER:
                int iRes = argValue.getInt();
                res = FieldDefImpl.Constants.integerDef.createInteger(iRes);
                break;
            case LONG:
                long lRes = argValue.getLong();
                res = FieldDefImpl.Constants.longDef.createLong(lRes);
                break;
            case FLOAT:
                double fRes = Math.floor(argValue.castAsDouble());
                res = FieldDefImpl.Constants.floatDef.createFloat((float) fRes);
                break;
            case DOUBLE:
                double dRes = Math.floor(argValue.castAsDouble());
                res = FieldDefImpl.Constants.doubleDef.createDouble(dRes);
                break;
            case NUMBER:
                BigDecimal nRes = argValue.getDecimal().setScale(0,
                        RoundingMode.FLOOR);
                res = FieldDefImpl.Constants.numberDef.createNumber(nRes);
                break;
            default:
                res = NullValueImpl.getInstance();
                break;
        }
        return res;
    }

    private FieldValueImpl getTan2Value(FieldValueImpl argLeft,
                                        FieldValueImpl argRight) {
        if (!argLeft.isNumeric() || !argRight.isNumeric()) {
            return NullValueImpl.getInstance();
        }

        double dRes = Math.atan2(argLeft.castAsDouble(),
                argRight.castAsDouble());
        return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
    }

    private FieldValueImpl getPowerValue(FieldValueImpl base,
                                         FieldValueImpl exponent) {
        if (!base.isNumeric() || !exponent.isNumeric()) {
            return NullValueImpl.getInstance();
        }
        double dRes = Math.pow(base.castAsDouble(),
                exponent.castAsDouble());
        return FieldDefImpl.Constants.doubleDef.createDouble(dRes);
    }

    private FieldValueImpl getRoundValue(FieldValueImpl number,
                                         FieldValueImpl places) {
        if (!number.isNumeric() || !places.isNumeric()) {
            return NullValueImpl.getInstance();
        }
        double n = number.castAsDouble();
        double p = places.castAsDouble();
        if (n == 0) {
            return FieldDefImpl.Constants.doubleDef.createDouble(0);
        }
        /* If n or p is not finite return n */
        if (!Double.isFinite(n) || !Double.isFinite(p)) {
            return FieldDefImpl.Constants.doubleDef.createDouble(n);
        }

        /* cast places to integer and trunc its value to +/- 30 */
        int d = (int) p;
        d = (d < 0) ? Math.max(-RND_MAX_PLACES, d) : Math.min(RND_MAX_PLACES, d);

        double multiplier = Math.pow(10, d);

        double val = (n<0) ?
                (-Math.floor(-n * multiplier + 0.5) / multiplier) :
                (Math.floor( n * multiplier + 0.5) / multiplier);

        if(val == -0.0d) {
            val = 0;
        }
        return FieldDefImpl.Constants.doubleDef.createDouble(val);
    }

    private FieldValueImpl getTruncValue(FieldValueImpl number,
                                         FieldValueImpl places) {
        if (!number.isNumeric() || !places.isNumeric()) {
            return NullValueImpl.getInstance();
        }
        double n = number.castAsDouble();
        double p = places.castAsDouble();
        if (n == 0) {
            return FieldDefImpl.Constants.doubleDef.createDouble(0);
        }
        /* If n or p is not finite return n */
        if (!Double.isFinite(n) || !Double.isFinite(p)) {
            return FieldDefImpl.Constants.doubleDef.createDouble(n);
        }

        /* cast places to integer and trunc its value to +/- 30 */
        int d = places.castAsInt();
        d = (d < 0) ? Math.max(-RND_MAX_PLACES, d) : Math.min(RND_MAX_PLACES, d);

        double multiplier = Math.pow(10, d);
        double val = (n > 0) ? (Math.floor(n * multiplier) / multiplier) :
                (Math.ceil(n * multiplier) / multiplier);
        if(val == -0.0d) {
            val = 0;
        }
        return FieldDefImpl.Constants.doubleDef.createDouble(val);
    }

    private FieldValueImpl getLogValue(FieldValueImpl number,
                                         FieldValueImpl base) {
        if (!number.isNumeric() || !base.isNumeric()) {
            return NullValueImpl.getInstance();
        }
        double val = Math.log(number.castAsDouble())/Math.log(base.castAsDouble());
        return FieldDefImpl.Constants.doubleDef.createDouble(val);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        for (PlanIter arg : theArgs) {
            arg.reset(rcb);
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
        for (PlanIter arg : theArgs) {
            arg.close(rcb);
        }
        state.close();
    }

    @Override
    protected void displayContent(
            StringBuilder sb,
            DisplayFormatter formatter,
            boolean verbose) {

        displayInputIters(sb, formatter, verbose, theArgs);
    }

    @Override
    void displayName(StringBuilder sb) {
        sb.append(theCode.name().replace("FN_MATH_", ""));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MathIter mathsIter = (MathIter) o;
        return theCode == mathsIter.theCode && Arrays.equals(theArgs,
                mathsIter.theArgs);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), theCode);
        result = 31 * result + Arrays.hashCode(theArgs);
        return result;
    }
}
