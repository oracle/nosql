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

package oracle.kv.impl.query.compiler;

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DoubleValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FloatValueImpl;
import oracle.kv.impl.api.table.IntegerValueImpl;
import oracle.kv.impl.api.table.LongValueImpl;
import oracle.kv.impl.api.table.StringValueImpl;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.CompOpIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.FieldDef.Type;

/*
 * boolean comp(any*, any*)
 *
 * Returns NULL if any operand returns NULL.
 * Returns false if any operand returns zero or more than 1 items.
 * Returns false if the items to compare are not comparable.
 */
public class FuncCompOp extends Function {

    FuncCompOp(FuncCode code, String name) {
        super(code,
              name,
              TypeManager.ANY_STAR(), /* param1 */
              TypeManager.ANY_STAR(), /* param2 */
              TypeManager.BOOLEAN_ONE()); /* RetType */
    }

    @Override
    boolean isValueComparison() {
        return true;
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall fncall) {

        return (fncall.getArg(0).mayReturnNULL() ||
                fncall.getArg(1).mayReturnNULL());
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        Location loc = fncall.getLocation();
        QueryControlBlock qcb = fncall.getQCB();
        StaticContext sctx = fncall.getSctx();
        boolean strict = qcb.strictMode();

        Expr op0 = fncall.getArg(0);
        Expr op1 = fncall.getArg(1);
        FieldDefImpl op0Def = op0.getType().getDef();
        FieldDefImpl op1Def = op1.getType().getDef();
        Type tc0 = op0Def.getType();
        Type tc1 = op1Def.getType();

        /*
         * Nothing to do if the operands have the same kind of type.
         */
        if (tc0 == tc1) {
            return fncall;
        }

        if (!TypeManager.areTypesComparable(op0.getType(), op1.getType())) {

            if (tc0 == Type.TIMESTAMP) {

                if (tc1 == Type.STRING) {
                    int prec = ((TimestampDefImpl)op0Def).getPrecision();
                    Expr cast = ExprCast.create(
                        qcb, sctx, loc, op1,
                        FieldDefImpl.Constants.timestampDefs[prec],
                        op1.getType().getQuantifier());
                    fncall.setArg(1, cast, false);
                    return fncall;
                }

                if (tc1 == Type.JSON || tc1 == Type.ANY_JSON_ATOMIC) {
                    return fncall;
                }
            }

            if (tc1 == Type.TIMESTAMP) {

                if (tc0 == Type.STRING) {
                    int prec = ((TimestampDefImpl)op1Def).getPrecision();
                    Expr cast = ExprCast.create(
                        qcb, sctx, loc, op0,
                        FieldDefImpl.Constants.timestampDefs[prec],
                        op0.getType().getQuantifier());
                    fncall.setArg(0, cast, false);
                    return fncall;
                }

                if (tc0 == Type.JSON || tc0 == Type.ANY_JSON_ATOMIC) {
                    return fncall;
                }
            }

           if (strict) {
               throw new QueryException(
                    "Incompatible types for comparison operator: \n" +
                    "Type1: " + op0.getType() + "\nType2: " + op1.getType(),
                    fncall.getLocation());
           }

           if (!op0.mayReturnNULL() && !op1.mayReturnNULL()) {
               return new ExprConst(qcb, sctx, loc, false);
           }

           return fncall;
        }

        return handleConstOperand(fncall);
    }

    /**
     * See javadoc for castConstInCompOp().
     */
    static Expr handleConstOperand(ExprFuncCall fncall) {

        Location loc = fncall.getLocation();
        QueryControlBlock qcb = fncall.getQCB();
        StaticContext sctx = fncall.getSctx();

        Function func = fncall.getFunction();
        Expr arg0 = fncall.getArg(0);
        Expr arg1 = fncall.getArg(1);
        FuncCode opCode = func.getCode();
        boolean isAnyOp = func.isAnyComparison();
        Expr varOp = null;
        ExprConst constOp = null;
        int constPos;

        /*
         * For simplicity, convert the "any" codes to their corresponding
         * value-comparison op code.
         */
        if (isAnyOp) {
            opCode = FuncAnyOp.anyToComp(opCode);
        }

        if (arg0.getKind() == ExprKind.CONST) {
            constOp = (ExprConst)arg0;
            constPos = 0;
            varOp = arg1;
            opCode = swapCompOp(opCode);
        } else if (arg1.getKind() == ExprKind.CONST) {
            constOp = (ExprConst)arg1;
            constPos = 1;
            varOp = arg0;
        } else {
            return fncall;
        }

        boolean strict = fncall.getQCB().strictMode();
        FieldDefImpl varDef = varOp.getType().getDef();
        FieldValueImpl constVal = constOp.getValue();
        boolean varNullable = varOp.mayReturnNULL();
        boolean varScalar = varOp.isScalar();

        /*
         * If it's a value comparison and the non-const operand may return
         * more than 1 items, the result of the comparison will probably be
         * an error. So, we should not attempt any rewrite that will mask the
         * error.
         */
        if (varOp.isMultiValued() && !isAnyOp) {
            return fncall;
        }

        FieldValueImpl newConstVal = castConstInCompOp(varDef,
                                                       false,
                                                       varNullable,
                                                       varScalar,
                                                       constVal,
                                                       opCode,
                                                       strict);

        if (newConstVal == constVal) {

            if (constVal.isJsonNull() &&
                (opCode == FuncCode.OP_GE || opCode == FuncCode.OP_LE)) {

                if (isAnyOp) {
                    func = CompilerAPI.getFuncLib().getFunc(FuncCode.OP_EQ_ANY);
                } else {
                    func = CompilerAPI.getFuncLib().getFunc(FuncCode.OP_EQ);
                }

                return ExprFuncCall.create(qcb, sctx, loc, func, fncall.getArgs());
            }

            return fncall;
        }

        if (newConstVal == BooleanValueImpl.falseValue) {
            return new ExprConst(qcb, sctx, loc, false);
        }

        if (newConstVal == BooleanValueImpl.trueValue) {
            return new ExprConst(qcb, sctx, loc, true);
        }

        ExprConst newConstOp = new ExprConst(qcb, sctx, loc, newConstVal);
        fncall.setArg(constPos, newConstOp, true/*destroy*/);
        return fncall;
    }

    /**
     * This method is called from handleConstOperand() above and it is also
     * called directly from IndexAnalyzer and BaseTableIter.
     *
     * The method handles the case where one of the operands is a const whose
     * type is comparable with, but not the same as the type of the other
     * operand (the "variable" operand). In this case, we may be able to either
     * (a) create a new const that has the same type as the variable operand, or
     * (b) conclude that the comparison is always false or true. Doing so is 
     * important for turning comparison preds into index search keys (because 
     * to do so requires that the two operands have exactly the same type). Even
     * if the predicate is not pushed into the index, doing (a) will save some 
     * type checking in the runtime evaluation of the operator. 
     *
     * Note: catching cases where the comparison is always false must take into
     * account the possibility that the variable operand returns one or more
     * NULLs (in which case the result of the comparison is NULL, not false).
     * This is indicated by the "nullable" param. If true, the result
     * is not always false. However, when called from IndexAnalyzer or 
     * BaseTableIter nullable is always false, because we know that the 
     * comparison is a predicate in the WHERE clause, in which case NULL is 
     * translated to false.
     *
     * Note: catching cases where the comparison is always true must take into
     * account the possibility that the variable operand returns EMPTY. If true,
     * the result of the comparison is false. So, we conclude "always true" only
     * if the variable operand is scalar. This is indicated by the "scalar"
     * param. 
     *
     * The method is called from BaseTableIter when the comp pred has been
     * pushed to an index as a start/stop condition, and the "const" operand
     * is actually an external variable. In this case, it is necessary that
     * (a) or (b) succeed. To provide this guarantee, castConstInCompOp() works
     * together with IndexAnalyzer.checkTypes(). The later will prevent an
     * index from being used if the former cannot guarantee (when called from
     * BaseTableIter) that (a) or (b) will take place. As a result, when called
     * from BaseTableIter, the "scalar" param is always true.
     *
     * If (b) is done, the method returns the false or true boolean value. 
     * If (a) is  done, it returns the result of the cast. Otherwise, it
     * returns the  original const value.
     */
    public static FieldValueImpl castConstInCompOp(
        FieldDefImpl targetType,
        boolean allowJsonNull,
        boolean nullable,
        boolean scalar,
        FieldValueImpl val,
        FuncCode opCode,
        boolean strict) {

        /*
         * EMPTY is always != to anything other than EMPTY
         */
        if (targetType.getType() == Type.EMPTY) {

            if (opCode == FuncCode.OP_NEQ) {
                return BooleanValueImpl.trueValue;
            }

            return BooleanValueImpl.falseValue;
        }

        if (val.isEMPTY()) {

            if (opCode == FuncCode.OP_LT ||
                opCode == FuncCode.OP_GT) {
                return BooleanValueImpl.falseValue;
            }

            return val;
        }

        if (val.isJsonNull()) {

            if (!nullable && opCode != FuncCode.OP_NEQ) {
                /*
                 * If the targetType does not include the json null the result
                 * is always false. The result is also always false if the 
                 * operator is <, <any, >, or >any 
                 */
               if (opCode == FuncCode.OP_LT ||
                   opCode == FuncCode.OP_GT ||
                   (!allowJsonNull &&
                    !targetType.isJson() &&
                    !targetType.isAnyJsonAtomic() &&
                    !targetType.isAny())) {
                   return BooleanValueImpl.falseValue;
               }
            }

            /*
             * If the op is != or !=any and varDef does not include jnull, the
             * result is true, EXCEPT when the op is actually !=any and the 
             * other operand returns EMPTY. For simplicity, we just don't do 
             * anything for the != and !=any ops.
             */
            return val;
        }

        if (targetType.isWildcard()) {
            return val;
        }

        if (targetType.getType() == val.getType()) {

            if (val.getType() == Type.TIMESTAMP) {
                int targetPrecision =
                    ((TimestampDefImpl)targetType).getPrecision();
                int valPrecision =
                    ((TimestampDefImpl)val.getDefinition()).getPrecision();

                if (valPrecision < targetPrecision) {
                    TimestampValueImpl res = ((TimestampValueImpl)val).
                        castToPrecision(targetPrecision);
                    return res;
                }

                if (valPrecision > targetPrecision) {

                    if (opCode == FuncCode.OP_GT || opCode == FuncCode.OP_LE) {
                        TimestampValueImpl res = ((TimestampValueImpl)val).
                            roundDownToPrecision(targetPrecision);
                        return res;
                    }

                    if (opCode == FuncCode.OP_LT || opCode == FuncCode.OP_GE) {
                        TimestampValueImpl res = ((TimestampValueImpl)val).
                            roundUpToPrecision(targetPrecision);
                        return res;
                    }

                    if (opCode == FuncCode.OP_EQ) {
                        TimestampValueImpl res = ((TimestampValueImpl)val).
                            castToPrecision(targetPrecision);
                        if (res.equals(val)) {
                            return res;
                        }
                        return BooleanValueImpl.falseValue;
                    }
                }
            }

            return val;
        }

        switch (targetType.getType()) {

        case INTEGER:
            if (val.isLong()) {

                long lng = ((LongValueImpl)val).get();

                if (lng < Integer.MIN_VALUE || lng > Integer.MAX_VALUE) {

                    if (nullable) {
                        return val;
                    }

                    /*
                     * In several cases here, the result will be always true,
                     * but because of the EMPTY complication, we just skip
                     * these cases. To correctly return always true in these
                     * cases, we have to know that the other operand is a scalar
                     * expr.
                     */
                    switch(opCode) {
                    case OP_EQ:
                        return BooleanValueImpl.falseValue;
                    case OP_NEQ:
                        if (scalar) {
                            return BooleanValueImpl.trueValue;
                        }
                        return val;
                    case OP_LT:
                        if (lng <= Integer.MIN_VALUE) {
                            return BooleanValueImpl.falseValue;
                        }
                        if (scalar) {
                            return BooleanValueImpl.trueValue;
                        }
                        return val;
                    case OP_LE:
                        if (lng < Integer.MIN_VALUE) {
                            return BooleanValueImpl.falseValue;
                        }
                        if (scalar) {
                            return BooleanValueImpl.trueValue;
                        }
                        return val;
                    case OP_GT:
                        if (lng <= Integer.MIN_VALUE) {
                            if (scalar) {
                                return BooleanValueImpl.trueValue;
                            }
                            return val;
                        }
                        return BooleanValueImpl.falseValue;
                    case OP_GE:
                        if (lng < Integer.MIN_VALUE) {
                            if (scalar) {
                                return BooleanValueImpl.trueValue;
                            }
                            return val;
                        }
                        return BooleanValueImpl.falseValue;
                    default:
                        throw new QueryStateException(
                            "Unexpected function " + opCode);
                    }
                }
             
                return FieldDefImpl.Constants.integerDef.createInteger(
                    (int)lng);
            }

            if (val.isNumeric()) {
                return val;
            }

            break;

        case LONG:
            if (val.isInteger()) {
                return (FieldDefImpl.Constants.longDef.
                        createLong(((IntegerValueImpl)val).get()));
            }

            if (val.isNumeric()) {
                return val;
            }

            break;

        case FLOAT:
            if (val.isInteger()) {
                return (FieldDefImpl.Constants.floatDef.
                        createFloat(((IntegerValueImpl)val).get()));
            }

            if (val.isLong()) {
                return (FieldDefImpl.Constants.floatDef.
                        createFloat(((LongValueImpl)val).get()));
            }

            if (val.isDouble()) {

                double dbl = ((DoubleValueImpl)val).get();

                if (dbl < -Float.MAX_VALUE || dbl > Float.MAX_VALUE) {

                    if (nullable) {
                        return val;
                    }

                    switch(opCode) {
                    case OP_EQ:
                        return BooleanValueImpl.falseValue;
                    case OP_NEQ:
                        return val;
                    case OP_LT:
                    case OP_LE:
                        if (dbl < Float.MIN_VALUE) {
                            return BooleanValueImpl.falseValue;
                        }
                        return val;
                    case OP_GT:
                    case OP_GE:
                        if (dbl < Float.MIN_VALUE) {
                            return val;
                        }
                        return BooleanValueImpl.falseValue;
                    default:
                        throw new QueryStateException(
                            "Unexpected function " + opCode);
                    }
                }

                return FieldDefImpl.Constants.floatDef.createFloat(
                    (float)dbl);
            }
            
            if (val.isNumeric()) {
                return val;
            }

            break;

        case DOUBLE:
            if (val.isInteger()) {
                return (FieldDefImpl.Constants.doubleDef.
                        createDouble(((IntegerValueImpl)val).get()));
            }
            if (val.isLong()) {
                return (FieldDefImpl.Constants.doubleDef.
                        createDouble(((LongValueImpl)val).get()));
            }
            if (val.isFloat()) {
                return (FieldDefImpl.Constants.doubleDef.
                        createDouble(((FloatValueImpl)val).get()));
            }

            if (val.isNumeric()) {
                return val;
            }

            break;

        case NUMBER:
            if (val.isInteger()) {
                return (FieldDefImpl.Constants.numberDef.
                        createNumber(((IntegerValueImpl)val).get()));
            }
            if (val.isLong()) {
                return (FieldDefImpl.Constants.numberDef.
                        createNumber(((LongValueImpl)val).get()));
            }
            if (val.isFloat()) {
                return (FieldDefImpl.Constants.numberDef.
                        createNumber(((FloatValueImpl)val).get()));
            }
            if (val.isDouble()) {
                return (FieldDefImpl.Constants.numberDef.
                        createNumber(((DoubleValueImpl)val).get()));
            }
            break;

        case ENUM:
            if (val.isString()) {
                try {
                    return targetType.createEnum(((StringValueImpl)val).get());
                } catch (IllegalArgumentException e) {
                    if (strict) {
                        throw e;
                    }
                    return BooleanValueImpl.falseValue;
                }
            }
            break;

        default:
            break;
        }

        throw new QueryStateException(
            "Unexpected case in castConstInCompOp()\n" + 
            "Target type =\n  " + targetType.getDDLString() +
            "\nValue =\n  " + val + "\nValue type = " + val.getType() +
            "\nComparison op = " + opCode);
    }

    static FuncCode swapCompOp(FuncCode op) {

        switch (op) {
        case OP_GT:
            return FuncCode.OP_LT;
        case OP_GE:
            return FuncCode.OP_LE;
        case OP_LT:
            return FuncCode.OP_GT;
        case OP_LE:
            return FuncCode.OP_GE;
        default:
            return op;
        }
    }

    static FuncCode getNegationOp(FuncCode op) {

        switch (op) {
        case OP_GT:
            return FuncCode.OP_LE;
        case OP_GE:
            return FuncCode.OP_LT;
        case OP_LT:
            return FuncCode.OP_GE;
        case OP_LE:
            return FuncCode.OP_GT;
        case OP_EQ:
            return FuncCode.OP_NEQ;
        case OP_NEQ:
            return FuncCode.OP_EQ;
        default:
            throw new QueryStateException("Unexpected function " + op);
        }
    }

    public static String printOp(FuncCode op) {

        switch (op) {
        case OP_GT:
            return ">";
        case OP_GE:
            return ">=";
        case OP_LT:
            return "<";
        case OP_LE:
            return "<=";
        case OP_EQ:
            return "=";
        case OP_NEQ:
            return "!=";
        default:
            throw new QueryStateException("Unexpected function " + op);
        }
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall fncall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(fncall);

        return new CompOpIter(fncall, resultReg, theCode, argIters);
    }
}
