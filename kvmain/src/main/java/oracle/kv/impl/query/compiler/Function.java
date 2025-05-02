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

import java.util.ArrayList;

import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.impl.api.table.FieldDefImpl;


/*
 * Base class for representing functions.
 *
 * theCode:
 * Each builtin function has a code that serves as its unique id.
 *
 * theName:
 * The function name. For now, it must be unique among all functions that are
 * visible to a query, i.e., no function overloading (something to reconsider
 * in the future ????).
 *
 * theIsVariadic:
 * Whether the function accepts a variable number of arguments. A variadic
 * function will, in general, have a fixed number N ( >= 0) of "declared"
 * params, followed by a variable number of "undeclared" params. All the
 * undeclared params are assumed to have the same type and this type is equal
 * to the type of the last declared param, or to ANY_STAR, if there are no
 * declared params.
 *
 * theParamTypes:
 * The types of the function's parameters. If the function is variadic,
 * theParamTypes stores the types of the declared params only.
 *
 * theReturnType:
 * The declared return type of the function. Depending on the args used in an
 * actual call of this function, a more tight return type may be deducable.
 * This is done by the getReturnType() method.
 */
public abstract class Function {

    final FuncCode theCode;

    final String theName;

    final boolean theIsVariadic;

    final ArrayList<ExprType> theParamTypes;

    final ExprType theReturnType;


    Function(
        FuncCode code,
        String name,
        ExprType retType) {

        theCode = code;
        theName = name;
        theIsVariadic = false;
        theParamTypes = null;
        theReturnType = retType;
    }

    Function(
        FuncCode code,
        String name,
        ExprType paramType,
        ExprType retType) {
        this(code, name, paramType, retType, false);
    }

    Function(
        FuncCode code,
        String name,
        ExprType paramType,
        ExprType retType,
        boolean isVariadic) {

        theCode = code;
        theName = name;
        theIsVariadic = isVariadic;
        theParamTypes = new ArrayList<ExprType>(1);
        theParamTypes.add(paramType);
        theReturnType = retType;
    }

    Function(
        FuncCode code,
        String name,
        ExprType param1Type,
        ExprType param2Type,
        ExprType retType) {

        theCode = code;
        theName = name;
        theIsVariadic = false;
        theParamTypes = new ArrayList<ExprType>(2);
        theParamTypes.add(param1Type);
        theParamTypes.add(param2Type);
        theReturnType = retType;
    }

    Function(
        FuncCode code,
        String name,
        ArrayList<ExprType> paramTypes,
        ExprType retType,
        boolean isVariadic) {

        theCode = code;
        theName = name;
        theIsVariadic = isVariadic;
        theParamTypes = paramTypes;
        theReturnType = retType;
    }

    public final FuncCode getCode() {
        return theCode;
    }

    public final String getName() {
        return theName;
    }

    final int getArity() {
        return (theParamTypes == null ? 0 : theParamTypes.size());
    }

    final boolean isVariadic() {
        return theIsVariadic;
    }

    boolean isValueComparison() {
        return false;
    }

    boolean isAnyComparison() {
        return false;
    }

    final boolean isComparison() {
        return isValueComparison() || isAnyComparison();
    }

    public boolean isRowProperty() {
        return false;
    }

    boolean isDeterministic() {
        return true;
    }

    boolean isIndexable() {
        return false;
    }

    ExprType getParamType(int i) {

        if (i < theParamTypes.size()) {
            return theParamTypes.get(i);
        }

        assert(isVariadic());

        if (theParamTypes.isEmpty()) {
            return TypeManager.ANY_STAR();
        }

        return theParamTypes.get(theParamTypes.size() - 1);
    }

    @SuppressWarnings("unused")
    public ExprType getRetType(FieldDefImpl inType) {
        return theReturnType;
    }

    /*
     * This method is redefined by any function that can compute a more
     * specific type based on the argument expressions.
     */
    @SuppressWarnings("unused")
    ExprType getRetType(ExprFuncCall caller) {
        return theReturnType;
    }

    abstract boolean mayReturnNULL(ExprFuncCall caller);

    abstract boolean mayReturnEmpty(ExprFuncCall caller);

    boolean isAggregate() {
        return false;
    }

    /*
     * Redefined by subclasses that actually need to do further analysis of a
     * function call, based on the argument expressions. Such analysis may
     * determine that the function call must be replaced by another expr, in which
     * case that other expr is created and returned.
     */
    Expr normalizeCall(ExprFuncCall funcCall) {
        return funcCall;
    }

    abstract PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall funcCall,
        PlanIter[] argIters);

    static Function getFunction(FuncCode c) {
        return CompilerAPI.getFuncLib().getFunc(c);
    }
}
