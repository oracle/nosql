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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.Objects;

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FuncRegexLike;
import oracle.kv.impl.query.compiler.Expr.ConstKind;

/*
 * boolean regex_like(string source, string pattern [, string flags]
 * 
 * source input value 
 * pattern regex pattern
 * flags  is a string that lets you change the default 
 *        matching behavior of the function 
 * 
 * Returns NULL If any parameter is null.
 * Return true if pattern matches source string.
 * Returns false if pattern does not match source string or if any parameter 
 *               is not a string type or not cardinality of 1.
 * 
 * throws QueryException if pattern string is invalid. 
 */
public class FuncRegexLikeIter extends PlanIter {
    
    private final PlanIter theRegexParam;
    private final Boolean theIsRegexConst;
    private final PlanIter theSource;
    private final PlanIter theFlagsParam;
    private final Boolean theIsFlagsParamConst;
    
    static private class FuncRegexLikeState extends PlanIterState {
        Pattern thePattern;
    }

    public FuncRegexLikeIter(Expr e, int resultReg, PlanIter[] argIters) {
        super(e, resultReg);
        ExprFuncCall fncall = (ExprFuncCall)e;
        theSource = argIters[0];
        theRegexParam = argIters[1];
        theIsRegexConst = ConstKind.isConst(fncall.getArg(1));
        if (argIters.length == 3) {
            theFlagsParam = argIters[2];
            theIsFlagsParamConst = ConstKind.isConst(fncall.getArg(2));
        } else {
            theFlagsParam = null;
            theIsFlagsParamConst = true;
        }
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncRegexLikeIter(DataInput in, short serialVersion) 
        throws IOException {
        super(in, serialVersion);
        theRegexParam = deserializeIter(in, serialVersion);
        theSource = deserializeIter(in, serialVersion);
        theFlagsParam = deserializeIter(in, serialVersion);
        theIsRegexConst = in.readBoolean();
        theIsFlagsParamConst = in.readBoolean();
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theRegexParam, out, serialVersion);
        serializeIter(theSource, out, serialVersion);
        serializeIter(theFlagsParam, out, serialVersion);
        out.writeBoolean(theIsRegexConst);
        out.writeBoolean(theIsFlagsParamConst);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_REGEX_LIKE;
    }
    
    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new FuncRegexLikeState());
        theRegexParam.open(rcb);
        theSource.open(rcb);
        if (theFlagsParam != null) {
            theFlagsParam.open(rcb);
        }
    }
    
    @Override
    public void reset(RuntimeControlBlock rcb) {
        theRegexParam.reset(rcb);
        theSource.reset(rcb);
        if (theFlagsParam != null) {
            theFlagsParam.reset(rcb);
        }
        FuncRegexLikeState state = 
            (FuncRegexLikeState)rcb.getState(theStatePos);
        state.reset(this);
        if (!(theIsFlagsParamConst && theIsRegexConst)) {
            state.thePattern = null;
        }
    }
    
    @Override
    public void close(RuntimeControlBlock rcb) {
        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theRegexParam.close(rcb);
        theSource.close(rcb);
        if (theFlagsParam != null) {
            theFlagsParam.close(rcb);
        }
        state.close();
    }
    
    @Override
    public boolean next(RuntimeControlBlock rcb) {
        
        String regex = null;
        String flags = null;
        String sourceValue = null;
        FieldValueImpl fvi;
        boolean more;

        FuncRegexLikeState state =
            (FuncRegexLikeState)rcb.getState(theStatePos);
        
        if (state.isDone()) {
            return false;
        }
 
        if (state.thePattern == null) {

            more = theRegexParam.next(rcb);
            fvi = rcb.getRegVal(theRegexParam.getResultReg());

            if (fvi.isNull()) {
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }

            regex = fvi.asString().get();

            if (theFlagsParam != null) {

                more = theFlagsParam.next(rcb);
                fvi = rcb.getRegVal(theFlagsParam.getResultReg());

                if (fvi.isNull()) {
                    rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                    state.done();
                    return true;
                }

                flags = fvi.asString().get();
            }  
           
            try {
                FuncRegexLike.verifyPattern(regex);
                if (flags == null) {
                    state.thePattern = Pattern.compile(regex);
                } else {
                    state.thePattern =
                        Pattern.compile(regex, 
                                        FuncRegexLike.convertFlags(flags));
                }
            } catch (PatternSyntaxException e) {
                throw new QueryException(
                    "The pattern [" + regex +
                    "] specified for the regex_like function is invalid.",
                    e,  getLocation());
            } 
        }

        more = theSource.next(rcb);

        if (!more) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        fvi = rcb.getRegVal(theSource.getResultReg());

        /*
         * The function defines source to allow for sequences. We only
         * will run regex on sequences containing only one value.
         */
        if (theSource.next(rcb)) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        if (fvi.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        if (!fvi.isString()) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        sourceValue = fvi.asString().get();
        boolean retVal;
        try {
            retVal = state.thePattern.matcher(sourceValue).matches();
        } catch (Throwable t) {
            throw new QueryException("An error was encountered during " +
                                     "regex_like processing. Cause: " +
                                     t, t, getLocation());

        }
        rcb.setRegVal(theResultReg, 
                      FieldDefImpl.Constants.booleanDef.createBoolean(retVal));
        state.done();
        return true;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        theSource.display(sb, formatter, verbose);
        sb.append("\n");
        theRegexParam.display(sb, formatter, verbose);
        if (theFlagsParam != null) {
            sb.append("\n");
            theFlagsParam.display(sb, formatter, verbose);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncRegexLikeIter)) {
            return false;
        }
        final FuncRegexLikeIter other = (FuncRegexLikeIter) obj;
        return Objects.equals(theRegexParam, other.theRegexParam) &&
            Objects.equals(theIsRegexConst, other.theIsRegexConst) &&
            Objects.equals(theSource, other.theSource) &&
            Objects.equals(theFlagsParam, other.theFlagsParam) &&
            Objects.equals(theIsFlagsParamConst, other.theIsFlagsParamConst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theRegexParam,
                            theIsRegexConst,
                            theSource,
                            theFlagsParam,
                            theIsFlagsParamConst);
    }
}
