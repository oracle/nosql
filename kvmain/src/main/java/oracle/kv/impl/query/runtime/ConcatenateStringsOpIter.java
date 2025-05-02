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
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;

/**
 * Iterator to implement the concatenate strings operator
 *
 * {@literal any* arg1 || any* arg2 -> string? }
 *
 * {@literal concat(any* arg1, any* arg2, ...) -> string? }
 *
 * Concatenation is an operator, ||, an a function called concat, that returns
 * the character string made by joining its character string operands in the
 * order given. If any of the args is a sequence, than all the items are
 * concatenated to the result in the order they appear in the sequence.
 * If all args are empty sequence an empty sequence is returned. If all the
 * arguments are sql null than a sql null is returned. The maximum number of
 * chars of the returned string will be less than STRING_MAX_SIZE = 2^18 - 1 in
 * chars ie. 512kb, in which case a runtime query exception is thrown.
 *
 * Note: According to RDBMS operator precedence the || operator is immediately
 * after +,- (as binary operators).
 *
 * Note: A sql null argument is converted to empty string during concatenation
 * unless all arguments are sql null, in which case the result is sql null. So
 * sql null can result only from the concatenation of two or more sql null
 * values.
 *
 * Note: All arguments are implicitly casted to string* (string sequence of any
 * length), at the moment all other types are castable to string (including
 * JSON null which is changed to convert to the string "null").
 *
 *
 * Example
 * SELECT col1 || col2 || col3 || col4 as Concatenation FROM tab1;
 *
 * Concatenation
 * -------------
 * abcdefghijkl
 */
public class ConcatenateStringsOpIter extends PlanIter {

    // STRING_MAX_SIZE = 2^18 - 1 in chars ie. 512kb
    public static final int STRING_MAX_SIZE = 262143;

    private final PlanIter[] theArgs;

    public ConcatenateStringsOpIter(
        Expr e,
        int resultReg,
        @SuppressWarnings("unused") FunctionLib.FuncCode code,
        PlanIter[] argIters) {

        super(e, resultReg);
        theArgs = argIters;
    }

    /**
     * FastExternalizable constructor.
     */
    public ConcatenateStringsOpIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
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
        serializeIters(theArgs, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.STRING_CONCAT;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        for (PlanIter argIter : theArgs) {
            argIter.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }


        StringBuilder sb = null;
        boolean allArgsEmpty = true;

        for (PlanIter argIter : theArgs) {

            boolean opNext = argIter.next(rcb);
            if (!opNext) {
                continue;
            }

            allArgsEmpty = false;

            while (opNext) {
                FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());

                if (!argValue.isNull()) {
                    String argStr = argValue.asString().get();

                    if (sb == null) {
                        sb = new StringBuilder();
                    }

                    if ((sb.length() + argStr.length()) > STRING_MAX_SIZE) {
                        throw new QueryException(
                            "String concatenation generates a string whose " +
                            "length exceeds the maximum allowed length of " +
                            STRING_MAX_SIZE, getLocation());
                    }
                    sb.append(argStr);
                }
                opNext = argIter.next(rcb);
            }
        }

        if (allArgsEmpty) {
            state.done();
            return false;
        }

        FieldValueImpl res;

        if (sb == null) {
            res = NullValueImpl.getInstance();
        } else {
            res = FieldDefImpl.Constants.stringDef.createString(sb.toString());
        }

        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        for (PlanIter argIter : theArgs) {
            argIter.reset(rcb);
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

        for (PlanIter argIter : theArgs) {
            argIter.close(rcb);
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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof ConcatenateStringsOpIter)) {
            return false;
        }
        final ConcatenateStringsOpIter other = (ConcatenateStringsOpIter) obj;
        return Arrays.equals(theArgs, other.theArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theArgs);
    }
}
