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

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.EmptyValueImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.compiler.ExprInOp;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.CompOpIter.CompResult;

public class InOpIter extends PlanIter {

    static private class InOpIterState extends PlanIterState {

        final CompResult theResult = new CompResult();

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theResult.clear();
        }
    }

    private final boolean theIsIN3;

    private final int theNumKeyComps;

    private final PlanIter[] theArgs;

    private final int theNumKeys;

    public InOpIter(
        ExprInOp e,
        int resultReg,
        PlanIter[] args) {

        super(e, resultReg);
        theArgs = args;
        theIsIN3 = e.isIN3();
        theNumKeyComps = e.getNumKeyComps();
        theNumKeys = e.getNumKeys();
    }

    InOpIter(DataInput in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theIsIN3 = in.readBoolean();
        theNumKeyComps = in.readInt();
        theArgs = deserializeIters(in, serialVersion);
        theNumKeys = in.readInt();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeBoolean(theIsIN3);
        out.writeInt(theNumKeyComps);
        serializeIters(theArgs, out, serialVersion);
        out.writeInt(theNumKeys);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.IN;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new InOpIterState());
        for (PlanIter arg : theArgs) {
            arg.open(rcb);
        }
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
    public boolean next(RuntimeControlBlock rcb) {

        InOpIterState state = (InOpIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        /* Compute the data key */
        for (int keyComp = 0; keyComp < theNumKeyComps; ++keyComp) {

            PlanIter arg = theArgs[keyComp];

            boolean more = arg.next(rcb);

            if (!more) {
                rcb.setRegVal(arg.theResultReg, EmptyValueImpl.getInstance());
            } else {
                if (arg.next(rcb)) {
                    throw new QueryException(
                        "An expression on the left-hand-side of an IN operator " +
                        "returns a sequence with more than one items.",
                        arg.getLocation());
                }

                if (rcb.getRegVal(arg.getResultReg()).isNull()) {
                    rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                    state.done();
                    return true;
                }
            }
        }

        if (theIsIN3) {
            return next3(rcb, state);
        }

        return next1(rcb, state);
    }

    private boolean next1(RuntimeControlBlock rcb, InOpIterState state) {

        FieldValueImpl argVal;
        FieldValueImpl keyVal;
        PlanIter argIter;
        int numNullResults = 0;

        for (int k = 1; k <= theNumKeys; ++k) { 

            boolean isFalseResult = false;
            boolean isNullResult = false;

            /* Compute the next search key and compare it with the data key */
            for (int keyComp = 0; keyComp < theNumKeyComps; ++keyComp) {

                PlanIter keyCompIter = theArgs[k * theNumKeyComps + keyComp];

                boolean more = keyCompIter.next(rcb);

                if (more) {
                    keyVal = rcb.getRegVal(keyCompIter.getResultReg());

                    if (keyCompIter.next(rcb)) {
                        throw new QueryException(
                            "An expression on the right-hand-side of an IN " +
                            "operator returns a sequence with more than one " +
                            "items.", keyCompIter.getLocation());
                    }
                } else {
                    keyVal = EmptyValueImpl.getInstance();
                }

                if (!isFalseResult) {
                    argIter = theArgs[keyComp];
                    argVal = rcb.getRegVal(argIter.getResultReg());
                    CompOpIter.compare(rcb, argVal, keyVal, FuncCode.OP_EQ,
                                       false, // forSort
                                       state.theResult, theLocation);

                    if (state.theResult.haveNull) {
                        isNullResult = true;
                    } else if (state.theResult.incompatible ||
                        state.theResult.comp != 0) {
                        isFalseResult = true;
                        isNullResult = false;
                    }
                }
            }

            if (isNullResult) {
                ++numNullResults;
            } else if (!isFalseResult) {
                rcb.setRegVal(theResultReg, BooleanValueImpl.trueValue);
                state.done();
                return true;
            }
        }

        if (numNullResults == theNumKeys) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
        } else {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
        }
        state.done();
        return true;
    }

    private boolean next3(RuntimeControlBlock rcb, InOpIterState state) {

        FieldValueImpl argVal;
        FieldValueImpl keyVal;
        PlanIter keyCompIter = theArgs[theNumKeyComps];
        PlanIter argIter;
        boolean moreKeyComps = true;
        int numKeys = 0;
        int numNullResults = 0;

        while (moreKeyComps) { 

            boolean isFalseResult = false;
            boolean isNullResult = false;
            boolean isPartialKey = false;

            /* Compute the next search key and compare it with the data key */
            for (int keyComp = 0; keyComp < theNumKeyComps; ++keyComp) {

                /* Compute the next key comp, if any */
                moreKeyComps = keyCompIter.next(rcb);

                if (!moreKeyComps) {
                    isPartialKey = true;
                    break;
                }

                /* Compare the current component from the data key with the
                 * corresponding component from the search key. */
                if (!isFalseResult) {
                    argIter = theArgs[keyComp];
                    argVal = rcb.getRegVal(argIter.getResultReg());
                    keyVal = rcb.getRegVal(keyCompIter.getResultReg());

                    if (!argVal.isNull() &&
                        !keyVal.isNull() &&
                        !keyVal.getDefinition().
                        isSubtype(argVal.getDefinition()) &&
                        !argVal.isEMPTY() &&
                        !argVal.isJsonNull() &&
                        !keyVal.isJsonNull()) {
                        throw new QueryException(
                            "Type mismatch in IN operator.\n" +
                            "LHS type: " + argVal.getType() + 
                            " RHS type: " + keyVal.getType() +
                            "\nLHS val = " + argVal +
                            " RHS val = " + keyVal,
                            keyCompIter.theLocation);
                    }

                    CompOpIter.compare(rcb, argVal, keyVal, FuncCode.OP_EQ,
                                       false, // forSort
                                       state.theResult, theLocation);

                    if (state.theResult.haveNull) {
                        isNullResult = true;
                    } else if (state.theResult.incompatible ||
                               state.theResult.comp != 0) {
                        isFalseResult = true;
                        isNullResult = false;
                    }
                }
            }

            if (isPartialKey) {
                break;
            }

            ++numKeys;

            if (isNullResult) {
                ++numNullResults;
                continue;
            }

            /* If the search and data keys matched, we are done and return true.
             * Else, we continue by computing the next search key */
            if (!isFalseResult) {
                rcb.setRegVal(theResultReg, BooleanValueImpl.trueValue);
                state.done();
                return true;
            }
        }

        if (numNullResults == numKeys) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
        } else {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
        }
        state.done();
        return true;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        int i = 0;

        formatter.indent(sb);
        sb.append("\"left-hand-side expressions\" : [\n");

        formatter.incIndent();
        for (; i < theNumKeyComps; ++i) {
            theArgs[i].display(sb, formatter, verbose);
            if (i < theNumKeyComps - 1) {
                sb.append(",\n");
            }
        }
        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("],\n");

        formatter.indent(sb);
        sb.append("\"right-hand-side expressions\" : [\n");
        formatter.incIndent();

        if (theNumKeyComps == 1 || theIsIN3) {
            for (; i < theArgs.length; ++i) {
                theArgs[i].display(sb, formatter, verbose);
                if (i < theArgs.length - 1) {
                    sb.append(",\n");
                }
            }
        } else {
            formatter.indent(sb);
            sb.append("[\n");
            formatter.incIndent();
            while (i < theArgs.length) {
                theArgs[i].display(sb, formatter, verbose);
                ++i;
                if (i % theNumKeyComps == 0) {
                    formatter.decIndent();
                    sb.append("\n");
                    formatter.indent(sb);
                    sb.append("]");
                    if (i < theArgs.length) {
                        sb.append(",\n");
                        formatter.indent(sb);
                        sb.append("[\n");
                        formatter.incIndent();
                    }
                } else if (i < theArgs.length) {
                    sb.append(",\n");
                }
            }
        }

        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("]");
    }
}
