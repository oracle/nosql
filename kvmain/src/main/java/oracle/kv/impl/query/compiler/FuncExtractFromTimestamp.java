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

import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.FuncExtractFromTimestampIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * int? year(any? timestamp)
 * int? month(any? timestamp)
 * int? day(any? timestamp)
 * int? hour(any? timestamp)
 * int? minute(any? timestamp)
 * int? second(any? timestamp)
 * int? millisecond(any? timestamp)
 * int? microsecond(any? timestamp)
 * int? nanosecond(any? timestamp)
 * int? week(any? timestamp)
 * int? isoweek(any? timestamp)
 * int? quarter(any? timestamp)
 * int? day_of_week(any? timestamp)
 * int? day_of_month(any? timestamp)
 * int? day_of_year(any? timestamp)
 *
 * Function to extract information from a TIMESTAMP value.
 */
public class FuncExtractFromTimestamp extends Function {

    /*
     * The temporal fields to extract from Timestamp.
     *
     * Don't change the ordering of the below values they are mapped to
     * FN_YEAR to FN_ISOWEEK in FuncCode and should have the same ordering.
     */
    public static enum Unit {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        MILLISECOND,
        MICROSECOND,
        NANOSECOND,
        WEEK,
        ISOWEEK,
        QUARTER,
        DAY_OF_WEEK,
        DAY_OF_MONTH,
        DAY_OF_YEAR;

        private static final Unit[] VALUES = values();
        public static final int VALUES_COUNT = VALUES.length;
        public static Unit valueOf(int ordinal) {
            return VALUES[ordinal];
        }
    }

    FuncExtractFromTimestamp(FuncCode code, String name) {
        super(code, name,
              TypeManager.ANY_QSTN(),
              TypeManager.INT_QSTN()); /* RetType */
    }

    @Override
    boolean isIndexable() {
        return true;
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return true;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return caller.getArg(0).mayReturnEmpty();
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
                     ExprFuncCall caller,
                     PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        Unit unit;
        if (theCode.ordinal() <= FuncCode.FN_ISOWEEK.ordinal()) {
            unit = Unit.valueOf(theCode.ordinal() - FuncCode.FN_YEAR.ordinal());
        } else {
            int offset = theCode.ordinal() - FuncCode.FN_QUARTER.ordinal();
            unit = Unit.valueOf(Unit.QUARTER.ordinal() + offset);
        }

        return new FuncExtractFromTimestampIter(caller, resultReg,
                                                unit, argIters[0]);
    }
}
