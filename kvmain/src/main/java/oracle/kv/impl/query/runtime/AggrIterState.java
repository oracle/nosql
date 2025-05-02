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

import java.math.BigDecimal;

import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.table.FieldDef.Type;


class AggrIterState extends PlanIterState {

    long theCount;

    long theLongSum;

    double theDoubleSum;

    BigDecimal theNumberSum = null;

    Type theSumType = Type.LONG;

    boolean theGotNumericInput;

    FieldValueImpl theMinMax = NullValueImpl.getInstance();

    @Override
    public void reset(PlanIter iter) {
        super.reset(iter);
        theCount = 0;
        theLongSum = 0;
        theDoubleSum = 0; 
        theNumberSum = null;
        theSumType = Type.LONG;
        theGotNumericInput = false;
        theMinMax = NullValueImpl.getInstance();
    }
}
