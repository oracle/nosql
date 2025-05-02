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

package oracle.kv.impl.api.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class IntegerRangeValue extends IntegerValueImpl {

    private static final long serialVersionUID = 1L;

    private final IntegerDefImpl theTypeDef;

    IntegerRangeValue(int value, IntegerDefImpl def) {
        super(value);
        theTypeDef = def;
        def.validateValue(value);
    }

    /**
     * This constructor creates IntegerValueImpl from the String format used for
     * sorted keys.
     */
    IntegerRangeValue(String keyValue, IntegerDefImpl def) {
        super(keyValue);
        theTypeDef = def;
        // No validation needed ????
    }
    
    /**
     * Constructor for FastExternalizable
     */
    IntegerRangeValue(DataInput in, IntegerDefImpl def)
            throws IOException {
        super(in);
        theTypeDef = def;
    }
    
    /**
     * Writes this object to the output stream. Format:
     * 
     * <ol>
     * <li> ({@link IntegerValueImpl}) {@code super}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        /* theTypeDef is not written */
    }
    
    @Override
    protected ValueType getValueType() {
        return ValueType.INTEGER_RANGE_VALUE;
    }

    @Override
    public IntegerRangeValue clone() {
        return new IntegerRangeValue(value, theTypeDef);
    }

    @Override
    public IntegerDefImpl getDefinition() {
        return theTypeDef;
    }
}
