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

class FloatRangeValue extends FloatValueImpl {

    private static final long serialVersionUID = 1L;

    private final FloatDefImpl theTypeDef;

    FloatRangeValue(float value, FloatDefImpl def) {
        super(value);
        theTypeDef = def;
        def.validateValue(value);
    }

    /**
     * This constructor creates DoubleValueImpl from the String format used for
     * sorted keys.
     */
    FloatRangeValue(String keyValue, FloatDefImpl def) {
        super(keyValue);
        theTypeDef = def;
        // No validation needed ????
    }

    /**
     * Constructor for FastExternalizable
     */
    FloatRangeValue(DataInput in, FloatDefImpl def)
            throws IOException {
        super(in);
        theTypeDef = def;
    }
    
    /**
     * Writes this object to the output stream. Format:
     * 
     * <ol>
     * <li> ({@link FloatValueImpl}) {@code super}
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
        return ValueType.FLOAT_RANGE_VALUE;
    }
    
    @Override
    public FloatRangeValue clone() {
        return new FloatRangeValue(value, theTypeDef);
    }

    @Override
    public FloatDefImpl getDefinition() {
        return theTypeDef;
    }
}
