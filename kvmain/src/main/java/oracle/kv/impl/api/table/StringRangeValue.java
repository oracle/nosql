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

class StringRangeValue extends StringValueImpl {

    private static final long serialVersionUID = 1L;

    private final StringDefImpl theTypeDef;

    StringRangeValue(String value, StringDefImpl def) {
        super(value);
        theTypeDef = def;
        def.validateValue(value);
    }
    
    /**
     * Constructor for FastExternalizable
     */
    StringRangeValue(DataInput in, short serialVersion, StringDefImpl def)
            throws IOException {
        super(in, serialVersion);
        theTypeDef = def;
    }
    
    /**
     * Writes this object to the output stream. Format:
     * 
     * <ol>
     * <li> ({@link StringValueImpl}) {@code super}
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
        return ValueType.STRING_RANGE_VALUE;
    }

    @Override
    public StringRangeValue clone() {
        return new StringRangeValue(value, theTypeDef);
    }

    @Override
    public StringDefImpl getDefinition() {
        return theTypeDef;
    }
}
