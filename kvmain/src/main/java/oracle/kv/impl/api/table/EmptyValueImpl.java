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

import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

/**
 * A class represents an Empty value, it is used only in index key.
 */
public class EmptyValueImpl extends FieldValueImpl {

    private static final long serialVersionUID = 1L;

    private static EmptyValueImpl instanceValue = new EmptyValueImpl();

    public static EmptyValueImpl getInstance() {
        return instanceValue;
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.EMPTY_VALUE;
    }

    @Override
    public long sizeof() {
        return 0;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.EMPTY;
    }

    @Override
    public FieldDefImpl getDefinition() {
        return FieldDefImpl.Constants.emptyDef;
    }

    @Override
    public boolean isSpecialValue() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    @Override
    public void toStringBuilder(StringBuilder sb, DisplayFormatter formatter) {
        sb.append("\"EMPTY\"");
    }

    @Override
    public EmptyValueImpl clone() {
        return getInstance();
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof EmptyValueImpl) {

            /* all empty values are the same, and equal */
            return 0;
        }
        throw new ClassCastException
            ("Object is not a EmptyValue");
    }

    @Override
    public String toString() {
        return "<EMPTY>";
    }

    @Override
    public boolean isEMPTY() {
        return true;
    }
}
