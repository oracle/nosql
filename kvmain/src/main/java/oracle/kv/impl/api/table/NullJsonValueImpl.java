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

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

/**
 * A class to encapsulate an explicitly null value for a field.  This class is
 * implemented as a singleton object and is never directly constructed or
 * accessed by applications.
 */
public class NullJsonValueImpl extends FieldValueImpl {

    private static final long serialVersionUID = 1L;

    private static NullJsonValueImpl instanceValue = new NullJsonValueImpl();

    public static NullJsonValueImpl getInstance() {
        return instanceValue;
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.NULL_JSON_VALUE;
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion) {
        /*
         * This class is only used during query return and is not serialized
         * with the table metadata otherwise. So there is no need to support
         * FastExternal.
         */
        fastExternalNotSupported();
    }

    @Override
    public long sizeof() {
        return 0;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.ANY_JSON_ATOMIC;
    }

    @Override
    public FieldDefImpl getDefinition() {
        return FieldDefImpl.Constants.anyJsonAtomicDef;
    }

    @Override
    public boolean isJsonNull() {
        return true;
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
        sb.append("null");
    }

    @Override
    public NullJsonValueImpl clone() {
        return getInstance();
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof NullJsonValueImpl) {

            /* all null values are the same, and equal */
            return 0;
        }
        throw new ClassCastException
            ("Object is not a NullJsonValue");
    }

    @Override
    public String toString() {
        return "null";
    }
}
