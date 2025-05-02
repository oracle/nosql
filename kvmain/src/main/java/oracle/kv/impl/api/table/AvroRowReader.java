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

import java.util.List;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;

/**
 * The interface of Avro row deserializer.
 */
public interface AvroRowReader {
    void readInteger(String fieldName, int val);
    void readLong(String fieldName, long val);
    void readFloat(String fieldName, float val);
    void readDouble(String fieldName, double val);
    void readNumber(String fieldName, byte[] bytes);
    void readTimestamp(String fieldName, FieldDef def, byte[] bytes);
    void readBinary(String fieldName, byte[] bytes);
    void readFixedBinary(String fieldName, FieldDef def, byte[] bytes);
    void readString(String fieldName, String val);
    void readBoolean(String fieldName, boolean val);
    void readEnum(String fieldName, FieldDef def, int index);
    void readNull(String fieldName);
    void readJsonNull(String fieldName);
    void readEmpty(String fieldName);
    void startRecord(String fieldName, FieldDef def, int size);
    void endRecord(int size);
    void startMap(String fieldName, FieldDef def, int size);
    void endMap(int size);
    void startArray(String fieldName, FieldDef def, FieldDef elemDef, int size);
    void endArray(int size);

    @SuppressWarnings("unused")
    default void startMapField(String fieldName) {
    }
    @SuppressWarnings("unused")
    default void endMapField(String fieldName) {
    }
    @SuppressWarnings("unused")
    default void startArrayField(int index) {
    }
    @SuppressWarnings("unused")
    default void endArrayField(int index) {
    }

    default void readCounterCRDT(String fieldName, FieldValueImpl val) {
        if (val.getType() == Type.INTEGER) {
            readInteger(fieldName, val.getInt());
        } else if (val.getType() == Type.LONG) {
            readLong(fieldName, val.getLong());
        } else if (val.getType() == Type.NUMBER) {
            readNumber(fieldName, val.getBytes());
        } else {
            throw new IllegalArgumentException("The MRCounter " +
                "for type " + val.getType() + " is not supported");
        }
    }

    @SuppressWarnings("unused")
    default void readDefaultJsonMRCounter(String fieldName,
                                          FieldDefImpl fDef,
                                          List<TablePath> paths) {}
}
