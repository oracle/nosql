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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import oracle.nosql.nson.Nson;
import oracle.nosql.nson.util.NioByteInputStream;
import oracle.nosql.nson.values.FieldValueEventHandler;
import oracle.nosql.nson.values.TimestampValue;

/**
 * An NSON FieldValueEventHandler instance that serializes NSON into
 * PrimaryKey.
 * We don't expect these fields on PrimaryKey:
 * Map, Array, Record, Binary, Fixed, Null, Empty, JsonNull
 */
class KeyCreator implements FieldValueEventHandler {

    private final PrimaryKeyImpl pKey;
    private final byte[] nsonBytes;
    private String currentKey;

    KeyCreator(PrimaryKeyImpl pKey, byte[] nsonBytes) {
        this.pKey = pKey;
        this.nsonBytes = nsonBytes;
    }

    /**
     * The is the entry to generate NSON events from NSON bytes and writes them
     * into PrimaryKey.
     */
    void create() {
        try (NioByteInputStream bis =
            new NioByteInputStream(ByteBuffer.wrap(nsonBytes))) {

            Nson.generateEventsFromNson(this, bis, false);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Error creating PrimaryKey from NSON: " + ioe.getMessage());
        }
    }

    @Override
    public boolean startMapField(String key) {
        currentKey = key;
        return false;
    }

    @Override
    public void endMapField(String key) {
        currentKey = null;
    }

    @Override
    public void booleanValue(boolean value) {
        pKey.put(currentKey, value);
    }

    @Override
    public void stringValue(String value) {
        /* We use String type in NSON for both String and Enum field values */
        FieldDefImpl currentDef = pKey.getFieldDef(currentKey);
        if (currentDef != null && currentDef.isEnum()) {
            pKey.putEnum(currentKey, value);
        } else {
            pKey.put(currentKey, value);
        }
    }

    @Override
    public void integerValue(int value) {
        pKey.put(currentKey, value);
    }

    @Override
    public void longValue(long value) {
        pKey.put(currentKey, value);
    }

    @Override
    public void doubleValue(double value) {
        /* We use double type in NSON for both float and double field values */
        FieldDefImpl currentDef = pKey.getFieldDef(currentKey);
        if (currentDef != null && currentDef.isFloat()) {
            pKey.put(currentKey, (float) value);
        } else {
            pKey.put(currentKey, value);
        }
    }

    @Override
    public void numberValue(BigDecimal value) {
        pKey.putNumber(currentKey, value);
    }

    @Override
    public void timestampValue(TimestampValue timestamp) {
        /* the precision of the timestamp in the record is needed */
        TimestampDefImpl def = (TimestampDefImpl) pKey.getDefinition().
                                                       getFieldDef(currentKey);

        pKey.put(currentKey, def.createTimestamp(timestamp.getValue()));
    }

    @Override
    public String toString() {
        return pKey.toString();
    }
}
