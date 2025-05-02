/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import oracle.kv.Value;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

public class DataSerializer {
    private static final String SEED_FIELD = "seed";
    private static final String OPERATION_POPULATE = "POPULATE";
    private static final String OPERATION_EXERCISE = "EXERCISE";

    String seedJsonStr(long seed) {
        return "{\"" + SEED_FIELD + "\":" + seed + "}";
    }

    /** Create a value for random seed. */
    Value seedToValue(long seed) {
        String seedStr = seedJsonStr(seed);
        return Value.createValue(seedStr.getBytes());
    }

    /** Retrieve a seed from a value. */
    long valueToSeed(Value value) {
        long val = 0;
        if (value != null) {
            String jsonStr = new String(value.getValue());
            ObjectNode root = JsonUtils.parseJsonObject(jsonStr);
            val = root.get(SEED_FIELD).asLong();
        }
        return val;
    }

    static class Data {
        public String operation = OPERATION_POPULATE;
        public boolean firstThread = true;
        public long index = 0l;

        public Data() {}

        Data(String operation, boolean firstThread, long index) {
            this.operation = operation;
            this.firstThread = firstThread;
            this.index = index;
        }
    }
    /** Create a value for a populate operation. */
    Value populateToValue(long index) {
        Value value = null;
        Data populate = new Data(OPERATION_POPULATE, false, index);
        byte[] val = JsonUtils.writeAsJson(populate).getBytes();
        value = Value.createValue(val);
        return value;
    }

    /** Create a value for an exercise operation. */
    Value exerciseToValue(long index, boolean firstThread) {
        Value value = null;
        Data populate = new Data(OPERATION_EXERCISE, firstThread, index);
        byte[] val = JsonUtils.writeAsJson(populate).getBytes();
        value = Value.createValue(val);
        return value;
    }

    /** Convert a value for an operation into a string. */
    String dataValueToString(Value value) {
        if (value == null) {
            return "null";
        }
        if (value.getFormat() != Value.Format.NONE) {
            throw new IllegalStateException("Expected NONE format");
        }
        String ret = null;

        String jsonStr = new String(value.getValue());
        Data data = JsonUtils.fromJson(jsonStr, Data.class);
        if (OPERATION_POPULATE.equals(data.operation)) {
            ret = String.format("pp-index=%#012x", data.index);
        } else if (OPERATION_EXERCISE.equals(data.operation)) {
            ret = String.format("e%s-index=%#012x",
                                (data.firstThread ? "a" : "b"),
                                data.index);
        } else {
            throw new IllegalStateException
                ("Unexpected operation: " + data.operation);
        }
        return ret;
    }

    /* Convert a data string (pattern) to value, used for LOB operation now. */
    Value dataStringToValue(String pattern) {
        if (pattern == null)
            return null;
        String delimiter = "-index=0x";
        String[] retStrs = pattern.split(delimiter);
        if ((retStrs.length != 2) || !retStrs[0].matches("pp|ea|eb")) {
            throw new IllegalStateException(
                "Expected data string format: " + pattern);
        }
        long index = 0l;
        try {
            index = Long.parseLong(retStrs[1], 16);
        } catch (NumberFormatException ne) {
            throw new IllegalStateException(
                "Expected data string format: " + ne, ne);
        }
        return retStrs[0].equals("pp") ?
               populateToValue(index) :
               exerciseToValue(index, retStrs[0].equals("ea"));
    }
}
