/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import java.math.BigDecimal;

import com.fasterxml.jackson.core.io.CharTypes;

/**
 * JsonSerializer is a {@link FieldValueEventHandler} instance that creates
 * a JSON string value without pretty printing.
 * @hide-in-sdk
 */
public class JsonSerializer implements FieldValueEventHandler {

    protected final StringBuilder sb;
    protected final JsonOptions options;

    /**
     * Constants use for serialization
     */
    protected static final String START_OBJECT = "{";
    protected static final String END_OBJECT = "}";
    protected static final String START_ARRAY = "[";
    protected static final String END_ARRAY = "]";
    protected static final String FIELD_SEP = ",";
    protected static final String QUOTE = "\"";

    /*
     * This can be modified by a child class instance
     */
    protected String KEY_SEP = ":";

    /**
     * Creates a new JsonSerializer.
     *
     * @param options {@link JsonOptions} to use for the serialization, or
     * null for default behavior.
     */
    public JsonSerializer(JsonOptions options) {
        if (options != null && options.getPrettyPrint() &&
            !(this instanceof JsonPrettySerializer)) {
            throw new IllegalArgumentException(
                "Use JsonPrettySerializer for pretty printing");
        }
        sb = new StringBuilder();
        this.options = options;
    }

    @Override
    public void startMap(int size) {
        sb.append(START_OBJECT);
    }

    @Override
    public void startArray(int size) {
        sb.append(START_ARRAY);
    }

    @Override
    public void endMap(int size) {
        int len = sb.length() - 1;
        if (len > 0 && sb.charAt(len) == ',') {
            sb.setLength(len);
        }
        sb.append(END_OBJECT);
    }

    @Override
    public void endArray(int size) {
        int len = sb.length() - 1;
        if (len > 0 && sb.charAt(len) == ',') {
            sb.setLength(len);
        }
        sb.append(END_ARRAY);
    }

    @Override
    public boolean startMapField(String key) {
        sb.append(QUOTE);
        CharTypes.appendQuoted(sb, key);
        sb.append(QUOTE).append(KEY_SEP);
        return false;
    }

    @Override
    public void endMapField(String key) {
        sb.append(FIELD_SEP);
    }

    @Override
    public void endArrayField(int index) {
        sb.append(FIELD_SEP);
    }

    @Override
    public void booleanValue(boolean value) {
        sb.append(Boolean.toString(value));
    }

    @Override
    public void binaryValue(byte[] byteArray) {
        sb.append(QUOTE).append(BinaryValue.encodeBase64(byteArray))
            .append(QUOTE);
    }

    @Override
    public void binaryValue(byte[] byteArray, int offset, int length) {
        /* TODO: offset/length in BinaryValue methods */
        sb.append(QUOTE).append(BinaryValue.encodeBase64(byteArray))
            .append(QUOTE);
    }

    @Override
    public void stringValue(String value) {
        sb.append(QUOTE);
        CharTypes.appendQuoted(sb, value);
        sb.append(QUOTE);
    }

    @Override
    public void integerValue(int value) {
        sb.append(Integer.toString(value));
    }

    @Override
    public void longValue(long value) {
        sb.append(Long.toString(value));
    }

    @Override
    public void doubleValue(double value) {
        sb.append(Double.toString(value));
    }

    @Override
    public void numberValue(BigDecimal value) {
        sb.append(value.toString());
    }

    @Override
    public void timestampValue(TimestampValue timestamp) {
        if (options != null && options.getTimestampAsLong()) {
            sb.append(String.valueOf(timestamp.getLong()));
        } else {
            sb.append(QUOTE).append(timestamp.getString()).append(QUOTE);
        }
    }

    @Override
    public void jsonNullValue() {
        sb.append("null");
    }

    @Override
    public void nullValue() {
        sb.append("null");
    }

    @Override
    public void emptyValue() {
        sb.append("\"EMPTY\"");
    }

    @Override
    public String toString() {
        return sb.toString();
    }
}
