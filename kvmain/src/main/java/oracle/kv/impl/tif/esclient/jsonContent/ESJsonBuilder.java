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

package oracle.kv.impl.tif.esclient.jsonContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;

/**
 * This is a wrapper over JsonGenerator. It is a limited one catering
 * to FTS use cases.
 *
 * It uses the more concrete class of ByteArrayOutputStream, instead of
 * OutputStream used for JsonGenerator.
 *
 * Also provides some convenience methods for write map type of values.
 *
 */
public class ESJsonBuilder {

    private JsonGenerator jsonGen;

    private final ByteArrayOutputStream bos;

    public static ESJsonBuilder builder() throws IOException {
        return new ESJsonBuilder(new ByteArrayOutputStream());
    }

    public ESJsonBuilder(ByteArrayOutputStream bos) throws IOException {
        this.bos = bos;
        this.jsonGen = ESJsonUtil.createGenerator(bos);
    }

    public JsonGenerator jsonGenarator() {
        return jsonGen;
    }

    public OutputStream getBos() {
        return bos;
    }

    public ESJsonBuilder startStructure(String structName) throws IOException {
        jsonGen.writeFieldName(structName);
        jsonGen.writeStartObject();
        return this;
    }

    public ESJsonBuilder startStructure() throws IOException {
        jsonGen.writeStartObject();
        return this;
    }

    public ESJsonBuilder endStructure() throws IOException {
        jsonGen.writeEndObject();
        return this;
    }

    public ESJsonBuilder startArray() throws IOException {
        jsonGen.writeStartArray();
        return this;
    }

    public ESJsonBuilder endArray() throws IOException {
        jsonGen.writeEndArray();
        return this;
    }

    public ESJsonBuilder field(String fieldName) throws IOException {
        if (fieldName == null) {
            throw new IllegalArgumentException
                    ("Json FieldName can not be null");
        }
        jsonGen.writeFieldName(fieldName);
        return this;
    }

    public ESJsonBuilder field(String fieldName, String value)
        throws IOException {

        if (fieldName == null) {

            if (value == null) {
                return nullValue();
            }
            jsonGen.writeString(value);

        } else { /* fieldName != null */

            if (value == null) {
                return nullField(fieldName);
            }
            jsonGen.writeStringField(fieldName, value);
        }
        return this;
    }

    public ESJsonBuilder field(Number value) throws IOException {
        return field(null, value);
    }

    public ESJsonBuilder field(String fieldName, Number value)
        throws IOException {

        if (fieldName == null) {

            if (value == null) {
                return nullValue();
            }

            if (value instanceof Integer) {
                jsonGen.writeNumber((int) value);
            } else if (value instanceof Long) {
                jsonGen.writeNumber((long) value);
            } else if (value instanceof Float) {
                jsonGen.writeNumber((float) value);
            } else if (value instanceof Double) {
                jsonGen.writeNumber((double) value);
            }

        } else { /* fieldName != null */

            if (value == null) {
                return nullField(fieldName);
            }

            if (value instanceof Integer) {
                jsonGen.writeNumberField(fieldName, (int) value);
            } else if (value instanceof Long) {
                jsonGen.writeNumberField(fieldName, (long) value);
            } else if (value instanceof Float) {
                jsonGen.writeNumberField(fieldName, (float) value);
            } else if (value instanceof Double) {
                jsonGen.writeNumberField(fieldName, (double) value);
            }

        }
        return this;
    }

    public ESJsonBuilder field(String name, Map<String, Object> value)
        throws IOException {
        jsonGen.writeFieldName(name);
        writeValue(value);
        return this;
    }

    public ESJsonBuilder value(String value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        jsonGen.writeString(value);
        return this;
    }

    public ESJsonBuilder value(Map<String, Object> value) throws IOException {
        writeValue(value);
        return this;
    }

    public ESJsonBuilder nullField(String name) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException
                    (" Json FieldName can not be null");
        }
        jsonGen.writeNullField(name);
        return this;
    }

    public ESJsonBuilder nullValue() throws IOException {
        jsonGen.writeNull();
        return this;
    }

    public void flushJsonGenerator() throws IOException {
        jsonGen.flush();
    }

    private void writeValue(Object value) throws IOException {
        if (value == null) {
            jsonGen.writeNull();
            return;
        }
        final Class<?> type = value.getClass();
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, ?> mapValue = (Map<String, ?>) value;
            jsonGen.writeStartObject();

            for (Map.Entry<String, ?> entry : mapValue.entrySet()) {
                jsonGen.writeFieldName(entry.getKey());
                final Object innerValue = entry.getValue();
                if (innerValue == null) {
                    jsonGen.writeNull();
                } else {
                    writeValue(innerValue);
                }
            }
            jsonGen.writeEndObject();
        } else if (value instanceof Iterable) {
            jsonGen.writeStartArray();
            for (Object v : (Iterable<?>) value) {
                writeValue(v);
            }
            jsonGen.writeEndArray();
        } else if (value instanceof Object[]) {
            jsonGen.writeStartArray();
            for (Object v : (Object[]) value) {
                writeValue(v);
            }
            jsonGen.writeEndArray();
        } else if (type == String.class) {
            jsonGen.writeString((String) value);
        } else if (type == Integer.class) {
            jsonGen.writeNumber(((Integer) value).intValue());
        } else if (type == Long.class) {
            jsonGen.writeNumber(((Long) value).longValue());
        } else if (type == Float.class) {
            jsonGen.writeNumber(((Float) value).floatValue());
        } else if (type == Double.class) {
            jsonGen.writeNumber(((Double) value).doubleValue());
        } else if (type == Byte.class) {
            jsonGen.writeNumber(((Byte) value).byteValue());
        } else if (type == Short.class) {
            jsonGen.writeNumber(((Short) value).shortValue());
        } else if (type == Boolean.class) {
            jsonGen.writeBoolean(((Boolean) value).booleanValue());
        } else if (type == byte[].class) {
            jsonGen.writeBinary((byte[]) value);
        }
    }

    public byte[] byteArray() throws IOException {
        jsonGen.flush();
        return bos.toByteArray();
    }

}
