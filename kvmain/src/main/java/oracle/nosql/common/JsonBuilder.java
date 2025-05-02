/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common;

/**
 * A builder to build up the JSON string, it is used to build the JSON
 * payload of Http Response.
 *
 * - To append primitive type values, use append(name, value).
 * - To append nested JSON object,
 *    o call startObject() to begin a nested object
 *    o call append(name, value) to append primitive type value.
 *    o call endObecjt() to finish the nested object
 * - To append nested JSON array
 *    o call startArray() to begin a nested array.
 *    o call append(name, value) to append primitive type value.
 *    o call endArray() to finish the nested array.
 * - To append JSON object or array in side nested JSON object or array,
 *   repeat above steps of nested JSON object/array inside its parent JSON
 *   object or array.
 */
public class JsonBuilder {
    private final StringBuilder sb;
    /* true if JSON root is object, false if JSON root is array. */
    private final boolean isObject;
    private boolean first;

    private JsonBuilder() {
        this(true);
    }

    private JsonBuilder(boolean isObject) {
        sb = new StringBuilder();
        this.isObject = isObject;
        first = true;
        if (isObject) {
            startObject(null);
        } else {
            startArray(null);
        }
    }

    /**
     * Create JSON the root is Object.
     */
    public static JsonBuilder create() {
        return new JsonBuilder();
    }

    /**
     * Create JSON the root can be Object or Array.
     * @param isObject true if JSON root is object, false if JSON root is array.
     */
    public static JsonBuilder create(boolean isObject) {
        return new JsonBuilder(isObject);
    }

    public JsonBuilder append(String name, Object value) {
        appendName(name);
        if (value instanceof String) {
            appendWithQuote(escape((String)value));
        } else {
            sb.append(value);
        }
        return this;
    }

    private static String escape(String str){
        final StringBuilder result = new StringBuilder();
        for (char ch: str.toCharArray()){
            if (ch == '\"' ){
                result.append("\\\"");
            } else if(ch == '\\'){
                result.append("\\\\");
            } else if(ch == '/'){
                result.append("\\/");
            } else if(ch == '\b'){
                result.append("\\b");
            } else if(ch == '\f'){
                result.append("\\f");
            } else if(ch == '\n'){
                result.append("\\n");
            } else if(ch == '\r'){
                result.append("\\r");
            } else if(ch == '\t'){
                result.append("\\t");
            } else {
                result.append(ch);
            }
        }
        return result.toString();
    }

    public JsonBuilder append(Object value) {
        return append(null, value);
    }

    public JsonBuilder appendJson(String name, String json) {
        appendName(name);
        sb.append(json);
        return this;
    }

    public JsonBuilder startObject(String name) {
        appendName(name);
        startStruct("{");
        return this;
    }

    public JsonBuilder endObject() {
        endStruct("}");
        return this;
    }

    public JsonBuilder startArray(String name) {
        appendName(name);
        startStruct("[");
        return this;
    }

    public JsonBuilder endArray() {
        endStruct("]");
        return this;
    }

    private void startStruct(String bracket) {
        sb.append(bracket);
        first = true;
    }

    private void endStruct(String bracket) {
        sb.append(bracket);
        if (first) {
            first = false;
        }
    }

    private void appendDelimiter() {
        if (first) {
            first = false;
        } else {
            sb.append(",");
        }
    }

    private void appendName(String name) {
        appendDelimiter();
        if (name != null) {
            appendWithQuote(escape(name));
            sb.append(":");
        }
    }

    private void appendWithQuote(String str) {
        sb.append("\"").append(str).append("\"");
    }

    @Override
    public String toString() {
        if (isObject) {
            endObject();
        } else {
            endArray();
        }
        return sb.toString();
    }
}
