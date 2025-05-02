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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;

@SuppressWarnings("deprecation")
public final class ESJsonUtil {

    private static final JsonFactory jsonFactory;
    private static final boolean DEBUG_ON = false;

    /**
     * Private constructor to satisfy checkStyle and to prevent
     * instantiation of this utility class.
     */
    private ESJsonUtil() {
        throw new AssertionError("Cannot instantiate utility class " +
                                 ESJsonUtil.class);
    }

    static {
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW,
                              false);
        /* Do not automatically close unclosed objects/arrays in
         * com.fasterxml.jackson.core.json.UTF8JsonGenerator#close() method
         */
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT,
                              false);
        jsonFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION,
                              true);
    }

    public static JsonFactory getJsonFactory() {
        return jsonFactory;
    }

    public static JsonGenerator createGenerator(OutputStream os)
        throws IOException {
        return jsonFactory.createGenerator(os, JsonEncoding.UTF8);
    }

    public static Map<String, Object> convertToMap(byte[] source)
        throws JsonParseException, IOException {
        return parseAsMap(createParser(source));
    }

    /**
     * Json stored in a column of a kvstore table can now be indexed
     * (stored) in elasticsearch. Although json data stored in a
     * kvstore table is stored in 'flattened' or 'collapsed' format,
     * that data is expanded and indexed in elasticsearch as a
     * possibly multi-level tree of json objects. For example,
     * suppose the kvstore table contains a column of type JSON in
     * which a document with the following content is inserted:
     *
     * {
     *   "info_1": {
     *     "firstname":"John",
     *     "lastname":"Smith",
     *     "address": {
     *       "number":"9115",
     *       "street":"Vaughan",
     *       "city":"Detroit",
     *       "state":"MA"
     *     }
     *   },
     *   "info_2": {
     *     "gender":"male",
     *     "expenses":10235.54,
     *     "children":3,
     *     "spouse":true
     *   }
     * }
     *
     * When referencing a given element (field) of the document,
     * the element is specified using a multi-part jsonpath;
     * that is,
     *
     * info_1.firstname, jsonField_1.address.number,
     * info_1.address.street, info_1.address.city,
     * info_1.address.state, info_2.gender, info_2.expenses,
     * info_2.children, info_2.spouse
     *
     * When the document is indexed (stored) in elasticsearch,
     * rather than sending the document contents to elasticsearch
     * as a single raw, json-formatted string, a mapping that
     * reflects the document structure (nested objects, arrays,
     * types, etc.) is registered with elasticsearch; which
     * then indexes the document contents according to the
     * structure specified in the mapping.
     *
     * This means that when a document like that described above
     * is retrieved from elasticsearch, the results of the query
     * will arrive in a form that includes duplicate properties/fields
     * in the json (see the method GetResponse.buildFromJson). For
     * example, after indexing the above document in elasticsearch,
     * requesting that document from elasticsearch will produce a
     * response with a '_source' component having the form:
     *
     * {
     *   "_pkey":{
     *     "_table":"JsonTable",
     *     "id":"3"
     *   },
     *   "info_1":{
     *     "firstname":"John"
     *   },
     *   "info_1":{
     *     "lastname":"Smith"
     *   },
     *   "info_1":{
     *     "address":{
     *       "number":"9115"
     *     },
     *     "address":{
     *       "street":"Vaughan"
     *       },
     *     "address":{
     *       "city":"Detroit"
     *     },
     *     "address":{
     *       "state":"MA"
     *     }
     *   },
     *   "info_2":{
     *     "gender":"Male"
     *   },
     *   "info_2":{
     *     "expenses":10235.54
     *   },
     *   "info_2":{
     *     "children":3
     *   },
     *   "info_2":{
     *     "spouse":true
     *   }
     * }
     *
     * To place the contents of such a response from elasticsearch
     * in a {@literal Map<String, Object>} that accurately reflects the contents
     * of the response, and which can be correctly processed on the
     * kvstore side, the response is parsed with duplication detection
     * disabled (otherwise errors will occur), and the duplicate
     * fields are 'flattened' or 'collapsed'. For example, for a
     * response with _source document like that shown above, the
     * contents of that document is converted into a new byte array
     * of the form:
     *
     * {
     *   "_pkey":{
     *     "_table":"JsonTable",
     *     "id":"3"
     *   },
     *   "info_1.firstname":"John",
     *   "info_1.lastname":"Smith",
     *   "info_1.address.number":"9115",
     *   "info_1.address.street":"Vaughan",
     *   "info_1.address.city":"Detroit",
     *   "info_1.address.state":"MA",
     *   "info_2.gender":"Male",
     *   "info_2.expenses":10235.54,
     *   "info_2.children":3,
     *   "info_2.spouse":true
     * }
     *
     * That is, the info_1 and info_2 objects are 'collapsed' into
     * separate scalar fields with multi-part, dot-separated names.
     * Since json contained in the resulting byte array contains
     * no duplicate fields, that result can then be parsed in the
     * usual way to produce the map that is ultimately returned
     * by this method.
     */
    public static Map<String, Object> convertToMapCollapseDuplicates(
                                          final byte[] source)
        throws JsonParseException, IOException {
        final JsonParser dupParser = createParser(source);
        dupParser.disable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

        final java.util.Set<String> allObjectNames = new java.util.HashSet<>();
        final java.util.Set<String> dupObjectNames = new java.util.HashSet<>();

        JsonToken dupToken = dupParser.nextToken();
        while (dupToken != null) {
            if (dupToken == JsonToken.START_OBJECT) {
                final String curName = dupParser.getCurrentName();
                if (curName != null) {
                    if (allObjectNames.contains(curName)) {
                        dupObjectNames.add(curName);
                    }
                    allObjectNames.add(curName);
                }
            }
            dupToken = dupParser.nextToken();
        }

        byte[] newSource = source;

        if (dupObjectNames.size() > 0) {

            final ByteArrayOutputStream srcByteStream =
                                            new ByteArrayOutputStream();
            final JsonGenerator jsonGen = createGenerator(srcByteStream);

            final JsonParser parser = createParser(source);
            parser.disable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

            String startName = null;
            Object startVal = null;
            JsonStreamContext startCtx = null;
            StringBuilder fieldBuf = null;

            JsonToken token = parser.nextToken();
            while (token != null) {

                final String curName = parser.getCurrentName();
                final Object curVal = objectValue(parser);
                final com.fasterxml.jackson.core.JsonStreamContext curCtx =
                          parser.getParsingContext();

                if (token == JsonToken.START_OBJECT) {
                    if (fieldBuf == null) {
                        jsonGen.writeStartObject();
                    }
                } else if (token == JsonToken.END_OBJECT) {
                    if (fieldBuf == null && curName != null) {
                       jsonGen.writeEndObject();
                    }
                    if (curCtx.equals(startCtx)) {
                        startName = null;
                        startVal = null;
                        startCtx = null;
                        fieldBuf = null;
                        token = parser.nextToken();
                        if (token != null) {
                            startName = parser.getCurrentName();
                            startVal = objectValue(parser);
                            startCtx = parser.getParsingContext();
                            if (DEBUG_ON) {
                                System.out.println("startName = " + startName +
                                    ", startValue = " + startVal +
                                    ", startContext = " + startCtx);
                            }
                        }
                        continue;
                    }
                } else if (token == JsonToken.FIELD_NAME) {
                    if (fieldBuf != null) {
                        if (curName != null && !curName.equals(startName)) {
                            fieldBuf.append("." + curName);
                        }
                    } else {
                        /* fieldBuf == null && dups ==> start of field */
                        if (dupObjectNames.contains(curName)) {
                            startName = curName;
                            startVal = curVal;
                            startCtx = curCtx;
                            fieldBuf = new StringBuilder(startName);
                        } else {
                            /* no dups ==> just write field name */
                            jsonGen.writeFieldName(curName);
                        }
                    }
                } else if (token == JsonToken.VALUE_STRING) {
                    if (fieldBuf != null) {
                        jsonGen.writeFieldName(fieldBuf.toString());
                    }
                    jsonGen.writeString(parser.getText());
                } else if (token == JsonToken.VALUE_NUMBER_INT) {
                    if (fieldBuf != null) {
                        jsonGen.writeFieldName(fieldBuf.toString());
                    }
                    jsonGen.writeNumber(parser.getIntValue());
                } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                    if (fieldBuf != null) {
                        jsonGen.writeFieldName(fieldBuf.toString());
                    }
                    jsonGen.writeNumber(parser.getFloatValue());
                } else if (token == JsonToken.VALUE_TRUE) {
                    if (fieldBuf != null) {
                        jsonGen.writeFieldName(fieldBuf.toString());
                    }
                    jsonGen.writeBoolean(Boolean.TRUE);
                } else if (token == JsonToken.VALUE_FALSE) {
                    if (fieldBuf != null) {
                        jsonGen.writeFieldName(fieldBuf.toString());
                    }
                    jsonGen.writeBoolean(Boolean.FALSE);
                } else if (token == JsonToken.VALUE_NULL) {
                    if (fieldBuf != null) {
                        jsonGen.writeFieldName(fieldBuf.toString());
                    }
                    jsonGen.writeNull();
                } else if (token == JsonToken.START_ARRAY) {
                    if (fieldBuf != null) {
                        jsonGen.writeFieldName(fieldBuf.toString());
                    }
                    jsonGen.writeStartArray();
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        jsonGen.writeObject(objectValue(parser));
                    }
                    jsonGen.writeEndArray();
                } else {
                    if (fieldBuf != null) {
                        jsonGen.writeFieldName(fieldBuf.toString());
                    }
                    jsonGen.writeString(parser.getText());
                }
                token = parser.nextToken();
            }
            jsonGen.writeEndObject();

            jsonGen.flush();
            newSource = srcByteStream.toByteArray();

        }

        /*
         * At this point any duplicates in the original source
         * should have been 'flattened'/'collapsed' and placed
         * in the newSource byte array. So there should be no
         * need to disable STRICT_DUPLICATE_DETECTION in the
         * parser created from newSource and passed to parseAsMap.
         */
        return parseAsMap(createParser(newSource));
    }

    public static JsonGenerator map(Map<String, String> values)
        throws IOException {
        if (values == null) {
            return null;
        }

        final JsonGenerator jsonGen =
            createGenerator(new ByteArrayOutputStream());

        jsonGen.writeStartObject();
        for (Map.Entry<String, String> value : values.entrySet()) {
            jsonGen.writeFieldName(value.getKey());
            jsonGen.writeString(value.getValue());
        }
        jsonGen.writeEndObject();
        jsonGen.flush();
        return jsonGen;
    }

    @SuppressWarnings("unchecked")
    public static JsonGenerator mapToJson(Map<String, Object> values)
        throws IOException {
        if (values == null) {
            return null;
        }

        final JsonGenerator jsonGen =
            createGenerator(new ByteArrayOutputStream());

        jsonGen.writeStartObject();
        for (Map.Entry<String, Object> value : values.entrySet()) {
            Object jsonValue = value.getValue();
            if (jsonValue instanceof Map) {
                jsonGen.writeFieldName(value.getKey());
                mapToJson((Map<String, Object>) jsonValue, jsonGen);
            } else if (jsonValue instanceof String){
                jsonGen.writeStringField(value.getKey(), (String) jsonValue);
            } else {
                throw new IOException("Invalid json value type , " + 
                        "only supported Map/String");
            }
        }
        jsonGen.writeEndObject();
        jsonGen.flush();
        return jsonGen;
    }

    @SuppressWarnings("unchecked")
    private static void mapToJson(Map<String, Object> values,
        JsonGenerator jsonGen) throws IOException {
        jsonGen.writeStartObject();
        for (Map.Entry<String, Object> value : values.entrySet()) {
            Object jsonValue = value.getValue();
            if (jsonValue instanceof Map) {
                jsonGen.writeFieldName(value.getKey());
                mapToJson((Map<String, Object>) jsonValue, jsonGen);
            } else if (jsonValue instanceof String){
                jsonGen.writeStringField(value.getKey(), (String) jsonValue);
            } else {
                throw new IOException("Invalid json value type , " + 
                          "only supported Map/String");
            }
        }
        jsonGen.writeEndObject();
    }

    public static JsonParser createParser(InputStream in)
        throws JsonParseException, IOException {
        final JsonParser parser = jsonFactory.createParser(in);
        return parser;
    }

    public static JsonParser createParser(byte[] b)
        throws JsonParseException, IOException {
        return jsonFactory.createParser(b);
    }

    public static JsonParser createParser(String jsonString)
        throws JsonParseException, IOException {
        final JsonParser parser = jsonFactory.createParser(jsonString);
        return parser;
    }

    public static boolean isEmptyJsonStr(String jsonString)
        throws JsonParseException, IOException {
        if (jsonString == null || jsonString.length() == 0) {
            return true;
        }
        final JsonParser parser = jsonFactory.createParser(jsonString);
        JsonToken token = parser.getCurrentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == JsonToken.START_OBJECT &&
                parser.nextToken() == JsonToken.END_OBJECT) {
            return true;
        }
        return false;
    }

    public static void validateToken(
                                     JsonToken expectedToken,
                                     JsonToken token,
                                     JsonParser parser)
        throws JsonParseException {

        if (token != expectedToken) {
            final String message =
                "Failed to parse object: expecting token of" +
                " type [%s] but found [%s]";
            throw new JsonParseException(parser,
                                         String.format(Locale.ROOT, message,
                                                       expectedToken, token));
        }

    }

    public static void validateFieldName(
                                         JsonParser parser,
                                         JsonToken token,
                                         String fieldName)
        throws IOException {
        validateToken(JsonToken.FIELD_NAME, token, parser);
        final String currentName = parser.getCurrentName();
        if (!currentName.equals(fieldName)) {
            final String msg =
                "Parse error: expected field : [%s] actual field: [%s]";
            throw new JsonParseException(parser,
                                         String.format(Locale.ROOT, msg,
                                                       fieldName,
                                                       currentName));
        }
    }

    public static String toStringUTF8Bytes(byte[] source) {
        final Charset utf8 = StandardCharsets.UTF_8;
        final char[] cBuf = new char[source.length];
        ByteArrayInputStream bis = null;
        try {
            bis = new ByteArrayInputStream(source);
            final Reader reader = new InputStreamReader(bis, utf8);
            reader.read(cBuf, 0, source.length);
        } catch (IOException e) {
            return null;

        } finally {
            try {
                if (bis != null) {
                    bis.close();
                }
            } catch (IOException e) /* CHECKSTYLE:OFF */ {
                /* stream might be already closed. */
            } /* CHECKSTYLE:ON */
        }

        return new String(cBuf);
    }

    /**
     * Not for complex maps with array type values. Only parses
     * string,scalarValue as map OR {@literal string,Map<string,ScalarValue>}
     * OR any nested maps of the same form. {@literal
     * string,array<scalarValue>} is not supported by this method.
     */
    public static Map<String, Object> parseAsMap(JsonParser parser)
        throws IOException {
        final Map<String, Object> map = new HashMap<String, Object>();
        JsonToken token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == JsonToken.START_OBJECT) {
            token = parser.nextToken();
        }
        for (; token == JsonToken.FIELD_NAME; token = parser.nextToken()) {
            /* Must point to field name */
            final String fieldName = parser.getCurrentName();
            Object value = null;
            /* And then the value... */
            token = parser.nextToken();
            if (token == JsonToken.START_OBJECT) {
                value = parseAsMap(parser);
            } else if (token == JsonToken.START_ARRAY) {
                final List<Object> listVal = new ArrayList<Object>();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    listVal.add(objectValue(parser));
                }
                value = listVal;
            } else {
                value = objectValue(parser);
            }
            map.put(fieldName, value);
        }

        return map;
    }

    public static Object objectValue(JsonParser parser) throws IOException {
        final JsonToken currentToken = parser.getCurrentToken();
        if (currentToken == JsonToken.VALUE_STRING) {
            return parser.getText();
        } else if (currentToken == JsonToken.VALUE_NUMBER_INT ||
                currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
            return parser.getNumberValue();
        } else if (currentToken == JsonToken.VALUE_TRUE) {
            return Boolean.TRUE;
        } else if (currentToken == JsonToken.VALUE_FALSE) {
            return Boolean.FALSE;
        } else if (currentToken == JsonToken.VALUE_NULL) {
            return null;
        } else {
            return parser.getText();
        }
    }

    public static Map<String, String> parseFailedHttpResponse(
        final String httpResponseStr) throws IOException {

        final Map<String, String> retMap = new HashMap<>();

        if (httpResponseStr == null) {
            return retMap;
        }

        final JsonParser parser = createParser(httpResponseStr);

        JsonToken token = parser.nextToken();
        while (token != null) {

            String curFieldName = null;
            String curFieldVal = null;

            if (token.isScalarValue()) {

                curFieldName = parser.getCurrentName();

                if (JsonToken.VALUE_STRING == token) {
                    curFieldVal = parser.getText();
                } else if (JsonToken.VALUE_NUMBER_INT == token ||
                           JsonToken.VALUE_NUMBER_FLOAT == token) {
                    curFieldVal = parser.getNumberValue().toString();
                } else if (JsonToken.VALUE_TRUE == token) {
                    curFieldVal = "true";
                } else if (JsonToken.VALUE_FALSE == token) {
                    curFieldVal = "true";
                } else {
                    final Object objVal = parser.getCurrentValue();
                    if (objVal != null) {
                        curFieldVal = parser.getCurrentValue().toString();
                    }
                }
                retMap.put(curFieldName, curFieldVal);
            }
            token = parser.nextToken();
        }
        return retMap;
    }
}
