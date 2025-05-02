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

package oracle.kv.impl.tif.esclient.esResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class GetResponse extends ESResponse
        implements JsonResponseObjectMapper<GetResponse> {

    private static final String FOUND = "found";
    private static final String FIELDS = "fields";

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean found;
    private Map<String, SourceField> sourceFields =
        new HashMap<String, SourceField>();
    private Map<String, Object> sourceAsMap;
    private byte[] source;

    public GetResponse() {

    }

    public GetResponse(String index,
            String type,
            String id,
            boolean found,
            byte[] source,
            long version) {

        this.index = index;
        this.id = id;
        this.type = type;
        this.found = found;
        this.source = source;
        this.version = version;

    }

    public String index() {
        return index;
    }

    public GetResponse index(String index1) {
        this.index = index1;
        return this;
    }

    public String type() {
        return type;
    }

    public GetResponse type(String type1) {
        this.type = type1;
        return this;
    }

    public String id() {
        return id;
    }

    public GetResponse id(String id1) {
        this.id = id1;
        return this;
    }

    public boolean isFound() {
        return found;
    }

    public GetResponse found(boolean found1) {
        this.found = found1;
        return this;
    }

    public long version() {
        return version;
    }

    public GetResponse version(long version1) {
        this.version = version1;
        return this;
    }

    public Map<String, SourceField> getSourceFields() {
        return sourceFields;
    }

    public GetResponse sourceFields(Map<String, SourceField> sourceFields1) {
        this.sourceFields = sourceFields1;
        return this;
    }

    public Map<String, Object> sourceAsMap() {
        return sourceAsMap;
    }

    public GetResponse sourceAsMap(Map<String, Object> sourceAsMap1) {
        this.sourceAsMap = sourceAsMap1;
        return this;
    }

    public byte[] source() {
        return source;
    }

    public GetResponse source(byte[] source1) {
        this.source = source1;
        return this;
    }

    /**
     * Get Response with and without _source is given below.
     *
     * parser is positioned right before the start curly braces of Get Response
     * structure.
     *
     * { "_index" : "twitter", "_type" : "type1", "_id" : "1", "_version" : 1,
     * "found" : true, "_source" : { "user" : "aditya", "post_date" :
     * "2009-11-15T14:12:12", "message" : "trying out Elasticsearch" } }
     *
     * with sourceField user. Note that _source is not there in response. And
     * field value is an array.
     *
     *
     * { "_index" : "twitter", "_type" : "type1", "_id" : "1", "_version" : 1,
     * "found" : true, "fields" : { "user" : [ "aditya" ] }
     */
    @Override
    public GetResponse buildFromJson(JsonParser parser) throws IOException {

        /*
         * Now that the response sent from elasticsearch may consist of
         * complex, multi-object json content, STRICT_DUPLICATE_DETECTION
         * must be disabled in the given parser; otherwise, a
         * JsonParseException (which is an IOException) will be enountered,
         * and failure will occur.
         */
        parser.disable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
        token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.FIELD_NAME, token, parser);
        String currentFieldName = parser.currentName();

        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {

            if (token == null) {
                break;
            }

            if (token == JsonToken.FIELD_NAME) {

                currentFieldName = parser.currentName();

            } else if (token.isScalarValue()) {

                if (_INDEX.equals(currentFieldName)) {
                    index = parser.getText();
                } else if (_TYPE.equals(currentFieldName)) {
                    type = parser.getText();
                } else if (_ID.equals(currentFieldName)) {
                    id = parser.getText();
                } else if (_VERSION.equals(currentFieldName)) {
                    version = parser.getLongValue();
                } else if (FOUND.equals(currentFieldName)) {
                    found(parser.getBooleanValue());
                } else {
                    sourceFields.put(currentFieldName,
                                     new SourceField
                                         (currentFieldName,
                                          Collections
                                          .singletonList(parser.getText())));
                }

            } else if (token == JsonToken.START_OBJECT) {

                if ("_source".equals(currentFieldName)) {

                    final ByteArrayOutputStream srcByteStream =
                        new ByteArrayOutputStream();
                    final JsonGenerator jsonGen =
                        ESJsonUtil.createGenerator(srcByteStream);

                    try {
                        jsonGen.copyCurrentStructure(parser);
                        jsonGen.flush();
                        source = srcByteStream.toByteArray();
                        sourceAsMap =
                            ESJsonUtil.convertToMapCollapseDuplicates(source);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw e;
                    } finally {
                        jsonGen.close(); // TODO: JsonFactory configuration for
                                         // autoclose?
                    }

                } else if (FIELDS.equals(currentFieldName)) {

                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        ESJsonUtil.validateToken(JsonToken.FIELD_NAME,
                                                 parser.currentToken(),
                                                 parser);
                        ESJsonUtil.validateToken(JsonToken.START_ARRAY,
                                                 parser.nextToken(), parser);
                        final SourceField srcField =
                            new SourceField(parser.currentName(),
                                            new ArrayList<Object>());
                        sourceFields.put(parser.currentName(), srcField);
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            srcField.values()
                                    .add(ESJsonUtil.objectValue(parser));
                        }
                    }
                }
            }
        }
        parsed(true);
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("GetResponse{");
        builder.append("found=").append(isFound());
        builder.append(",index=").append(index());
        builder.append(",type=").append(type());
        builder.append(",id=").append(id());
        builder.append(",version=").append(version());
        return builder.append("}").toString();

    }

    @Override
    public GetResponse buildErrorReponse(ESException e) {
        return null;
    }

    @Override
    public GetResponse buildFromRestResponse(RestResponse restResp)
        throws IOException {

        /* Cannot return null. See SR27090. */
        statusCode(restResp.getStatusCode());
        if (statusCode() == RestStatus.OK.getStatus()) {
            found = true;
        }
        parsed(true);
        return this;
    }
}
