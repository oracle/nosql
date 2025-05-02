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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * This Class is only used in unit tests for getting shards and replicas index
 * settings.
 * 
 * Currently, this does not do complete parsing of the reponse, only the
 * required part.
 *
 */

public class GetIndexResponse extends ESResponse
        implements JsonResponseObjectMapper<GetIndexResponse> {

    private final String SETTINGS = "settings";
    private final String MAPPINGS = "mappings";
    private final String ALIASES = "aliases";

    /* mappings are not parsed -not used currently - will be added in future */
    private Map<String, Object> mappings = new LinkedHashMap<String, Object>();
    private Map<String, Object> settings = new LinkedHashMap<String, Object>();
    /* Not used currently - will be added in future */
    private Map<String, Object> aliases = new LinkedHashMap<String, Object>();
    private String indexName;
    private boolean found = false;

    public boolean isFound() {
        return found;
    }

    public GetIndexResponse found(boolean exists) {
        this.found = exists;
        return this;
    }

    public String indexName() {
        return indexName;
    }

    public GetIndexResponse indexName(String indexName1) {
        this.indexName = indexName1;
        return this;
    }

    public GetIndexResponse() {

    }

    public Map<String, Object> mappings() {
        return mappings;
    }

    public GetIndexResponse mappings(Map<String, Object> mappingSpec) {
        this.mappings = mappingSpec;
        return this;
    }

    public Map<String, Object> settings() {
        return settings;
    }

    public GetIndexResponse settings(Map<String, Object> settings) {
        this.settings = settings;
        return this;
    }

    public Map<String, Object> aliases() {
        return aliases;
    }

    public GetIndexResponse aliases(Map<String, Object> aliases2) {
        this.aliases = aliases2;
        return this;
    }

    /**
     * Currently only fetches settings: #shards and #replicas
     */
    @Override
    public GetIndexResponse buildFromJson(JsonParser parser)
        throws IOException {

        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);

        // Get Index Name:
        ESJsonUtil.validateToken(JsonToken.FIELD_NAME, parser.nextToken(),
                                 parser);
        indexName(parser.currentName());

        ESJsonUtil.validateToken(JsonToken.START_OBJECT, parser.nextToken(),
                                 parser);

        String featureName = null;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                featureName = parser.currentName();
                continue;
            }
            if (MAPPINGS.equals(featureName)) {
                mappings = parseMappings(parser);
            }
            if (SETTINGS.equals(featureName)) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                    if (token == JsonToken.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        if ("index".equals(currentFieldName)) {
                            parser.nextFieldName();
                        }
                        continue;
                    } else if (token.isScalarValue()) {
                        settings.put(currentFieldName, parser.getText());
                    } else {
                        parser.skipChildren();
                    }
                }
            } else if (ALIASES.equals(featureName)) {
                aliases = ESJsonUtil.parseAsMap(parser);
            } else {
                parser.skipChildren();
            }

        }
        found = true;
        parsed(true);
        return this;

    }

    @Override
    public GetIndexResponse buildErrorReponse(ESException e) {
        return null;
    }

    /**
     * Get Mapping Response is built using the http response.
     */
    @Override
    public GetIndexResponse buildFromRestResponse(RestResponse restResp)
        throws IOException {
        statusCode(restResp.getStatusCode());
        if (statusCode() == RestStatus.OK.getStatus()) {
            found = true;
        }
        parsed(true);
        return this;
    }

    @Override
    public String toString() {
        return "GetResponse:[" + "statusCode:" + statusCode() + "  Mapping:" +
                mappings() + "  Settings:" + settings() + " ]";
    }

    @SuppressWarnings("unused")
    private Map<String, Object> parseMappings(JsonParser parser) {
        return null;
    }

}
