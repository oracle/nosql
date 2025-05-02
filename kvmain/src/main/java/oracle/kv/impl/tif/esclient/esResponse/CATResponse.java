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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class CATResponse extends ESResponse
        implements JsonResponseObjectMapper<CATResponse> {

    private List<Map<String, Object>> responseMaps =
        new ArrayList<Map<String, Object>>();
    private boolean exists = false;
    private boolean error = false;

    public CATResponse() {

    }

    public List<Map<String, Object>> getResponseMaps() {
        return responseMaps;
    }

    public Set<String> getIndices() {
        Set<String> indices = new HashSet<String>();
        for (Map<String, Object> map : responseMaps) {
            indices.add((String) map.get("index"));
        }
        return indices;
    }
    
    /*
     * FTS index names are all lower cased.
     * Caller should make sure that index names
     * are in lower case.
     */
    public Map<String, Long> getCounts(Set<String> indices ) {
        Map<String, Long> counts = new HashMap<String,Long>();
        for (Map<String, Object> map : responseMaps) {
            String index = (String) map.get("index");
            if(indices.contains(map.get("index"))) {
                Object countObj = map.get("docs.count");
                Long count = null;
                if (countObj instanceof String) {
                    count = Long.parseLong((String) countObj);
                } else {
                    count = (Long) countObj;
                }
                
                counts.put(index, count);
            }
        }
        return counts;
    }

    @Override
    public CATResponse buildFromJson(JsonParser parser) throws IOException {
        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_ARRAY, token, parser);
        exists = true;

        while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
            if (token == JsonToken.START_OBJECT) {
                Map<String, Object> map = ESJsonUtil.parseAsMap(parser);
                responseMaps.add(map);
            }

        }

        parsed(true);
        return this;

    }

    @Override
    public CATResponse buildErrorReponse(ESException e) {

        if (e.errorStatus() == RestStatus.NOT_FOUND) {
            exists = true;
            this.statusCode(RestStatus.NOT_FOUND.getStatus());

        } else if (e.errorType().contains("not_found")) {
            exists = true;
        } else {
            error = true;
        }

        return this;
    }

    public boolean exists() {
        return exists;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    public boolean error() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    @Override
    public CATResponse buildFromRestResponse(RestResponse restResp)
        throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String toString() {
        return responseMaps.toString();
    }

}
