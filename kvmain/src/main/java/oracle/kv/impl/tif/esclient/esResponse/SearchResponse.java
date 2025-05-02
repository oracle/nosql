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
import java.util.List;
import java.util.Map;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class SearchResponse extends ESResponse
        implements JsonResponseObjectMapper<SearchResponse> {

    private static final String TOOK = "took";
    private static final String TIMED_OUT = "timed_out";
    private static final String _SHARDS = "_shards";
    private static final String HITS = "hits";

    private int took;
    private boolean timed_out;
    private Hits hits = new Hits();
    private ShardInfo shardInfo = new ShardInfo();

    public SearchResponse() {

    }

    public SearchResponse(int took, boolean timed_out, Hits hits) {
        this.took = took;
        this.timed_out = timed_out;
        this.hits = hits;
    }

    public int took() {
        return took;
    }

    public SearchResponse took(int took1) {
        this.took = took1;
        return this;
    }

    public boolean timed_out() {
        return timed_out;
    }

    public SearchResponse timed_out(boolean timed_out1) {
        this.timed_out = timed_out1;
        return this;
    }

    public Hits hits() {
        return hits;
    }

    public SearchResponse hits(Hits hits1) {
        this.hits = hits1;
        return this;
    }

    public SearchResponse shardInfo(ShardInfo shardInfo1) {
        this.shardInfo = shardInfo1;
        return this;
    }

    public ShardInfo shardInfo() {
        return shardInfo;
    }

    /**
     * Builds Search Response from the response Json in the HttpEntity of the
     * HttpResponse.
     * 
     * parser is positioned right before the start curly braces.
     * 
     */
    @Override
    public SearchResponse buildFromJson(JsonParser parser) throws IOException {

        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
        token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.FIELD_NAME, token, parser);
        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.currentName();
                continue;
            }
            if (token.isScalarValue()) {
                if (TOOK.equals(currentFieldName)) {
                    took = parser.getIntValue();
                } else if (TIMED_OUT.equals(currentFieldName)) {
                    timed_out = parser.getBooleanValue();
                }
            } else if (HITS.equals(currentFieldName)) {
                hits.buildFromJson(parser);
            } else if (_SHARDS.equals(currentFieldName)) {
                shardInfo = new ShardInfo();
                shardInfo.buildFromJson(parser);
            } else {
                throw new IOException(new InvalidResponseException
                                          ("Unknown Field: " +
                                           currentFieldName +
                                           " at:" +
                                           parser.currentTokenLocation()));
            }
        }
        parsed(true);
        return this;
    }

    @Override
    public SearchResponse buildErrorReponse(ESException e) {
        return null;
    }

    @Override
    public SearchResponse buildFromRestResponse(RestResponse restResp) {
        return null;
    }

    @Override
    public String toString() {
        return "SearchResponse:[ took:" + took + " timedout:" + timed_out +
                " shards: [ total:" + shardInfo.total() + " successful:" +
                shardInfo.successful() + " failed:" + shardInfo.failed() +
                " ]" + hits;
    }

    public static class Hits extends ESResponse
            implements JsonResponseObjectMapper<Hits> {

        private static final String TOTAL = "total";
        private static final String MAX_SCORE = "max_score";
        private static final String HITS_HITS = "hits";
        private static final String RELATION = "relation";
        private static final String TOTALVALUE = "value";

        private int totalValue = -1;
        private String relation = "";
        private float max_score = -1.0f;
        private List<HitItem> hitItems = new ArrayList<HitItem>();

        public Hits() {

        }

        public Hits(int totalValue, int max_score, List<HitItem> hitItems) {
            this.totalValue = totalValue;
            this.max_score = max_score;
            this.hitItems = hitItems;
        }

        public String getRelation() {
            return relation;
        }

        public int totalValue() {
            return totalValue;
        }

        public Hits totalValue(int total1Value) {
            this.totalValue = total1Value;
            return this;
        }

        public float max_score() {
            return max_score;
        }

        public Hits max_score(float max_score1) {
            this.max_score = max_score1;
            return this;
        }

        public List<HitItem> hitItems() {
            return hitItems;
        }

        public Hits hitItems(List<HitItem> hitItems1) {
            this.hitItems = hitItems1;
            return this;
        }

        @Override
        public String toString() {
            return "hits:[total:" + totalValue + " max_score:" + max_score +
                    hitItems + "]";
        }

        /**
         * Build the structure "hits" in the search response. The parser is at
         * the start curly braces after fieldName "hits" has been consumed by
         * the caller code.
         */
        @Override
        public Hits buildFromJson(JsonParser parser) throws IOException {
            JsonToken token = parser.currentToken();

            ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
            token = parser.nextToken();
            ESJsonUtil.validateToken(JsonToken.FIELD_NAME, token, parser);
            String currentFieldName = parser.currentName();
            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    continue;
                }
                if (token.isScalarValue()) {
                    if (MAX_SCORE.equals(currentFieldName)) {
                        if (token != JsonToken.VALUE_NULL) {
                            max_score = parser.getFloatValue();
                        } else {
                            max_score = 0.0f;
                        }
                    }
                } else if (TOTAL.equals(currentFieldName)) {
                    ESJsonUtil.validateToken(JsonToken.START_OBJECT, token,
                            parser);
                    while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                        if (token == JsonToken.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                            continue;
                        }
                        if (token.isScalarValue()) {
                            if (TOTALVALUE.equals(currentFieldName)) {
                                totalValue = parser.getIntValue();
                            }
                            else if (RELATION.equals(currentFieldName)) {
                                relation = parser.getText();
                            }
                        }
                    }
                } else if (HITS_HITS.equals(currentFieldName)) {
                    ESJsonUtil.validateToken(JsonToken.START_ARRAY, token,
                            parser);
                    while ((token =
                                parser.nextToken()) != JsonToken.END_ARRAY) {
                        HitItem hit = new HitItem();
                        hitItems.add(hit.buildFromJson(parser));
                    }
                }
            }
            return this;
        }

        @Override
        public Hits buildErrorReponse(ESException e) {
            return null;
        }

        @Override
        public Hits buildFromRestResponse(RestResponse restResp) {
            return null;
        }

        public static class HitItem extends ESResponse
                implements JsonResponseObjectMapper<HitItem> {

            private static final String FIELDS = "fields";
            private static final String SOURCE = "_source";
            private static final String SCORE = "_score";

            private String index;
            private String type;
            private String id;
            private float score;
            private Map<String, SourceField> sourceFields =
                new HashMap<String, SourceField>();
            private Map<String, Object> sourceAsMap;
            private byte[] source;

            public HitItem() {

            }

            public HitItem(String index,
                    String type,
                    String id,
                    byte[] source,
                    float score) {

                this.index = index;
                this.id = id;
                this.type = type;
                this.source = source;
                this.score = score;

            }

            public String index() {
                return index;
            }

            public HitItem index(String index1) {
                this.index = index1;
                return this;
            }

            public String type() {
                return type;
            }

            public HitItem type(String type1) {
                this.type = type1;
                return this;
            }

            public String id() {
                return id;
            }

            public HitItem id(String id1) {
                this.id = id1;
                return this;
            }

            public float score() {
                return score;
            }

            public HitItem score(float score1) {
                this.score = score1;
                return this;
            }

            public Map<String, SourceField> getSourceFields() {
                return sourceFields;
            }

            public HitItem
                    sourceFields(Map<String, SourceField> sourceFields1) {
                this.sourceFields = sourceFields1;
                return this;
            }

            public Map<String, Object> sourceAsMap() {
                return sourceAsMap;
            }

            public HitItem sourceAsMap(Map<String, Object> sourceAsMap1) {
                this.sourceAsMap = sourceAsMap1;
                return this;
            }

            public byte[] source() {
                return source;
            }

            public HitItem source(byte[] source1) {
                this.source = source1;
                return this;
            }

            @Override
            public String toString() {
                String s = "hitItem:[index:" + index + " type:" + type +
                        " id:" + id + " score:" + score;

                if (!sourceFields.isEmpty()) {
                    s = s + " fields:[" + sourceFields + "]";
                }

                if (sourceAsMap != null) {
                    s = s + " source:[" + sourceAsMap + "]";
                }

                return s;
            }

            /**
             * Parse an individual hit item. The parser should be positioned at
             * the start curly bracket. Method returns with parser on end curly
             * bracket.
             * 
             */

            @Override
            public HitItem buildFromJson(JsonParser parser)
                throws IOException {
                JsonToken token = parser.currentToken();
                ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
                token = parser.nextToken();
                ESJsonUtil.validateToken(JsonToken.FIELD_NAME, token, parser);
                String currentFieldName = parser.currentName();
                while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                    if (token == JsonToken.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isScalarValue()) {
                        if (_INDEX.equals(currentFieldName)) {
                            index = parser.getText();
                        } else if (_TYPE.equals(currentFieldName)) {
                            type = parser.getText();
                        } else if (_ID.equals(currentFieldName)) {
                            id = parser.getText();
                        } else if (SCORE.equals(currentFieldName)) {
                            if (token != JsonToken.VALUE_NULL) {
                                score = parser.getFloatValue();
                            } else {
                                score = 0.0f;
                            }
                        } else {
                            sourceFields.put(currentFieldName,
                                             new SourceField(currentFieldName,
                                                             Collections.singletonList(parser.getText())));
                        }
                    } else if (token == JsonToken.START_OBJECT) {
                        if (SOURCE.equals(currentFieldName)) {
                            ByteArrayOutputStream srcByteStream =
                                new ByteArrayOutputStream();
                            JsonGenerator jsonGen =
                                ESJsonUtil.createGenerator(srcByteStream);
                            try {
                                jsonGen.copyCurrentStructure(parser);
                                jsonGen.flush();
                                source = srcByteStream.toByteArray();
                                sourceAsMap = ESJsonUtil.convertToMap(source);
                            } finally {
                                jsonGen.close(); // TODO: JsonFactory
                                                 // configuration for
                                                 // autoclose?
                            }
                        } else if (FIELDS.equals(currentFieldName)) {
                            while (parser.nextToken() != JsonToken.END_OBJECT) {
                                ESJsonUtil.validateToken(JsonToken.FIELD_NAME,
                                                         parser.currentToken(),
                                                         parser);
                                ESJsonUtil.validateToken(JsonToken.START_ARRAY,
                                                         parser.nextToken(),
                                                         parser);
                                SourceField srcField =
                                    new SourceField(parser.currentName(),
                                                    new ArrayList<Object>());
                                sourceFields.put(parser.currentName(),
                                                 srcField);
                                while (parser.nextToken() != JsonToken.END_ARRAY) {
                                    srcField.values()
                                            .add(ESJsonUtil.objectValue(parser));
                                }
                            }
                        }
                    }
                }
                return this;
            }

            @Override
            public HitItem buildErrorReponse(ESException e) {
                return null;
            }

            @Override
            public HitItem buildFromRestResponse(RestResponse restResp) {
                return null;
            }

        }

    }

}
