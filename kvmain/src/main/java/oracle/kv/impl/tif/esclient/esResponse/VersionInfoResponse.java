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

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class VersionInfoResponse extends ESResponse
        implements JsonResponseObjectMapper<VersionInfoResponse> {

    private static final String CLUSTER_NAME = "cluster_name";
    private static final String VERSION_STRUCT = "version";
    private static final String VERSION_NUMBER = "number";
    private static final String LUCENE_VERSION = "lucene_version";

    private String clusterName;
    private String version;
    private String luceneVersion;

    @Override
    public VersionInfoResponse buildFromJson(JsonParser parser)
        throws IOException {

        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
        String currentFieldName = null;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {

            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.currentName();
                continue;
            } else if (token.isScalarValue()) {

                if (CLUSTER_NAME.equals(currentFieldName)) {
                    clusterName = parser.getText();
                }

            } else if (token == JsonToken.START_OBJECT) {
                if (VERSION_STRUCT.equals(currentFieldName)) {
                    while ((token =
                        parser.nextToken()) != JsonToken.END_OBJECT) {
                        if (token == JsonToken.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                            continue;
                        } else if (token.isScalarValue()) {

                            if (VERSION_NUMBER.equals(currentFieldName)) {
                                version = parser.getText();
                            } else if (LUCENE_VERSION.equals(currentFieldName)) {
                                luceneVersion = parser.getText();
                            }

                        } else if (token.isStructStart()) {
                            parser.skipChildren();
                        }
                    }
                }
            }
        }

        parsed(true);
        return this;
    }

    @Override
    public VersionInfoResponse buildErrorReponse(ESException e) {
        return null;
    }

    @Override
    public VersionInfoResponse buildFromRestResponse(RestResponse restResp)
        throws IOException {
        return null;
    }

    public String clusterName() {
        return clusterName;
    }

    public VersionInfoResponse clusterName(String clusterName1) {
        this.clusterName = clusterName1;
        return this;
    }

    public String version() {
        return version;
    }

    public VersionInfoResponse version(String version1) {
        this.version = version1;
        return this;
    }

    public String luceneVersion() {
        return luceneVersion;
    }

    public VersionInfoResponse luceneVersion(String luceneVersion1) {
        this.luceneVersion = luceneVersion1;
        return this;
    }

}
