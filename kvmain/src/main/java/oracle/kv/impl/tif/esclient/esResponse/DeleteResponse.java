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
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class DeleteResponse extends ESWriteResponse
        implements JsonResponseObjectMapper<DeleteResponse> {

    /*
     * It is mostly ESWriteResponse but for the field "found".
     */
    private static final String FOUND = "found";

    /*
     * Delete Response is determined by found. If true, it means document was
     * deleted. However, result field can also be checked.
     */
    private boolean found;
    private boolean error;

    public boolean isFound() {
        return found;
    }

    public DeleteResponse found(boolean found2) {
        this.found = found2;
        return this;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    @Override
    public DeleteResponse buildFromJson(JsonParser parser) throws IOException {
        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String currentFieldName = parser.currentName();

            if (FOUND.equals(currentFieldName)) {
                if (token.isScalarValue()) {
                    found(parser.getBooleanValue());
                }
            } else {
                ESWriteResponse.buildFromJson(parser, this);
            }
        }
        parsed(true);
        return this;
    }

    @Override
    public DeleteResponse buildErrorReponse(ESException e) {
        if (e != null) {
            if (e.errorStatus() == RestStatus.NOT_FOUND) {
                found = false;
                this.statusCode(RestStatus.NOT_FOUND.getStatus());
            }
        } else {
            error = true;
        }

        return this;
    }

    @Override
    public DeleteResponse buildFromRestResponse(RestResponse restResp) {
        return null;
    }

}
