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

import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;

public class DeleteIndexResponse extends AcknowledgeResponse
        implements JsonResponseObjectMapper<DeleteIndexResponse> {

    /*
     * If index did not exists, the reponse will come via error response.
     */
    boolean exists = true;

    /*
     * Error response may not actually mean error, this field is the actual
     * indicator of error.
     */
    boolean error = false;

    public DeleteIndexResponse() {

    }

    public DeleteIndexResponse(boolean acknowledged) {
        super(acknowledged);
    }

    @Override
    public DeleteIndexResponse acknowledged(boolean acknowledged) {
        super.acknowledged(acknowledged);
        return this;
    }

    public boolean exists() {
        return exists;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    /**
     * Builds the acknowlege response. The parser should be positioned before
     * the start of curly braces.
     */
    @Override
    public DeleteIndexResponse buildFromJson(JsonParser parser)
        throws IOException {
        super.buildAcknowledgeResponse(parser);
        return this;
    }

    @Override
    public DeleteIndexResponse buildErrorReponse(ESException e) {
        if (e != null) {
            if (e.errorStatus() == RestStatus.NOT_FOUND) {
                exists = false;
                this.statusCode(RestStatus.NOT_FOUND.getStatus());
            }
        } else {
            error = true;
        }

        return this;
    }

    @Override
    public DeleteIndexResponse buildFromRestResponse(RestResponse restResp) {
        // TODO Auto-generated method stub
        return null;
    }

}
