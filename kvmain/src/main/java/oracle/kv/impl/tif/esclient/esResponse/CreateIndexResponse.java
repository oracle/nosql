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

public class CreateIndexResponse extends AcknowledgeResponse
        implements JsonResponseObjectMapper<CreateIndexResponse> {

    private static final String ALREADY_EXISTS_EXCEPTION =
        "already_exists_exception";

    private boolean alreadyExists = false;
    private boolean error = false;

    public CreateIndexResponse() {

    }

    public CreateIndexResponse(boolean acknowledged) {
        super(acknowledged);
    }

    @Override
    public CreateIndexResponse acknowledged(boolean acknowledged) {
        super.acknowledged(acknowledged);
        return this;
    }

    public boolean alreadyExists() {
        return alreadyExists;
    }

    public void setAlreadyExists(boolean alreadyExists) {
        this.alreadyExists = alreadyExists;
    }

    public boolean isError() {
        return error;
    }

    public void error(boolean error1) {
        this.error = error1;
    }

    /**
     * Builds the acknowlege response. The parser should be positioned before
     * the start of curly braces.
     */
    @Override
    public CreateIndexResponse buildFromJson(JsonParser parser)
        throws IOException {
        super.buildAcknowledgeResponse(parser);
        return this;
    }

    @Override
    public CreateIndexResponse buildErrorReponse(ESException e) {
        if (e != null) {
            if (e.errorStatus() == RestStatus.BAD_REQUEST &&
                    e.errorType().contains(ALREADY_EXISTS_EXCEPTION)) {
                alreadyExists = true;
                this.statusCode(RestStatus.BAD_REQUEST.getStatus());
            } else {
                error = true;
            }
        }

        return this;
    }

    @Override
    public CreateIndexResponse buildFromRestResponse(RestResponse restResp) {
        return null;
    }

}
