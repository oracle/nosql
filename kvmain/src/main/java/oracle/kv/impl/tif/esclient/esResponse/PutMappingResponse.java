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

import com.fasterxml.jackson.core.JsonParser;

public class PutMappingResponse extends AcknowledgeResponse implements
        JsonResponseObjectMapper<PutMappingResponse> {

    public PutMappingResponse() {

    }

    public PutMappingResponse(boolean acknowledged) {
        super(acknowledged);
    }

    @Override
    public PutMappingResponse acknowledged(boolean acknowledged) {
        super.acknowledged(acknowledged);
        return this;
    }

    @Override
    public PutMappingResponse buildFromJson(JsonParser parser)
        throws IOException {
        super.buildAcknowledgeResponse(parser);
        return this;
    }

    @Override
    public PutMappingResponse buildErrorReponse(ESException e) {
        return null;
    }

    @Override
    public PutMappingResponse buildFromRestResponse(RestResponse restResp) {
        return null;
    }

}
