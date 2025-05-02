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

public class MappingExistResponse extends ESResponse implements
        JsonResponseObjectMapper<MappingExistResponse> {

    private boolean exists = false;
    private boolean error = false;

    public MappingExistResponse() {

    }

    public boolean exists() {
        return exists;
    }

    public MappingExistResponse exist(boolean exist) {
        this.exists = exist;
        return this;
    }

    public boolean isError() {
        return error;
    }

    public MappingExistResponse error(boolean error1) {
        this.error = error1;
        return this;
    }

    @Override
    public MappingExistResponse buildFromJson(JsonParser parser)
        throws IOException {
        return null;
    }

    /**
     * A notFound Response which is a valid response,
     * can come as error as well.
     * This makes sure that the error is a valid error.
     */
    @Override
    public MappingExistResponse buildErrorReponse(ESException e) {
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
    public MappingExistResponse buildFromRestResponse(RestResponse restResp)
        throws IOException {
        if (restResp.getStatusCode() == 200) {
            exists = true;
        }
        parsed(true);
        return this;
    }

}
