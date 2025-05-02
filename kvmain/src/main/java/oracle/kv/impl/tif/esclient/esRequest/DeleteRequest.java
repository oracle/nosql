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

package oracle.kv.impl.tif.esclient.esRequest;

import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;

public class DeleteRequest extends ESWriteRequest<DeleteRequest> {

    public DeleteRequest() {

    }

    public DeleteRequest(String index, String id) {
        super(index, id);
    }

    @Override
    public String toString() {
        return "{ delete:" + index + " ,id:" + id + " }";
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validateWithId() != null) {
            throw validateWithId();
        }
        String endpoint = endpoint(
                                   index()+ "/_doc", id());

        RequestParams params = new RequestParams();
        // params.routing(routing());
        // params.version(version());
        params.refreshType(
                           refreshType());

        return new RestRequest(ESHttpMethods.HttpDelete, endpoint, null,
                               params.getParams(), null);
    }

    @Override
    public RequestType requestType() {
        return RequestType.DELETE;
    }

}
