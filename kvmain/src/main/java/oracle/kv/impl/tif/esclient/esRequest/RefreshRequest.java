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

public class RefreshRequest extends ESRequest<RefreshRequest>
        implements ESRestRequestGenerator {

    private String[] indices;

    public RefreshRequest(String... indices) {
        this.indices = indices;
    }

    public InvalidRequestException validate() {
        InvalidRequestException exception = null;
        if (indices == null) {
            exception =
                new InvalidRequestException("index names are not provided");
        }

        return exception;

    }

    @Override
    public RestRequest generateRestRequest() {
        /*
         * TODO: Currently request validation is not being used, as exceptions
         * are handled during response time.
         * 
         * However, for some requests like refresh and clusterhealth with
         * invalid indices or zero length indices, request timesout and is
         * retried.
         * 
         * These cases need to throw exceptions at request creation time.
         */
        if (validate() != null) {
            throw validate();
        }
        String method = ESHttpMethods.HttpPost;
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < indices.length; i++) {
            sb.append(indices[i]);
            if (i < indices.length - 1)
                sb.append(",");
        }

        String endpoint = endpoint(sb.toString(), "_refresh");

        RequestParams parameters = new RequestParams();

        return new RestRequest(method, endpoint, null, parameters.getParams(),
                               null);
    }

    @Override
    public RequestType requestType() {
        return RequestType.REFRESH;
    }
}
