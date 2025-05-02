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

public class ClusterHealthRequest extends ESRequest<ClusterHealthRequest>
        implements ESRestRequestGenerator {

    public ClusterHealthRequest() {

    }

    public ClusterHealthRequest(String index) {
        super(index);
    }

    @Override
    public RestRequest generateRestRequest() {
        String method = ESHttpMethods.HttpGet;

        String endpoint = endpoint(
                                   "_cluster", "health", index());

        RequestParams parameters = new RequestParams();
        parameters.clusterHealthLevel(
                                      "cluster");
        return new RestRequest(method, endpoint, null, parameters.getParams(),
                               null);
    }

    @Override
    public RequestType requestType() {
        return RequestType.CLUSTER_HEALTH;
    }

}
