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

public class GetIndexRequest extends ESRequest<GetIndexRequest>
        implements ESRestRequestGenerator {

    private String featureName;

    public GetIndexRequest(String index) {
        this.index = index;
    }

    public GetIndexRequest(String index, Feature feature) {
        this.index = index;
        this.featureName = feature.featureName();
    }

    public String featureName() {
        return featureName;
    }

    public GetIndexRequest featureName(Feature feature) {
        this.featureName = feature.featureName();
        return this;
    }

    public InvalidRequestException validate() {
        InvalidRequestException exception = null;
        if (index == null || index.length() <= 0) {
            exception =
                new InvalidRequestException("index name is not provided");
        }
        if (featureName == null || featureName.length() <= 0 || !validFeature(
                                                                              featureName)) {
            exception =
                new InvalidRequestException("feature name is not valid");

        }
        return exception;
    }

    private boolean validFeature(String featureName2) {

        for (Feature feature : Feature.values()) {
            String validName = feature.featureName();
            if (validName.equals(
                                 featureName2)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validate() != null) {
            throw validate();
        }
        String method = ESHttpMethods.HttpGet;

        String endpoint = endpoint(
                                   index(), featureName());

        RequestParams parameters = new RequestParams();
        return new RestRequest(method, endpoint,
                               null, parameters.getParams(), null);
    }

    @Override
    public RequestType requestType() {
        return RequestType.GET_INDEX;
    }

    public static enum Feature {
        SETTINGS("_settings"), MAPPINGS("_mappings"), ALIASES("_aliases");
        private final String name;

        Feature(String name) {
            this.name = name;
        }

        public String featureName() {
            return name;
        }

    }
}
