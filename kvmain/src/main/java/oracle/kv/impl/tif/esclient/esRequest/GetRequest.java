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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;

public class GetRequest extends ESRequest<GetRequest>
        implements ESRestRequestGenerator {
    private String id;
    private List<String> storedFields;
    private String routing;
    private boolean refresh = false;

    public GetRequest(String index, String id) {
        super(index);
        this.id = id;
    }

    public GetRequest id(String id1) {
        this.id = id1;
        return this;
    }

    public String id() {
        return id;
    }

    public GetRequest routing(String routing1) {
        this.routing = routing1;
        return this;
    }

    public String routing() {
        return routing;
    }

    /**
     * By default refresh is false. Set it to true if a refresh should be
     * executed before Get This is to get the latest values.
     * 
     * Mainly meant for unit test currently.
     */
    public GetRequest refresh(boolean refresh1) {
        this.refresh = refresh1;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    /**
     * By default _source field is returned. It can be overridden by specifying
     * explicitly the stored fields to be returned.
     */
    public GetRequest storedFields(List<String> storedFields1) {
        this.storedFields = storedFields1;
        return this;
    }

    public List<String> storedFields() {
        return this.storedFields;
    }

    @Override
    public String toString() {
        return "{ Get:" + index + " ,id:" + id + " }";
    }

    public InvalidRequestException validate() {
        InvalidRequestException exception = null;
        if (index == null || index.length() <= 0) {
            exception =
                new InvalidRequestException("index name is not provided");
        }
        if (id == null || id.length() <= 0) {
            exception =
                new InvalidRequestException("id is required for deletion.");
        }

        return exception;
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validate() != null) {
            throw validate();
        }
        String id1 = null;
        try {
            id1 = URLEncoder.encode(
                                    id(), StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            id1 = id();
        }
        String endpoint = endpoint(
                                   index()+"/_doc", id1);

        RequestParams params = new RequestParams();
        params.routing(
                       routing);
        params.storedFields(
                            storedFields);
        params.refresh(
                       refresh());
        Boolean fetchSource = Boolean.TRUE;
        if (storedFields != null && storedFields.size() > 0) {
            fetchSource = Boolean.FALSE;
        }
        params.fetchSource(
                           fetchSource);

        return new RestRequest(ESHttpMethods.HttpGet, endpoint, null,
                               params.getParams(), null);
    }

    @Override
    public RequestType requestType() {
        return RequestType.GET;
    }
}
