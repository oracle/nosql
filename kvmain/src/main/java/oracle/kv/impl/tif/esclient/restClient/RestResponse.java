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

package oracle.kv.impl.tif.esclient.restClient;

import java.net.http.HttpResponse;
import java.util.Objects;

import oracle.kv.impl.tif.esclient.httpClient.HttpHost;

public class RestResponse {

    private final HttpHost host;
    private final HttpResponse<String> response;
    /*
     * This is set to true if statusCode < 300.
     * For the status 200 OK, ESResponse statusCode is to be used.
     */
    private boolean success = false;
    private boolean retriable = false;

    public RestResponse(HttpHost host, HttpResponse<String> response) {
        Objects.requireNonNull(host, "node cannot be null");
        Objects.requireNonNull(response, "response cannot be null");
        this.host = host;
        this.response = response;
    }

    public boolean isSuccess() {
        return success;
    }

    public void success(boolean success1) {
        this.success = success1;
    }

    public boolean isRetriable() {
        return retriable;
    }

    public void retriable(boolean retriable1) {
        this.retriable = retriable1;
    }

    /**
     * Returns the node that returned this response
     */
    public HttpHost host() {
        return host;
    }

    public int getStatusCode() {
        return response.statusCode();
    }
     
    /**
     * Returns the value of the first header with a specified name of
     * this message. If there is more than one matching header in the
     * message the first element is returned. If there is no matching
     * header in the message <code>null</code> is returned.
     */
    public String getHeader(String name) {
        if (response.headers().firstValue(name).isPresent()) {
            return response.headers().firstValue(name).get();
        }
        return null;
    }

    public HttpResponse<String> httpResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "Response {" + " host=" + host
               + ", response=" + response.statusCode() + " }";
    }
}
