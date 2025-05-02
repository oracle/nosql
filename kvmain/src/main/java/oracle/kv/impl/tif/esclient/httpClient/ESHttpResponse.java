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

package oracle.kv.impl.tif.esclient.httpClient;

import java.util.Objects;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ESHttpResponse {

    private final HttpRequest requestLine;
    private final HttpHost host;
    private final HttpResponse<String> response;

    ESHttpResponse(HttpHost host, HttpResponse<String> response) {
        Objects.requireNonNull(host, "node cannot be null");
        Objects.requireNonNull(response, "response cannot be null");
        this.host = host;
        this.response = response;
        this.requestLine = response.request();
    }

    /**
     * Returns the request line that generated this response
     */
    public HttpRequest getHttpRequest() {
        return requestLine;
    }

    public int getStatusCode() {
        return response.statusCode();
    }

    public String getHeader(String name) {
        if (response.headers().firstValue(name).isPresent()) {
            return response.headers().firstValue(name).get();
        }
        return null;
    }

    /**
     * Returns the node that returned this response
     */
    public HttpHost getHost() {
        return host;
    }

    HttpResponse<String> getHttpResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "Response{" + "requestLine=" + requestLine + ", host=" + host
                + ", response=" + getStatusCode() + '}';
    }

}
