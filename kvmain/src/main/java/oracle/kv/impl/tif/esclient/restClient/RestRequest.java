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

import java.util.Map;

public class RestRequest {

    private final String method;
    private final String endpoint;
    private final byte[] entity;
    private final Map<String, String> params;
    private final Map<String, String> headers;

    public RestRequest(String method,
            String endpoint,
            byte[] entity,
            Map<String, String> params,
            Map<String, String> headers) {
        this.method = method;
        this.endpoint = endpoint;
        this.entity = entity;
        this.params = params;
        this.headers = headers;
    }

    public String method() {
        return method;
    }

    public String endpoint() {
        return endpoint;
    }

    public byte[] entity() {
        return entity;
    }

    public Map<String, String> params() {
        return params;
    }

    public Map<String, String> headers() {
        return headers;
    }

    @Override
    public String toString() {
        /* logging user data (entity) is not allowed  */
        String s = "Request {" + "headers= " + headers + " method= '" + method
            + '\'' + ", endpoint= '"
                + endpoint + '\'' + ", params= " + params ;
        s += " }";
        return s;
    }
}
