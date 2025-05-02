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

package oracle.kv.impl.admin.web.service;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ALLOW;
import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.admin.web.ResponseHandler;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.handler.codec.http.HttpHeaders;

/**
 * Web service base class to implement the common functions of web service.
 */
public abstract class SubServiceBase implements AdminSubService {

    /*
     * Allowed methods for OPTIONS
     */
    private static final String ALLOWED_METHODS =
        (GET + ", " +
         POST + ", " +
         DELETE + ", " +
         PUT + ", " +
         OPTIONS);

    /*
     * Access control methods for OPTIONS
     */
    private static final String ACCESS_CONTROL_METHODS =
        (GET + ", " +
         POST + ", " +
         DELETE + ", " +
         PUT);

    /*
     * Allowed headers for OPTIONS
     */
    private static final String ALLOWED_HEADERS =
        "Cache-Control, Pragma, Origin, Authorization, " +
        "Content-Type, X-Requested-With";

    protected SkLogger logger;

    public SubServiceBase(SkLogger logger) {
        this.logger = logger;
    }

    /*
     * Retrieve the sub registry map using the specified key. If such entry
     * does not exist, create the sub registry map and return the map.
     */
    protected Map<String, AdminSubService>
        getMapFromRegistry(String key,
                           Map<String, Map<String, AdminSubService>> registry) {
        Map<String, AdminSubService> map = registry.get(key);
        if (map == null) {
            map = new HashMap<String, AdminSubService>();
            registry.put(key, map);
        }
        return map;
    }

    /*
     * Set the common headers for Http response.
     */
    protected void setCommonOptionsHeader(ResponseHandler handler) {
        final HttpHeaders headers = handler.getHeaders();
        headers.set(ALLOW, ALLOWED_METHODS);
        headers.set(ACCESS_CONTROL_ALLOW_HEADERS, ALLOWED_HEADERS);
        headers.set(ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_METHODS);
        headers.set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    }

    @Override
    public void shutdown() {
        /* By default there is nothing to do for shutdown the sub service */
    }
}
