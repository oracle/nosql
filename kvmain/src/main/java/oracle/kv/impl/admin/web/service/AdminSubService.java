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

import java.util.Map;

import oracle.kv.impl.admin.web.ResponseHandler;
import oracle.kv.impl.admin.web.WrappedHttpRequest;

/**
 * Define the functionalities for admin web service. A web service will be
 * able to register itself to a registry map, handle the input request from
 * request handler, do the actual execution logic, then return the results
 * through response handler.
 */
public interface AdminSubService {

    /**
     * Register current service into the registry map. The registry map use a
     * pair of string as the key, for example, &lt;key1, key2&gt;. key1 is used to
     * map content in Http request path, key2 is used to map content in Http
     * request payload.
     */
    public void
        registerService(Map<String, Map<String, AdminSubService>> registry);

    /**
     * Actual execution of the service. Input from request will be handled by
     * service, service output will write to the response handler.
     */
    public void executeService(WrappedHttpRequest request,
                               ResponseHandler handler);

    /**
     * Signal the internal service to shutdown
     */
    public void shutdown();
}
