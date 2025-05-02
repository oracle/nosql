/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.http;

import oracle.nosql.common.contextlogger.LogContext;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

/**
 * An interface for a web-based Service. Service instances are registered
 * with the ProxyRequestHandler and looked up by URI path.
 *
 * The handleRequest method returns FullHttpResponse to allow the services
 * more control over the response headers and content. There is relatively
 * little shared response code among services.
 */
public interface Service {

    /**
     * Handles a request
     *
     * TODO: it'd be nice to abstract out HTTP 1.1 vs HTTP 2 but the Netty
     * objects involved are different. When we want to handle both, think about
     * better abstractions. Maybe just add an HTTP 2-based handleRequest method.
     */
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc);

    /**
     * Returns true if the uri/path indicates this service
     *
     * TODO: should the method be passed as well? For now, the service will
     * deal with support of specific methods or not.
     */
    public boolean lookupService(String uri);

    /**
     * Called when the response is about to be passed back to Netty
     */
    default FullHttpResponse onResponse(FullHttpResponse response,
                                        ChannelHandlerContext ctx) {
        return response;
    }

    /**
     * Allows a service to clean up resources on server shutdown
     */
    default void shutDown() {}
}
