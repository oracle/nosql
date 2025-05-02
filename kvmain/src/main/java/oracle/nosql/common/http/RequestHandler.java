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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;


/**
 * Instances of this class are used to handle an HTTP request.
 */
public interface RequestHandler {

    /**
     * Handles a request
     *
     * @param request the incoming request
     * @param ctx context used for consolidation of resource allocation
     *
     * The implementing class must return a FullHttpResponse.
     *
     * TODO: can the ctx be avoided?
     */
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx);

    /**
     * Shut down a request handler
     */
    default void shutDown() {}
}
