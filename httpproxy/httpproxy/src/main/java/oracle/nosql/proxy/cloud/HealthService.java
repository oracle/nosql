/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.cloud;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static oracle.nosql.common.http.Constants.CONNECTION;
import static oracle.nosql.common.http.Constants.CONTENT_LENGTH;
import static oracle.nosql.common.http.Constants.CONTENT_TYPE;
import static oracle.nosql.proxy.protocol.HttpConstants.APPLICATION_JSON;
import static oracle.nosql.proxy.protocol.HttpConstants.HEALTH_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.pathInURIAllVersions;

import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.Service;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.health.HealthStatus;
import oracle.nosql.health.HealthStatus.Beacon;
import oracle.nosql.proxy.util.ShutdownManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
/**
 * .../health/...
 * GET /V0/tools/health
 */
public class HealthService implements Service {

    private final SkLogger logger;

    public HealthService(SkLogger logger) {
        this.logger = logger;
    }

    private void sendErrorResponse(ChannelHandlerContext ctx) {
        /* respond with error */
        FullHttpResponse resp =
            new DefaultFullHttpResponse(HTTP_1_1, NOT_IMPLEMENTED);
        /* set zero content and connection: close */
        resp.headers().setInt(CONTENT_LENGTH, 0)
                      .set(CONNECTION, "close");
        /* write response to channel */
        final ChannelFuture f = ctx.writeAndFlush(resp);
        /* when response is sent, close connection */
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                ctx.close();
            }
        });
    }

    /**
     * Handles a request
     */
    @Override
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc) {

        /* if we are in graceful shutdown mode, return error */
        if (ShutdownManager.getInstance(logger).inShutdown()) {
            /* respond with error */
            sendErrorResponse(ctx);
            logger.fine("Proxy health service in shutdown: returning error",
                        lc);
            /* skip default server processing */
            return null;
        }

        /* TODO: create an actual payload based on spec */
        ByteBuf content = ctx.alloc().directBuffer();

        HealthStatus hs = new HealthStatus(Beacon.GREEN, "ALL OK");
        content.writeCharSequence(hs.toJsonString(), UTF_8);

        logger.fine("Proxy health service request on channel: " + ctx, lc);

        FullHttpResponse resp =
            new DefaultFullHttpResponse(HTTP_1_1, OK, content);
        resp.headers().set(CONTENT_TYPE, APPLICATION_JSON)
            .setInt(CONTENT_LENGTH, content.readableBytes());
        return resp;
    }

    /**
     * Returns true if the path indicates this service
     */
    @Override
    public boolean lookupService(String uri) {
        return pathInURIAllVersions(uri, HEALTH_PATH);
    }
}
