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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpDecoderConfig;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.ReadTimeoutHandler;
import oracle.nosql.common.sklogger.SkLogger;

final class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
    private static final String CODEC_HANDLER_NAME = "http-codec";
    private static final String AGG_HANDLER_NAME = "http-aggregator";
    private static final String HTTP_HANDLER_NAME = "http-server-handler";
    private static final String READ_TIMEOUT_HANDLER_NAME =
        "http-read-timeout-handler";

    private final HttpServerHandler handler;
    private final int maxChunkSize;
    private final int maxRequestSize;
    private final int idleReadTimeout;
    private final SkLogger logger;
    private final SslContext sslCtx;

    public HttpServerInitializer(HttpServerHandler handler,
                                 HttpServer server,
                                 SslContext sslCtx,
                                 SkLogger logger) {

        this.handler = handler;
        this.logger = logger;
        this.maxRequestSize = server.getMaxRequestSize();
        this.maxChunkSize = server.getMaxChunkSize();
        this.idleReadTimeout = server.getIdleReadTimeout();
        this.sslCtx = sslCtx;
    }

    /**
     * Initialize a channel with handlers that:
     * 1 -- handle and HTTP
     * 2 -- handle chunked HTTP requests implicitly, only calling channelRead
     * with FullHttpRequest.
     * 3 -- the request handler itself
     *
     * TODO: HttpContentCompressor, other options?
     */
    @Override
    public void initChannel(SocketChannel ch) {
        logger.fine("HttpServiceInitializer, initializing new channel");
        /*
         * Consider exposing the hard-coded arguments for config. Defaults
         * from netty are in HttpObjectDecoder constants and are
         *  4096 - max initial line length
         *  8192 - max header size
         *  8192 - max chunk size (our default is 64k)
         *  headers are validated by default
         */
        HttpDecoderConfig codecConfig = new HttpDecoderConfig()
            .setMaxChunkSize(maxChunkSize);
        ChannelPipeline p = ch.pipeline();
        if (sslCtx != null) {
            p.addLast(sslCtx.newHandler(ch.alloc()));
        }
        p.addLast(READ_TIMEOUT_HANDLER_NAME,
                  new ReadTimeoutHandler(idleReadTimeout));
        p.addLast(CODEC_HANDLER_NAME,
                  new HttpServerCodec(codecConfig));
        p.addLast(AGG_HANDLER_NAME,
                  new HttpObjectAggregator(maxRequestSize));
        p.addLast(HTTP_HANDLER_NAME, handler);
    }
}
