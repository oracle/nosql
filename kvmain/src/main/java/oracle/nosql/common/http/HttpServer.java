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

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import oracle.nosql.common.sklogger.SkLogger;

/**
 * Netty HTTP server. Initialization process:
 *
 * 1. create event loops for handling both accepting connections and data
 * 2. bootstrap a server, setting the event loop groups, socket options, and
 * a ChannelInitializer instance to initialize newly-created channels.
 * 3. the ChannelInitializer is responsible for setting up the pipeline for
 * handling data input on the channel. The pipeline includes an instance of
 * ChannelInboundHandlerAdapter (HttpServerHandler in this case) configured to
 * handle data and respond.
 *
 * An instance of RequestHandler must be implemented by the user of this
 * class. That interface is used to handle all requests on this server instance.
 */
public class HttpServer {
    static final int DEFAULT_MAX_REQUEST_SIZE = 4 * 1024 * 1024; // 4MB
    static final int DEFAULT_MAX_CHUNK_SIZE = 65536;
    static final int DEFAULT_IDLE_READ_TIMEOUT = 0;

    private final Channel channel;
    private final Channel httpsChannel;
    private final HttpServerHandler handler;

    /* hard-wired for now */
    private final int maxRequestSize;
    private final int maxChunkSize;

    /* How many seconds before an idle channel read to timeout */
    private final int idleReadTimeout;

    /*
     * bossGroup accepts incoming connections. The workerGroup handles data
     * requests on established connections. The parameter is number of
     * threads used in the event loop group.
     */
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    /* if non-null use this Executor to handle requests */
    private final Executor executor;

    private final SkLogger logger;


    /**
     * Simplified constructor that defaults most arguments. See doc
     * below
     */
    public HttpServer(int httpPort,
                      int httpsPort,
                      Executor executor,
                      RequestHandler requestHandler,
                      SslContext ctx,
                      SkLogger logger) throws InterruptedException {
        this(null /* default to localhost */, httpPort, httpsPort,
             0, 0, /* accept, netty worker threads defaulted */
             executor,
             0, /* maxRequestSize */
             0, /* maxChunkSize */
             0, /* readIdleTimeout */
             requestHandler, ctx, logger);
    }

    /**
     * Compatibility constructor -- used before pool threads was added
     */
    public HttpServer(String httpHost,
                      int httpPort,
                      int httpsPort,
                      int numAcceptThreads,
                      int numWorkerThreads,
                      int maxRequestSize,
                      int maxChunkSize,
                      int idleReadTimeout,
                      RequestHandler requestHandler,
                      SslContext ctx,
                      SkLogger logger) throws InterruptedException {
        this(httpHost, httpPort, httpsPort, numAcceptThreads, numWorkerThreads,
             null, maxRequestSize, maxChunkSize, idleReadTimeout,
             requestHandler, ctx, logger);
    }

    /**
     * Constructs an HTTP server instance. A constructed server should be
     * cleanly shut down using {@link #shutdown}. When this constructor
     * returns the server will be ready to accept requests
     *
     * @param httpHost the HTTP host name to listen on. This can be used to
     * restrict the network interfaces used for request handling on a machine
     * with multiple interfaces
     * @param httpPort the IP port to use for non-secure HTTP requests
     * @param httpsPort the IP port to use for secure HTTPS requests
     * @param numAcceptThreads the number of threads dedicated to accepting
     * new connections. This is typically small, even 1, because accepting new
     * connections is a relatively infrequent event. If 0 is passed the default
     * number from Netty is used, which is either in the system property
     * "io.netty.eventLoopThreads" or 2 * the number of cpus available
     * @param numWorkerThreads the number of Netty threads dedicated to
     * handling requests. The expectation is that the handlers called in these
     * threads will not block and if the handler must do a lot of work the task
     * should be handed off to another thread.  If 0 is passed the default
     * number from Netty is used, which is either in the system property
     * "io.netty.eventLoopThreads" or 2 * the number of cpus available
     * @param executor if this is non-null this Executor is used to execute
     * requests instead of the netty worker threads. This is the recommended
     * configuration for servers that do a lot of work (e.g. database calls,
     * I/O, etc). The executor is owned by the caller so it can be shared
     * by the caller for other tasks.
     * @param maxRequestSize the maximum size, in bytes, of HTTP requests that
     * can be handled. Anything larger will be rejected/discarded. It defaults
     * to 4MB
     * @param maxChunkSize the maximum size of a "chunk" (portion) of an
     * HTTP request that has be split into multiple network packets. It defaults
     * to 64K
     * @param idleReadTimeout the timeout, in seconds, used to detect an idle
     * connection. Idle connections are closed
     * @param requestHandler the handler for individual HTTP requests
     * @param ctx SSL context for secure connections
     * @param logger the logger instance to use
     */

    public HttpServer(String httpHost,
                      int httpPort,
                      int httpsPort,
                      int numAcceptThreads,
                      int numWorkerThreads,
                      Executor executor,
                      int maxRequestSize,
                      int maxChunkSize,
                      int idleReadTimeout,
                      RequestHandler requestHandler,
                      SslContext ctx,
                      SkLogger logger) throws InterruptedException {

        this.logger = logger;
        this.executor = executor;
        this.maxRequestSize =
            (maxRequestSize == 0 ?
                 DEFAULT_MAX_REQUEST_SIZE : maxRequestSize);
        this.maxChunkSize =
            (maxChunkSize == 0 ?
                 DEFAULT_MAX_CHUNK_SIZE : maxChunkSize);
        this.idleReadTimeout =
            (idleReadTimeout == 0 ?
                 DEFAULT_IDLE_READ_TIMEOUT : idleReadTimeout);

        /* use a local thread factory to name and control threads, if needed */
        bossGroup = new NioEventLoopGroup(
            numAcceptThreads, new HttpServerThreadFactory("HttpServerAccept"));
        workerGroup = new NioEventLoopGroup(
            numWorkerThreads, new HttpServerThreadFactory("HttpServerWorker"));
        handler = new HttpServerHandler(requestHandler, executor, logger);

        /*
         * http and https are different channels that share workers and event
         * loops
         */

        channel =
            (httpPort != 0 ? createChannel(httpHost, httpPort, null) : null);

        if (httpsPort != 0 && ctx != null) {
            httpsChannel = createChannel(httpHost, httpsPort, ctx);
        } else {
            httpsChannel = null;
        }
    }

    /**
     * Shared code for creating channels based on http vs https
     */
    private Channel createChannel(String host, int port, SslContext sslCtx)
        throws InterruptedException {

        ServerBootstrap sb = new ServerBootstrap();
        sb.group(bossGroup, workerGroup)

            /* use a new NioServerSocketChannel instance for new channels */
            .channel(NioServerSocketChannel.class)

            /* use HttpServerInitializer to init new channels */
            .childHandler(new HttpServerInitializer(handler,
                                                    this,
                                                    sslCtx,
                                                    logger))

            /* set some socket options */
            .option(ChannelOption.SO_BACKLOG, 1024)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        /*
         * Bind to specific host name
         */
        if (host != null) {
            return sb.bind(new InetSocketAddress(host, port)).sync().channel();
        }

        /* Bind and start to accept incoming connections */
        return sb.bind(port).sync().channel();
    }

    int getMaxRequestSize() {
        return maxRequestSize;
    }

    int getMaxChunkSize() {
        return maxChunkSize;
    }

    int getIdleReadTimeout() {
        return idleReadTimeout;
    }

    public boolean getUsingExecutor() {
        return executor != null;
    }

    Executor getExecutor() {
        return executor;
    }

    /**
     * Wait for shutdown
     */
    public HttpServer waitForShutdown() throws InterruptedException {
        if (channel != null) {
            channel.closeFuture().await();
        }
        if (httpsChannel != null) {
            httpsChannel.closeFuture().await();
        }
        return this;
    }

    /**
     * Cleanly shut down the server and threads.
     */
    public HttpServer shutdown() throws InterruptedException {

        /* allow the request handler to clean up */
        if (handler != null) {
            handler.shutDown();
        }

        if (channel != null) {
            channel.close();
        }
        if (httpsChannel != null) {
            httpsChannel.close();
        }
        logger.info("Shutting down HttpServer");
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        waitForShutdown();
        return this;
    }

    /**
     * Return HTTP server's SkLogger
     */
    public SkLogger getLogger() {
        return logger;
    }

    public static class HttpServerThreadFactory extends DefaultThreadFactory {
        HttpServerThreadFactory(String name) {
            super(name);
        }
    }
}
