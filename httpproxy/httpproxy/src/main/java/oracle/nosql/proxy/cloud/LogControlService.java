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

package oracle.nosql.proxy.cloud;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static oracle.nosql.common.http.Constants.CONTENT_LENGTH;
import static oracle.nosql.common.http.Constants.CONTENT_TYPE;
import static oracle.nosql.proxy.protocol.HttpConstants.APPLICATION_JSON;
import static oracle.nosql.proxy.protocol.HttpConstants.ENTRYPOINT;
import static oracle.nosql.proxy.protocol.HttpConstants.LOGCONTROL_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.LOG_LEVEL;
import static oracle.nosql.proxy.protocol.HttpConstants.TENANT_ID;
import static oracle.nosql.proxy.protocol.HttpConstants.pathInURIAllVersions;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.http.Service;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;

/**
 * This service provides an administrative REST interface for controlling
 * logging configuration.  It allows logging levels to be specified for
 * particular tentants or particular entrypoints.
 *
 * Information maintained through this interface is consulted during the
 * creation of every LogContext object, to influence the log level value
 * carried in the LogContext.
 *
 * The operations are as follows
 *
 * GET /V2/tools/logcontrol with no parameters produces a JSON dump of the
 *    logcontrol configuration, as does every PUT and DELETE
 *
 * PUT /V2/tools/logcontrol configures an entrypoint or tenant for a specified
 *    log level. Parameters:
 *    ?level=LEVEL is the desired log level
 *    ?entrypoint=POST%20/V2/nosql/data... is the entrypoint path to tag with
 *     that level. The entrypoint uses the HTTP method as part of the mapping
 *    ?tenantid=blahblah is the tenant to tag with the level
 * E.g.
 *    PUT /V2/tools/logcontrol?level=FINE&ampentrypoint=POST%20/V2/nosql/data
 *
 *    At least one of entrypoint or tenantid must be present.  If both are
 *    present in the same invocation, they are treated as two independent
 *    requests.  It might be expected that presenting both entrypoint and
 *    tenantid in the same request would imply that a request must satisfy both
 *    conditions in order to have its log level tagged; however this is not the
 *    case.
 *
 * DELETE /V2/tools/logcontrol deletes the configuration for an entrypoint or
 *    tenant. Takes the same entrypoint and tenantid parameters as PUT.
 */
public class LogControlService implements Service {

    private final LogControl logControl;
    private final SkLogger logger;

    public LogControlService(LogControl logControl, SkLogger logger) {
        this.logControl = logControl;
        this.logger = logger;
    }

    @Override
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc) {
        HttpMethod method = request.method();
        String uri = request.uri();
        logger.info("Proxy log control service request on channel: " + ctx, lc);

        try {
            final QueryStringDecoder qsd = new QueryStringDecoder(uri);
            final String[] pathArray = qsd.path().split("/");
            if (pathArray.length != 4) {
                throw new IllegalArgumentException
                    ("Incorrect path: " + qsd.path());
            }

            final Map<String, List<String>> params = qsd.parameters();
            final List<String> lp = params.get(LOG_LEVEL);
            final List<String> tp = params.get(TENANT_ID);
            final List<String> ep = params.get(ENTRYPOINT);

            /*
             * Note: levels can be set for requests with null tenantIds
             * by using the tenantId string "nullTenantId". See
             * LogControl.NULL_TENANT_ID.
             */

            if (HttpMethod.PUT.equals(method) ||
                HttpMethod.DELETE.equals(method)) {

                /* Both PUT and DELETE want tenant or entrypoint */
                if (tp == null && ep == null) {
                    throw new IllegalArgumentException
                        ("Missing parameter: one of " + TENANT_ID +
                         " or " + ENTRYPOINT + " is required");
                }

                if (HttpMethod.PUT.equals(method)) {

                    if (lp == null) {
                        throw new IllegalArgumentException
                            ("Missing required parameter " + LOG_LEVEL);
                    }
                    final Level logLevel = Level.parse(lp.get(0));

                    if (tp != null) {
                        final String tenantId = tp.get(0);
                        logControl.updateLogContext(lc, tenantId);
                        logControl.setTenantLogLevel(tenantId, logLevel);
                        logger.log(Level.FINE, "LogControl added tenant " +
                                   tenantId + " with logLevel " + logLevel, lc);
                    }

                    if (ep != null) {
                        final String entrypoint = ep.get(0);
                        logControl.setEntrypointLogLevel(entrypoint, logLevel);
                        logger.log(Level.FINE, "LogControl added entrypoint " +
                                entrypoint + " with logLevel " + logLevel, lc);
                    }
                } else { /* method == DELETE */

                    if (tp != null) {
                        final String tenantId = tp.get(0);
                        logControl.updateLogContext(lc, tenantId);
                        logControl.removeTenantLogLevel(tenantId);
                        logger.log(Level.FINE, "LogControl removed tenant " +
                                   tenantId, lc);
                    }

                    if (ep != null) {
                        final String entrypoint = ep.get(0);
                        logControl.removeEntrypointLogLevel(entrypoint);
                        logger.log(Level.FINE, "LogControl removed entrypoint " +
                                entrypoint, lc);
                    }
                }
            }
            /* Any other method is treated as a GET */

            final ByteBuf payload = ctx.alloc().directBuffer();
            payload.writeCharSequence(logControl.toJsonString(), UTF_8);

            HttpResponseStatus status = HttpResponseStatus.OK;
            FullHttpResponse resp =
                new DefaultFullHttpResponse(HTTP_1_1, status, payload);
            /*
             * It's necessary to add the header below to allow script-based
             * access from browsers.
             */
            resp.headers().set(CONTENT_TYPE, APPLICATION_JSON)
                .setInt(CONTENT_LENGTH, payload.readableBytes())
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            return resp;
        } catch (Exception e) {

            final ByteBuf payload = ctx.alloc().directBuffer();
            payload.writeCharSequence(e.getMessage(), UTF_8);
            logger.info("Exception handling request: " + payload, lc);
            FullHttpResponse resp =
                new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST, payload);
            resp.headers().set(CONTENT_TYPE, APPLICATION_JSON)
                .setInt(CONTENT_LENGTH, payload.readableBytes())
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            return resp;
        }
    }

    /**
     * Returns true if the path indicates this service
     */
    @Override
    public boolean lookupService(String uri) {
        return pathInURIAllVersions(uri, LOGCONTROL_PATH);
    }
}
