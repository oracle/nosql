/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 */

package oracle.nosql.proxy.cloud;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.common.http.Constants.CONTENT_LENGTH;
import static oracle.nosql.proxy.protocol.HttpConstants.CACHEUPDATE_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.TABLE_ID;
import static oracle.nosql.proxy.protocol.HttpConstants.TABLE_NAME;
import static oracle.nosql.proxy.protocol.HttpConstants.pathInURIAllVersions;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.Service;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.rest.cloud.CloudRestDataService;
import oracle.nosql.proxy.security.AccessChecker;

/**
 * This service provides an REST interface for updating table name mapping in
 * security cache by SC in case of drop table.
 *
 * The operations are as follows
 *
 * PUT /cacheupdate
 *    ?tablename=blahblah is the fully qualified name of dropped table
 *    ?tableid=blahblah is the table OCID
 *
 *    Table name, in the form of compartmentId/tableName, used to flush security
 *    cache in access checker. Table OCID, used to flush TableCache in
 *    CloudDataService and CloudRestDataService.
 */
public class CacheUpdateService implements Service {
    private final AccessChecker ac;
    private final CloudDataService dataService;
    private final CloudRestDataService restDataService;
    private final SkLogger logger;

    public CacheUpdateService(AccessChecker ac,
                              CloudDataService dataService,
                              CloudRestDataService restDataService,
                              SkLogger logger) {
        this.ac = ac;
        this.dataService = dataService;
        this.restDataService = restDataService;
        this.logger = logger;
    }

    @Override
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc) {
        HttpMethod method = request.method();
        String uri = request.uri();

        try {
            if (!HttpMethod.PUT.equals(method)) {
                throw new IllegalArgumentException
                    ("Unsupported HTTP method: " + method);
            }
            final QueryStringDecoder qsd = new QueryStringDecoder(uri);
            final String[] pathArray = qsd.path().split("/");

            if (pathArray.length != 4) {
                throw new IllegalArgumentException
                    ("Incorrect path: " + qsd.path());
            }

            final Map<String, List<String>> params = qsd.parameters();
            final List<String> tp = params.get(TABLE_NAME);
            final List<String> ip = params.get(TABLE_ID);

            if (tp == null && ip == null) {
                throw new IllegalArgumentException
                    ("Missing parameter: neither " + TABLE_NAME + " or " +
                     TABLE_ID + " is specified");
            }

            String tableName = null;
            String tableId = null;
            if (tp != null && ac != null) {
                tableName = tp.get(0);
                ac.resetTableNameMapping(tableName);
            }

            if (ip != null) {
                tableId = ip.get(0);
                if (dataService != null) {
                    dataService.flushTableCache(tableId);
                }
                if (restDataService != null) {
                    restDataService.flushTableCache(tableId);
                }
            }

            trace("CacheUpdate: reset security cache and TableCache for " +
                  tableName + ", table OCID " + tableId, lc);

            FullHttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1, OK);
            resp.headers().setInt(CONTENT_LENGTH, 0);
            return resp;
        } catch (Exception e) {
            logger.info("Exception handling request: " + e.getMessage(), lc);
            FullHttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1,
                                                                BAD_REQUEST);
            resp.headers().setInt(CONTENT_LENGTH, 0);
            return resp;
        }
    }

    private void trace(String message, LogContext lc) {
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(message, lc);
        }
    }

    /**
     * Returns true if the path indicates this service
     */
    @Override
    public boolean lookupService(String uri) {
        return pathInURIAllVersions(uri, CACHEUPDATE_PATH);
    }
}
