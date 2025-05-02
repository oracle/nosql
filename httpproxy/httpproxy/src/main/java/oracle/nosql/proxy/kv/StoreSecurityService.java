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

package oracle.nosql.proxy.kv;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.common.http.Constants.CONTENT_LENGTH;
import static oracle.nosql.common.http.Constants.CONTENT_TYPE;
import static oracle.nosql.proxy.protocol.HttpConstants.BEARER_PREFIX;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;
import oracle.kv.impl.security.login.UserLoginAPI;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.Service;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;

/*
 * This is common parent class holds the security services related
 * functionalities.
 */
public abstract class StoreSecurityService implements Service {

    /*
     * Prefix of security related service
     */
    protected static final String SECURITY_PREFIX = "/security";

    /*
     * Store handle used to get the latest topology
     */
    protected final KVStoreImpl store;

    /*
     * For logging in http client
     */
    protected final SkLogger logger;

    public StoreSecurityService(KVStoreImpl store,
                                SkLogger logger) {
        this.store = store;
        this.logger = logger;
    }

    /*
     * Return error response to client
     */
    protected FullHttpResponse errorResponse(HttpResponseStatus status,
                                             String message) {
        final FullHttpResponse resp =
            new DefaultFullHttpResponse(HTTP_1_1, status,
                                        copiedBuffer(message.getBytes()));
        resp.headers().set(CONTENT_TYPE, "application/octet-stream")
            .setInt(CONTENT_LENGTH, resp.content().readableBytes());
        return resp;
    }

    /*
     * Return a OK response to client
     */
    protected FullHttpResponse okResponse(String message) {
        FullHttpResponse resp;
        if (message == null) {
            /*
             * In this case, content is an empty string
             */
            resp = new DefaultFullHttpResponse(HTTP_1_1,
                                               HttpResponseStatus.OK);
        } else {
            resp = new DefaultFullHttpResponse(
                HTTP_1_1,
                HttpResponseStatus.OK,
                copiedBuffer(message.getBytes()));
        }
        resp.headers().set(CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
            .setInt(CONTENT_LENGTH, resp.content().readableBytes());
        return resp;
    }

    /*
     * Extract the BEARER login token from the request
     */
    protected LoginToken getBearerToken(FullHttpRequest request) {
        /* Retrieve authorization header value */
        final String auth = request.headers().get(AUTHORIZATION);

        if (auth == null || !auth.substring(0, BEARER_PREFIX.length()).
            equalsIgnoreCase(BEARER_PREFIX)) {
            throw new IllegalArgumentException(
                "Authorization value need to start with \"Bearer \"");
        }

        /* Create the token */
        final String tokenHex =
            auth.substring(BEARER_PREFIX.length(), auth.length());

        if (tokenHex.isEmpty()) {
            throw new IllegalArgumentException("Token field is empty");
        }
        return LoginToken.fromByteArray(LoginToken.convertHexToBytes(tokenHex));
    }

    /**
     * Convert login token to JSON string in format:
     * {
     *     "token" : "xxxxx",
     *     "expireAt" : 123455 (long)
     * }
     * The login token need to encode in hex string format in order to later
     * reuse in HTTP header for proceeding operations.
     */
    public static String convertLoginToken(LoginToken token) {
        final StringBuilder sb = new StringBuilder();
        sb.append("{\"token\":\"").append(
            LoginToken.convertBytesToHex(token.toByteArray())).
            append("\", \"expireAt\":").append(
            Long.toString(token.getExpireTime())).append("}");
        return sb.toString();
    }

    /*
     * Return proper login API for specific token, need store handle for
     * topology resolution.
     */
    protected UserLoginAPI getLoginAPI(LoginToken token)
        throws RemoteException, NotBoundException {

        final SessionId sessId = token.getSessionId();
        final Topology topo = store.getTopology();
        final String storename = topo.getKVStoreName();

        /*
         * Check every SN in the store for persistent token resolution, until
         * one pass
         */
        if (sessId.getIdValueScope() == SessionId.IdScope.PERSISTENT) {
            final List<RepNode> repNodes = topo.getSortedRepNodes();
            RemoteException toThrow = null;
            for (RepNode rn : repNodes) {
                final StorageNodeId snid = rn.getStorageNodeId();
                final StorageNode sn = topo.get(snid);
                try {
                    return RegistryUtils.getRepNodeLogin(
                        storename, sn.getHostname(),
                        sn.getRegistryPort(), rn.getResourceId(),
                        (LoginManager) null);
                } catch (RemoteException re) {
                    if (toThrow == null) {
                        toThrow = re;
                    }
                } catch (NotBoundException nbe) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }

            if (toThrow != null) {
                throw toThrow;
            }

            throw new RemoteException("No RepNode available");
        }

        /* Non-persistent case, look for the allocator directly */
        final ResourceId rid = sessId.getAllocator();
        if (!(rid instanceof RepNodeId)) {
            throw new IllegalArgumentException("Expected a RepNodeId: " + rid);
        }

        final RepNodeId rnid = (RepNodeId) rid;
        final RepGroup rg = topo.get(new RepGroupId(rnid.getGroupId()));
        final RepNode rn = rg.get(rnid);
        if (rn == null) {
            throw new IllegalArgumentException(
                "Missing RepNode with id " + rnid + " in topology");
        }
        final StorageNodeId snid = rn.getStorageNodeId();
        final StorageNode sn = topo.get(snid);

        return RegistryUtils.getRepNodeLogin(
            storename, sn.getHostname(),
            sn.getRegistryPort(), rn.getResourceId(),
            (LoginManager) null);
    }

    /*
     * Common exception handling of request
     */
    @Override
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc) {
        try {
            return handleSecurityRequest(request, ctx, lc);
        } catch (Exception e) {
            final String errMessage =
                "Fail to serve security request: " +
                request.uri() + " Problem: " + e;
            logger.info(errMessage);
            return errorResponse(HttpResponseStatus.BAD_REQUEST,
                                 errMessage);
        }
    }

    /*
     * Sub class to implement specific request handling
     */
    protected abstract FullHttpResponse
        handleSecurityRequest(FullHttpRequest request,
                              ChannelHandlerContext ctx,
                              LogContext lc) throws Exception;
}
