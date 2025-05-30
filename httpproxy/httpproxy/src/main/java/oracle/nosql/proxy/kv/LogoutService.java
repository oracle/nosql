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

package oracle.nosql.proxy.kv;

import static oracle.nosql.proxy.protocol.HttpConstants.KVLOGOUT_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.pathInURIAllVersions;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.UserLoginAPI;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

/*
 * This class holds the logout service functionality, the request sent to this
 * service need to provide authorization header with value in below format:
 * Authorization:Bearer login_token_in_hex
 * Successful logout will provide an OK response with message
 * "logout completed" in HTTP payload.
 * Failure of logout operation will response with BAD_REQUEST with error
 * message in HTTP payload.
 */
public class LogoutService extends StoreSecurityService {

    public LogoutService(KVStoreImpl store, SkLogger logger) {
        super(store, logger);
    }

    @Override
    public FullHttpResponse
        handleSecurityRequest(FullHttpRequest request,
                              ChannelHandlerContext ctx,
                              LogContext lc) throws Exception {

        final LoginToken token = getBearerToken(request);

        final UserLoginAPI loginAPI = getLoginAPI(token);
        try {
            logger.fine("User logout", lc);
            loginAPI.logout(token);
            logger.fine("User logout succeeded", lc);
            return okResponse(null);
        } catch (Exception e) {
            logger.fine("User logout failed: " + e, lc);
            throw e;
        }
    }

    @Override
    public boolean lookupService(String path) {
        return pathInURIAllVersions(path, KVLOGOUT_PATH);
    }
}
