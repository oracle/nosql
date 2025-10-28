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

import static oracle.nosql.proxy.protocol.HttpConstants.KVRENEW_PATH;
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
 * This class holds the login token renew service functionality,
 * the request sent to this service need to provide authorization header with
 * value in below format:
 * Authorization:Bearer login_token_in_hex
 * Successful renew will provide an OK response with the new login token's hex
 * format in HTTP payload.
 * If the store server is not supporting session extension, the same login
 * token hex as in the request header will send back to driver in an OK HTTP
 * response.
 * Failure of logout operation will response with BAD_REQUEST with error
 * message in HTTP payload.
 */
public class TokenRenewService extends StoreSecurityService {

    public TokenRenewService(KVStoreImpl store, SkLogger logger) {
        super(store, logger);
    }

    @Override
    public FullHttpResponse
        handleSecurityRequest(FullHttpRequest request,
                              ChannelHandlerContext ctx,
                              LogContext lc) throws Exception {

        final LoginToken token = getBearerToken(request);

        final UserLoginAPI loginAPI = getLoginAPI(token);

        final LoginToken newToken =
            loginAPI.requestSessionExtension(token);
        if (newToken == null) {
            throw new IllegalArgumentException("Renew login toke fail");
        }
        return okResponse(convertLoginToken(newToken));
    }

    @Override
    public boolean lookupService(String path) {
        return pathInURIAllVersions(path, KVRENEW_PATH);
    }
}
