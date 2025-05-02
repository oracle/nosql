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

import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static oracle.nosql.common.http.Constants.BASIC_PREFIX;
import static oracle.nosql.proxy.protocol.HttpConstants.KVLOGIN_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.pathInURIAllVersions;

import java.util.Base64;

import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

/*
 * This class holds the login service functionality, the request sent to this
 * service need to provide authorization header with value in below format:
 * Authorization:Basic base64_encode(user:password)
 * Successful login will provide an OK response with binary format of login
 * token encoded in hex format, stored in HTTP payload.
 * Failure of login operation will response with BAD_REQUEST with error message
 * in HTTP payload.
 */
public class LoginService extends StoreSecurityService {

    public LoginService(KVStoreImpl store,
                        SkLogger logger) {
        super(store, logger);
    }

    @Override
    public FullHttpResponse
        handleSecurityRequest(FullHttpRequest request,
                              ChannelHandlerContext ctx,
                              LogContext lc) {

        /* Retrieve authorization header value */
        final String basicHeaderValue =
            request.headers().get(AUTHORIZATION);

        if (basicHeaderValue == null ||
            !basicHeaderValue.substring(0, BASIC_PREFIX.length()).
            equalsIgnoreCase(BASIC_PREFIX)) {
            throw new IllegalArgumentException(
                "Authorization value need to start with \"Basic \"");
        }

        /* Retrieve the encoded information */
        final String encodedAuth =
            basicHeaderValue.substring(BASIC_PREFIX.length(),
                                       basicHeaderValue.length());
        if (encodedAuth.isEmpty()) {
            throw new IllegalArgumentException(
                "Credentials information is empty");
        }

        /* Decode the value use base64 format */
        final byte[] decoded =
            Base64.getDecoder().decode(encodedAuth.getBytes());
        /* The decoded auth info should be in format userName:passwd */
        final String auth = new String(decoded);
        final String[] authArrays = auth.split(":", 2);
        if (authArrays.length != 2) {
            throw new IllegalArgumentException(
                "Credentails is not in format user:password");
        }
        final String user = authArrays[0];
        final String pass = authArrays[1];
        if (user.isEmpty()) {
            throw new IllegalArgumentException(
                "User name is empty");
        }
        if (pass.isEmpty()) {
            throw new IllegalArgumentException(
                "Password is empty");
        }
        try {
            LoginCredentials cred =
                new PasswordCredentials(user, pass.toCharArray());
            final RepNodeLoginManager rnlm =
                new RepNodeLoginManager(cred.getUsername(), false);
            rnlm.setTopology(store.getDispatcher().getTopologyManager());
            rnlm.login(cred);
            final LoginToken login = rnlm.getLoginHandle().getLoginToken();
            return okResponse(convertLoginToken(login));
        } catch (RuntimeException re) {
            logger.fine("login failed: " + re, lc);
            throw re;
        }
    }

    @Override
    public boolean lookupService(String path) {
        return pathInURIAllVersions(path, KVLOGIN_PATH);
    }

}
