/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.admin.web.service;

import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static oracle.nosql.common.http.Constants.BEARER_PREFIX;
import static oracle.nosql.common.http.Constants.NULL_KEY;

import java.util.Map;

import oracle.kv.impl.admin.LoginService;
import oracle.kv.impl.admin.web.ResponseHandler;
import oracle.kv.impl.admin.web.WrappedHttpRequest;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.UserLoginImpl;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class LogoutWebService extends SubServiceBase {

    private static final String LOGOUT_SERVICE_NAME = "logout";

    /* Handle of the admin user login service */
    protected final LoginService loginService;

    public LogoutWebService(LoginService loginService, SkLogger logger) {
        super(logger);
        this.loginService = loginService;
    }

    @Override
    public void registerService(
        Map<String, Map<String, AdminSubService>> registry) {

        final Map<String, AdminSubService> map =
            getMapFromRegistry(LOGOUT_SERVICE_NAME, registry);

        /* The second key is NULL for logout service */
        final AdminSubService exist = map.put(NULL_KEY, this);

        if (exist != null) {
            throw new IllegalStateException(
                "Conflict service mapping: " + LOGOUT_SERVICE_NAME);
        }
    }

    @Override
    public void executeService(WrappedHttpRequest request,
                               ResponseHandler handler) {

        /* Return usage to user */
        if (request.getRequest().method().equals(HttpMethod.OPTIONS)) {
            setCommonOptionsHeader(handler);
            handler.setResponseStatus(HttpResponseStatus.OK);
            handler.println(
               "Provide login token in " +
               "\"Authoriztion\" header.");
            handler.println(
               "\"Authoriation\" value start with \"Bearer \" text, " +
               "following the obtained token string from login");
            handler.println(
               "Example: Authorization:Bearer token_string");
            return;
        }

        /* Retrieve authorization header value */
        final String auth = request.getRequest().headers().get(AUTHORIZATION);

        if (auth == null || !auth.startsWith(BEARER_PREFIX)) {
            throw new IllegalArgumentException(
                "Authorization value need to start with \"Bearer \"");
        }

        /* Create the token */
        final String tokenHex =
            auth.substring(BEARER_PREFIX.length(), auth.length());

        if (tokenHex.isEmpty()) {
            throw new IllegalArgumentException("Token field is empty");
        }

        /* Parse token back to byte array and create login token */
        final LoginToken token =
            LoginToken.fromByteArray(LoginToken.convertHexToBytes(tokenHex));

        Exception logoutException = null;
        try {
            /* Logout for user */
            ((UserLoginImpl)loginService.getUserLogin()).
                logout(token, SerialVersion.CURRENT);
        } catch (Exception e) {
            logoutException = e;
        }

        /* Login failed */
        if (logoutException != null) {
            throw new IllegalArgumentException("Authentication failure: ",
                                               logoutException);
        }

        /*
         * Generate result JSON payload, set token in returnValue's
         * "token field"
         */
        final ShellCommandResult scr =
            ShellCommandResult.getDefault(LOGOUT_SERVICE_NAME);

        /* Print the JSON result */
        handler.setResponseStatus(HttpResponseStatus.OK);
        handler.setAppJsonHeader();
        handler.println(scr.convertToJson());
    }
}
