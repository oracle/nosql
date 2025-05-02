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
import static oracle.nosql.common.http.Constants.BASIC_PREFIX;
import static oracle.nosql.common.http.Constants.NULL_KEY;

import java.util.Base64;
import java.util.Map;

import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.LoginService;
import oracle.kv.impl.admin.web.ResponseHandler;
import oracle.kv.impl.admin.web.WrappedHttpRequest;
import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.UserLoginImpl;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.nosql.common.sklogger.SkLogger;

import oracle.nosql.common.json.ObjectNode;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Login service provide API to accept user name, password from client and
 * complete the authentication of the user. A login token will be granted and
 * send back to user in the "token" field of returnValue of the Http response
 * JSON payload.
 */
public class LoginWebService extends SubServiceBase {

    private static final String LOGIN_SERVICE_NAME = "login";

    private static final String TOKEN_FIELD_NAME = "token";

    /* Handle of the admin user login service */
    protected final LoginService loginService;

    public LoginWebService(LoginService loginService, SkLogger logger) {
        super(logger);
        this.loginService = loginService;
    }

    @Override
    public void registerService(
        Map<String, Map<String, AdminSubService>> registry) {

        /*
         * Create login service's entry in registry map, the first key is
         * login service name.
         */
        final Map<String, AdminSubService> map =
            getMapFromRegistry(LOGIN_SERVICE_NAME, registry);

        /* The second key is NULL for login service */
        final AdminSubService exist = map.put(NULL_KEY, this);

        if (exist != null) {
            throw new IllegalStateException(
                "Conflict service mapping: " + LOGIN_SERVICE_NAME);
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
               "Provide basic authentication information in " +
               "\"Authoriztion\" header.");
            handler.println(
               "\"Authoriation\" value start with \"Basic \" text, " +
               "following user_name:password pair encoded in Base64 format.");
            handler.println(
               "Example: Authorization:Basic base64(user_name:password)");
            return;
        }

        /* Retrieve authorization header value */
        final String basicHeaderValue =
            request.getRequest().headers().get(AUTHORIZATION);

        LoginCredentials cred = null;
        if (basicHeaderValue == null) {
            /* allow for Anonymous Login */
        } else if (!basicHeaderValue.startsWith(BASIC_PREFIX)) {
            throw new IllegalArgumentException(
                "Authorization value need to start with \"Basic \"");
        } else {
            /* Retrieve the encoded information */
            final String encodedAuth =
                basicHeaderValue.substring(BASIC_PREFIX.length(),
                                           basicHeaderValue.length());
            if (encodedAuth.isEmpty()) {
                throw new IllegalArgumentException(
                    "Authentication infomation is empty");
            }
            /* Decode the value use base64 format */
            final byte[] decoded =
                Base64.getDecoder().decode(encodedAuth.getBytes());

            /* The decoded auth info should be in format userName:passwd */
            final String auth = new String(decoded);
            final String[] authArrays = auth.split(":");
            if (authArrays.length != 2) {
                throw new IllegalArgumentException(
                    "Authentication information need to encoded with " +
                    "base64 in the format userName:password");
            }

            final String user = authArrays[0];
            final String pass = authArrays[1];
            cred = new PasswordCredentials(user, pass.toCharArray());
        }

        LoginResult result = null;
        Exception loginException = null;
        try {
            /* Login for user */
            result =
                ((UserLoginImpl)loginService.getUserLogin()).login(
                    cred, request.getClientHostName());
        } catch (Exception e) {
            loginException = e;
        }

        /* Login failed */
        if (loginException != null) {
            throw new IllegalArgumentException("Authentication failure: ",
                                               loginException);
        }
        if (result == null) {
            throw new IllegalArgumentException(
                "Authentication failure: Token result: " + result);
        }

        /*
         * Generate result JSON payload, set token in returnValue's
         * "token field"
         */
        final ShellCommandResult scr =
            ShellCommandResult.getDefault(LOGIN_SERVICE_NAME);
        final ObjectNode top = JsonUtils.createObjectNode();
        top.put(TOKEN_FIELD_NAME,
                LoginToken.convertBytesToHex(
                    result.getLoginToken().toByteArray()));
        scr.setReturnValue(top);

        /* Print the JSON result */
        handler.setResponseStatus(HttpResponseStatus.OK);
        handler.setAppJsonHeader();
        handler.println(scr.convertToJson());
    }
}
