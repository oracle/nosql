/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.admin.web.service;

import static oracle.nosql.common.http.Constants.NULL_KEY;
import static org.easymock.EasyMock.createMock;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.admin.LoginService;
import oracle.kv.impl.admin.web.AdminWebServiceTest;
import oracle.kv.impl.admin.web.ResponseHandler;
import oracle.kv.impl.admin.web.WrappedHttpRequest;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.nosql.common.sklogger.SkLogger;

import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

public class LogoutWebServiceTest {
    private static final LoginService loginService =
        createMock(LoginService.class);
    private final SkLogger logger =
        new SkLogger(LoggerUtils.getLogger(getClass(), "Test"));
    private String clientHostName = "localhost";

    @Test
    public void testRegisterService() throws Exception {
        Map<String, Map<String, AdminSubService>> registry =
            new HashMap<String, Map<String, AdminSubService>>();
        LogoutWebService lws = new LogoutWebService(loginService, logger);
        lws.registerService(registry);
        AdminSubService ws = registry.get("logout").get(NULL_KEY);
        assertTrue(ws instanceof LogoutWebService);
        try {
            lws.registerService(registry);
        } catch (IllegalStateException e) {
            /* Expected */
        }
    }

    @Test
    public void testExecuteService() throws Exception {

        LogoutWebService lws = new LogoutWebService(loginService, logger);
        FullHttpRequest msg =
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                           HttpMethod.OPTIONS,
                                           "/V0/nosql/admin/logout");
        ChannelHandlerContext ctx =
            AdminWebServiceTest.createExpectationAndMock(clientHostName);
        WrappedHttpRequest request = new WrappedHttpRequest(msg, ctx);
        ResponseHandler handler = new ResponseHandler();
        lws.executeService(request, handler);


        final byte[] sid = new byte[20];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) i;
        }
        LoginToken token =
            new LoginToken(new SessionId(sid), 1234567890123456L);
        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.GET,
                                         "/V0/nosql/admin/logout");
        msg.headers().set(HttpHeaderNames.AUTHORIZATION, "Bearer " +
                          LoginToken.convertBytesToHex(
                              token.toByteArray()));

        ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
        request = new WrappedHttpRequest(msg, ctx);
        handler = new ResponseHandler();
        try {
            lws.executeService(request, handler);
        } catch (IllegalArgumentException e) {
            /* Exppected */
        }

        msg =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                       HttpMethod.OPTIONS,
                                       "/V0/nosql/admin/logout");
        ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
        request = new WrappedHttpRequest(msg, ctx);
        handler = new ResponseHandler();
        lws.executeService(request, handler);

        msg =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                       HttpMethod.POST,
                                       "/V0/nosql/admin/logout");
        msg.headers().set(HttpHeaderNames.AUTHORIZATION, "Bearer ");
        ctx =
            AdminWebServiceTest.createExpectationAndMock(clientHostName);
        request = new WrappedHttpRequest(msg, ctx);
        handler = new ResponseHandler();
        try {
            lws.executeService(request, handler);
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        msg =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                       HttpMethod.POST,
                                       "/V0/nosql/admin/logout");
        ctx =
            AdminWebServiceTest.createExpectationAndMock(clientHostName);
        request = new WrappedHttpRequest(msg, ctx);
        handler = new ResponseHandler();
        try {
            lws.executeService(request, handler);
        } catch (IllegalArgumentException e) {
            /* Expected */
        }
    }
}
