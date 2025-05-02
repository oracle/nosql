/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.web.service;

import static oracle.nosql.common.http.Constants.NULL_KEY;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import oracle.kv.LoginCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.LoginService;
import oracle.kv.impl.admin.web.AdminWebServiceTest;
import oracle.kv.impl.admin.web.ResponseHandler;
import oracle.kv.impl.admin.web.WrappedHttpRequest;
import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;
import oracle.kv.impl.security.login.UserLoginImpl;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.nosql.common.sklogger.SkLogger;

import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

public class LoginWebServiceTest extends TestBase {

    private static final LoginService loginService =
        createMock(LoginService.class);
    private final SkLogger sklogger =
        new SkLogger(LoggerUtils.getLogger(getClass(), "Test"));
    private String clientHostName = "localhost";

    @Test
    public void testRegisterService() throws Exception {
        Map<String, Map<String, AdminSubService>> registry =
            new HashMap<String, Map<String, AdminSubService>>();
        LoginWebService lws = new LoginWebService(loginService, sklogger);
        lws.registerService(registry);
        AdminSubService ws = registry.get("login").get(NULL_KEY);
        assertTrue(ws instanceof LoginWebService);
        try {
            lws.registerService(registry);
        } catch (IllegalStateException e) {
            /* Expected */
        }
    }

    @Test
    public void testExecuteService() throws Exception {
        LoginWebService lws = new LoginWebService(loginService, sklogger);
        UserLoginImpl userLogin = createMock(UserLoginImpl.class);
        LoginResult loginResult = createMock(LoginResult.class);
        final byte[] sid = new byte[20];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) i;
        }
        LoginToken token =
            new LoginToken(new SessionId(sid), 1234567890123456L);
        FullHttpRequest msg =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                       HttpMethod.GET,
                                       "/V0/nosql/admin/login");
        msg.headers().set(HttpHeaderNames.AUTHORIZATION, "Basic " +
            Base64.getEncoder().encodeToString("root:test123".getBytes()));
        expect(loginResult.getLoginToken()).andReturn(token).anyTimes();
        replay(loginResult);
        expect(userLogin.login(
            anyObject(LoginCredentials.class),
            anyString())).andReturn(loginResult).anyTimes();
        replay(userLogin);
        expect(loginService.getUserLogin()).andStubReturn(userLogin);
        replay(loginService);

        ChannelHandlerContext ctx =
            AdminWebServiceTest.createExpectationAndMock(clientHostName);
        WrappedHttpRequest request = new WrappedHttpRequest(msg, ctx);
        ResponseHandler handler = new ResponseHandler();
        lws.executeService(request, handler);
        ShellCommandResult scr =
            JsonUtils.readValue(handler.getPayload().toString(),
                                ShellCommandResult.class);
        assertEquals(scr.getOperation(), "login");
        assertEquals(scr.getReturnCode(), 5000);
        assertEquals(scr.getReturnValue().get("token").asText(),
                     LoginToken.convertBytesToHex(token.toByteArray()));

        msg =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                       HttpMethod.OPTIONS,
                                       "/V0/nosql/admin/login");
        ctx =
            AdminWebServiceTest.createExpectationAndMock(clientHostName);
        request = new WrappedHttpRequest(msg, ctx);
        handler = new ResponseHandler();
        lws.executeService(request, handler);

        msg =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                       HttpMethod.POST,
                                       "/V0/nosql/admin/login");
        msg.headers().set(HttpHeaderNames.AUTHORIZATION, "Basic ");
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
                                       "/V0/nosql/admin/login");
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
