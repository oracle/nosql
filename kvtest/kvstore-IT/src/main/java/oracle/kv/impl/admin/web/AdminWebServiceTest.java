/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.web;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.LoginService;
import oracle.kv.impl.admin.web.service.AdminSubService;
import oracle.kv.impl.admin.web.service.CommandWebService;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.sklogger.SkLogger;

import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

public class AdminWebServiceTest extends TestBase {

    private static final CommandService cs = createMock(CommandService.class);
    private static final LoginService login = createMock(LoginService.class);
    private static final SkLogger sklogger =
        new SkLogger(LoggerUtils.getLogger(AdminWebServiceTest.class, "Test"));
    private static final AdminWebService adminService =
        new AdminWebService(cs, login, null, 0, sklogger);
    private final String testCommandName = "ping";
    private final String testPath = "/V0/nosql/admin";
    private FullHttpRequest msg;
    private CommandInputs inputs;
    private String clientHostName = "localhost";

    @BeforeClass
    public static void setUpClass() {
        adminService.initService();
    }

    @Override
    public void setUp()
        throws Exception {

        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         testPath);
        inputs = new CommandInputs();
        inputs.setCommand(testCommandName);
        msg.content().writeBytes(JsonUtils.print(inputs).getBytes());
    }

    @Override
    public void tearDown()
        throws Exception {

        msg = null;
        inputs = null;
    }

    @Test
    public void testHandleRequest() throws Exception {
        ChannelHandlerContext ctx = createExpectationAndMock(clientHostName);
        adminService.handleRequest(msg, ctx,
                                   new LogControl().generateLogContext
                                   (msg.method().name() + " " + msg.uri()));

        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         testPath + "/abc");
        inputs = new CommandInputs();
        inputs.setCommand("abc");
        msg.content().writeBytes(JsonUtils.print(inputs).getBytes());
        ctx = createExpectationAndMock(clientHostName);
        try {
            adminService.handleRequest(
                msg, ctx, new LogControl().generateLogContext(
                msg.method().name() + " " + msg.uri()));
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         testPath);
        msg.content().writeBytes("no json".getBytes());
        ctx = createExpectationAndMock(clientHostName);
        try {
            adminService.handleRequest(
                msg, ctx, new LogControl().generateLogContext(
                msg.method().name() + " " + msg.uri()));
        } catch (IllegalArgumentException e) {
            /* Expected */
        }
    }

    @Test
    public void testLookupService() throws Exception {
        try {
            adminService.lookupService("abc");
        } catch (IllegalArgumentException e) {
            /* Expceted */
        }
    }

    @Test
    public void testGetServiceFromRegistry() throws Exception {
        ChannelHandlerContext ctx = createExpectationAndMock(clientHostName);
        WrappedHttpRequest wrappedRequest =
            new WrappedHttpRequest(msg, ctx);
        AdminSubService ws =
            adminService.getServiceFromRegistry(wrappedRequest);

        assertTrue(ws instanceof CommandWebService);
        CommandWebService cws = (CommandWebService) ws;
        assertEquals(cws.getShellCommand().getCommandName(), testCommandName);
        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         testPath + "/plans");
        inputs = new CommandInputs();
        inputs.setCommand("show");
        msg.content().writeBytes(JsonUtils.print(inputs).getBytes());
        ctx = createExpectationAndMock(clientHostName);
        wrappedRequest = new WrappedHttpRequest(msg, ctx);
        ws = adminService.getServiceFromRegistry(wrappedRequest);

        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         testPath);
        ctx = createExpectationAndMock(clientHostName);
        wrappedRequest = new WrappedHttpRequest(msg, ctx);
        try {
            ws = adminService.getServiceFromRegistry(wrappedRequest);
        } catch (IllegalArgumentException e) {
            /* Expected */
        }
        ctx = createExpectationAndMock(clientHostName);
        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         testPath);
        inputs = new CommandInputs();
        msg.content().writeBytes(JsonUtils.print(inputs).getBytes());
        ctx = createExpectationAndMock(clientHostName);
        wrappedRequest = new WrappedHttpRequest(msg, ctx);
        try {
            ws = adminService.getServiceFromRegistry(wrappedRequest);
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        ctx = createExpectationAndMock(clientHostName);
        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         testPath + "/abc");
        inputs = new CommandInputs();
        msg.content().writeBytes(JsonUtils.print(inputs).getBytes());
        ctx = createExpectationAndMock(clientHostName);
        wrappedRequest = new WrappedHttpRequest(msg, ctx);
        try {
            ws = adminService.getServiceFromRegistry(wrappedRequest);
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        ctx = createExpectationAndMock(clientHostName);
        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         testPath + "/login");
        wrappedRequest = new WrappedHttpRequest(msg, ctx);
        try {
            ws = adminService.getServiceFromRegistry(wrappedRequest);
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        ctx = createExpectationAndMock(clientHostName);
        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.GET,
                                         testPath);
        inputs = new CommandInputs();
        inputs.setCommand("ping");
        msg.content().writeBytes(JsonUtils.print(inputs).getBytes());
        wrappedRequest = new WrappedHttpRequest(msg, ctx);

        try {
            ws = adminService.getServiceFromRegistry(wrappedRequest);
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

    }

    public static ChannelHandlerContext
        createExpectationAndMock(String hostName) {
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        Channel channel = createMock(Channel.class);
        InetSocketAddress socketAddress = new InetSocketAddress(hostName,
                                                                8080);
        ChannelFuture future = createMock(ChannelFuture.class);
        ChannelFuture futureForClose = createMock(ChannelFuture.class);

        expect(future.addListener(anyObject())).andReturn(futureForClose);
        replay(future);

        expect(channel.isOpen()).andReturn(true);
        expect(
            channel.writeAndFlush(anyObject(DefaultFullHttpResponse.class))).
                andReturn(future);
        expect(channel.remoteAddress()).andReturn(socketAddress);
        replay(channel);
        expect(ctx.channel()).andReturn(channel);
        replay(ctx);
        return ctx;
    }
}
