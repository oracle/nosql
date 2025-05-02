/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.web.service;

import static oracle.nosql.common.http.Constants.NULL_KEY;
import static org.easymock.EasyMock.anyShort;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.admin.client.DELETE;
import oracle.kv.impl.admin.client.GET;
import oracle.kv.impl.admin.client.PUT;
import oracle.kv.impl.admin.web.AdminWebServiceTest;
import oracle.kv.impl.admin.web.CommandInputs;
import oracle.kv.impl.admin.web.ResponseHandler;
import oracle.kv.impl.admin.web.WrappedHttpRequest;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.nosql.common.sklogger.SkLogger;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.ObjectNode;

import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

public class CommandWebServiceTest extends TestBase {

    private static CommandService cs = createMock(CommandService.class);

    private final SkLogger sklogger =
        new SkLogger(LoggerUtils.getLogger(getClass(), "Test"));
    private static final String OPERATION = "ping";
    private static final String MESSAGE = "test";
    private static final int RETURN_CODE = 3000;
    private String clientHostName = "localhost";

    @Test
    public void testRegisterService() throws Exception {
        Map<String, Map<String, AdminSubService>> registry =
            new HashMap<String, Map<String, AdminSubService>>();
        for (ShellCommand sc: CommandShell.getAllCommands()) {
            CommandWebService cws =
                new CommandWebService(sc, cs, null, 0, false, sklogger);
            cws.registerService(registry);
        }

        /*
         * Test single name command
         */
        CommandWebService cws =
            (CommandWebService) registry.get(
                NULL_KEY).get(OPERATION);
        assertEquals(cws.getShellCommand().getCommandName(), OPERATION);

        /*
         * Test command with sub command
         */
        cws = (CommandWebService) registry.get(
            "plan").get("deploy-sn");
        assertEquals(cws.getShellCommand().getCommandName(), "deploy-sn");

        /*
         * Test command with sub reverse mapping
         */
        cws = (CommandWebService) registry.get(
            "topology").get("show");
        assertEquals(cws.getShellCommand().getCommandName(), "topology");

        cws = new CommandWebService(
            CommandShell.getAllCommands().get(17), cs, null, 0, false, sklogger);
        try {
            cws.registerService(registry);
        } catch (IllegalStateException e) {
            /* Expected */
        }

        cws = new CommandWebService(
            CommandShell.getAllCommands().get(18), cs, null, 0, false, sklogger);
        try {
            cws.registerService(registry);
        } catch (IllegalStateException e) {
            /* Expected */
        }

        cws = new CommandWebService(
            CommandShell.getAllCommands().get(23), cs, null, 0, false, sklogger);
        try {
            cws.registerService(registry);
        } catch (IllegalStateException e) {
            /* Expected */
        }

        registry.clear();
        cws.checkHttpMethod(HttpMethod.GET);
        cws = new CommandWebService(
            new GetCommand(), cs, null, 0, false, sklogger);
        cws.registerService(registry);
        cws.checkHttpMethod(HttpMethod.PUT);
        cws = new CommandWebService(
            new PutCommand(), cs, null, 0, false, sklogger);
        cws.registerService(registry);
        cws.checkHttpMethod(HttpMethod.DELETE);
        cws = new CommandWebService(
            new DeleteCommand(), cs, null, 0, false, sklogger);
        cws.registerService(registry);
    }

    @Test
    public void testExecuteService() throws Exception {
        List<? extends ShellCommand> commands = CommandShell.getAllCommands();
        try {
            FullHttpRequest msg =
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                           HttpMethod.GET,
                                           "/V0/nosql/admin");
            CommandInputs inputs = new CommandInputs();
            inputs.setCommand(OPERATION);
            msg.content().writeBytes(JsonUtils.writeAsJson(inputs).getBytes());
            ChannelHandlerContext ctx =
                AdminWebServiceTest.createExpectationAndMock(clientHostName);
            WrappedHttpRequest request = new WrappedHttpRequest(msg, ctx);
            CommandShell.setCommands(Arrays.asList(new MyCommand()));
            ResponseHandler handler = new ResponseHandler();
            CommandWebService cws = new CommandWebService(
                new MyCommand(), cs, null, 0, false, sklogger);
            cws.executeService(request, handler);
            ShellCommandResult scr =
                JsonUtils.fromJson(
                    handler.getPayload().toString(), ShellCommandResult.class);
            assertEquals(scr.getOperation(), OPERATION);
            assertEquals(scr.getReturnCode(), RETURN_CODE);
            assertEquals(scr.getDescription(), MESSAGE);

            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.GET,
                                             "/V0/nosql/admin");
            inputs = new CommandInputs();
            inputs.setCommand(OPERATION);
            ObjectNode [] ons = new ObjectNode[2];
            ons[0] = JsonUtils.createObjectNode();
            ons[0].put("security", true);
            ons[1] = JsonUtils.createObjectNode();
            ArrayNode an = JsonUtils.createArrayNode();
            ons[1].set("params", an);
            ObjectNode anOn = JsonUtils.createObjectNode();
            anOn.put("javaMisc", "abc");
            an.add(anOn);
            inputs.setArguments(ons);
            msg.content().writeBytes(JsonUtils.writeAsJson(inputs).getBytes());
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            request = new WrappedHttpRequest(msg, ctx);
            CommandShell.setCommands(Arrays.asList(new MyCommand()));
            handler = new ResponseHandler();
            cws = new CommandWebService(
                new MyCommand(), cs, null, 0, false, sklogger);
            cws.executeService(request, handler);

            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.GET,
                                             "/V0/nosql/admin");
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            request = new WrappedHttpRequest(msg, ctx);
            CommandShell.setCommands(Arrays.asList(new MyCommand()));
            handler = new ResponseHandler();
            cws = new CommandWebService(
                new MyCommand(), cs, null, 0, false, sklogger);
            cws.executeService(request, handler);

            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.POST,
                                             "/V0/nosql/admin/topology");
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            request = new WrappedHttpRequest(msg, ctx);
            CommandShell.setCommands(Arrays.asList(new MyCommand()));
            Set<HttpMethod> set = new HashSet<HttpMethod>();
            set.add(HttpMethod.POST);
            handler = new ResponseHandler();
            List<String> names = new ArrayList<String>();
            names.add("parentSub");
            cws = new CommandWebService(
                new MyCommand(), cs, names, set, null, 0, false, sklogger);
            cws.executeService(request, handler);

            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.OPTIONS,
                                             "/V0/nosql/admin/topology");
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            request = new WrappedHttpRequest(msg, ctx);
            CommandShell.setCommands(Arrays.asList(new MyCommand()));
            handler = new ResponseHandler();
            cws = new CommandWebService(
                new MyCommand(), cs, null, 0, false, sklogger);
            cws.executeService(request, handler);

            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.POST,
                                             "/V0/nosql/admin");
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            inputs = new CommandInputs();
            inputs.setCommand(OPERATION);
            msg.content().writeBytes(JsonUtils.writeAsJson(inputs).getBytes());
            request = new WrappedHttpRequest(msg, ctx);
            ShellCommand se = new ShellExceptionCommand();
            CommandShell.setCommands(Arrays.asList(se));
            handler = new ResponseHandler();
            cws = new CommandWebService(se, cs, null, 0, false, sklogger);
            cws.executeService(request, handler);

            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.POST,
                                             "/V0/nosql/admin");
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            inputs = new CommandInputs();
            inputs.setCommand(OPERATION);
            msg.content().writeBytes(JsonUtils.writeAsJson(inputs).getBytes());
            request = new WrappedHttpRequest(msg, ctx);
            se = new ShellExceptionCommandWithRedirect();
            CommandShell.setCommands(Arrays.asList(se));
            handler = new ResponseHandler();
            cws = new CommandWebService(se, cs, null, 0, false, sklogger);
            cws.executeService(request, handler);

            cs = createMock(CommandService.class);
            expect(
                cs.getMasterWebServiceAddress(isNull(), anyShort())).
                    andReturn(new URI("http://master")).anyTimes();
            replay(cs);
            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.POST,
                                             "/V0/nosql/admin");
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            inputs = new CommandInputs();
            inputs.setCommand(OPERATION);
            msg.content().writeBytes(JsonUtils.writeAsJson(inputs).getBytes());
            request = new WrappedHttpRequest(msg, ctx);
            se = new ShellExceptionCommandWithRedirect();
            CommandShell.setCommands(Arrays.asList(se));
            handler = new ResponseHandler();
            cws = new CommandWebService(se, cs, null, 0, false, sklogger);
            cws.executeService(request, handler);

            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.POST,
                                             "/V0/nosql/admin");
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            inputs = new CommandInputs();
            inputs.setCommand(OPERATION);
            msg.content().writeBytes(JsonUtils.writeAsJson(inputs).getBytes());
            request = new WrappedHttpRequest(msg, ctx);
            se = new ShellExceptionCommandWithRedirect();
            CommandShell.setCommands(Arrays.asList(se));
            handler = new ResponseHandler();
            cws = new CommandWebService(se, cs, null, 0, true, sklogger);
            cws.executeService(request, handler);

            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.POST,
                                             "/V0/nosql/admin");
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            inputs = new CommandInputs();
            inputs.setCommand(OPERATION);
            msg.content().writeBytes(JsonUtils.writeAsJson(inputs).getBytes());
            request = new WrappedHttpRequest(msg, ctx);
            se = new ShellExceptionCommandWithRedirect();
            CommandShell.setCommands(Arrays.asList(se));
            handler = new ResponseHandler();
            cws = new CommandWebService(se, cs, null, 0, true, sklogger);
            request.getRequest().headers().add(
                HttpHeaderNames.AUTHORIZATION,
                "Bearer AUTHORIZATIONHEADER123");
            cws.executeService(request, handler);

            msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                             HttpMethod.POST,
                                             "/V0/nosql/admin");
            ctx = AdminWebServiceTest.createExpectationAndMock(clientHostName);
            inputs = new CommandInputs();
            inputs.setCommand(OPERATION);
            msg.content().writeBytes(JsonUtils.writeAsJson(inputs).getBytes());
            request = new WrappedHttpRequest(msg, ctx);
            se = new ShellExceptionCommandWithRedirect();
            CommandShell.setCommands(Arrays.asList(se));
            handler = new ResponseHandler();
            cws = new CommandWebService(se, cs, null, 0, true, sklogger);
            request.getRequest().headers().add(
                HttpHeaderNames.AUTHORIZATION,
                "Bearer ");
            cws.executeService(request, handler);
        } finally {
            CommandShell.setCommands(commands);
        }
    }

    private class MyCommand extends ShellCommand {

        protected MyCommand() {
            super(OPERATION, 4);
        }

        @Override
        protected String getCommandDescription() {
            return null;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return null;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            assertEquals(args[0], OPERATION);
            ShellCommandResult scr = new ShellCommandResult();
            scr.setOperation(OPERATION);
            scr.setReturnCode(RETURN_CODE);
            scr.setDescription(MESSAGE);
            return scr;
        }
    }

    @GET
    private class GetCommand extends ShellCommand {
        protected GetCommand() {
            super("GET", 3);
        }
        @Override
        protected String getCommandDescription() {
            return null;
        }
        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return null;
        }
    }


    @PUT
    private class PutCommand extends ShellCommand {
        protected PutCommand() {
            super("PUT", 3);
        }
        @Override
        protected String getCommandDescription() {
            return null;
        }
        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return null;
        }
    }

    @DELETE
    private class DeleteCommand extends ShellCommand {
        protected DeleteCommand() {
            super("DELETE", 6);
        }
        @Override
        protected String getCommandDescription() {
            return null;
        }
        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return null;
        }
    }

    private class ShellExceptionCommand extends ShellCommand {

        protected ShellExceptionCommand() {
            super(OPERATION, 4);
        }

        @Override
        protected String getCommandDescription() {
            return null;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return null;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            throw new ShellArgumentException("Fail to parse JSON");
        }
    }

    private class ShellExceptionCommandWithRedirect extends ShellCommand {

        protected ShellExceptionCommandWithRedirect() {
            super(OPERATION, 4);
        }

        @Override
        protected String getCommandDescription() {
            return null;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return null;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            ShellException se =
                new ShellException("Fail to parse JSON",
                                   ErrorMessage.NOSQL_5300,
                                   CommandResult.NO_CLEANUP_JOBS);
            throw se;
        }
    }
}
