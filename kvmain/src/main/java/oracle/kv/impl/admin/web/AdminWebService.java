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

package oracle.kv.impl.admin.web;

import static oracle.nosql.common.http.Constants.NOSQL_ADMIN_PATH;
import static oracle.nosql.common.http.Constants.NULL_KEY;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.LoginService;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.admin.web.service.AdminSubService;
import oracle.kv.impl.admin.web.service.CommandWebService;
import oracle.kv.impl.admin.web.service.LoginWebService;
import oracle.kv.impl.admin.web.service.LogoutWebService;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.Service;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;

/**
 * AdminWebService handles the incoming Http request. It will first lookup the
 * web service from registry map according to the combination of request's
 * path, JSON field command in Http payload, Http method. If a admin sub web
 * service is found, then request handler will execute the web service for the
 * request. Http response will be handled in ResponseHandler. After request is
 * executed on web service, request handler will call ResponseHandler to
 * generate the result. If any exception happened during request processing,
 * a JSON payload of ShellCommandResult will be saved in the Http response to
 * indicate the problem.
 */
public class AdminWebService implements Service {

    /* Logger for request handler */
    private final SkLogger logger;

    /* Used to create CommandWebService */
    private final CommandService commandService;

    /* Used to create LoginWebService */
    private final LoginService loginService;

    /* Used by CommandShell created in CommandWebService */
    private final String hostName;
    private final int registryPort;

    /* Store the mapping of request to web service */
    private final Map<String, Map<String, AdminSubService>> registryMap =
        new HashMap<String, Map<String, AdminSubService>>();

    public AdminWebService(CommandService commandService,
                           LoginService loginService,
                           String hostName,
                           int registryPort,
                           SkLogger logger) {
        this.logger = logger;
        this.commandService = commandService;
        this.loginService = loginService;
        this.hostName = hostName;
        this.registryPort = registryPort;
    }

    /*
     * Return web service from registry map according to related information
     * in Http request. The first component of request path will be used to
     * map to the first key of registry, then the command field in request JSON
     * payload will indicate the second key in registry map. If any of these
     * keys is not present, then a NULL key will be used to look for the web
     * service. If not mapping service is found, then IAE will be thrown.
     * Eventually a response with JSON payload of 5100 in the returnCode field
     * will be sent to the client in the IAE case.
     */
    AdminSubService getServiceFromRegistry(WrappedHttpRequest wrapRequest) {

        /* Retrieve information from wrapped request */
        final List<String> paths = wrapRequest.getPaths();
        final CommandInputs inputs = wrapRequest.getInputs();
        final HttpMethod method = wrapRequest.getRequest().method();

        /* NULL key for empty path */
        if (paths.isEmpty()) {
            paths.add(NULL_KEY);
        }

        String command = null;
        /* NULL key for empty command */
        if (inputs == null || inputs.getCommand() == null) {
            command = NULL_KEY;
        } else {
            command = inputs.getCommand();
        }

        final Map<String, AdminSubService> map = registryMap.get(paths.get(0));
        if (map == null) {
            throw new IllegalArgumentException(
                "Cannot find mapping service for sub path: " +
                paths.get(0));
        }

        final AdminSubService ws = map.get(command);
        if (ws == null) {
            throw new IllegalArgumentException(
                "Cannot find mapping service for command key: " +
                command);
        }

        /*
         * For command web service, we need to check the request method
         * mapping the annotation on the command.
         */
        if (ws instanceof CommandWebService) {
            final CommandWebService commandWs = (CommandWebService) ws;
            if (!commandWs.checkHttpMethod(method)) {
                throw new IllegalArgumentException(
                    "Cannot find mapping service for http method: " +
                    method.name());
            }
        }
        return ws;
    }

    /*
     * This must be called before RequestHandler is in use. The initialization
     * create web services instances for all shell commands and register them
     * into the registry map.
     * Login service will also be created to handle login request.
     */
    public AdminWebService initService() {

        boolean isSecure = false;
        if (loginService != null) {
            isSecure = true;
        }

        registryMap.clear();

        for (ShellCommand sc : CommandShell.getAllCommands()) {
            final AdminSubService ws =
                new CommandWebService(sc, commandService, hostName,
                                      registryPort, isSecure, logger);
            ws.registerService(registryMap);
        }

        if (isSecure) {
            /* Register security login and logout services */
            final AdminSubService login =
                new LoginWebService(loginService, logger);
            login.registerService(registryMap);
            final AdminSubService logout =
                new LogoutWebService(loginService, logger);
            logout.registerService(registryMap);
        }

        return this;
    }

    @Override
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc) {

        final ResponseHandler handler = new ResponseHandler();

        try {

            /* First phase to parse Http request and stored the result */
            final WrappedHttpRequest wrappedRequest =
                new WrappedHttpRequest(request, ctx);

            /* Retrieve web service */
            final AdminSubService ws = getServiceFromRegistry(wrappedRequest);

            /* Execute web service */
            ws.executeService(wrappedRequest, handler);

            return handler.createResponse();

        } catch (Exception e) {

            /* Clear up any pre-exist info created in execution */
            handler.setDefaults();

            /* Use Json result */
            handler.setAppJsonHeader();

            /* Generate POJO instance for command result */
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("Http request failed");

            /* Create the message to indicate the problem */
            scr.setDescription(e.toString());

            /*
             * In general, the return code of exception is already handled by
             * shell command execution, so the WebService.executeService() is
             * not likely to throw any exception up to here. If the exception
             * can show up to this level but it is not
             * IllegalArgumentException, then this is probably
             * internal problem/bug caused.Therefore, we set 5500 by default
             * here. For IAE, it should be 5100 code.
             * Return a proper code back to the client according to above
             * logic.
             */
            scr.setReturnCode(ErrorMessage.NOSQL_5500.getValue());
            if (e instanceof IllegalArgumentException) {
                scr.setReturnCode(ErrorMessage.NOSQL_5100.getValue());
            } else {
                /*
                 * Log the internal problem
                 */
                logger.severe(LoggerUtils.getStackTrace(e));
            }

            /* print JSON result in Http payload */
            handler.println(scr.convertToJson());
            return handler.createResponse();
        } finally {

            /*
             * Clear the references in handler to avoid any protential
             * problem.
             */
            handler.clear();
        }
    }

    @Override
    public boolean lookupService(String uri) {
        return uri.startsWith(NOSQL_ADMIN_PATH);
    }

    public void shutdown() {
        for (Map<String,AdminSubService> map: registryMap.values()) {
            for (AdminSubService service : map.values()) {
                service.shutdown();
            }
        }
    }
}
