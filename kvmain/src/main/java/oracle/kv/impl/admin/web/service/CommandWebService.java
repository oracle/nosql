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

import java.net.URI;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminNotReadyException;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.admin.client.DELETE;
import oracle.kv.impl.admin.client.GET;
import oracle.kv.impl.admin.client.POST;
import oracle.kv.impl.admin.client.PUT;
import oracle.kv.impl.admin.client.ShowCommand;
import oracle.kv.impl.admin.client.VerifyCommand;
import oracle.kv.impl.admin.web.CommandInputs;
import oracle.kv.impl.admin.web.ResponseHandler;
import oracle.kv.impl.admin.web.WrappedHttpRequest;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.CommonShell.LoginHelper;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Web service for admin CLI commands. Each of the admin CLI command will be
 * wrapped to a command web service and registered with the registry map. The
 * execution of command web service contain following steps:
 * 1. Parse the inputs from Http request.
 * 2. Execute the command and produce JSON output.
 * 3. Parse the JSON output to Http response.
 */
public class CommandWebService extends SubServiceBase {

    /*
     * Represented command of this web service.
     */
    private final ShellCommand shellCommand;

    /*
     * Handle of the server command service instance.
     */
    private final CommandService commandService;

    /*
     * Used by parent-sub command, to store parent command name plus sub
     * command name. For a single command, this field is null.
     */
    private final List<String> parentSubCommandName;

    /*
     * The supported Http method of this service.
     */
    private Set<HttpMethod> supportedHttpMethod;

    /*
     * Whether access token validation is needed
     */
    private final boolean isSecure;

    /*
     * Whether this service is signal to shutdown
     */
    private boolean isShutdown;

    private final String hostName;
    private final int registryPort;

    /*
     * This is the first layer of initialization. The shellCommand could be
     * a parent command in this case, the second phase initialization calling
     * registerService will set proper values for parentSubCommandName. For
     * a single name command, just keep the parentSubCommandName field null.
     */
    public CommandWebService(ShellCommand shellCommand,
                             CommandService commandService,
                             String hostName,
                             int registryPort,
                             boolean isSecure,
                             SkLogger logger) {
        super(logger);
        this.shellCommand = shellCommand;
        this.commandService = commandService;
        this.hostName = hostName;
        this.registryPort = registryPort;
        this.isSecure = isSecure;
        parentSubCommandName = null;
        supportedHttpMethod = new HashSet<HttpMethod>();
    }

    /*
     * Constructor used for sub command
     */
    public CommandWebService(ShellCommand shellCommand,
                             CommandService commandService,
                             List<String> parentSubCommandName,
                             Set<HttpMethod> supportedHttpMethod,
                             String hostName,
                             int registryPort,
                             boolean isSecure,
                             SkLogger logger) {
        super(logger);
        this.shellCommand = shellCommand;
        this.commandService = commandService;
        this.parentSubCommandName = parentSubCommandName;
        this.supportedHttpMethod = supportedHttpMethod;
        this.hostName = hostName;
        this.registryPort = registryPort;
        this.isSecure = isSecure;
    }

    /*
     * For a single name command, the first key of registry map is NULL, the
     * second key will be the command name. For command with sub commands,
     * generally the parent command name if the first key, the sub command
     * name will be the second key. However, for verify command and show
     * command, because they are verbs and should present on the request path,
     * we will use the sub command name to be the first key of registry map,
     * the parent command name (show/verify) will be the second key.
     */
    @Override
    public void
        registerService(Map<String, Map<String, AdminSubService>> registry) {
        String conflictName = null;
        if (shellCommand instanceof ShowCommand ||
            shellCommand instanceof VerifyCommand) {
            final CommandWithSubs commandWithSubs =
                (CommandWithSubs) shellCommand;
            for (ShellCommand sc : commandWithSubs.getSubCommands()) {
                final AdminSubService ws = makeSubCommandService(sc);
                final Map<String, AdminSubService> map =
                    getMapFromRegistry(sc.getCommandName(), registry);
                final AdminSubService exist =
                    map.put(commandWithSubs.getCommandName(), ws);
                if (exist != null) {
                    conflictName = commandWithSubs.getCommandName();
                    break;
                }
            }
        } else if (shellCommand instanceof CommandWithSubs) {
            final CommandWithSubs commandWithSubs =
                (CommandWithSubs) shellCommand;
            final Map<String, AdminSubService> map =
                getMapFromRegistry(commandWithSubs.getCommandName(), registry);
            for (ShellCommand sc : commandWithSubs.getSubCommands()) {
                final AdminSubService ws = makeSubCommandService(sc);
                final AdminSubService exist = map.put(sc.getCommandName(), ws);
                if (exist != null) {
                    conflictName = sc.getCommandName();
                    break;
                }
            }
        } else {
            Map<String, AdminSubService> map =
                getMapFromRegistry(NULL_KEY, registry);
            supportedHttpMethod =
                getClassHttpMethods(shellCommand.getClass());
            supportedHttpMethod.add(HttpMethod.OPTIONS);
            final AdminSubService exist =
                map.put(shellCommand.getCommandName(), this);
            if (exist != null) {
                conflictName = shellCommand.getCommandName();
            }
        }

        /*
         * If there is conflict, means that we have duplicate definition of
         * mapping of commands, throw ISE.
         */
        if (conflictName != null) {
            throw new IllegalStateException(
                "Conflict service mapping: " + conflictName);
        }
    }

    /*
     * For a sub command, we will add the parent command's name in
     * parentSubCommandName, then add the sub command's name in the list. Save
     * parent command's name will assist command parsing during execution.
     */
    private AdminSubService makeSubCommandService(ShellCommand sc) {
        final List<String> subCommandFullName = new ArrayList<String>();
        subCommandFullName.add(shellCommand.getCommandName());
        subCommandFullName.add(sc.getCommandName());
        final Set<HttpMethod> subSupportedMethod =
            getClassHttpMethods(sc.getClass());
        subSupportedMethod.add(HttpMethod.OPTIONS);
        return new CommandWebService(sc, commandService, subCommandFullName,
                                     subSupportedMethod, hostName, registryPort,
                                     isSecure, logger);
    }

    /*
     * Make up all the arguments for the command execution, including command
     * name, command arguments and their values.
     */
    private List<String>
        makeCommandAllArgs(WrappedHttpRequest wrappedRequest)
        throws ShellException {
        final List<String> allArgs = new ArrayList<String>();
        if (parentSubCommandName == null) {
            allArgs.add(shellCommand.getCommandName());
        } else {
            allArgs.addAll(parentSubCommandName);
        }
        final CommandInputs inputs = wrappedRequest.getInputs();
        if (inputs == null) {
            return allArgs;
        }
        List<String> inputArgs = inputs.getFilteredArguments();
        if (inputArgs != null && inputArgs.size() > 0) {
            allArgs.addAll(inputArgs);
        }
        return allArgs;
    }

    /**
     * Check that whether the supplied Http method is supported by this
     * service.
     */
    public boolean checkHttpMethod(HttpMethod method) {
        return supportedHttpMethod.contains(method);
    }

    @Override
    public void executeService(WrappedHttpRequest request,
                               ResponseHandler handler) {

        /*
         * If the request is asking for OPTIONS, return command usage and
         * descriptions in textual Http payload.
         */
        if (request.getRequest().method().equals(HttpMethod.OPTIONS)) {
            setCommonOptionsHeader(handler);
            handler.setResponseStatus(HttpResponseStatus.OK);
            handler.println(Shell.getCommandSyntax(shellCommand));
            handler.println(Shell.getCommandDescription(shellCommand));
            return;
        }

        LoginHandle loginHandle = null;

        try {

            /* Get login token from wrapped request */
            loginHandle = handleLoginToken(request);

            final LoginHandle shellLoginHandle = loginHandle;

            /*
             * The implementation of shell command execution need to have a
             * shell instance to pass in for operations. Create a mocked shell
             * here without any input/output. We only need it for making a call
             * to the shell command's execute method.
             * The command service and login handle are the real things make
             * the command execution working.
             */
            final CommandShell shell = new CommandShell(null, null) {
                @Override
                public CommandServiceAPI getAdmin(boolean force)
                    throws ShellException {
                    try {
                        if (isShutdown) {
                            throw new ShellException(
                                "Service is shutdown while waiting for " +
                                "command completion, please retry the " +
                                "operation or check on the command status",
                                ErrorMessage.NOSQL_5100,
                                CommandResult.NO_CLEANUP_JOBS);
                        }
                        final CommandServiceAPI admin =
                                CommandServiceAPI.wrap(commandService,
                                                       shellLoginHandle);
                        if (request.getPaths().contains("plan") &&
                            admin.getAdminStatus().getReplicationState().
                                isReplica()) {
                            throw new ShellException(
                                "Cannot serve request, " +
                                "please retry on master",
                                ErrorMessage.NOSQL_5300,
                                CommandResult.NO_CLEANUP_JOBS);
                        }
                        return admin;
                    } catch (RemoteException e) {
                        throw new ShellException(
                            "Fail to create command service API",
                            ErrorMessage.NOSQL_5500,
                            CommandResult.NO_CLEANUP_JOBS);
                    }
                }

                @Override
                public LoginManager getLoginManager() {
                    return new LoginManager() {

                        @Override
                        public String getUsername() {
                            return null;
                        }

                        @Override
                        public LoginHandle getHandle(HostPort target,
                                                     ResourceType rtype,
                                                     boolean cachedOnly) {
                            return shellLoginHandle;
                        }

                        @Override
                        public LoginHandle getHandle(ResourceId target,
                                                     boolean cachedOnly) {
                            return shellLoginHandle;
                        }

                        @Override
                        public void logout() throws SessionAccessException {
                        }
                    };
                }
            };

            /* Disable login helper in case the shell prompt for password */
            shell.setLoginHelper(new LoginHelper() {
                @Override
                protected KVStore
                    getAuthenticatedStore(final KVStoreConfig config)
                    throws ShellException {
                    return null;
                }
            });

            /* Set host name and port for commands need to obtain remote API */
            shell.setAdminHostPort(hostName, registryPort);

            /* Set to always use JSON output */
            shell.setJson(true);

            /* Parse all the command arguments, including command names */
            final List<String> args = makeCommandAllArgs(request);

            /* Start from the single name command or parent command */
            final ShellCommand command = shell.findCommand(args.get(0));

            /* Parse the common flags */
            final String[] argArray =
                shell.checkCommonFlags(args.toArray(new String[args.size()]));

            /* Produce JSON output */
            final ShellCommandResult result =
                command.executeJsonOutput(argArray, shell);

            final int errorCode = result.getReturnCode();

            /* Check that whether we need to redirection here */
            handleRedirectCode(errorCode, request, handler, loginHandle);

            /* Output the results to handler buffer */
            handler.setAppJsonHeader();
            handler.println(result.convertToJson());
        } catch (ShellException se) {
            final CommandResult cr = se.getCommandResult();

            /* Check that whether we need to redirection here */
            if (cr != null) {
                handleRedirectCode(cr.getErrorCode(),
                                   request, handler, loginHandle);
            }

            handler.handleShellException(shellCommand.getCommandName(), se);
        } catch (Exception e) {
            if (e instanceof AdminFaultException) {
                final AdminFaultException afe = (AdminFaultException) e;
                final CommandResult cr = afe.getCommandResult();
                if (cr != null) {
                    handleRedirectCode(cr.getErrorCode(),
                                       request, handler, loginHandle);
                }
            } else if (e instanceof AdminNotReadyException) {
                final AdminNotReadyException anre = (AdminNotReadyException) e;
                handleRedirectCode(
                    anre.getErrorMessage().getValue(),
                    request, handler, loginHandle);
            }
            handler.handleUnknownException(
                shellCommand.getCommandName(), e, logger);
        }
    }

    /*
     * If the command failure result is range between code 5300(inclusive) and
     * 5500, the Http response status will be SERVICE_UNAVAILABLE, code 503.
     * In this case, we try to retrieve admin master web service address from
     * command service, and set it to Location header of Http response.
     */
    private void handleRedirectCode(int errorCode,
                                    WrappedHttpRequest request,
                                    ResponseHandler handler,
                                    LoginHandle loginHandle) {
        if (errorCode >= ErrorMessage.NOSQL_5300.getValue() &&
            errorCode < ErrorMessage.NOSQL_5500.getValue()) {
            try {
                AuthContext authCtx = null;
                if (loginHandle != null) {
                    authCtx = new AuthContext(loginHandle.getLoginToken());
                }
                final URI masterAddress =
                    commandService.getMasterWebServiceAddress(
                        authCtx, SerialVersion.CURRENT);
                if (masterAddress == null) {
                    logger.info("Fail to retrieve master web service URI");
                    return;
                }
                final URI currentAddress =
                    new URI(request.getRequest().uri());
                final URI redirectAddress =
                    new URI(masterAddress.getScheme(), null,
                            masterAddress.getHost(),
                            masterAddress.getPort(),
                            currentAddress.getPath(),
                            currentAddress.getQuery(), null);
                handler.getHeaders().set(HttpHeaderNames.LOCATION,
                                         redirectAddress.toString());
                handler.setResponseStatus(
                    HttpResponseStatus.PERMANENT_REDIRECT);
            } catch (Exception e) {
                logger.severe("Fail to handle redirect request: " + e);
            }
        }
    }

    /*
     * Create login handle of the login token.
     */
    private LoginHandle handleLoginToken(WrappedHttpRequest wrappedRequest) {

        if (!isSecure) {
            return null;
        }

        final FullHttpRequest request = wrappedRequest.getRequest();

        /* Retrieve the authorization header value */
        final String auth = request.headers().get(AUTHORIZATION);

        /* Authorization info need to start with bearer for token validation */
        if (auth == null || !auth.startsWith(BEARER_PREFIX)) {
            throw new AuthenticationRequiredException(
                "Authorization value need to start with " + BEARER_PREFIX,
                false);
        }

        /* Create the token */
        final String tokenHex =
            auth.substring(BEARER_PREFIX.length(), auth.length());

        if (tokenHex.isEmpty()) {
            throw new AuthenticationRequiredException(
                "Token field is empty",
                false);
        }

        /* Parse token back to byte array and create login token */
        final LoginToken token =
            LoginToken.fromByteArray(LoginToken.convertHexToBytes(tokenHex));

        return new LoginHandle(token) {

            /*
             * There is no any client login manager for this handle. No auto
             * renew mechanism for this login handle.
             */
            @Override
            public LoginToken renewToken(LoginToken currToken)
                throws SessionAccessException {
                return null;
            }

            /*
             * No login manager to do the logout action.
             */
            @Override
            public void logoutToken() throws SessionAccessException {
            }

            /*
             * Usable for any type of resource.
             */
            @Override
            public boolean isUsable(ResourceType rtype) {
                return true;
            }
        };
    }

    /*
     * For unit test
     */
    public ShellCommand getShellCommand() {
        return shellCommand;
    }

    /*
     * Return the annotated Http method of specific class.
     */
    private Set<HttpMethod> getClassHttpMethods(Class<?> c) {
        final Set<HttpMethod> set = new HashSet<HttpMethod>();
        if (c.isAnnotationPresent(GET.class)) {
            set.add(HttpMethod.GET);
        }
        if (c.isAnnotationPresent(PUT.class)) {
            set.add(HttpMethod.PUT);
        }
        if (c.isAnnotationPresent(POST.class)) {
            set.add(HttpMethod.POST);
        }
        if (c.isAnnotationPresent(DELETE.class)) {
            set.add(HttpMethod.DELETE);
        }
        return set;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
    }
}
