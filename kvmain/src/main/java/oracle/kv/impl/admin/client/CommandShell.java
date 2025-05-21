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

package oracle.kv.impl.admin.client;

import static oracle.kv.impl.util.CommandParser.JSON_FLAG;
import static oracle.kv.impl.util.CommandParser.JSON_V1_FLAG;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.KVVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.KVStoreInternalFactory;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.security.PasswordExpiredException;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.TopologyResolver;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.ShellPasswordReader;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.shell.AggregateCommand;
import oracle.kv.shell.DeleteCommand;
import oracle.kv.shell.ExecuteCommand;
import oracle.kv.shell.GetCommand;
import oracle.kv.shell.PutCommand;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.CommonShell;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellRCFile;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * To implement a new command:
 * 1.  Implement a class that extends ShellCommand.
 * 2.  Add it to the static list, commands, in this class.
 *
 * Commands that have subcommands should extend SubCommand.  See one of the
 * existing classes for example code (e.g. PlanCommand).
 */
public class CommandShell extends CommonShell {

    public static final String COMMAND_NAME = "runcli";
    public static final String COMMAND_NAME_ALIAS = "runadmin";
    public static final String COMMAND_DESC =
        "runs the command line interface";
    public static final String COMMAND_ARGS =
        CommandParser.getHostUsage() + " " +
        CommandParser.getPortUsage() + " | " +
        CommandParser.getHelperHostUsage() + eolt +
        CommandParser.optional(CommandParser.getStoreUsage()) + eolt +
        CommandParser.optional(CommandParser.getUserUsage()) + " " +
        CommandParser.optional(CommandParser.getSecurityUsage()) + eolt +
        CommandParser.optional(CommandParser.getAdminUserUsage()) + " " +
        CommandParser.optional(CommandParser.getAdminSecurityUsage()) + eolt +
        CommandParser.optional(CommandParser.getTimeoutUsage()) + " " +
        CommandParser.optional(CommandParser.getConsistencyUsage()) + eolt +
        CommandParser.optional(CommandParser.getDurabilityUsage()) + eolt +
        CommandParser.optional(CommandParser.getDnsCacheTTLUsage()) + eolt +
        CommandParser.optional(CommandParser.getRegOpenTimeoutUsage()) + eolt +
        CommandParser.optional(CommandParser.getRegReadTimeoutUsage()) + eolt +
        "[single command and arguments]";

    /*
     * Internal use only, used to indicate that the CLI is run
     * from KVStoreMain.
     */
    public static final String RUN_BY_KVSTORE_MAIN= "-kvstore-main";

    /* The section name in rc file for this shell */
    private static String RC_FILE_SECTION = "kvcli";

    private static final Logger logger =
        LoggerUtils.getLogger(CommandShell.class, "command");

    private CommandServiceAPI cs;
    private final CommandLoginHelper loginHelper;
    private boolean retry = false;
    private String commandName = null;

    /* Host and port of the currently connected admin */
    private String adminHostname = null;
    private int adminRegistryPort = 0;
    private List<String> helperHosts = null;

    /*
     * Addresses of the known Admins. The list is updated whenever a new
     * connection to an Admin is established. This list may be null during
     * initialization or if there is an error updating the list.
     */
    private List<URI> knownAdmins = null;

    /*
     * State of the connected Admin. This is set at time of connection and may
     * not reflect the current state of the Admin. The value may be null if not
     * connected or the Admin is not configured.
     */
    private State adminState = null;
    private String adminUser;
    private String adminSecurityFile;
    private String storeSecurityDir;
    private int regOpenTimeout;
    private int regReadTimeout;

    static final String prompt = "kv-> ";
    static final String usageHeader =
        "Oracle NoSQL Database Administrative Commands:" + eol;
    static final String versionString = " (" +
        KVVersion.CURRENT_VERSION.getNumericVersionString() + ")";

    public static boolean loginTest;

    /*
     * The list of commands available
     */
    private static
        List<? extends ShellCommand> commands =
                       Arrays.asList(new AggregateCommand(),
                                     new AwaitCommand(),
                                     new ConfigureCommand(),
                                     new ConnectCommand(),
                                     new DebugCommand(),
                                     new DeleteCommand(),
                                     new ExecuteCommand(),
                                     new Shell.ExitCommand(),
                                     new GetCommand(),
                                     new Shell.HelpCommand(),
                                     new HiddenCommand(),
                                     new HistoryCommand(),
                                     new Shell.LoadCommand(),
                                     new LogtailCommand(),
                                     new NamespaceCommand(),
                                     new PageCommand(),
                                     new PingCommand(),
                                     new PlanCommand(),
                                     new PolicyCommand(),
                                     new PoolCommand(),
                                     new PutCommand(),
                                     new RepairAdminQuorumCommand(),
                                     new ShowCommand(),
                                     new SnapshotCommand(),
                                     new TableCommand(),
                                     new TableSizeCommand(),
                                     new TimeCommand(),
                                     new TopologyCommand(),
                                     new VerboseCommand(),
                                     new VerifyCommand()
                                     );

    /*
     * The keywords whose following value will be masked with mask
     * character(*) in the command line history.
     */
    private static final String[] maskFlags = new String[] {
        "-password", "identified by"
    };

    /*
     * The maximum number of RNs to query for a topology. The larger the
     * number the more current the topology, but the longer the admin access
     * delay.
     */
    private static final int MAX_EXAMINE_RN_COUNT = 10;

    public CommandShell(InputStream input, PrintStream output) {
        super(input, output, maskFlags);
        Collections.sort(commands, new Shell.CommandComparator());
        loginHelper = new CommandLoginHelper();
        setLoginHelper(loginHelper);
    }

    @Override
    public void init() {
        if (!isNoConnect()) {
            try {
                connect();
            } catch (ShellException se) {
                /*
                 * The exception may be thrown while connecting a store or
                 * admin service
                 */
                displayResultReport("connect store or admin service",
                                    se.getCommandResult(),
                                    se.getMessage());
                if (getDebug()) {
                    se.printStackTrace(output);
                }
                /*
                 * If in json mode, we give up following execution in order to
                 * return a single result report to client.
                 */
                if (getJson()) {
                    if (dontExit()) {
                        /* For testing, save the exist code and do not exit*/
                        this.exitCode = EXIT_UNKNOWN;
                        return;
                    }
                    System.exit(EXIT_UNKNOWN);
                }
            }
        } else {
            output.println("Not connected to a store or admin service.  Use " +
                           "the connect command to connect.");
        }
    }

    @Override
    public void shutdown() {
        final LoginManager alm = getLoginManager();
        if (alm != null) {
            try {
                alm.logout();
            } catch (SessionAccessException e) {
            }
        }
        closeStore();
    }

    @Override
    public List<? extends ShellCommand> getCommands() {
        return commands;
    }

    /*
     * Retrieve all the static commands supported by this shell.
     */
    public static List<? extends ShellCommand> getAllCommands() {
        return commands;
    }

    /*
     * For unit test
     */
    public static void setCommands(List<? extends ShellCommand> newCommands) {
        commands = newCommands;
    }

    public String[] getCommandToRun() {
        return commandToRun;
    }

    @Override
    public String getPrompt() {
        return isNoPrompt() ? null : prompt;
    }

    @Override
    public String getUsageHeader() {
        return usageHeader;
    }

    /*
     * If retry is true, return that, but be sure to reset the value
     */
    @Override
    public boolean doRetry() {
        boolean oldVal = retry;
        retry = false;
        return oldVal;
    }

    @Override
    public void handleUnknownException(String line, Exception e) {
        if (e instanceof AdminFaultException) {
            AdminFaultException afe = (AdminFaultException) e;
            String faultClassName = afe.getFaultClassName();
            String msg = afe.getMessage();
            /* Don't treat IllegalCommandException as a "fault" */
            if (faultClassName.contains("IllegalCommandException")) {
                /* strip version info from message -- it's just usage */
                int endIndex = msg.indexOf(versionString);
                if (endIndex > 0) {
                    msg = msg.substring(0, endIndex);
                }
                e = null;
            }
            history.add(line, e);
            exitCode = EXIT_UNKNOWN;
            displayResultReport(line, afe.getCommandResult(), msg);
            if ((e != null) && getDebug()) {
                e.printStackTrace(output);
            }
        } else if (e instanceof KVSecurityException) {
            super.handleKVSecurityException(line, (KVSecurityException) e);
        } else {
            super.handleUnknownException(line, e);
        }
    }

    /**
     * Handles uncaught {@link KVSecurityException}s during execution of admin
     * commands. Currently we will retry login and connect to admin for
     * {@link AuthenticationRequiredException}s. The
     * {@link KVSecurityException}s during execution of store commands have
     * been wrapped as {@link ShellException}s and handled elsewhere.
     *
     * @param line command line
     * @param kvse instance of KVSecurityException
     * @return true if re-connect to admin successfully, or a ShellException
     * calls for a retry, otherwise returns false.
     */
    @Override
    public boolean handleKVSecurityException(String line,
                                             KVSecurityException kvse) {
        if (kvse instanceof AuthenticationRequiredException) {
            try {
                /* Login and connect to the admin again. */
                connectAdmin(true /* force login */);

                /* Retry the command */
                return true;
            } catch (ShellException se) {
                return handleShellException(line, se);
            } catch (Exception e) {
                handleUnknownException(line, e);
                return false;
            }
        }
        return super.handleKVSecurityException(line, kvse);
    }

    /**
     * Gets the command service. Makes a connection if necessary and caches
     * the handle. Subsequent calls may return the same handle.
     */
    public CommandServiceAPI getAdmin()
        throws ShellException {
        return getAdmin(false);

    }

    /**
     * Gets the command service. Makes a connection if necessary and caches
     * the handle. Subsequent calls may return the same handle. If force is
     * true a new connection is always made.
     *
     * @param force if true always make a new connection
     */
    public CommandServiceAPI getAdmin(boolean force)
        throws ShellException {

        ensureConnection(force);
        return cs;
    }

    public String getAdminHostname() {
        return adminHostname;
    }

    /* public for unit test */
    public int getAdminPort() {
        return adminRegistryPort;
    }

    public void connect()
        throws ShellException {

        /*
         * When store security directory is specified, CLI login with admin
         * and store with store SSL credentials.
         */
        if (storeSecurityDir != null) {
            final TrustLoginHelper trustLoginHelper = new TrustLoginHelper();
            trustLoginHelper.login();
            return;
        }

        /*
         * Also update AdminLogin with security file, so that Admin login
         * will use the security file specified to login Admin after store
         * login failed.
         */
        loginHelper.updateAdminLogin(adminUser, adminSecurityFile);
        connectAdmin(false /* force login */);

        /*
         * Get hosts:ports for all SNs so that even if the helper-hosts do not
         * contain a SN that has RNs, we can still connect to the store.
         */
        List<String> extraHostPorts;
        try {
            Parameters param = cs.getParameters();
            extraHostPorts = new ArrayList<String>();
            for (StorageNodeParams snParam : param.getStorageNodeParams()) {
                extraHostPorts.add(snParam.getHostname() + ":" +
                              snParam.getRegistryPort());
            }
        } catch (Exception re) {
            extraHostPorts = null;
        }
        connectStore(extraHostPorts);

    }

    public LoginManager getLoginManager() {
        return loginHelper.getAdminLoginMgr();
    }

    /*
     * Centralized call for connection issues.  If the exception indicates that
     * the admin service cannot be contacted null out the handle and force a
     * reconnect on a retry.
     */
    public void noAdmin(RemoteException e)
        throws ShellException{

        if (e != null) {
            if (cs != null) {
                Throwable t = e.getCause();
                if (t != null) {
                    if (t instanceof EOFException ||
                        t instanceof java.net.ConnectException ||
                        t instanceof java.rmi.ConnectException) {
                        cs = null;
                        retry = true;
                    }
                }
            }
        }

        throw new ShellException("Cannot contact admin", e,
                                 ErrorMessage.NOSQL_5300,
                                 CommandResult.NO_CLEANUP_JOBS);
    }

    /**
     * Ensure that we have a connection to the admin.  If the current
     * connection is read-only, then make one attempt to connect again in hopes
     * of connecting to the master.  Make 10 connection attempts if there is no
     * current connection, including additional attempts if a read-only
     * connection is found first. If force is true a new connection is always
     * made.
     *
     * @param force if true always make a new connection
     */
    private void ensureConnection(boolean force) throws ShellException {
        if ((cs != null) && !force) {
            /*
             * We have a command service, but it is read-only.  Try to
             * reconnect to the master once.
             */
            if (isReadOnly()) {
                connectAdmin(false /* force login */);
            }
            return;
        }

        ShellException ce = null;
        echo("Lost connection to Admin." + eol);
        echo("Reconnecting...");
        for (int i = 0; i < 10; i++) {
            try {
                echo(".");
                connectAdmin(false /* force login */);
                echo(eol);
                return;
            } catch (ShellException se) {
                final Throwable t = se.getCause();
                if (t != null &&
                    t instanceof AuthenticationFailureException) {
                    throw se;
                }
                ce = se;
            }
            try {
                Thread.sleep(6000);
            } catch (InterruptedException ignore) { }
        }
        if (ce != null) {
            throw ce;
        }
    }

    /**
     * Connects to the Admin at the specified host and port as the specified
     * user. If the Admin at host:port is not the master an attempt is made to
     * find and connect to the master.
     *
     * @param host hostname of Admin
     * @param port registry port of Admin
     * @param user user name
     * @param securityFileName login file path
     *
     * @throws ShellException if no Admin found
     */
    public void connectAdmin(String host,
                             int port,
                             String user,
                             String securityFileName)
        throws ShellException {

        loginHelper.updateAdminLogin(user, securityFileName);
        connectAdmin(host, port, false /* force login */);
        updateKnownAdmins();
    }

    /**
     * Reconnects to the master Admin. The initial attempt is made
     * the last Admin at adminHostname:adminRegistryPort. If that fails the
     * list of known Admins is tried. Upon successful return cs is set to the
     * connected Admin, adminHostname:adminRegistryPort are set to it's address,
     * and readOnly is true if the Admin is not the master.
     *
     * @param forceLogin force login before connection
     *
     * @throws ShellException if no Admin found
     */
    private void connectAdmin(boolean forceLogin) throws ShellException {
        ShellException e = null;

        /* Attempt to reconnect to the current admin */
        try {
            if (adminHostname == null) {
                connectAdmin(helperHosts, forceLogin);
            } else {
                connectAdmin(adminHostname, adminRegistryPort, forceLogin);
            }

            /*
             * If a connection was made to the master we are done. Note that if
             * the Admin is not configured isReadOnly() will return false and
             * we will remain connected to that bootstrap Admin.
             */
            if (!isReadOnly()) {
                updateKnownAdmins();
                return;
            }
        } catch (ShellException se) {
            e = se;
        }

        /*
         * We either have no Admin, or it is not a master. If we know of other
         * Admins, try to connect to one of them.
         */
        if (knownAdmins != null) {
            for (URI addr : knownAdmins) {
                try {
                    connectAdmin(addr.getHost(), addr.getPort(), forceLogin);

                    /* If the master was found, we are done */
                    if (!isReadOnly()) {
                        break;
                    }
                } catch (ShellException se) {
                    e = se;
                }
            }
        }

        /* If no Admin at this point, fail */
        if (cs == null) {
            if (e != null) {
                throw e;
            }
            throw new ShellException("Cannot connect to Admin at " +
                                     adminHostname + ":" + adminRegistryPort,
                                     e, ErrorMessage.NOSQL_5300,
                                     CommandResult.NO_CLEANUP_JOBS);
        }

        if (isReadOnly()) {
            echo("Connected to Admin in read-only mode" + eol);
        }
        updateKnownAdmins();
    }

    /**
     * Updates the list of known Admins. The list should be updated each time
     * we connect as the Admin configuration may have changed.
     */
    private void updateKnownAdmins() {
        final List<URI> admins = new ArrayList<>();

        try {
            final Parameters p = cs.getParameters();

            for (AdminParams ap : p.getAdminParams()) {
                final StorageNodeParams snp = p.get(ap.getStorageNodeId());
                try {
                    admins.add(new URI("rmi", null,
                                       snp.getHostname(), snp.getRegistryPort(),
                                       null, null, null));
                } catch (URISyntaxException use) {
                    throw new NonfatalAssertionException
                    ("Unexpected bad URL: " + use.getMessage(), use);
                }
            }
            knownAdmins = admins;
        } catch (Exception ignore) {
            /*
             * There are host of problems that can happen, but since the known
             * Admin list is not critical to operation we silently ignore them.
             */
        }

        /* Only update the list if we actually found Admins */
        if (!admins.isEmpty()) {
            knownAdmins = admins;
        }
    }

    /**
     * Connects to an Admin at any host:port from the given host:port
     * list. If there is no Admins at the given host:port list, tries getting
     * a current topology and connecting to an admin using the SN lists in the
     * topology.
     */
    private CommandServiceAPI getAdminInternal(List<String> hostPortsList,
                                               boolean forceLogin)
        throws Exception {
        if (hostPortsList == null) {
            throw new ShellException("Hostport list is not specified.",
                                     ErrorMessage.NOSQL_5300,
                                     CommandResult.NO_CLEANUP_JOBS);
        }
        Exception e = null;
        CommandServiceAPI result;
        try {
            result = searchAdmin(hostPortsList, forceLogin);
            if (result != null) {
                return result;
            }
        } catch (ShellException se) {
           /*
            * If the shell already login as anonymous user, do not attempt to
            * login RN and search because RN does not handle anonymous login,
            * so throw the exception here.
            * Also, if the exception is authentication failure, throw the
            * exception immediately.
            */
            if (isAnonymousLogin() ||
                (se.getCause() instanceof AuthenticationFailureException)) {
                throw se;
            }

            if (loginTest) {
                /*For tests on admin login, need to throw the login exception*/
                throw se;
            }
            e = se;
        }


        if (getDebug()) {
            if (e != null) {
                e.printStackTrace();
            }
            echo("Cannot connect to any Admins at " + hostPortsList +
                 " Try to connect to Admins at other SNs." + eol);
        }
        Topology topo = null;
        try {
            LoginManager rnLoginManager = loginHelper.
                getRNLoginMgr(hostPortsList.toArray(new String[0]));

            /*search for a current topology*/
            topo = TopologyLocator.get(hostPortsList.toArray(new String[0]),
                                       MAX_EXAMINE_RN_COUNT,
                                       rnLoginManager, null, null);
        } catch (Exception ee) {
            if (getDebug()) {
                echo("Failed to get topology." + eol);
            }

            throw new ShellException("Cannot connect to any admins.",
                                     ee, ErrorMessage.NOSQL_5300,
                                     CommandResult.NO_CLEANUP_JOBS);


        }
        if (topo != null) {
            List<String> hostports = new ArrayList<String>();
            for (StorageNode sn : topo.getSortedStorageNodes()) {
                hostports.add(sn.getHostname() + ":" +
                              sn.getRegistryPort());
            }
            result = searchAdmin(hostports, forceLogin);

            if (result != null) {
                echo("Connected to Admin at: " + adminHostname + ":" +
                    adminRegistryPort + eol);
                return result;
            }
        }

        throw new ShellException("Cannot connect to any Admin" +
            (hostPortsList == null ? "." : (" at: " +
            hostPortsList.toString())), e, ErrorMessage.NOSQL_5300,
            CommandResult.NO_CLEANUP_JOBS);

    }

    /**
     * Tries connecting to an available admin at the given host:port list.
     */
    private CommandServiceAPI searchAdmin(List<String> hostPortsList,
                                          boolean forceLogin)
        throws Exception {
        Exception e = null;
        for (String hostPort : hostPortsList) {
            HostPort curHostPort = HostPort.parse(hostPort);
            String host = curHostPort.hostname();
            int port = curHostPort.port();
            try {
                CommandServiceAPI admin = loginHelper.
                    getAuthenticatedAdmin(host,
                                          port,
                                          forceLogin);
                if (admin != null) {
                    adminHostname = host;
                    adminRegistryPort = port;
                    return admin;
                }

            } catch (RemoteException re) {
                /*Preserve the first exception as the reason. */
                if (e == null) {
                    e = re;
                }
                continue;
            } catch (NotBoundException nbe) {
                if (e == null) {
                    e = nbe;
                }
                continue;
            } catch (ShellException se) {
                if (se.getCause() instanceof AuthenticationFailureException) {
                    throw se;
                } else if (e == null) {
                    e = se;
                }
                continue;
            } catch (AuthenticationRequiredException are) {
                /*Throw exception to retry force login. */
                String exceptionHost = host + ":" + port;
                echo("Admin " + exceptionHost +
                     " requires authentication." + eol);
                throw are;
            } catch (AdminFaultException afe) {
               if (afe.getFaultClassName().equals(
                   SessionAccessException.class.getName())) {
                   /*Throw exception to retry force login. */
                   throw afe;
               } else if (e == null) {
                   e = afe;
               }
               continue;

            }
        }
        if (loginTest) {
            /*For tests on admin login, need to throw the login exception*/
            if (e != null) {
                throw e;
            }
        }
        if (e instanceof ShellException) {
            throw e;
        }
        throw new ShellException("Cannot connect to any Admin" +
            (hostPortsList == null ? "." : (" at: " +
            hostPortsList.toString())), e, ErrorMessage.NOSQL_5300,
            CommandResult.NO_CLEANUP_JOBS);
    }


    private void connectAdmin(String host, int port, boolean forceLogin)
        throws ShellException {
        List<String> hostPortList = new ArrayList<String>();
        hostPortList.add(host + ":" + port);
        connectAdmin(hostPortList, forceLogin);
    }

    /**
     * Connects to an Admin at any host:port from the given host:port
     * list. If the connected Admin is not the master an attempt is made
     * to find and connect to the master. Upon successful return cs is set to
     * the connected Admin, adminHostname:adminRegistryPort are set to it's
     * address, and readOnly is true if the Admin is not the master.
     *
     * @param hostPortsList host:port list
     * @param forceLogin force login before connection
     *
     * @throws ShellException if no Admin found
     */
    private void connectAdmin(List<String> hostPortsList, boolean forceLogin)
        throws ShellException {
        Exception e = null;
        try {
            CommandServiceAPI admin = getAdminInternal(hostPortsList,
                                                       forceLogin);

            final State state;
            try {
                state = admin.getAdminStatus().getReplicationState();
            } catch (AuthenticationRequiredException are) {
                String exceptionHost = adminHostname + ":" + adminRegistryPort;
                echo("Admin " + exceptionHost +
                     " requires authentication." + eol);
                throw are;
            }

            if (state != null) {
                switch (state) {
                case MASTER:
                    setAdmin(admin, state);
                    return;
                case REPLICA:
                    setAdmin(admin, state);

                    /* A replica is ok, but try to connect to the master */
                    final URI rmiaddr = cs.getMasterRmiAddress();
                    try {
                        echo("Redirecting to master at " + rmiaddr + eol);
                        connectAdmin(rmiaddr.getHost(), rmiaddr.getPort(),
                                     false);
                        return;
                    } catch (ShellException e2) {
                        echo("Redirect failed to master at " + rmiaddr + eol);
                    }
                    return;
                case DETACHED:
                case UNKNOWN:
                    break;
                }
            }
            /*
             * Here because state == DETACHED, UNKNOWN, or no state (not yet
             * configured).  Switch to this admin since at least we were able
             * to talk to it.
             */
            setAdmin(admin, state);
            return;
        } catch (AuthenticationRequiredException are) {
            /*
             * Will fail if try connecting to a secured admin without login.
             * Retry connecting and force login.
             */
            connectAdmin(hostPortsList, true);
            return;
        } catch (RemoteException re) {
            /*
             * Logging in another admin may be problematic, because it needs
             * to verify the login token from the issued admin, which may fail
             * due to network issues. In this case, a SessionAccessException
             * will be thrown, and we try to force to directly log in the new
             * admin.
             */
            if (re.getCause() instanceof SessionAccessException) {
                echo("Problem in verifying the login. " +
                     "Need to log in to Admin" +
                     (hostPortsList == null ? "" :
                     (" at: " + hostPortsList.toString())) +
                     " again." + eol);
                connectAdmin(hostPortsList, true);
                return;
            }
            e = re;
        } catch (ShellException se) {
            throw se;
        } catch (Exception ee) {
            if (loginTest) {
                throw new ShellException(ee.getMessage());
            }
            e = ee;
        }
        throw new ShellException(
            "Cannot connect to Admin" +
            (hostPortsList == null ? "." : (" at: " + hostPortsList)) +
            "\n" + e.getMessage(),
            e, ErrorMessage.NOSQL_5300, CommandResult.NO_CLEANUP_JOBS);
    }

    private void setAdmin(CommandServiceAPI admin, State state) {
        cs = admin;
        adminState = state;
    }

    /*
     * Used by CommandWebService for commands need to obtain remote API
     * from registry, e.g. 'show versions' command.
     */
    public void setAdminHostPort(String host, int port) {
        adminHostname = host;
        adminRegistryPort = port;
    }

    /**
     * Returns true if the command shell is connected to the Admin in read only
     * mode. This value set when the connection is made and should be used for
     * informational purposes only as mastership may change. Returns false
     * if the Admin is the master, has not been configured, or no connection
     * has been made.
     */
    boolean isReadOnly() {
        return (adminState != null) && !adminState.isMaster();
    }

    /* Internal use for unit test */
    int getCsfOpenTimeout() {
        return regOpenTimeout;
    }

    /* Internal use for unit test */
    int getCsfReadTimeout() {
        return regReadTimeout;
    }

    static class ConnectCommand extends CommandWithSubs {
        private static final String COMMAND = "connect";
        private static final String HOST_FLAG = "-host";
        private static final String HOST_FLAG_DESC = HOST_FLAG + " <hostname>";
        private static final String PORT_FLAG = "-port";

        private static final List<? extends SubCommand> subs =
            Arrays.asList(new ConnectAdminSubCommand(),
                          new ConnectStoreSubCommand());

        ConnectCommand() {
            super(subs, COMMAND, 4, 2);
        }

        @Override
        protected String getCommandOverview() {
            return "Encapsulates commands that connect to the specified " +
                "host and registry port" + eol + "to perform administrative " +
                "functions or connect to the specified store to" + eol +
                "perform data access functions.";
        }

        /* ConnectAdmin command */
        static class ConnectAdminSubCommand extends SubCommand {
            private static final String SUB_COMMAND = "admin";
            private static final String PORT_FLAG_DESC =
                PORT_FLAG + " <registry port>";

            static final String CONNECT_ADMIN_COMMAND_DESC =
                "Connects to the specified host and registry port to " +
                "perform" + eolt + "administrative functions.  An Admin " +
                "service must be active on the" + eolt + "target host.  " +
                "If the instance is secured, you may need to provide" + eolt +
                "login credentials.";

            static final String CONNECT_ADMIN_COMMAND_SYNTAX =
                COMMAND + " " + SUB_COMMAND + " " + HOST_FLAG_DESC + " " +
                PORT_FLAG_DESC + eolt +
                CommandParser.optional(CommandParser.getUserUsage()) + " " +
                CommandParser.optional(CommandParser.getSecurityUsage());

            ConnectAdminSubCommand() {
                super(SUB_COMMAND, 3);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                String host = null;
                int port = 0;
                String user = null;
                String security = null;

                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (HOST_FLAG.equals(arg)) {
                        host = Shell.nextArg(args, i++, this);
                    } else if (PORT_FLAG.equals(arg)) {
                        port = parseUnsignedInt(Shell.nextArg(args, i++, this));
                    } else if (CommandParser.USER_FLAG.equals(arg)) {
                        user = Shell.nextArg(args, i++, this);
                    } else if (CommandParser.SECURITY_FLAG.equals(arg)) {
                        security = Shell.nextArg(args, i++, this);
                    } else {
                        invalidArgument(arg);
                    }
                }
                if (host == null || port == 0) {
                    shell.badArgCount(this);
                }
                final CommandShell cmd = (CommandShell) shell;
                cmd.connectAdmin(host, port, user, security);

                /*
                 * If security enabled, the command history will be cleared
                 * after each re-connection.
                 */
                if (cmd.loginHelper.isSecuredAdmin()) {
                    cmd.getHistory().clear();
                }
                return "Connected.";
            }

            @Override
            protected String getCommandSyntax() {
                return CONNECT_ADMIN_COMMAND_SYNTAX;
            }

            @Override
            protected String getCommandDescription() {
                return CONNECT_ADMIN_COMMAND_DESC;
            }
        }

        /* ConnectStore command */
        static class ConnectStoreSubCommand extends SubCommand {
            final static String SUB_COMMAND = "store";
            final static String CONNECT_STORE_COMMAND_SYNTAX =
                COMMAND + " " + SUB_COMMAND + " " +
                ConnectStoreCommand.CONNECT_STORE_COMMAND_ARGUMENTS;

            private final ConnectStoreCommand connCommand;

            ConnectStoreSubCommand() {
                super(SUB_COMMAND, 4);
                connCommand = new ConnectStoreCommand();
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                CommandShell cShell = (CommandShell)shell;

                /*try to get host:ports for all SNs to connect to the store. */
                List<HostPort> extraHostPorts = new ArrayList<HostPort>();
                try {
                    CommandServiceAPI cs = cShell.getAdmin();
                    if (cs != null) {
                        Parameters param = cs.getParameters();
                        for (StorageNodeParams snParam :
                            param.getStorageNodeParams()) {
                            extraHostPorts.add(
                                new HostPort(snParam.getHostname(),
                                snParam.getRegistryPort()));
                       }
                    }
                } catch (Exception e) {
                }
                return connCommand.execute(args, shell, extraHostPorts);
            }

            @Override
            protected String getCommandSyntax() {
                return CONNECT_STORE_COMMAND_SYNTAX;
            }

            @Override
            protected String getCommandDescription() {
                return ConnectStoreCommand.CONNECT_STORE_COMMAND_DESC;
            }
        }
    }

    private class CommandShellParser extends ShellParser {

        CommandShellParser(String[] args,
                           String[] rcArgs,
                           String[] requiredFlags) {
            super(args, rcArgs, requiredFlags);
        }

        @Override
        public String getShellUsage() {
            final String usage;
            if (commandName == null) {
                usage = KVCLI_USAGE_PREFIX + eolt + COMMAND_ARGS;
            } else {
                usage = KVSTORE_USAGE_PREFIX + commandName + " " + eolt +
                    COMMAND_ARGS;
            }
            return usage;
        }

        @Override
        protected void verifyArgs() {
            super.verifyArgs();
            if (!isNoConnect()) {
                adminHostname = getHostname();
                adminRegistryPort = getRegistryPort();
                helperHosts = getHelperHosts();

                if ((adminHostname == null || adminRegistryPort == 0) &&
                    (helperHosts == null)) {
                    usage("Missing required argument");
                }

                if ((adminHostname != null || adminRegistryPort != 0) &&
                    (helperHosts != null)) {
                    usage("Only one of either " +  HELPER_HOSTS_FLAG + " or " +
                          HOST_FLAG + " plus " + PORT_FLAG +
                          " may be specified");
                }

                if (helperHosts != null) {
                    try {
                        validateHelperHosts(helperHosts);
                    } catch (IllegalArgumentException e) {
                        usage("Illegal value for " + HELPER_HOSTS_FLAG);
                    }
                }

                /*
                 * If no separate user or login file is set for admin, admin
                 * will share the same user and login file with store.
                 */
                if (adminUser == null && adminSecurityFile == null) {
                    adminUser = getUserName();
                    adminSecurityFile = getSecurityFile();
                }
            }

            regOpenTimeout = getRegistryOpenTimeout();
            regReadTimeout = getRegistryReadTimeout();
        }

        /**
         * Validate that each helper host entry in the form
         * <string>:<number>
         */
        private void validateHelperHosts(List<String> helperHostVal)
            throws IllegalArgumentException {

            if (helperHostVal == null) {
                throw new IllegalArgumentException
                    ("Helper hosts cannot be null.");
            }

            String[] hostPorts = helperHostVal.toArray(new String[0]);
            HostPort.parse(hostPorts);
        }

        // TODO: change to json
        @Override
        public boolean checkExtraArg(String arg) {
            if (ADMIN_USER_FLAG.equals(arg)) {
                /* Admin and store may need different users and login files */
                adminUser = nextArg(arg);
                return true;
            } else if (ADMIN_SECURITY_FLAG.equals(arg)) {
                adminSecurityFile = nextArg(arg);
                return true;
            }
            return false;
        }

        @Override
        public boolean checkExtraHiddenFlag(String arg) {
            if (RUN_BY_KVSTORE_MAIN.equals(arg)) {
                commandName = nextArg(arg);
                return true;
            } else if (STORE_SECURITY_DIR_FLAG.equals(arg)) {
                storeSecurityDir = nextArg(arg);
                final String error = FileUtils.verifyDirectory(
                    new File(storeSecurityDir));
                if (error != null) {
                    usage(error + ", flag " +
                          CommandParser.STORE_SECURITY_DIR_FLAG +
                          " requires the path of store security directory");
                }
                return true;
            }
            return false;
        }
    }

    public void parseArgs(String args[]) {
        final String[] requiredFlags = new String[0];
        final String[] rcArgs = ShellRCFile.readSection(RC_FILE_SECTION);
        final ShellParser parser = new CommandShellParser(args, rcArgs,
                                                          requiredFlags);
        parser.parseArgs();
    }

    public static void main(String[] args) {
        CommandShell shell = new CommandShell(System.in, System.out);
        try {
            shell.parseArgs(args);
            final String[] commandArgs = shell.getCommandToRun();
            if (commandArgs != null) {
                shell.checkJson(commandArgs);
            }
        } catch (Exception e) {
            String error;
            if(args != null && checkArg(args, CommandParser.JSON_FLAG)) {
                String operation = "";
                for(String arg: args) {
                    operation += arg + " ";
                }
                CommandResult result =
                    new CommandResult.CommandFails(
                        e.getMessage(), ErrorMessage.NOSQL_5100,
                        CommandResult.NO_CLEANUP_JOBS);
                error = ShellCommandResult.toJsonReport(operation, result);
            } else if (args != null &&
                       checkArg(args, CommandParser.JSON_V1_FLAG)) {
                String operation = "";
                for(String arg: args) {
                    operation += arg + " ";
                }
                CommandResult result =
                    new CommandResult.CommandFails(
                        e.getMessage(), ErrorMessage.NOSQL_5100,
                        CommandResult.NO_CLEANUP_JOBS);
                /* V1 JSON */
                error = Shell.toJsonReport(operation, result);
            } else {
                error = "Argument error: " + e.toString();
            }
            System.err.println(error);
            if (shell.dontExit()) {
                return;
            }
            System.exit(1);
        }
        shell.start();
        if ((shell.getExitCode() != EXIT_OK) && !shell.dontExit()) {
            System.exit(shell.getExitCode());
        }
    }

    /**
     * A helper class for client login of Admin.
     */
    private class CommandLoginHelper extends LoginHelper {
        private final KVStoreLogin adminLogin = new KVStoreLogin();
        private static final String loginConnErrMsg =
            "Cannot connect to Admin login service %s:%d";
        private LoginManager adminLoginMgr;
        private boolean isSecuredAdmin;

        /* Uses to record the last AFE during loginAdmin() method */
        private AuthenticationFailureException lastAdminAfe;

        /* Password reader used by password renewal */
        private PasswordReader renewPasswordReader;

        public CommandLoginHelper() {
            super();
        }

        /**
         * Updates the login information of admin.
         *
         * @param newUser new user name
         * @param newLoginFile new login file path
         * @throws ShellException if there was a problem loading security
         * configuration information
         */
        private void updateAdminLogin(final String newUser,
                                      final String newLoginFile)
            throws ShellException {

            try {
                adminLogin.updateLoginInfo(newUser, newLoginFile);

                /* Login is needed if SSL transport is used */
                isSecuredAdmin = adminLogin.foundTransportSettings();
                adminLogin.prepareRegistryCSF(regOpenTimeout, regReadTimeout);
                adminLoginMgr = null;
            } catch (IllegalStateException ise) {
                throw new ShellException(ise.getMessage());
            } catch (IllegalArgumentException iae) {
                throw new ShellException(iae.getMessage());
            }
        }

        /**
         * Returns the CommandServiceAPI. If admin is secured, authentication
         * will be conducted using login file or via user interaction.
         *
         * @param host hostname of admin
         * @param port registry port of admin
         * @param forceLogin force login before connection
         * @return commandServiceAPI of admin
         */
        private CommandServiceAPI
            getAuthenticatedAdmin(final String host,
                                  final int port,
                                  final boolean forceLogin)
            throws ShellException,
                   RemoteException,
                   NotBoundException {

            if (forceLogin || (isSecuredAdmin && (adminLoginMgr == null))) {
                loginAdmin(host, port);
            }
            return RegistryUtils.getAdmin(
                host, port, adminLoginMgr, logger);
        }

        private LoginManager getAdminLoginMgr() {
            return adminLoginMgr;
        }

        /*
         * Tries to login the admin.
         */
        private void loginAdmin(String host, int port)
            throws ShellException {

            final LoginCredentials storeCred = checkStoreCreds();
            final String adminLoginUser = adminLogin.getUserName();
            lastAdminAfe = null;

            if (storeCred != null) {
                if (loginAdmin(host, port, storeCred)) {
                    return;
                }
            }

            /* Try anonymous login if user name is not specified. */
            if (adminLoginUser == null) {
                if (loginAdmin(host, port, null /* anonymous loginCreds */)) {
                    return;
                }
                echo("Could not login to Admin as anonymous: " +
                     lastAdminAfe.getMessage() + eol);
            }

            /* Login using explicit username and login file. */
            try {
                if (!loginAdmin(host, port,
                    adminLogin.makeShellLoginCredentials())) {
                    throw new ShellException(
                        "Login failed: " + lastAdminAfe.getMessage(),
                        lastAdminAfe);
                }
            } catch (IOException ioe) {
                throw new ShellException("Failed to get login credentials: " +
                                         ioe.getMessage());
            } catch (IllegalArgumentException iae) {
                throw new ShellException("Login properties error: " +
                                         iae.getMessage());
            } catch (AuthenticationFailureException afe) {
                throw new ShellException("Login failed: " + afe.getMessage(),
                                         afe);
            }
        }

        /**
         * Tries to login to admin using specified credentials.
         *
         * @param hostname the host name of the admin service.
         * @param registryPort the registry port of the admin service.
         * @param loginCreds login credentials. If null, anonymous login will
         * be tried.
         * @return true if and only if login successfully
         * @throws ShellException if fail to connect to admin login service.
         */
        private boolean loginAdmin(String hostname,
                                   int registryPort,
                                   final LoginCredentials loginCreds)
            throws ShellException {

            final String userName =
                (loginCreds == null) ? null : loginCreds.getUsername();
            final AdminLoginManager alm =
                new AdminLoginManager(userName, true, logger);

            try {
                if (alm.bootstrap(hostname, registryPort,
                                  loginCreds)) {
                    setAdminLoginMgr(alm);
                    echo("Logged in to Admin as " +
                         Objects.toString(userName, "anonymous") + eol);
                    return true;
                }
                throw new ShellException(
                    String.format(loginConnErrMsg, hostname, registryPort));
            } catch (PasswordExpiredException pee) {
                if (userName != null) {
                    echo(pee.getMessage() + eol);

                    if (loginCreds instanceof PasswordCredentials &&
                        renewPassword(alm, hostname,
                                      registryPort,
                                      (PasswordCredentials)loginCreds)) {
                        setAdminLoginMgr(alm);
                        echo("Logged in to Admin as " + userName + eol);
                        return true;
                    }
                }
                lastAdminAfe = pee;
                setAdminLoginMgr(null);
                return false;
            } catch (AuthenticationFailureException afe) {
                lastAdminAfe = afe;
                setAdminLoginMgr(null);
                return false;
            }
        }

        private void setAdminLoginMgr(AdminLoginManager alm) {
            if (adminLoginMgr != null) {
                try {
                    adminLoginMgr.logout();
                } catch (SessionAccessException e) {
                }
            }
            adminLoginMgr = alm;
        }

        /**
         * Gets the login manager for RN.
         */
        LoginManager getRNLoginMgr(String[] hostPortList)
            throws ShellException {
            if (!adminLogin.foundTransportSettings()) {
                /*Login is not needed.*/
                return null;
            }

            final LoginCredentials storeCreds = checkStoreCreds();
            if (storeCreds != null) {
               try {
                   LoginManager result = KVStoreLogin.
                       getRepNodeLoginMgr(hostPortList, storeCreds,
                                          null /* storeName */);
                   return result;
               } catch (AuthenticationFailureException afe) {
               }
            }

            try {
                LoginManager result = KVStoreLogin.
                    getRepNodeLoginMgr(hostPortList,
                                       adminLogin.makeShellLoginCredentials(),
                                       null /* storeName */);
                return result;
            } catch (IOException ioe) {
                throw new ShellException("Failed to get login credentials: " +
                    ioe.getMessage());
            } catch (IllegalArgumentException iae) {
                throw new ShellException("Login properties error: " +
                                         iae.getMessage());
            } catch (AuthenticationFailureException afe) {
                throw new ShellException("Login failed: " + afe.getMessage(),
                                         afe);
            }

        }

        /**
         * Check if storeCreds can be used to login.
         */
        private LoginCredentials checkStoreCreds() {
            final String adminLoginUser = adminLogin.getUserName();
            final LoginCredentials storeCred = getCredentials();
            /*
             * When store creds is available, if no admin user is
             * specified, or the admin user is identical with store user,
             * return true to try to login admin using the store creds.
             */
            if (storeCred != null) {
                if ((adminLoginUser == null) ||
                    adminLoginUser.equals(storeCred.getUsername())) {
                    return storeCred;
                }
            }
            return null;

        }

        boolean isSecuredAdmin() {
            return isSecuredAdmin;
        }

        private boolean renewPassword(AdminLoginManager alm,
                                      String hostname,
                                      int registryPort,
                                      final PasswordCredentials oldCreds)
            throws ShellException {
            try {
                /* Prompt for password renewal */
                final PasswordReader READER = getRenewPasswordReader();
                final char[] newPlainPassword =
                    READER.readPassword("Enter the new password: ");
                final String errorMsg =
                    CommandUtils.verifyPassword(READER, newPlainPassword);

                if (errorMsg != null) {
                    throw new ShellException(
                        "Renew password failed: " + errorMsg);
                }

                return alm.renewPassword(hostname, registryPort,
                                         oldCreds, newPlainPassword);
            } catch (AuthenticationFailureException afe) {
                /* Re-throw as ShellException so CLI can output the error */
                throw new ShellException(afe.getMessage(), afe);
            } catch (IOException ioe) {
                throw new ShellException(
                    "Could not read password from console: " + ioe, ioe);
            }
        }

        private boolean isAnonymousLogin() {
            return isSecuredAdmin &&
                adminLogin.getUserName() == null &&
                adminLoginMgr != null &&
                storeLogin.getUserName() == null;
        }

        private PasswordReader getRenewPasswordReader() {
            if (renewPasswordReader == null) {
                renewPasswordReader = new ShellPasswordReader();
            }
            return renewPasswordReader;
        }

        /* test only */
        private void setRenewPasswordReader(PasswordReader reader) {
            renewPasswordReader = reader;
        }
    }

    public boolean isAnonymousLogin() {
        return loginHelper.isAnonymousLogin();
    }

    public boolean isSecuredAdmin() {
        return loginHelper.isSecuredAdmin();
    }

    /* test only */
    public void setRenewPasswordReader(PasswordReader reader) {
        loginHelper.setRenewPasswordReader(reader);
    }

    /*
     * Make all JSON output consistent with ShellCommandResult's fields.
     */
    @Override
    protected String run(String name, String[] args, String line)
        throws ShellException {

        final ShellCommand command = findCommand(name);

        /*
         * Use default run to handle non-JSON and JSON v1 execution output
         * in following situations. JSON v1 handling need to keep alive for
         * existing users. If both command specific JSON and global JSON is
         * not specified, use the default run from Shell.
         */
        if (command == null || command.overrideJsonFlag() ||
            checkArg(args, JSON_V1_FLAG) ||
            (!checkArg(args, JSON_FLAG) && !globalJson)) {
            return super.run(name, args, line);
        }

        String[] cmdArgs = checkCommonFlags(args);

        cmdArgs = checkJson(cmdArgs);

        final ShellCommandResult result =
            command.executeJsonOutput(cmdArgs, this);

        exitCode = command.getExitCode();

        return CommandJsonUtils.handleConversionFailure(
            (CommandJsonUtils.JsonConversionTask<String>)() -> {
                return result.convertToJson();
            });
    }

    /**
     * The helper used to login admin and store with store server-side
     * SSL credentials and InternalLoginManager.
     */
    private class TrustLoginHelper {
        private static final String COMMON_LOGIN_ERROR_MSG =
            "Unable to login with store SSL credentials, ";

        private void login()
            throws ShellException {

            initSocketFactory();

            final String[] hostPorts = getHostPorts(null);
            final Map<HostPort, LoginManager> loginMgrs =
                initLoginMgrs(hostPorts);
            Exception e = null;
            for (Map.Entry<HostPort, LoginManager> entry :
                loginMgrs.entrySet()) {
                try {
                    loginAdmin(entry.getKey(), entry.getValue());
                    break;
                } catch (RuntimeException |
                         NotBoundException |
                         RemoteException exp) {
                    e = exp;
                }
            }
            if (cs == null) {
                throw new ShellException(
                    COMMON_LOGIN_ERROR_MSG + "cannot connect to any admin" +
                    (hostPorts == null ? "." : " at: " +
                    Arrays.toString(hostPorts)), e, ErrorMessage.NOSQL_5300,
                    CommandResult.NO_CLEANUP_JOBS);
            }

            if (kvstoreName == null) {
                echo("Logged in to Admin with store SSL credentials" + eol);
            } else {
                final KVStoreConfig kvstoreConfig =
                    new KVStoreConfig(kvstoreName, hostPorts);
                final ClientId cid = new ClientId(new SecureRandom().nextLong());
                for (LoginManager loginMgr : loginMgrs.values()) {
                    try {
                        loginStore(cid, kvstoreConfig, loginMgr);
                        break;
                    } catch (RuntimeException | KVStoreException exp) {
                        e = exp;
                    }
                }
                if (store == null) {
                    throw new ShellException(
                        COMMON_LOGIN_ERROR_MSG + "cannot connect to store" +
                        (hostPorts == null ? "." : " at: " +
                        Arrays.toString(hostPorts)), e, ErrorMessage.NOSQL_5300,
                        CommandResult.NO_CLEANUP_JOBS);
                }
                echo("Logged in to Admin and store with" +
                     " store SSL credentials" + eol);
            }
        }

        private void initSocketFactory() throws ShellException {
            final File secFile = getSecurityFile();

            try {
                final LoadParameters lp = LoadParameters.getParameters(
                    secFile, logger);
                final SecurityParams sp = new SecurityParams(lp, secFile);
                sp.initRMISocketPolicies(logger);
                final RMISocketPolicy rmiPolicy = sp.getRMISocketPolicy();
                final RMISocketPolicy.SocketFactoryArgs args =
                    new RMISocketPolicy.SocketFactoryArgs()
                        .setCsfName(ClientSocketFactory.registryFactoryName());
                RegistryUtils.setServerRegistryCSF(rmiPolicy.getRegistryCSF(args));
            } catch (RuntimeException re) {
                throw new ShellException(
                    COMMON_LOGIN_ERROR_MSG + "error configuring socket", re);
            }
        }

        private File getSecurityFile() throws ShellException {
            final File secDir = new File(storeSecurityDir);
            if (!secDir.exists()) {
                throw new ShellException(
                    COMMON_LOGIN_ERROR_MSG + secDir.getAbsolutePath() +
                    " doesn't exist");
            }
            final File secFile = new File(secDir, FileNames.SECURITY_CONFIG_FILE);
            if (!secFile.exists()) {
                throw new ShellException(
                    COMMON_LOGIN_ERROR_MSG + secFile.getAbsolutePath() +
                    " doesn't exist");
            }
            return secFile;
        }

        private Map<HostPort, LoginManager> initLoginMgrs(String[] hostPorts) {
            final Map<HostPort, LoginManager> loginManagers = new HashMap<>();
            for (String hostPort : hostPorts) {
                final HostPort curHostPort = HostPort.parse(hostPort);
                final TopologyResolver.SNInfo snInfo =
                    new TopologyResolver.SNInfo(
                        curHostPort.hostname(), curHostPort.port(), null);
                final LoginManager loginMgr = new InternalLoginManager(
                    new TopologyResolver() {
                        @Override
                        public SNInfo getStorageNode(ResourceId rid) {
                            return snInfo;
                        }

                        @Override
                        public List<RepNodeId> listRepNodeIds(int maxReturn) {
                            return null;
                        }
                    },
                    logger);
                loginManagers.put(curHostPort, loginMgr);
            }
            return loginManagers;
        }

        private void loginStore(ClientId clientId,
                                KVStoreConfig kvConfig,
                                LoginManager loginMgr)
            throws KVStoreException {

            final RequestDispatcher handler = RequestDispatcherImpl.
                createForClient(
                    kvConfig, clientId, loginMgr,
                    (t, e) -> logger.log(
                        Level.SEVERE,
                        "Uncaught exception in thread:" + t.getName(),
                        e),
                    logger);
            store = KVStoreInternalFactory.getStore(
                kvConfig, handler, loginMgr, logger);
        }

        private void loginAdmin(HostPort hostPort, LoginManager loginMgr)
            throws NotBoundException, RemoteException {

            cs = RegistryUtils.getAdmin(
                hostPort.hostname(), hostPort.port(), loginMgr);
        }
    }
}
