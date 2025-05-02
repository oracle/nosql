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

package oracle.kv.util.shell;

import static oracle.kv.impl.api.table.TableImpl.validateNamespace;
import static oracle.kv.impl.util.CommandParser.CONSISTENCY_FLAG;
import static oracle.kv.impl.util.CommandParser.DURABILITY_FLAG;
import static oracle.kv.impl.util.CommandParser.FROM_FLAG;
import static oracle.kv.impl.util.CommandParser.HOST_FLAG;
import static oracle.kv.impl.util.CommandParser.LAST_FLAG;
import static oracle.kv.impl.util.CommandParser.NAME_FLAG;
import static oracle.kv.impl.util.CommandParser.OFF_FLAG;
import static oracle.kv.impl.util.CommandParser.ON_FLAG;
import static oracle.kv.impl.util.CommandParser.PORT_FLAG;
import static oracle.kv.impl.util.CommandParser.SECURITY_FLAG;
import static oracle.kv.impl.util.CommandParser.TIMEOUT_FLAG;
import static oracle.kv.impl.util.CommandParser.TO_FLAG;
import static oracle.kv.impl.util.CommandParser.USER_FLAG;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.LoginCredentials;
import oracle.kv.StatementResult;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.client.DdlJsonFormat;
import oracle.kv.impl.query.shell.output.ResultOutputFactory;
import oracle.kv.impl.query.shell.output.ResultOutputFactory.OutputMode;
import oracle.kv.impl.query.shell.output.ResultOutputFactory.ResultOutput;
import oracle.kv.impl.security.PasswordExpiredException;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.KVStoreLogin.CredentialsProvider;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.HostPort;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.util.ErrorMessage;

/*
 * This class encapsulates some sharable utility commands:
 *  history, timer, page, connect store.
 */
public abstract class CommonShell extends Shell {

    protected String kvstoreName = null;
    private String storeHostname = null;
    private int storePort = 0;
    private String storeUser = null;
    private String storeSecurityFile = null;
    private List<String> helperHosts = null;
    protected String[] commandToRun;

    private int nextCommandIdx = 0;
    private boolean noprompt = false;
    private boolean noconnect = false;

    /*
     * This is an internal flag used by tests to allow them to
     * call CommandShell.main without exiting.
     */
    private boolean dontExit = false;

    private Integer storeTimeout = null;
    private Consistency storeConsistency = null;
    private Durability storeDurability = null;

    private LoginHelper loginHelper = null;
    protected KVStore store = null;
    private KVStoreConfig kvstoreConfig = null;
    private int pageHeight = 0;
    private String namespace = null;

    /* Default consistency policy used for this connection. */
    final static Consistency CONSISTENCY_DEF = Consistency.ABSOLUTE;
    /*
     * By default, use a more aggressive default Durability to make
     * sure that records get into a database.
     */
    final static Durability DURABILITY_DEF = Durability.COMMIT_SYNC;

    /* Default request timeout used for this connection. */
    final static int REQUEST_TIMEOUT_DEF = 5000;

    private static Map<String, Consistency> consistencyMap;
    private static Map<String, Durability> durabilityMap;
    static {
        Map<String, Consistency> consMap = new HashMap<>();
        consMap.put("NONE_REQUIRED", Consistency.NONE_REQUIRED);
        @SuppressWarnings("deprecation")
        final Consistency NONE_REQUIRED_NO_MASTER =
            Consistency.NONE_REQUIRED_NO_MASTER;
        consMap.put("NONE_REQUIRED_NO_MASTER", NONE_REQUIRED_NO_MASTER);
        consMap.put("ABSOLUTE", Consistency.ABSOLUTE);
        consistencyMap = Collections.unmodifiableMap(consMap);

        Map<String, Durability> durMap = new HashMap<>();
        durMap.put("COMMIT_SYNC", Durability.COMMIT_SYNC);
        durMap.put("COMMIT_NO_SYNC", Durability.COMMIT_NO_SYNC);
        durMap.put("COMMIT_WRITE_NO_SYNC", Durability.COMMIT_WRITE_NO_SYNC);
        durabilityMap = Collections.unmodifiableMap(durMap);
    }

    public CommonShell(InputStream input,
                       PrintStream output) {
        this(input, output, null);
    }

    public CommonShell(InputStream input,
                       PrintStream output,
                       String[] maskFlags) {
        super(input, output, true, maskFlags);
    }

    public KVStore getStore() throws ShellException {
        if (store == null) {
            throw new ShellException("Not Connected.");
        }
        return store;
    }

    public KVStoreConfig getKVStoreConfig() {
        return kvstoreConfig;
    }

    public void setStoreConsistency(Consistency consistency) {
        storeConsistency = consistency;
    }

    public Consistency getStoreConsistency() {
        return (storeConsistency != null) ?
                storeConsistency : CONSISTENCY_DEF;
    }

    public static String getConsistencyName(Consistency consistency) {
        if (consistency instanceof Consistency.Time) {
            final Consistency.Time tcons = (Consistency.Time) consistency;
            return tcons.getName() + "[permissibleLag_ms=" +
                tcons.getPermissibleLag(TimeUnit.MILLISECONDS) +
                ", timeout_ms=" + tcons.getTimeout(TimeUnit.MILLISECONDS) +
                "]";
        }
        for (Entry<String, Consistency> entry : consistencyMap.entrySet()) {
            if (consistency.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    public void setStoreDurability(Durability durability) {
        storeDurability = durability;
    }

    public Durability getStoreDurability() {
        return (storeDurability != null) ?
                storeDurability : DURABILITY_DEF;
    }

    public static String getDurabilityName(Durability durability) {
        for (Entry<String, Durability> entry : durabilityMap.entrySet()) {
            if (durability.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return "Durability[MasterSync=" + durability.getMasterSync() +
            ", ReplicaSync=" + durability.getReplicaSync() +
            ", ReplicaAck= " + durability.getReplicaAck() + "]";
    }

    public void setRequestTimeout(int timeout) {
        storeTimeout = timeout;
    }

    public int getRequestTimeout() {
        return (storeTimeout != null) ?
                storeTimeout.intValue() : REQUEST_TIMEOUT_DEF;
    }

    public void connectStore() throws ShellException {

        connectStore(null);
    }

    public void connectStore(List<String> extraHostPorts)
        throws ShellException {

        if (kvstoreName != null) {
            getLoginHelper().updateStoreLogin(storeUser, storeSecurityFile);
            openStore(extraHostPorts);
        }
    }

    void setPageHeight(int height) {
        pageHeight = height;
    }

    public int getPageHeight() {
        if (pageHeight == 0) {
            final ShellInputReader inputReader = getInput();
            return (inputReader == null) ? -1 : inputReader.getTerminalHeight();
        }
        return pageHeight;
    }

    public boolean isPagingEnabled() {
        return (getPageHeight() >= 0);
    }

    public boolean isNoPrompt() {
        return noprompt;
    }

    public boolean isNoConnect() {
        return noconnect;
    }

    public boolean dontExit() {
        return dontExit;
    }

    public static Consistency getConsistency(String name) {
        return consistencyMap.get(name.toUpperCase());
    }

    public static Set<String> getConsistencyNames() {
        return consistencyMap.keySet();
    }

    public static Durability getDurability(String name) {
        return durabilityMap.get(name.toUpperCase());
    }

    public static Set<String> getDurabilityNames() {
        return durabilityMap.keySet();
    }

    public void setNamespace(final String namespace) {
        if (namespace != null) {
            validateNamespace(namespace);
        }
        this.namespace = namespace;
    }

    public String getNamespace() {
        return namespace;
    }

    public ExecuteOptions getExecuteOptions() {
        return new ExecuteOptions()
            .setConsistency(getStoreConsistency())
            .setTimeout(getRequestTimeout(), TimeUnit.MILLISECONDS)
            .setNamespace(getNamespace());
    }

    public void setLoginHelper(LoginHelper loginHelper) {
        this.loginHelper = loginHelper;
    }

    public LoginHelper getLoginHelper() {
        if (loginHelper == null) {
            loginHelper = new LoginHelper();
        }
        return loginHelper;
    }

    public void openStore(String host,
                          int port,
                          String storeName,
                          String user,
                          String securityFile)
        throws ShellException {

        openStore(host, port, storeName, user, securityFile,
                  null /* timeout */, null /* consistency */,
                  null /* durability */);
    }

    public void openStore(String host,
                          int port,
                          String storeName,
                          String user,
                          String securityFile,
                          Integer timeout,
                          Consistency consistency,
                          Durability durability)
        throws ShellException {

        storeHostname = host;
        storePort = port;
        kvstoreName = storeName;
        if (timeout != null) {
            setRequestTimeout(timeout);
        }
        if (consistency != null) {
            setStoreConsistency(consistency);
        }
        if (durability != null) {
            setStoreDurability(durability);
        }

        try {
            getLoginHelper().updateStoreLogin(user, securityFile);
        } catch (IllegalStateException ise) {
            throw new ShellException(ise.getMessage());
        } catch (IllegalArgumentException iae) {
            output.println(iae.getMessage());
        }
        openStore();
    }

    protected String[] getHostPorts(List<String> extraHostPorts) {
        List<String> hostports = new ArrayList<>();
        if (storeHostname != null) {
            hostports.add(storeHostname + ":" + storePort);
        } else {
            hostports.addAll(helperHosts);
        }
       if (extraHostPorts != null) {
           hostports.addAll(extraHostPorts);
       }
        return hostports.toArray(new String[0]);
    }

    public void openStore() throws ShellException {
        openStore(null);
    }

    /* Open the store. */
    public void openStore(List<String> extraHostPorts)
        throws ShellException {

        final String[] hostports = getHostPorts(extraHostPorts);
        kvstoreConfig = new KVStoreConfig(kvstoreName, hostports);

        final Durability durability = getStoreDurability();
        kvstoreConfig.setDurability(durability);

        final Consistency consistency = getStoreConsistency();
        kvstoreConfig.setConsistency(consistency);

        final int requestTimeout = getRequestTimeout();
        kvstoreConfig.setRequestTimeout(requestTimeout, TimeUnit.MILLISECONDS);
        /* Make sure socket timeout >= request timeout */
        final long socketTimeout =
            kvstoreConfig.getSocketReadTimeout(TimeUnit.MILLISECONDS);
        if (!kvstoreConfig.getUseAsync() && (socketTimeout < requestTimeout)) {
            kvstoreConfig.setSocketReadTimeout(requestTimeout,
                                               TimeUnit.MILLISECONDS);
        }

        try {
            store = getLoginHelper().getAuthenticatedStore(kvstoreConfig);
        } catch (ShellException se) {
            throw se;
        } catch (Exception e) {
            throw new ShellException("Cannot connect to " + kvstoreName +
                " at " + (storeHostname != null ?
                          (storeHostname + ":" + storePort) :
                          joinHostsWithComma(hostports)),
                e);
        }
    }

    private String joinHostsWithComma(String[] hostports) {
        StringBuilder sb = new StringBuilder();
        for (String hp : hostports) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(hp);
        }
        return sb.toString();
    }

    public void closeStore() {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    public abstract class ShellParser extends CommandParser {
        private List<String> reqFlags;

        /*
         * The flag indicates that if there is a non-hidden argument
         * provided.
         */
        private boolean hasNonHiddenArg = false;

        /*
         * Hidden flags: -noprompt, -noconnect, -hidden, -verbose, -debug,
         *               -no-exit, help flag(-help, help, -?, ?)
         */
        public ShellParser(String[] args,
                           String[] rcArgs,
                           String[] requiredFlags) {
            /*
             * The true argument tells CommandParser that this class will
             * handle all flags, not just those unrecognized.
             */
            super(args, rcArgs, true);
            if (requiredFlags != null) {
                reqFlags = new ArrayList<>();
                reqFlags.addAll(Arrays.asList(requiredFlags));
            }
        }

        @Override
        protected void verifyArgs() {
            /*
             * If no argument except those hidden flags is provided, then
             * don't try to connect to a store.
             */
            if (!hasNonHiddenArg && !noconnect) {
                noconnect = true;
            }

            if (!noconnect) {
                if (reqFlags != null && reqFlags.size() > 0) {
                    usage("Missing required argument");
                }
            }
            if ((commandToRun != null) &&
                (nextCommandIdx < commandToRun.length)) {
                usage("Flags may not follow commands");
            }
        }

        /* Abstract function to get the usage of shell. */
        public abstract String getShellUsage();

        @Override
        public void usage(String errorMsg) {
            String error = "";
            if (errorMsg != null) {
                error += errorMsg + eol;
            }
            error += getShellUsage();
            if (argArray != null &&
                Shell.checkArg(argArray, CommandParser.JSON_FLAG)) {
                String operation = "";
                for (String arg: argArray) {
                    operation += arg + " ";
                }
                CommandResult result =
                    new CommandResult.CommandFails(
                        error, ErrorMessage.NOSQL_5100,
                        CommandResult.NO_CLEANUP_JOBS);
                final boolean isJsonV1 =
                    Shell.checkArg(argArray, CommandParser.JSON_V1_FLAG);
                if (isJsonV1) {
                    error = Shell.toJsonReport(operation, result);
                } else {
                    error =
                        ShellCommandResult.toJsonReport(operation, result);
                }
            }

            System.err.println(error);
            System.exit(1);
        }

        public boolean checkExtraArg(@SuppressWarnings("unused") String arg) {
            return false;
        }

        @Override
        protected boolean checkArg(String arg) {

            /*
             * In order to allow an embedded command to use the same flags as
             * the shell executable, if the commandToRun is set, just pass them
             * through.
             */
            if (checkHiddenFlags(arg)) {
                return true;
            }

            if (commandToRun == null) {
                checkRequiredFlag(arg);

                if (HOST_FLAG.equals(arg)) {
                    storeHostname = nextArg(arg);
                    return true;
                } else if (PORT_FLAG.equals(arg)) {
                    storePort = Integer.parseInt(nextArg(arg));
                    return true;
                } else if (HELPER_HOSTS_FLAG.equals(arg)) {
                    addHelperHosts(nextArg(arg));
                    return true;
                } else if (STORE_FLAG.equals(arg)) {
                    kvstoreName = nextArg(arg);
                    return true;
                } else if (USER_FLAG.equals(arg)) {
                    storeUser = nextArg(arg);
                    return true;
                } else if (SECURITY_FLAG.equals(arg)) {
                    storeSecurityFile = nextArg(arg);
                    return true;
                } else if (TIMEOUT_FLAG.equals(arg)) {
                    storeTimeout = nextIntArg(arg);
                    if(storeTimeout <= 0) {
                        usage("Flag " + arg + " requires a positive integer");
                    }
                    return true;
                } else if (CONSISTENCY_FLAG.equals(arg)) {
                    final String cname = nextArg(arg);
                    storeConsistency = getConsistency(cname);
                    if (storeConsistency == null) {
                        usage(CommandParser.getConsistencyUsage());
                    }
                    return true;
                } else if (DURABILITY_FLAG.equals(arg)) {
                    final String dname = nextArg(arg);
                    storeDurability = getDurability(dname);
                    if (storeDurability == null) {
                        usage(CommandParser.getDurabilityUsage());
                    }
                    return true;
                } else if (REG_OPEN_TIME_FLAG.equals(arg)) {
                    registryOpenTimeout = Integer.parseInt(nextArg(arg));
                    if(registryOpenTimeout <= 0) {
                        usage("Flag " + arg + " requires a positive integer");
                    }
                    return true;
                } else if (REG_READ_TIME_FLAG.equals(arg)) {
                    registryReadTimeout = Integer.parseInt(nextArg(arg));
                    if(registryReadTimeout <= 0) {
                        usage("Flag " + arg + " requires a positive integer");
                    }
                    return true;
                } else if (checkExtraArg(arg)) {
                    return true;
                }
            }
            if (!isIgnoreUnknownArg()) {
                addToCommand(arg);
            }
            return true;
        }

        /*
         * Check if the input argument is one of the hidden flags:
         *  -noconnect, -noprompt, -verbose, -debug, -hidden or
         *  help flag (-help, help, -?, ?).
         *
         * If the argument is a help flag for shell but not for commandToRun,
         * then print the usage message of shell and exit current program.
         */
        private boolean checkHiddenFlags(final String arg) {

            if (NOCONNECT_FLAG.equals(arg)) {
                noconnect = true;
                return true;
            }

            if (NOPROMPT_FLAG.equals(arg)) {
                noprompt = true;
                return true;
            }

            if (DONTEXIT_FLAG.equals(arg)) {
                dontExit = true;
                return true;
            }

            if (commandToRun == null && isHelpFlag(arg)) {
                usage(null);
            }

            if (VERBOSE_FLAG.equals(arg)) {
                setVerbose(true);
            } else if (DEBUG_FLAG.equals(arg)) {
                setDebug(true);
            } else if (HIDDEN_FLAG.equals(arg)) {
                setHidden(true);
            } else if (JSON_FLAG.equals(arg)) {
                setJson(true);
            } else if (!checkExtraHiddenFlag(arg)) {
                if (!hasNonHiddenArg) {
                    hasNonHiddenArg = true;
                }
                return false;
            }

            /* Some commands take this flag as well */
            if (commandToRun != null) {
                addToCommand(arg);
            }
            return true;
        }

        protected boolean checkExtraHiddenFlag(@SuppressWarnings("unused")
                                               final String arg) {
            return false;
        }

        private void checkRequiredFlag(String arg) {
            if (reqFlags == null || reqFlags.isEmpty()) {
                return;
            }
            if (reqFlags.contains(arg)) {
                reqFlags.remove(arg);
            }
        }

        private void addHelperHosts(String arg) {
            helperHosts = new ArrayList<>();
            String[] hostports = arg.split(",");
            helperHosts.addAll(Arrays.asList(hostports));
        }

        /*
         * Add unrecognized args to the commandToRun array.
         */
        private void addToCommand(String arg) {
            if (commandToRun == null) {
                commandToRun = new String[getNRemainingArgs() + 1];
            }
            commandToRun[nextCommandIdx++] = arg;
        }

        @Override
        public String getHostname() {
            return storeHostname;
        }

        @Override
        public int getRegistryPort() {
            return storePort;
        }

        @Override
        public String getStoreName() {
            return kvstoreName;
        }

        @Override
        public String getUserName() {
            return storeUser;
        }

        @Override
        public String getSecurityFile() {
            return storeSecurityFile;
        }

        public List<String> getHelperHosts() {
            return helperHosts;
        }
    }

    /**
     * A helper class for client login of store.
     */
    public static class LoginHelper implements CredentialsProvider {
        protected final KVStoreLogin storeLogin = new KVStoreLogin();
        private LoginCredentials storeCreds;
        private boolean isSecuredStore;

        /**
         * Updates the login information of store.
         *
         * @param newUser new user name
         * @param newLoginFile new login file path
         */
        void updateStoreLogin(final String newUser,
                              final String newLoginFile) {
            storeLogin.updateLoginInfo(newUser, newLoginFile);
            isSecuredStore = storeLogin.foundTransportSettings();
            storeCreds = null;
        }


        /**
         * Returns a kvstore handler. If the store is secured, authentication
         * will be conducted using login file or via user interaction.
         *
         * @param config the kvconfig specified by user
         * @return kvstore handler
         */
        protected KVStore getAuthenticatedStore(final KVStoreConfig config)
            throws ShellException {

            config.setSecurityProperties(storeLogin.getSecurityProperties());

            try {
                if (isSecuredStore && storeCreds == null) {
                    storeCreds = storeLogin.makeShellLoginCredentials();
                }
                return KVStoreFactory.getStore(config, storeCreds,
                    KVStoreLogin.makeReauthenticateHandler(this));
            } catch (PasswordExpiredException pee) {
                storeCreds = null;
                String errorMsg = "Login failed: Password is expired. " +
                                "Log into the Admin CLI to renew your " +
                                "password.";
                throw new ShellException(errorMsg, pee);
            } catch (AuthenticationFailureException afe) {
                storeCreds = null;
                String errorMsg = "Login failed: " + afe.getMessage();
                throw new ShellException(errorMsg, afe);
            } catch (IOException ioe) {
                throw new ShellException("Failed to get login credentials: " +
                                         ioe.getMessage());
            } catch (IllegalArgumentException iae) {
                throw new ShellException(
                    "Login properties error: " + iae.getMessage());
            }
        }

        @Override
        public LoginCredentials getCredentials() {
            return storeCreds;
        }
    }

    /* ConnectStore command */
    public static class ConnectStoreCommand extends ShellCommand {
        static final String NAME = "connect";

        public static final String CONNECT_STORE_COMMAND_DESC =
            "Connects to a KVStore to perform data access functions." +
            eolt + "If the instance is secured, you may need to provide " +
            "login credentials.";

        public static final String CONNECT_STORE_COMMAND_ARGUMENTS =
            CommandParser.optional(CommandParser.getHostUsage()) + " " +
            CommandParser.optional(CommandParser.getPortUsage()) + " " +
            NAME_FLAG + " <storeName>" + eolt +
            CommandParser.optional(CommandParser.getTimeoutUsage()) + " " +
            eolt +
            CommandParser.optional(CommandParser.getConsistencyUsage()) +
            eolt +
            CommandParser.optional(CommandParser.getDurabilityUsage()) +
            eolt +
            CommandParser.optional(CommandParser.getUserUsage()) + " " +
            CommandParser.optional(CommandParser.getSecurityUsage());

        public static final String CONNECT_STORE_COMMAND_SYNTAX =
            NAME + " " + CONNECT_STORE_COMMAND_ARGUMENTS;

        public ConnectStoreCommand() {
            super(NAME, 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return execute(args, shell, new ArrayList<HostPort>());
        }


        public String execute(String[] args, Shell shell,
            List<HostPort> extraHostports) throws ShellException {

            Shell.checkHelp(args, this);
            String hostname = null;
            int port = 0;
            String storeName = null;
            String user = null;
            String security = null;
            Integer timeout = null;
            Consistency consistency = null;
            Durability durability = null;

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (CommandParser.HOST_FLAG.equals(arg)) {
                    hostname = Shell.nextArg(args, i++, this);
                } else if (PORT_FLAG.equals(arg)) {
                    port = parseInt(Shell.nextArg(args, i++, this));
                    if (port < 1 || port > 65535) {
                        invalidArgument(arg);
                    }
                } else if (NAME_FLAG.equals(arg)) {
                    storeName = Shell.nextArg(args, i++, this);
                } else if (USER_FLAG.equals(arg)) {
                    user = Shell.nextArg(args, i++, this);
                } else if (SECURITY_FLAG.equals(arg)) {
                    security = Shell.nextArg(args, i++, this);
                } else if (TIMEOUT_FLAG.equals(arg)) {
                    timeout = parseInt(Shell.nextArg(args, i++, this));
                    if (timeout <= 0) {
                        invalidArgument(arg);
                    }
                } else if (CONSISTENCY_FLAG.equals(arg)) {
                    final String cname = Shell.nextArg(args, i++, this);
                    consistency = getConsistency(cname);
                    if (consistency == null) {
                        invalidArgument(arg);
                    }
                } else if (DURABILITY_FLAG.equals(arg)) {
                    final String dname = Shell.nextArg(args, i++, this);
                    durability = getDurability(dname);
                    if (durability == null) {
                        invalidArgument(arg);
                    }
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (storeName == null) {
                shell.requiredArg(NAME_FLAG, this);
            }

            /* Close current store, open new one.*/
            final CommonShell cshell = (CommonShell) shell;
            cshell.closeStore();

            List<HostPort> hostports = new ArrayList<>();
            if (hostname != null && port != 0) {
                hostports.add(new HostPort(hostname, port));
            } else {
                if (cshell.helperHosts != null) {
                    /*
                     * If a list of helperhost is used as argument of the shell,
                     * then -host and -port should be specified either both or
                     * not at all. Specifying only one of them cause the
                     * exception that miss the another argument.
                     */
                    if (hostname == null && port != 0) {
                        shell.requiredArg(HOST_FLAG, this);
                    } else if (hostname != null && port == 0) {
                        shell.requiredArg(PORT_FLAG, this);
                    }
                    assert(hostname == null && port == 0);
                    for (String helperHost : cshell.helperHosts) {
                        hostports.add(HostPort.parse(helperHost));
                    }
                } else {
                    if (hostname == null) {
                        hostname = cshell.storeHostname;
                    }
                    if (port == 0) {
                        port = cshell.storePort;
                    }
                    hostports.add(new HostPort(hostname, port));
                }
                if (extraHostports != null) {
                    hostports.addAll(extraHostports);
                }
            }

            boolean connected = false;
            for (HostPort hostPort : hostports) {
                try {
                    hostname = hostPort.hostname();
                    port = hostPort.port();
                    cshell.openStore(hostname, port, storeName, user, security,
                                     timeout, consistency, durability);
                    connected = true;
                    break;
                } catch (ShellException ignored) {
                }
            }

            if (!connected) {
                String fmt = "Failed to connect to %s at %s" + eol +
                    "Warning: You are no longer connected to a store.";
                String helperHosts = Arrays.toString(
                     hostports.toArray(new HostPort[hostports.size()]));
                throw new ShellException(String.format(fmt, storeName,
                                                       helperHosts));
            }
            String ret = "Connected to " + storeName + " at " + hostname
                + ":" + port + ".";
            if(timeout != null) {
                ret += " Set timeout: " + timeout + "ms.";
            }
            if(consistency != null) {
                ret += " Set consistency: " + consistency + ".";
            }
            if(durability != null) {
                ret += " Set durability: " + durability + ".";
            }
            return ret;
        }

        @Override
        protected String getCommandSyntax() {
            return CONNECT_STORE_COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return CONNECT_STORE_COMMAND_DESC;
        }
    }

    /* Time command */
    public static class TimeCommand extends ShellCommand {
        private final static String NAME = "timer";

        final static String SYNTAX = NAME + " " +
            CommandParser.optional(ON_FLAG + " | " + OFF_FLAG);
        final static String DESCRIPTION = "Turns the measurement and display " +
            "of execution time for commands on or off.";

        public TimeCommand() {
            super(NAME, 5);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            if (args.length > 2) {
                shell.badArgCount(this);
            } else if (args.length > 1) {
                String arg = args[1];
                if (ON_FLAG.equals(arg)) {
                    shell.setTimer(true);
                } else if (OFF_FLAG.equals(arg)) {
                    shell.setTimer(false);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            return "Timer is now " + (shell.getTimer() ? "on" : "off");
        }

        @Override
        public String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    /* Page command */
    public static class PageCommand extends ShellCommand {
        private final static String NAME = "page";

        final static String SYNTAX = NAME + " " +
            CommandParser.optional(ON_FLAG + " | <n> | " + OFF_FLAG);
        final static String DESCRIPTION =
            "Turns query output paging on or off.  If specified, n is used " +
            "as the page" + eolt + "height. If n is 0, or 'on' is specified " +
            "the default page height is used." + eolt + "'off' turns paging " +
            "off.";

        public PageCommand() {
            super(NAME, 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommonShell comShell = (CommonShell)shell;
            int height;
            if (args.length > 2) {
                shell.badArgCount(this);
            } else if (args.length > 1) {
                String arg = args[1];
                if (ON_FLAG.equals(arg)) {
                    comShell.setPageHeight(0);
                } else if (OFF_FLAG.equals(arg)) {
                    comShell.setPageHeight(-1);
                } else {
                    height = parseInt(arg);
                    if (height < 1) {
                        invalidArgument(arg);
                    }
                    comShell.setPageHeight(height);
                }
            }
            if (comShell.isPagingEnabled()) {
                return "Paging mode is now on, height: " +
                    comShell.getPageHeight();
            }
            return "Paging mode is now off";
        }

        @Override
        public String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    /* History command */
    public static class HistoryCommand extends ShellCommand {
        final static String NAME = "history";

        final static String SYNTAX = NAME + " " +
            CommandParser.optional(LAST_FLAG + " <n>") + " " +
            CommandParser.optional(FROM_FLAG + " <n>") + " " +
            CommandParser.optional(TO_FLAG + " <n>");

        final static String DESCRIPTION =
            "Displays command history.  By default all history is " +
            "displayed." + eolt + "Optional flags are used to choose " +
            "ranges for display";

        public HistoryCommand() {
            super(NAME, 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            Shell.CommandHistory history = shell.getHistory();
            int from = 0;
            int to = history.getSize();
            boolean isLast = false;

            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (LAST_FLAG.equals(arg)) {
                        from = parseInt(Shell.nextArg(args, i++, this));
                        isLast = true;
                    } else if (FROM_FLAG.equals(arg)) {
                        from = parseInt(Shell.nextArg(args, i++, this));
                    } else if (TO_FLAG.equals(arg)) {
                        to = parseInt(Shell.nextArg(args, i++, this));
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }
                if (isLast) {
                    from = history.getSize() - from + 1;
                }
            }
            /*
             * The index of command are shown as 1-based index in
             * the output, so covert it to 0-based index when locating
             * it in CommandHistory list.
             */
            return history.dump(toZeroBasedIndex(from), toZeroBasedIndex(to));
        }

        private int toZeroBasedIndex(int index) {
            return (index > 0) ? (index - 1) : 0;
        }

        @Override
        protected String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    public static class DebugCommand extends ShellCommand {

        private static String NAME = "debug";

        final static String SYNTAX = NAME + " " +
            CommandParser.optional(ON_FLAG + " | " + OFF_FLAG);

        final static String DESCRIPTION =
            "Toggles or sets the global debug setting.  This property can " +
            "also" + eolt + "be set per-command using the -debug flag.";

        public DebugCommand() {
            super(NAME, 4);
        }

        @Override
        public boolean isHidden() {
            return true;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            if (args.length > 2) {
                shell.badArgCount(this);
            } else if (args.length > 1) {
                String arg = args[1];
                if (ON_FLAG.equals(arg)) {
                    shell.setDebug(true);
                } else if (OFF_FLAG.equals(arg)) {
                    shell.setDebug(false);
                } else {
                    return "Invalid argument: " + arg + eolt +
                        getBriefHelp();
                }
            } else {
                shell.toggleDebug();
            }
            return "Debug mode is now " + (shell.getDebug()? "on" : "off");
        }

        @Override
        protected String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    public static class VerboseCommand extends ShellCommand {

        private final static String NAME = "verbose";

        final static String SYNTAX = NAME + " " +
            CommandParser.optional(ON_FLAG + " | " + OFF_FLAG);

        final static String DESCRIPTION =
            "Toggles or sets the global verbosity setting.  This " +
            "property can also" + eolt + "be set per-command using " +
            "the -verbose flag.";

        public VerboseCommand() {
            super(NAME, 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            if (args.length > 2) {
                shell.badArgCount(this);
            } else if (args.length > 1) {
                String arg = args[1];
                if (ON_FLAG.equals(arg)) {
                    shell.setVerbose(true);
                } else if (OFF_FLAG.equals(arg)) {
                    shell.setVerbose(false);
                } else {
                    return "Invalid argument: " + arg + eolt +
                        getBriefHelp();
                }
            } else {
                shell.toggleVerbose();
            }
            return "Verbose mode is now " + (shell.getVerbose()? "on" : "off");
        }

        @Override
        protected String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    public static class HiddenCommand extends ShellCommand {

        private final static String NAME = "hidden";

        final static String SYNTAX = NAME + " " +
            CommandParser.optional(ON_FLAG + " | <n> | " + OFF_FLAG);

        final static String DESCRIPTION =
            "Toggles visibility of commands and flags that are normally" +
            eolt + "hidden. Use these only if advised to do so by Oracle " +
            "Support.";

        public HiddenCommand() {
            super(NAME, 3);
        }

        @Override
        protected boolean isHidden() {
            return true;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            if (args.length > 2) {
                shell.badArgCount(this);
            } if (args.length > 1) {
                String arg = args[1];
                if (ON_FLAG.equals(arg)) {
                    shell.setHidden(true);
                } else if (OFF_FLAG.equals(arg)) {
                    shell.setHidden(false);
                } else {
                    return "Invalid argument: " + arg + eolt +
                        getBriefHelp();
                }
            } else {
                shell.toggleHidden();
            }
            return "Hidden commands and parameters are " +
                (shell.getHidden() ? "enabled" : "disabled");
        }

        @Override
        protected String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    /* Namespace command */
    public static class NamespaceCommand extends ShellCommand {

        final static String NAME = "namespace";
        final static String SYNTAX = NAME + " " +
            CommandParser.optional("namespace");
        final static String DESCRIPTION =
            "Sets or clears the default namespace to use for table " +
            "operations and queries";

        public NamespaceCommand() {
            super(NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommonShell commonShell = ((CommonShell)shell);
            if (args.length > 2) {
                shell.badArgCount(this);
            } else if (args.length > 1) {
                commonShell.setNamespace(args[1]);
            } else {
                /* clear the namespace */
                commonShell.setNamespace(null);
            }

            return ("Namespace is " +
                    NameUtils.switchToExternalUse(commonShell.getNamespace()));
        }

        @Override
        public String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    /**
     * Display the result depending on its type and outcome. Operations that
     * ended in error show their error messages and status info, operations
     * like 'describe' and 'show' display their results, and operations that
     * generate plan execution return status info.
     */
    public String displayDDLResults(StatementResult result) {

        if (result.getErrorMessage() != null) {
            return result.getErrorMessage() + "\n" + result.getInfo();
        }

        /*
         * Statement was successful, output for describes, shows, and noop
         * should be displayed.
         */
        if (result.isSuccessful()) {
            if (result.getResult() != null) {
                return result.getResult();
            }
            if (result.getInfo().equals(DdlJsonFormat.NOOP_STATUS)) {
                return result.getInfo();
            }
            return "Statement completed successfully";
        }

        /*
         * For now, all info is returned as unstructured text. In the future,
         * when the execute option can specify json or not, we may return
         * status info in json form.
         */
        return "Statement did not complete successfully:\n" +
            result.getInfo();
    }

    public String displayDMLResults(final OutputMode outputMode,
                                    final StatementResult result)
        throws ShellException {

        return displayDMLResults(outputMode, result,
                                 isPagingEnabled(), getOutput());
    }

    public String displayDMLResults(final OutputMode outputMode,
                                    final StatementResult result,
                                    final boolean isPagingEnabled,
                                    final PrintStream queryOutput)
        throws ShellException {

        if (result.getErrorMessage() != null) {
            return result.getErrorMessage() + "\n" + result.getInfo();
        }

        final ResultOutput resultOutput =
            ResultOutputFactory.getOutput(outputMode, this, queryOutput,
                                          result.getResultDef(),
                                          result.iterator(),
                                          isPagingEnabled,
                                          getPageHeight());

        try {
            long num = resultOutput.outputResultSet();
            return String.format("\n%d %s returned", num,
                                 ((num > 1) ? "rows" : "row"));
        } catch (IllegalArgumentException iae) {
            throw new ShellException("Failed to display result set: " +
                                     iae.getMessage(), iae);
        } catch (IOException ioe) {
            throw new ShellException("Failed to display result set: " +
                                     ioe.getMessage(), ioe);
        }
    }

    public void start() {
        init();

        /*
         * For testing, once exit code is set, give up following execution
         * in order to output a single error message.
         */
        if (getExitCode() != 0 && dontExit()) {
            shutdown();
            return;
        }

        try {
            if (commandToRun != null) {
                try {
                    String result = run(commandToRun[0], commandToRun);
                    output.println(result);
                } catch (ShellException se) {
                    handleShellException(commandToRun[0], se);
                } catch (Exception e) {
                    handleUnknownException(commandToRun[0], e);
                }
            } else {
                loop();
            }
        } finally {
            shutdown();
        }
    }
}
