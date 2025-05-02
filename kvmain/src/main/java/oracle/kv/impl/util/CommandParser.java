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

package oracle.kv.impl.util;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;

import oracle.kv.KVStoreConfig;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.UnixDomainNetworkAddress;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.util.kvlite.KVLite;
import oracle.kv.util.shell.Shell;

/**
 * A base class for programs that have command line arguments.  It implements
 * several common arguments as well as a mechanism for class extension to add
 * more arguments.
 *
 * Usage pattern:
 *   1.  instantiate the object
 *   2.  call parseArgs()
 *
 * These abstract methods must be implemented by extending classes: usage()
 * checkArg() verifyArgs().  See javadoc for details.
 *
 * Arguments supported by this class directly:
 *  host:        -host <hostname> (defaults to "hostname")
 *  port:        -port <port>
 *  storename:   -store <store name>
 *  rootdir:     -root <full_path_to_root_dir>
 *  security file: -security <security file>
 *  username:    -username <username>
 *  usage:       -usage
 *  dns cache ttl: -dns-cachettl <ttl>
 */
public abstract class CommandParser {

    /**
     * Flag strings
     */
    public static final String ROOT_FLAG = "-root";
    public static final String HOST_FLAG = "-host";
    public static final String STORE_FLAG = "-store";
    public static final String PORT_FLAG = "-port";
    public static final String NO_ADMIN_FLAG = "-noadmin";
    public static final String USAGE_FLAG = "-usage";
    public static final String VERBOSE_FLAG = "-verbose";
    public static final String DEBUG_FLAG = "-debug";
    public static final String USER_FLAG = "-username";
    public static final String SECURITY_FLAG = "-security";
    public static final String ADMIN_USER_FLAG = "-admin-username";
    public static final String ADMIN_SECURITY_FLAG = "-admin-security";
    public static final String STORE_SECURITY_DIR_FLAG = "-store-security-dir";
    public static final String TIMEOUT_FLAG = "-timeout";
    public static final String CONSISTENCY_FLAG = "-consistency";
    public static final String DURABILITY_FLAG = "-durability";
    public static final String HELPER_HOSTS_FLAG = "-helper-hosts";
    public static final String NOCONNECT_FLAG = "-noconnect";
    public static final String NOPROMPT_FLAG = "-noprompt";
    public static final String DONTEXIT_FLAG = "-no-exit";
    public static final String HIDDEN_FLAG = "-hidden";
    public static final String NAME_FLAG = "-name";
    public static final String LAST_FLAG = "-last";
    public static final String FROM_FLAG = "-from";
    public static final String TO_FLAG = "-to";
    public static final String ON_FLAG = "on";
    public static final String OFF_FLAG = "off";
    public static final String CONSISTENCY_TIME_FLAG = "-time";
    public static final String PERMISSIBLE_LAG_FLAG = "-permissible-lag";
    public static final String MASTER_SYNC_FLAG = "-master-sync";
    public static final String REPLICA_SYNC_FLAG = "-replica-sync";
    public static final String REPLICA_ACK_FLAG = "-replica-ack";
    public static final String TABLE_FLAG = "-table";
    public static final String FILE_FLAG = "-file";
    public static final String JSON_FLAG = "-json";
    public static final String JSON_V1_FLAG = "-json-v1";
    public static final String DNS_CACHETTL_FLAG = "-dns-cachettl";
    public static final String COMMAND_FLAG = "-command";
    public static final String PRETTY_FLAG = "-pretty";
    public static final String REG_OPEN_TIME_FLAG = "-registry-open-timeout";
    public static final String REG_READ_TIME_FLAG = "-registry-read-timeout";
    public static final String NAMESPACE_FLAG = "-namespace";

    /* -admin is no longer used, but is allowed for compatibility. */
    private static final String ADMIN_FLAG = "-admin";

    public static final int JSON_V1 = 1;

    public static final int JSON_V2 = 2;

    public static final String KVSTORE_USAGE_PREFIX =
        "Usage: java -jar KVHOME/lib/kvstore.jar ";

    public static final String KVCLI_USAGE_PREFIX =
        "Usage: java -jar KVHOME/lib/kvcli.jar ";

    public static final String KVSQLCLI_USAGE_PREFIX =
        "Usage: java -jar KVHOME/lib/sql.jar ";

    protected String rootDir;
    protected String hostname;
    protected String storeName;
    protected String userName;
    protected String securityFile;
    protected String adminUserName;
    protected String adminSecurityFilePath;
    protected boolean runBootAdmin;
    protected int registryPort;
    protected String portStr;
    protected boolean verbose;
    protected boolean json;
    protected boolean jsonV1;
    protected String[] argArray;
    /*
     * DNS cache TTL values. The first value corresponds to
     * ParameterState.COMMON_DNS_CACHE_TTL, the second
     * ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL. Set to default if not
     * specified on the command line.
     */
    protected final int[] dnsCacheTTLs =
        new int[] {
            Integer.parseInt(
                ParameterState.COMMON_DNS_CACHE_TTL_DEFAULT),
            Integer.parseInt(
                ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL_DEFAULT),
        };
    protected int registryOpenTimeout =
        KVStoreConfig.DEFAULT_REGISTRY_OPEN_TIMEOUT;
    protected int registryReadTimeout =
        KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT;
    private final boolean dontParse;

    private String[] rcArgs;
    private String[] inputArgs;
    private int argc;
    private boolean ignoreUnknown;

    public CommandParser(String[] args) {
        this(args, false);
    }

    public CommandParser(String[] args, boolean dontParse) {
        this(args, null, dontParse);
    }

    public CommandParser(String[] args, String[] rcArgs, boolean dontParse) {
        this.dontParse = dontParse;
        if (args == null) {
            throw new IllegalArgumentException();
        }
        inputArgs = args;
        this.rcArgs = rcArgs;
        ignoreUnknown = false;
    }

    public void setDefaults(String rootDir,
                            String storeName,
                            String hostname,
                            int registryPort) {
        this.rootDir = rootDir;
        this.storeName = storeName;
        this.hostname = hostname;
        this.registryPort = registryPort;
        this.portStr = Integer.toString(registryPort);
    }

    public String getPortStr() {
        return portStr;
    }

    public String getHostname() {
        return hostname;
    }

    public String getRootDir() {
        return rootDir;
    }

    public String getStoreName() {
        return storeName;
    }

    public String getUserName() {
        return userName;
    }

    public String getSecurityFile() {
        return securityFile;
    }

    public String getAdminUserName() {
        return adminUserName;
    }

    public String getAdminSecurityFile() {
        return adminSecurityFilePath;
    }

    public int getRegistryPort() {
        return registryPort;
    }

    public boolean isRunBootAdmin() {
        return runBootAdmin;
    }

    public boolean getVerbose() {
        return verbose;
    }

    public boolean getJson() {
        return json || jsonV1;
    }

    public int getJsonVersion() {
        if (!getJson()) {
            return -1;
        }
        return json ? JSON_V2 : JSON_V1;
    }

    public static int getJsonVersion(String[] args) {
        if (Shell.checkArg(args, CommandParser.JSON_FLAG)) {
            return CommandParser.JSON_V2;
        }
        if (Shell.checkArg(args, CommandParser.JSON_V1_FLAG)) {
            return CommandParser.JSON_V1;
        }
        return -1;
    }

    public int[] getDnsCacheTTLs() {
         return dnsCacheTTLs;
    }

    public int getRegistryOpenTimeout() {
        return registryOpenTimeout;
    }

    public int getRegistryReadTimeout() {
        return registryReadTimeout;
    }

    public void verbose(String msg) {
        if (verbose) {
            System.err.println(msg);
        }
    }

    public String nextArg(String arg) {
        if (argc >= argArray.length) {
            usage("Flag " + arg + " requires an argument");
        }
        return argArray[argc++];
    }

    public int nextIntArg(String arg) {
        String next = nextArg(arg);
        try {
            return Integer.parseInt(next);
        } catch (NumberFormatException e) {
            usage("Flag " + arg + " requires an integer argument");
            return 0; // for compiler, usage always ends abnormally
        }
    }

    /**
     * Returns the next argument as a long, calling the usage method if the
     * value is illegal.
     *
     * @param arg the flag associated with the next argument
     * @return the next argument as a long
     */
    public long nextLongArg(final String arg) {
        final String next = nextArg(arg);
        try {
            return Long.parseLong(next);
        } catch (final NumberFormatException e) {
            usage("Flag " + arg + " requires a long argument");
            return 0; // for compiler, usage always ends abnormally
        }
    }

    /**
     * Returns the next argument as a constant of the specified enum type.
     * Converts the value to uppercase for parsing by the enum's valueOf
     * method.  Calls the usage method if the value is illegal.
     *
     * @param <E> the enum type
     * @param arg the flag associated with the next argument
     * @param enumClass the enum class
     * @return a constant of the specified enum type
     */
    public <E extends Enum<E>> E nextEnumArg(final String arg,
                                             final Class<E> enumClass) {
        final String next = nextArg(arg);
        try {
            return E.valueOf(enumClass, next.toUpperCase());
        } catch (final IllegalArgumentException e) {
            usage("Flag " + arg + " requires a constant of enum type " +
                  enumClass.getName() + ", one of: " +
                  Arrays.toString(enumClass.getEnumConstants()));
            return null; // for compiler, usage always ends abnormally
        }
    }

    /**
     * Returns the next argument as a set of constants of the specified enum
     * type.  Looks for multiple values separated by commas, and converts
     * values to uppercase for parsing by the enum's valueOf method.  Calls the
     * usage method if any value is illegal.
     *
     * @param <E> the enum type
     * @param arg the flag associated with the next argument
     * @param enumClass the enum class
     * @return a set of constants of the specified enum type
     */
    public <E extends Enum<E>> EnumSet<E> nextEnumSetArg(
        final String arg, final Class<E> enumClass) {

        final EnumSet<E> set = EnumSet.noneOf(enumClass);
        for (final String value : nextArg(arg).split(",")) {
            try {
                set.add(Enum.valueOf(enumClass, value.toUpperCase()));
            } catch (final IllegalArgumentException e) {
                usage("Flag " + arg +
                      " requires one or more constants of enum type " +
                      enumClass.getName() + ": " +
                      Arrays.toString(enumClass.getEnumConstants()));
            }
        }
        return set;
    }

    protected int getNRemainingArgs() {
        return argArray.length - argc;
    }

    protected void missingArg(String arg) {
        usage("Flag " + arg + " is required");
    }

    private void unknownArg(String arg) {
        usage("Unknown argument: " + arg);
    }

    public boolean isIgnoreUnknownArg() {
        return ignoreUnknown;
    }

    public void parseArgs() {
        if (rcArgs != null) {
            argArray = rcArgs;
            ignoreUnknown = true;
            doParseArgs();
        }
        argArray = inputArgs;
        ignoreUnknown = false;
        doParseArgs();
        verifyArgs();
    }

    /**
     * Validate the hostname by constructing an URI object to check if the
     * hostname is valid for URI. Only allows hostnames with the Unix domain
     * prefix if allowUnixDomain is true.
     */
    @SuppressWarnings("unused")
    protected void validateHostname(final String name,
                                    final boolean allowUnixDomain) {
        if (allowUnixDomain && NetworkAddress.isUnixDomainHostname(name)) {
            final UnixDomainNetworkAddress address = (UnixDomainNetworkAddress)
                NetworkAddress.createNetworkAddress(name, 0);
            final String path = address.getPathname();
            if (path.isEmpty()) {
                return;
            }
            final File file = new File(path);
            final File directory = file.getParentFile();
            if ((directory == null) || !directory.exists()) {
                usage("Unix domain socket directory not found: " + directory);
            } else if (!directory.canWrite()) {
                usage("Unix domain socket directory not writable: " +
                      directory);
            }
        } else {
            try {
                new URI("rmi", name, null, null);
            } catch (URISyntaxException use) {
                usage("Invalid hostname: " + use.getMessage());
            }
        }
    }

    private void doParseArgs() {
        int nArgs = argArray.length;
        argc = 0;
        String errorArg = null;
        runBootAdmin = true;

        /*
         * Set json flag first in case an error is thrown during argument
         * check.
         */
        while (argc < nArgs) {
            final String arg = argArray[argc++];
            if (arg.equals(JSON_FLAG)) {
                json = true;
            } if (arg.equals(JSON_V1_FLAG)) {
                jsonV1 = true;
            }
        }

        if (json && jsonV1) {
            usage("cannot specify -json and -json-v1 together");
        }

        argc = 0;

        while (argc < nArgs) {
            String thisArg = argArray[argc++];

            /* Parse and set dns cache TTL before anything else. */
            if (thisArg.equals(DNS_CACHETTL_FLAG)) {
                parseDnsCacheTTL(nextArg(thisArg));
                ConfigUtils.applyDnsCacheTTL(dnsCacheTTLs, null);
                continue;
            }

            if (dontParse) {
                if (!checkArg(thisArg)) {
                    if (!isIgnoreUnknownArg()) {
                        unknownArg(thisArg);
                    }
                }
                continue;
            }
            if (thisArg.equals(ROOT_FLAG)) {
                rootDir = nextArg(thisArg);
            } else if (thisArg.equals(USAGE_FLAG)) {
                usage(null);
            } else if (thisArg.equals(VERBOSE_FLAG)) {
                verbose = true;
            } else if (thisArg.equals(HOST_FLAG)) {
                hostname = nextArg(thisArg);
                validateHostname(hostname, true /* allowUnixDomain */);
            } else if (thisArg.equals(PORT_FLAG)) {
                portStr = nextArg(thisArg);
                if (!portStr.contains(KVLite.DEFAULT_SPLIT_STR))
                    registryPort = Integer.parseInt(portStr);
            } else if (thisArg.equals(ADMIN_FLAG)) {
                System.err.println
                    ("WARNING: the " + ADMIN_FLAG +
                     " argument is obsolete and was benignly ignored.");
                /* Consume the obsolete arg that should follow -admin*/
                nextArg(thisArg);
            } else if (thisArg.equals(NO_ADMIN_FLAG)) {
                runBootAdmin = false;
            } else if (thisArg.equals(STORE_FLAG)) {
                storeName = nextArg(thisArg);
            } else if (thisArg.equals(USER_FLAG)) {
                userName = nextArg(thisArg);
            } else if (thisArg.equals(SECURITY_FLAG)) {
                securityFile = nextArg(thisArg);
            } else if (thisArg.equals(ADMIN_USER_FLAG)) {
                adminUserName = nextArg(thisArg);
            } else if (thisArg.equals(ADMIN_SECURITY_FLAG)) {
                adminSecurityFilePath = nextArg(thisArg);
            } else if (thisArg.equals(JSON_FLAG)) {
            } else if (thisArg.equals(JSON_V1_FLAG)) {
            } else if (thisArg.equals(REG_OPEN_TIME_FLAG)) {
                registryOpenTimeout = Integer.parseInt(nextArg(thisArg));
            } else if (thisArg.equals(REG_READ_TIME_FLAG)) {
                registryReadTimeout = Integer.parseInt(nextArg(thisArg));
            } else if (!checkArg(thisArg)) {
                if (!isIgnoreUnknownArg()) {
                    errorArg=thisArg;
                }
            }
        }
        if (errorArg != null) {
            unknownArg(errorArg);
        }
    }

    private void parseDnsCacheTTL(String arg) {
        final String[] ttlStrings = arg.split(",");
        dnsCacheTTLs[0] = Integer.parseInt(ttlStrings[0]);
        if (ttlStrings.length == 2) {
            dnsCacheTTLs[1] = Integer.parseInt(ttlStrings[1]);
        } else if (ttlStrings.length > 2) {
            usage("Too many values for " + DNS_CACHETTL_FLAG);
        }
    }

    /**
     * Methods for implementing classes.
     */
    public static String optional(String msg) {
        return "[" + msg + "]";
    }

    public static String getUsage() {
        return optional(USAGE_FLAG);
    }

    public static String getRootUsage() {
        return ROOT_FLAG + " <rootDirectory>";
    }

    public static String getHostUsage() {
        return HOST_FLAG + " <hostname>";
    }

    public static String getStoreUsage() {
        return STORE_FLAG + " <storeName>";
    }

    public static String getPortUsage() {
        return PORT_FLAG + " <port>";
    }

    public static String getNoAdminUsage() {
        return NO_ADMIN_FLAG;
    }

    public static String getUserUsage() {
        return USER_FLAG + " <user>";
    }

    public static String getSecurityUsage() {
        return SECURITY_FLAG + " <security-file-path>";
    }

    public static String getAdminUserUsage() {
        return ADMIN_USER_FLAG + " <adminUser>";
    }

    public static String getAdminSecurityUsage() {
        return ADMIN_SECURITY_FLAG + " <admin-security-file-path>";
    }

    public static String getTimeoutUsage() {
        return TIMEOUT_FLAG + " <timeout ms>";
    }

    public static String getConsistencyUsage() {
        return CONSISTENCY_FLAG +
            " <ABSOLUTE(default) | NONE_REQUIRED | NONE_REQUIRED_NO_MASTER>";
    }

    public static String getDurabilityUsage() {
        return DURABILITY_FLAG +
            " <COMMIT_SYNC(default) | COMMIT_NO_SYNC | COMMIT_WRITE_NO_SYNC>";
    }

    public static String getJsonUsage() {
        return optional(JSON_FLAG + "|" + JSON_V1_FLAG);
    }

    public static String getHelperHostUsage() {
        return HELPER_HOSTS_FLAG + " <host:port[,host:port]*>";
    }

    public static String getDnsCacheTTLUsage() {
        return DNS_CACHETTL_FLAG +
            " <cache ttl in sec[,cache negative ttl in sec]>";
    }

    public static String getRegOpenTimeoutUsage() {
         return REG_OPEN_TIME_FLAG + " <time in ms>";
    }

    public static String getRegReadTimeoutUsage() {
        return REG_READ_TIME_FLAG + " <time in ms>";
    }

    /**
     * Used to extend the base arguments to additional ones.  Returns true if
     * the argument is expected and valid.  Calls usage() if the arg is
     * recognized but invalid.  Returns false if arg is not recognized.
     */
    protected abstract boolean checkArg(String arg);

    /**
     * Called after parsing all args.  Calls usage() if all required arguments
     * are not set.
     */
    protected abstract void verifyArgs();

    /**
     * Prints optional error message, prints usage, then throws an
     * IllegalArgumentException or calls System.exit.  Must not return
     * normally.
     */
    public abstract void usage(String errorMsg);
}
