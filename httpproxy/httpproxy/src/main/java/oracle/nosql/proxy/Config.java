/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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
package oracle.nosql.proxy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStoreConfig;
import oracle.kv.RequestLimitConfig;

/**
 * Contains all configuration parameters for the proxy.
 * Each parameter can be specified via several mechanisms. The mechanisms, and
 * order of precedence in lowest to highest are:
 *
 * a) default values
 * b) a properties file
 * c) environment variable
 * d) command line arguments
 *
 * For example, if a param is set these ways, the order of precedence is:
 * - set via property file, env var, command line:
 *          the command line value is used
 * - set via property file only:
 *           the property file is used
 * - not set:
 *           the default value, if any is used.
 *
 * A parameter named "foobar" can be specified these ways:
 *
 * in a property file:
 *      foobar=123
 *
 * as an env variable:
 *      export foobar=123
 *
 * on the command line
 *      -foobar 123
 */
public class Config {

    public enum ProxyType {KVPROXY,    /* on-prem */
                           CLOUD,      /* NDCS */
                           CLOUDSIM,   /* cloudsim */
                           CLOUDTEST}  /* test proxy, uses a mocked tenant
                                          manager and access checker, but sets
                                          up all the cloud services */

    enum ParamType {STRING, INT, FILE, BOOL, DOUBLE}

    enum ParamContext {CLOUD, ON_PREM, ALL, HIDDEN}

    /* ------- Params ---------*/

    /*
     * A collection of all param definitions, used for validation and iteration.
     * Adding a parameter definition here will automatically add it to the
     * parameterDefinitions map. Parameters can optionally have default values.
     */
    private static Map<String, ParamDef> paramDefinitions = new TreeMap<>();

    /*
     * Params for all proxy types
     */
    public static ParamDef PROXY_TYPE =
        new ParamDef("proxyType", "KVPROXY", ParamContext.ALL);
    public static ParamDef CONFIG_FILE =
        new ParamDef("config", ParamType.FILE, ParamContext.ALL);
    public static ParamDef HOSTNAME =
        new ParamDef("hostname", ParamContext.ALL);
    public static ParamDef HTTP_PORT =
        new ParamDef("httpPort", "80", ParamType.INT, ParamContext.ALL);
    public static ParamDef HTTPS_PORT =
        new ParamDef("httpsPort", "443", ParamType.INT, ParamContext.ALL);

    /*
     * This is used to size the number of Netty threads used to handle incoming
     * requests from the network. If REQUEST_THREAD_POOL_SIZE is 0 these threads
     * will also be used to handle the requests through sending the request
     * to KV. In this case the number should be relatively high in order to
     * keep up with traffic. If REQUEST_THREAD_POOL_SIZE is non-zero
     * requests are handled using a separate pool. If that is the case this
     * number can be smaller. If 0 Netty defaults to 2 * nCPUs (maybe 3x,
     * depending on netty release)
     */
    public static ParamDef NUM_REQUEST_THREADS =
        new ParamDef("numRequestThreads", "32", ParamType.INT,
                     ParamContext.ALL);
    public static ParamDef NUM_ACCEPT_THREADS =
        new ParamDef("numAcceptThreads", "3", ParamType.INT, ParamContext.ALL);
    public static ParamDef IDLE_READ_TIMEOUT =
        new ParamDef("idleReadTimeout", "0", ParamType.INT, ParamContext.ALL);
    public static ParamDef MONITOR_STATS_ENABLED =
        new ParamDef("monitorStatsEnabled", "false",
                     ParamType.BOOL, ParamContext.ALL);
    /*
     * Set to non-zero to use a thread pool separate from Netty for handling
     * requests. The pool is sized based on this parameter
     */
    public static ParamDef REQUEST_THREAD_POOL_SIZE =
        new ParamDef("requestThreadPoolSize", "0", ParamType.INT, ParamContext.ALL);
    /*
     * Set to non-zero to use a thread pool to handle async responses from
     * KV client calls rather than using the KV threads themselves. This
     * parameter is only used if ASYNC is true
     */
    public static ParamDef KV_THREAD_POOL_SIZE =
        new ParamDef("kvThreadPoolSize", "0",
                     ParamType.INT, ParamContext.ALL);

    /**  Wallet file that holds the salt value for the query cache */
    public static ParamDef SALT_WALLET =
        new ParamDef("saltWallet", ParamType.FILE, ParamContext.CLOUD);
    public static ParamDef VERBOSE =
        new ParamDef("verbose", "false",
                     ParamType.BOOL, ParamContext.ALL);
    public static ParamDef ASYNC =
        new ParamDef("async", "false",
                     ParamType.BOOL, ParamContext.ALL);
    public static ParamDef FORCE_V3 =
        new ParamDef("forceV3", "false",
                     ParamType.BOOL, ParamContext.HIDDEN);
    public static ParamDef FORCE_V4 =
        new ParamDef("forceV4", "false",
                     ParamType.BOOL, ParamContext.HIDDEN);
    /* NO_LIMITS works with cloudsim in a no-limits, but still cloudsim mode */
    public static ParamDef NO_LIMITS =
        new ParamDef("noLimits", "false",
                     ParamType.BOOL, ParamContext.CLOUD);

    /** Enable query tracing. Off by default */
    public static ParamDef QUERY_TRACING =
        new ParamDef("queryTrace", "false",
                     ParamType.BOOL, ParamContext.HIDDEN);

    /* enable internal IAM auth retries */
    public static ParamDef AUTH_RETRIES_ENABLED =
        new ParamDef("authRetriesEnabled", "false",
                     ParamType.BOOL, ParamContext.CLOUD);

    /* maximum active/pending retries overall */
    public static ParamDef MAX_ACTIVE_RETRY_COUNT =
        new ParamDef("maxActiveRetryCount", "50",
                     ParamType.INT, ParamContext.CLOUD);

    /* maximum retries per client */
    public static ParamDef MAX_RETRIES_PER_REQUEST =
        new ParamDef("maxRetriesPerRequest", "10",
                     ParamType.INT, ParamContext.CLOUD);

    /* Retry delay, in milliseconds */
    public static ParamDef RETRY_DELAY_MS =
        new ParamDef("retryDelayMs", "30",
                     ParamType.INT, ParamContext.CLOUD);

    /*
     * parameters for configuring the table cache in the cloud
     * TABLE_CACHE_EXPIRATION_SEC - how long an entry will remain in the
     *   cache after its last access (inactivity time)
     * TABLE_CACHE_REFRESH_SEC - How often entries will be
     *   refreshed while still in the cache
     * TABLE_CACHE_CHECK_INTERVAL_SEC - how often to check if any entries
     *   need to be expired (evicted) or refreshed
     */
    /* expire after 15 minutes of inactivity */
    public static ParamDef TABLE_CACHE_EXPIRATION_SEC =
        new ParamDef("tableCacheExpirationSec", "900",
                     ParamType.INT, ParamContext.CLOUD);
    /*
     * refresh entries every 10 minutes
     *
     * Note that the SC config FLUSH_PROXY_CACHE_TIMEOUT_SEC depends on this
     * tableCacheRefreshSec, any change to the default value of this parameter
     * should also be applied to FLUSH_PROXY_CACHE_TIMEOUT_SEC in SCConfig
     */
    public static ParamDef TABLE_CACHE_REFRESH_SEC =
        new ParamDef("tableCacheRefreshSec", "600",
                     ParamType.INT, ParamContext.CLOUD);
    /* check for expired or need refresh entries every 10 seconds */
    public static ParamDef TABLE_CACHE_CHECK_INTERVAL_SEC =
        new ParamDef("tableCacheCheckIntervalSec", "10",
                     ParamType.INT, ParamContext.CLOUD);

    /*
     * Configurations for the kv client handle used by the proxy for accessing
     * the kvstore
     */
    public static ParamDef KV_CONSISTENCY =
        new ParamDef("kvConsistency", "NONE_REQUIRED", ParamContext.ALL);
    public static ParamDef KV_DURABILITY =  new ParamDef("kvDurability",
                                                         "COMMIT_NO_SYNC",
                                                         ParamContext.ALL);
    public static ParamDef KV_REQUEST_TIMEOUT =
        new ParamDef("kvRequestTimeout", "-1", ParamType.INT,
                     ParamContext.ALL);
    public static ParamDef KV_REQUEST_LIMIT =
        new ParamDef("kvRequestLimit", "-1", ParamType.INT,
                     ParamContext.ALL);

    /*
     * On-prem only parameters
     */
    /** Filename of login.properties file for kvclient authentication */
    public static ParamDef STORE_SECURITY_FILE =
        new ParamDef("storeSecurityFile", ParamType.FILE,
                     ParamContext.ON_PREM);

    /* If SSL is set */
    public static ParamDef SSL_CERTIFICATE =
        new ParamDef("sslCertificate", ParamType.FILE, ParamContext.ON_PREM);
    public static ParamDef SSL_PRIVATE_KEY =
        new ParamDef("sslPrivateKey", ParamType.FILE, ParamContext.ON_PREM);
    public static ParamDef SSL_PRIVATE_KEY_PASS =
        new ParamDef("sslPrivateKeyPass", ParamContext.ON_PREM);
    public static ParamDef SSL_SECURITY_DIR =
        new ParamDef("sslSecurityDir", ParamType.FILE, ParamContext.ON_PREM);
    public static ParamDef SSL_CIPHERS =
        new ParamDef("sslCiphers", ParamType.STRING, ParamContext.ON_PREM);
    public static ParamDef SSL_PROTOCOLS =
        new ParamDef("sslProtocols", "TLSv1.2,TLSv1.1,TLSv1",
                     ParamType.STRING, ParamContext.ON_PREM);

    /** Store name of the target kvstore */
    public static ParamDef STORE_NAME =
        new ParamDef("storeName", ParamContext.ON_PREM);
    /** helper hosts of the target kvstore */
    public static ParamDef HELPER_HOSTS =
        new ParamDef("helperHosts", ParamContext.ON_PREM);

    /*
     * Cloud only parameters
     */
    /** Cloud only: True if the proxy should do limiting based on errors */
    public static ParamDef ERROR_LIMITING_ENABLED =
        new ParamDef("errorLimitingEnabled", "false",
                     ParamType.BOOL, ParamContext.CLOUD);

    /** Cloud only: SC httpRequest connect timeout in seconds. */
    public static ParamDef SC_REQUEST_CONNECT_TIMEOUT_SEC =
        new ParamDef("scRequestConnectTimeoutSec", "0",
                     ParamType.INT, ParamContext.CLOUD);

    /** Cloud only: SC httpRequest read timeout in seconds. */
    public static ParamDef SC_REQUEST_READ_TIMEOUT_SEC =
        new ParamDef("scRequestReadTimeoutSec", "0",
                      ParamType.INT, ParamContext.CLOUD);

    /** Cloud only: SC httpRequest latency threshold for logging at INFO */
    public static ParamDef SC_LATENCY_INFO_THRESHOLD_MS =
        new ParamDef("scLatencyInfoThresholdMs", "0",
                      ParamType.INT, ParamContext.CLOUD);

    /* to ignore unknown parameters (for future compatibility) */
    public static ParamDef IGNORE_UNKNOWN =
        new ParamDef("ignoreUnknownFields", "false",
                     ParamType.BOOL, ParamContext.CLOUD);

    /**
     * The interval of pulling persistent rules task, used in PullRulesThread
     * of FilterHandler
     */
    public static ParamDef PULL_RULES_INTERVAL_SEC =
            new ParamDef("pullRulesIntervalSec", "60",
                         ParamType.INT, ParamContext.CLOUD);

    /* ErrorManager config settings */
    /*
     *  delayResponseThreshold: Number of errors per second from any one
     *  IP that are allowed before this manager starts delaying responses.
     */
    public static ParamDef ERROR_DELAY_RESPONSE_THRESHOLD =
            new ParamDef("errorDelayResponseThreshold", "5",
                         ParamType.INT, ParamContext.CLOUD);
    /*
     *  delayResponseMs: Amount of time to delay responses, in millis.
     */
    public static ParamDef ERROR_DELAY_RESPONSE_MS =
            new ParamDef("errorDelayResponseMs", "200",
                         ParamType.INT, ParamContext.CLOUD);
    /*
     *  dnrThreshold: Number of errors per second from any one IP that
     *  will trigger DNR (Do Not Respond). This must be equal or
     *  greater than delayResponseThreshold.
     */
    public static ParamDef ERROR_DNR_THRESHOLD =
            new ParamDef("errorDnrThreshold", "10",
                         ParamType.INT, ParamContext.CLOUD);

    /*
     *  errorCreditMs: Use N milliseconds of error "credit" from the
     *  past before limiting. For example, if this value is set to 3000,
     *  and errorThreshold is 5, and no errors have happened for an IP
     *  in the last 3 seconds, allow 15 errors this second before
     *  starting to delay responses.
     */
    public static ParamDef ERROR_CREDIT_MS =
            new ParamDef("errorCreditMs", "1000",
                         ParamType.INT, ParamContext.CLOUD);
    /*
     *  errorCacheSize: Maximum number of unique IP addresses to track
     *  error rates for.
     */
    @Deprecated
    public static ParamDef ERROR_CACHE_SIZE =
            new ParamDef("errorCacheSize", "10000",
                         ParamType.INT, ParamContext.CLOUD);
    /*
     *  errorCacheLifetimeMs: Maximum amount of time, in millis, to keep
     *  track of any specific IP error rate.
     */
    public static ParamDef ERROR_CACHE_LIFETIME_MS =
            new ParamDef("errorCacheLifetimeMs", "3600000",
                         ParamType.INT, ParamContext.CLOUD);
    /*
     *  errorDelayPoolSize: Size of thread pool used to manage delaying
     *  responses. This is also used once per minute to update stats.
     */
    public static ParamDef ERROR_DELAY_POOL_SIZE =
            new ParamDef("errorDelayPoolSize", "5",
                         ParamType.INT, ParamContext.CLOUD);

    /*
     * DRL: Distributed Rate Limiting
     */
    public static ParamDef DRL_ENABLED =
        new ParamDef("drlEnabled", "false",
                     ParamType.BOOL, ParamContext.CLOUD);

    /*
     * DRLUseDistributed: if false, rate limiting will be done independent
     * of any other rate limiting services. If true, rate limiting will be
     * distributed across services.
     */
    public static ParamDef DRL_USE_DISTRIBUTED =
        new ParamDef("drlUseDistributed", "false",
                     ParamType.BOOL, ParamContext.CLOUD);
    /*
     * DRLTableName: name of the KV table to use to track distributed
     * rate limiting information
     */
    public static ParamDef DRL_TABLE_NAME =
            new ParamDef("drlTableName", "DRLTable",
                         ParamType.STRING, ParamContext.CLOUD);
    /*
     * DRLUpdateIntervalMs: interval between distributed updates
     * This defaults to 200ms (update 5 times per second).
     */
    public static ParamDef DRL_UPDATE_INTERVAL_MS =
            new ParamDef("drlUpdateIntervalMs", "200",
                         ParamType.INT, ParamContext.CLOUD);
    /*
     * DRLDelayPoolSize: size of thread pool for sending back delayed responses
     */
    public static ParamDef DRL_DELAY_POOL_SIZE =
            new ParamDef("drlDelayPoolSize", "5",
                         ParamType.INT, ParamContext.CLOUD);
    /*
     * DRLRateFactor: a "fudge factor" so limiting can allow more (or less)
     * than the specified table rates. This may be adjusted based on rate
     * limiter testing in real cloud environments.
     */
    public static ParamDef DRL_RATE_FACTOR =
            new ParamDef("drlRateFactor", "1.0",
                         ParamType.DOUBLE, ParamContext.CLOUD);
    /*
     * DRLCreditMs: amount of "credit" to allow limiters, in millis
     */
    public static ParamDef DRL_CREDIT_MS =
            new ParamDef("drlCreditMs", "1000",
                         ParamType.INT, ParamContext.CLOUD);
    /*
     * DRLOptInRequired: if false, rate limiting will be done for all
     * tables. If true, only requests that opt-in will use DRL.
     * This is intended to be temporary, while we test the DRL system in
     * production environments. Eventually this should be false by default.
     */
    public static ParamDef DRL_OPT_IN_REQUIRED =
        new ParamDef("drlOptInRequired", "true",
                     ParamType.BOOL, ParamContext.CLOUD);

    /** cloud only: table cache warmup */
    public static ParamDef WARMUP_FILE =
        new ParamDef("warmupFile", ParamType.STRING, ParamContext.CLOUD);

    /* warmup file recency: warmupFile must be no older than this */
    public static ParamDef WARMUP_FILE_RECENCY_MS =
            new ParamDef("warmupFileRecencyMs", "600000",
                         ParamType.INT, ParamContext.CLOUD);

    /* warmup maximum time: don't spend more than this warming up cache */
    public static ParamDef WARMUP_TIME_MS =
            new ParamDef("warmupTimeMs", "20000",
                         ParamType.INT, ParamContext.CLOUD);

    /* warmup file save interval */
    public static ParamDef WARMUP_FILE_SAVE_INTERVAL_MS =
            new ParamDef("warmupFileSaveIntervalMs", "10000",
                         ParamType.INT, ParamContext.CLOUD);

    public static ParamDef IAM_CONFIG_FILE =
        new ParamDef("iamConfigFile", ParamType.FILE, ParamContext.CLOUD);

    /**
     * Cloud only: False if disable child table, it is enabled by default.
     * It only affects the CLOUD mode with SC not for CLOUDSIM.
     */
    public static ParamDef CHILD_TABLE_ENABLED =
            new ParamDef("childTableEnabled", "true",
                         ParamType.BOOL, ParamContext.CLOUD);

    /**
     * Cloud only: true if enabled customer managed encryption key(CMEK), it is
     * disabled by default.
     */
    public static ParamDef CMEK_ENABLED =
            new ParamDef("cmekEnabled", "false",
                         ParamType.BOOL, ParamContext.CLOUD);

    /* ------- End params ---------*/

    /*
     * Support for customizing the kvclient configuration.  Keep maps of
     * consistency and durability string values to actual values for ease of
     * parameter translation.
     */
    private static Map<String, Consistency> consistencyMap;
    private static Map<String, Durability> durabilityMap;
    private static final Set<String> VALID_SSL_PROTOCOLS = new HashSet<>();
    static {
        Map<String, Consistency> consMap = new HashMap<>();
        consMap.put("NONE_REQUIRED", Consistency.NONE_REQUIRED);
        consMap.put("ABSOLUTE", Consistency.ABSOLUTE);
        consistencyMap = Collections.unmodifiableMap(consMap);

        /* Support the standard KV durabilities:
         *
         * COMMIT_SYNC requires sync d from the master only,
         *                      no_sync durability from replicas
         * COMMIT_NO_SYNC requires no_sync from all shard members
         * COMMIT_WRITE_NO_SYNC requires write_no_sync durability from the
         *                      master, no_sync durability from replicas
         */

        Map<String, Durability> durMap = new HashMap<>();
        durMap.put("COMMIT_SYNC", Durability.COMMIT_SYNC);
        durMap.put("COMMIT_NO_SYNC", Durability.COMMIT_NO_SYNC);
        durMap.put("COMMIT_WRITE_NO_SYNC", Durability.COMMIT_WRITE_NO_SYNC);

        /*
         * In addition to the three standard convenience durabilities above, add
         * more Durability policies
         *
         * COMMIT_ALL_SYNC requires sync from both master and replica
         * COMMIT_ALL_WRITE_NO_SYNC requires write_no_sync from both master and
         */
        durMap.put("COMMIT_ALL_SYNC",
                   new Durability(Durability.SyncPolicy.SYNC,
                                  Durability.SyncPolicy.SYNC,
                                  Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));
        durMap.put("COMMIT_ALL_WRITE_NO_SYNC",
                   new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                                  Durability.SyncPolicy.WRITE_NO_SYNC,
                                  Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

        durabilityMap = Collections.unmodifiableMap(durMap);

        VALID_SSL_PROTOCOLS.add("SSLv2");
        VALID_SSL_PROTOCOLS.add("SSLv3");
        VALID_SSL_PROTOCOLS.add("TLSv1");
        VALID_SSL_PROTOCOLS.add("TLSv1.1");
        VALID_SSL_PROTOCOLS.add("TLSv1.2");
        VALID_SSL_PROTOCOLS.add("TLSv1.3");
    }

    /* Cache of the params values */
    private final Properties paramVals;

    /*
     * useSSL is a computed param and is inferred to be true when the SSL
     * certificate and private key are set or a security directory contains
     * store.keys and store.wallet(EE)/store.passwd(CE) is set.
     */
    private boolean useSSL;

    /*
     * This kvstoreConfig object is used to hold attributes of the kvclient
     * handle. It doesn't need to have a valid storename and helper hosts,
     * as those are spliced in later by the tenant manager
     */
    private final KVStoreConfig templateKVStoreConfig;

    /**
     * Initialize the config object. Meant to be used by a main program, when
     * parameters come in as tthe argv.
     */
    public Config(String [] argv) {
        this(convertCommandLine(argv));
    }

    /**
     * Initialize the config object, passing in the commandline as a
     * Properties object. Useful for tests, so that they can mimic the behavior
     * of a command line call.
     */
    public Config(Properties commandLineVals) {
        paramVals = loadParams(commandLineVals);
        validate();
        templateKVStoreConfig = makeTemplateKVStoreConfig();
    }

    /*
     * return true if the properties contain IGNORE_UNKNOWN set to
     * true.
     */
    private static boolean ignoreUnknown(Properties props) {
        String val = props.getProperty(IGNORE_UNKNOWN.paramName);
        if (val == null) {
            return false;
        }
        return Boolean.parseBoolean(val);
    }

    private static void warnUnknown(String paramName) {
        System.err.println("Warning: ignoring unknown parameter '" +
                           paramName + "'");
    }

    /** Convert the command line arg to a Properties object */
    private static Properties convertCommandLine(String[] argv) {

        Properties commandLineVals = new Properties();
        if (argv.length == 0) {
            return commandLineVals;
        }

        /*
         * Special cases for -help [type] and -version
         */
        if (argv[0].toLowerCase().equals("-version") ||
            argv[0].toLowerCase().equals("version")) {
            System.err.println("Proxy version: " +
                               DataServiceHandler.getProxyVersion());
            System.exit(0);
        }
        if (argv[0].toLowerCase().equals("-help") ||
            argv[0].toLowerCase().equals("help")) {
            if (argv.length > 1) {
                try {
                    ProxyType type = ProxyType.valueOf(argv[1]);
                    printUsage(type);
                } catch (IllegalStateException iae) {
                    usageAndThrow(iae.getMessage(), null);
                }
            } else {
                printUsage(null);
            }
            System.exit(0);
        }

        /*
         *  We expect the argument vector to be composed of pairs of
         * -argType <argVal>
         */
        if ((argv.length%2) != 0) {
            usageAndThrow
                ("Arguments should be in pairs, in the form of -arg <val> ");
        }

        Set<String> validParamNames = paramDefinitions.keySet();
        for (int i = 0; i < argv.length; i++) {
            String argKey = argv[i];

            if (!argKey.startsWith("-")) {
                usageAndThrow(argKey + " must start with -");
            }

            String paramName = argKey.substring(1);
            if (!validParamNames.contains(paramName)) {
                if (ignoreUnknown(commandLineVals)) {
                    warnUnknown(paramName);
                } else {
                    usageAndThrow(argKey + " is not a valid argument");
                }
            }

            String val = argv[++i];
            commandLineVals.setProperty(paramName, val);
        }
        return commandLineVals;
    }

    private Properties loadParams(Properties commandLineVals) {

        /* Find all the environment variables */
        Map<String, String> envVars = System.getenv();
        Set<String> envNames = envVars.keySet();

        Properties loadedProps = new Properties();

        Set<String> legitParamNames = paramDefinitions.keySet();

        /*
         * Was a config file specified via command line or environment
         * variable? If so, load the file.
         */
        String configFileName =
            commandLineVals.getProperty(CONFIG_FILE.paramName);
        if (configFileName == null) {
            if (envNames.contains(CONFIG_FILE.paramName)) {
                configFileName = envVars.get(CONFIG_FILE.paramName);
            }
        }

        if (configFileName != null) {
            /* read in the file contents */
            Properties fromFileProps = new Properties();
            try {
                fromFileProps.load(new FileInputStream(configFileName));
            } catch (IOException e) {
                usageAndThrow("Couldn't read file " + configFileName);
            }

            for (@SuppressWarnings("unchecked")
                 Enumeration<String> filePropNames =
                     (Enumeration<String>) fromFileProps.propertyNames();
                 filePropNames.hasMoreElements();) {

                String paramName = filePropNames.nextElement();
                if (legitParamNames.contains(paramName)) {
                    loadedProps.setProperty
                        (paramName,
                         fromFileProps.getProperty(paramName));
                } else if (ignoreUnknown(fromFileProps) ||
                           ignoreUnknown(commandLineVals)) {
                    warnUnknown(paramName);
                } else {
                    usageAndThrow(configFileName +
                                  " has this unknown property " + paramName);
                }
            }
        }

        /*
         * Check the environment vars and see if they should override any param
         * vals
         */
        for (Map.Entry<String, String> entry: envVars.entrySet()) {
            String paramName = entry.getKey();
            if (legitParamNames.contains(paramName)) {
                loadedProps.setProperty(paramName, entry.getValue());
            }
        }

        /* Was the value specified via command line? Load that last. */
        for (String paramName: commandLineVals.stringPropertyNames()) {
            if (legitParamNames.contains(paramName)) {
                loadedProps.setProperty(paramName,
                                        commandLineVals.getProperty(paramName));
            }
        }

        return loadedProps;
    }

    private static void printUsage(ProxyType type) {
        StringBuilder sb = new StringBuilder();
        sb.append("Usage:\n");
        sb.append("\t[-help <KVPROXY|CLOUD>]\n");
        sb.append("\t[-version]\n\n");

        for (Map.Entry<String, ParamDef> entry: paramDefinitions.entrySet()) {
            ParamDef def = entry.getValue();
            if (type != null && !contextMatch(type, def.context)) {
                continue;
            }
            /* don't show cloud options unless CLOUD is given */
            if (type == null && (def.context == ParamContext.CLOUD ||
                                 def.context == ParamContext.HIDDEN)) {
                continue;
            }
            sb.append("\t[-").append(entry.getKey()).append(" <");
            if (def.defaultVal !=null) {
                sb.append("default: ").append(def.defaultVal);
            }
            sb.append(">]\n");
        }
        System.err.println(sb.toString());
    }

    private static void usageAndThrow(String message) {
        usageAndThrow(message, null);
    }

    private static void usageAndThrow(String message, ProxyType type) {
        System.err.println(message);
        printUsage(type);
        throw new IllegalArgumentException(message);
    }

    /**
     * Check that parameters are valid values.
     *
     * Note that the caller checks whether the appropriate and required
     * arguments are provided for each type of Proxy. This class only checks if
     * configuration parameters are valid values, not whether they're correct
     * for the type of proxy.
     */
    private void validate() {
        /* For all parameters that have values, check that the value is valid */
        for (ParamDef definition: paramDefinitions.values()) {
            definition.validateIfExists(paramVals);
        }

        /*
         * Check combinations of params
         */

        validateSSL();

        final File storeSecurityFile = getStoreSecurityFile();
        if (storeSecurityFile != null) {
            final String filePath = storeSecurityFile.getAbsolutePath();
            /*
             * This check assuming the password store is wallet, so it cannot
             * be used for KVPROXY
             */
            if (!getProxyType().equals(ProxyType.KVPROXY)) {
                ensureClientSecurity(filePath);
            }

            /*
             * Set the value of the security file as a parameter.
             * NOTE: this is an odd intervention when run with Docker,
             * because environment variables are usually set explicitly.
             * Perhaps we need to change our directions and not go through this
             * level of indirection. TODO: lql
             */
            System.setProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY,
                               filePath);
        }
    }

    /**
     * Decide if we're using SSL.
     */
    private void validateSSL() {
        /*
         * If security dir is set, use cert and private key from security dir,
         * ignore the cert and private key parameter values.
         */
        final File securityDir = getSSLSecurityDir();
        if (securityDir != null) {
            useSSL = true;
            reConfigHttpPort();
            return;
        }

        /*
         * If the ssl cert or private key is set, it implies that we're using
         * SSL
         */
        final File SSLCert = getSSLCertificate();
        final File SSLPrivateKey = getSSLPrivateKey();

        if ((SSLCert != null) && (SSLPrivateKey != null)) {
            useSSL = true;
        } else if ((SSLCert == null) && (SSLPrivateKey == null)) {
            useSSL = false;
        } else {
            throw new IllegalArgumentException
                ("Both " + SSL_CERTIFICATE + " and " + SSL_PRIVATE_KEY +
                 " must be specified if using https");
        }
        String[] protocols = getSSLProtocols();
        if (useSSL && protocols != null) {
            for (String protocol : protocols) {
                if (!VALID_SSL_PROTOCOLS.contains(protocol)) {
                    throw new IllegalArgumentException
                        (protocol +" is not a valid SSL protocol name." +
                         " Must be one of " + VALID_SSL_PROTOCOLS +
                         ", format protocol[,protocol]*");
                }
            }
        }
        reConfigHttpPort();
    }

    /*
     * Make sure we start the http server with only http OR https, by
     * clearing the appropriate port value. Since we've set d efault values
     * for http and https ports for convenience, take the opportunity to
     * set the non-used port to 0, to avoid setting up that connection.
     */
    private void reConfigHttpPort() {
        if (useSSL()) {
            setHttpPort(0);
        } else {
            setHttpsPort(0);
        }
    }

    public ProxyType getProxyType() {
        String proxyVal = paramVals.getProperty(PROXY_TYPE.paramName,
                                                PROXY_TYPE.defaultVal);
        return ProxyType.valueOf(proxyVal);
    }

    /* Used by tests */
    public void setProxyType(ProxyType pType) {
        paramVals.setProperty(PROXY_TYPE.paramName, pType.toString());
    }

    public int getNumRequestThreads() {
        return getInt(NUM_REQUEST_THREADS);
    }

    public void setNumRequestThreads(int numRequestThreads) {
        paramVals.setProperty(NUM_REQUEST_THREADS.paramName,
                              Integer.toString(numRequestThreads));
    }

    public int getNumAcceptThreads() {
        return getInt(NUM_ACCEPT_THREADS);
    }

    public void setNumAcceptThreads(int numThreads) {
        paramVals.setProperty(NUM_ACCEPT_THREADS.paramName,
                              Integer.toString(numThreads));
    }

    public int getRequestThreadPoolSize() {
        return getInt(REQUEST_THREAD_POOL_SIZE);
    }

    public void setRequestThreadPoolSize(int size) {
        paramVals.setProperty(REQUEST_THREAD_POOL_SIZE.paramName,
                              Integer.toString(size));
    }

    public int getKVThreadPoolSize() {
        return getInt(KV_THREAD_POOL_SIZE);
    }

    public void setKVThreadPoolSize(int size) {
        paramVals.setProperty(KV_THREAD_POOL_SIZE.paramName,
                              Integer.toString(size));
    }

    public String getHostname() {
        return paramVals.getProperty(HOSTNAME.paramName);
    }

    public int getHttpPort() {
        return getInt(HTTP_PORT);
    }

    private void setHttpPort(int httpPort) {
        paramVals.setProperty(HTTP_PORT.paramName, Integer.toString(httpPort));
    }

    public int getHttpsPort() {
        return getInt(HTTPS_PORT);
    }

    private void setHttpsPort(int httpsPort) {
        paramVals.setProperty(HTTPS_PORT.paramName,
                              Integer.toString(httpsPort));
    }

    public int getIdleReadTimeout() {
        return getInt(IDLE_READ_TIMEOUT);
    }

    public int getScRequestConnectTimeoutSec() {
        return getInt(SC_REQUEST_CONNECT_TIMEOUT_SEC);
    }

    public int getScRequestReadTimeoutSec() {
        return getInt(SC_REQUEST_READ_TIMEOUT_SEC);
    }

    public int getScLatencyInfoThresholdMs() {
        return getInt(SC_LATENCY_INFO_THRESHOLD_MS);
    }

    public String getStoreName() {
        return paramVals.getProperty(STORE_NAME.paramName,
                                     STORE_NAME.defaultVal);
    }

    public String[] getHelperHosts() {
        /*
         * helperHosts is of the format:
         *  host:port[,host:port]*
         * Remove white space, then split
         */
        String helperString =
            paramVals.getProperty(HELPER_HOSTS.paramName,
                                  STORE_NAME.defaultVal);
        if (helperString == null) {
            return new String[0];
        }
        return helperString.replaceAll("\\s", "").split(",");
    }

    public File getSSLCertificate() {
        return getFile(SSL_CERTIFICATE);
    }

    public File getSSLSecurityDir() {
        return getFile(SSL_SECURITY_DIR);
    }

    File getStoreSecurityFile() {
        return getFile(STORE_SECURITY_FILE);
    }

    public File getSSLPrivateKey() {
        return getFile(SSL_PRIVATE_KEY);
    }

    public String getSSLPrivateKeyPass() {
        return paramVals.getProperty(SSL_PRIVATE_KEY_PASS.paramName);
    }

    public List<String> getSSLCiphers() {
        /*
         * SSL ciphers is of the format:
         *  cipher[,cipher]*
         * Remove white space, then split
         */
        String ciphersString = paramVals.getProperty(SSL_CIPHERS.paramName);
        if (ciphersString == null) {
            return null;
        }
        return Arrays.asList(ciphersString.replaceAll("\\s", "").split(","));
    }

    public String[] getSSLProtocols() {
        /*
         * SSL protocols is of the format:
         *  protocol[,protocols]*
         * Remove white space, then split
         */
        String protocols = paramVals.getProperty(SSL_PROTOCOLS.paramName);
        if (protocols == null) {
            return null;
        }
        return protocols.replaceAll("\\s", "").split(",");
    }

    public File getSaltWallet() {
        return getFile(SALT_WALLET);
    }

    public boolean getVerbose() {
        return getBool(VERBOSE);
    }

    public boolean getForceV3() {
        return getBool(FORCE_V3);
    }

    public boolean getForceV4() {
        return getBool(FORCE_V4);
    }

    public boolean getNoLimits() {
        return getBool(NO_LIMITS);
    }

    public boolean getAsync() {
        return getBool(ASYNC);
    }

    public boolean getAuthRetriesEnabled() {
        return getBool(AUTH_RETRIES_ENABLED);
    }

    public int getMaxActiveRetryCount() {
        return getInt(MAX_ACTIVE_RETRY_COUNT);
    }

    public int getMaxRetriesPerRequest() {
        return getInt(MAX_RETRIES_PER_REQUEST);
    }

    public int getRetryDelayMs() {
        return getInt(RETRY_DELAY_MS);
    }

    public boolean getQueryTracing() {
        return getBool(QUERY_TRACING);
    }

    public boolean isMonitorStatsEnabled() {
        return getBool(MONITOR_STATS_ENABLED);
    }

    public boolean isErrorLimitingEnabled() {
        return getBool(ERROR_LIMITING_ENABLED);
    }

    public File getIAMConfigFile() {
        return getFile(IAM_CONFIG_FILE);
    }

    public boolean useSSL() {
        return useSSL;
    }

    public int getPullRulesIntervalSec() {
        return getInt(PULL_RULES_INTERVAL_SEC);
    }

    public String getWarmupFile() {
        return paramVals.getProperty(WARMUP_FILE.paramName,
                                     WARMUP_FILE.defaultVal);
    }

    public int getWarmupFileRecencyMs() {
        return getInt(WARMUP_FILE_RECENCY_MS);
    }

    public int getWarmupTimeMs() {
        return getInt(WARMUP_TIME_MS);
    }

    public int getWarmupFileSaveIntervalMs() {
        return getInt(WARMUP_FILE_SAVE_INTERVAL_MS);
    }

    public boolean isChildTableEnabled() {
        return getBool(CHILD_TABLE_ENABLED);
    }

    public int getErrorDelayResponseThreshold() {
        return getInt(ERROR_DELAY_RESPONSE_THRESHOLD);
    }

    public int getErrorDelayResponseMs() {
        return getInt(ERROR_DELAY_RESPONSE_MS);
    }

    public int getErrorDnrThreshold() {
        return getInt(ERROR_DNR_THRESHOLD);
    }

    public int getErrorCreditMs() {
        return getInt(ERROR_CREDIT_MS);
    }

    public int getErrorCacheLifetimeMs() {
        return getInt(ERROR_CACHE_LIFETIME_MS);
    }

    public int getErrorDelayPoolSize() {
        return getInt(ERROR_DELAY_POOL_SIZE);
    }

    public boolean isDRLEnabled() {
        return getBool(DRL_ENABLED);
    }

    public boolean getDRLUseDistributed() {
        return getBool(DRL_USE_DISTRIBUTED);
    }

    public String getDRLTableName() {
        return getString(DRL_TABLE_NAME);
    }

    public boolean getDRLOptInRequired() {
        return getBool(DRL_OPT_IN_REQUIRED);
    }

    public int getDRLUpdateIntervalMs() {
        return getInt(DRL_UPDATE_INTERVAL_MS);
    }

    public int getDRLDelayPoolSize() {
        return getInt(DRL_DELAY_POOL_SIZE);
    }

    public int getDRLCreditMs() {
        return getInt(DRL_CREDIT_MS);
    }

    public double getDRLRateFactor() {
        return getDouble(DRL_RATE_FACTOR);
    }

    public int getTableCacheExpirationSec() {
        return getInt(TABLE_CACHE_EXPIRATION_SEC);
    }

    public int getTableCacheRefreshSec() {
        return getInt(TABLE_CACHE_REFRESH_SEC);
    }

    public int getTableCacheCheckIntervalSec() {
        return getInt(TABLE_CACHE_CHECK_INTERVAL_SEC);
    }

    public boolean isCmekEnabled() {
        return getBool(CMEK_ENABLED);
    }

    /* Helpers to convert a String property value to a type */
    private boolean getBool(ParamDef def) {
        String val = paramVals.getProperty(def.paramName, def.defaultVal);
        if (val == null) {
            return false;
        }
        return Boolean.parseBoolean(val);
    }

    /**
     * Returns null if this parameter type has no value.
     * If the parameter has a value, a File will be returned,
     * but the caller must check whether the file actually
     * exists.
     */
    private File getFile(ParamDef def) {
        String val = paramVals.getProperty(def.paramName, def.defaultVal);
        if (val == null) {
            return null;
        }
        return new File(val);
    }

    private String getString(ParamDef def) {
        return paramVals.getProperty(def.paramName, def.defaultVal);
    }

    private int getInt(ParamDef def) {
        String val = paramVals.getProperty(def.paramName, def.defaultVal);
        if (val == null) {
            return 0;
        }
        return Integer.parseInt(val);
    }

    private double getDouble(ParamDef def) {
        String val = paramVals.getProperty(def.paramName, def.defaultVal);
        if (val == null) {
            return 0.0;
        }
        return Double.parseDouble(val);
    }

    public KVStoreConfig getTemplateKVStoreConfig() {
        return templateKVStoreConfig;
    }

    KVStoreConfig makeTemplateKVStoreConfig() {
        final KVStoreConfig kvConfig = new KVStoreConfig("dummy", "host:0");

        int timeout = getInt(KV_REQUEST_TIMEOUT);
        if (timeout != -1) {
            kvConfig.setRequestTimeout(timeout,
                                     TimeUnit.MILLISECONDS);
        }

        String durability = paramVals.getProperty(KV_DURABILITY.paramName);
        if (durability != null) {
            Durability d = durabilityMap.get(durability);
            if (d != null) {
                kvConfig.setDurability(d);
            } else {
                throw new IllegalArgumentException
                    (durability + " is not a valid value for " +
                     KV_DURABILITY.paramName);
            }
        }
        String consistency = paramVals.getProperty(KV_CONSISTENCY.paramName);
        if (consistency != null) {
            Consistency c = consistencyMap.get(consistency);
            if (c != null) {
                kvConfig.setConsistency(c);
            } else {
                throw new IllegalArgumentException
                    (c + " is not a valid value for " +
                     KV_CONSISTENCY.paramName);
            }
        }

        /*
         * If specified, set the max number of active requests. If the
         * parameter isn't specified, use the number of request threads for
         * backward compatibility. If we don't do this, KV may fail before all
         * request threads are used at once, effectively limiting requests.
         * If proxy is running in non-async mode, just use number of threads.
         */
        int requestLimit = getInt(KV_REQUEST_LIMIT);
        if (requestLimit < 0 || getAsync() == false) {
            requestLimit = Math.max(getNumRequestThreads(),
                                    getRequestThreadPoolSize());
        }
        if (requestLimit > RequestLimitConfig.DEFAULT_MAX_ACTIVE_REQUESTS) {
            kvConfig.setRequestLimit(
                new RequestLimitConfig(
                    requestLimit,
                    RequestLimitConfig.DEFAULT_REQUEST_THRESHOLD_PERCENT,
                    RequestLimitConfig.DEFAULT_NODE_LIMIT_PERCENT));
        }

        return kvConfig;
    }

    /**
     * Displays all the non-null parameters, either because they're explicitly
     * set or because they have default values.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ParamDef> pe: paramDefinitions.entrySet()) {
            String val = paramVals.getProperty(pe.getKey(),
                                               pe.getValue().defaultVal);
            if (val != null && contextMatch(getProxyType(),
                                            pe.getValue().context)) {
                sb.append(pe.getKey()).append("=").append(val).append("\n");
            }
        }
        return sb.toString();
    }

    /**
     * Return true if the parameter context matches the type of proxy:
     * ALL -- always true
     * HIDDEN -- always false
     * CLOUD -- true for anything but KVPROXY
     * ON_PREM -- true for only KVPROXY
     */
    private static boolean contextMatch(ProxyType type, ParamContext context) {
        if (context == ParamContext.ALL) {
            return true;
        } else if (context == ParamContext.HIDDEN) {
            return false;
        }
        if (type == ProxyType.KVPROXY) {
            return (context == ParamContext.ON_PREM);
        }
        return (context != ParamContext.ON_PREM);
    }

    /**
     * Ensure NoSQL client-side security configuration based on given login
     * properties file. This method checks password store identified by given
     * password store NoSQL security property.
     *
     * @param loginFile NoSQL login properties file
     */
    public static void ensureClientSecurity(String loginFile) {
        final String WALLET_PROPERTY = "oracle.kv.auth.wallet.dir";
        if (loginFile == null) {
            throw new IllegalArgumentException(loginFile + " is required");
        }
        final Properties secProps = loadProps(loginFile);
        final String pwdStore = secProps.getProperty(WALLET_PROPERTY);
        if (pwdStore == null) {
            throw new IllegalArgumentException(
                "Invalid login properties file, no password store specified");
        }
        if (!exists(loginFile, pwdStore)) {
            throw new IllegalArgumentException(
                "Password store " + pwdStore + " doesn't exist");
        }
    }

    private static boolean exists(String loginFile, String artifact) {
        if (new File(artifact).exists()) {
            return true;
        }
        final File sourceDir =
            new File(loginFile).getAbsoluteFile().getParentFile();
        final File propFile = new File(sourceDir, artifact);
        return propFile.exists();
    }

    private static Properties loadProps(String loginFile) {
        final Properties securityProps = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(loginFile);
            securityProps.load(fis);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage());
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage());
            }
        }
        return securityProps;
    }

    /*
     * Yeah, this could be done with generic types, but too fussy and verbose
     * for this tiny case. This is public only for use by test classes.
     */
    public static class ParamDef {

        final private ParamType pType;
        final private ParamContext context;
        public final String paramName;
        final private String defaultVal;

        ParamDef(String paramName, String defaultVal, ParamType pType,
                 ParamContext context) {
            this.paramName = paramName;
            this.defaultVal = defaultVal;
            this.pType = pType;
            this.context = context;
            paramDefinitions.put(paramName, this);
        }

        ParamDef(String paramName, ParamContext context) {
            this(paramName, null, ParamType.STRING, context);
        }

        ParamDef(String paramName, String defaultVal, ParamContext context) {
            this(paramName, defaultVal, ParamType.STRING, context);
        }

        ParamDef(String paramName, ParamType pType, ParamContext context) {
            this(paramName, null, pType, context);
        }

        void validateIfExists(Properties paramVals) {
            String val = paramVals.getProperty(paramName);
            if (val == null) {
                return;
            }
            switch (pType) {
                case INT:
                    Integer.parseInt(val);
                    break;
                case DOUBLE:
                    Double.parseDouble(val);
                    break;
                case FILE:
                    File f = new File(val);
                    if (!f.exists()) {
                        usageAndThrow(val + " is not a valid file for " +
                                      paramName);
                    }
                    break;
                default:
                    /* nothing to do for strings and booleans */
            }
        }

        @Override
        public String toString() {
            return paramName;
        }
    }
}
