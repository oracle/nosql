/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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
import java.security.KeyStore;
import java.security.Security;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import javax.net.ssl.KeyManagerFactory;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import oracle.kv.KVVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.HttpServer;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.http.ServiceRequestHandler;
import oracle.nosql.common.kv.drl.LimiterManager;
import oracle.nosql.common.sklogger.MetricProcessor;
import oracle.nosql.common.sklogger.MetricRegistry;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.Config.ProxyType;
import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.cloud.CloudDataService;
import oracle.nosql.proxy.cloud.HealthService;
import oracle.nosql.proxy.cloud.LogControlService;
import oracle.nosql.proxy.cloud.ProxyHealthSource;
import oracle.nosql.proxy.filter.FilterHandler;
import oracle.nosql.proxy.filter.FilterService;
import oracle.nosql.proxy.cloud.CacheUpdateService;
import oracle.nosql.proxy.kv.KVDataService;
import oracle.nosql.proxy.kv.KVTenantManager;
import oracle.nosql.proxy.kv.LoginService;
import oracle.nosql.proxy.kv.LogoutService;
import oracle.nosql.proxy.kv.TokenRenewService;
import oracle.nosql.proxy.protocol.HttpConstants;
import oracle.nosql.proxy.rest.cloud.CloudRestDataService;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.util.ErrorManager;
import oracle.nosql.proxy.util.ShutdownManager;
import oracle.nosql.util.HttpServerHealth;
import oracle.nosql.util.ph.HealthReportAgent;

/**
 * The top-level class for an HTTP-based proxy
 *
 * Logging:
 * A java.util.logging config file can be created and referenced when
 * the containing process is started. The important packages to
 * configure are:
 *  oracle.nosql (the proxy and related classes)
 *  io.netty (netty)
 */
public final class Proxy {

    /* The Metrics collecting interval in MS. It is 60 seconds. */
    private static final long MONITOR_INTERVAL = 60_000;

    /* ComponentId to mark the source of log for Monitor system.*/
    private static final String COMPONENT_ID = "proxy";

    private static final SkLogger logger =
        new ProxyLogger(Proxy.class.getName(), COMPONENT_ID,
                        "proxy_worker.log");

    /* The parameters used to configure the proxy */
    private final Config config;

    HttpServer server;

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final boolean verbose;

    private final MonitorStats stats;

    private HealthReportAgent healthAgent;

    private final TenantManager tm;

    private final ErrorManager errorManager;

    private final LimiterManager limiterManager;

    private final LogControl logControl;

    private final AccessChecker ac;

    private final ProxyAuditManager audit;

    private ServiceRequestHandler requestHandler;

    private ProxyHealthSource healthSource;

    /*
     * Static values used for KVProxy security
     */
    private static final String WALLET_MANAGER_CLASS =
        "oracle.kv.impl.security.wallet.WalletManager";
    private static final String FILESTORE_MANAGER_CLASS =
        "oracle.kv.impl.security.filestore.FileStoreManager";
    private static final String WALLET_DIR = "store.wallet";
    private static final String PASSWD_FILE = "store.passwd";
    private static final String KEYSTORE_FILE = "store.keys";
    private static final String PWD_STORE_ALIAS = "keystore";

    private static final int MAX_REQUEST_SIZE = 64 * 1024 * 1024; // 64M
    private static final int MAX_CHUNK_SIZE = 128 * 1024; // 128k

    public Proxy(Config config,
                 TenantManager tm,
                 AccessChecker ac,
                 ProxyAuditManager audit) {

        this.config = config;
        this.tm = tm;

        /* tm can be null in test configurations */
        if (tm != null) {
            tm.setLogger(logger);
        }

        this.verbose = config.getVerbose();

        this.logControl = new LogControl();
        this.ac = ac;
        if (ac != null) {
            this.ac.setLogger(logger);
            this.ac.setLogControl(logControl);
        }

        this.audit = audit;
        if (audit != null) {
            this.audit.setLogger(logger);
        }

        /* Configures Netty logging */
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);

        if (config.isMonitorStatsEnabled()) {
            /*
             * The SkLogger is a "metrics processor", or plugin used by the
             * MetricRegistry to write the proxy's statistics into a log file.
             */
            final SkLogger metricLogger = new SkLogger(
                MonitorStats.class.getName(), COMPONENT_ID, "proxy_metric.log");

            boolean doMetrics = false;
            if (metricLogger.isMetricLoggable()) {
                MetricRegistry.defaultRegistry
                              .addMetricProcessor(metricLogger);
                doMetrics = true;
            }

            /* Cloud proxies do also have additional metrics plugins */
            if (ProxyType.CLOUD.equals(config.getProxyType())) {
                MetricProcessor cloudProcessor =
                    MonitorStats.getCloudProcessor(logger);
                if (cloudProcessor != null) {
                    MetricRegistry.defaultRegistry.addMetricProcessor(
                        cloudProcessor);
                    doMetrics = true;
                }
            }

            if (doMetrics) {
                stats = new MonitorStats(metricLogger);
                MetricRegistry.defaultRegistry
                              .startProcessors(MONITOR_INTERVAL);
            } else {
                System.err.println("WARN: Metrics enabled on cmdline but " +
                                   "not enabled in SkLogger config");
                stats = null;
            }
        } else {
            stats = null;
        }

        if (config.isErrorLimitingEnabled()) {
            errorManager = new ErrorManager(logger, stats,
                               (int)MONITOR_INTERVAL, config);
        } else {
            errorManager = null;
        }

        if (config.isDRLEnabled()) {
            LimiterManager.Config cfg = new LimiterManager.Config();
            cfg.useDistributed = config.getDRLUseDistributed();
            if (config.getDRLTableName() != null) {
                String tName = config.getDRLTableName();
                if (tName.indexOf(":") > 0) {
                    String[] elements = tName.split(":");
                    if (elements.length != 2) {
                        throw new IllegalArgumentException("table name '" +
                             tName + "' is invalid. " +
                             "Must be a single table name or " +
                             "namespace:tablename.");
                    }
                    cfg.drlNamespace = elements[0];
                    cfg.drlTableName = elements[1];
                } else {
                    cfg.drlTableName = tName;
                    cfg.drlNamespace = null;
                }
            }
            cfg.drlUpdateIntervalMs = config.getDRLUpdateIntervalMs();
            cfg.delayPoolSize = config.getDRLDelayPoolSize();
            cfg.limitCreditMs = config.getDRLCreditMs();
            cfg.rateFactor = config.getDRLRateFactor();
            limiterManager = new LimiterManager(logger, cfg);
            if (stats != null) {
                stats.setLimiterManager(limiterManager);
            }
        } else {
            limiterManager = null;
        }
    }

    /**
     * For unit test.
     */
    public ProxyHealthSource getHealthSource() {
        return healthSource;
    }

    public TenantManager getTenantManager() {
        return tm;
    }

    /**
     * TODO: use timeout -- means new methods in HttpServer
     */
    public void shutdown(long timeout,
                         TimeUnit unit) throws InterruptedException {

        if (!shutdown.compareAndSet(false,  true)) {
            /* Already shutdown. */
            return;
        }

        if (server == null) {
            /* not started */
            return;
        }

        tm.close();

        if (ac != null) {
            ac.close();
        }
        if (errorManager != null) {
            errorManager.shutDown();
        }
        if (limiterManager != null) {
            limiterManager.shutDown();
        }
        server.shutdown();
        MetricRegistry.defaultRegistry.stopProcessors();
        final String subject = "Proxy shut down";
        logger.logEvent("Proxy", Level.INFO, subject,
                        null /* message */, null /* throwable */);
    }

    static Proxy initialize(Config config,
                            TenantManager tm,
                            AccessChecker ac,
                            ProxyAuditManager audit) {

        final String msg ="Starting Proxy using configuration:\n" + config;
        verbose(config.getVerbose(), "Starting Proxy");
        logger.info(msg);

        final Proxy server = new Proxy(config, tm, ac, audit);
        server.start();

        /*
         * Log exits
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    final String msg = "Proxy shutdown hook called, exiting";
                    verbose(config.getVerbose(), msg);
                    logger.info(msg);
                }
            }
            );

        return server;
    }

    /*
     * Start proxy with certificate and private key file for HTTPS.
     */
    private void start() {

        SslContext sslCtx = null;
        try {
            if (config.useSSL()) {
                /*
                 * The config.validate() was called earlier and sanity
                 * checked required files are valid.
                 */
                final String msg = "Proxy creating SSL channel";
                verbose(msg);
                logger.info(msg);
                SslContextBuilder builder;
                if (config.getSSLSecurityDir() != null) {
                    final char[] kestorePass = retrieveKeystorePass();
                    final KeyStore keyStore =
                        KeyStore.getInstance(KeyStore.getDefaultType());
                    final File keystoreFile =
                        new File(config.getSSLSecurityDir(),
                                 KEYSTORE_FILE);
                    keyStore.load(new FileInputStream(keystoreFile),
                                  kestorePass);
                    final KeyManagerFactory kmf =
                        KeyManagerFactory.getInstance(
                            KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(keyStore, kestorePass);
                    builder = SslContextBuilder.forServer(kmf);
                } else {
                    builder = SslContextBuilder.forServer(
                        config.getSSLCertificate(), config.getSSLPrivateKey(),
                        config.getSSLPrivateKeyPass());
                }
                /*
                 * Cloud only:
                 * When proxy is started with BCFIPS approved-only mode, BCFIPS
                 * requires the KeyStore is built as BCFKS for private key.
                 * Netty only uses the type from KeyStore.getDefaultType().
                 * Although there is an API SslContextBuilder.keyStoreType,
                 * it only applies to truststore, see:
                 * io.netty.handler.sslJdkSslServerContext
                 *
                 * Overwriting the default store type to BCFKS to build
                 * Netty SslContext, then revert it back to original default.
                 */
                boolean fipsApproved = Boolean.getBoolean(
                    "org.bouncycastle.fips.approved_only");
                String kstype = KeyStore.getDefaultType();
                try {
                    if (fipsApproved) {
                        Security.setProperty("keystore.type", "BCFKS");
                        logger.info("Running in FIPS approved only mode, " +
                                    "set default keystore type to " +
                                    KeyStore.getDefaultType());
                    }
                    configSslContext(builder);
                    sslCtx = builder.build();
                } finally {
                    if (fipsApproved) {
                        Security.setProperty("keystore.type", kstype);
                        logger.info("SslContext initialization completed, " +
                                    "reverted default keystore type to " +
                                    KeyStore.getDefaultType());
                    }
                }
            }
            startServer(sslCtx, true);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Unable to start proxy: ").append(e.getMessage()).
                append(". Configuration used:\n" );
            sb.append(config);
            logger.severe(sb.toString());
            throw new RuntimeException(sb.toString(), e);
        }
    }

    private void configSslContext(SslContextBuilder builder) {
        if (config.getSSLCiphers() != null) {
            builder.ciphers(config.getSSLCiphers());
        }
        if (config.getSSLProtocols() != null) {
            builder.protocols(config.getSSLProtocols());
        }
    }

    private char[] retrieveKeystorePass() {
        PasswordManager pwdManager = null;
        PasswordStore pwdStore = null;
        final File securityDir = config.getSSLSecurityDir();
        try {
            final File walletFile = new File(securityDir,
                                             WALLET_DIR);
            final File plainTextFile = new File(securityDir,
                                                PASSWD_FILE);
            if (walletFile.exists()) {
                pwdManager =
                    PasswordManager.load(WALLET_MANAGER_CLASS);
                pwdStore = pwdManager.getStoreHandle(walletFile);
            } else if (plainTextFile.exists()) {
                pwdManager =
                    PasswordManager.load(FILESTORE_MANAGER_CLASS);
                pwdStore = pwdManager.getStoreHandle(plainTextFile);
            } else {
                throw new IllegalArgumentException(
                    "Cannot find password store file in security directory: " +
                    config.getSSLSecurityDir().getAbsolutePath());
            }
            pwdStore.open(null); /* must be autologin */
            return pwdStore.getSecret(PWD_STORE_ALIAS);
        } catch (Throwable e) {
            throw new IllegalStateException(
                "Fail to load password store, please make sure you have EE " +
                "version if the password is stored in wallet", e);
        } finally {
            if (pwdStore != null) {
                pwdStore.discard();
            }
        }
    }

    /*
     * Start proxy with sslContext for HTTPS.
     * Test env: if startNetty is false, don't start a server instance
     */
    public void startServer(SslContext sslCtx, boolean startNetty)
        throws Exception {

        requestHandler = new ServiceRequestHandler(logControl, logger);
        addServices();
        warmupCache();
        /*
         * NOTE: if config.getHostname() is null the server will listen
         * on all available interfaces. This is the default value.
         */
        if (startNetty) {
            server = new HttpServer(config.getHostname(),
                                    config.getHttpPort(),
                                    config.getHttpsPort(),
                                    config.getNumAcceptThreads(),
                                    config.getNumRequestThreads(),
                                    MAX_REQUEST_SIZE,
                                    MAX_CHUNK_SIZE,
                                    config.getIdleReadTimeout(),
                                    requestHandler,
                                    sslCtx,
                                    logger);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Proxy started:\n");
        sb.append(config.toString());
        sb.append("proxyVersion=" + DataServiceHandler.getProxyVersion());
        sb.append("\n");
        sb.append("kvclientVersion=" +
                  KVVersion.CURRENT_VERSION.getNumericVersionString());
        sb.append("\n");
        verbose(sb.toString());
        logger.logEvent("Proxy", Level.INFO, sb.toString(),
                        null /* message */, null /* throwable */);
        if (healthAgent != null) {
            healthAgent.start();
        }
    }

    /**
     * Creates a LogContext, used for the non-server test environment.
     * The serverless test environment needs a LogContext or calls that
     * require it fail
     */
    public LogContext generateLogContext(String path) {
        return logControl.generateLogContext(path);
    }

    /**
     * Returns the DataService, used for the non-server test environment.
     * This is called by tests to get the DataService instance for direct
     * calls, bypassing the web server.
     */
    public DataService getService(String serviceName) {
        return (DataService) requestHandler.getService(serviceName);
    }

    /**
     * Adds all locally known services to the request handler.
     */
    private void addServices() {

        /*
         * TenantManager is the keeper of the table cache for
         * sharing among related services.
         */
        tm.createTableCache(config, stats, logger);
        assert(tm.getTableCache() != null);

        switch (config.getProxyType()) {
        case KVPROXY:
            requestHandler.addService("ProxyData",
                new KVDataService(logger, tm, stats, audit,
                                  config,
                                  logControl));
            final KVStoreImpl store = ((KVTenantManager)tm).getStore();
            requestHandler.addService("Login",
                                      new LoginService(store, logger));
            requestHandler.addService("Logout",
                                      new LogoutService(store, logger));
            requestHandler.addService("Renew",
                                      new TokenRenewService(store, logger));
            requestHandler.addService("Health", new HealthService(logger));
            /* enable for test purpose */
            if (Boolean.getBoolean("test.kvlogcontrol")) {
                requestHandler.addService("LogControl",
                                          new LogControlService(logControl,
                                                                logger));
            }
            break;
        case CLOUD:
        case CLOUDTEST: {
            final FilterHandler filter =
                new FilterHandler(tm,
                                  config.getPullRulesIntervalSec(),
                                  logger);

            final CloudDataService dataService =
                new CloudDataService(logger, tm, ac, stats, audit, filter,
                                     errorManager,
                                     limiterManager,
                                     config,
                                     logControl);
            requestHandler.addService("ProxyData", dataService);

            final CloudRestDataService restDataService =
                new CloudRestDataService(logger, tm, ac, stats, audit, filter,
                                         errorManager,
                                         limiterManager,
                                         config,
                                         logControl);
            requestHandler.addService("ProxyRestData", restDataService);
            requestHandler.addService("Health", new HealthService(logger));
            requestHandler.addService("LogControl",
                                      new LogControlService(logControl,
                                                            logger));
            requestHandler.addService("TableState",
                                      new CacheUpdateService(ac,
                                                             dataService,
                                                             restDataService,
                                                             logger));

            requestHandler.addService("Filters",
                                      new FilterService(filter, logger));

            /* Check that the proxy is able to accept incoming http requests */
            HttpServerHealth httpServerHealth = new HttpServerHealth(
                "localhost",
                config.useSSL() ? config.getHttpsPort() : config.getHttpPort(),
                config.useSSL() ? config.getSSLCertificate() : null,
                HttpConstants.NOSQL_VERSION + "/" + HttpConstants.HEALTH_PATH,
                "GREEN");
            healthSource = new ProxyHealthSource(dataService, ac,
                                                 httpServerHealth);
            healthAgent =
                new HealthReportAgent(false /* isGlobalComponent */,
                                      logger,
                                      healthSource);
            break;
        }
        case CLOUDSIM: {
            final FilterHandler filter =
                new FilterHandler(tm,
                                  config.getPullRulesIntervalSec(),
                                  logger);
            requestHandler.addService("ProxyData",
                new CloudDataService(logger, tm, ac, stats, audit, filter,
                                     errorManager,
                                     limiterManager,
                                     config, logControl));

            final CloudRestDataService restDataService =
                new CloudRestDataService(logger, tm, ac, stats, audit,
                                         filter, errorManager,
                                         limiterManager,
                                         config, logControl);
            requestHandler.addService("ProxyRestData", restDataService);
            requestHandler.addService("Health", new HealthService(logger));

            requestHandler.addService("Filters",
                                      new FilterService(filter, logger));

            /* add LogControl for testing, at least */
            requestHandler.addService("LogControl",
                                      new LogControlService(logControl,
                                                            logger));
            break;
        }
        default:
            break;
        }
    }

    private void warmupCache() {
        String warmupFile = config.getWarmupFile();
        if (warmupFile == null || warmupFile.isEmpty()) {
            return;
        }
        long recencyMs = config.getWarmupFileRecencyMs();
        long timeLimitMs = config.getWarmupTimeMs();
        try {
            verbose("Warming up table cache from " + warmupFile);
            tm.getTableCache().warmUpFromFile(warmupFile,
                                              recencyMs,
                                              timeLimitMs);
        } catch (Exception e) {
            logger.warning("Got error trying to warm up table cache: " + e);
        }
        /* set a hook to save cached nsnames to file on shutdown */
        ShutdownManager.getInstance(logger).setShutdownHook(() -> {
            try {
                tm.getTableCache().saveKeysToFile(warmupFile);
            } catch (Exception e) {}
        });

        /*
         * Start up a simple background thread to occasionally save the
         * cache nsnames to the warmup file (for next proxy restart).
         * We do this in case the shutdown hook never gets called, which
         * can happen if the ShutdownManager is not being used.
         * default save interval is every 10 seconds
         */
        final int saveInterval = config.getWarmupFileSaveIntervalMs();

        /* allow config to say "no background thread" with a zero value */
        if (saveInterval > 0) {
            Thread cacheSaveThread = new Thread(()-> {
                while (true) {
                    try {
                        Thread.sleep(saveInterval);
                    } catch (Exception e) {}
                    if (ShutdownManager.getInstance(logger).inShutdown() ||
                        shutdown.get() == true) {
                        break;
                    }
                    try {
                        tm.getTableCache().saveKeysToFile(warmupFile);
                    } catch (Exception e) {
                        logger.warning("Can't save table cache keys to " +
                            warmupFile + ": " + e);
                        break;
                    }
                }
            }, "cacheSaveThread");
            cacheSaveThread.setDaemon(true);
            cacheSaveThread.start();
        }
    }

    private static void verbose(final boolean verbose, final String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }

    private void verbose(final String msg) {
        verbose(verbose, msg);
    }

    public boolean isOnPrem() {
        return config.getProxyType() == ProxyType.KVPROXY;
    }
}
