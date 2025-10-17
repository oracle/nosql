/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static oracle.nosql.proxy.protocol.HttpConstants.TENANT_ID;
import static oracle.nosql.proxy.protocol.NsonProtocol.ERROR_CODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import oracle.kv.KVVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.as.AggregationService;
import oracle.kv.util.kvlite.KVLite;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableResult.State;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.nson.Nson;
import oracle.nosql.nson.values.MapWalker;
import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.kv.KVTenantManager;
import oracle.nosql.proxy.protocol.HttpConstants;
import oracle.nosql.proxy.sc.LocalTenantManager;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.security.AccessCheckerFactory;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.security.SecureTestUtil;
import oracle.nosql.proxy.util.KVLiteBase;
import oracle.nosql.proxy.util.PassThroughTableCache;
import oracle.nosql.proxy.util.TableCache.TableEntry;
import oracle.nosql.util.HttpRequest;
import oracle.nosql.util.HttpResponse;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableRequestLimits;
import oracle.nosql.util.tmi.TenantLimits;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class ProxyTestBase extends KVLiteBase {

    /*
     * Proxy state
     */
    private static int PROXY_PORT = 8095;
    protected static int PROXY_HTTPS_PORT = 8096;
    private static int PROXY_ACCEPT_THREADS = 2;
    private static int PROXY_REQUEST_THREADS = 0; /* use default */
    private static int PROXY_RETRY_DELAY_MS_DEFAULT = 30;
    /*
     * If these next 2 are non-zero they will use thread pools for requests
     * as well as handling async completions from KV. Default off. They can be
     * turned on using -Dproxy.request.poolsize=<N> and
     * -Dproxy.kv.poolsize=<N> from the command line
     */
    private static int PROXY_REQUEST_POOL_SIZE = 0;
    private static int PROXY_KV_POOL_SIZE = 0;
    private static boolean PROXY_MONITOR_STATS_ENABLED = false;
    protected static boolean SECURITY_ENABLED = false;
    protected static String TEST_TENANT_ID = "ProxyTestTenant";
    protected static String TEST_COMPARTMENT_ID = null;
    protected static String TEST_COMPARTMENT_ID_FOR_UPDATE = null;

    protected static String PROXY_REQUEST_THREADS_PROP = "proxy.request.threads";
    protected static String PROXY_ACCEPT_THREADS_PROP = "proxy.accept.threads";
    protected static String PROXY_REQUEST_POOL_SIZE_PROP =
        "proxy.request.poolsize";
    protected static String PROXY_KV_POOL_SIZE_PROP =
        "proxy.kv.poolsize";
    protected static String PROXY_ASYNC_PROP = "test.async";
    protected static String PROXY_ERROR_LIMITING_PROP = "test.errorlimiting";
    protected static String PROXY_THROTTLING_RETRIES_PROP =
                                "test.throttlingretries";
    protected static String PROXY_AUTH_RETRIES_PROP = "test.authretries";
    protected static String PROXY_RETRY_DELAY_MS_PROP = "test.retrydelayms";
    protected static String PROXY_NUM_PROXIES_PROP = "test.numproxies";
    protected static String PROXY_DRL_ENABLED_PROP = "test.drlenabled";
    protected static String PROXY_DRL_USE_DISTRIBUTED_PROP =
                                "test.drlusedistributed";
    protected static String PROXY_DRL_TABLENAME_PROP = "test.drltablename";
    protected static String PROXY_DRL_RATE_FACTOR_PROP = "test.drlratefactor";
    protected static String KVLITE_USETHREADS_PROP = "test.usethreads";
    protected static String KVLITE_MULTISHARD_PROP = "test.multishard";
    protected static String KVLITE_MEMORYMB_PROP = "test.memorymb";
    protected static String ONPREM_PROP = "onprem";
    protected static String USEMC_PROP = "usemc";
    protected static String USECLOUD_PROP = "usecloud";
    protected static String VERBOSE_PROP = "test.verbose";
    protected static String TENANT_ID_PROP = "tenant.id";
    protected static String TEST_V3_PROP = "test.v3";
    protected static String NO_LIMITS_PROP = "test.nolimits";
    protected static String OS_PROP = "os.name";

    protected static boolean isLinux =
        System.getProperty(OS_PROP).toLowerCase().contains("linux");
    protected static boolean isMac =
        System.getProperty(OS_PROP).toLowerCase().contains("mac");

    protected static String TEST_MRTABLE_PROP = "test.mrtable";

    protected static String COMPARTMENT_ID_PROP = "test.compartment";
    protected static String COMPARTMENT_ID_FOR_UPDATE_PROP = "test.compartment.forupdate";
    protected static String OCI_CONFIG_FILE_PROP = "oci.config.file";
    protected static String OCI_PROFILE_PROP = "oci.profile";
    protected static String TENANT_LIMITS_FILE_PROP = "tenant.limits.file";

    /*
     * Tests don't need or use peak throughput information. Ideally there would
     * be a way to disable it.
     */
    protected static final int PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC =
        Integer.MAX_VALUE;
    protected static final int PEAK_THROUGHPUT_DEFAULT_TTL_DAY = 1;

    /*
     * Operation throttling constants. 0 means reset to the default in the SC.
     * A small number eliminates throttling by allowed an operation for every
     * millisecond in that number (1 = 1 op/ms, 1000 means 1 op/second).
     */
    protected static final int NO_OP_THROTTLE = 1;
    protected static final int DEFAULT_OP_THROTTLE = 0;

    protected final static String DEFINED_TAG_NAMESPACE = "tagging_test";
    protected final static String DEFINED_TAG_PROP = "tbac_key";
    protected final static String DEFAULT_DEFINED_TAG_NAMESPACE = "Oracle-Tags";

    private static final int MIN_READ = 1;

    protected static String hostName = getHostName();
    protected static final int startPort = 13240;
    protected static KVLite kvlite;
    protected static Proxy[] proxies;
    protected static Proxy proxy = null;
    protected static int numProxies = 1;
    protected static TenantManager[] tms;
    protected static TenantManager tm = null;
    protected static AccessChecker ac = null;
    protected static ProxyAuditManager audit = null;

    /* set to true if running against an existing cloud proxy */
    protected static boolean useMiniCloud = false;

    protected static boolean cloudRunning = false;
    protected static String tmUrlBase = null;
    protected static boolean onprem = false;
    protected static boolean verbose = false;
    protected static boolean multishard = false;
    protected static int memoryMB = 0;
    protected static boolean SSLRunning = false;
    protected static boolean testV3 = false;

    protected static boolean useCloudService = false;
    protected static String OCI_CONFIG_FILE = "~/.oci/config";
    protected static String OCI_PROFILE = null;

    protected static RequestLimits rlimits = RequestLimits.defaultLimits();

    /* non-static, used by sub-classes */
    AggregationService as;

    /*
     * Create http and https handles. In test mode the SSL config is not secure
     * but still exercises SSL.
     */
    protected static NoSQLHandle handle = null;
    protected static NoSQLHandle[] handles = null;
    protected static int currentHandleNum = 0;

    protected static NoSQLHandle sslHandle = null;

    /*
     * An instance with non-default limits to make tests run reasonably
     */
    protected static int NUM_TABLES = 10;
    protected static int NUM_SCHEMA_EVOLUTIONS = 6;

    protected static TenantLimits tenantLimits =
        TenantLimits.getNewDefault();
    static {
        tenantLimits.setNumTables(NUM_TABLES)
            /*
             * NOTE: the per-table read/write limits need to be >=
             * 1/2 of the per-tenant limit in order for
             * LimitsTest to work correctly.
             * See testTableProvisioningLimits.
             */
            .setDdlRequestsRate(400)
            .setTableLimitReductionsRate(50)
            .setNumFreeTables(3)
            .setNumAutoScalingTables(3)
            .setBillingModeChangeRate(2);
        TableRequestLimits tableLimits = tenantLimits.getStandardTableLimits();
        tableLimits.setTableReadUnits(90000)
                   .setTableWriteUnits(30000)
                   .setSchemaEvolutions(NUM_SCHEMA_EVOLUTIONS);
    }

    protected static boolean USE_SSL_HOOK = false;
    protected static boolean TEST_TRACE = Boolean.getBoolean("test.trace");

    @Rule
    public final TestRule watchman = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            if (TEST_TRACE) {
                System.out.println("Starting test: " +
                                   description.getMethodName());
            }
        }
    };

    @BeforeClass
    public static void staticSetUp()
        throws Exception {
        staticSetUp(tenantLimits);
    }

    public static void staticSetUp(TenantLimits tl)
       throws Exception {

        doStaticSetup();

        cleanupTestDir();

        proxy = startup(tl, true/*startkvlite*/);

        if (useMiniCloud) {
            if (scHost != null && scPort != null) {
                tmUrlBase = "http://" + scHost + ":" + scPort + "/V0/tm/";
            }
        }
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        if (useMiniCloud) {
            if (scUrlBase != null) {
                deleteTier(getTenantId());
            }
            return;
        }

        if (handle != null) {
            for (int x=0; x<numProxies; x++) {
                handles[x].close();
                handles[x] = null;
            }
            handle = null;
        }

        if (sslHandle != null) {
            sslHandle.close();
            sslHandle = null;
        }

        for (int x=0; x<numProxies; x++) {
            if (proxies != null && proxies[x] != null) {
                proxies[x].shutdown(3, TimeUnit.SECONDS);
                proxies[x] = null;
            }
            if (tms != null && tms[x] != null) {
                tms[x].close();
                tms[x] = null;
            }
        }
        proxy = null;
        tm = null;

        if (kvlite != null) {
            kvlite.stop(false);
        }

        /* leave state for debugging cleanupTestDir(); */
    }

    @Before
    public void setUp() throws Exception {

        /* if proxy is not running in local test mode, don't set up handle */
        if (!cloudRunning && proxy == null) {
            return;
        }

        /*
         * Configure the endpoint(s)
         */
        if (handle == null) {
            handles = new NoSQLHandle[numProxies];
            for (int x=0; x<numProxies; x++) {
                handles[x] = configHandle(getProxyEndpoint(x));
                if (testV3) {
                    forceV3((NoSQLHandleImpl)handles[x]);
                }
            }
            handle = handles[0];
        }
        setOpThrottling(getTenantId(), NO_OP_THROTTLE);

        /*
         * Only configure https if not running in minicloud, for now.
         */
        if (cloudRunning == false && SSLRunning == true) {
            if (sslHandle == null) {
            sslHandle = configHandle("https://" + hostName + ":" +
                                     getProxyHttpsPort(0));
            }
        }
        //startAggregationService(1, 1, true);
        dropAllTables(handle, true);
    }

    @After
    public void tearDown() throws Exception {

        if (handle != null) {
            dropAllTables(handle, true);
            //handle.close();
        }

        setOpThrottling(getTenantId(), DEFAULT_OP_THROTTLE);

        if (sslHandle != null) {
            //sslHandle.close();
        }
        //stopAggregationService();
    }

    /*
     * Some tests need a fully initialized kv system metadata table.
     * This waits for it, BUT a table must have already been created.
     * TODO: use new kv-based mechanism to avoid need to create a
     * table.
     */
    protected static void waitForStoreInit(int timeoutSecs)
        throws Exception {
        if (proxy != null) {
            proxy.getTenantManager().waitForStoreInit(timeoutSecs);
        }
        if (tms != null) {
            for (TenantManager tm : tms) {
                tm.waitForStoreInit(timeoutSecs);
            }
        }
    }

    protected NoSQLHandle getNextHandle() {
        if (handles == null) {
            return null;
        }
        if (currentHandleNum >= numProxies) {
            currentHandleNum = 0;
        }
        return handles[currentHandleNum++];
    }

    /* Used by ElasticityTest to start a proxy without also starting KVLite */
    public static Proxy startProxy()
        throws Exception {

        return startup(tenantLimits, false);
    }

    private static Proxy startup(TenantLimits pTenantLimts,
                                 boolean startKVLite)
        throws Exception {

        /*
         * Determine if running against an existing cloud proxy such as the
         * MiniCloud. If so, don't start KVLite or a proxy or the aggregation
         * service. Also check for proxy host and port set in system properties
         * to override the defaults.
         */
        onprem = Boolean.getBoolean(ONPREM_PROP);
        useMiniCloud = Boolean.getBoolean(USEMC_PROP);
        useCloudService = Boolean.getBoolean(USECLOUD_PROP);
        /* cloudRunning is general flag for both minicloud or cloud test */
        cloudRunning = useMiniCloud || useCloudService;
        verbose = Boolean.getBoolean(VERBOSE_PROP);
        testV3 = Boolean.getBoolean(TEST_V3_PROP);

        verbose("Starting tests in verbose output mode");

        String proxyHost = System.getProperty("proxy.host");
        if (proxyHost != null) {
            hostName = proxyHost;
        }

        PROXY_PORT = Integer.getInteger("proxy.port", PROXY_PORT);
        PROXY_MONITOR_STATS_ENABLED = Boolean.getBoolean("monitor");
        SECURITY_ENABLED = Boolean.getBoolean("security");

        String tenantId = System.getProperty(TENANT_ID_PROP);
        if (tenantId != null) {
            TEST_TENANT_ID = tenantId;
        }

        String compartmentId = System.getProperty(COMPARTMENT_ID_PROP);
        if (compartmentId != null) {
            TEST_COMPARTMENT_ID = compartmentId;
        }

        compartmentId = System.getProperty(COMPARTMENT_ID_FOR_UPDATE_PROP);
        if (compartmentId != null) {
            TEST_COMPARTMENT_ID_FOR_UPDATE = compartmentId;
        }

        numProxies = Integer.getInteger(PROXY_NUM_PROXIES_PROP, 1);

        if (useCloudService) {

            if (proxyHost == null) {
                fail("System property \"proxy.host\" must be set if run " +
                     "against cloud service");
            } else {
                /* To prevent the test from running to production env */
                if (!proxyHost.endsWith("oci.oc-test.com")) {
                    fail("The test can only be run against the service in " +
                         "pre-production, proxy.host=" + proxyHost);
                }
            }

            PROXY_HTTPS_PORT = 443;

            if (TEST_COMPARTMENT_ID == null) {
                fail("System property \"" + COMPARTMENT_ID_PROP +
                     "\" must be set if run against cloud service");
            }

            if (TEST_COMPARTMENT_ID_FOR_UPDATE == null) {
                fail("System property \"" + COMPARTMENT_ID_FOR_UPDATE_PROP +
                     "\" must be set if run against cloud service");
            }

            String value = System.getProperty(OCI_PROFILE_PROP);
            if (value != null) {
                OCI_PROFILE = value;
            }
            if (OCI_PROFILE == null) {
                fail("System property \"" + OCI_PROFILE_PROP +
                     "\" must be set when test against cloud service");
            }

            value = System.getProperty(OCI_CONFIG_FILE_PROP);
            if (value != null) {
                OCI_CONFIG_FILE = value;
            }

            value = System.getProperty(TENANT_LIMITS_FILE_PROP);
            if (value != null) {
                try (FileInputStream fis = new FileInputStream(value)) {
                    tenantLimits = JsonUtils.readValue(fis, TenantLimits.class);
                } catch (IOException ex) {
                    fail("Unable to load tenant limits from file: " + value);
                }
            } else {
                /*
                 * If tenantLimits is not provided, set the tenantLimits to null.
                 * Some tests depend on the tenantLimits will be skipped in
                 * cloud test if the tenantLimits is not provided.
                 */
                tenantLimits = null;
            }
        }

        if (cloudRunning) {
            /*
             * Add a new tier with specified tenantLimits, add the test
             * tenantId associated with the tier.
             */
            addTier(getTenantId(), pTenantLimts);
            return proxy;
        }

        boolean noLimits = Boolean.getBoolean(NO_LIMITS_PROP);

        if (startKVLite) {
            multishard = Boolean.getBoolean(KVLITE_MULTISHARD_PROP);
            memoryMB = Integer.getInteger(KVLITE_MEMORYMB_PROP, 0);
            boolean useThreads = Boolean.getBoolean(KVLITE_USETHREADS_PROP);
            if (useThreads) {
                multishard = false;
                verbose("Starting kvlite using threads in this jvm");
            } else {
                verbose("Starting kvlite using separate jvm process, " +
                        "multishard=" + multishard +
                        ", memoryMB=" + memoryMB);
            }

            kvlite = startKVLite(hostName,
                                 null, /* default store name */
                                 useThreads,
                                 verbose,
                                 multishard,
                                 memoryMB,
                                 false); /* secured */
        }

        /*
         * Set Netty to use JDK logger factory.
         *
         * Since 19.1, KV added slf4j-api.jar on the class path. By default,
         * Netty tries to instantiate slf4j logger first then JDK logger, so
         * it will use slf4j-api by default because of that KV change. However,
         * slf4j needs additional implementation jar to do actual logging,
         * otherwise, it will only produce NOP warnings.
         */
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        SelfSignedCertificate ssc = new SelfSignedCertificate(getHostName());
        prepareTruststore(ssc.certificate());
        /*
         * Configure both HTTP and HTTPS
         * TODO: think about how to inject connection failures into
         * proxy's HTTP handling -- drop connections, etc
         */
        Properties commandLine = new Properties();

        /*
         * the no-limits property is not used in general. It's present to
         * see the failures that occur when expected limits are not
         * enforced (-Dtest.nolimits=true)
         */
        if (noLimits) {
            commandLine.setProperty(Config.NO_LIMITS.paramName,
                                    Boolean.toString(noLimits));
        }
        commandLine.setProperty(Config.STORE_NAME.paramName,
                                getStoreName());

        commandLine.setProperty(Config.HELPER_HOSTS.paramName,
                                (hostName + ":" + getKVPort()));


        Config.ProxyType ptype = (onprem ? Config.ProxyType.KVPROXY :
                                  Config.ProxyType.CLOUDTEST);

        commandLine.setProperty(Config.PROXY_TYPE.paramName, ptype.name());

        if (USE_SSL_HOOK) {
            commandLine.setProperty(Config.SSL_CERTIFICATE.paramName,
                    ssc.certificate().getAbsolutePath());
            commandLine.setProperty(Config.SSL_PRIVATE_KEY.paramName,
                    ssc.privateKey().getAbsolutePath());

            /* netty disable TLSv1.3 by default, enable it for SslTest */
            commandLine.setProperty(Config.SSL_PROTOCOLS.paramName,
                                    "TLSv1.3,TLSv1.2,TLSv1.1");
            SSLRunning = true;
        } else {
            SSLRunning = false;
        }


        /*
         * note the properties need to be checked at start time, to allow
         * the BeforeClass method to set them for specific tests
         */
        int reqThreads = Integer.getInteger(PROXY_REQUEST_THREADS_PROP,
                                            PROXY_REQUEST_THREADS);
        int accThreads = Integer.getInteger(PROXY_ACCEPT_THREADS_PROP,
                                            PROXY_ACCEPT_THREADS);
        int requestPoolSize = Integer.getInteger(PROXY_REQUEST_POOL_SIZE_PROP,
                                                 PROXY_REQUEST_POOL_SIZE);
        int kvPoolSize = Integer.getInteger(PROXY_KV_POOL_SIZE_PROP,
                                            PROXY_KV_POOL_SIZE);

        commandLine.setProperty(Config.NUM_REQUEST_THREADS.paramName,
                Integer.toString(reqThreads));
        commandLine.setProperty(Config.NUM_ACCEPT_THREADS.paramName,
                Integer.toString(accThreads));
        commandLine.setProperty(Config.REQUEST_THREAD_POOL_SIZE.paramName,
                Integer.toString(requestPoolSize));
        commandLine.setProperty(Config.KV_THREAD_POOL_SIZE.paramName,
                Integer.toString(kvPoolSize));
        commandLine.setProperty(Config.MONITOR_STATS_ENABLED.paramName,
                Boolean.toString(PROXY_MONITOR_STATS_ENABLED));

        commandLine.setProperty(Config.VERBOSE.paramName,
                                Boolean.toString(verbose));

        /* async now defaults to true */
        setDefaultTrue(commandLine, PROXY_ASYNC_PROP, Config.ASYNC.paramName);

        /* default auth retries to true */
        setDefaultTrue(commandLine, PROXY_AUTH_RETRIES_PROP,
                       Config.AUTH_RETRIES_ENABLED.paramName);

        /* Error limiting configs */
        /* default error limiting to true */
        setDefaultTrue(commandLine, PROXY_ERROR_LIMITING_PROP,
                       Config.ERROR_LIMITING_ENABLED.paramName);

        /*
         * TODO: Possibly configure these?
         * ERROR_DELAY_RESPONSE_THRESHOLD 5
         * ERROR_DELAY_RESPONSE_MS 200
         * ERROR_DNR_THRESHOLD 10
         * ERROR_CREDIT_MS 1000
         * ERROR_CACHE_SIZE 10000
         * ERROR_CACHE_LIFETIME_MS 3600000
         * ERROR_DELAY_POOL_SIZE 5
         */

        /* Rate limiting configs */
        commandLine.setProperty(Config.DRL_ENABLED.paramName,
                        Boolean.toString(
                        Boolean.getBoolean(PROXY_DRL_ENABLED_PROP)));
        commandLine.setProperty(Config.DRL_USE_DISTRIBUTED.paramName,
                        Boolean.toString(
                        Boolean.getBoolean(PROXY_DRL_USE_DISTRIBUTED_PROP)));
        String prop = System.getProperty(PROXY_DRL_TABLENAME_PROP);
        if (prop != null && prop.compareTo("") != 0) {
            commandLine.setProperty(Config.DRL_TABLE_NAME.paramName, prop);
        }
        prop = System.getProperty(PROXY_DRL_RATE_FACTOR_PROP);
        if (prop != null && prop.compareTo("") != 0) {
            commandLine.setProperty(Config.DRL_RATE_FACTOR.paramName, prop);
        }

        int retryDelayMs = Integer.getInteger(PROXY_RETRY_DELAY_MS_PROP,
                                              PROXY_RETRY_DELAY_MS_DEFAULT);
        commandLine.setProperty(Config.RETRY_DELAY_MS.paramName,
                                Integer.toString(retryDelayMs));
        //This is needed to enable query tracing, in addition to setting
        //traceLevel in the driver.
        commandLine.setProperty(Config.QUERY_TRACING.paramName, "true");

        /*
         * This is to test MR table locally, set MR table names to property
         * "test.mrtable", comma separated.
         */
        String tables = System.getProperty("test.mrtable");
        List<String> mrTableNames = null;
        if (tables != null) {
            mrTableNames = new ArrayList<>();
            for (String tname : tables.split(",")) {
                mrTableNames.add(tname.trim().toLowerCase());
            }
        }

        /* create a simple access checker */
        ac = AccessCheckerFactory.createInsecureAccessChecker();

        proxies = new Proxy[numProxies];
        tms = new TenantManager[numProxies];
        for (int x=0; x<numProxies; x++) {
            commandLine.setProperty(Config.HTTP_PORT.paramName,
                            Integer.toString(getProxyPort(x)));
            if (USE_SSL_HOOK) {
                commandLine.setProperty(Config.HTTPS_PORT.paramName,
                        Integer.toString(getProxyHttpsPort(x)));
            }

            /* create config from commandLine properties */
            Config cfg = new Config(commandLine);
            if (x > 0) {
                TimeUnit.MILLISECONDS.sleep(300);
            }
            /* create an appropriate TenantManager */
            if (onprem) {
                /* note: in KVPROXY mode the proxy *requires* a KVTenantManager */
                tms[x] = KVTenantManager.createTenantManager(cfg);
            } else {
                tms[x] = new TestTenantManager(cfg, mrTableNames);
            }
            proxies[x] = Proxy.initialize(cfg, tms[x], ac, audit);
            assert proxies[x] != null;
        }
        proxy = proxies[0];
        tm = tms[0];
        return proxy;
    }

    protected static int getRegionId() {
       return tm.getLocalRegionId();
    }

    public static Proxy getProxy() {
        return getProxy(0);
    }

    public static Proxy getProxy(int proxyNum) {
        return proxies != null ? proxies[proxyNum] : null;
    }

    protected NoSQLHandle configHandle(String endpoint) {

        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(endpoint);
        return setupHandle(hconfig);
    }

    protected NoSQLHandle configHandle(URL url) {

        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(url);
        return setupHandle(hconfig);
    }

    /* Set configuration values for the handle */
    protected NoSQLHandle setupHandle(NoSQLHandleConfig hconfig) {

        /*
         * 5 retries, default retry algorithm
         */
        hconfig.configureDefaultRetryHandler(5, 0);

        hconfig.setRequestTimeout(30000);

        setHandleConfig(hconfig);

        /* allow test cases to add/modify handle config */
        perTestHandleConfig(hconfig);
        return getHandle(hconfig);
    }

    protected void setHandleConfig(NoSQLHandleConfig config) {
        config.setDefaultCompartment(TEST_COMPARTMENT_ID);
        if (useCloudService) {
            SecureTestUtil.setAuthProvider(config, OCI_CONFIG_FILE, OCI_PROFILE);
        } else {
            SecureTestUtil.setAuthProvider(config,
                                           SECURITY_ENABLED,
                                           onprem,
                                           getTenantId());
        }
    }

    protected void perTestHandleConfig(NoSQLHandleConfig hconfig) {
         /* no-op */
    }

    protected NoSQLHandle configNoRetryHandle(String tenant) {

        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(getProxyEndpoint());

        /*
         * no retry
         */
        hconfig.configureDefaultRetryHandler(0, 0);

        SecureTestUtil.setAuthProvider(hconfig,
                                       SECURITY_ENABLED,
                                       onprem,
                                       tenant);
        return getHandle(hconfig);
    }

    protected void checkErrorMessage(Throwable t) {
        if (t == null || t.getMessage() == null) {
            return;
        }
        assertFalse(t.getMessage().contains(AccessContext.INTERNAL_OCID_PREFIX));
    }

    public static String getProxyEndpoint() {
        return getProxyEndpoint(0);
    }

    public static String getProxyEndpoint(int proxyNum) {
        try {
            if (useCloudService) {
                return getProxyHttpsEndpoint(proxyNum);
            }
            return "http://" + hostName + ":" + getProxyPort(proxyNum);
        } catch (Exception e) {
        }
        return null;
    }

    public static String getProxyHttpsEndpoint() {
        return getProxyHttpsEndpoint(0);
    }

    public static String getProxyHttpsEndpoint(int proxyNum) {
        try {
            return "https://" + hostName + ":" + getProxyHttpsPort(proxyNum);
        } catch (Exception e) {
        }
        return null;
    }

    public static URL getProxyURL() {
        return getProxyURL(0);
    }

    public static URL getProxyURL(int proxyNum) {
        try {
            return new URL("http", hostName, getProxyPort(proxyNum), "/");
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * Allows classes to create a differently-configured NoSQLHandle.
     */
    protected NoSQLHandle getHandle(NoSQLHandleConfig config) {
        /*
         * Create a Logger. Configuration for the logger is in proxy/build.xml
         */
        Logger logger = Logger.getLogger(getClass().getName());
        config.setLogger(logger);

        /*
         * Open the handle
         */
        NoSQLHandle h = NoSQLHandleFactory.createNoSQLHandle(config);

        /* do a simple op to set the protocol version properly */
        try {
            GetTableRequest getTable =
                new GetTableRequest().setTableName("noop");
            h.getTable(getTable);
        } catch (TableNotFoundException e) {}

        return h;
    }

    /*
     * Takes a certificate file and puts it in a trust store (KeyStore)
     * named "proxycert" and sets the javax.net.ssl.trustStore property to
     * that file so the driver SSL configuration finds it.
     */
    protected static void prepareTruststore(File certFile)
        throws Exception {

        File trustStore = new File(getTestDir(), "proxycert");
        BufferedInputStream bis = null;
        try {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(null, null);
            FileInputStream fis = new FileInputStream(certFile);
            bis = new BufferedInputStream(fis);
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Certificate cert = null;
            while (bis.available() > 0) {
                cert = factory.generateCertificate(bis);
            }
            ks.setCertificateEntry("test", cert);
            ks.store(new FileOutputStream(trustStore), "123456".toCharArray());

        } finally {
            if (bis != null) {
                bis.close();
            }
        }

        /* set the trust store property, so driver can use that */
        System.setProperty("javax.net.ssl.trustStore",
                           trustStore.getAbsolutePath());
    }

    /*
     * Utility methods for use by subclasses
     */

    /**
     * Starts an instance of AggregationService using the specified
     * poll periods. There must only be one instance running in the process
     * at any time. There is no (current) way to stop this service, which
     * runs in its own thread.
     */
    synchronized AggregationService startAggregationService(
        int throughputPollPeriodSec,
        int sizePollPeriodSec,
        boolean verbose) throws Exception {

        /*
         * Don't start the AS if running against an existing cloud service
         */
        if (cloudRunning || onprem) {
            return null;
        }

        if (as != null) {
            throw new IllegalArgumentException(
                "Can't start AggregationService, it's already running");
        }

        as = startAggregationService(throughputPollPeriodSec,
                                     sizePollPeriodSec,
                                     verbose);
        return as;
    }

    synchronized public static AggregationService startAggregationServiceStatic(
        int throughputPollPeriodSec,
        int sizePollPeriodSec,
        boolean verbose) throws Exception {

        final int maxRetries = 10;
        final int delay = 1000;
        int numRetries = 0;

        /*
         * Don't start the AS if running against an existing cloud service
         */
        if (cloudRunning || onprem) {
            return null;
        }

        AggregationService aggSrv;

        Exception failEx = null;
        while (numRetries < maxRetries) {
            try {
                /* NOTE: verbose isn't yet used */
                aggSrv = AggregationService.createAggregationService(
                    getStoreName(),
                    new String[] {(hostName + ":" + getKVPort())},
                    180, /* throughputHistorySecs */
                    throughputPollPeriodSec,
                    sizePollPeriodSec,
                    PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC,
                    PEAK_THROUGHPUT_DEFAULT_TTL_DAY,
                    5); /* max threads */
                assert aggSrv != null;
                return aggSrv;
            } catch (IllegalStateException ise) {
                failEx = ise;
                try { Thread.sleep(delay); } catch (InterruptedException ie) {}
                ++numRetries;
            }
        }
        throw new IllegalArgumentException(
            "Unable to start AggregationService, last exception: " + failEx);
    }

    synchronized void stopAggregationService() throws Exception {
        if (as != null) {
            as.stop();
            as = null;
        }
   }

    /**
     * Executes a table (DDL) operation using the supplied statement.
     *
     * @param handle handle to the proxy
     * @param statement a table statement
     * @param state if non-null, wait for the table to reach the supplied state
     */
    static TableResult tableOperation(NoSQLHandle handle,
                                      String statement,
                                      TableResult.State state,
                                      Class<? extends Exception> expected) {
        return tableOperation(handle, statement, null, null, null,
                              state, expected);
    }

    /**
     * Executes a table (DDL) operation using the supplied statement.
     *
     * @param handle handle to the proxy
     * @param statement a table statement
     * @param state if non-null, wait for the table to reach the supplied state
     * @param tableName if non-null set it; it is used for changing table limits
     */
    static TableResult tableOperation(NoSQLHandle handle,
                                      String statement,
                                      TableLimits limits,
                                      String tableName,
                                      TableResult.State state,
                                      Class<? extends Exception> expected) {
        return tableOperation(handle, statement, limits, null, tableName,
                              state, expected);
    }

    static TableResult tableOperation(NoSQLHandle handle,
                                      String statement,
                                      TableLimits limits,
                                      String compartment,
                                      String tableName,
                                      TableResult.State state,
                                      Class<? extends Exception> expected) {

        TableRequest tableRequest = new TableRequest()
            .setStatement(statement)
            .setTableName(tableName)
            .setTableLimits(limits)
            .setCompartment(compartment)
            .setTimeout(15000);

        try {
            TableResult tres = handle.tableRequest(tableRequest);
            assertNotNull(tres);
            if (state != null) {
                tres.waitForCompletion(handle, 20000, 200);
                assertEquals(state, tres.getTableState());
            }
            if (expected != null) {
                fail("Expect to fail but succeed");
            }
            return tres;
        } catch (Exception e) {
            if (expected == null ||
                !expected.equals(e.getClass())) {
                fail("Unexpected exception. Expected " + expected + ", got " +
                     e);
            }
        }
        return null;
    }

    /**
     * Simpler version of tableOperation. This will not support
     * a change of limits as it doesn't accept a table name.
     */
    protected static TableResult tableOperation(NoSQLHandle handle,
                                                String statement,
                                                TableLimits limits,
                                                int waitMillis) {
        assertTrue(waitMillis > 500);
        TableRequest tableRequest = new TableRequest()
            .setStatement(statement)
            .setTableLimits(limits)
            .setTimeout(15000);

        return handle.doTableRequest(tableRequest, waitMillis, 200);
    }

    /**
     * Simpler version of tableOperation. This will not support
     * a change of limits as it doesn't accept a table name.
     */
    protected static TableResult tableOperation(NoSQLHandle handle,
                                                String compartment,
                                                String statement,
                                                TableLimits limits,
                                                int waitMillis) {
        assertTrue(waitMillis > 500);
        TableRequest tableRequest = new TableRequest()
            .setStatement(statement)
            .setTableLimits(limits)
            .setCompartment(compartment)
            .setTimeout(15000);

        return handle.doTableRequest(tableRequest, waitMillis, 1000);
    }

    /**
     * Executes a table (DDL) operation using the supplied statement.
     * This method should only be called if success is expected.
     *
     * @param handle handle to the proxy
     * @param statement a table statement
     * @limits must be non-null if it is a create table statement
     * @param state if non-null, wait for the table to reach the supplied state
     * @param waitMillis the amount of time to wait for the state to be reached
     * if state is non-null; ignored if state is null.
     */
    protected static TableResult tableOperation(NoSQLHandle handle,
                                                String statement,
                                                TableLimits limits,
                                                TableResult.State state,
                                                int waitMillis) {
        return tableOperation(handle, statement, limits, null,
                              state, waitMillis);
    }

    protected static TableResult tableOperation(NoSQLHandle handle,
                                                 String statement,
                                                 TableLimits limits,
                                                 String tableName,
                                                 TableResult.State state,
                                                 int waitMillis) {
         return tableOperation(handle, statement, limits, getCompartmentId(),
                               tableName, null /* matchETag */, state,
                               waitMillis);
    }

    /**
     * Executes a table (DDL) operation using the supplied statement.
     * This method should only be called if success is expected.
     *
     * @param handle handle to the proxy
     * @param statement a table statement
     * @limits must be non-null if it is a create table statement
     * @param comparmtentId compartment id if available
     * @tableName if non-null it's an alter table limits
     * @matchETag the etag that must be matched for this operation
     * @param state if non-null, wait for the table to reach the supplied state
     * @param waitMillis the amount of time to wait for the state to be reached
     * if state is non-null; ignored if state is null.
     */
    static TableResult tableOperation(NoSQLHandle handle,
                                      String statement,
                                      TableLimits limits,
                                      String compartment,
                                      String tableName,
                                      String matchETag,
                                      TableResult.State state,
                                      int waitMillis) {

        assertTrue(waitMillis > 500);

        String startTimeStr = Instant.now().toString();

        TableResult tres = null;
        TableRequest tableRequest = new TableRequest()
            .setStatement(statement)
            .setTableLimits(limits)
            .setTableName(tableName)
            .setCompartment(compartment)
            .setMatchEtag(matchETag)
            .setTimeout(15000);

        tres = handle.tableRequest(tableRequest);
        assertNotNull(tres);

        if (tres != null &&
            tres.getTableName() != null &&
            state != null) {
            tres.waitForCompletion(handle, waitMillis, 200);
            String msg = "[" + Instant.now() + "]Table " + tres.getTableName() +
                         " failed to reach " + state + " within " + waitMillis +
                         "ms, startTime = " + startTimeStr;
            assertEquals(msg, state, tres.getTableState());
        }
        return tres;
    }

    /* list tables */
    ListTablesResult listTables() {
        return listTables(handle);
    }

    /* list tables */
    ListTablesResult listTables(NoSQLHandle thandle) {

        ListTablesRequest listTables =
            new ListTablesRequest();
        /* ListTablesRequest returns ListTablesResult */
        ListTablesResult lres = thandle.listTables(listTables);
        return lres;
    }

    /* list tables */
    protected List<String> listNonSYSTables(NoSQLHandle thandle) {

        ListTablesResult ltr = listTables(thandle);
        ArrayList<String> tables = new ArrayList<String>();
        for (String tableName: ltr.getTables()) {
            if (tableName.startsWith("SYS$")) {
                continue;
            }
            tables.add(tableName);
        }
        return tables;
    }

    /**
     * Gets indexes for the named table, returning them as independent
     * JSON strings, one for each index.
     *
     * For now, these are extracted from the GetTableResult. At some point this
     * may be added to the public API.
     */
    static String[] listIndexes(NoSQLHandle handle, String tableName) {
        final String INDEXES = "indexes"; /* this is from TableJsonUtils */
        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName);
        TableResult res = handle.getTable(getTable);
        /* parse the JSON for navigation */

        String[] indexes = null;
        MapValue map = FieldValue.createFromJson(res.getSchema(), null).asMap();
        if (map.get(INDEXES) != null) {
            ArrayValue array = map.get(INDEXES).asArray();
            indexes = new String[array.size()];
            for (int i = 0; i < array.size(); i++) {
                indexes[i] = array.get(i).toJson();
            }
        }

        /* indexes are in top-level "indexes" field, which is an array */

        return indexes;
    }

    /**
     * Delays for the specified number of milliseconds, ignoring exceptions
     */
    public static void delay(int delayMS) {
        try {
            Thread.sleep(delayMS);
        } catch (Exception e) {
        }
    }

    /**
     * Delete all records from table specified.
     */
    void deleteTable(String tableName) {
        QueryRequest queryRequest = new QueryRequest().setStatement(
            ("delete from " + tableName));
        do {
            handle.query(queryRequest);
        } while (!queryRequest.isDone());
    }

    /* TODO: when we can rely on the sizes, assert specific sizes */
    void assertReadKB(Result res) {
        if (onprem) return;
        assertTrue(res.getReadKBInternal() > 0);
    }

    /* TODO: when we can rely on the sizes, assert specific sizes */
    void assertWriteKB(Result res) {
        if (onprem) return;
        assertTrue(res.getWriteKBInternal() > 0);
    }

    void assertReadKB(Result result, int expReadKB, boolean isAbsolute) {
        if (onprem) return;
        assertReadKB(expReadKB,
                     result.getReadKBInternal(),
                     result.getReadUnitsInternal(),
                     isAbsolute);
    }

    void assertWriteKB(Result result, int expWriteKB) {
        if (onprem) return;
        assertWriteKB(expWriteKB,
                      result.getWriteKBInternal(),
                      result.getWriteUnitsInternal());
    }

    void assertReadKB(int expKB,
                      int actualKB,
                      int actualUnits,
                      boolean isAbsolute) {
        if (onprem) return;
        assertEquals("Wrong readKB", expKB, actualKB);
        assertReadKBUnits(actualKB, actualUnits, isAbsolute);
    }

    void assertReadKBUnits(int actualKB, int actualUnits, boolean isAbsolute) {
        if (onprem) return;
        int exp = isAbsolute ? actualKB * 2 : actualKB;
        assertEquals("Wrong readUnits", exp, actualUnits);
    }

    void assertWriteKB(int expKB, int actualKB, int actualUnits) {
        if (onprem) return;
        assertEquals("Wrong writeKB", expKB, actualKB);
        assertEquals("Wrong writeUnits", expKB, actualUnits);
    }

    void assertCost(Result ret, int readUnits, int writeUnits) {
        if (onprem) return;
        assertEquals(readUnits, ret.getReadUnitsInternal());
        assertEquals(writeUnits, ret.getWriteUnitsInternal());
    }

    static void dropTable(NoSQLHandle nosqlHandle, String tableName) {
        TableResult tres = dropTableWithoutWait(nosqlHandle, tableName);

        if (tres.getTableState().equals(TableResult.State.DROPPED)) {
            return;
        }

        tres.waitForCompletion(nosqlHandle, 20000, 200);
    }

    static void dropTableWithoutWait(String tableName) {
        dropTableWithoutWait(handle, tableName);
    }

    private static TableResult dropTableWithoutWait(NoSQLHandle nosqlHandle,
                                                    String tableName) {
        final String dropTableDdl = "drop table if exists " + tableName;

        TableRequest tableRequest = new TableRequest()
            .setStatement(dropTableDdl)
            .setTimeout(100000);

        TableResult tres = nosqlHandle.tableRequest(tableRequest);
        assertNotNull(tres);
        return tres;
    }

    /**
     * Lists all tables and drops them.
     */
    void dropAllTables() {
        dropAllTables(handle, false);
    }

    protected static void dropAllTables(NoSQLHandle nosqlHandle, boolean wait) {

        /* get the names of all tables under this tenant */
        ListTablesRequest listTables = new ListTablesRequest();
        ListTablesResult lres = nosqlHandle.listTables(listTables);
        ArrayList<TableResult> droppedTables = new ArrayList<TableResult>();

        String[] tables = lres.getTables();
        if (tables.length == 0) {
            return;
        }

        /*
         * clean up all the tables in descending order of name, this is to drop
         * child table before its parent
         */
        Arrays.sort(tables, String.CASE_INSENSITIVE_ORDER.reversed());
        for (int i = 0; i < tables.length; i++) {
            String tableName = tables[i];
            /* on-prem config may find system tables, which can't be dropped */
            if (tableName.startsWith("SYS$")) {
                continue;
            }

            /* ignore, but note exceptions */
            try {
                if (wait) {
                    dropTable(nosqlHandle, tableName);
                    continue;
                }
                TableResult tres = dropTableWithoutWait(nosqlHandle, tableName);
                droppedTables.add(tres);
            } catch (Exception e) {
                System.err.println("DropAllTables: drop fail, table "
                                   + tableName + ": " + e);
            }
        }
        if (wait) {
            return;
        }

        /*
         * don't wait for ACTIVE state. This may mean occasional
         * failures but as long as tests pass that is ok.
         */

        /* wait for all tables dropped */
        for (TableResult tres: droppedTables) {
            /* ignore, but note exceptions */
            try {
                tres.waitForCompletion(nosqlHandle, 100000, 200);
            } catch (Exception e) {
                System.err.println("DropAllTables: drop wait fail, table "
                                   + tres + ": " + e);
            }
        }
    }

    static String tenantIdQueryString() {
        return "?" + TENANT_ID + "=" + getTenantId();
    }

    /* these may be more flexible in the future */

    static String getEndpoint() {
        return getEndpoint(0);
    }

    static String getEndpoint(int proxyNum) {
        return getProxyHost() + ":" + getProxyPort(proxyNum);
    }

    public static boolean onprem() {
        return onprem;
    }

    public static int getProxyPort() {
        return getProxyPort(0);
    }

    public static int getProxyPort(int proxyNum) {
        return PROXY_PORT + (proxyNum * 10);
    }

    public static String getProxyHost() {
        return hostName;
    }

    public static int getProxyHttpsPort() {
        return getProxyHttpsPort(0);
    }

    public static int getProxyHttpsPort(int proxyNum) {
        return PROXY_HTTPS_PORT + (proxyNum * 10);
    }

    protected static String getCompartmentId() {
        if (TEST_COMPARTMENT_ID != null) {
            return TEST_COMPARTMENT_ID;
        }
        return getTenantId();
    }

    public static String getTenantId() {
        return TEST_TENANT_ID;
    }

    public static boolean isSecure() {
        return SECURITY_ENABLED;
    }

    static boolean getProxyMonitorStatusEnabled() {
        return PROXY_MONITOR_STATS_ENABLED;
    }

    protected static String getProxyBase() {
        return System.getProperty("proxyroot");
    }

    /*
     * TODO: Remove this method after upgrade the configured KV has the fix for
     * NOSQL-378.
     *
     * Now the fix for NOSQL-378 has been included in KV 21.3 but not for 21.2
     * used by current proxy yet. The fix impacts the expected read cost in
     * query related tests including QueryTest, ChildTableTest and KVProxyTest.
     *
     * In order to make the unit tests can be run with KV with or without fix,
     * call this method to check if current KV has this fix and adjust query
     * cost if needed.
     */
    protected static boolean dontDoubleChargeKey() {
        return checkKVVersion(21, 3, 1);
    }

    /*
     * Used to skip test if run against KV prior to the specified version
     * <major>.<minor>.<patch>.
     */
    protected static void assumeKVVersion(String test,
                                          int major,
                                          int minior,
                                          int patch) {

        assumeTrue("Skipping " + test + " if run against KV prior to " +
                   (major + "." + minior + "." + patch) + ": " +
                   KVVersion.CURRENT_VERSION.getNumericVersionString(),
                   checkKVVersion(major, minior, patch));
    }

    /*
     * Returns true if the current KV is >= version <major.minor.patch>
     */
    public static boolean checkKVVersion(int major, int minior, int patch) {
        KVVersion minVersion = new KVVersion(major, minior, patch, null);
        return KVVersion.CURRENT_VERSION.compareTo(minVersion) >= 0;
    }

    protected static String makeString(int size) {
        final String pattern = "abcde";
        StringBuilder sb = new StringBuilder(size);
        while (sb.length() < size) {
            sb.append(pattern);
        }
        sb.delete(size, size + pattern.length());
        return sb.toString();
    }

    /**
     * Returns the expected read and write KB for the PutRequest, the return
     * value is an int array that contains 2 values: ReadKB and WriteKB.
     */
    static int[] getPutReadWriteCost(PutRequest request,
                                     boolean shouldSucceed,
                                     boolean rowPresent,
                                     int recordKB,
                                     boolean putOverwrite) {

        final int minRead = getMinRead();
        int readKB = 0;
        int writeKB = 0;

        if (request.getOption() != null) {
            boolean readReturnRow = rowPresent;

            switch (request.getOption()) {
            case IfAbsent:
                readKB = readReturnRow ? recordKB : minRead;
                writeKB = shouldSucceed ? recordKB : 0;
                break;
            case IfVersion:
                readKB = readReturnRow ? recordKB : minRead;
                writeKB = shouldSucceed ? (recordKB /* old record size */ +
                                           recordKB /* new record size */) : 0;
                break;
            case IfPresent:
                /*
                 * PutIfPresent can return previous row and cost MIN_READ for
                 * searching existing row
                 */
                readKB = readReturnRow ? recordKB : minRead;
                writeKB = shouldSucceed ? (recordKB /* old record size */ +
                                           recordKB /* new record size */) : 0;
                break;
            }
        } else {
            /* Put can return previous row. If putOverwrite is true put
             * overwrites existing row i.e. delete + insert and consume 2x
             * write units.
             */
            readKB = rowPresent ? recordKB  : 0;
            writeKB = (putOverwrite) ? recordKB + recordKB : recordKB;
        }

        return new int[] {readKB, writeKB};
    }

    /**
     * Returns the expected read and write KB for the DeleteRequest, the return
     * value is an int array that contains 2 values: ReadKB and WriteKB.
     */
    static int[] getDeleteReadWriteCost(DeleteRequest request,
                                        boolean shouldSucceed,
                                        boolean rowPresent,
                                        int recordKB) {

        final int minRead = getMinRead();
        int readKB = 0;
        int writeKB = 0;

        boolean readReturnRow = rowPresent;
        if (request.getMatchVersion() != null) {
            /*
             * The record is present but the version does not matched, read
             * cost is recordKB, otherwise MIN_READ.
             */
            readKB = readReturnRow ? recordKB : minRead;
            writeKB = shouldSucceed ? recordKB : 0;
        } else {
            /* Delete can return previous row */
            readKB = readReturnRow ? recordKB : minRead;
            writeKB = shouldSucceed ? recordKB : 0;
        }

        return new int[] {readKB, writeKB};
    }

    protected static int getMinRead() {
        return MIN_READ;
    }

    protected static String getSCURL() {
        if (cloudRunning) {
            return scUrlBase;
        }
        return null;
    }

    protected static String setOpThrottling(String tenantId, int value) {
        if (!useMiniCloud) {
            return null;
        }
        final String TID = "tenantId";
        final String RATE = "rate";

        /* a map for results -- makes JSON handling easier */
        Map<String, Object> payload = new HashMap<String, Object>();
        Map<String, Object> oprate = new HashMap<String, Object>();
        payload.put("opRate", oprate);
        oprate.put(RATE, value);
        if (tenantId != null) {
            oprate.put(TID, tenantId);
        }

        final String url = tmUrlBase + "config";
        final HttpRequest httpRequest = new HttpRequest().disableRetry();

        HttpResponse response =
            httpRequest.doHttpPost(url, JsonUtils.print(payload));
        return response.getOutput();
    }

    protected static HttpResponse getPeakUsage(String tenantId,
                                               String tableName,
                                               long startTime,
                                               long endTime) {
        /* Cloud-only */
        if (tmUrlBase == null) {
            return null;
        }

        String url = tmUrlBase + "tables/" + tableName +
            "/peakusage?tenantid=" + tenantId + "&compartmentid=" + tenantId;

        if (startTime != 0) {
            url = url + "&start_timestamp=" + TimeUtils.getTimeStr(startTime);
        }
        if (endTime != 0) {
            url = url + "&end_timestamp=" + TimeUtils.getTimeStr(endTime);
        }

        final HttpRequest httpRequest = new HttpRequest().disableRetry();

        return httpRequest.doHttpGet(url);
    }

    protected static List<MapValue> doQuery(NoSQLHandle qHandle, String query) {
        List<MapValue> results = new ArrayList<MapValue>();
        QueryRequest queryRequest = new QueryRequest().setStatement(query);
        do {
            QueryResult qres = qHandle.query(queryRequest);
            results.addAll(qres.getResults());
        } while (!queryRequest.isDone());
        return results;
    }

    protected static List<MapValue> doPreparedQuery(
        NoSQLHandle qHandle, String query) {

        List<MapValue> results = new ArrayList<MapValue>();
        PrepareRequest prepReq = new PrepareRequest()
            .setStatement(query);
        PrepareResult prepRet = qHandle.prepare(prepReq);
        assertNotNull(prepRet.getPreparedStatement());

        QueryRequest queryRequest =
            new QueryRequest().setPreparedStatement(prepRet);
        do {
            QueryResult qres = qHandle.query(queryRequest);
            results.addAll(qres.getResults());
        } while (!queryRequest.isDone());
        return results;
    }

    protected static void doTableRequest(NoSQLHandle nosqlHandle,
                                         String statement,
                                         boolean isDrop) {
        TableRequest tableRequest = new TableRequest()
            .setStatement(statement);
        TableResult tres = nosqlHandle.tableRequest(tableRequest);
        State waitState = isDrop ? State.DROPPED : State.ACTIVE;

        tres.waitForCompletion(nosqlHandle, 60000, 200);
        assertEquals(tres.getTableState(), waitState);
    }

    /**
     * Simple put utility, assumes success
     */
    protected static void doPut(NoSQLHandle nosqlHandle,
                                String tableName,
                                String rowAsJson) {
        PutRequest preq = new PutRequest().setTableName(tableName)
            .setValueFromJson(rowAsJson, null);
        PutResult pres = nosqlHandle.put(preq);
        assertNotNull(pres.getVersion());
    }

    /**
     * Creates a (driver-based) MapValue from a JSON string.
     */
    protected static MapValue createMapValueFromJson(String json) {
        return oracle.nosql.driver.values.JsonUtils.createValueFromJson(
            json, null).asMap();
    }

    /*
     * NOTE: this may not work on-prem because the limit is not
     * enforced.
     */
    public static int getEffectiveMaxReadKB(QueryRequest qr) {
        return (qr.getMaxReadKB() == 0 ? rlimits.getRequestReadKBLimit() :
                qr.getMaxReadKB());
    }

    /**
     * This is factored here so that it can be used by both the cloudsim-based
     * tests and kv.
     */
    protected void doLargeRow(NoSQLHandle thandle, boolean doWriteMultiple) {
        final String createTableStatement =
            "create table bigtable(" +
            "id integer, " +
            "large array(string), " +
            "primary key(id))";
        TableRequest tableRequest = new TableRequest()
            .setStatement(createTableStatement);
        TableResult tres = thandle.tableRequest(tableRequest);
        tres.waitForCompletion(thandle, 10000, 200);
        MapValue value = new MapValue().put("id", 1);
        ArrayValue array = createLargeStringArray(3500000);
        value.put("large", array);
        PutRequest preq = new PutRequest().setTableName("bigtable").
            setValue(value);
        PutResult pres = thandle.put(preq);
        assertNotNull(pres.getVersion());

        if (doWriteMultiple) {
            /*
             * Now with write multiple
             */
            WriteMultipleRequest wmReq = new WriteMultipleRequest();
            /* don't reuse the PutRequest above, it has been modified */
            preq = new PutRequest().setTableName("bigtable").setValue(value);
            wmReq.add(preq, false);
            WriteMultipleResult wmRes = thandle.writeMultiple(wmReq);
            assertEquals(1, wmRes.size());
        }
    }

    private ArrayValue createLargeStringArray(int size) {
        ArrayValue array = new ArrayValue();
        int tsize = 0;
        final String s = "abcdefghijklmnop";
        while (tsize < size) {
            array.add(s);
            tsize += s.length();
        }
        return array;
    }

    protected static void verbose(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }

    /**
     * Get an HttpClient instance.
     * Used by tests that need low-level http clients.
     */
    protected static HttpClient createHttpClient(String host,
                                                 int port,
                                                 int numThreads,
                                                 String name,
                                                 Logger logger) {
        /*
         * java SDK changed its internal HttpClient constructors
         * as of 5.3.2. So use reflection to figure out which
         * methods to call.
         */
        try {
            Class<?> hcClass = Class.forName(
                              "oracle.nosql.driver.httpclient.HttpClient");

            try {
                /* new driver method */
                return (HttpClient)hcClass.getMethod(
                                             "createMinimalClient",
                                             String.class,
                                             int.class,
                                             SslContext.class,
                                             int.class,
                                             String.class,
                                             Logger.class)
                                       .invoke(null,
                                               host,
                                               port,
                                               null /* SslContext */,
                                               0 /* handshakeTimeout */,
                                               name,
                                               logger);
            } catch (NoSuchMethodException e) {
                /* old driver method */
                return (HttpClient)hcClass.getDeclaredConstructor(
                                                   String.class,
                                                   int.class,
                                                   int.class,
                                                   int.class,
                                                   int.class,
                                                   SslContext.class,
                                                   String.class,
                                                   Logger.class)
                                               .newInstance(
                                                   host,
                                                   port,
                                                   numThreads,
                                                   0 /* ConnectionPoolSize */,
                                                   0 /* PoolMaxPending */,
                                                   null /* SslContext */,
                                                   name,
                                                   logger);
            }
        } catch (Exception e) {
            System.out.println("Can't create HttpClient: " + e);
            return null;
        }
    }

    protected static int getV4ErrorCode(ByteBuf buf) {
        oracle.nosql.proxy.protocol.ByteInputStream b = null;
        try {
            buf.readerIndex(0);
            b = new oracle.nosql.proxy.protocol.ByteInputStream(buf);
            MapWalker walker = new MapWalker(b);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    return Nson.readNsonInt(b);
                } else {
                    walker.skip();
                }
            }
        } catch (Exception e) {
            return -1;
        } finally {
            if (b != null) {
                b.close();
            }
        }
        return -1;
    }

    protected static void forceV3(NoSQLHandleImpl handle) {
        assertNotNull(handle);
        short version = handle.getSerialVersion();
        if (version <= 3) {
            return;
        }

        /* Sigh. we can't guarantee that the SDK has this method. */
        Class<?> clientClass = null;
        try {
            clientClass = Class.forName("oracle.nosql.driver.http.Client");
        } catch (Throwable e) {
            System.out.println("Could not find Client class:" + e);
            clientClass = null;
        }
        assertNotNull(clientClass);
        Method setVersionFunction = null;
        try {
            setVersionFunction = clientClass.getMethod("setSerialVersion",
                                                        short.class);
        } catch (Throwable e) {
            verbose("Could not find Client.setSerialVersion(): " + e);
            verbose("Skipping test");
            setVersionFunction = null;
        }
        assumeTrue(setVersionFunction != null);
        try {
            setVersionFunction.invoke(handle.getClient(), (short)3);
        } catch (Exception e) {
            verbose("Could not invoke Client.setSerialVersion(): " + e);
            verbose("Skipping test");
            assumeTrue(false);
        }
        verbose("Set serial version to 3");
    }


    protected static TableResult getTable(String tableName,
                                          NoSQLHandle handle) {
        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName);
        return handle.getTable(getTable);
    }

    /* set the given cmdline parameter to true unless set otherwise in prop */
    protected static void setDefaultTrue(Properties cmdLine,
                                         String prop, String param) {
        boolean propVal = true;
        if (System.getProperty(prop) != null) {
            propVal = Boolean.getBoolean(prop);
        }
        cmdLine.setProperty(param, Boolean.toString(propVal));
    }

    /*
     * Get table ocid using SC rest call
     *   /V0/tm/tables/<tableName>?tenantid=&&compartmentid=
     */
    protected static String scGetTableOcid(String tenantId,
                                           String compartmentId,
                                           String tableName)
        throws Exception {

        if (tmUrlBase == null) {
            return null;
        }

        HttpRequest httpRequest = new HttpRequest().disableRetry();
        String url = tmUrlBase + "tables/" + tableName +
                     "?" + HttpConstants.TENANT_ID + "=" + tenantId +
                     "&&" + HttpConstants.COMPARTMENT_ID + "=" + compartmentId;

        HttpResponse res = httpRequest.doHttpGet(url);
        if (res.getStatusCode() != HttpURLConnection.HTTP_OK) {
            throw new IllegalStateException(
                "Failed to get table " + res.getOutput());
        }

        TableInfo tif = JsonUtils.readValue(res.getOutput(), TableInfo.class);
        String ocid = tif.getTableOcid();
        if (ocid != null) {
            return ocid.replace("_", ".");
        }
        return null;
    }

    public static String currentTimeString() {
        return ZonedDateTime.now(ZoneOffset.UTC).
            format(DateTimeFormatter.ISO_INSTANT);
    }

    protected static void assertTableOcid(String ocid) {
        if (onprem) {
            assertNull(ocid);
        } else {
            assertNotNull(ocid);
            if (cloudRunning) {
                assertTrue(ocid.contains(AccessContext.EXTERNAL_OCID_PREFIX));
            }
        }
    }

    public static class Timer {
        private long start;
        private long end;

        public Timer() {
            start = System.currentTimeMillis();
        }

        public Timer start() {
            start = System.currentTimeMillis();
            return this;
        }

        public Timer stop() {
            end = System.currentTimeMillis();
            return this;
        }

        public double getTimeSeconds() {
            return (end - start)/1000.0;
        }

        public long getTimeMillis() {
            return end - start;
        }

        public long getStartMillis() {
            return start;
        }

        public long getEndMillis() {
            return end;
        }

        @Override
        public String toString() {
            return Double.toString(getTimeSeconds());
        }
    }

    /*
     * The TanantManager used in local proxy cloud unit test.
     *   - Simulate MR table, mrTableNames is list of MR table names.
     */
    private static class TestTenantManager extends LocalTenantManager {

        private final List<String> mrTableNames;

        private TestTenantManager(Config cfg, List<String> mrTableNames) {
            super(connectKVStore(cfg.getTemplateKVStoreConfig()
                                    .setStoreName(cfg.getStoreName())
                                    .setHelperHosts(cfg.getHelperHosts())),
                  cfg.getStoreName(),
                  false, /* noLimits */
                  cfg.getHelperHosts());
            this.mrTableNames = mrTableNames;
        }

        @Override
        public void createTableCache(Config config,
                                     MonitorStats stats,
                                     SkLogger logger) {

            tableCache = new PassThroughTableCache(this, logger) {
                @Override
                public TableEntry getTable(String namespace,
                                           String tableName,
                                           String nsname,
                                           LogContext lc) {
                   TableEntry entry = super.getTable(namespace,
                                                     tableName,
                                                     nsname,
                                                     lc);
                   return convertEntry(entry);
                }
            };
        }

        private TableEntry convertEntry(TableEntry entry) {

            return new TableEntry(entry.getTable()) {

                @Override
                public KVStoreImpl getStore() {
                    return entry.getStore();
                }

                @Override
                public TableAPIImpl getTableAPI() {
                    return entry.getTableAPI();
                }

                @Override
                public String getStoreName() {
                    return entry.getStoreName();
                }

                @Override
                public RequestLimits getRequestLimits() {
                    return entry.getRequestLimits();
                }

                @Override
                public boolean isMultiRegion() {
                    if (mrTableNames != null) {
                        return mrTableNames.contains(
                                getTable().getFullName().toLowerCase());
                    }
                    return false;
                }

                @Override
                public boolean isInitialized() {
                    return true;
                }
            };
        }
    }
}
