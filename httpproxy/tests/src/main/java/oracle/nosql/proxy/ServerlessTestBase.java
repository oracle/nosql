/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static oracle.nosql.driver.util.HttpConstants.ACCEPT;
import static oracle.nosql.driver.util.HttpConstants.CONNECTION;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_LENGTH;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_TYPE;
import static oracle.nosql.driver.util.HttpConstants.REQUEST_ID_HEADER;
import static oracle.nosql.driver.util.HttpConstants.REQUEST_SERDE_VERSION_HEADER;
import static oracle.nosql.driver.util.HttpConstants.USER_AGENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import oracle.kv.util.kvlite.KVLite;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableResult.State;
import oracle.nosql.driver.ops.serde.BinaryProtocol;
import oracle.nosql.driver.ops.serde.BinarySerializerFactory;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;
import oracle.nosql.driver.util.NettyByteInputStream;
import oracle.nosql.driver.util.NettyByteOutputStream;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.kv.KVTenantManager;
import oracle.nosql.proxy.sc.LocalTenantManager;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.security.AccessCheckerFactory;
import oracle.nosql.proxy.util.KVLiteBase;
import oracle.nosql.util.tmi.TableRequestLimits;
import oracle.nosql.util.tmi.TenantLimits;

/**
 * This is a test base class that creates a kvlite instance and
 * a proxy that runs in-process without an HTTP server.
 * Test cases look like this:
 *   PutRequest putRequest = new PutRequest()
 *      .setValue(value)
 *      .setTableName(tableName);
 *   PutResult pres = (PutResult) doRequest(putRequest, ....);
 *
 * The doRequest method is the same as the various methods on the SDK's
 * NoSQLHandle but abstracted to handle different serialization mechanisms and
 * protocols.
 */
public class ServerlessTestBase extends KVLiteBase {

    /*
     * Proxy state
     */
    protected static boolean SECURITY_ENABLED = false;
    protected final static String TEST_TENANT_ID =
            System.getProperty("tenant.id", "ProxyTestTenant");
    protected static String PROXY_ASYNC_PROP = "test.async";
    protected static String PROXY_ERROR_LIMITING_PROP = "test.errorlimiting";
    protected static String KVLITE_USETHREADS_PROP = "test.usethreads";
    protected static String KVLITE_MULTISHARD_PROP = "test.multishard";
    protected static String KVLITE_MEMORYMB_PROP = "test.memorymb";

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

    protected static String hostName = getHostName();
    protected static final int startPort = 13240;
    protected static KVLite kvlite;
    protected static Proxy proxy;
    protected static TenantManager tm = null;
    protected static AccessChecker ac = null;
    protected static ProxyAuditManager audit = null;

    /* set to true if running against an existing cloud proxy */
    protected static boolean onprem = false;
    protected static boolean verbose = false;
    protected static boolean multishard = false;
    protected static int memoryMB = 0;
    protected static boolean isAsync = Boolean.getBoolean(PROXY_ASYNC_PROP);

    protected static DataService dataService;
    protected static TableLimits tableLimits = new TableLimits(10, 20, 1);
    protected static LogContext lc;

    /* make these available to V4 tests */

    protected static short v3ProtocolVersion = 3;
    protected static SerializerFactory binaryFactory =
        new BinarySerializerFactory();
    protected static RequestSerializer binarySerializer =
        new BinaryRequestSerializer();
    protected static ResponseDeserializer binaryDeserializer =
        new BinaryResponseDeserializer();

    /*
     * An instance with non-default limits to make tests run reasonably
     */
    protected static TenantLimits tenantLimits =
        TenantLimits.getNewDefault();
    static {
        tenantLimits.setNumTables(10)
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
                   .setSchemaEvolutions(6);
    }

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

        assumeTrue("Skipping serverless tests for minicloud or cloud runs",
                   !Boolean.getBoolean("usemc") &&
                   !Boolean.getBoolean("usecloud"));

        staticSetUp(tenantLimits);
    }

    public static void staticSetUp(TenantLimits tl)
       throws Exception {
        doStaticSetup();

        startup(tl);
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        if (proxy != null) {
            proxy.shutdown(3, TimeUnit.SECONDS);
        }

        if (tm != null) {
            tm.close();
        }

        if (kvlite != null) {
            kvlite.stop(false);
        }

        cleanupTestDir();
    }

    @Before
    public void setUp() throws Exception {
        /* RequestContext must use a couple of local overloads */
        dataService.setRequestContextFactory(new ServerlessContextFactory());
        lc = proxy.generateLogContext("none");
    }

    @After
    public void tearDown() throws Exception {
    }

    protected static void startup() throws Exception {
        startup(tenantLimits);
    }

    protected static void startup(TenantLimits pTenantLimts)
        throws Exception {

        /*
         * Determine if running against an existing cloud proxy such as the
         * MiniCloud. If so, don't start KVLite or a proxy or the aggregation
         * service. Also check for proxy host and port set in system properties
         * to override the defaults.
         */
        onprem = Boolean.getBoolean("onprem");
        verbose = Boolean.getBoolean("test.verbose");

        if (verbose) {
            System.out.println("Starting tests in verbose output mode");
        }

        Boolean securityEnabled = Boolean.getBoolean("security");
        if (securityEnabled) {
            SECURITY_ENABLED = true;
        }

        cleanupTestDir();

        multishard = Boolean.getBoolean(KVLITE_MULTISHARD_PROP);
        memoryMB = Integer.getInteger(KVLITE_MEMORYMB_PROP, 0);
        boolean useThreads = Boolean.getBoolean(KVLITE_USETHREADS_PROP);
        if (useThreads) {
            multishard = false;
            if (verbose) {
                System.out.println("Starting kvlite using threads in this jvm");
            }
        } else if (verbose) {
            System.out.println("Starting kvlite using separate jvm process, " +
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

        /*
         * Use Properties to create a Config object for Proxy
         */
        Properties commandLine = new Properties();
        commandLine.setProperty(Config.STORE_NAME.paramName,
                                getStoreName());

        commandLine.setProperty(Config.HELPER_HOSTS.paramName,
                                (hostName + ":" + getKVPort()));


        Config.ProxyType ptype = (onprem ? Config.ProxyType.KVPROXY :
                                  Config.ProxyType.CLOUDTEST);

        commandLine.setProperty(Config.PROXY_TYPE.paramName, ptype.name());

        commandLine.setProperty(Config.VERBOSE.paramName,
                                Boolean.toString(verbose));
        commandLine.setProperty(Config.ASYNC.paramName, Boolean.toString(
                                    Boolean.getBoolean(PROXY_ASYNC_PROP)));

        /* Error limiting configs */
        commandLine.setProperty(Config.ERROR_LIMITING_ENABLED.paramName,
                                Boolean.toString(
                                Boolean.getBoolean(PROXY_ERROR_LIMITING_PROP)));

        /* create config from commandLine properties */
        Config cfg = new Config(commandLine);

        /* create an appropriate TenantManager */
        if (onprem) {
            /* note: in KVPROXY mode the proxy *requires* a KVTenantManager */
            tm = KVTenantManager.createTenantManager(cfg);
        } else {
            tm = LocalTenantManager.createTenantManager(cfg);
        }

        /* create a simple access checker */
        ac = AccessCheckerFactory.createInsecureAccessChecker();

        /* this creates and starts a proxy without the netty server */
        proxy = new Proxy(cfg, tm, ac, audit);
        proxy.startServer(null, false);
        dataService = (DataService) proxy.getService("ProxyData");
    }

    static TableResult tableOp(String statement,
                               TableLimits limits,
                               String tableName,
                               RequestSerializer ser,
                               ResponseDeserializer deser,
                               short protocol) {

        TableRequest tableRequest = new TableRequest()
            .setStatement(statement)
            .setTableLimits(tableLimits)
            .setTableName(tableName);
        TableResult res =
            (TableResult) doRequest(tableRequest, ser, deser, protocol);
        return waitForCompletion(res.getTableName(),
                                 res.getOperationId(),
                                 ser, deser, protocol);
    }

    static TableResult waitForCompletion(String tableName,
                                         String opId,
                                         RequestSerializer ser,
                                         ResponseDeserializer deser,
                                         short protocol) {

        TableResult res = getTable(tableName, opId, ser, deser, protocol);
        TableResult.State state = res.getTableState();
        while (!isTerminal(state)) {
            try {
                Thread.sleep(500);
            } catch (Exception e) {} // ignore
            res = getTable(tableName, opId, ser, deser, protocol);
            state = res.getTableState();
        }
        return res;
    }

    static TableResult getTable(String tableName, String opId,
                                RequestSerializer ser,
                                ResponseDeserializer deser,
                                short protocol) {

        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName).
            setOperationId(opId);
        return (TableResult) doRequest(getTable, ser, deser, protocol);
    }

    static boolean isTerminal(TableResult.State state) {
        return state == State.ACTIVE || state == State.DROPPED;
    }

    interface RequestSerializer {
        ByteBuf serialize(Request request) throws IOException;
    }

    interface ResponseDeserializer {
        Result deserialize(Request request, ServerlessContext ctx)
            throws IOException;
    }

    static Result doV3Request(Request request) {
        return doRequest(request, binarySerializer,
                         binaryDeserializer, v3ProtocolVersion);
    }

    /**
     * Perform the request using direct proxy calls:
     *  1. serialize the request
     *  2. create a FullHttpRequest to encapsulate the payload
     *  3. add HTTP headers expected by proxy
     *  4. call the proxy's DataService.handleRequest method directly using
     *  a local ServerlessContext instance as an opaque reference so that the
     *  finishOp() method can grab the response
     *  5. deserialize the response which is in the ServerlessContext object
     */
    static Result doRequest(Request req,
                            RequestSerializer ser,
                            ResponseDeserializer deser,
                            short protocol) {

        try {
            ServerlessContext ctx = new ServerlessContext();
            ByteBuf content = ser.serialize(req);
            FullHttpRequest request =
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                           HttpMethod.POST,
                                           "http://direct", /* no URI */
                                           content);
            /* infer version header from protocol */
            String versionHeader = (protocol >= 4 ? "v4" : null);
            addRequestHeaders(req, request, versionHeader);
            /*
             * do the op
             * NOTE: handleRequest always returns null. Responses are handled
             * via DataService.finishOp()
             */
            dataService.handleRequest(request,
                                      null,
                                      lc,
                                      ctx);

            /* if not using an async proxy this is a no-op */
            ctx.await(req.getTimeoutInternal());

            /* release request buffer */
            content.release();

            return deser.deserialize(req, ctx);
        } catch (IOException ioe) {
            throw new NoSQLException("Exception in doRequest", ioe);
        }
    }

    static void addRequestHeaders(Request request,
                                  FullHttpRequest httpRequest,
                                  String versionHeader) {
        HttpHeaders headers = httpRequest.headers();
        headers.set(CONTENT_TYPE, "application/octet-stream")
            .set(CONNECTION, "keep-alive")
            .set(ACCEPT, "application/octet-stream")
            .set(USER_AGENT, "direct")
            .set(REQUEST_ID_HEADER, "no-id")
            .set(CONTENT_LENGTH, httpRequest.content().readableBytes());
        if (!onprem) {
            headers.set("Authorization", "Bearer nobody");
        }
        if (versionHeader != null) {
            headers.set(REQUEST_SERDE_VERSION_HEADER, versionHeader);
        }
    }

    /**
     * Serialize a Request returning a new ByteBuf
     */
    static class BinaryRequestSerializer implements RequestSerializer {
        @Override
        public ByteBuf serialize(Request req) throws IOException {
            ByteBuf content = Unpooled.buffer();
            ByteOutputStream bos = new NettyByteOutputStream(content);
            bos.writeShort(v3ProtocolVersion);
            Serializer ser = req.createSerializer(binaryFactory);
            if (req instanceof QueryRequest ||
                req instanceof PrepareRequest) {
                ser.serialize(req,
                              v3ProtocolVersion,
                              QueryDriver.QUERY_V3,
                              bos);
            } else {
                ser.serialize(req,
                              v3ProtocolVersion,
                              bos);
            }

            return content;
        }
    }

    /**
     * Deserialize response
     */
    static class BinaryResponseDeserializer implements ResponseDeserializer {

        @Override
        public Result deserialize(Request req,
                                  ServerlessContext ctx) throws IOException {

            assertEquals(HttpResponseStatus.OK, ctx.status);
            ByteBuf content = ctx.content;
            try (ByteInputStream bis = new NettyByteInputStream(content)) {
                int code = bis.readByte();
                if (code == 0) {
                    Result res;
                    Serializer ser = req.createDeserializer(binaryFactory);
                    if (req instanceof QueryRequest ||
                        req instanceof PrepareRequest) {
                        res = ser.deserialize(req,
                                              bis,
                                              v3ProtocolVersion,
                                              QueryDriver.QUERY_V3);
                    } else {
                        res = ser.deserialize(req,
                                              bis,
                                              v3ProtocolVersion);
                    }
                    return res;
                }
                String err = SerializationUtil.readString(bis);
                throw BinaryProtocol.mapException(code, err);
            } finally {
                /* release the response buffer */
                content.release();
            }
        }
    }

    /*
     * Used to get the proxy's response which would normally be sent
     * directly to the client
     */
    static class ServerlessContextFactory
        implements DataServiceHandler.RequestContextFactory {

        @Override
        public DataServiceHandler.RequestContext createRequestContext(
            FullHttpRequest request,
            ChannelHandlerContext ctx,
            LogContext lc,
            Object callerContext) {

            return new DataServiceHandler.RequestContext(
                request, ctx, lc, callerContext) {

                @Override
                public void finishOp(FullHttpResponse response) {
                    if (callerContext instanceof ServerlessContext) {
                        ((ServerlessContext) callerContext).setResponse(response);
                    }
                }

                @Override
                public void resetBuffers() {
                    if (bbis != null) {
                        bbis.buffer().readerIndex(inputOffset);
                    }
                    if (bbos == null) {
                        ByteBuf resp = Unpooled.buffer();
                        bbos = new oracle.nosql.proxy.protocol.ByteOutputStream(
                            resp);
                    }
                    bbos.buffer().writerIndex(0);
                }
            };
        }
    }

    /**
     * holds http response info for deserialization
     */
    static class ServerlessContext {
        public ByteBuf content;
        public HttpResponseStatus status;
        private ReentrantLock lock;
        private Condition cond;

        public ServerlessContext() {
            lock = new ReentrantLock();
            cond = lock.newCondition();
        }

        public void await(int timeoutMs) {
            if (lock == null) {
                return; // nothing to do, not async
            }
            if (timeoutMs == 0) {
                timeoutMs = 5000; // default
            }
            lock.lock();
            try {
                while (status == null) {
                    boolean ok =
                        cond.await(timeoutMs, TimeUnit.MILLISECONDS);
                    if (!ok) {
                        /* timeout */
                        throw new RequestTimeoutException(
                            "Operation timed out after " + timeoutMs +
                            " milliseconds");
                    }
                }
            } catch (InterruptedException ie) {
                throw new IllegalStateException("Timeout waiting for response");
            } finally {
                lock.unlock();
            }
        }

        public void setResponse(FullHttpResponse response) {
            if (lock == null) {
                this.status = response.status();
                this.content = response.content();
            } else {
                lock.lock();
                this.status = response.status();
                this.content = response.content();
                try {
                    cond.signal();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    protected static String genString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append((char)('A' + i % 26));
        }
        return sb.toString();
    }
}
