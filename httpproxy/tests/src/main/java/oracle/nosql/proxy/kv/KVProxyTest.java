/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.kv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Writer;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValueFactory;
import oracle.nosql.driver.BatchOperationNumberLimitException;
import oracle.nosql.driver.IndexExistsException;
import oracle.nosql.driver.IndexNotFoundException;
import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.OperationNotSupportedException;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.ResourceExistsException;
import oracle.nosql.driver.ResourceNotFoundException;
import oracle.nosql.driver.SystemException;
import oracle.nosql.driver.TableExistsException;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.UserInfo;
import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.SystemRequest;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.ProxyMain;
import oracle.nosql.proxy.ProxyTestBase;

public class KVProxyTest extends ProxyTestBase {

    final static int BATCH_OP_NUMBER_LIMIT = rlimits.getBatchOpNumberLimit();
    protected String endpoint;
    protected StoreAccessTokenProvider authProvider;
    protected NoSQLHandle kvhandle;
    protected static CommandServiceAPI admin;
    protected static KVStore store;

    private static void writeFile(String string, File file)
        throws IOException {

        final Writer out = new FileWriter(file);
        try {
            out.write(string);
            out.flush();
        } finally {
            out.close();
        }
    }

    @BeforeClass
    public static void staticSetUp() throws Exception {
        assumeTrue(!Boolean.getBoolean(USEMC_PROP) &&
                   !Boolean.getBoolean(USECLOUD_PROP));

        verbose = Boolean.getBoolean(VERBOSE_PROP);

        cleanupTestDir();
        /*
         * Filter out the std output of kvlite. It prints to stdout
         * when generating security information.
         */
        PrintStream printStreamOriginal = System.out;
        System.setOut(new PrintStream(new OutputStream() {
                @Override
                public void write(int b) throws IOException {}
            }));
        kvlite = startKVLite(hostName,
                             null, // default store name
                             false, // useThreads = false
                             false, // verbose = false
                             false, // isMultiShard = false
                             0,     // memoryMB = 0
                             true); // isSecure = true
        System.setOut(printStreamOriginal);

        final String securityDir = getTestDir() + "/security";
        final String loginFile = securityDir + "/user.security";

        /* create admin user */
        final KVStoreLogin storeLogin =
            new KVStoreLogin(
                "admin", loginFile);
        storeLogin.loadSecurityProperties();
        storeLogin.prepareRegistryCSF();
        final LoginCredentials creds =
            storeLogin.makeShellLoginCredentials();
        final LoginManager loginMgr =
            KVStoreLogin.getAdminLoginMgr(hostName, getKVPort(), creds);
        setAdmin(loginMgr);
        KVStoreConfig config = new KVStoreConfig(getStoreName(),
                                                 hostName+":"+
                                                 getKVPort());
        config.setSecurityProperties(storeLogin.getSecurityProperties());
        store = KVStoreFactory.getStore(config, creds, null);

        /* create proxy user with base privileges */
        int planId = admin.createCreateUserPlan(
            "Create User", "proxy", true, false,
            "NoSql00__123456".toCharArray());
        execPlan(planId);

        final File proxyLoginFile = new File(securityDir, "proxy.security");
        final File passwordFile = new File(securityDir, "proxy.passwd");
        writeFile("Password Store:\n" +
                  "secret.proxy=NoSql00__123456\n",
                  passwordFile);
        writeFile("oracle.kv.auth.pwdfile.file=" + passwordFile +
                  "\noracle.kv.auth.username=proxy" +
                  "\noracle.kv.transport=ssl" +
                  "\noracle.kv.ssl.trustStore=" + securityDir +
                  "/client.trust",
                  proxyLoginFile);

        /* create test user with extra privileges */
        planId = admin.createCreateUserPlan(
            "Create User", "test", true, false,
            "NoSql00__123456".toCharArray());
        execPlan(planId);

        final Set<String> roles = new HashSet<String>();
        roles.add(RoleInstance.READWRITE_NAME);
        roles.add(RoleInstance.DBADMIN_NAME);
        roles.add(RoleInstance.SYSADMIN_NAME);
        planId = admin.createGrantPlan(
            "Grant User", "test",
            roles);
        execPlan(planId);

        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        SelfSignedCertificate ssc = new SelfSignedCertificate(getHostName());
        prepareTruststore(ssc.certificate());

        Properties commandLine = new Properties();
        commandLine.setProperty(Config.PROXY_TYPE.paramName,
                                Config.ProxyType.KVPROXY.name());
        commandLine.setProperty(Config.HTTPS_PORT.paramName,
                                Integer.toString(
                                    ProxyTestBase.getProxyHttpsPort()));
        commandLine.setProperty(Config.STORE_NAME.paramName,
                                getStoreName());
        commandLine.setProperty(Config.HELPER_HOSTS.paramName,
                                hostName + ":" + getKVPort());

        commandLine.setProperty(Config.STORE_SECURITY_FILE.paramName,
                                proxyLoginFile.getAbsolutePath());

        commandLine.setProperty(Config.SSL_CERTIFICATE.paramName,
                                ssc.certificate().getAbsolutePath());
        commandLine.setProperty(Config.SSL_PRIVATE_KEY.paramName,
                                ssc.privateKey().getAbsolutePath());

        /* async now defaults to true */
        boolean async = true;
        if (System.getProperty(PROXY_ASYNC_PROP) != null) {
            async = Boolean.getBoolean(PROXY_ASYNC_PROP);
        }
        commandLine.setProperty(Config.ASYNC.paramName, Boolean.toString(
                                async));
        commandLine.setProperty(Config.VERBOSE.paramName, Boolean.toString(
                                verbose));

        proxy = ProxyMain.startProxy(commandLine);
        waitForStoreInit(20);
    }

    protected static void setAdmin(LoginManager mgr) throws Exception {
        admin = RegistryUtils.getAdmin(hostName, getKVPort(), mgr);
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        if (tm != null) {
            tm.close();
            tm = null;
        }
        if (proxy != null) {
            proxy.shutdown(3, TimeUnit.SECONDS);
            proxy = null;
        }
        if (kvlite != null) {
            kvlite.stop(true);
            kvlite = null;
        }
        System.clearProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY);
        System.clearProperty("javax.net.ssl.trustStore");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        endpoint = getProxyHttpsEndpoint();
        authProvider = new StoreAccessTokenProvider(
            "test", "NoSql00__123456".toCharArray());
        getHandle();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (kvhandle != null) {
            dropAllMetadata(kvhandle);
            kvhandle.close();
        }

        if (authProvider != null) {
            authProvider.close();
            authProvider = null;
        }
    }

    /**
     * A method to drop tables, namespaces, and users
     */
    static void dropAllMetadata(NoSQLHandle nosqlHandle) {
        final Set<String> exceptUsers = new HashSet<String>();
        exceptUsers.add("test");
        exceptUsers.add("proxy");

        if (nosqlHandle == null) {
            return;
        }
        /* dropAllTables doesn't catch namespaced tables -- TODO */
        ProxyTestBase.dropAllTables(nosqlHandle, true);
        dropAllNamespaces(nosqlHandle); // this uses cascade to drop tables
        dropAllUsers(nosqlHandle, exceptUsers);
    }

    static void dropAllNamespaces(NoSQLHandle nosqlHandle) {
        String[] namespaces = nosqlHandle.listNamespaces();
        if (namespaces == null) {
            return;
        }

        for (String ns : namespaces) {
            if (ns.equals("sysdefault")) {
                continue;
            }
            /* use cascade to remove tables in namespaces */
            String statement ="drop namespace " + ns + " cascade";
            doSysOp(nosqlHandle, statement);
        }
    }

    static void dropAllUsers(NoSQLHandle nosqlHandle,
                             Set<String> exceptUsers) {

        UserInfo[] uInfo = nosqlHandle.listUsers();
        if (uInfo == null) {
            return;
        }
        for (UserInfo u : uInfo) {
            if (u.getName().equals("admin") ||
                (exceptUsers != null && exceptUsers.contains(u.getName()))) {
                continue;
            }
            String statement ="drop user " + u.getName();
            doSysOp(nosqlHandle, statement);
        }
    }

    protected void getHandle() {
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);

        config.setAuthorizationProvider(authProvider);

        /*
         * Open the handle
         */
        kvhandle = NoSQLHandleFactory.createNoSQLHandle(config);
        dropAllMetadata(kvhandle);
    }

    @Test
    public void testBasic()
        throws Exception {

        final String tableName = "test";
        final String createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(id INTEGER, " +
            " pin INTEGER, " +
            " name STRING, " +
            " PRIMARY KEY(SHARD(pin), id))";

        TableRequest tableRequest = new TableRequest()
            .setStatement(createTableStatement);
        TableResult tres = kvhandle.tableRequest(tableRequest);
        tres.waitForCompletion(kvhandle, 60000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.ACTIVE);
        /* limits can be null but if not, should be set to 0 */
        assertTrue(tres.getTableLimits() == null ||
                   tres.getTableLimits().getReadUnits() == 0);

        /*
         * PUT a row
         */
        MapValue value = new MapValue().put("id", 1).
            put("pin", "654321").put("name", "test1");

        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult putResult = kvhandle.put(putRequest);
        assertNotNull(putResult.getVersion());

        /*
         * GET the row
         */
        MapValue key = new MapValue().put("id", 1).put("pin", "654321");

        GetRequest getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        GetResult getRes = kvhandle.get(getRequest);
        assertEquals("test1",
                     getRes.getValue().get("name").asString().getValue());

        /*
         * PUT a second row using JSON
         */
        String jsonString =
            "{\"id\": 2, \"pin\": 123456, \"name\":\"test2\"}";

        putRequest = new PutRequest()
            .setValueFromJson(jsonString, null)
            .setTableName(tableName);
        putResult = kvhandle.put(putRequest);
        assertNotNull(putResult.getVersion());

        /*
         * GET the second row
         */
        key = new MapValue().put("id", 2).put("pin", "123456");
        getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        assertEquals(
            "test2",
            kvhandle.get(getRequest).getValue().get("name").
            asString().getValue());

        try {
            QueryRequest queryRequest = new QueryRequest().
                setStatement("SELECT * from " + tableName +
                             " WHERE name= \"test2\"");
            QueryResult qres = kvhandle.query(queryRequest);
            List<MapValue> results = qres.getResults();
            assertEquals(results.size(), 1);
            assertEquals(2, results.get(0).get("id").asInteger().getValue());
        } catch (RequestTimeoutException rte) {
            if (KVVersion.CURRENT_VERSION.getMajor() >= 20 ||
                !(rte.getCause() instanceof SystemException)) {
                throw rte;
            }
            /* ignore this exception for 19 for now; known bug */
        }

        /*
         * Put in the third row, the pin/name field is the same as the
         * second row
         */
        jsonString = "{\"id\": 3, \"pin\": 123456, \"name\":\"test2\"}";

        putRequest = new PutRequest()
            .setValueFromJson(jsonString, null) // no options
            .setTableName(tableName);
        putResult = kvhandle.put(putRequest);
        assertNotNull(putResult.getVersion());

        /*
         * Create index, test query by indexed field
         */
        final String createIndexStatement =
            "CREATE INDEX IF NOT EXISTS idx1 ON " + tableName +
            " (name)";

        tableRequest =
            new TableRequest().setStatement(createIndexStatement);
        kvhandle.tableRequest(tableRequest);
        tres.waitForCompletion(kvhandle, 60000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.ACTIVE);

        QueryRequest queryRequest = new QueryRequest().
            setStatement("SELECT * from " + tableName +
                         " WHERE name= \"test2\"");
        QueryResult qres = kvhandle.query(queryRequest);
        List<MapValue> results = qres.getResults();
        assertEquals(results.size(), 2);
        assertEquals("test2",
                     results.get(0).get("name").asString().getValue());

        /*
         * DELETE the first row
         */
        key = new MapValue().put("id", 1).put("pin", "654321");
        DeleteRequest delRequest = new DeleteRequest()
            .setKey(key)
            .setTableName(tableName);
        DeleteResult delResult = kvhandle.delete(delRequest);
        assertTrue(delResult.getSuccess());

        /*
         * MultiDelete where name is test2
         */
        key = new MapValue().put("pin", "123456");
        MultiDeleteRequest multiDelRequest = new MultiDeleteRequest()
            .setKey(key)
            .setTableName(tableName);

        MultiDeleteResult mRes = kvhandle.multiDelete(multiDelRequest);
        assertEquals(mRes.getNumDeletions(), 2);

        /*
         * There should be no record in the table now
         */
        queryRequest = new QueryRequest().
            setStatement("SELECT * from " + tableName);
        qres = kvhandle.query(queryRequest);
        results = qres.getResults();
        assertEquals(results.size(), 0);
    }

    /*
     * Test data limits:
     *  key size
     *  index key size
     *  row size
     */
    @Test
    public void testDataLimits()
        throws Exception {

        final String tableName = "limits";
        final String createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(id STRING, " +
            " idx STRING, " +
            " name STRING, " +
            " PRIMARY KEY(id))";
        final String addIndex = "create index idx on limits(idx)";

        tableOperation(kvhandle, createTableStatement, null,
            TableResult.State.ACTIVE, 20000);
        tableOperation(kvhandle, addIndex, null, TableResult.State.ACTIVE,
            20000);

        /*
         * PUT a row that exceeds cloud key and value limits.
         */
        MapValue value = new MapValue().put("id", makeString(500))
            .put("idx", makeString(300)).put("name", makeString(600000));

        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult putResult = kvhandle.put(putRequest);
        assertNotNull(putResult.getVersion());
    }

    /*
     * Test table limits:
     *  # of indexes (cloud defaults to 5)
     */
    @Test
    public void testTableLimits()
        throws Exception {

        final String createTableStatement =
            "create table if not exists limits(" +
            "id integer, i0 integer, i1 integer, i2 integer, i3 integer, " +
            "i4 integer, i5 integer, i6 integer, primary key(id))";

        tableOperation(kvhandle, createTableStatement, null,
                       TableResult.State.ACTIVE, 20000);

        for (int i = 0; i < 7; i++) {
            String addIndex = "create index idx" + i + " on limits(i" +
                i + ")";

            tableOperation(kvhandle, addIndex, null,
                TableResult.State.ACTIVE, 20000);
        }
    }

    @Test
    public void testInvalidToken()
        throws Exception {

        final String tableName = "test";
        final String createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(id INTEGER, " +
            " pin INTEGER, " +
            " name STRING, " +
            " PRIMARY KEY(SHARD(pin), id))";
        TableRequest tableRequest = new TableRequest()
            .setStatement(createTableStatement);
        TableResult tres = kvhandle.tableRequest(tableRequest);
        tres.waitForCompletion(kvhandle, 60000, 1000);

        StoreAccessTokenProvider authProvider =
            new MockProvider("test", "NoSql00__123456".toCharArray());
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        config.setAuthorizationProvider(authProvider);
        NoSQLHandle testHandle = NoSQLHandleFactory.createNoSQLHandle(config);
        MapValue value =
            new MapValue().put("id", 1).put("pin", "654321").
            put("name", "test1");
        try {
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName("test");
            testHandle.put(putRequest);
        } catch (InvalidAuthorizationException e) {
            /* Expect sec IAE here */
            return;
        } finally {
            testHandle.close();
        }
        fail("Should not reach here");
    }

    private class MockProvider extends StoreAccessTokenProvider {
        public MockProvider(String userName, char[] password) {
            super(userName, password);
        }

        @Override
        public String getAuthorizationString(Request request) {
            return "Bearer InvalidToken@#!";
        }
    }

    @Test
    public void testChildTables()
        throws Exception {

        String tableName = "parent";
        String createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(id INTEGER, " +
            " pin INTEGER, " +
            " name STRING, " +
            " PRIMARY KEY(SHARD(pin), id))";

        TableRequest tableRequest = new TableRequest()
            .setStatement(createTableStatement);
        TableResult tres = kvhandle.tableRequest(tableRequest);
        tres.waitForCompletion(kvhandle, 60000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.ACTIVE);

        tableName = "parent.child";
        createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(childId INTEGER, " +
            " childName STRING, " +
            " PRIMARY KEY(childId))";

        tableRequest = new TableRequest()
            .setStatement(createTableStatement);
        tres = kvhandle.tableRequest(tableRequest);
        tres.waitForCompletion(kvhandle, 60000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.ACTIVE);

        ListTablesRequest listTables = new ListTablesRequest();
        kvhandle.listTables(listTables);

        /*
         * PUT a row
         */
        MapValue value = new MapValue().put("id", 1).
            put("pin", "654321").put("name", "test1").
            put("childId", 1).put("childName", "cName");

        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult putResult = kvhandle.put(putRequest);
        assertNotNull(putResult.getVersion());

        /*
         * GET the row
         */
        MapValue key = new MapValue().put("id", 1).put("pin", "654321").
            put("childId", 1);

        GetRequest getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        GetResult getRes = kvhandle.get(getRequest);
        assertEquals("cName",
                     getRes.getValue().get("childName").
                     asString().getValue());

        /*
         * PUT a second row using JSON
         */
        String jsonString =
            "{\"id\": 2, \"pin\": 123456, \"name\":\"test2\"," +
            "\"childId\": 2, \"childName\":\"cName2\"}";

        putRequest = new PutRequest()
            .setValueFromJson(jsonString, null)
            .setTableName(tableName);
        putResult = kvhandle.put(putRequest);
        assertNotNull(putResult.getVersion());

        /*
         * GET the second row
         */
        key = new MapValue().put("id", 2).put("pin", "123456").
            put("childId", 2);
        getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        assertEquals(
            "cName2",
            kvhandle.get(getRequest).getValue().get("childName").
            asString().getValue());

        /*
         * QUERY the table. The table name is inferred from the
         * query statement.
         */
        try {
            QueryRequest queryRequest = new QueryRequest().
                setStatement("SELECT * from " + tableName +
                             " WHERE childName= \"cName2\"");
            QueryResult qres = kvhandle.query(queryRequest);
            List<MapValue> results = qres.getResults();
            assertEquals(results.size(), 1);
            assertEquals(2, results.get(0).get("id").asInteger().getValue());
        } catch (RequestTimeoutException rte) {
            if (KVVersion.CURRENT_VERSION.getMajor() >= 20 ||
                !(rte.getCause() instanceof SystemException)) {
                throw rte;
            }
            /* ignore this exception for 19 for now; known bug */
        }

        /*
         * Put in the third row, the pin/name/childName field is the same
         * as the second row
         */
        jsonString = "{\"id\": 3, \"pin\": 123456, \"name\":\"test2\"," +
            "\"childId\": 3, \"childName\":\"cName2\"}";

        putRequest = new PutRequest()
            .setValueFromJson(jsonString, null) // no options
            .setTableName(tableName);
        putResult = kvhandle.put(putRequest);
        assertNotNull(putResult.getVersion());

        /*
         * Create index, test query by indexed field
         */
        final String createIndexStatement =
            "CREATE INDEX IF NOT EXISTS idx1 ON " + tableName +
            " (childName)";

        tableRequest =
            new TableRequest().setStatement(createIndexStatement);
        tres  = kvhandle.tableRequest(tableRequest);
        tres.waitForCompletion(kvhandle, 60000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.ACTIVE);

        QueryRequest queryRequest = new QueryRequest().
            setStatement("SELECT * from " + tableName +
                         " WHERE childName= \"cName2\"");
        QueryResult qres = kvhandle.query(queryRequest);
        List<MapValue> results = qres.getResults();
        assertEquals(results.size(), 2);
        assertEquals("cName2",
                     results.get(0).get("childName").asString().getValue());

        /*
         * DELETE the first row
         */
        key = new MapValue().put("id", 1).put("pin", "654321").
            put("childId", 1);
        DeleteRequest delRequest = new DeleteRequest()
            .setKey(key)
            .setTableName(tableName);
        DeleteResult delResult = kvhandle.delete(delRequest);
        assertTrue(delResult.getSuccess());

        /*
         * MultiDelete where name is test2
         */
        key = new MapValue().put("pin", "123456");
        MultiDeleteRequest multiDelRequest = new MultiDeleteRequest()
            .setKey(key)
            .setTableName(tableName);

        MultiDeleteResult mRes = kvhandle.multiDelete(multiDelRequest);
        assertEquals(mRes.getNumDeletions(), 2);

        /*
         * There should be no record in the table now
         */
        queryRequest = new QueryRequest().
            setStatement("SELECT * from " + tableName);
        qres = kvhandle.query(queryRequest);
        results = qres.getResults();
        assertEquals(results.size(), 0);

        /*
         * DROP the child table
         */
        tableRequest = new TableRequest()
            .setStatement("DROP TABLE IF EXISTS " + tableName);
        tres = kvhandle.tableRequest(tableRequest);
        tres.waitForCompletion(kvhandle, 20000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.DROPPED);
        /*
         * DROP the parent table
         */
        tableName = "parent";
        tableRequest = new TableRequest()
            .setStatement("DROP TABLE IF EXISTS " + tableName);
        tres = kvhandle.tableRequest(tableRequest);
        tres.waitForCompletion(kvhandle, 20000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.DROPPED);
    }

    @Test
    public void testSecureSysOp()
        throws Exception {

        SystemResult dres = doSysOp(kvhandle, "create namespace myns");

        /* create a user -- use a password with white space and quotes */
        dres = doSysOp(kvhandle,
                       "create user newuser " +
                       "identified by 'ChrisToph \"_12&%'");

        dres = doSysOp(kvhandle, "show namespaces");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(kvhandle, "show users");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(kvhandle, "show as json roles");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(kvhandle, "show as json user admin");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        String[] roles = kvhandle.listRoles();
        /*
         * The number of default roles may vary with the kv release.
         * Don't assume a specific number. This range is safe for now.
         */
        assertTrue(roles.length > 2 && roles.length < 10);
    }

    @Test
    public void testSystemExceptions()
        throws Exception {

        if (cloudRunning) {
            return;
        }
        final String createTable = "Create table " +
            "foo(id integer, name string, primary key(id))";
        final String createIndex = "create index idx on foo(name)";

        doSysOp(kvhandle, "create namespace myns");

        doSysOp(kvhandle, createTable);
        doSysOp(kvhandle, createIndex);

        doSysOp(kvhandle,
                "create user newuser identified by 'ChrisToph \"_12&%'");
        /*
         * test error conditions
         */
        try {
            doSysOp(kvhandle, "drop namespace not_a_namespace");
            fail("operation should have failed");
        } catch (ResourceNotFoundException e) {
            /* success */
        }
        try {
            doSysOp(kvhandle, "show as json user not_a_user");
            fail("operation should have failed");
        } catch (ResourceNotFoundException e) {
            /* success */
        }
        try {
            doSysOp(kvhandle, "show as json role not_a_role");
            fail("operation should have failed");
        } catch (ResourceNotFoundException e) {
            /* success */
        }

        try {
            doSysOp(kvhandle,
                    "create user newuser identified by 'Chrioph \"_12&%'");
            fail("operation should have failed");
        } catch (ResourceExistsException e) {
            /* success */
        }

        try {
            doSysOp(kvhandle, "create namespace myns");
            fail("operation should have failed");
        } catch (ResourceExistsException e) {
            /* success */
        }

        try {
            doSysOp(kvhandle, "drop table not_a_table");
            fail("operation should have failed");
        } catch (TableNotFoundException e) {
            /* success */
        }

        try {
            doSysOp(kvhandle, "drop index no_index on foo");
            fail("operation should have failed");
        } catch (IndexNotFoundException e) {
            /* success */
        }

        try {
            doSysOp(kvhandle, createTable);
            fail("operation should have failed");
        } catch (TableExistsException e) {
            /* success */
        }

        try {
            doSysOp(kvhandle, createIndex);
        } catch (IndexExistsException e) {
            /* success */
        }
    }

    /*
     * Avoid security-related operations
     */
    @Test
    public void testSystem()
        throws Exception {

        SystemResult dres = doSysOp(kvhandle, "create namespace myns");

        dres = doSysOp(kvhandle, "show namespaces");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(kvhandle, "show users");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(kvhandle, "show as json user admin");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(kvhandle, "show as json roles");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        /*
         * Create a table using this mechanism.
         */
        dres = doSysOp(kvhandle,
                       "create table foo(id integer, primary key(id))");

        dres = doSysOp(kvhandle, "show as json tables");
        assertNotNull(dres.getResultString());
    }

    @Test
    public void testNamespaces()
        throws Exception {

        final String parentName = "myns:parent";
        final String childName = "myns:parent.child";
        final int numParent = 30;
        final int numChild = 40;

        doSysOp(kvhandle, "create namespace myns");

        /* parent in myns */
        TableRequest treq = new TableRequest().setStatement(
            "create table myns:parent(id integer, primary key(id))");
        TableResult tres = kvhandle.tableRequest(treq);
        tres.waitForCompletion(kvhandle, 100000, 1000);

        /* child in myns */
        treq = new TableRequest().setStatement(
            "create table myns:parent.child(cid integer, name string, " +
            "primary key(cid))");
        tres = kvhandle.tableRequest(treq);
        tres.waitForCompletion(kvhandle, 100000, 1000);

        /* put data in both tables */
        PutRequest preq = new PutRequest();
        MapValue value = new MapValue();
        for (int i = 0; i < numParent; i++) {
            value.put("name", "myname"); // ignored in parent
            value.put("id", i);
            preq.setTableName(parentName).setValue(value);
            PutResult pres = kvhandle.put(preq);
            assertNotNull("Parent put failed", pres.getVersion());
            for (int j = 0; j < numChild; j++) {
                value.put("cid", j); // ignored in parent
                preq.setTableName(childName).setValue(value);
                pres = kvhandle.put(preq);
                assertNotNull("Child put failed", pres.getVersion());
                assertNoUnits(pres);
            }
        }

        /* get parent */
        GetRequest getReq = new GetRequest().setTableName(parentName)
            .setKey(new MapValue().put("id", 1));
        GetResult getRes = kvhandle.get(getReq);
        assertNotNull(getRes.getValue());

        /* get child */
        getReq = new GetRequest().setTableName(childName)
            .setKey(new MapValue().put("id", 1).put("cid", 1));
        getRes = kvhandle.get(getReq);
        assertNotNull(getRes.getValue());
        assertNoUnits(getRes);

        try {
            /* query parent */
            String query = "select * from " + parentName;
            List<MapValue> res = doQuery(kvhandle, query);
            assertEquals(numParent, res.size());

            /* query child */
            query = "select * from " + childName;
            res = doQuery(kvhandle, query);
            assertEquals(numParent * numChild, res.size());

            /* prepared query on child */
            res = doPreparedQuery(kvhandle, query);
            assertEquals(numParent * numChild, res.size());
        } catch (RequestTimeoutException rte) {
            if (KVVersion.CURRENT_VERSION.getMajor() >= 20 ||
                !(rte.getCause() instanceof SystemException)) {
                throw rte;
            }
            /* ignore this exception for 19 for now; known bug */
        }

        /* test ListTables with namespace */
        ListTablesRequest listTables =
            new ListTablesRequest().setNamespace("myns");
        ListTablesResult lres = kvhandle.listTables(listTables);
        assertEquals(2, lres.getTables().length);
    }

    @Test
    public void testDefaultNamespacesImpl()
        throws Exception {

        /* skip this test if using older SDKs */
        Class<?> handleImplClass = null;
        try {
            handleImplClass = Class.forName(
                                  "oracle.nosql.driver.http.NoSQLHandleImpl");
        } catch (Throwable e) {
            System.out.println("Could not find NoSQLHandleImpl class:" + e);
            handleImplClass = null;
        }
        assertNotNull(handleImplClass);
        Method setDefaultNamespaceFunction = null;
        try {
            setDefaultNamespaceFunction = handleImplClass.getMethod(
                                    "setDefaultNamespace", String.class);
            verbose("Using NoSQLHandleImpl.setDefaultNamespace()");
        } catch (Throwable e) {
            verbose("Could not find NoSQLHandleImpl.setDefaultNamespace(): " +
                    "Skipping test");
            setDefaultNamespaceFunction = null;
        }
        assumeTrue(setDefaultNamespaceFunction != null);


        final String parentName = "parent";
        final String childName = "parent.child";
        final String nsParentName = "myns:parent";
        final String nsChildName = "myns:parent.child";
        final int numParent = 30;
        final int numChild = 40;

        doSysOp(kvhandle, "create namespace myns");

        /* ((NoSQLHandleImpl)kvhandle).setDefaultNamespace("myns"); */
        setDefaultNamespaceFunction.invoke((NoSQLHandleImpl)kvhandle, "myns");

        /* parent in myns */
        TableRequest treq = new TableRequest().setStatement(
            "create table parent(id integer, primary key(id))");
        TableResult tres = kvhandle.tableRequest(treq);
        tres.waitForCompletion(kvhandle, 100000, 1000);

        /* child in myns */
        treq = new TableRequest().setStatement(
            "create table parent.child(cid integer, name string, " +
            "primary key(cid))");
        tres = kvhandle.tableRequest(treq);
        tres.waitForCompletion(kvhandle, 100000, 1000);

        ListTablesRequest listTables;
        ListTablesResult lres;

        /* test ListTables with no namespace: should get just myns tables */
        listTables = new ListTablesRequest();
        lres = kvhandle.listTables(listTables);
        assertEquals(2, lres.getTables().length);

        /* test ListTables with explicit namespace */
        listTables = new ListTablesRequest().setNamespace("myns");
        lres = kvhandle.listTables(listTables);
        assertEquals(2, lres.getTables().length);

        /* test ListTables with explicit invalid */
        listTables = new ListTablesRequest().setNamespace("invalid");
        lres = kvhandle.listTables(listTables);
        assertEquals(0, lres.getTables().length);

        /* test that dropping table works correctly */
        treq = new TableRequest().setStatement("drop table parent.child");
        tres = kvhandle.tableRequest(treq);
        tres.waitForCompletion(kvhandle, 100000, 1000);

        /* test that ns:tablename overrides invalid default ns in DDL */
        /* ((NoSQLHandleImpl)kvhandle).setDefaultNamespace("invalid"); */
        setDefaultNamespaceFunction.invoke(
                                    (NoSQLHandleImpl)kvhandle, "invalid");
        treq = new TableRequest().setStatement(
            "create table myns:parent.child(cid integer, name string, " +
            "primary key(cid))");
        tres = kvhandle.tableRequest(treq);
        tres.waitForCompletion(kvhandle, 100000, 1000);

        /* test ListTables with explicit namespace */
        listTables = new ListTablesRequest().setNamespace("myns");
        lres = kvhandle.listTables(listTables);
        assertEquals(2, lres.getTables().length);

        /* test ListTables with default invalid namespace */
        listTables = new ListTablesRequest();
        lres = kvhandle.listTables(listTables);
        assertEquals(0, lres.getTables().length);

        /* reset default namespace to valid namespace */
        /* ((NoSQLHandleImpl)kvhandle).setDefaultNamespace("myns"); */
        setDefaultNamespaceFunction.invoke(
                                    (NoSQLHandleImpl)kvhandle, "myns");

        /* put data in both tables */
        PutRequest preq = new PutRequest();
        MapValue value = new MapValue();
        for (int i = 0; i < numParent; i++) {
            value.put("name", "myname"); // ignored in parent
            value.put("id", i);
            preq.setTableName(parentName).setValue(value);
            PutResult pres = kvhandle.put(preq);
            assertNotNull("Parent put failed", pres.getVersion());
            for (int j = 0; j < numChild; j++) {
                value.put("cid", j); // ignored in parent
                preq.setTableName(childName).setValue(value);
                pres = kvhandle.put(preq);
                assertNotNull("Child put failed", pres.getVersion());
                assertNoUnits(pres);
            }
        }

        /* get parent */
        GetRequest getReq = new GetRequest().setTableName(parentName)
            .setKey(new MapValue().put("id", 1));
        GetResult getRes = kvhandle.get(getReq);
        assertNotNull(getRes.getValue());

        /* get child */
        getReq = new GetRequest().setTableName(childName)
            .setKey(new MapValue().put("id", 1).put("cid", 1));
        getRes = kvhandle.get(getReq);
        assertNotNull(getRes.getValue());
        assertNoUnits(getRes);

        try {
            /* query parent */
            String query = "select * from " + parentName;
            List<MapValue> res = doQuery(kvhandle, query);
            assertEquals(numParent, res.size());

            /* query child */
            query = "select * from " + childName;
            res = doQuery(kvhandle, query);
            assertEquals(numParent * numChild, res.size());

            /* prepared query on child */
            res = doPreparedQuery(kvhandle, query);
            assertEquals(numParent * numChild, res.size());

            /* query parent with explicit namespace */
            query = "select * from " + nsParentName;
            res = doQuery(kvhandle, query);
            assertEquals(numParent, res.size());

            /* query child with explicit namespace */
            query = "select * from " + nsChildName;
            res = doQuery(kvhandle, query);
            assertEquals(numParent * numChild, res.size());

            /* set an invalid default namespace, check all again */
            /* ((NoSQLHandleImpl)kvhandle).setDefaultNamespace("invalid"); */
            setDefaultNamespaceFunction.invoke(
                                        (NoSQLHandleImpl)kvhandle, "invalid");

            /* query parent with explicit namespace: should work */
            query = "select * from " + nsParentName;
            res = doQuery(kvhandle, query);
            assertEquals(numParent, res.size());

            /* query child with explicit namespace: should work */
            query = "select * from " + nsChildName;
            res = doQuery(kvhandle, query);
            assertEquals(numParent * numChild, res.size());

            /* prepared query on child with explicit namespace: should work */
            res = doPreparedQuery(kvhandle, query);
            assertEquals(numParent * numChild, res.size());

            /* query parent with default invalid namespace: should fail */
            query = "select * from " + parentName;
            try {
                res = doQuery(kvhandle, query);
                fail("Expected TableNotFoundException");
            } catch (TableNotFoundException tne) {
                /* expected */
            }

            /* query child with default invalid namespace: should fail */
            query = "select * from " + childName;
            try {
                res = doQuery(kvhandle, query);
                fail("Expected TableNotFoundException");
            } catch (TableNotFoundException tne) {
                /* expected */
            }

            /* prepared query with default invalid namespace: should fail */
            try {
                res = doPreparedQuery(kvhandle, query);
                fail("Expected TableNotFoundException");
            } catch (TableNotFoundException tne) {
                /* expected */
            }

            /* verify exception for DDL with invalid default namespace */
            try {
                treq = new TableRequest().setStatement(
                    "alter table parent.child using TTL 5 DAYS");
                tres = kvhandle.tableRequest(treq);
                tres.waitForCompletion(kvhandle, 100000, 1000);
                fail("Expected TableNotFoundException");
            } catch (TableNotFoundException tne) {
                /* expected */
            }

        } catch (RequestTimeoutException rte) {
            if (KVVersion.CURRENT_VERSION.getMajor() >= 20 ||
                !(rte.getCause() instanceof SystemException)) {
                throw rte;
            }
            /* ignore this exception for 19 for now; known bug */
        }

        /* drop namespace - use cascade to remove tables */
        doSysOp(kvhandle, "drop namespace myns cascade");

        /* verify that setting the namespace back to null works */
        /* ((NoSQLHandleImpl)kvhandle).setDefaultNamespace(null); */
        setDefaultNamespaceFunction.invoke((NoSQLHandleImpl)kvhandle,
                                           (String)null);
        testNamespaces();
    }

    @Test
    public void testDefaultNamespacesConfig()
        throws Exception {

        /* skip this test if using older SDKs */
        Class<?> handleConfigClass = null;
        try {
            handleConfigClass =
                Class.forName("oracle.nosql.driver.NoSQLHandleConfig");
        } catch (Throwable e) {
            System.out.println("Could not find NoSQLHandleConfig class:" + e);
            handleConfigClass = null;
        }
        assertNotNull(handleConfigClass);
        Method setDefaultNamespaceFunction = null;
        try {
            setDefaultNamespaceFunction = handleConfigClass.getMethod(
                                    "setDefaultNamespace", String.class);
        } catch (Throwable e) {
            verbose("Could not find NoSQLHandleConfig.setDefaultNamespace(): " +
                    "Skipping test");
            setDefaultNamespaceFunction = null;
        }
        assumeTrue(setDefaultNamespaceFunction != null);

        final String parentName = "parent";
        final String childName = "parent.child";
        final String nsParentName = "myns:parent";
        final String nsChildName = "myns:parent.child";
        final int numParent = 30;
        final int numChild = 40;

        /*
         * This test is the same as testDefaultnamespacesImpl except it
         * depends on the (currently hidden) NoSQLHandleConfig setting.
         * So it must create its own handle.
         */
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        config.setAuthorizationProvider(authProvider);

        /* config.setDefaultNamespace("myns"); */
        setDefaultNamespaceFunction.invoke(config, "myns");

        NoSQLHandle myhandle = NoSQLHandleFactory.createNoSQLHandle(config);

        doSysOp(myhandle, "create namespace myns");

        /* parent in myns */
        TableRequest treq = new TableRequest().setStatement(
            "create table parent(id integer, primary key(id))");
        TableResult tres = myhandle.tableRequest(treq);
        tres.waitForCompletion(myhandle, 100000, 1000);

        /* child in myns */
        treq = new TableRequest().setStatement(
            "create table parent.child(cid integer, name string, " +
            "primary key(cid))");
        tres = myhandle.tableRequest(treq);
        tres.waitForCompletion(myhandle, 100000, 1000);

        ListTablesRequest listTables;
        ListTablesResult lres;

        /* test ListTables with no namespace: should get just myns tables */
        listTables = new ListTablesRequest();
        lres = myhandle.listTables(listTables);
        assertEquals(2, lres.getTables().length);

        /* test ListTables with explicit namespace */
        listTables = new ListTablesRequest().setNamespace("myns");
        lres = myhandle.listTables(listTables);
        assertEquals(2, lres.getTables().length);

        /* test ListTables with explicit invalid */
        listTables = new ListTablesRequest().setNamespace("invalid");
        lres = myhandle.listTables(listTables);
        assertEquals(0, lres.getTables().length);

        /* put data in both tables */
        PutRequest preq = new PutRequest();
        MapValue value = new MapValue();
        for (int i = 0; i < numParent; i++) {
            value.put("name", "myname"); // ignored in parent
            value.put("id", i);
            preq.setTableName(parentName).setValue(value);
            PutResult pres = myhandle.put(preq);
            assertNotNull("Parent put failed", pres.getVersion());
            for (int j = 0; j < numChild; j++) {
                value.put("cid", j); // ignored in parent
                preq.setTableName(childName).setValue(value);
                pres = myhandle.put(preq);
                assertNotNull("Child put failed", pres.getVersion());
                assertNoUnits(pres);
            }
        }

        /* get parent */
        GetRequest getReq = new GetRequest().setTableName(parentName)
            .setKey(new MapValue().put("id", 1));
        GetResult getRes = myhandle.get(getReq);
        assertNotNull(getRes.getValue());

        /* get child */
        getReq = new GetRequest().setTableName(childName)
            .setKey(new MapValue().put("id", 1).put("cid", 1));
        getRes = myhandle.get(getReq);
        assertNotNull(getRes.getValue());
        assertNoUnits(getRes);

        try {
            /* query parent */
            String query = "select * from " + parentName;
            List<MapValue> res = doQuery(myhandle, query);
            assertEquals(numParent, res.size());

            /* query child */
            query = "select * from " + childName;
            res = doQuery(myhandle, query);
            assertEquals(numParent * numChild, res.size());

            /* prepared query on child */
            res = doPreparedQuery(myhandle, query);
            assertEquals(numParent * numChild, res.size());

            /* query parent with explicit namespace */
            query = "select * from " + nsParentName;
            res = doQuery(myhandle, query);
            assertEquals(numParent, res.size());

            /* query child with explicit namespace */
            query = "select * from " + nsChildName;
            res = doQuery(myhandle, query);
            assertEquals(numParent * numChild, res.size());

        } catch (RequestTimeoutException rte) {
            if (KVVersion.CURRENT_VERSION.getMajor() >= 20 ||
                !(rte.getCause() instanceof SystemException)) {
                throw rte;
            }
            /* ignore this exception for 19 for now; known bug */
        }

        /* drop namespace - use cascade to remove tables */
        doSysOp(myhandle, "drop namespace myns cascade");
    }

    /**
     * Test that the limit on # of operations in WriteMultiple isn't enforced
     */
    @Test
    public void writeMultipleTest() {

        doSysOp(kvhandle, "create table foo(id1 string, " +
                "id2 integer, primary key(shard(id1), id2))");

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        for (int i = 0; i < BATCH_OP_NUMBER_LIMIT +1; i++) {
            PutRequest pr = new PutRequest().setTableName("foo").
                setValueFromJson("{\"id1\":\"a\", \"id2\":" + i + "}", null);
            wmReq.add(pr, false);
        }
        try {
            WriteMultipleResult wmRes = kvhandle.writeMultiple(wmReq);
            assertEquals(BATCH_OP_NUMBER_LIMIT + 1, wmRes.size());
        } catch (BatchOperationNumberLimitException ex) {
            fail("operation should have succeeded");
        }
    }

    /**
     * Tests synchronization of table metdata when mixing driver-based table
     * operations and direct kv-based table changes made using the sql shell
     * or admin CLI, or even a KVStore handle directly -- any operation that
     * bypasses the proxy and any caching it might do.
     */
    @Test
    public void tableSync() throws Exception {
        /*
         * Case 1:
         *   create table
         *   drop it via KV
         *   create again using if exists
         *   try to put data
         */
        final String table1 =
            "create table if not exists mytable(id integer, primary key(id))";
        doTableRequest(kvhandle, table1, false);
        dropTableUsingAdmin("mytable");
        doTableRequest(kvhandle, table1, false);

        doPut(kvhandle, "mytable", "{\"id\": 1}");
        doTableRequest(kvhandle, "drop table mytable", true);
    }

    @Test
    public void testUnsupportedOps() throws Exception {

        doSysOp(kvhandle, "create table foo(id integer, primary key(id))");
        try {
            TableUsageRequest req =
                new TableUsageRequest().setTableName("foo");
            kvhandle.getTableUsage(req);
            fail("op should have failed");
        } catch (OperationNotSupportedException e) {
            /* success */
        }
    }

    /*
     * Asserts that a not secure provider cannot access a secure proxy
     */
    @Test
    public void testNonSecureAccess()
        throws Exception {

        if (cloudRunning) {
            return;
        }
        NoSQLHandle testHandle = null;
        StoreAccessTokenProvider sap = new StoreAccessTokenProvider();
        try {
            NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
            config.setAuthorizationProvider(sap);
            sap.setAutoRenew(false);
            testHandle = NoSQLHandleFactory.createNoSQLHandle(config);
            final String tableName = "nonsecure";
            final String createTableStatement =
                "CREATE TABLE IF NOT EXISTS " + tableName +
                "(id INTEGER, " +
                " pin INTEGER, " +
                " name STRING, " +
                " PRIMARY KEY(SHARD(pin), id))";

            TableRequest tableRequest = new TableRequest()
                .setStatement(createTableStatement);
            TableResult tres = testHandle.tableRequest(tableRequest);
            tres.waitForCompletion(testHandle, 60000, 1000);
            assertEquals(tres.getTableState(), TableResult.State.ACTIVE);
        } catch (IllegalArgumentException iae) {
            /* Expected */
            if (iae.getMessage().contains("Illegal Argument: " +
                "Missing authentication information") == false) {
                fail("Expected <Illegal Argument: " +
                        "Missing authentication information>, but got <" +
                        iae.getMessage() + ">");
            }
            return;
        } finally {
            sap.close();
            if (testHandle != null) {
                testHandle.close();
            }
        }
        fail("Should not reach here");
    }

    @Test
    public void testTokenTimeout()
        throws Exception {

        /* skip until kv auth issue in async path is fixed */
        assumeTrue(false);

        StoreAccessTokenProvider sap =
            new StoreAccessTokenProvider("test",
                                         "NoSql00__123456".toCharArray());
        NoSQLHandle testHandle = null;

        try {
            /* Set the expiration time to 3 seconds */
            final ParameterMap map = new ParameterMap();
            map.setParameter(ParameterState.GP_SESSION_TIMEOUT,
                             "3 SECONDS");
            map.setParameter(ParameterState.GP_LOGIN_CACHE_TIMEOUT,
                             "3 SECONDS");
            int planId =
                admin.createChangeGlobalSecurityParamsPlan("change timeout",
                                                           map);
            execPlan(planId);

            /* Disable auto renew */
            sap.setAutoRenew(false);

            NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
            config.setAuthorizationProvider(sap);
            testHandle = NoSQLHandleFactory.createNoSQLHandle(config);

            /* Get a login token then wait for expiration */
            sap.getAuthorizationString(null);

            /* Wait for the login token to expire */
            Thread.sleep(5000);
            final String tableName = "timeout";
            final String createTableStatement =
                "CREATE TABLE IF NOT EXISTS " + tableName +
                "(id INTEGER, " +
                " pin INTEGER, " +
                " name STRING, " +
                " PRIMARY KEY(SHARD(pin), id))";
            TableRequest tableRequest = new TableRequest()
                .setStatement(createTableStatement);
            TableResult tres = testHandle.tableRequest(tableRequest);

            /* Wait for expiration before getTable */
            Thread.sleep(5000);
            tres.waitForCompletion(testHandle, 60000, 1000);
            assertEquals(tres.getTableState(), TableResult.State.ACTIVE);

            /* Wait for expiration before put request */
            Thread.sleep(5000);
            MapValue value = new MapValue().put("id", 1).
                put("pin", "654321").put("name", "test1");
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tableName);
            PutResult putResult = testHandle.put(putRequest);
            assertNotNull(putResult.getVersion());

            /* Wait for expiration before get request */
            Thread.sleep(5000);
            MapValue key = new MapValue().put("id", 1).put("pin", "654321");
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName(tableName);
            GetResult getRes = testHandle.get(getRequest);
            Thread.sleep(5000);
            assertEquals("test1",
                         getRes.getValue().get("name").asString().getValue());

            /* Wait for expiration before query request */
            Thread.sleep(5000);
            try {
                QueryRequest queryRequest = new QueryRequest().
                    setStatement("SELECT * from " + tableName);
                QueryResult qres = testHandle.query(queryRequest);
                List<MapValue> results = qres.getResults();
                assertEquals(results.size(), 1);
            } catch (RequestTimeoutException rte) {
                if (KVVersion.CURRENT_VERSION.getMajor() >= 20 ||
                    !(rte.getCause() instanceof SystemException)) {
                    throw rte;
                }
                /* ignore this exception for 19 for now; known bug */
            }

            /* Wait for expiration before create index request */
            Thread.sleep(5000);
            final String createIndexStatement =
                "CREATE INDEX IF NOT EXISTS idx1 ON " + tableName +
                " (name)";
            tableRequest =
                new TableRequest().setStatement(createIndexStatement);
            testHandle.tableRequest(tableRequest);

            /* Wait for expiration before getTable */
            Thread.sleep(5000);
            tres.waitForCompletion(testHandle, 60000, 1000);
            assertEquals(tres.getTableState(), TableResult.State.ACTIVE);

            /* Wait for expiration before multi delete request */
            Thread.sleep(5000);
            key = new MapValue().put("pin", "654321");
            MultiDeleteRequest multiDelRequest = new MultiDeleteRequest()
                .setKey(key)
                .setTableName(tableName);

            MultiDeleteResult mRes = testHandle.multiDelete(multiDelRequest);
            assertEquals(mRes.getNumDeletions(), 1);
        } finally {
            /* Set the expiration time back to default */
            final ParameterMap map = new ParameterMap();
            map.setParameter(ParameterState.GP_SESSION_TIMEOUT,
                             ParameterState.GP_SESSION_TIMEOUT_DEFAULT);
            map.setParameter(ParameterState.GP_LOGIN_CACHE_TIMEOUT,
                             ParameterState.GP_LOGIN_CACHE_TIMEOUT_DEFAULT);
            int planId =
                admin.createChangeGlobalSecurityParamsPlan("change back",
                                                           map);
            execPlan(planId);

            sap.close();
            if (testHandle != null) {
                testHandle.close();
            }
        }
    }

    @Test
    public void testLargeRow() {
        doLargeRow(kvhandle, true);
    }

    /*
     * Ensure that MR table DDL statements pass through the proxy
     * properly
     */
    @Test
    public void testMultiRegionBasic() {
        final String show = "show regions";
        final String createRegion = "create region remoteRegion";
        final String setRegion = "set local region localRegion";
        final String createTable = "create table mrtable(id integer, " +
            "primary key(id)) in regions localRegion";

        /*
         * doSysOp will throw on any failures; there is no "error" return
         * information in SystemResult.
         */
        SystemResult res = doSysOp(kvhandle, createRegion);
        res = doSysOp(kvhandle, setRegion);
        res = doSysOp(kvhandle, show);
        String resString = res.getResultString();
        assertTrue(resString.contains("localRegion") &&
                   resString.contains("remoteRegion"));

        /* count sys tables first */
        ListTablesRequest listTables = new ListTablesRequest();
        ListTablesResult lres = kvhandle.listTables(listTables);
        int numSysTables = lres.getTables().length;

        res = doSysOp(kvhandle, createTable);

        lres = kvhandle.listTables(listTables);
        assertEquals(numSysTables + 1, lres.getTables().length);

        /* this will throw if the table doesn't exist */
        getTable("mrtable", kvhandle);

        /* test a simple put */
        PutRequest putRequest = new PutRequest()
            .setValue(new MapValue().put("id", 1))
            .setTableName("mrtable");
        PutResult putResult = kvhandle.put(putRequest);
        assertNotNull(putResult.getVersion());
    }

    @Test
    public void testQueryWithSmallLimit() {
        final int recordKB = 2;
        final int minRead = getMinRead();

        String ddl = "create table smallLimitTest(id integer, " +
                        "longString string, primary key(id))";
        doTableRequest(kvhandle, ddl, false);

        /* Load 2 rows to table */
        PutRequest putReq = new PutRequest().setTableName("smallLimitTest");
        PutResult putRet;
        MapValue row;
        for (int i = 0; i < 2; i++) {
            row = new MapValue()
                    .put("id", i)
                    .put("longString", genString((recordKB - 1) * 1024));
            putReq.setValue(row);

            putRet = kvhandle.put(putReq);
            assertNotNull(putRet.getVersion());
        }

        String query;
        QueryRequest req;
        QueryResult ret;

        /* Query with maxReadKB of 1, expect an IAE */
        query = "select * from smallLimitTest";
        req = new QueryRequest().setStatement(query).setMaxReadKB(1);
        int numExec = 0;
        if (checkKVVersion(21, 3, 6)) {
            /* Query should always make progress with small limit */
            int cnt = 0;
            do {
                numExec++;
                ret = kvhandle.query(req);
                cnt += ret.getResults().size();
            } while (!req.isDone());
            assertEquals(2, cnt);
        } else {
            try {
                do {
                    numExec++;
                    ret = kvhandle.query(req);
                } while (!req.isDone());
                fail("Expect to catch IAE but not");
            } catch (IllegalArgumentException iae) {
                assertEquals(2, numExec);
            }
        }

        /* Update with number-based limit of 1 */
        int newRecordKB = 1;
        String longString = genString((newRecordKB - 1) * 1024);
        query = "update smallLimitTest set longString = \"" + longString +
                "\" where id = 0";
        req = new QueryRequest().setStatement(query).setLimit(1);
        ret = kvhandle.query(req);
        assertNull(ret.getContinuationKey());

        /* Update with maxReadKB of 1, expect an IAE */
        int expReadKB = dontDoubleChargeKey() ? recordKB : minRead + recordKB;
        query = "update smallLimitTest set longString = \"" + longString +
                "\" where id = 1";
        if (checkKVVersion(21, 3, 6)) {
            /* Query should always make progress with small limit */
            req = new QueryRequest().setStatement(query).setMaxReadKB(1);
            ret = kvhandle.query(req);
            assertEquals(1, ret.getResults().size());
        } else {
            for (int kb = 1; kb <= expReadKB; kb++) {
                req = new QueryRequest().setStatement(query).setMaxReadKB(kb);
                try {
                    ret = kvhandle.query(req);
                    if (kb < expReadKB) {
                        fail("Expect to catch IAE but not");
                    }
                } catch (IllegalArgumentException iae) {
                    assertTrue("Expect to succeed with maxReadKB of " + kb +
                               ", but fail: " + iae.getMessage(),
                               kb < expReadKB);
                }
            }
        }
    }

    /**
     * Tests to ensure CRDT works properly on proxy:
     * 1. Test inserting a row with a CRDT column and also updating
     * the CRDT using UPDATE sql.
     * 2. Use the direct driver to mimic the behaivor of an mrtable agent
     * to put a remote row with CRDT from other regions. Test reading
     * the CRDT using the driver.
     */
    @Test
    public void testCRDT() {
        TableAPIImpl tableApi = (TableAPIImpl)store.getTableAPI();

        final String setRegion = "set local region localRegion";
        doSysOp(kvhandle, setRegion);

        /* Test reading different types of CRDT. */
        testCRDT(Type.INTEGER, tableApi);

        testCRDT(Type.LONG, tableApi);

        testCRDT(Type.NUMBER, tableApi);

        /* MR counter is not allowed in non MR table */
        String ddl = "create table foo(id integer, c INTEGER as mr_counter, " +
                                       "primary key(id))";
        try {
            doSysOp(kvhandle, ddl);
            fail("operation should have failed");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage()
                         .contains("MR_counters are not allowed in the table"));
        }
    }

    private void testCRDT(Type type,
                          TableAPIImpl tableApi) {
        String tableName = "mrtable" + type;
        final String createTable = "create table " + tableName +
            "(id integer, count "  + type + " as mr_counter" +
            ", primary key(id)) in regions localRegion";
        doSysOp(kvhandle, createTable);
        /* this will throw if the table doesn't exist */
        getTable(tableName, kvhandle);

        /* Insert a row with CRDT.  */
        String insertStmt = "insert into " + tableName +
            " values (1, default)";
        QueryRequest req = new QueryRequest().setStatement(insertStmt);
        kvhandle.query(req);

        String updateStmt = "Update " + tableName +
            " set count = count + 1 where id = 1";
        req = new QueryRequest().setStatement(updateStmt);
        kvhandle.query(req);

        /* Read the CRDT. */
        MapValue key = new MapValue().put("id", 1);

        GetRequest getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        checkGetRes(type, kvhandle.get(getRequest), 1);

        /*
         * Use the direct driver to mimic the behavior of an mrtable agent.
         * It puts a remote row which has non-zero counts for remote
         * regions.
         */
        TableImpl table = (TableImpl)tableApi.getTable(tableName);
        RowImpl row = table.createRow();
        row.put("id", 2);

        row.setRegionId(2);
        row.setModificationTime(System.currentTimeMillis());
        row.setExpirationTime(0);

        FieldValueImpl crdt =
            table.getField("count").
            createCRDTValue();
        addCounts(type, crdt, 10, 12);
        RecordValueImpl record = row;
        record.putInternal(record.getFieldPos("count"),
                    crdt, false);
        tableApi.putResolve(row, null, null);

        /* Read the CRDT in the remote row. */
        key = new MapValue().put("id", 2);

        getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        checkGetRes(type, kvhandle.get(getRequest), 22);

    }

    private void checkGetRes(Type type, GetResult getRes, int expected) {
        if (type == Type.INTEGER) {
            assertEquals(expected,
                       getRes.getValue().get("count").asInteger().getValue());
        } else if (type == Type.LONG) {
            assertEquals(expected,
                         getRes.getValue().get("count").asLong().getValue());
        } else {
            assertEquals(new BigDecimal(expected),
                         getRes.getValue().get("count").asNumber().getValue());
        }
    }

    static void addCounts(Type type,
                          FieldValueImpl crdt,
                          int region1,
                          int region2) {
        crdt.putMRCounterEntry(1, createValue(type, region1));
        crdt.putMRCounterEntry(2, createValue(type, region2));
    }

    static FieldValueImpl createValue(Type type,
                                      int value) {
        if (type == Type.INTEGER) {
            return (FieldValueImpl)FieldValueFactory.createInteger(value);
        } else if (type == Type.LONG) {
            return (FieldValueImpl)FieldValueFactory.createLong(value);
        } else {
            return (FieldValueImpl)FieldValueFactory.createNumber(value);
        }
    }

    private String genString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append((char)('A' + i % 26));
        }
        return sb.toString();
    }

    protected static SystemResult doSysOp(NoSQLHandle nosqlHandle,
                                          String statement) {
        /*
         * New kv code appears to take a while to return new
         * plans if the store is fresh. Make the timeout long
         * enough to cover that
         */
        SystemRequest sreq =
            new SystemRequest().setStatement(statement.toCharArray()).
            setTimeout(20000);
        SystemResult sres = nosqlHandle.systemRequest(sreq);
        sres.waitForCompletion(nosqlHandle, 20000, 1000);
        return sres;
    }

    protected static void dropTableUsingAdmin(String tableName)
        throws Exception {
        int planId = admin.createRemoveTablePlan("dropTable", null, tableName);
        execPlan(planId);
    }

    protected static void execPlan(int planId) throws Exception {
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);
    }

    static void assertNoUnits(Result res) {
        assertEquals(0, res.getReadKBInternal());
        assertEquals(0, res.getReadUnitsInternal());
        assertEquals(0, res.getWriteKBInternal());
        assertEquals(0, res.getWriteUnitsInternal());
    }
}
