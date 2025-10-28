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

import static oracle.nosql.proxy.protocol.HttpConstants.ENTRYPOINT;
import static oracle.nosql.proxy.protocol.HttpConstants.LOGCONTROL_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.LOG_LEVEL;
import static oracle.nosql.proxy.protocol.HttpConstants.TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.junit.BeforeClass;
import org.junit.Test;

import oracle.kv.impl.api.table.NameUtils;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.util.HttpRequest;
import oracle.nosql.util.HttpResponse;

public class TenantLogTest extends ProxyTestBase {
    /* Used to enable LogControl service in KVProxy, just for test purpose */
    private static final String TEST_KV_LOGCONTROL_PROP = "test.kvlogcontrol";
    private final SkLogger logger = new SkLogger(Proxy.class.getName(),
                                                 "proxy", "proxy_worker.log");
    private final int WAIT_MS = 20000;

    private final TestLogHandler testLogHandler = new TestLogHandler();
    private final HttpRequest httpRequest = new HttpRequest();

    @BeforeClass
    public static void staticSetUp() throws Exception {

        assumeTrue("Skipping TenantLogTest for minicloud or cloud test",
                   !Boolean.getBoolean(USEMC_PROP) &&
                   !Boolean.getBoolean(USECLOUD_PROP));

        if (Boolean.getBoolean("onprem")) {
            System.setProperty(TEST_KV_LOGCONTROL_PROP, "true");
        }
        ProxyTestBase.staticSetUp();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        logger.addHandler(testLogHandler);
    }


    @Override
    public void tearDown() throws Exception {
        if (onprem) {
            doSysOp(handle, "drop namespace if exists " + getTenantId());
        }

        logger.getLogger().removeHandler(testLogHandler);
        super.tearDown();
    }

    @Test
    public void testTenantLog() {

        final String namespace;
        if (onprem) {
            namespace = getTenantId();
            doSysOp(handle, "create namespace " + namespace);
        } else {
            namespace = "in.valid.iac.name.space";
        }

        /* Log all calls for specific tenant/namespace at FINE */
        setLogLevel(namespace, Level.FINE);
        runOps(handle, namespace, true /*logEnabled*/, false /*nullEnabled*/);

        clearLogLevel(namespace, Level.FINE);
        runOps(handle, namespace, false /*logEnabled*/, false /*nullEnabled*/);
    }

    @Test
    public void testNullTenantLog() {

        final String namespace;
        if (onprem) {
            namespace = getTenantId();
            doSysOp(handle, "create namespace " + namespace);
        } else {
            namespace = "in.valid.iac.name.space";
        }

        /* Log all calls only when they do not have a valid tenantId */
        setLogLevel("nullTenantId", Level.FINE);
        runOps(handle, namespace, false /*logEnabled*/, true /*nullEnabled*/);

        clearLogLevel("nullTenantId", Level.FINE);
        runOps(handle, namespace, false /*logEnabled*/, false /*nullEnabled*/);
    }

    @Test
    public void testDataPath() {
        final String namespace;
        if (onprem) {
            namespace = getTenantId();
            doSysOp(handle, "create namespace " + namespace);
        } else {
            namespace = "in.valid.iac.name.space";
        }

        /* log all calls to /V2/nosql/data at FINE */
        setDataPathLevel(Level.FINE);
        runOps(handle, namespace, true /*logEnabled*/, false /*nullEnabled*/);

        clearDataPathLevel();
        runOps(handle, namespace, false /*logEnabled*/, false /*nullEnabled*/);
    }

    private void runOps(NoSQLHandle nosqlHandle,
                        String namespace,
                        boolean logEnabled,
                        boolean nullEnabled) {

        final int ddlWaitMs = WAIT_MS;
        final String tableName = (onprem) ?
            NameUtils.makeQualifiedName(namespace, "foo") : "foo";

        testLogHandler.flush();

        /* create table */
        String ddl = "create table " + tableName +
                     "(id integer, name string, primary key(id))";
        tableOperation(nosqlHandle, ddl,
                       (onprem ? null : new TableLimits(10, 10, 1)), ddlWaitMs);
        checkLog(namespace, logEnabled, nullEnabled,
                 OpCode.CREATE_TABLE, OpCode.GET_TABLE);

        /* create Index */
        ddl = "create index idxName on " + tableName + "(name)";
        tableOperation(nosqlHandle, ddl, null, ddlWaitMs);
        checkLog(namespace, logEnabled, nullEnabled,
                 OpCode.CREATE_INDEX, OpCode.GET_TABLE);

        /* alter table */
        ddl = "alter table " + tableName + "(add age integer)";
        tableOperation(nosqlHandle, ddl, null, ddlWaitMs);
        checkLog(namespace, logEnabled, nullEnabled,
                 OpCode.ALTER_TABLE, OpCode.GET_TABLE);

        if (!onprem) {
            /* update table limits */
            tableOperation(nosqlHandle, null, new TableLimits(11, 10, 1),
                           namespace, tableName, null /* matchETag */,
                           TableResult.State.ACTIVE, ddlWaitMs);
            checkLog(namespace, logEnabled, nullEnabled,
                     OpCode.ALTER_TABLE, OpCode.GET_TABLE);
        }

        /* list tables */
        ListTablesRequest listTables = new ListTablesRequest();
        if (onprem) {
            listTables.setNamespace(namespace);
        }
        nosqlHandle.listTables(listTables);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.LIST_TABLES);

        /* get indexes */
        GetIndexesRequest getIndexes = new GetIndexesRequest()
                .setTableName(tableName);
        nosqlHandle.getIndexes(getIndexes);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.GET_INDEXES);

        /* drop Index */
        ddl = "drop index if exists idxName on " + tableName;
        tableOperation(nosqlHandle, ddl, null, ddlWaitMs);
        checkLog(namespace, logEnabled, nullEnabled,
                 OpCode.DROP_INDEX, OpCode.GET_TABLE);

        /* put */
        MapValue val = new MapValue();
        val.put("id", 1).put("name", "oracle");
        PutRequest put = new PutRequest()
                .setTableName(tableName)
                .setValue(val);
        nosqlHandle.put(put);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.PUT);

        /* get */
        GetRequest get = new GetRequest()
                .setTableName(tableName)
                .setKey(val);
        nosqlHandle.get(get);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.GET);

        /* delete */
        DeleteRequest delete = new DeleteRequest()
                .setTableName(tableName)
                .setKey(val);
        nosqlHandle.delete(delete);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.DELETE);

        /* write-multiple */
        WriteMultipleRequest mput = new WriteMultipleRequest()
                .add(put, false);
        nosqlHandle.writeMultiple(mput);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.WRITE_MULTIPLE);

        /* multi-delete */
        MultiDeleteRequest mdel = new MultiDeleteRequest()
                .setTableName(tableName)
                .setKey(val);
        nosqlHandle.multiDelete(mdel);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.MULTI_DELETE);

        /* prepare */
        String stmt = "select * from " + tableName;
        PrepareRequest prep = new PrepareRequest().setStatement(stmt);
        PrepareResult prepRet = nosqlHandle.prepare(prep);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.PREPARE);

        /* query */
        QueryRequest query = new QueryRequest().setStatement(stmt);
        nosqlHandle.query(query);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.GET);

        /* query prepared stmt */
        query = new QueryRequest().setPreparedStatement(prepRet);
        nosqlHandle.query(query);
        checkLog(namespace, logEnabled, nullEnabled, OpCode.GET);

        /* drop table */
        ddl = "drop table " + tableName;
        tableOperation(nosqlHandle, ddl, null, ddlWaitMs);
        checkLog(namespace, logEnabled, nullEnabled,
                 OpCode.DROP_TABLE, OpCode.GET_TABLE);
    }

    private void checkLog(String namespace, boolean logEnabled,
                          boolean nullEnabled, OpCode... ops) {

        if (!logEnabled && !nullEnabled) {
            assertTrue(testLogHandler.getRecords().isEmpty());
            return;
        }

        if (nullEnabled) {
            boolean found = false;
            final String expected = "handleRequest(), headers=";
            for (LogRecord lr : testLogHandler.getRecords()) {
                if (lr.getMessage().contains(expected)) {
                    found = true;
                    break;
                }
            }
            assertTrue("Did not find \"" + expected + "\" in log", found);
        }

        if (logEnabled) {
            final String fmt = " handleRequest, op %s, " +
                               "namespace %s";
            boolean found;
            String line;
            for (OpCode op : ops) {
                found = false;
                line = String.format(fmt, op.name(), namespace);
                for (LogRecord lr : testLogHandler.getRecords()) {
                    verbose("Log line: " + lr.getMessage());
                    if (lr.getMessage().contains(line)) {
                        found = true;
                        break;
                    }
                }
                assertTrue("Did not find \"" + line + "\" in log", found);
            }
        }

        testLogHandler.flush();
    }

    private void doSysOp(NoSQLHandle handle, String ddl) {
        SystemResult ret = handle.doSystemRequest(ddl, WAIT_MS, 1000);
        assertEquals(SystemResult.State.COMPLETE, ret.getOperationState());
    }

    private void setDataPathLevel(Level level) {
        String url = getDataPathUrl(level);
        HttpResponse resp = httpRequest.doHttpPut(url, null);
        assertEquals(200, resp.getStatusCode());
    }

    private void clearDataPathLevel() {
        String url = getDataPathUrl(null);
        HttpResponse resp = httpRequest.doHttpDelete(url, null);
        assertEquals(200, resp.getStatusCode());
    }

    private void setLogLevel(String tenant, Level level) {
        String url = getLogControlUrl(tenant, level);
        HttpResponse resp = httpRequest.doHttpPut(url, null);
        assertEquals(200, resp.getStatusCode());
    }

    private void clearLogLevel(String tenant, Level level) {
        String url = getLogControlUrl(tenant, level);
        HttpResponse resp = httpRequest.doHttpDelete(url, null);
        assertEquals(200, resp.getStatusCode());
    }

    private String getLogControlUrl(String tenant, Level level) {
        return getProxyEndpoint() + "/V2/" + LOGCONTROL_PATH +
               "?" + TENANT_ID + "=" + tenant +
               "&" + LOG_LEVEL + "=" + level.getName();
    }

    private String getDataPathUrl(Level level) {
        /* "POST /V2/nosql/data" == "POST%20%2FV2%2Fnosql%2Fdata" */
        String url = getProxyEndpoint() + "/V2/" + LOGCONTROL_PATH +
               "?" + ENTRYPOINT + "=" + "POST%20%2FV2%2Fnosql%2Fdata";
        if (level == null) {
            return url;
        }
        return url + "&" + LOG_LEVEL + "=" + level.getName();
    }

    private static class TestLogHandler extends Handler {
        private List<LogRecord> records = new ArrayList<>();

        TestLogHandler() {
            setLevel(Level.ALL);
        }

        @Override
        public void publish(LogRecord lr) {
            if (lr.getLevel() == Level.FINE) {
                records.add(lr);
            }
        }

        public List<LogRecord> getRecords() {
            return records;
        }

        @Override
        public void flush() {
            /*
            System.out.println("\n==" + records.size() + "==");
            for (LogRecord lr : records) {
                if (lr.getMessage().contains("[updateLogContext]")) {
                    System.out.println(lr.getLevel() + ": " + lr.getMessage());
                }
            }
            */
            records.clear();
        }

        @Override
        public void close()
            throws SecurityException {
        }
    }
}
