/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static oracle.nosql.proxy.protocol.JsonProtocol.FREE_TIER_SYS_TAGS;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.util.tmi.TableInfo.ActivityPhase;

import com.oracle.bmc.nosql.model.CreateTableDetails;
import com.oracle.bmc.nosql.model.QueryDetails;
import com.oracle.bmc.nosql.model.Table;
import com.oracle.bmc.nosql.model.Table.LifecycleState;
import com.oracle.bmc.nosql.model.TableCollection;
import com.oracle.bmc.nosql.model.TableLimits;
import com.oracle.bmc.nosql.model.TableSummary;
import com.oracle.bmc.nosql.model.UpdateRowDetails;
import com.oracle.bmc.nosql.model.UpdateTableDetails;
import com.oracle.bmc.nosql.requests.CreateTableRequest;
import com.oracle.bmc.nosql.requests.ListTablesRequest;
import com.oracle.bmc.nosql.requests.QueryRequest;
import com.oracle.bmc.nosql.requests.UpdateRowRequest;
import com.oracle.bmc.nosql.requests.UpdateTableRequest;
import com.oracle.bmc.nosql.responses.GetTableResponse;
import com.oracle.bmc.nosql.responses.ListTablesResponse;
import com.oracle.bmc.nosql.responses.UpdateRowResponse;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * This test suite is only for miniCloud test.
 *
 * Test free table operations including create/get/list, update table tags,
 * table activity status change from IDEL to ACTIVE on data access operation
 * and limit.
 */
public class FreeTableTest extends RestAPITestBase {

    private final String ddlFmt =
        "create table %s(id integer, name string, primary key(id))";

    private final TableLimits limits_50_50_1 = TableLimits.builder()
            .maxReadUnits(50)
            .maxWriteUnits(50)
            .maxStorageInGBs(1)
            .build();

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /*
         * Run the test in minicloud only.
         *
         * Most of preprod envs do not support free table, so disable it in
         * cloud test
         */
        Assume.assumeTrue("Skipping FreeTableTest if not run against minicloud",
                          Boolean.getBoolean(USEMC_PROP));

        RestAPITestBase.staticSetUp();
        setFreeTableStore(true);
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        setFreeTableStore(false);
        RestAPITestBase.staticTearDown();
    }

    /*
     * Test basic free table operation: create, get and list
     */
    @Test
    public void testBasic() {
        String tableName;
        GetTableResponse gtRes;
        Table table;
        TableCollection tc;
        String tableOcid;

        tableName = "freeTable";
        /* Create table */
        createTestTable(tableName, limits_50_50_1,
                        true /* isAutoReclaimable */);

        /* Get Table */
        gtRes = getTable(tableName);
        tableOcid = gtRes.getTable().getId();
        assertFreeTableInfo(gtRes.getTable(), LifecycleState.Active);

        /* Sets the table's state to IDLE_P1 */
        setTableActivity(getTenantId(), tableOcid,
                         System.currentTimeMillis() - (daysToMs(30) + 5000),
                         ActivityPhase.IDLE_P1);
        gtRes = getTable(tableName);
        assertFreeTableInfo(gtRes.getTable(), LifecycleState.Inactive);

        /* List tables */
        tc = listTableInfos();
        assertEquals(1, tc.getItems().size());
        assertEquals(tenantLimits.getNumFreeTables(),
                     tc.getMaxAutoReclaimableTables().intValue());
        assertEquals(1, tc.getAutoReclaimableTables().intValue());
        assertFreeTableInfo(tc.getItems().get(0), LifecycleState.Inactive);
        /*
         * Test free table with tags
         */
        tableName = "freeTableWithTags";
        /* freeform tags */
        Map<String, String> freeTags = new HashMap<>();
        freeTags.put("createBy", "OracleNosql");
        freeTags.put("accountType", "IAMUser");

        /* predefined tags */
        Map<String, Map<String, Object>> definedTags = new HashMap<>();
        Map<String, Object> props = new HashMap<>();
        props.put("type", "backup");
        props.put("purpose", "WebTier");
        definedTags.put("Operations", props);

        /* Create table with tags */
        createTestTable(tableName, buildCreateTableDdl(tableName),
                        limits_50_50_1, true/* isAutoReclaimable*/,
                        freeTags, definedTags);

        /* Get Table */
        gtRes = getTable(tableName);
        table = gtRes.getTable();
        assertFreeTableInfo(table, LifecycleState.Active);
        assertTableTags(table, freeTags, definedTags);

        /* List Tables */
        tc = listTableInfos();
        assertEquals(2, tc.getItems().size());
        assertEquals(tenantLimits.getNumFreeTables(),
                     tc.getMaxAutoReclaimableTables().intValue());
        assertEquals(2, tc.getAutoReclaimableTables().intValue());
        for (TableSummary tbs : tc.getItems()) {
            if (tbs.getName().equals(tableName)) {
                assertTableTags(tbs, freeTags, definedTags);
                assertFreeTableInfo(tbs, LifecycleState.Active);
            } else {
                assertFreeTableInfo(tbs, LifecycleState.Inactive);
            }
        }
    }

    /*
     * Test data access op will change table state from IDLE to ACTIVE
     */
    @Test
    public void testActivityState() throws Exception {
        String tableName = "testActivityState";
        String query = "select * from " + tableName;

        /* Create a free table*/
        String ddl = buildCreateTableDdl(tableName);
        String tableOcid =
            scCreateTable(getTenantId(), getCompartmentId(), tableName,
                          buildCreateTableDdl(tableName),
                          new oracle.nosql.util.tmi.TableLimits(50, 50, 1),
                          true /* isFreeTable */);

        /* Sets the table's state to IDLE_P1 */
        setTableActivity(getTenantId(), tableOcid,
                         System.currentTimeMillis() - (daysToMs(30) + 5000),
                         ActivityPhase.IDLE_P1);

        /* Check table state is inactive */
        GetTableResponse gtRes = getTable(tableName);
        assertEquals(LifecycleState.Inactive,
                     gtRes.getTable().getLifecycleState());

        /* DDL operation will not change the table IDLE state */
        ddl = "alter table " + tableName + "(add age integer)";
        UpdateTableRequest utReq = buildUpdateTableRequest(tableName, ddl);
        executeDdlFail(utReq, "TableDeploymentLimitExceeded");

        /* Check table state is inactive */
        gtRes = getTable(tableName);
        assertEquals(LifecycleState.Inactive,
                     gtRes.getTable().getLifecycleState());

        /* Put a row */
        Map<String, Object> value = new HashMap<String, Object>();
        value.put("id", 0);
        value.put("name", "nosql");
        UpdateRowDetails row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        UpdateRowResponse putRet = client.updateRow(putReq);
        assertNotNull(putRet.getUpdateRowResult().getVersion());

        /* Data access op, table state should be changed to ACTIVE */
        gtRes = getTable(tableName);
        assertEquals(LifecycleState.Active,
                     gtRes.getTable().getLifecycleState());

        QueryDetails info = QueryDetails.builder()
                .statement(query)
                .compartmentId(getCompartmentId())
                .build();
        QueryRequest qryReq = QueryRequest.builder()
                .queryDetails(info)
                .build();
        client.query(qryReq);
        assertEquals(LifecycleState.Active,
                     gtRes.getTable().getLifecycleState());

        /*
         * Test using binary java driver
         */

        /* Create a free table*/
        dropTable(tableName);
        tableOcid = scCreateTable(getTenantId(), getCompartmentId(),
                                  tableName, buildCreateTableDdl(tableName),
                                  new oracle.nosql.util.tmi.TableLimits(50, 50, 1),
                                  true /* isFreeTable */);

        /* Sets the table's state to IDLE_P1 */
        setTableActivity(getTenantId(), tableOcid,
                         System.currentTimeMillis() - (daysToMs(75) + 5000),
                         ActivityPhase.IDLE_P2);
        gtRes = getTable(tableName);
        assertEquals(LifecycleState.Inactive,
                     gtRes.getTable().getLifecycleState());

        oracle.nosql.driver.ops.QueryRequest qreq =
            new oracle.nosql.driver.ops.QueryRequest()
                .setStatement(query)
                .setCompartment(getCompartmentId());
        handle = configHandle(getProxyEndpoint());
        handle.query(qreq);

        /* Data access op, table state should be changed to ACTIVE */
        gtRes = getTable(tableName);
        assertEquals(LifecycleState.Active,
                     gtRes.getTable().getLifecycleState());

        /*
         * Test child tables
         *
         * The activity state of child table is always same as its top parent,
         * activating child table from IDLE will activate its top parent.
         */
        String childName = tableName + ".c";
        String childDdl = "create table " + childName +
                          "(idc integer, s string, primary key(idc))";
        String grandChildName = childName + ".g";
        String grandChildDdl = "create table " + grandChildName +
                               "(idg integer, s string, primary key(idg))";
        String childOcid;
        String grandChildOcid;

        childOcid = scCreateTable(getTenantId(), getCompartmentId(),
                                  childName, childDdl,
                                  null /* tableLimits */,
                                  true /* isFreeTable */);
        grandChildOcid = scCreateTable(getTenantId(), getCompartmentId(),
                                       grandChildName, grandChildDdl,
                                       null /* tableLimits */,
                                       true /* isFreeTable */);

        final String[] allTableOcids = new String[] {
            tableOcid,
            childOcid,
            grandChildOcid
        };

        /* Set activity of the table and child table to IDLE_P2 */
        setTableActivity(ActivityPhase.IDLE_P2, tableOcid);
        checkTableActivity(LifecycleState.Inactive, true, allTableOcids);

        /* Query child table */
        query = "select * from " + childName;
        qreq = new oracle.nosql.driver.ops.QueryRequest()
                    .setStatement(query)
                    .setCompartment(getCompartmentId());
        handle.query(qreq);
        /*
         * Verify that the activity of both top table and 2 child tables
         * should be ACTIVE
         */
        checkTableActivity(LifecycleState.Active, false, allTableOcids);

        /* Set activity of the table and child table to IDLE_P2 */
        setTableActivity(ActivityPhase.IDLE_P2, tableOcid);
        checkTableActivity(LifecycleState.Inactive, true, allTableOcids);

        /* Query top table */
        query = "select * from " + grandChildName;
        qreq = new oracle.nosql.driver.ops.QueryRequest()
                    .setStatement(query)
                    .setCompartment(getCompartmentId());
        handle.query(qreq);

        /*
         * Verify that the activity of both top table and the child tables
         * should be ACTIVE
         */
        checkTableActivity(LifecycleState.Active, false, allTableOcids);
    }

    private void setTableActivity(ActivityPhase activity,
                                  String... tableOcids) {
        final long estDdlMs = System.currentTimeMillis() -
                                (expirationTimeInMs(activity) + 5000);
        for (String tableOcid : tableOcids) {
            /* Set activity of the top table to IDLE_P2 */
            setTableActivity(getTenantId(), tableOcid, estDdlMs, activity);
        }
    }

    private void checkTableActivity(LifecycleState expState,
                                    boolean expireAtSameTime,
                                    String... tableOcids) {
        Table table;
        Date expireTime = null;
        for (String ocid : tableOcids) {
            table = getTable(ocid).getTable();
            /* check activity state */
            assertEquals(expState, table.getLifecycleState());

            /* check expiration time */
            if (expState == LifecycleState.Inactive) {
                assertNotNull(table.getTimeOfExpiration());
                if (expireAtSameTime) {
                    if (expireTime == null) {
                        expireTime = table.getTimeOfExpiration();
                    } else {
                        assertEquals(expireTime, table.getTimeOfExpiration());
                    }
                }
            } else {
                assertNull(table.getTimeOfExpiration());
            }
        }
    }

    private static long expirationTimeInMs(ActivityPhase activity) {
        if (activity == ActivityPhase.IDLE_P1) {
            return daysToMs(30);
        }
        if (activity == ActivityPhase.IDLE_P2) {
            return daysToMs(75);
        }
        return 0;
    }

    /*
     * The system tags should be kept when alter table or update tags.
     */
    @Test
    public void testUpdateTableTags() {
        String tableName = "testUpdateTagTags";
        /* freeform tags */
        Map<String, String> freeTags = new HashMap<>();
        freeTags.put("createBy", "OracleNosql");
        freeTags.put("accountType", "IAMUser");

        /* predefined tags */
        Map<String, Map<String, Object>> definedTags = new HashMap<>();
        Map<String, Object> definedProps = new HashMap<>();
        definedProps.put(DEFINED_TAG_PROP, "v0");
        definedTags.put(DEFINED_TAG_NAMESPACE, definedProps);

        /* Creates a free table and verify its system tags */
        createTestTable(tableName, buildCreateTableDdl(tableName),
                        limits_50_50_1, true, freeTags, definedTags);
        GetTableResponse gtRes = getTable(tableName);
        Table table = gtRes.getTable();
        assertEquals(FREE_TIER_SYS_TAGS, table.getSystemTags());
        assertTableTags(table, freeTags, definedTags);

        /* Alter table schema, system tags should be kept */
        freeTags.put("accountType", "free");
        definedProps.put(DEFINED_TAG_PROP, "v1");

        UpdateTableDetails utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .ddlStatement("alter table " + tableName + "(add age integer)")
                .build();
        UpdateTableRequest utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdl(utReq);
        gtRes = getTable(tableName);
        table = gtRes.getTable();
        assertTableTags(table, table.getFreeformTags(), table.getDefinedTags());
        assertEquals(FREE_TIER_SYS_TAGS, table.getSystemTags());

        /* Update table tags, system tags should be kept */
        utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .freeformTags(freeTags)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdl(utReq);
        gtRes = getTable(tableName);
        table = gtRes.getTable();
        assertTableTags(table, freeTags, Collections.emptyMap());
        assertEquals(FREE_TIER_SYS_TAGS, table.getSystemTags());

        utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .freeformTags(freeTags)
                .definedTags(definedTags)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdl(utReq);
        gtRes = getTable(tableName);
        table = gtRes.getTable();
        assertTableTags(table, freeTags, definedTags);
        assertEquals(FREE_TIER_SYS_TAGS, table.getSystemTags());
    }

    /*
     * Test the limit on the number of free table and table limits quota
     */
    @Test
    public void testLimits() {
        final int maxNumFreeTables = tenantLimits.getNumFreeTables();
        String tableNamePrefix = "freeTable";
        /* Create 2 free tables */
        for (int i = 0; i < maxNumFreeTables - 1; i++) {
            createTestTable(tableNamePrefix + i, limits_50_50_1,
                            true /* isAutoReclaimable */);
        }

        /*
         * read/write limits exceeds quota.
         */
        String tableName = tableNamePrefix + (maxNumFreeTables - 1);
        TableLimits execeedQuota = TableLimits.builder()
                .maxWriteUnits(100)
                .maxReadUnits(100)
                .maxStorageInGBs(1).build();
        CreateTableDetails.Builder info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(buildCreateTableDdl(tableName))
                .isAutoReclaimable(true)
                .tableLimits(execeedQuota);
        CreateTableRequest req = CreateTableRequest.builder()
                .createTableDetails(info.build())
                .build();
        executeDdlFail(req, "TableDeploymentLimitExceeded");

        /* create another free table */
        info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(buildCreateTableDdl(tableName))
                .isAutoReclaimable(true)
                .tableLimits(limits_50_50_1);
        req = CreateTableRequest.builder()
                .createTableDetails(info.build())
                .build();
        executeDdl(req);

        /*
         * The number of free table exceeds the limit.
         */
        tableName = tableNamePrefix + maxNumFreeTables;
        info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(buildCreateTableDdl(tableName))
                .isAutoReclaimable(true)
                .tableLimits(limits_50_50_1);
        req = CreateTableRequest.builder()
                .createTableDetails(info.build())
                .build();
        executeDdlFail(req, "TableLimitExceeded");
    }

    @Test
    public void testChildTable() {
        final int maxNumFreeTables = tenantLimits.getNumFreeTables();
        String pcDdl = "create table %s(" +
                       "   %s integer, s string, " +
                       "   primary key(%s))";

        createTestTable("p", limits_50_50_1, true);

        CreateTableDetails info;
        CreateTableRequest req;

        /*
         * If parent table is auto reclaimable, child table must be auto
         * reclaimable
         */
        String ddl = String.format(pcDdl, "p.c", "idc", "idc");
        info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name("p.c")
                .ddlStatement(ddl)
                .isAutoReclaimable(false)
                .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdlFail(req, "IllegalArgument");

        /*
         * Test the number of parent and its child tables exceeds
         * maxNumFreeTables.
         */
        GetTableResponse gtRet;
        String tableName = "p";
        String pkey;
        for (int i = 0; i < maxNumFreeTables; i++) {
            tableName += ".c" + i;
            pkey = "idk" + String.valueOf(i);
            ddl = String.format(pcDdl, tableName, pkey, pkey);

            info = CreateTableDetails.builder()
                    .compartmentId(getCompartmentId())
                    .name(tableName)
                    .ddlStatement(ddl)
                    .isAutoReclaimable(true)
                    .build();
            req = CreateTableRequest.builder()
                    .createTableDetails(info)
                    .build();

            if (i == maxNumFreeTables - 1) {
                executeDdlFail(req, "TableLimitExceeded");
            } else {
                executeDdl(req);
                gtRet = getTable(tableName);
                assertTrue(gtRet.getTable().getIsAutoReclaimable());
            }
        }
    }

    private void createTestTable(String tableName,
                                 TableLimits limits,
                                 boolean isAutoReclaimable) {
        createTestTable(tableName, buildCreateTableDdl(tableName),
                        limits, isAutoReclaimable, null, null);
    }

    private void createTestTable(String tableName,
                                 String ddl,
                                 TableLimits limits,
                                 boolean isAutoReclaimable,
                                 Map<String, String> freeTags,
                                 Map<String, Map<String, Object>> defineTags) {

        CreateTableRequest req;

        /* Create table */
        CreateTableDetails.Builder payload = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl)
                .tableLimits(limits);
        if (isAutoReclaimable) {
            payload.isAutoReclaimable(isAutoReclaimable);
        }
        if (freeTags != null) {
            payload.freeformTags(freeTags);
        }
        if (defineTags != null) {
            payload.definedTags(defineTags);
        }
        req = CreateTableRequest.builder()
                .createTableDetails(payload.build())
                .build();
        executeDdl(req);
    }

    /* List tables */
    private TableCollection listTableInfos() {
        ListTablesRequest lstReq = ListTablesRequest.builder()
                .compartmentId(getCompartmentId())
                .build();
        ListTablesResponse lstRes = client.listTables(lstReq);
        return lstRes.getTableCollection();
    }

    private String buildCreateTableDdl(String tableName) {
        return String.format(ddlFmt, tableName);
    }

    private void assertFreeTableInfo(Table table, LifecycleState state) {
        assertTrue(table.getIsAutoReclaimable());
        assertEquals(FREE_TIER_SYS_TAGS, table.getSystemTags());
        assertEquals(state, table.getLifecycleState());
        if (state == LifecycleState.Inactive) {
            assertNotNull(table.getTimeOfExpiration());
            assertTrue(table.getTimeOfExpiration()
                        .after(Date.from(Instant.now())));
        } else {
            assertNull(table.getTimeOfExpiration());
        }
    }

    private void assertTableTags(Table table,
                                 Map<String, String> freeTags,
                                 Map<String, Map<String, Object>> definedTags) {
        assertEquals(freeTags, table.getFreeformTags());
        assertDefinedTags(definedTags, table.getDefinedTags());
    }

    private static long daysToMs(int days) {
        return days * 24L * 3600L * 1000L;
    }

    private void assertFreeTableInfo(TableSummary ts, LifecycleState state) {
        assertTrue(ts.getIsAutoReclaimable());
        assertEquals(FREE_TIER_SYS_TAGS, ts.getSystemTags());
        assertEquals(state, ts.getLifecycleState());
        if (state == LifecycleState.Inactive) {
            assertNotNull(ts.getTimeOfExpiration());
            assertTrue(ts.getTimeOfExpiration()
                         .after(Date.from(Instant.now())));
        } else {
            assertNull(ts.getTimeOfExpiration());
        }
    }

    private void assertTableTags(TableSummary ts,
                                 Map<String, String> freeTags,
                                 Map<String, Map<String, Object>> definedTags) {
        assertEquals(freeTags, ts.getFreeformTags());
        assertEquals(definedTags, ts.getDefinedTags());
    }
}
