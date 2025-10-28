/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.model.ChangeTableCompartmentDetails;
import com.oracle.bmc.nosql.model.Column;
import com.oracle.bmc.nosql.model.CreateIndexDetails;
import com.oracle.bmc.nosql.model.CreateTableDetails;
import com.oracle.bmc.nosql.model.Identity;
import com.oracle.bmc.nosql.model.IndexKey;
import com.oracle.bmc.nosql.model.Schema;
import com.oracle.bmc.nosql.model.Table;
import com.oracle.bmc.nosql.model.Table.LifecycleState;
import com.oracle.bmc.nosql.model.TableCollection;
import com.oracle.bmc.nosql.model.TableLimits;
import com.oracle.bmc.nosql.model.TableLimits.CapacityMode;
import com.oracle.bmc.nosql.model.TableSummary;
import com.oracle.bmc.nosql.model.TableUsageSummary;
import com.oracle.bmc.nosql.model.UpdateTableDetails;
import com.oracle.bmc.nosql.requests.ChangeTableCompartmentRequest;
import com.oracle.bmc.nosql.requests.CreateIndexRequest;
import com.oracle.bmc.nosql.requests.CreateTableRequest;
import com.oracle.bmc.nosql.requests.DeleteTableRequest;
import com.oracle.bmc.nosql.requests.GetTableRequest;
import com.oracle.bmc.nosql.requests.ListTableUsageRequest;
import com.oracle.bmc.nosql.requests.ListTablesRequest;
import com.oracle.bmc.nosql.requests.UpdateTableRequest;
import com.oracle.bmc.nosql.responses.GetTableResponse;
import com.oracle.bmc.nosql.responses.ListTableUsageResponse;
import com.oracle.bmc.nosql.responses.ListTablesResponse;

/**
 * Test table related APIs:
 *  o list tables
 *  o create table
 *  o alter table
 *  o drop table
 *  o get table
 *  o get table usage
 *  o move compartment
 */
public class TableTest extends RestAPITestBase {
    @Rule
    public final TestName test = new TestName();

    private final String newCompartmentId = getCompartmentIdMoveTo();

    @Override
    public void tearDown() throws Exception {
        if (test.getMethodName().equals("testChangeCompartment")) {
            removeAllTables(newCompartmentId);
        }
        super.tearDown();
    }

    @Test
    public void testCreateTable() throws Exception {
        String tableName = "foo";
        String ddl = "create table " + tableName + "(" +
                        "sid integer, " +
                        "id integer, " +
                        "name string, " +
                        "age integer, " +
                        "primary key(shard(sid), id))";

        TableLimits limits = TableLimits.builder()
                .maxReadUnits(200)
                .maxWriteUnits(200)
                .maxStorageInGBs(2)
                .build();

        CreateTableRequest req;
        CreateTableDetails info;

        /* Create table */
        info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl)
                .tableLimits(limits)
                .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdl(req);

        /* Check table */
        GetTableRequest getReq = GetTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .build();
        GetTableResponse getRes = client.getTable(getReq);
        Table table = getRes.getTable();
        HashMap<String, String> columns = new HashMap<String, String>();
        columns.put("sid", "integer");
        columns.put("id", "integer");
        columns.put("name", "string");
        columns.put("age", "integer");
        String[] pkeys = new String[] {"sid","id"};
        String[] sKeys = new String[] {"sid"};
        validateTable(table, tableName, columns, pkeys, sKeys, limits,
                      (cloudRunning ? ddl : null), -1 /* ttl */);
        if (cloudRunning) {
            assertEquals(ddl, table.getDdlStatement());
        }

        /* Create table with if not exists */
        String ddlIfNotExists = ddl.replace(tableName,
                "if not exists " + tableName);
        info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddlIfNotExists)
                .tableLimits(limits)
                .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdl(req);

        /* Create table but table already exists, get TableAleadyExists error */
        info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl)
                .tableLimits(limits)
                .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdlFail(req, "TableAlreadyExists");
    }

    @Test
    public void testCreateTableBadRequest() {
        String tableName = "foo1";
        String ddl = "create table if not exists " + tableName + "(" +
                        "id integer, " +
                        "name string, " +
                        "primary key(id))";

        TableLimits limits = TableLimits.builder()
                .maxReadUnits(200)
                .maxWriteUnits(200)
                .maxStorageInGBs(2)
                .build();

        CreateTableRequest req;
        CreateTableDetails info;

        /* Invalid name: name should not be empty or contain white space only */
        info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name("")
                .ddlStatement(ddl)
                .tableLimits(limits)
                .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");

        /*
         * Invalid compartmentId: compartmentId should not be empty or
         * contain white space only
         */
        info = CreateTableDetails.builder()
                .compartmentId("")
                .name(tableName)
                .ddlStatement(ddl)
                .tableLimits(limits)
                .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");

        /*
         * Invalid ddlStatement: ddlStatement should not be empty or contain
         * white space only
         */
        info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement("")
                .tableLimits(limits)
                .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");

        /*
         * Invalid TableLimits
         */
        TableLimits[] limitsBad = new TableLimits[] {
            TableLimits.builder().maxReadUnits(0).maxWriteUnits(1)
                .maxStorageInGBs(1).build(),
            TableLimits.builder().maxReadUnits(1).maxWriteUnits(0)
                .maxStorageInGBs(1).build(),
            TableLimits.builder().maxReadUnits(1).maxWriteUnits(1)
                .maxStorageInGBs(0).build(),
        };
        for (TableLimits limit : limitsBad) {
            info = CreateTableDetails.builder()
                    .compartmentId(getCompartmentId())
                    .name(tableName)
                    .ddlStatement(ddl)
                    .tableLimits(limit)
                    .build();
            req = CreateTableRequest.builder()
                    .createTableDetails(info)
                    .build();
            executeDdlFail(req, 400, "InvalidParameter");
        }

        /* Invalid DDL, it is not CREATE TABLE statement */
        info = CreateTableDetails.builder()
            .compartmentId(getCompartmentId())
            .name(tableName)
            .ddlStatement("alter table " + tableName + "(add n1 integer)")
            .tableLimits(limits)
        .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");

        /* Table name provided doesn't match the name in ddl statement */
        info = CreateTableDetails.builder()
            .compartmentId(getCompartmentId())
            .name(tableName)
            .ddlStatement("create table abc(id integer, primary key(id))")
            .tableLimits(limits)
        .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");
    }

    @Test
    public void testUpdateTable() {

        final String tableName = "foo";
        String ddl = "create table if not exists " + tableName + "(" +
                         "id integer, name String, age integer, " +
                         "primary key(id))";

        /* Create table */
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(1000)
                .maxWriteUnits(1000)
                .maxStorageInGBs(1)
                .build();
        createTable(tableName, ddl, limits);

        /* Alter table */
        ddl = "alter table " + tableName + "(add i1 integer)";
        alterTable(tableName, ddl);

        GetTableResponse gtRes = getTable(tableName);
        HashMap<String, String> columns = new HashMap<String, String>();
        columns.put("id", "integer");
        columns.put("name", "string");
        columns.put("age", "integer");
        columns.put("i1", "integer");
        validateTable(gtRes.getTable(), tableName, columns, new String[] {"id"},
                      null /* shardKey */, limits, null /* tableDdl */,
                      -1 /* ttl */);
        if (cloudRunning) {
            assertTrue(gtRes.getTable().getDdlStatement()
                            .toLowerCase().contains("if not exists"));
        }

        /* Alter TableLimits */
        limits = TableLimits.builder()
                .maxReadUnits(1500)
                .maxWriteUnits(1500)
                .maxStorageInGBs(2)
                .build();
        updateTable(tableName, limits);

        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, limits);
    }

    @Test
    public void testUpdateTableWithMatchETag() {
        assumeTrue("Skipping testUpdateTableWithMatchETag() if not minicloud " +
                   "test", cloudRunning);

        final String tableName = "foo";
        String ddl = "create table if not exists " + tableName + "(" +
                         "id integer, name String, age integer, " +
                         "primary key(id))";

        UpdateTableDetails info;
        UpdateTableRequest req;

        /* Create table */
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(1000)
                .maxWriteUnits(1000)
                .maxStorageInGBs(1)
                .build();
        createTable(tableName, ddl, limits);

        GetTableResponse gtRes = getTable(tableName);
        String oldETag = gtRes.getEtag();
        assertNotNull(oldETag);

        /* Alter table schema */
        ddl = "alter table " + tableName + "(add i1 integer)";
        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .ddlStatement(ddl)
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdl(req);

        /* Verify table information after alter table */
        gtRes = getTable(tableName);
        String currentETag = gtRes.getEtag();
        assertNotNull(currentETag);

        /*
         * Alter table with mismatched ETag, expect to get ETagMismatch error
         */
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .ifMatch(oldETag)
                .build();
        executeDdlFail(req, "ETagMismatch");

        /*
         * Update table limits
         */

        /* Update limits with matched ETag */
        limits = TableLimits.builder()
                .maxReadUnits(1500)
                .maxWriteUnits(1500)
                .maxStorageInGBs(2)
                .build();
        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .tableLimits(limits)
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .ifMatch(currentETag)
                .updateTableDetails(info)
                .build();
        executeDdl(req);
        oldETag = currentETag;

        /* Verify table information after update table limits */
        gtRes = getTable(tableName);
        currentETag = gtRes.getEtag();
        assertNotNull(currentETag);
        validateTable(gtRes.getTable(), tableName, null /* columns */,
                      new String[] {"id"}, null /* shardKeys */,
                      limits, null /* tableDdl */, -1 /* ttl */);

        /*
         * Update limits with mismatched ETag, get ETagMismatch error
         */
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .ifMatch(oldETag)
                .build();
        executeDdlFail(req, "ETagMismatch");

        /*
         * Update tags
         */

        /* update tags with matched ETag */
        Map<String, String> freeformTags = new HashMap<>();
        freeformTags.put("createBy", "OracleNosql");
        freeformTags.put("accountType", "IAMUser");

        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .freeformTags(freeformTags)
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .ifMatch(currentETag)
                .build();
        executeDdl(req);
        oldETag = currentETag;

        /* verify table information after update tags */
        gtRes = getTable(tableName);
        assertEquals(freeformTags, gtRes.getTable().getFreeformTags());
        assertDefinedTags(Collections.emptyMap(),
                          gtRes.getTable().getDefinedTags());

        /* update tags with mismatched ETag, get ETagMismatch error */
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .ifMatch(oldETag)
                .build();
        executeDdlFail(req, "ETagMismatch");
    }

    @Test
    public void testUpdateTableBadRequest() {
        final String tableName = "foo";

        UpdateTableDetails info;
        UpdateTableRequest req;

        /*
         * Invalid compartmentId: compartmentId should not be empty or contain
         * white space only
         */
        String ddl = "alter table " + tableName + "(add i1 integer)";
        info = UpdateTableDetails.builder()
                .compartmentId("")
                .ddlStatement(ddl)
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");

        /*
         * Invalid ddlStatement: ddlStatement should not be empty or contain
         * white space only
         */
        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .ddlStatement(" ")
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");

        /*
         * Invalid TableLimits, readUnits/writeUnits/StorageInGBs should be
         * great than 0
         */
        TableLimits[] limitsBad = new TableLimits[] {
            TableLimits.builder().maxReadUnits(0).maxWriteUnits(1)
                .maxStorageInGBs(1).build(),
            TableLimits.builder().maxReadUnits(1).maxWriteUnits(0)
                .maxStorageInGBs(1).build(),
            TableLimits.builder().maxReadUnits(1).maxWriteUnits(1)
                .maxStorageInGBs(0).build(),
        };
        for (TableLimits limit : limitsBad) {
            info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .tableLimits(limit)
                .build();
            req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
            executeDdlFail(req, 400, "InvalidParameter");
        }

        /* Either ddlStatement or tableLimits should not be null */
        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");

        /* Only one of either ddlStatement or tableLimits may be specified */
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(100)
                .maxWriteUnits(200)
                .maxStorageInGBs(2)
                .build();
        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .ddlStatement(ddl)
                .tableLimits(limits)
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");

        /* Only one of either tableLimits or tags may be specified */
        Map<String, String> freeformTags = new HashMap<>();
        freeformTags.put("k1", "v1");
        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .tableLimits(limits)
                .freeformTags(freeformTags)
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");

        /* Table not found */
        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .ddlStatement(ddl)
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        if (cloudRunning) {
            executeDdlFail(req, 404, "NotAuthorizedOrNotFound");
        } else {
            executeDdlFail(req, 400, "InvalidParameter");
        }

        /* Invalid ddl, ddl is not ALTER TABLE statement */
        ddl = "create table " + tableName + "(id integer, primary key(id))";
        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .ddlStatement(ddl)
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdlFail(req, 404,
                (cloudRunning ? "NotAuthorizedOrNotFound" : "TableNotFound"));

        /* Table name given doesn't match the name in ddl statement */
        ddl = "alter table abc(add i1 integer)";
        info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .ddlStatement(ddl)
                .build();
        req = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdlFail(req, 400, "InvalidParameter");
    }

    @Test
    public void testDeleteTable() {
        final String tableName = "foo";
        String ddl = "create table if not exists " + tableName + "(" +
                         "id integer, name String, age integer, " +
                         "primary key(id))";

        /* Create table */
        createTable(tableName, ddl);

        DeleteTableRequest req;

        /* Delete table */
        req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .build();
        executeDdl(req);

        try {
            getTable(tableName);
            fail("Expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
            checkErrorMessage(ex);
        }

        /* Delete table if exists */
        req = DeleteTableRequest.builder()
                    .compartmentId(getCompartmentId())
                    .tableNameOrId(tableName)
                    .isIfExists(true)
                    .build();
        executeDdl(req);

        /* Delete a non-exists table using if exists */
        req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId("invalid")
                .isIfExists(true)
                .build();
        executeDdl(req);

        /* Delete a non-exists table */
        req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId("invalid")
                .isIfExists(false)
                .build();
        executeDdlFail(req, "TableNotFound");
    }

    @Test
    public void testDeleteTableWithMatchETag() {
        assumeTrue("Skipping testDeleteTableWithMatchETag() if not minicloud " +
                   "test", cloudRunning);

        final String tableName = "foo";
        String ddl = "create table if not exists " + tableName + "(" +
                         "id integer, name String, age integer, " +
                         "primary key(id))";

        /* Create table */
        createTable(tableName, ddl);
        GetTableResponse gtRes = getTable(tableName);
        String currentETag = gtRes.getEtag();
        assertNotNull(currentETag);

        /* Alter table schema */
        ddl = "alter table " + tableName + "(add i1 integer)";
        UpdateTableDetails info = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .ddlStatement(ddl)
                .build();
        UpdateTableRequest utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdl(utReq);
        String oldETag = currentETag;

        /* Verify table information after alter table */
        gtRes = getTable(tableName);
        currentETag = gtRes.getEtag();

        DeleteTableRequest req;

        /* Delete table with mismatched ETag */
        req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .ifMatch(oldETag)
                .build();
        executeDdlFail(req, "ETagMismatch");

        /* Delete table with matched ETag */
        req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .ifMatch(currentETag)
                .build();
        executeDdl(req);

        try {
            getTable(tableName);
            fail("Expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
            checkErrorMessage(ex);
        }

        /*
         * Delete non-existing table with if exists and ETag, expect to succeed.
         */
        req = DeleteTableRequest.builder()
                    .compartmentId(getCompartmentId())
                    .tableNameOrId(tableName)
                    .isIfExists(true)
                    .ifMatch(currentETag)
                    .build();
        executeDdl(req);
    }

    @Test
    public void testGetTable() {
        String tableName = "foo";
        String ddl = "create table " + tableName + "(" +
                        "sid integer, " +
                        "id integer, " +
                        "name string, " +
                        "age integer, " +
                        "address json, " +
                        "dateTime timestamp(6), " +
                        "bin binary, " +
                        "bin20 binary(20), " +
                        "color enum(YELLOW, BLUE, RED), " +
                        "hobbies map(string), " +
                        "numbers array(number), " +
                        "info record(ri integer, rs string, rm map(integer)), " +
                        "primary key(shard(sid), id)) using ttl 3 days";
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(200)
                .maxWriteUnits(200)
                .maxStorageInGBs(2)
                .build();

        createTable(tableName, ddl, limits);

        GetTableRequest req;
        GetTableResponse ret;

        req = GetTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .build();
        ret = client.getTable(req);
        assertNotNull(ret);
        assertNotNull(ret.getEtag());

        Table table = ret.getTable();
        Schema schema = table.getSchema();
        assertEquals(getCompartmentId(), table.getCompartmentId());
        assertNotNull(schema.getTtl());
        assertEquals(3, schema.getTtl().intValue());
        assertNotNull(table.getDdlStatement());
        assertTableOcid(table.getId());
        HashMap<String, String> columns = new HashMap<String, String>();
        columns.put("sid", "integer");
        columns.put("id", "integer");
        columns.put("name", "string");
        columns.put("age", "integer");
        columns.put("address", "json");
        columns.put("dateTime", "timestamp(6)");
        columns.put("bin", "binary");
        columns.put("bin20", "binary(20)");
        columns.put("color", "enum(YELLOW, BLUE, RED)");
        columns.put("hobbies", "map(string)");
        columns.put("numbers", "array(number)");
        columns.put("info", "record(ri integer, rs string, rm map(integer))");
        String[] pkeys = new String[] {"sid","id"};
        String[] sKeys = new String[] {"sid"};
        validateTable(table, tableName, columns, pkeys, sKeys, limits,
                      (cloudRunning ? ddl : null), 3 /* ttl */);

        /*
         * Test identity and uuid
         */

        /*
         * seqNo INTEGER GNERATED ALWAYS AS IDENTITY
         * guid0 STRING AS UUID
         */
        ddl = "ALTER TABLE " + tableName +
                "(ADD seqNo INTEGER GENERATED ALWAYS AS IDENTITY," +
                " ADD guid0 STRING AS UUID)";
        alterTable(tableName, ddl, true);
        ret = client.getTable(req);
        assertNotNull(ret);
        schema = ret.getTable().getSchema();
        assertNotNull(schema);
        checkIdenity(schema.getIdentity(), "seqNo", true, false);
        int uuidCols = 0;
        for (Column col : schema.getColumns()) {
            if (col.getName().equals("guid0")) {
                assertTrue(col.getIsAsUuid());
                assertFalse(col.getIsGenerated());
                uuidCols++;
            } else {
                assertFalse(col.getIsAsUuid());
                assertFalse(col.getIsGenerated());
            }
        }
        assertEquals(1, uuidCols);

        /*
         * seqNo INTEGER GNERATED BY DEFAULT AS IDENTITY
         * guid0 STRING AS UUID
         */
        ddl = "ALTER TABLE " + tableName +
              "(MODIFY seqNo GENERATED BY DEFAULT AS IDENTITY)";
        alterTable(tableName, ddl, true);
        ret = client.getTable(req);
        assertNotNull(ret);
        checkIdenity(ret.getTable().getSchema().getIdentity(), "seqNo",
                        false, false);

        /*
         * seqNo INTEGER GNERATED BY DEFAULT ON NULL AS IDENTITY
         * guid0 STRING AS UUID
         */
        ddl = "ALTER TABLE " + tableName +
              "(MODIFY seqNo GENERATED BY DEFAULT ON NULL AS IDENTITY)";
        alterTable(tableName, ddl, true);
        ret = client.getTable(req);
        assertNotNull(ret);
        checkIdenity(ret.getTable().getSchema().getIdentity(), "seqNo",
                     false, true);

        /*
         * seqNo INTEGER
         * guid0 STRING AS UUID
         * guid1 STRING AS UUID GENERATED BY DEFAULT)
         */
        ddl = "ALTER TABLE " + tableName +
              "(MODIFY seqNo DROP IDENTITY," +
              " ADD guid1 STRING AS UUID GENERATED BY DEFAULT)";
        alterTable(tableName, ddl, true);
        ret = client.getTable(req);
        assertNotNull(ret);
        schema = ret.getTable().getSchema();
        assertNull(schema.getIdentity());
        uuidCols = 0;
        for (Column col : schema.getColumns()) {
            if (col.getName().equals("guid0")) {
                assertTrue(col.getIsAsUuid());
                assertFalse(col.getIsGenerated());
                uuidCols++;
            } else if (col.getName().equals("guid1")) {
                assertTrue(col.getIsAsUuid());
                assertTrue(col.getIsGenerated());
                uuidCols++;
            } else {
                assertFalse(col.getIsAsUuid());
                assertFalse(col.getIsGenerated());
            }
        }
        assertEquals(2, uuidCols);

        /* Table not found */
        req = GetTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId("tableNotExists")
                .build();
        try {
            client.getTable(req);
            fail("expect to fail but not");
        } catch (BmcException ex) {
            assertEquals(404 /* Table not found */ , ex.getStatusCode());
            checkErrorMessage(ex);
        }
    }

    /**
     * Test get/update/delete table using table ocid.
     */
    @Test
    public void testWithTableOcid() {
        assumeTrue("Skipping testWithTableOcid() if not minicloud test ",
                   cloudRunning);

        final String tableName = "testGetUpdateDeleteTableWithTableOcid";
        String ddl = "create table if not exists " + tableName + "(" +
                         "id integer, name String, age integer, " +
                         "primary key(id))";
        /* Create table */
        createTable(tableName, ddl);

        final String tableOcid = getTableId(tableName);

        /* Get table */
        GetTableRequest gtReq = GetTableRequest.builder()
                .tableNameOrId(tableOcid)
                .build();
        GetTableResponse gtRet = client.getTable(gtReq);
        assertNotNull(gtRet);
        assertNotNull(gtRet.getEtag());

        /* Alter table */
        ddl = "alter table " + tableName + "(add i1 integer)";
        UpdateTableDetails info = UpdateTableDetails.builder()
                .ddlStatement(ddl)
                .build();
        UpdateTableRequest utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableOcid)
                .updateTableDetails(info)
                .build();
        executeDdl(utReq);

        GetTableResponse gtRes = getTable(tableName);
        HashMap<String, String> columns = new HashMap<String, String>();
        columns.put("id", "integer");
        columns.put("name", "string");
        columns.put("age", "integer");
        columns.put("i1", "integer");
        validateTable(gtRes.getTable(), tableName, columns, new String[] {"id"},
                      null /* shardKeys */, defaultLimits, null /* tableDdl */,
                      -1 /* ttl*/);

        /* Invalid table ocid: update table with mismatched table ocid. */
        String createTableFoo = "create table foo (id integer, primary key(id))";
        createTable("foo", createTableFoo);
        String fooOcid = getTableId("foo");

        ddl = "alter table " + tableName + "(drop i1)";
        info = UpdateTableDetails.builder()
                .ddlStatement(ddl)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(fooOcid)
                .updateTableDetails(info)
                .build();
        executeDdlFail(utReq, 400, "InvalidParameter");

        /* Update TableLimits */
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(101)
                .maxWriteUnits(101)
                .maxStorageInGBs(2)
                .build();
        info = UpdateTableDetails.builder()
                .tableLimits(limits)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableOcid)
                .updateTableDetails(info)
                .build();
        executeDdl(utReq);

        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, limits);

        /* Delete table */
        DeleteTableRequest dtReq = DeleteTableRequest.builder()
                .tableNameOrId(tableOcid)
                .build();
        executeDdl(dtReq);

        try {
            getTable(tableName);
            fail("Expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testNonExistentTableOcid() {
        assumeTrue("Skipping testNonExistentTableOcid() if not minicloud test",
                   cloudRunning);

        String tableName = "testNonExistentTableOcid";
        String ddl = "create table if not exists " + tableName + "(" +
                     "id integer, name String, age integer, " +
                     "primary key(id))";
        /* Create table */
        createTable(tableName, ddl);

        /* fake non-existent table ocid */
        String tableOcid = getTableId(tableName) + "notexist";

        /* Get table */
        try {
            getTable(tableOcid);
            fail("GetTable expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
        }

        /* Alter table */
        try {
            ddl = "alter table " + tableName + "(add i1 integer)";
            UpdateTableDetails info = UpdateTableDetails.builder()
                    .ddlStatement(ddl)
                    .build();
            UpdateTableRequest utReq = UpdateTableRequest.builder()
                    .tableNameOrId(tableOcid)
                    .updateTableDetails(info)
                    .build();
            executeDdl(utReq);
            fail("AlterTable expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
        }

        /* List table usage */
        try {
            ListTableUsageRequest.Builder builder =
                    ListTableUsageRequest.builder()
                    .tableNameOrId(tableOcid);
            client.listTableUsage(builder.build());
            fail("ListTableUsage expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
        }

        /* Delete a non-exists table using if exists */
        DeleteTableRequest req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId("invalid")
                .isIfExists(true)
                .build();
        executeDdl(req);

        /* Delete a non-exists table */
        req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId("invalid")
                .isIfExists(false)
                .build();
        executeDdlFail(req, "TableNotFound");
    }

    /*
     * Test list tables
     * the cases with advanced parameters are for minicloud test only
     */
    @Test
    public void testListTables() {

        final String[] tableNames = new String[] {
            "nosqlUsers9",
            "Emails",
            "Addresses",
            "andcUsers10"
        };
        final int numTables = tableNames.length;

        /* freeform tags */
        final Map<String, String> freeformTags = new HashMap<>();
        freeformTags.put("createBy", "OracleNosql");
        freeformTags.put("accountType", "IAMUser");
        final List<String> tablesWithFreeformTags =
                Arrays.asList("nosqlUsers9", "Emails");

        /* predefined tags */
        final Map<String, Map<String, Object>> definedTags = new HashMap<>();
        Map<String, Object> props = new HashMap<>();
        props.put(DEFINED_TAG_PROP, "WebTier");
        definedTags.put(DEFINED_TAG_NAMESPACE, props);

        final List<String> tablesWithDefinedTags =
                Arrays.asList("nosqlUsers9", "Addresses");

        final List<String> timeCreatedAsc = new ArrayList<String>();
        Map<String, String> freeform;
        Map<String, Map<String, Object>> defined;
        /*
         * Create tables for testing list tables. One of them is auto scaling
         * table.
         */
        for (String tableName : tableNames) {
            freeform = tablesWithFreeformTags.contains(tableName) ?
                        freeformTags : null;
            defined = tablesWithDefinedTags.contains(tableName) ?
                        definedTags : null;
            TableLimits limits;
            if (tableName.equals(tableNames[0])) {
                /* create one auto scaling table */
                limits = TableLimits.builder()
                    .maxStorageInGBs(100)
                    .capacityMode(CapacityMode.OnDemand)
                    .build();
            } else {
                limits = defaultLimits;
            }
            createTestTable(tableName, limits, freeform, defined);
            timeCreatedAsc.add(tableName);
        }
        if (cloudRunning) {
            ListTablesRequest listRequest =
                ListTablesRequest.builder().
                                  compartmentId(getCompartmentId()).build();
            ListTablesResponse listResponse = client.listTables(listRequest);
            TableCollection tc = listResponse.getTableCollection();
            assertEquals(0, tc.getAutoReclaimableTables().intValue());
            assertEquals(1, tc.getOnDemandCapacityTables().intValue());
            if (tenantLimits != null) {
                assertEquals(tenantLimits.getNumFreeTables(),
                             tc.getMaxAutoReclaimableTables().intValue());
                assertEquals(tenantLimits.getNumAutoScalingTables(),
                             tc.getMaxOnDemandCapacityTables().intValue());
            }
        }
        final List<String> timeCreatedDesc = new ArrayList<>(timeCreatedAsc);
        Collections.reverse(timeCreatedDesc);
        final List<String> nameAsc = new ArrayList<>(timeCreatedAsc);
        Collections.sort(nameAsc);

        List<TableSummary> tables;
        ListTablesRequest.LifecycleState state = null;
        ListTablesRequest.SortBy sortBy = null;
        ListTablesRequest.SortOrder sortOrder = null;
        int limit = 0;
        String namePattern = null;

        /* list table with defaults for all parameters */
        tables = doListTables(null /* namePattern */,
                              null /* LifecycleState */,
                              null /* SortBy */,
                              null /* sortOrder */,
                              0    /* limit */,
                              -1);

        for (TableSummary table : tables) {
             assertTrue(timeCreatedAsc.contains(table.getName()));
             assertTableOcid(table.getId());
        }
        assertEquals(numTables, tables.size());
        if (cloudRunning) {
            assertSortedTable(tables, timeCreatedDesc);
        }

        /* list table with state = ACTIVE and limit = 3 */
        state = ListTablesRequest.LifecycleState.Active;
        limit = 3;
        tables = doListTables(null   /* namePattern */,
                              state  /* LifecycleState */,
                              null   /* SortBy */,
                              null   /* sortOrder */,
                              limit  /* limit */,
                              numTables);
        for (TableSummary table : tables) {
            assertEquals(LifecycleState.Active, table.getLifecycleState());
            assertTrue(timeCreatedAsc.contains(table.getName()));

            if (cloudRunning) {
                /* verify freeformTags and definedTags */
                if (tablesWithFreeformTags.contains(table.getName())) {
                    assertEquals(freeformTags, table.getFreeformTags());
                }
                if (tablesWithDefinedTags.contains(table.getName())) {
                    assertDefinedTags(definedTags, table.getDefinedTags());
                }
            }
        }

        /* Below tests uses advanced parameters, they are for miniCloud only */
        if (!cloudRunning) {
            return;
        }

        /* list table with defaults for all parameters */
        tables = doListTables(null /* namePattern */,
                              null /* LifecycleState */,
                              null /* SortBy */,
                              null /* sortOrder */,
                              0    /* limit */,
                              -1);
        assertSortedTable(tables, timeCreatedDesc);

        /* list table with state = "CREATING", return 0 row */
        state = ListTablesRequest.LifecycleState.Creating;
        limit = 3;
        tables = doListTables(null      /* namePattern */,
                              state     /* LifecycleState */,
                              null      /* SortBy */,
                              null      /* sortOrder */,
                              limit     /* limit */,
                              0);

        /* list table with state = "ACTIVE" order by timeCreated desc */
        state = ListTablesRequest.LifecycleState.Active;
        sortBy = ListTablesRequest.SortBy.TimeCreated;
        sortOrder = ListTablesRequest.SortOrder.Asc;
        limit = 3;
        tables = doListTables(null      /* namePattern */,
                              state     /* LifecycleState */,
                              sortBy    /* SortBy */,
                              sortOrder /* sortOrder */,
                              limit     /* limit */,
                              numTables);
        for (TableSummary table : tables) {
            assertEquals(LifecycleState.Active, table.getLifecycleState());
        }
        assertSortedTable(tables, timeCreatedAsc);

        /* list table with state = "ACTIVE" order by name */
        state = ListTablesRequest.LifecycleState.Active;
        sortBy = ListTablesRequest.SortBy.Name;
        sortOrder = ListTablesRequest.SortOrder.Asc;
        limit = 4;
        tables = doListTables(null      /* namePattern */,
                              state     /* LifecycleState */,
                              sortBy    /* SortBy */,
                              sortOrder /* sortOrder */,
                              limit     /* limit */,
                              numTables);
        for (TableSummary table : tables) {
            assertEquals(LifecycleState.Active, table.getLifecycleState());
        }
        assertSortedTable(tables, nameAsc);

        /*
         * list table:
         *  namepattern = "*users*"
         *  state = "ACTIVE"
         *  order by timeCreated asc
         */
        namePattern = "*Users*";
        state = ListTablesRequest.LifecycleState.Active;
        sortBy = ListTablesRequest.SortBy.TimeCreated;
        sortOrder = ListTablesRequest.SortOrder.Asc;
        limit = 1;
        tables = doListTables(namePattern /* namePattern */,
                              state       /* LifecycleState */,
                              sortBy      /* SortBy */,
                              sortOrder   /* sortOrder */,
                              limit       /* limit */,
                              2);
        int prevIndex = -1;
        for (TableSummary table : tables) {
            assertEquals(LifecycleState.Active, table.getLifecycleState());
            int i = timeCreatedAsc.lastIndexOf(table.getName());
            assertTrue(prevIndex < i);
            prevIndex = i;
        }

        /*
         * list table:
         *  namepattern = "*Users?"
         *  state = "ACTIVE"
         *  order by timeCreated asc
         */
        namePattern = "*Users?";
        state = ListTablesRequest.LifecycleState.Active;
        sortBy = ListTablesRequest.SortBy.TimeCreated;
        sortOrder = ListTablesRequest.SortOrder.Asc;
        limit = 3;
        tables = doListTables(namePattern /* namePattern */,
                              state       /* LifecycleState */,
                              sortBy      /* SortBy */,
                              sortOrder   /* sortOrder */,
                              limit       /* limit */,
                              1);
        TableSummary table = tables.get(0);
        assertEquals(LifecycleState.Active, table.getLifecycleState());
        assertEquals("nosqlUsers9", table.getName());
    }

    private List<TableSummary>
        doListTables(String namePattern,
                     ListTablesRequest.LifecycleState state,
                     ListTablesRequest.SortBy sortBy,
                     ListTablesRequest.SortOrder sortOrder,
                     int limit,
                     int expCount) {

        String nextPage = null;
        int count = 0;
        ListTablesRequest req;
        ListTablesResponse res;
        TableCollection tc;

        ListTablesRequest.Builder builder =
            ListTablesRequest.builder()
                .compartmentId(getCompartmentId())
                .limit(limit);
        if (namePattern != null) {
            builder.name(namePattern);
        }
        if (state != null) {
            builder.lifecycleState(state);
        }
        if (sortBy != null) {
            builder.sortBy(sortBy);
        }
        if (sortOrder != null) {
            builder.sortOrder(sortOrder);
        }

        List<TableSummary> results = new ArrayList<TableSummary>();
        do {
            if (nextPage != null) {
                builder.page(nextPage);
            }

            req = builder.build();
            res = client.listTables(req);
            tc = res.getTableCollection();
            count += tc.getItems().size();
            if (limit > 0) {
                assertTrue(tc.getItems().size() <= limit);
            }
            for (TableSummary table : tc.getItems()) {
                assertNotNull(table.getName());
                results.add(table);
            }
            nextPage = res.getOpcNextPage();
        } while (nextPage != null);

        if (expCount >= 0) {
            assertEquals(expCount, count);
        }
        return results;
    }

    private void assertSortedTable(List<TableSummary> tables,
                                   List<String> expSorted) {
        assertEquals(expSorted.size(), tables.size());
        for (int i = 0; i < tables.size(); i++) {
            assertEquals(expSorted.get(i), tables.get(i).getName());
        }
    }

    @Test
    public void testTableTags() {
        assumeTrue("Skipping testTableTags() if not minicloud test",
                   cloudRunning);

        String tableName = "foo";
        String ddl = "create table " + tableName + "(" +
                        "sid integer, " +
                        "id integer, " +
                        "name string, " +
                        "age integer, " +
                        "primary key(shard(sid), id))";

        TableLimits limits = TableLimits.builder()
                .maxReadUnits(200)
                .maxWriteUnits(200)
                .maxStorageInGBs(2)
                .build();

        /* freeform tags */
        Map<String, String> freeformTags = new HashMap<>();
        freeformTags.put("createBy", "OracleNosql");
        freeformTags.put("accountType", "IAMUser");

        Map<String, String> freeformTags1 = new HashMap<>();
        freeformTags1.put("createBy", "ANDC");
        freeformTags1.put("accountType", "testUser");

        /* predefined tags */
        Map<String, Map<String, Object>> definedTags = null;
        definedTags = new HashMap<>();
        Map<String, Object> props = new HashMap<>();
        props.put(DEFINED_TAG_PROP, "definedTags");
        definedTags.put(DEFINED_TAG_NAMESPACE, props);

        /* Create table */
        createTable(tableName, ddl, limits, freeformTags, definedTags);

        /* Get table and check tags */
        GetTableResponse gtRes = getTable(tableName);
        Table table = gtRes.getTable();
        assertEquals(freeformTags, table.getFreeformTags());
        assertDefinedTags(definedTags, table.getDefinedTags());

        /* Update tags */
        props.put(DEFINED_TAG_PROP, "updatedKey");
        UpdateTableDetails utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .definedTags(definedTags)
                .freeformTags(freeformTags1)
                .build();
        UpdateTableRequest utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdl(utReq);

        gtRes = getTable(tableName);
        table = gtRes.getTable();
        assertEquals(freeformTags1, table.getFreeformTags());
        assertDefinedTags(definedTags, table.getDefinedTags());

        /* Remove freeformTags */
        utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .freeformTags(Collections.emptyMap())
                .definedTags(definedTags)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdl(utReq);

        gtRes = getTable(tableName);
        table = gtRes.getTable();
        assertTrue(table.getFreeformTags().isEmpty());
        assertDefinedTags(definedTags, table.getDefinedTags());

        /* Remove definedTags */
        utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .definedTags(Collections.emptyMap())
                .freeformTags(freeformTags)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdl(utReq);

        gtRes = getTable(tableName);
        table = gtRes.getTable();
        assertDefinedTags(Collections.emptyMap(), table.getDefinedTags());
        assertEquals(freeformTags, table.getFreeformTags());

        /*
         * Invalid tags
         */

        /* The value of 'k1' in freeformTags is null */
        freeformTags1 = new HashMap<>();
        freeformTags1.put("k1", null);
        utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .freeformTags(freeformTags1)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdlFail(utReq, 400, "InvalidParameter");

        /* The tags of a namespace in definedTags is null */
        definedTags.put("andc", null);
        utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .definedTags(definedTags)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdlFail(utReq, 400, "InvalidParameter");

        /* The value of andc.k1 in definedTags is null */
        Map<String, Object> kvs = new HashMap<>();
        kvs.put("k1", null);
        definedTags.put("andc", kvs);
        utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .definedTags(definedTags)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdlFail(utReq, 400, "InvalidParameter");

        /*
         * The value of andc.k1 in definedTags is invalid type, valid type is
         * INTEGER/STRING/BOOLEAN
         */
        kvs.clear();
        kvs.put("k1", new ArrayList<String>());
        definedTags.put("andc", kvs);
        utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .definedTags(definedTags)
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(utInfo)
                .build();
        executeDdlFail(utReq, 400, "InvalidParameter");
    }

    @Test
    public void testChangeCompartment() throws Exception {
        assumeTrue("Skipping testChangeCompartment() if not minicloud test",
                   cloudRunning);

        final String tableName = "changeCompartmentTest";
        final String ddl = "create table if not exists " + tableName + "(" +
                           "id integer, name String, age integer, " +
                           "primary key(id))";

        String origCompt = getCompartmentId();

        /* Create table */
        createTable(tableName, ddl);
        GetTableResponse gtRes = getTable(tableName);
        String origETag = gtRes.getEtag();
        String tableOcid = gtRes.getTable().getId();

        /* Move the table to newCompt */
        execChangeCompartment(origCompt, newCompartmentId, tableName);

        /* Get table using ocid */
        gtRes = getTable(tableOcid);
        assertEquals(newCompartmentId, gtRes.getTable().getCompartmentId());

        /* Get table using compartment + tableName */
        gtRes = getTable(newCompartmentId, tableName);
        assertEquals(newCompartmentId, gtRes.getTable().getCompartmentId());

        /* Find table with origCompt, should not found */
        try {
            getTable(origCompt, tableName);
            fail("Expect to get 404(TableNotFound) but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            checkErrorMessage(ex);
        }

        /* Move the table back to origCompt */
        execChangeCompartment(newCompartmentId, origCompt, tableName);

        /* Get table using ocid */
        gtRes = getTable(tableOcid);
        assertEquals(origCompt, gtRes.getTable().getCompartmentId());

        /* Get table using compartment + tableName */
        gtRes = getTable(origCompt, tableName);
        assertEquals(origCompt, gtRes.getTable().getCompartmentId());
        String currentETag = gtRes.getEtag();

        /*
         * Move table to newCompt with unmatched ETag, should fail with
         * ETagMismatch error.
         */
        execChangeCompartment(origCompt, newCompartmentId, tableName, origETag,
                              true /* expFail */, "ETagMismatch");

        /*
         * Move table to newCompt with matched ETag, should succeed
         */
        execChangeCompartment(origCompt, newCompartmentId, tableName,
                              currentETag, false /* expFail */, null);

        /*
         * Move table to newCompt again, it should have failed with
         * 404(NotAuthorizedOrNotFound)
         */
        execChangeCompartment(origCompt, newCompartmentId, tableName,
                              null /* matchETag */, true /* expFail */,
                              "NotAuthorizedOrNotFound");

        /* Create table in origCompt */
        createTable(tableName, ddl);
        /*
         * Move table to newCompt, the table already existed in newCompt,
         * it should have failed with TableAlreadyExists error
         */
        execChangeCompartment(origCompt, newCompartmentId, tableName,
                              null /* matchETag */, true /* expFail */,
                              "TableAlreadyExists");

        /* Drop the table in newCompt */
        dropTable(newCompartmentId, tableName, false /* ifExists */,
                  true /* wait */);

        /* Bad parameter, toCompartment can not be null or empty */
        execChangeCompartment(origCompt, "", tableName, null /* matchETag */,
                              true /* expFail */, "InvalidParameter");

        /* Change table compartment using tableOcid: origCompt -> newCompt */
        tableOcid = getTableId(tableName);
        execChangeCompartment(null, newCompartmentId, tableOcid);

        gtRes = getTable(tableOcid);
        assertEquals(newCompartmentId, gtRes.getTable().getCompartmentId());
    }

    private void execChangeCompartment(String fromCompt,
                                       String toCompt,
                                       String tableNameOrId) {
        execChangeCompartment(fromCompt, toCompt, tableNameOrId, null,
                              false, null);
    }

    private void execChangeCompartment(String fromCompt,
                                       String toCompt,
                                       String tableNameOrId,
                                       String matchETag,
                                       boolean expFail,
                                       String errorCode) {
        ChangeTableCompartmentRequest req;
        ChangeTableCompartmentDetails info;

        info = ChangeTableCompartmentDetails.builder()
                .fromCompartmentId(fromCompt)
                .toCompartmentId(toCompt)
                .build();
        req = ChangeTableCompartmentRequest.builder()
                .tableNameOrId(tableNameOrId)
                .changeTableCompartmentDetails(info)
                .ifMatch(matchETag)
                .build();

        if (expFail) {
            executeDdlFail(req, errorCode);
        } else {
            executeDdl(req);
        }
    }

    @Test
    public void testGetTableUsage() throws Exception {
        assumeTrue("Skipping testGetTableUsage() if not minicloud test",
                   cloudRunning);

        final String tableName = "testUsage";
        final String ddl = "create table if not exists " + tableName + "(" +
                           "id integer, name String, " +
                           "primary key(id))";
        /* Create table */
        createTable(tableName, ddl);

        long startTime = System.currentTimeMillis();
        long millsInSlice0 = startTime % 60_000L;
        int delayMsToNextSlice = (int)(60_000L - millsInSlice0);
        /* Start put/get at slice0 + 3sec to avoid edge case */
        Thread.sleep(delayMsToNextSlice + 3000);
        long slice0Start = startTime + delayMsToNextSlice;

        /* Put a row */
        Map<String, Object> row = new HashMap<>();
        row.put("id", 0);
        row.put("name", "name0");
        putRow(tableName, row);

        /* Wait for another minute to slice1 */
        Thread.sleep(60_000);

        /* Get a row */
        getRow(tableName, Arrays.asList("id:0"));
        /*
         * Wait for another minute to make sure the usage data for slice0 and
         * slice1 are collected and write to store.
         */
        Thread.sleep(60_000);

        List<TableUsageSummary> results;
        /* List all usages from slice0 */
        results = doListTableUsage(tableName, false /*isTableOcid */,
                                   slice0Start, 0, 0, 0);
        assertTrue(!results.isEmpty());
        assertUsages(results, 1, 1);

        /* List table usages with limit = 1 */
        results = doListTableUsage(tableName,  false /*isTableOcid */,
                                   slice0Start, 0, 1, 0);
        assertTrue(!results.isEmpty());
        assertUsages(results, 1, 1);

        long endTime = System.currentTimeMillis();
        /* List table usages with time range and limit = 1 */
        results = doListTableUsage(tableName, false /*isTableOcid */,
                                   slice0Start, endTime, 1, 0);
        assertTrue(!results.isEmpty());
        assertUsages(results, 1, 1);

        /* List table usages with time <= endTime and limit = 1 */
        results = doListTableUsage(tableName, false /*isTableOcid */, 0,
                                   endTime, 1, 4);
        assertTrue(!results.isEmpty());
        assertUsages(results, 1, 1);

        /* List usages using tableOcid */
        final String tableOcid = getTableId(tableName);
        results = doListTableUsage(tableOcid, true /*isTableOcid */,
                                   slice0Start, 0, 0, 0);
        assertTrue(!results.isEmpty());
        assertUsages(results, 1, 1);
    }

    @Test
    public void testTableNameMapping()
        throws Exception {

        /*
         * Run this test in minicloud only
         *
         * This test directly calls SC API to create table to test proxy cache,
         * it can only be run in minicloud.
         */
        assumeTrue("Skipping testTableNameMapping() if not minicloud test",
                   useMiniCloud);

        String tableName = "testTableNameMapping";
        String ddl = "create table " + tableName + "(" +
                     "id integer, name String, age integer, " +
                     "primary key(id))";

        /* drop non-existing table */
        dropTable(tableName);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* get table to cache mapping */
        getTable(tableName);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* get table */
        getTable(tableName);

        /* list table usage to cache mapping */
        ListTableUsageRequest.Builder builder =
                ListTableUsageRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName);
        client.listTableUsage(builder.build());

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* list table usage */
        builder = ListTableUsageRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName);
        client.listTableUsage(builder.build());

        /* alter table to cache mapping */
        String alterddl = "alter table " + tableName + "(add i1 integer)";
        UpdateTableDetails info = UpdateTableDetails.builder()
                .ddlStatement(alterddl)
                .compartmentId(getCompartmentId())
                .build();
        UpdateTableRequest utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdl(utReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* alter table */
        info = UpdateTableDetails.builder()
                .ddlStatement(alterddl)
                .compartmentId(getCompartmentId())
                .build();
        utReq = UpdateTableRequest.builder()
                .tableNameOrId(tableName)
                .updateTableDetails(info)
                .build();
        executeDdl(utReq);

        /* drop table to cache mapping */
        DeleteTableRequest req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .isIfExists(true)
                .build();
        executeDdl(req);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* drop table */
        req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .isIfExists(true)
                .build();
        executeDdl(req);
    }

    @Test
    public void testInvalidCompartmentId() {
        assumeTrue("Skipping testInvalidCompartmentId() if not minicloud test",
                   cloudRunning);

        String tableName = "testInvalidCompartmentId";
        String ddl = "create table if not exists " + tableName + "(" +
                     "id integer, name String, age integer, " +
                     "primary key(id))";

        /* Get table */
        try {
            GetTableRequest gtr = GetTableRequest.builder()
                    .tableNameOrId(tableName)
                    .build();
            client.getTable(gtr);
            fail("GetTable expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            assertTrue(ex.getMessage().contains("compartment id"));
        }

        /* Alter table */
        try {
            ddl = "alter table " + tableName + "(add i1 integer)";
            UpdateTableDetails utd = UpdateTableDetails.builder()
                    .ddlStatement(ddl)
                    .build();
            UpdateTableRequest utReq = UpdateTableRequest.builder()
                    .tableNameOrId(tableName)
                    .updateTableDetails(utd)
                    .build();
            client.updateTable(utReq);
            fail("AlterTable expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            assertTrue(ex.getMessage().contains("compartment id"));
        }

        /* List table usage */
        try {
            ListTableUsageRequest.Builder builder =
                    ListTableUsageRequest.builder()
                    .tableNameOrId(tableName);
            client.listTableUsage(builder.build());
            fail("ListTableUsage expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            assertTrue(ex.getMessage().contains("compartment id"));
        }

        /* Delete a non-exists table using if exists */
        DeleteTableRequest dtr = DeleteTableRequest.builder()
                .tableNameOrId("invalid")
                .isIfExists(true)
                .build();
        try {
            client.deleteTable(dtr);
            fail("ListTableUsage expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            assertTrue(ex.getMessage().contains("compartment id"));
        }
    }

    /**
     * Test tenant max auto scaling table count and limits mode max change per
     * day.
     */
    @Test
    public void testAutoScalingTableLimits() {
        assumeTrue("Skipping testAutoScalingTableLimits() if not minicloud " +
                   "or cloud test", cloudRunning);

        /* Create auto scaling table */
        String tableName = "testusers";
        String ddl = "create table if not exists testusers(" +
            "id integer, name string, primary key(id))";
        TableLimits limits = TableLimits.builder()
                                .maxStorageInGBs(20)
                                .capacityMode(CapacityMode.OnDemand)
                                .build();
        CreateTableDetails info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl)
                .tableLimits(limits)
                .build();
        CreateTableRequest req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdl(req);

        GetTableRequest getReq = GetTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .build();
        GetTableResponse getRes = client.getTable(getReq);
        validateTable(getRes.getTable(), tableName, limits);

        /*
         * Create 2 more auto scaling tables.
         */
        createTestTable("tableName1", limits, null, null);
        createTestTable("tableName2", limits, null, null);

        /*
         * Cannot create more than 3 auto scaling tables.
         */
        ddl = "create table if not exists tableName3(" +
            "id integer, name string, primary key(id))";
        info = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name("tableName3")
                .ddlStatement(ddl)
                .tableLimits(limits)
                .build();
        req = CreateTableRequest.builder()
                .createTableDetails(info)
                .build();
        executeDdlFail(req, "TableLimitExceeded");

        /*
         * Alter the table limits mode from AUTO_SCALING to PROVISIONED
         */
        limits = TableLimits.builder()
                    .maxReadUnits(10)
                    .maxWriteUnits(20)
                    .maxStorageInGBs(30)
                    .capacityMode(CapacityMode.Provisioned)
                    .build();
        updateTable(tableName, limits);
        getRes = client.getTable(getReq);
        validateTable(getRes.getTable(), tableName, limits);

        if (tenantLimits != null) {
            limits = null;
            if (tenantLimits.getBillingModeChangeRate() == 2) {
                /*
                 * Alter the table limits mode from PROVISIONED to AUTO_SCALING
                 */
                limits = TableLimits.builder()
                            .maxStorageInGBs(50)
                            .capacityMode(CapacityMode.OnDemand)
                            .build();
                updateTable(tableName, limits);
                getRes = client.getTable(getReq);
                validateTable(getRes.getTable(), tableName, limits);

                /*
                 * Cannot change the limits mode any more after reaching mode max
                 * allowed changes per day.
                 */
                limits = TableLimits.builder()
                            .maxReadUnits(10)
                            .maxWriteUnits(20)
                            .maxStorageInGBs(30)
                            .capacityMode(CapacityMode.Provisioned)
                            .build();
            } else if (tenantLimits.getBillingModeChangeRate() == 1) {
                limits = TableLimits.builder()
                            .maxStorageInGBs(50)
                            .capacityMode(CapacityMode.OnDemand)
                            .build();
            }

            if (limits != null) {
                UpdateTableRequest updateReq =
                    buildUpdateTableRequest(tableName, limits);
                executeDdlFail(updateReq, "OperationRateLimitExceeded");
            }
        }
    }

    @Test
    public void testRetryToken() {
        /*
         * TODO: NOSQL-718
         * Enable this in cloud test after fix it
         */
        assumeTrue("Skipping testRetryToken() if not minicloud test",
                   useMiniCloud);

        final String tableName = "testRetryToken";
        final String indexName = "idxName";
        final String ddl = "create table " + tableName +
                           "(id integer, name string, primary key(id))";
        final TableLimits limits = TableLimits.builder()
                .maxReadUnits(50)
                .maxWriteUnits(50)
                .maxStorageInGBs(1)
                .build();

        final long now = System.currentTimeMillis();
        final String createTableToken = "token_create_table_" + now;
        final String createIndexToken = "token_create_index_" + now;

        String origRequestId;
        String workRequestId;

        /*
         * Create table
         */
        CreateTableRequest createTableReq;
        CreateTableDetails createTableInfo;

        /* Create table with a retry token */
        createTableInfo = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl)
                .tableLimits(limits)
                .build();
        createTableReq = CreateTableRequest.builder()
                .createTableDetails(createTableInfo)
                .opcRetryToken(createTableToken)
                .build();
        origRequestId = executeDdl(createTableReq);
        /*
         * Create table with retry token again, should get the original request
         * id.
         */
        workRequestId = executeDdl(createTableReq, false);
        assertEquals(origRequestId, workRequestId);

        /*
         * The request does not match the original request, should get
         * 409(InvalidatedRetryToken) error.
         */
        Map<String, String> tags = new HashMap<>();
        tags.put("production", "NDCS");
        createTableInfo = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl)
                .tableLimits(limits)
                .freeformTags(tags)
                .build();
        createTableReq = CreateTableRequest.builder()
                .createTableDetails(createTableInfo)
                .opcRetryToken(createTableToken)
                .build();
        executeDdlFail(createTableReq, 409, "InvalidatedRetryToken");

        /*
         * Create index
         */
        CreateIndexRequest createIndexReq;
        CreateIndexDetails createIndexInfo;

        List<IndexKey> keys = new ArrayList<IndexKey>();
        keys.add(IndexKey.builder().columnName("name").build());
        createIndexInfo = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId(getCompartmentId())
                .keys(keys)
                .build();
        createIndexReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(createIndexInfo)
                .opcRetryToken(createIndexToken)
                .build();
        origRequestId = executeDdl(createIndexReq);
        /*
         * Create index with retry token again, should get the original request
         * id.
         */
        workRequestId = executeDdl(createIndexReq, false);
        assertEquals(origRequestId, workRequestId);

        /*
         * Creating another index with the retry token used for previous index
         * should get 409(InvalidatedRetryToken).
         */
        keys.clear();
        keys.add(IndexKey.builder().columnName("name").build());
        keys.add(IndexKey.builder().columnName("id").build());
        createIndexInfo = CreateIndexDetails.builder()
                .name("idxNameId")
                .compartmentId(getCompartmentId())
                .keys(keys)
                .build();
        createIndexReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(createIndexInfo)
                .opcRetryToken(createIndexToken)
                .build();
        executeDdlFail(createIndexReq, 409, "InvalidatedRetryToken");

        /*
         * Create index with the retry token of create-table operation, it
         * should get 409(InvalidatedRetryToken).
         */
        createIndexReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(createIndexInfo)
                .opcRetryToken(createTableToken)
                .build();
        executeDdlFail(createIndexReq, 409, "InvalidatedRetryToken");

        /* Invalid retry token */
        String invalidRetryToken1 = "abc!";
        createTableInfo = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl)
                .tableLimits(limits)
                .build();
        createTableReq = CreateTableRequest.builder()
                .createTableDetails(createTableInfo)
                .opcRetryToken(invalidRetryToken1)
                .build();
        executeDdlFail(createTableReq, 400, "InvalidParameter");

        StringBuilder sb = new StringBuilder(65);
        for (int i = 0; i < 65; i++) {
            sb.append("A");
        }
        String invalidRetryToken2 = sb.toString();
        createTableReq = CreateTableRequest.builder()
                .createTableDetails(createTableInfo)
                .opcRetryToken(invalidRetryToken2)
                .build();
        executeDdlFail(createTableReq, 400, "InvalidParameter");
    }

    @Test
    public void testUpdateTableWithCreateDdl() {
        String tableName = "foo";
        String ddl = "create table " + tableName + "(" +
                        "id integer, " +
                        "name string, " +
                        "primary key(id))";
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(10)
                .maxWriteUnits(10)
                .maxStorageInGBs(1)
                .build();
        createTable(tableName, ddl, limits);

        GetTableResponse gtRes;
        String[] pkeys = new String[] {"id"};
        Map<String, String> columns = new HashMap<String, String>();
        columns.put("id", "INTEGER");
        columns.put("name", "STRING");

        /*
         * ALTER TABLE foo(ADD info JSON)
         */
        ddl = "create table " + tableName + "(" +
                "id integer, " +
                "name string, " +
                "info json, " +
                "primary key(id))";
        alterTable(tableName, ddl);

        columns.put("info", "JSON");
        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, columns, pkeys, null, limits,
                      (cloudRunning ? ddl : null), -1 /* ttl */);

        /*
         * ALTER TABLE foo(MODIFY id GENERATED ALWAYS AS IDENTITY)
         */
        ddl = "create table " + tableName + "(" +
                "id integer generated always as identity, " +
                "name string, " +
                "info json, " +
                "primary key(id))";
        alterTable(tableName, ddl);

        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, columns, pkeys, null, limits,
                      (cloudRunning ? ddl : null), -1 /* ttl */);
        checkIdenity(gtRes.getTable().getSchema().getIdentity(),
                     "id", true /* isAlways */, false /* isOnNull */);

        /*
         * ALTER TABLE foo(ADD uid0 STRING AS UUID,
         *                 DROP name,
         *                 DROP info,
         *                 MODIFY id GENERATED ALWAYS AS IDENTITY
         *                   (START WITH 2 INCREMENT BY 2 MAXVALUE 100
         *                    CACHE 10 CYCLE))
         */
        ddl = "create table " + tableName + "(" +
                "id integer generated always as identity(" +
                    "start with 2 increment by 2 maxvalue 100 cache 10 cycle), " +
                "uid0 string as UUID, " +
                "primary key(id))";
        alterTable(tableName, ddl);

        columns.remove("name");
        columns.remove("info");
        columns.put("uid0", "STRING_UUID");
        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, columns, pkeys, null, limits,
                      (cloudRunning ? ddl : null), -1 /* ttl */);
        checkIdenity(gtRes.getTable().getSchema().getIdentity(),
                     "id", true /* isAlways */, false /* isOnNull */);


        /*
         * ALTER TABLE foo(ADD uid1 STRING AS UUID GENERATED BY DEFAULT,
         *                 ADD address RECORD(
         *                   line STRING, city STRING, zipcode STRING),
         *                 DROP uid0,
         *                 MODIFY id DROP IDENTITY)
         */
        ddl = "create table " + tableName + "(" +
                "id integer, " +
                "uid1 string as UUID GENERATED BY DEFAULT, " +
                "address record(line string, city string, zipcode string), " +
                "primary key(id))";
        alterTable(tableName, ddl);

        columns.remove("uid0");
        columns.put("uid1", "STRING_UUID_GENERATED");
        columns.put("address", "RECORD(line STRING, city STRING, zipcode STRING)");
        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, columns, pkeys, null, limits,
                      (cloudRunning ? ddl : null), -1 /* ttl */);
        assertNull(gtRes.getTable().getSchema().getIdentity());

        /*
         *  ALTER TABLE foo(ADD address.lines ARRAY(STRING),
         *                  ADD address.country STRING,
         *                  DROP address.line)
         */
        ddl = "create table " + tableName + "(" +
                "id integer, " +
                "uid1 string as UUID GENERATED BY DEFAULT, " +
                "address record(city string, zipcode string, " +
                               "lines array(string), country string), " +
                "primary key(id))";
        alterTable(tableName, ddl);

        columns.put("address",
                    "RECORD(city STRING, zipcode STRING, " +
                           "lines ARRAY(STRING), country STRING)");
        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, columns, pkeys, null, limits,
                      (cloudRunning ? ddl : null), -1 /* ttl */);

        /*
         * ALTER TABLE foo USING TTL 30 DAYS
         */
        String ddlWithTtl = ddl + " using TTL 30 days";
        alterTable(tableName, ddlWithTtl);
        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, columns, pkeys, null, limits,
                      (cloudRunning ? ddlWithTtl : null), 30 /* ttl */);

        /*
         * ALTER TABLE foo USING TTL 90 DAYS
         */
        ddlWithTtl = ddl + " using TTL 90 days";
        alterTable(tableName, ddlWithTtl);
        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, columns, pkeys, null, limits,
                      (cloudRunning ? ddlWithTtl : null), 90 /* ttl */);

        /*
         * ALTER TABLE foo USING TTL 0 DAYS
         */
        alterTable(tableName, ddl);
        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, columns, pkeys, null, limits,
                     (cloudRunning ? ddl : null), 0 /* ttl */);

        /*
         * Using semantical equivalent ddl, update-table does nothing and
         * the ddl statement of table should be changed.
         */
        ddl = "create table " + tableName + "(" +
                "id integer, " +
                "uid1 string as UUID GENERATED BY DEFAULT, " +
                "address record(city string, zipcode string, " +
                               "lines array(string), country string), " +
                "primary key(id))";
        alterTable(tableName, ddl);
        gtRes = getTable(tableName);
        validateTable(gtRes.getTable(), tableName, columns, pkeys, null, limits,
                     (cloudRunning ? ddl : null), 0 /* ttl */);

        /*
         * Bad requests
         */

        /*
         * Can't modify primary key fields
         */
        ddl = "create table if not exists " + tableName + "(" +
                "id integer, " +
                "name string, " +
                "primary key(id, name))";
        UpdateTableRequest req = buildUpdateTableRequest(tableName, ddl);
        executeDdlFail(req, 400,
                (cloudRunning ? "IllegalArgument" : "InvalidParameter"));

        /*
         * Multiple alter table operations are needed to evolve to target table,
         * only single alter table is supported at this time.
         */
        ddl = "create table if not exists " + tableName + "(" +
                "id integer, " +
                "name string, " +
                "age integer, " +
                "primary key(id)) using ttl 10 days";
        req = buildUpdateTableRequest(tableName, ddl);
        executeDdlFail(req, 400,
                (cloudRunning ? "IllegalArgument" : "InvalidParameter"));

        /* The table name 'foo' does not match the table name in statement */
        req = buildUpdateTableRequest("foo", ddl);
        executeDdlFail(req, 400,
                (cloudRunning ? "IllegalArgument" : "InvalidParameter"));

        /* Table not found */
        ddl = "create table foo1(id integer, s string, primary key(id))";
        req = buildUpdateTableRequest("foo1", ddl);
        executeDdlFail(req, 404,
                (cloudRunning ? "NotAuthorizedOrNotFound" : "TableNotFound"));
    }

    private List<TableUsageSummary> doListTableUsage(String tableNameOrId,
                                                     boolean isTableOcid,
                                                     long startTime,
                                                     long endTime,
                                                     int limit,
                                                     int maxEntries) {
        ListTableUsageRequest req;
        ListTableUsageResponse res;

        ListTableUsageRequest.Builder builder = ListTableUsageRequest.builder()
            .tableNameOrId(tableNameOrId);
        if (!isTableOcid) {
            builder.compartmentId(getCompartmentId());
        }
        if (startTime > 0) {
            builder.timeStart(new Date(startTime));
        }
        if (endTime > 0) {
            builder.timeEnd(new Date(endTime));
        }
        if (limit > 0) {
            builder.limit(limit);
        }

        String nextPage = null;
        List<TableUsageSummary> results = new ArrayList<>();
        List<TableUsageSummary> usages;
        do {
            req = builder.page(nextPage).build();
            res = client.listTableUsage(req);
            nextPage = res.getOpcNextPage();
            usages = res.getTableUsageCollection().getItems();
            if (!usages.isEmpty()) {
                results.addAll(res.getTableUsageCollection().getItems());
            }
            if (limit > 0 && nextPage != null) {
                assertEquals(limit, usages.size());
            }
            if (maxEntries > 0 && results.size() >= maxEntries) {
                break;
            }
        } while (nextPage != null);
        return results;
    }

    private void assertUsages(List<TableUsageSummary> results,
                              int expWriteUnits,
                              int expReadUnits) {
        int writeUnits = 0;
        int readUnits = 0;
        for (TableUsageSummary tus : results) {

            assertNotNull(tus.getWriteUnits());
            assertNotNull(tus.getReadUnits());
            assertNotNull(tus.getReadThrottleCount());
            assertNotNull(tus.getWriteThrottleCount());
            assertNotNull(tus.getStorageThrottleCount());
            assertNotNull(tus.getStorageInGBs());
            assertNotNull(tus.getMaxShardSizeUsageInPercent());

            if (tus.getWriteUnits() > 0) {
                writeUnits += tus.getWriteUnits();
            }
            if (tus.getReadUnits() > 0) {
                readUnits += tus.getReadUnits();
            }
        }
        assertEquals(expWriteUnits, writeUnits);
        assertEquals(expReadUnits, readUnits);
    }

    private void createTestTable(String tableName,
                                 TableLimits limits,
                                 Map<String, String> freeformTags,
                                 Map<String, Map<String, Object>> definedTags) {
        String ddl = "create table if not exists " + tableName + "(" +
                        "id integer, " +
                        "name string, " +
                        "age integer, " +
                        "primary key(id))";

        /* Create table */
        CreateTableDetails ctInfo = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl)
                .tableLimits(limits)
                .freeformTags(freeformTags)
                .definedTags(definedTags)
                .build();
        CreateTableRequest ctReq = CreateTableRequest.builder()
                .createTableDetails(ctInfo)
                .build();
        executeDdl(ctReq);
    }

    private void validateTable(Table table,
                               String tableName,
                               TableLimits limits) {
        validateTable(table, tableName, null /* columns */,
                      null /* primaryKeys */, null /* shardKeys */,
                      limits, null /* tableDdl */, -1 /* ttlInDays */);
    }

    private void validateTable(Table table,
                               String tableName,
                               Map<String, String> columns,
                               String[] primaryKeys,
                               String[] shardKeys,
                               TableLimits limits,
                               String tableDdl,
                               int ttlInDays) {
        assertNotNull(table);

        assertTableOcid(table.getId());

        assertEquals(getCompartmentId(), table.getCompartmentId());
        assertEquals(tableName, table.getName());
        assertNotNull(table.getTimeCreated());
        assertNotNull(table.getDdlStatement());

        Schema schema = table.getSchema();
        assertNotNull(schema);

        if (columns != null) {
            List<Column> cols = schema.getColumns();
            assertEquals(columns.size(), cols.size());
            for (Column col : cols) {
                String name = col.getName();
                assertTrue(columns.containsKey(name));
                String type = columns.get(name).toUpperCase();
                if (type.startsWith("STRING")) {
                    assertEquals(type.contains("UUID"), col.getIsAsUuid());
                    assertEquals(type.contains("GENERATED"),
                                 col.getIsGenerated());
                    type = "STRING";
                }
                assertTrue(type.equalsIgnoreCase(col.getType()));

                if (schema.getPrimaryKey().contains(name)) {
                    assertFalse(col.getIsNullable());
                } else {
                    assertTrue(col.getIsNullable());
                }
            }
        }

        if (primaryKeys != null) {
            assertEquals(primaryKeys.length, schema.getPrimaryKey().size());
            int i = 0;
            for (String key : schema.getPrimaryKey()) {
                assertTrue(key.equalsIgnoreCase(primaryKeys[i++]));
            }

            String[] skeys = (shardKeys != null) ? shardKeys : primaryKeys;
            assertEquals(skeys.length, schema.getShardKey().size());
            i = 0;
            for (String key : schema.getShardKey()) {
                assertTrue(key.equalsIgnoreCase(skeys[i++]));
            }
        }

        if (limits != null) {
            TableLimits resLimits = table.getTableLimits();
            assertEquals(limits.getMaxStorageInGBs(),
                         resLimits.getMaxStorageInGBs());
            if (limits.getCapacityMode() == null ||
                limits.getCapacityMode() == CapacityMode.Provisioned) {
                assertEquals(CapacityMode.Provisioned,
                             resLimits.getCapacityMode());
                assertEquals(limits.getMaxReadUnits(),
                             resLimits.getMaxReadUnits());
                assertEquals(limits.getMaxWriteUnits(),
                             resLimits.getMaxWriteUnits());
            } else {
                assertEquals(CapacityMode.OnDemand,
                             resLimits.getCapacityMode());
            }
        }

        if (tableDdl != null) {
            assertEquals(tableDdl, table.getDdlStatement());
        }

        Integer ttl = table.getSchema().getTtl();
        if (ttlInDays >= 0) {
            assertNotNull(ttl);
            assertEquals(ttlInDays, ttl.intValue());
        } else {
            assertNull(ttl);
        }

        /* TODO: more validation */
    }

    private void checkIdenity(Identity identity,
                              String colName,
                              boolean isAlways,
                              boolean isOnNull) {
        if (checkKVVersion(22, 2, 7)) {
            assertNotNull(identity);
            assertEquals(colName, identity.getColumnName());
            assertEquals(isAlways, identity.getIsAlways());
            assertEquals(isOnNull, identity.getIsNull());
        }
    }
}
