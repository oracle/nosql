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
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.model.DeleteRowResult;
import com.oracle.bmc.nosql.model.RequestUsage;
import com.oracle.bmc.nosql.model.Row;
import com.oracle.bmc.nosql.model.UpdateRowDetails;
import com.oracle.bmc.nosql.model.UpdateRowDetails.Option;
import com.oracle.bmc.nosql.model.UpdateRowResult;
import com.oracle.bmc.nosql.requests.DeleteRowRequest;
import com.oracle.bmc.nosql.requests.GetRowRequest;
import com.oracle.bmc.nosql.requests.UpdateRowRequest;
import com.oracle.bmc.nosql.responses.DeleteRowResponse;
import com.oracle.bmc.nosql.responses.GetRowResponse;
import com.oracle.bmc.nosql.responses.UpdateRowResponse;

import oracle.kv.Version;
import oracle.nosql.common.json.JsonUtils;

/**
 * Test simple CRUD APIs:
 *  o put
 *  o get
 *  o delete
 */
public class RowTest extends RestAPITestBase {

    private final String tableName = "foo";
    private String tableDdl = "create table if not exists " + tableName +
                              "(id integer, name String, age integer, " +
                              "primary key(id))";

    @Test
    public void testPut() {

        createTable(tableName, tableDdl);

        UpdateRowRequest req;
        UpdateRowResponse res;

        Map<String, Object> value = createValue(1);

        /*
         * Test isExactMatch, default = false
         */
        value.put("notExistsField", 1);
        UpdateRowDetails row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        res = client.updateRow(req);
        checkUpdateRowResponse(res, true, null, null);

        /*
         * Set isExactMatch = false
         */
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .isExactMatch(false)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        res = client.updateRow(req);
        checkUpdateRowResponse(res, true, null, null);
        String etag = res.getEtag();

        /*
         * Test etag
         */
        value = createValue(1);
        value.put("name", value.get("name") + "_upd");
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .ifMatch(etag)
                .build();
        res = client.updateRow(req);
        etag = res.getEtag();
        checkUpdateRowResponse(res, true, null, null);

        /* verify row */
        List<String> key = new ArrayList<String>();
        key.add("id:1");
        GetRowRequest getReq = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        GetRowResponse getRet = client.getRow(getReq);
        checkGetRowResponse(getRet, value);
        assertETagEquals(etag, getRet.getEtag());

        /*
         * Invalid comparmentId, compartmentId should not be empty or contain
         * white space only
         */
        row = UpdateRowDetails.builder()
                .compartmentId(" ")
                .value(value)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        updateRowFail(req, 400 /* bad request */);

        /*
         * Invalid value, it should not be empty.
         */
        value.clear();
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        updateRowFail(req, 400 /* bad request */);

        /*
         * Invalid identityCacheSize, it should not be negative value.
         */
        value = createValue(0);
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .identityCacheSize(-1)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        updateRowFail(req, 400 /* bad request */);

        /*
         * Invalid ttl, it should not be negative value.
         */
        value = createValue(0);
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .ttl(-1)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        updateRowFail(req, 400 /* bad request */);

        /*
         * Invalid timeoutInMs, it should not be negative value.
         */
        value = createValue(0);
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .timeoutInMs(-1)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        updateRowFail(req, 400 /* bad request */);

        /*
         * Invalid matchVersion.
         */
        value = createValue(0);
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .ifMatch("invalid")
                .build();
        updateRowFail(req, 400 /* bad request */);

        /*
         * Table not found.
         */
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId("notFound")
                .updateRowDetails(row)
                .build();
        updateRowFail(req, 404 /* Table not found */);

        /*
         * isExactMatch = true, row contains unexpected field
         */
        value.put("notExistsField", 1);
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .isExactMatch(true)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        updateRowFail(req, 400 /* bad request */);

        /*
         * Invalid value, missing primary key field
         */
        value = createValue(1);
        value.remove("id");
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        req = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        updateRowFail(req, 400 /* bad request */);
    }

    private void updateRowFail(UpdateRowRequest req, int expCode) {
        try {
            client.updateRow(req);
            fail("expect to fail but not");
        } catch (BmcException ex) {
            assertEquals(expCode , ex.getStatusCode());
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testPutWithOption() {

        createTable(tableName, tableDdl);

        /* Put a row */
        Map<String, Object> value = createValue(0);
        UpdateRowDetails row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        UpdateRowResponse putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet);

        /* PutIfAbsent failed */
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .option(Option.IfAbsent)
                .value(value)
                .build();
        putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet, false, null, null);

        /* PutIfAbsent with returnRow failed. */
        Map<String, Object> prevValue = value;
        String prevVersion = putRet.getUpdateRowResult().getVersion();

        value = createValue(0);
        value.put("name", value.get("name") + "_upd");
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .option(Option.IfAbsent)
                .value(value)
                .isGetReturnRow(true)
                .build();
        putReq = UpdateRowRequest.builder()
                    .tableNameOrId(tableName)
                    .updateRowDetails(row)
                    .build();
        putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet, prevValue, prevVersion);

        /* PutIfAbsent OK */
        value = createValue(1);
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .option(Option.IfAbsent)
                .value(value)
                .build();
        putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet);

        /* PutIfPresent OK */
        value.put("name", value.get("name") + "_upd");
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .option(Option.IfPresent)
                .value(value)
                .build();
        putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet);
        prevValue = value;
        prevVersion = putRet.getUpdateRowResult().getVersion();

        /* PutIfPresent failed */
        value = createValue(2);
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .option(Option.IfPresent)
                .value(value)
                .build();
        putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet, false, null, null);

        /* PutIfVersion OK */
        value = prevValue;
        value.put("name", value.get("name")+ "_putifVersion");
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .ifMatch(prevVersion)
                .updateRowDetails(row)
                .build();
        putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet);

        int id = ((Integer)value.get("id")).intValue();
        Map<String, Object> exp = getRow(tableName, createKey(id));
        assertEquals(exp, value);

        /*
         * PutIfVersion with unmatched version, get 412 error
         */

        prevValue = exp;
        String oldPrevVersion = prevVersion;
        prevVersion = putRet.getUpdateRowResult().getVersion();
        value.put("name", value.get("name")+ "_putifVersion_failed");
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .isGetReturnRow(true)
                .build();
        putReq = UpdateRowRequest.builder()
                    .tableNameOrId(tableName)
                    .ifMatch(oldPrevVersion)
                    .updateRowDetails(row)
                    .build();
        updateRowFail(putReq, 412 /* Precondition Failed */);
    }

    @Test
    public void testTTL() {
        String tableName = "foo";
        String ddl = "create table if not exists " + tableName + "(" +
                        "id integer, name String, age integer, " +
                        "primary key(id)) " +
                        "using ttl 2 days";

        /* Create table */
        createTable(tableName, ddl);

        /* Put row */
        int id = 0;
        Map<String, Object> value = createValue(id);
        UpdateRowDetails row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        UpdateRowResponse putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet, true, null, null);

        /* Get row and check ttl */
        List<String> key = createKey(id);
        GetRowRequest getReq = GetRowRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .key(key)
                .build();
        GetRowResponse getRet = client.getRow(getReq);
        checkGetRowResponse(getRet, value);
        Date defExpTime = getRet.getRow().getTimeOfExpiration();
        assertTrue(defExpTime!= null);

        /* Put row with TTL = 3 &&  isTtlUseTableDefault(false) */
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .ttl(3)
                .isTtlUseTableDefault(false)
                .build();
        putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet, true, null, null);

        getRet = client.getRow(getReq);
        checkGetRowResponse(getRet, value);
        Date expTime = getRet.getRow().getTimeOfExpiration();
        assertTrue(expTime.getTime() > defExpTime.getTime());

        /* Put row with TTL = 4 && isTtlUseTableDefault(true) */
        row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .ttl(4)
                .isTtlUseTableDefault(true)
                .build();
        putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet, true, null, null);

        getRet = client.getRow(getReq);
        checkGetRowResponse(getRet, value);
        expTime = getRet.getRow().getTimeOfExpiration();
        assertTrue(expTime.getTime() == defExpTime.getTime());
    }

    @Test
    public void testIdentityValue() {
        String tableName = "foo";
        String ddl = "create table if not exists " + tableName + "(" +
                          "id integer generated always as identity, " +
                          "name String, " +
                          "age integer, " +
                          "primary key(id))";

        /* Create table */
        createTable(tableName, ddl);

        /* Put row */
        Map<String, Object> value = new HashMap<String, Object>();
        value.put("name", "jack");
        value.put("age", 21);
        UpdateRowDetails row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        UpdateRowResponse putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet, true, null, null);
        assertEquals("1", putRet.getUpdateRowResult().getGeneratedValue());
    }

    @Test
    public void testDelete() {

        createTable(tableName, tableDdl);

        /* Put a row */
        int id = 1;
        String version = putRow(tableName, createValue(id));

        /* Delete a row */
        List<String> key = createKey(id);
        DeleteRowRequest req = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        DeleteRowResponse res = client.deleteRow(req);
        checkDeleteRowResponse(res);

        /* Delete again, failed */
        res = client.deleteRow(req);
        checkDeleteRowResponse(res, false, null, null);

        /*
         * DeleteIfVersion with old version, get 412 error
         */
        String matchVersion = version;
        version = putRow(tableName, createValue(id));

        req = DeleteRowRequest.builder()
                    .tableNameOrId(tableName)
                    .compartmentId(getCompartmentId())
                    .ifMatch(matchVersion)
                    .isGetReturnRow(true)
                    .key(key)
                    .build();
        deleteRowFail(req, 412 /* Precondition Failed */);

        /* DeleteIfVersion OK */
        matchVersion = version;
        req = DeleteRowRequest.builder()
                    .tableNameOrId(tableName)
                    .compartmentId(getCompartmentId())
                    .ifMatch(version)
                    .isGetReturnRow(true)
                    .key(key)
                    .build();
        res = client.deleteRow(req);
        checkDeleteRowResponse(res, true, null, null);

        /* Invalid key */
        key.clear();
        req = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        deleteRowFail(req, 400 /* bad request */);

        /* Invalid key: key element should not be empty */
        key.add("id:1");
        key.add("");
        req = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        deleteRowFail(req, 400 /* bad request */);

        /* Invalid timeoutInMs, it should not be negative */
        key.clear();
        key.add("id:1");
        req = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .timeoutInMs(-1)
                .key(key)
                .build();
        deleteRowFail(req, 400 /* bad request */);

        /* Table not found */
        key.clear();
        key.add("id:1");
        req = DeleteRowRequest.builder()
                .tableNameOrId("invalid")
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        deleteRowFail(req, 404 /* Table not found */);

        /* Invalid primary key, invalid format 'column-name:value' */
        key.clear();
        key.add("id1");
        req = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        deleteRowFail(req, 400 /* bad request */);

        /* Invalid primary key, invalid type */
        key.clear();
        key.add("id:test");
        req = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        deleteRowFail(req, 400 /* bad request */);

        /* Invalid primary key, field not exist */
        key.clear();
        key.add("id:1");
        key.add("invalid:1");
        req = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        deleteRowFail(req, 400 /* bad request */);
    }

    private void deleteRowFail(DeleteRowRequest req, int expCode) {
        try {
            client.deleteRow(req);
            fail("expect to fail but not");
        } catch (BmcException ex) {
            assertEquals(expCode , ex.getStatusCode());
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testCaseInsensitive() {

        createTable(tableName, tableDdl);

        Map<String, Object> value = new HashMap<String, Object>();
        value.put("ID", 0);
        value.put("NaMe", "name0");
        value.put("AgE", 20);

        UpdateRowDetails row = UpdateRowDetails.builder()
                .compartmentId(getCompartmentId())
                .value(value)
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        UpdateRowResponse putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet);

        Map<String, Object> retVal = getRow(tableName, createKey(0));
        assertEquals(createValue(0), retVal);
    }

    @Test
    public void testGet() {
        final String tableName = "foo";
        String ddl = "create table if not exists " + tableName + "(" +
                "k1 string, k2 integer, name String, age integer, " +
                "primary key(shard(k1), k2))";
        /* Create table */
        createTable(tableName, ddl);

        /* Put a row */
        Map<String, Object> value = new HashMap<String, Object>();
        value.put("k1", "t1");
        value.put("k2", 1);
        value.put("name", "name1");
        value.put("age", 20);
        String version = putRow(tableName, value);

        GetRowRequest req;
        GetRowResponse ret;

        /* Get row */
        List<String> key = new ArrayList<String>();
        key.add("k1:t1");
        key.add("k2:1");
        req = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        ret = client.getRow(req);
        checkGetRowResponse(ret, value);
        assertETagEquals(version, ret.getEtag());

        /* Get row but not exists */
        String opcRequestId = "get-req-1";
        key.clear();
        key.add("k1:t2");
        key.add("k2:1");
        req = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .opcRequestId(opcRequestId)
                .build();
        ret = client.getRow(req);
        checkGetRowResponse(ret, null);
        assertNull(ret.getRow().getValue());
        assertEquals(opcRequestId, ret.getOpcRequestId());

        /* Test the value of key field contains ":" */
        value.clear();
        value.put("k1", "t2:id");
        value.put("k2", 2);
        value.put("name", "name2");
        value.put("age", 21);
        version = putRow(tableName, value);

        key.clear();
        key.add("k1:t2:id");
        key.add("k2:2");
        req = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        ret = client.getRow(req);
        checkGetRowResponse(ret, value);
        assertETagEquals(version, ret.getEtag());

        /* Invalid key */
        key.clear();
        req = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        getRowFail(req, 400 /* bad request */);

        /* Invalid key: key element should not be empty */
        key.add("k1:1");
        key.add("");
        req = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        getRowFail(req, 400 /* bad request */);

        /* Invalid timeoutInMs, it should not be negative */
        key.clear();
        key.add("k1:t1");
        key.add("k2:1");
        req = GetRowRequest.builder()
                    .tableNameOrId(tableName)
                    .compartmentId(getCompartmentId())
                    .timeoutInMs(-1)
                    .key(key)
                    .build();
        getRowFail(req, 400 /* bad request */);

        /* Table not found */
        key.clear();
        key.add("k1:1");
        req = GetRowRequest.builder()
                .tableNameOrId("invalid")
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        getRowFail(req, 404 /* Table not found */);

        /* Invalid primary key, invalid format 'column-name:value' */
        key.clear();
        key.add("k1");
        req = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        getRowFail(req, 400 /* bad request */);

        /* Invalid primary key, invalid type */
        key.clear();
        key.add("k1:t1");
        key.add("k2:abc");
        req = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        getRowFail(req, 400 /* bad request */);

        /* Invalid primary key, field not exist */
        key.clear();
        key.add("k1:t1");
        key.add("invalid:1");
        req = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        getRowFail(req, 400 /* bad request */);

        /* Invalid primary key, miss primary key field */
        key.clear();
        key.add("k1:t1");
        req = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        getRowFail(req, 400 /* bad request */);
    }

    /**
     * Test put/get/delete row using tableOcid.
     */
    @Test
    public void testWithTableOcid() {
        if (!cloudRunning) {
            return;
        }

        createTable(tableName, tableDdl);

        final String tableOcid = getTableId(tableName);

        /* Put a row */
        Map<String, Object> value = createValue(0);
        UpdateRowDetails row = UpdateRowDetails.builder()
                .value(value)
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableOcid)
                .updateRowDetails(row)
                .build();
        UpdateRowResponse putRet = client.updateRow(putReq);
        checkUpdateRowResponse(putRet);

        /* Get row */
        List<String> key = new ArrayList<String>();
        key.add("id:0");
        GetRowRequest gtReq = GetRowRequest.builder()
                .tableNameOrId(tableOcid)
                .key(key)
                .build();
        GetRowResponse gtRet = client.getRow(gtReq);
        checkGetRowResponse(gtRet, value);

        /* Delete row */
        DeleteRowRequest delReq = DeleteRowRequest.builder()
                .tableNameOrId(tableOcid)
                .key(key)
                .build();
        DeleteRowResponse delRet = client.deleteRow(delReq);
        checkDeleteRowResponse(delRet);
    }

    @Test
    public void testRowNonExistentTableOcid() {
        if (!cloudRunning) {
            return;
        }

        String tableName = "testIndexNonExistentTableOcid";
        String ddl = "create table if not exists " + tableName + "(" +
                     "id integer, name String, age integer, " +
                     "primary key(id))";
        /* Create table */
        createTable(tableName, ddl);

        /* fake non-existent table ocid */
        String tableOcid = getTableId(tableName) + "notexist";

        /* Put a row */
        Map<String, Object> value = createValue(0);
        UpdateRowDetails row = UpdateRowDetails.builder()
                .value(value)
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableOcid)
                .updateRowDetails(row)
                .build();
        try {
            client.updateRow(putReq);
            fail("UpdateRow expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
        }

        /* Get row */
        List<String> key = new ArrayList<String>();
        key.add("id:0");
        GetRowRequest gtReq = GetRowRequest.builder()
                .tableNameOrId(tableOcid)
                .key(key)
                .build();
        try {
            client.getRow(gtReq);
            fail("GetRow expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
        }

        /* Delete row */
        DeleteRowRequest delReq = DeleteRowRequest.builder()
                .tableNameOrId(tableOcid)
                .key(key)
                .build();
        try {
            client.deleteRow(delReq);
            fail("DeleteRow expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
        }
    }

    @Test
    public void testRowTableNameMapping()
        throws Exception {

        /*
         * Run this test for minicloud only
         *
         * This test directly calls SC api to create table to test proxy cache,
         * it can only be run in minicloud.
         */
        assumeTrue("Skipping testRowTableNameMapping() if not minicloud test",
                   useMiniCloud);

        String tableName = "testRowTableNameMapping";
        String ddl = "create table " + tableName + "(" +
                     "id integer, name String, age integer, " +
                     "primary key(id))";
        String ddl2 = "create table " + tableName + "(" +
                      "id1 integer, name String, age integer, " +
                      "primary key(id1))";

        /* drop non-existing table */
        dropTable(tableName);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* Put a row to see if it can fetch table info correctly */
        Map<String, Object> value = createValue(0);
        UpdateRowDetails row = UpdateRowDetails.builder()
                .value(value)
                .compartmentId(getCompartmentId())
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        client.updateRow(putReq);
        client.updateRow(putReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl2);

        /* Put a row */
        value = new HashMap<String, Object>();
        value.put("id1", 2);
        value.put("name", "name2");
        row = UpdateRowDetails.builder()
                .value(value)
                .compartmentId(getCompartmentId())
                .build();
        putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        client.updateRow(putReq);

        /* Get a row to cache mapping */
        List<String> key = new ArrayList<String>();
        key.add("id1:0");
        GetRowRequest gtReq = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        client.getRow(gtReq);
        client.getRow(gtReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* Get row */
        key = new ArrayList<String>();
        key.add("id:0");
        gtReq = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        client.getRow(gtReq);

        /* Delete row to cache mapping */
        DeleteRowRequest delReq = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        client.deleteRow(delReq);
        client.deleteRow(delReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl2);

        /* Delete row */
        key = new ArrayList<String>();
        key.add("id1:0");
        delReq = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .key(key)
                .build();
        client.deleteRow(delReq);
    }

    @Test
    public void testRowInvalidCompartmentId() {
        String tableName = "testRowInvalidCompartmentId";
        /* Put a row */
        Map<String, Object> value = createValue(0);
        UpdateRowDetails row = UpdateRowDetails.builder()
                .value(value)
                .build();
        UpdateRowRequest putReq = UpdateRowRequest.builder()
                .tableNameOrId(tableName)
                .updateRowDetails(row)
                .build();
        try {
            client.updateRow(putReq);
            fail("UpdateRow expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            if (cloudRunning) {
                assertTrue(ex.getMessage().contains("compartment id"));
            }
        }

        /* Get row */
        List<String> key = new ArrayList<String>();
        key.add("id:0");
        GetRowRequest gtReq = GetRowRequest.builder()
                .tableNameOrId(tableName)
                .key(key)
                .build();
        try {
            client.getRow(gtReq);
            fail("GetRow expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            if (cloudRunning) {
                assertTrue(ex.getMessage().contains("compartment id"));
            }
        }

        /* Delete row */
        DeleteRowRequest delReq = DeleteRowRequest.builder()
                .tableNameOrId(tableName)
                .key(key)
                .build();
        try {
            client.deleteRow(delReq);
            fail("DeleteRow expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404 , ex.getStatusCode());
            if (cloudRunning) {
                assertTrue(ex.getMessage().contains("compartment id"));
            }
        }
    }

    private void getRowFail(GetRowRequest req, int expCode) {
        try {
            client.getRow(req);
            fail("expect to fail but not");
        } catch (BmcException ex) {
            assertEquals(expCode , ex.getStatusCode());
            checkErrorMessage(ex);
        }
    }

    private Map<String, Object> createValue(int i) {
        Map<String, Object> value = new HashMap<String, Object>();
        value.put("id", i);
        value.put("name", "name" + i);
        value.put("age", 20 + i % 40);
        return value;
    }

    private List<String> createKey(int i) {
        List<String> key = new ArrayList<String>();
        key.add("id:" + i);
        return key;
    }

    private void checkUpdateRowResponse(UpdateRowResponse res) {
        checkUpdateRowResponse(res, true, null, null);
    }

    private void checkUpdateRowResponse(UpdateRowResponse res,
                                        Map<String, Object> prevRow,
                                        String prevVersion) {
        checkUpdateRowResponse(res, false, prevRow, prevVersion);
    }

    private void checkUpdateRowResponse(UpdateRowResponse res,
                                        boolean succeed,
                                        Map<String, Object> prevRow,
                                        String prevVersion) {
        assertNotNull(res);

        UpdateRowResult result = res.getUpdateRowResult();
        assertNotNull(result);

        RequestUsage usage = result.getUsage();
        assertNotNull(usage);
        if (succeed) {
            assertNotNull(result.getVersion());
            assertTrue(usage.getWriteUnitsConsumed() > 0);
            assertNotNull(res.getEtag());
        } else {
            assertNull(result.getVersion());
            assertTrue(usage.getWriteUnitsConsumed() == 0);
            assertNull(res.getEtag());
        }

        if (prevRow != null) {
            assertEquals(prevRow, result.getExistingValue());
            assertTrue(usage.getReadUnitsConsumed() > 0);
        }
        if (prevVersion != null) {
            assertEquals(prevVersion, result.getExistingVersion());
        }
    }

    private void checkGetRowResponse(GetRowResponse res,
                                     Map<String, Object> exp) {
        assertNotNull(res);
        assertNotNull(res.getOpcRequestId());

        Row row = res.getRow();
        assertNotNull(row);

        RequestUsage usage = row.getUsage();
        assertNotNull(usage);
        assertTrue(usage.getReadUnitsConsumed() > 0);
        assertTrue(usage.getWriteUnitsConsumed() == 0);

        if (row.getValue() != null) {
            assertNotNull(res.getEtag());
        }
        if (exp != null) {
            assertEquals(exp, row.getValue());
        }
    }

    private void checkDeleteRowResponse(DeleteRowResponse res) {
        checkDeleteRowResponse(res, true, null, null);
    }

    private void checkDeleteRowResponse(DeleteRowResponse res,
                                        boolean succeed,
                                        Map<String, Object> prevValue,
                                        String prevVersion) {
        assertNotNull(res);

        DeleteRowResult result = res.getDeleteRowResult();
        assertNotNull(result);
        assertTrue(result.getIsSuccess() == succeed);

        RequestUsage usage = result.getUsage();
        assertNotNull(usage);
        if (succeed) {
            assertTrue(usage.getWriteUnitsConsumed() > 0);
        } else {
            assertTrue(usage.getWriteUnitsConsumed() == 0);
        }
        assertTrue(usage.getReadUnitsConsumed() > 0);

        if (prevValue != null) {
            assertEquals(prevValue, result.getExistingValue());
            assertEquals(prevVersion, result.getExistingVersion());
        }
    }

    private void assertETagEquals(String exp, String etag) {
        Version v0 = Version.fromByteArray(JsonUtils.decodeBase64(exp));
        Version v1 = Version.fromByteArray(JsonUtils.decodeBase64(etag));
        assertEquals(v0.getRepGroupUUID(), v1.getRepGroupUUID());
        assertEquals(v0.getVLSN(), v1.getVLSN());
    }
}
