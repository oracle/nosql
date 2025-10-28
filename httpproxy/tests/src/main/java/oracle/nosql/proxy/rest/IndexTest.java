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
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.model.CreateIndexDetails;
import com.oracle.bmc.nosql.model.Index;
import com.oracle.bmc.nosql.model.IndexKey;
import com.oracle.bmc.nosql.model.IndexSummary;
import com.oracle.bmc.nosql.requests.CreateIndexRequest;
import com.oracle.bmc.nosql.requests.DeleteIndexRequest;
import com.oracle.bmc.nosql.requests.GetIndexRequest;
import com.oracle.bmc.nosql.requests.ListIndexesRequest;
import com.oracle.bmc.nosql.requests.ListIndexesRequest.LifecycleState;
import com.oracle.bmc.nosql.responses.GetIndexResponse;
import com.oracle.bmc.nosql.responses.ListIndexesResponse;

/**
 * Indexes related APIs:
 *   o create index
 *   o drop index
 *   o get index
 *   o get indexes
 */
public class IndexTest extends RestAPITestBase {

    @Test
    public void testIndexBasic() {
        final String tableName = "testIndexBasic";
        final String indexName = "idxNamePhoneAge";
        final String ddl = "create table " + tableName + "(" +
                        "id integer, " +
                        "name string, " +
                        "age integer, " +
                        "info json, " +
                        "primary key(id))";

        createTable(tableName, ddl);

        /* Create Index */
        List<IndexKey> keys = new ArrayList<IndexKey>();
        keys.add(IndexKey.builder().columnName("name").build());
        keys.add(IndexKey.builder()
                    .columnName("info")
                    .jsonPath("phone")
                    .jsonFieldType("string")
                    .build());
        keys.add(IndexKey.builder().columnName("age").build());
        keys.add(IndexKey.builder()
                    .columnName("info")
                    .jsonPath("address[].city.id")
                    .jsonFieldType("Integer")
                    .build());

        CreateIndexDetails info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId(getCompartmentId())
                .keys(keys)
                .build();
        CreateIndexRequest ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdl(ciReq);

        /* Get Index */
        GetIndexRequest giReq = GetIndexRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .indexName(indexName)
                .build();
        GetIndexResponse giRes = client.getIndex(giReq);
        Index index = giRes.getIndex();
        assertNotNull(index);
        assertEquals(indexName, index.getName());
        assertEquals(getCompartmentId(), index.getCompartmentId());
        assertEquals(tableName, index.getTableName());
        assertIndexKeys(keys, index.getKeys());

        /* Create Index with if not exists */
        info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId(getCompartmentId())
                .keys(keys)
                .isIfNotExists(true)
                .build();
        ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdl(ciReq);

        /* Create Index but index already exists, get IndexAlreadyExists error */
        info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId(getCompartmentId())
                .keys(keys)
                .build();
        ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdlFail(ciReq, "IndexAlreadyExists");

        /* Drop Index */
        DeleteIndexRequest diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .build();
        executeDdl(diReq);

        /* Get Index, Index not found (404, NotAuthorizedOrNotFound, false) */
        try {
            giRes = client.getIndex(giReq);
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
        }

        /* Drop Index with if not exists */
        diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .isIfExists(true)
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .build();
        executeDdl(diReq);

        /* Drop Index but index not exists, get 404 (Index not found) error */
        diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .isIfExists(false)
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .build();
        executeDdlFail(diReq, "IndexNotFound");
    }

    @Test
    public void testIndexBasicWithTableOcid() {

        /* Run this test for minicloud only */
        assumeTrue("Skipping testIndexBasicWithTableOcid() if not minicloud " +
                   "test", cloudRunning);

        final String tableName = "testIndexBasicWithTableOcid";
        final String indexName = "idxNamePhoneAge";
        final String ddl = "create table " + tableName + "(" +
                        "id integer, " +
                        "name string, " +
                        "age integer, " +
                        "info json, " +
                        "primary key(id))";
        createTable(tableName, ddl);

        final String tableOcid = getTableId(tableName);

        /* Create Index */
        List<IndexKey> keys = new ArrayList<IndexKey>();
        keys.add(IndexKey.builder().columnName("name").build());
        keys.add(IndexKey.builder()
                    .columnName("info")
                    .jsonPath("phone")
                    .jsonFieldType("string")
                    .build());
        keys.add(IndexKey.builder().columnName("age").build());
        keys.add(IndexKey.builder()
                    .columnName("info")
                    .jsonPath("address[].city.id")
                    .jsonFieldType("Integer")
                    .build());

        CreateIndexDetails info = CreateIndexDetails.builder()
                .name(indexName)
                .keys(keys)
                .build();
        CreateIndexRequest ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableOcid)
                .createIndexDetails(info)
                .build();
        executeDdl(ciReq);

        /* Get Index */
        GetIndexRequest giReq = GetIndexRequest.builder()
                .tableNameOrId(tableOcid)
                .indexName(indexName)
                .build();
        GetIndexResponse giRes = client.getIndex(giReq);
        Index index = giRes.getIndex();
        assertNotNull(index);
        assertEquals(indexName, index.getName());
        assertEquals(getCompartmentId(), index.getCompartmentId());
        assertEquals(tableName, index.getTableName());
        assertIndexKeys(keys, index.getKeys());

        /* Create Index with if not exists */
        info = CreateIndexDetails.builder()
                .name(indexName)
                .keys(keys)
                .isIfNotExists(true)
                .build();
        ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableOcid)
                .createIndexDetails(info)
                .build();
        executeDdl(ciReq);

        /* Create Index but index already exists, get IndexAlreadyExists error */
        info = CreateIndexDetails.builder()
                .name(indexName)
                .keys(keys)
                .build();
        ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableOcid)
                .createIndexDetails(info)
                .build();
        executeDdlFail(ciReq, "IndexAlreadyExists");

        /* Drop Index */
        DeleteIndexRequest diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .tableNameOrId(tableOcid)
                .build();
        executeDdl(diReq);

        /* Get Index, Index not found (404, NotAuthorizedOrNotFound, false) */
        try {
            giRes = client.getIndex(giReq);
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
        }

        /* Drop Index with if not exists */
        diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .isIfExists(true)
                .tableNameOrId(tableOcid)
                .build();
        executeDdl(diReq);

        /* Drop Index but index not exists, get 404 (Index not found) error */
        diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .isIfExists(false)
                .tableNameOrId(tableOcid)
                .build();
        executeDdlFail(diReq, "IndexNotFound");
    }

    @Test
    public void testIndexNonExistentTableOcid() {

        /* Run this test for minicloud only */
        assumeTrue("Skipping testIndexNonExistentTableOcid() if not minicloud " +
                   "test", cloudRunning);

        String tableName = "testIndexNonExistentTableOcid";
        String ddl = "create table if not exists " + tableName + "(" +
                     "id integer, name String, age integer, " +
                     "primary key(id))";
        /* Create table */
        createTable(tableName, ddl);

        /* fake non-existent table ocid */
        String tableOcid = getTableId(tableName) + "notexist";

        /* Create Index */
        List<IndexKey> keys = new ArrayList<IndexKey>();
        String indexName = "idx_name";
        keys.add(IndexKey.builder().columnName("name").build());
        try {
            createIndex(tableOcid, "idx", keys, true /* wait */);
            fail("CreateIndex expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
        }

        /* Get Index */
        try {
            GetIndexRequest giReq = GetIndexRequest.builder()
                    .tableNameOrId(tableOcid)
                    .indexName(indexName)
                    .build();
            client.getIndex(giReq);
            fail("GetIndex expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
        }

        /* Delete Index */
        try {
            DeleteIndexRequest diReq = DeleteIndexRequest.builder()
                    .indexName(indexName)
                    .tableNameOrId(tableOcid)
                    .build();
            executeDdl(diReq);
            fail("DeleteIndex expect table-not-found but not");
        } catch (BmcException ex) {
            assertEquals(404 /* table not found */, ex.getStatusCode());
        }
    }

    @Test
    public void testCreateIndexBadRequest() {

        final String tableName = "foo";
        final String indexName = "idxName";
        List<IndexKey> keys = new ArrayList<IndexKey>();

        CreateIndexDetails info;
        CreateIndexRequest req;

        /* Invalid name: name should not be empty or contain white space only */
        info = CreateIndexDetails.builder()
                .name(" ")
                .compartmentId(getCompartmentId())
                .keys(keys)
                .build();
        req = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdlFail(req, 400 /* bad request */, "InvalidParameter");

        /* Invalid name: name should not be empty or contain white space only */
        info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId("")
                .keys(keys)
                .build();
        req = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdlFail(req, 400 /* bad request */, "InvalidParameter");

        /* Invalid keys, it should not be empty */
        info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId(getCompartmentId())
                .keys(keys)
                .build();
        req = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdlFail(req, 400 /* bad request */, "InvalidParameter");

        /* Invalid IndexKey */
        keys.clear();
        keys.add(IndexKey.builder()
                    .jsonPath("info.phone")
                    .jsonFieldType("string")
                    .build());
        executeDdlFail(req, 400 /* bad request */, "InvalidParameter");

        keys.clear();
        keys.add(IndexKey.builder()
                    .columnName("name")
                    .jsonPath("info.phone")
                    .build());
        executeDdlFail(req, 400 /* bad request */, "InvalidParameter");

        keys.clear();
        keys.add(IndexKey.builder()
                    .columnName("name")
                    .jsonFieldType("string")
                    .build());
        executeDdlFail(req, 400 /* bad request */, "InvalidParameter");

        /* Table not found */
        keys.clear();
        keys.add(IndexKey.builder().columnName("name").build());
        info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId(getCompartmentId())
                .keys(keys)
                .build();
        req = CreateIndexRequest.builder()
                .tableNameOrId("invalid")
                .createIndexDetails(info)
                .build();
        if (cloudRunning) {
            executeDdlFail(req, 404, "NotAuthorizedOrNotFound");
        } else {
            executeDdlFail(req, 404, "TableNotFound");
        }
    }

    @Test
    public void testDropIndexWithMatchETag() {

        /* Run this test for minicloud only */
        assumeTrue("Skipping testDropIndexWithMatchETag() if not minicloud " +
                   "test", cloudRunning);

        final String tableName = "foo";
        final String indexName = "idxNamePhoneAge";
        final String ddl = "create table " + tableName + "(" +
                        "id integer, " +
                        "name string, " +
                        "age integer, " +
                        "info json, " +
                        "primary key(id))";

        createTable(tableName, ddl);

        /* Create Index */
        List<IndexKey> keys = new ArrayList<IndexKey>();
        keys.add(IndexKey.builder().columnName("name").build());
        CreateIndexDetails info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId(getCompartmentId())
                .keys(keys)
                .build();
        CreateIndexRequest ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdl(ciReq);

        /* Get index info and current ETag */
        GetIndexResponse giRes = getIndex(tableName, indexName);
        String currentETag = giRes.getEtag();

        /* Delete the index with ETag */
        DeleteIndexRequest diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .ifMatch(currentETag)
                .build();
        executeDdl(diReq);

        /* Create index again with different columns */
        keys.clear();
        keys.add(IndexKey.builder().columnName("name").build());
        keys.add(IndexKey.builder().columnName("age").build());
        info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId(getCompartmentId())
                .keys(keys)
                .build();
        ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdl(ciReq);

        /* Get index info and current ETag */
        giRes = getIndex(tableName, indexName);
        String oldETag = currentETag;
        currentETag = giRes.getEtag();

        /* Delete the index with mismatched ETag, get ETagMismatch error */
        diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .ifMatch(oldETag)
                .build();
        executeDdlFail(diReq, "ETagMismatch");

        /* Delete the index with ETag, expect to succeed */
        diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .ifMatch(currentETag)
                .build();
        executeDdl(diReq);

        /* Get Index, Index not found (404, NotAuthorizedOrNotFound, false) */
        try {
            giRes = getIndex(tableName, indexName);
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
        }

        /*
         * Delete the index with ifExists and mismatched ETag, expect to succeed
         *
         * For delete-if-exists, if index does not exists the ETag will be
         * ignored, it expects to succeed.
         */
        diReq = DeleteIndexRequest.builder()
                .indexName(indexName)
                .compartmentId(getCompartmentId())
                .tableNameOrId(tableName)
                .isIfExists(true)
                .ifMatch(oldETag)
                .build();
        executeDdl(diReq);
    }

    @Test
    public void testGetIndexes() {
        final String tableName = "foo";
        String ddl = "create table if not exists " + tableName + "(" +
                "id integer, " +
                "name string, " +
                "age integer, " +
                "zipCode string, " +
                "primary key(id))";
        createTable(tableName, ddl);

        /* List all indexes, no index exists */
        List<IndexSummary> indexes =
            runGetIndexes(tableName,
                          false /* isTableOcid */,
                          null /* namePattern */,
                          null /* LifecycleState */,
                          null /* SortBy */,
                          null /* SortOrder */,
                          0    /* limit */);
        assertEquals(0, indexes.size());

        List<String> timeCreatedAsc = new ArrayList<String>();

        /* Create Indexes */
        List<IndexKey> keys = new ArrayList<IndexKey>();
        String indexName = "idx_name";
        keys.add(IndexKey.builder().columnName("name").build());
        createIndexAndVerify(tableName, indexName, keys);
        timeCreatedAsc.add(indexName);

        indexName = "idx_name_age";
        keys.clear();
        keys.add(IndexKey.builder().columnName("name").build());
        keys.add(IndexKey.builder().columnName("age").build());
        createIndexAndVerify(tableName, indexName, keys);
        timeCreatedAsc.add(indexName);

        indexName = "idx_zipcode";
        keys.clear();
        keys.add(IndexKey.builder().columnName("zipcode").build());
        createIndexAndVerify(tableName, indexName, keys);
        timeCreatedAsc.add(indexName);

        indexName = "idx_name_zipcode";
        keys.clear();
        keys.add(IndexKey.builder().columnName("name").build());
        keys.add(IndexKey.builder().columnName("zipcode").build());
        createIndexAndVerify(tableName, indexName, keys);
        timeCreatedAsc.add(indexName);

        List<String> timeCreatedDesc = new ArrayList<String>(timeCreatedAsc);
        Collections.reverse(timeCreatedDesc);
        List<String> nameAsc = new ArrayList<String>(timeCreatedAsc);
        Collections.sort(nameAsc);

        /* List all indexes */
        indexes = runGetIndexes(tableName,
                                false /* isTableOcid */,
                                null /* namePattern */,
                                null /* LifecycleState */,
                                null /* SortBy */,
                                null /* SortOrder */,
                                0    /* limit */);
        assertEquals(timeCreatedAsc.size(), indexes.size());
        if (cloudRunning) {
            assertSortedIndex(indexes, timeCreatedDesc);
        }

        /* List all indexes */
        indexes = runGetIndexes(tableName,
                                false /* isTableOcid */,
                                null /* namePattern */,
                                null /* LifecycleState */,
                                null /* SortBy */,
                                null /* SortOrder */,
                                1    /* limit */);
        assertEquals(timeCreatedAsc.size(), indexes.size());

        /* Below tests for advanced parameters are for minicloud only:
         *  1. Filtering with namePattern/start
         *  2. Sorting by timeCreated or name */
        if (!cloudRunning) {
            return;
        }

        /* List those indexes whose name's prefix is idx_name */
        String namePattern = "*_name*";
        indexes = runGetIndexes(tableName,
                                false /* isTableOcid */,
                                namePattern,
                                null /* LifecycleState */,
                                null /* SortBy */,
                                null /* SortOrder */,
                                0    /* limit */);
        assertEquals(3, indexes.size());
        for (IndexSummary index : indexes) {
            assertTrue(index.getName().startsWith("idx_name"));
        }

        /* No matched index, should return 0 index. */
        indexes = runGetIndexes(tableName,
                                false /* isTableOcid */,
                                "*not*",
                                null /* LifecycleState */,
                                null /* SortBy */,
                                null /* SortOrder */,
                                0    /* limit */);
        assertEquals(0, indexes.size());

        /* List indexes filtered by state */
        indexes = runGetIndexes(tableName,
                                false /* isTableOcid */,
                                null,
                                LifecycleState.All,
                                null /* SortBy */,
                                null /* SortOrder */,
                                0    /* limit */);
        assertEquals(4, indexes.size());

        indexes = runGetIndexes(tableName,
                                false /* isTableOcid */,
                                null,
                                LifecycleState.Creating,
                                null /* SortBy */,
                                null /* SortOrder */,
                                0    /* limit */);
        assertEquals(0, indexes.size());

        /* List indexes sorted by createdTime asc */
        indexes = runGetIndexes(tableName,
                                false /* isTableOcid */,
                                null /* namePattern */,
                                LifecycleState.Active,
                                ListIndexesRequest.SortBy.TimeCreated,
                                ListIndexesRequest.SortOrder.Asc,
                                1);
        assertEquals(4, indexes.size());
        assertSortedIndex(indexes, timeCreatedAsc);

        /* List indexes sorted by createdTime desc */
        indexes = runGetIndexes(tableName,
                                false /* isTableOcid */,
                                null /* namePattern */,
                                null /* LifecycleState */,
                                ListIndexesRequest.SortBy.TimeCreated,
                                ListIndexesRequest.SortOrder.Desc,
                                2);
        assertEquals(4, indexes.size());
        assertSortedIndex(indexes, timeCreatedDesc);

        /* List indexes sorted by name desc */
        indexes = runGetIndexes(tableName,
                                false /* isTableOcid */,
                                null /* namePattern */,
                                null /* LifecycleState */,
                                ListIndexesRequest.SortBy.Name,
                                ListIndexesRequest.SortOrder.Asc,
                                3);
        assertEquals(4, indexes.size());
        assertSortedIndex(indexes, nameAsc);

        /* List indexes using tableOcid, for miniCloud test only */
        if (cloudRunning) {
            String tableOcid = getTableId(tableName);
            indexes = runGetIndexes(tableOcid,
                                    true /* isTableOcid */,
                                    null /* namePattern */,
                                    null /* LifecycleState */,
                                    null /* SortBy */,
                                    null /* SortOrder */,
                                    0    /* limit */);
            assertEquals(timeCreatedAsc.size(), indexes.size());
        }
    }

    @Test
    public void testIndexTableNameMapping()
        throws Exception {

        /*
         * Run this test in minicloud only
         *
         * This test bypasses proxy and call SC API to create table to test
         * proxy cache, it can only be run against minicloud.
         */
        assumeTrue("Skipping testIndexTableNameMapping() if not minicloud " +
                   "test", useMiniCloud);

        String tableName = "testIndexTableNameMapping";
        String ddl = "create table " + tableName + "(" +
                     "id integer, name String, age integer, " +
                     "primary key(id))";

        /* drop non-existing table */
        dropTable(tableName);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* Create Index to cache mapping */
        List<IndexKey> keys = new ArrayList<IndexKey>();
        keys.add(IndexKey.builder().columnName("name").build());
        CreateIndexDetails info = CreateIndexDetails.builder()
                .compartmentId(getCompartmentId())
                .name("idx1")
                .keys(keys)
                .build();
        CreateIndexRequest ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdl(ciReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* Create Index */
        keys = new ArrayList<IndexKey>();
        keys.add(IndexKey.builder().columnName("name").build());
        info = CreateIndexDetails.builder()
                .compartmentId(getCompartmentId())
                .name("idx1")
                .keys(keys)
                .build();
        ciReq = CreateIndexRequest.builder()
                .tableNameOrId(tableName)
                .createIndexDetails(info)
                .build();
        executeDdl(ciReq);

        /* Get Index to cache mapping */
        GetIndexRequest giReq = GetIndexRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .indexName("idx1")
                .build();
         client.getIndex(giReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* Get Index */
        giReq = GetIndexRequest.builder()
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .indexName("idx1")
                .build();
        try {
            client.getIndex(giReq);
            fail("expected IndexNotFound");
        } catch (BmcException be) {
            assertEquals(be.getStatusCode(), 404);
            assertEquals(be.getServiceCode(), "IndexNotFound");
        }

        /* Drop Index to cache mapping */
        DeleteIndexRequest diReq = DeleteIndexRequest.builder()
                .indexName("idx1")
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .build();
        client.deleteIndex(diReq);

        /* re-create table */
        scRecreateTable(getTenantId(), getCompartmentId(), tableName, ddl);

        /* Drop Index */
        diReq = DeleteIndexRequest.builder()
                .indexName("idx1")
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .build();
        client.deleteIndex(diReq);
    }

    @Test
    public void testIndexInvalidCompartmentId() {

        /* Run this test for minicloud only */
        assumeTrue("Skipping testIndexInvalidCompartmentId() if not minicloud " +
                   "test", cloudRunning);

        String tableName = "testIndexInvalidCompartmentId";
        /* Create Index */
        List<IndexKey> keys = new ArrayList<IndexKey>();
        String indexName = "idx_name";
        keys.add(IndexKey.builder().columnName("name").build());
        try {
            CreateIndexDetails info = CreateIndexDetails.builder()
                    .name(indexName)
                    .keys(keys)
                    .build();
            CreateIndexRequest req = CreateIndexRequest.builder()
                    .tableNameOrId(tableName)
                    .createIndexDetails(info)
                    .build();
            client.createIndex(req);
            fail("CreateIndex expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            assertTrue(ex.getMessage().contains("compartment id"));
        }

        /* Get Index */
        try {
            GetIndexRequest giReq = GetIndexRequest.builder()
                    .tableNameOrId(tableName)
                    .indexName(indexName)
                    .build();
            client.getIndex(giReq);
            fail("GetIndex expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            assertTrue(ex.getMessage().contains("compartment id"));
        }

        /* Delete Index */
        try {
            DeleteIndexRequest diReq = DeleteIndexRequest.builder()
                    .indexName(indexName)
                    .tableNameOrId(tableName)
                    .build();
            executeDdl(diReq);
            fail("DeleteIndex expect 404 but not");
        } catch (BmcException ex) {
            assertEquals(404 , ex.getStatusCode());
            assertTrue(ex.getMessage().contains("compartment id"));
        }
    }

    private void assertSortedIndex(List<IndexSummary> indexes,
                                   List<String> expSorted) {
        assertEquals(expSorted.size(), indexes.size());
        for (int i = 0; i < indexes.size(); i++) {
            assertEquals(expSorted.get(i), indexes.get(i).getName());
        }
    }

    private void createIndexAndVerify(String tableName,
                                      String indexName,
                                      List<IndexKey> keys) {
        createIndex(tableName, indexName, keys, true /* wait */);

        GetIndexResponse res = getIndex(tableName, indexName);
        Index index = res.getIndex();
        assertIndexKeys(keys, index.getKeys());
        assertEquals(Index.LifecycleState.Active, index.getLifecycleState());
    }

    private GetIndexResponse getIndex(String tableName, String indexName) {
        GetIndexRequest req = GetIndexRequest.builder()
                .indexName(indexName)
                .tableNameOrId(tableName)
                .compartmentId(getCompartmentId())
                .build();
        return client.getIndex(req);
    }

    private List<IndexSummary>
        runGetIndexes(String tableNameOrId,
                      boolean isTableOcid,
                      String namePattern,
                      ListIndexesRequest.LifecycleState state,
                      ListIndexesRequest.SortBy sortBy,
                      ListIndexesRequest.SortOrder sortOrder,
                      int limit) {

        ListIndexesRequest.Builder builder = ListIndexesRequest.builder()
                .tableNameOrId(tableNameOrId)
                .limit(limit);
        if (!isTableOcid) {
            builder.compartmentId(getCompartmentId());
        }
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

        ListIndexesResponse liRes;
        String page = null;
        List<IndexSummary> indexes = new ArrayList<>();

        while (true) {
            if (page != null) {
                builder.page(page);
            }

            liRes = client.listIndexes(builder.build());
            assertNotNull(liRes);

            List<IndexSummary> items = liRes.getIndexCollection().getItems();
            if (limit > 0) {
                assertTrue(items.size() <= limit);
            }
            indexes.addAll(items);
            page = liRes.getOpcNextPage();
            if (page == null) {
                break;
            }
        }
        return indexes;
    }

    private void assertIndexKeys(List<IndexKey> expKeys, List<IndexKey> keys) {
        assertEquals(expKeys.size(), keys.size());
        int i = 0;
        for (IndexKey key : keys) {
            assertIndexKey(expKeys.get(i++), key);
        }
    }

    private void assertIndexKey(IndexKey expKey, IndexKey key) {
        assertEquals(expKey.getColumnName(), key.getColumnName());
        if (expKey.getJsonPath() == null) {
            assertNull(key.getJsonPath());
            assertNull(key.getJsonFieldType());
        } else {
            assertEquals(expKey.getJsonPath(), key.getJsonPath());
            assertTrue(expKey.getJsonFieldType()
                        .equalsIgnoreCase(key.getJsonFieldType()));
        }
    }
}
