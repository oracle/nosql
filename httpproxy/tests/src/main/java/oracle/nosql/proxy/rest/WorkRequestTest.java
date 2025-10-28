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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.model.TableLimits;
import com.oracle.bmc.nosql.model.WorkRequest;
import com.oracle.bmc.nosql.model.WorkRequest.OperationType;
import com.oracle.bmc.nosql.model.WorkRequest.Status;
import com.oracle.bmc.nosql.model.WorkRequestError;
import com.oracle.bmc.nosql.model.WorkRequestLogEntry;
import com.oracle.bmc.nosql.model.WorkRequestResource;
import com.oracle.bmc.nosql.model.WorkRequestResource.ActionType;
import com.oracle.bmc.nosql.model.WorkRequestSummary;
import com.oracle.bmc.nosql.requests.DeleteWorkRequestRequest;
import com.oracle.bmc.nosql.requests.GetWorkRequestRequest;
import com.oracle.bmc.nosql.requests.ListWorkRequestErrorsRequest;
import com.oracle.bmc.nosql.requests.ListWorkRequestLogsRequest;
import com.oracle.bmc.nosql.requests.ListWorkRequestsRequest;
import com.oracle.bmc.nosql.responses.GetWorkRequestResponse;
import com.oracle.bmc.nosql.responses.ListWorkRequestErrorsResponse;
import com.oracle.bmc.nosql.responses.ListWorkRequestLogsResponse;
import com.oracle.bmc.nosql.responses.ListWorkRequestsResponse;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test suite is only for miniCloud test.
 *
 * WorkRequest related APIs:
 *   o getWorkRequest
 *   o listWorkRequests
 *   o listWorkRequestLogs
 *   o listWorkRequestErrors
 *   o cancelWorkRequest(NYI)
 */
public class WorkRequestTest extends RestAPITestBase {

    @BeforeClass
    public static void staticSetUp() throws Exception {
        Assume.assumeTrue(
            "Skipping WorkRequestTest if not minicloud or cloud test",
            Boolean.getBoolean(USEMC_PROP) ||
            Boolean.getBoolean(USECLOUD_PROP));

        RestAPITestBase.staticSetUp();
    }

    @Test
    public void testInvalidWorkRequest() {
        String invalidWorkRequestId =
            "ocid1.nosqltableworkrequest.oc1.iad.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        try {
            GetWorkRequestRequest req = GetWorkRequestRequest.builder()
                    .workRequestId(invalidWorkRequestId)
                    .build();
            client.getWorkRequest(req);
        } catch (BmcException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals(ErrorCode.RESOURCE_NOT_FOUND.getErrorCode(),
                         e.getServiceCode());
        }

        try {
            ListWorkRequestErrorsRequest lwrer = ListWorkRequestErrorsRequest
                    .builder().workRequestId(invalidWorkRequestId).build();
            client.listWorkRequestErrors(lwrer);
        } catch (BmcException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals(ErrorCode.RESOURCE_NOT_FOUND.getErrorCode(),
                         e.getServiceCode());
        }

        try {
            ListWorkRequestLogsRequest lwrlr = ListWorkRequestLogsRequest
                    .builder().workRequestId(invalidWorkRequestId).build();
            client.listWorkRequestLogs(lwrlr);
        } catch (BmcException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals(ErrorCode.RESOURCE_NOT_FOUND.getErrorCode(),
                         e.getServiceCode());
        }

        try {
            DeleteWorkRequestRequest dwrr = DeleteWorkRequestRequest
                    .builder().workRequestId(invalidWorkRequestId).build();
            client.deleteWorkRequest(dwrr);
        } catch (BmcException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals(ErrorCode.RESOURCE_NOT_FOUND.getErrorCode(),
                         e.getServiceCode());
        }
    }

    @Test
    public void testGetWorkRequest() {
        final String tableName = "testGetWorkRequest";
        String workReqId;

        String ddl = genCreateTableDdl(tableName);
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(100)
                .maxWriteUnits(100)
                .maxStorageInGBs(1)
                .build();

        /* create table */
        workReqId = createTable(tableName, ddl, limits, false);
        checkWorkRequest(workReqId,
                         getCompartmentId(),
                         tableName,
                         OperationType.CreateTable);

        /* create index */
        workReqId = createIndex(tableName, "idxNameAge",
                                new String[] {"name", "age"},
                                false /* ifNotExists */,
                                false /* wait */);
        checkWorkRequest(workReqId,
                          getCompartmentId(),
                          tableName,
                          OperationType.UpdateTable);

        /* alter table */
        ddl = genAlterTableDdl(tableName);
        workReqId = alterTable(tableName, ddl, false /* wait */);
        checkWorkRequest(workReqId,
                         getCompartmentId(),
                         tableName,
                         OperationType.UpdateTable);

        /* alter tableLimits */
        limits = TableLimits.builder()
                .maxReadUnits(200)
                .maxWriteUnits(200)
                .maxStorageInGBs(2)
                .build();
        workReqId = updateTable(tableName, limits, false /* wait */);
        checkWorkRequest(workReqId,
                         getCompartmentId(),
                         tableName,
                         OperationType.UpdateTable);

        /* drop table */
        workReqId = dropTable(tableName,
                              false /* isIfExists */,
                              false /* wait */);
        checkWorkRequest(workReqId,
                         getCompartmentId(),
                         tableName,
                         OperationType.DeleteTable);
    }

    @Test
    public void testListWorkRequestLogError() {
        final String tableName = "testListWorkRequestLogError";
        String workReqId;

        String ddl = genCreateTableDdl(tableName);
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(100)
                .maxWriteUnits(100)
                .maxStorageInGBs(1)
                .build();

        /* create table */
        workReqId = createTable(tableName, ddl, limits, false);
        checkWorkRequest(workReqId,
                          getCompartmentId(),
                          tableName,
                          OperationType.CreateTable);

        /* Get workRequest Log */
        getWorkRequestLog(workReqId);
        getWorkRequestError(workReqId, false /* expFail */, null);

        ddl = "alter table " + tableName + "(add date Timestamp)";
        /* Get workRequest Error */
        workReqId = alterTable(tableName, ddl, false);
        waitForStatus(workReqId, WorkRequest.Status.Failed);
        /* Get workRequest error */
        getWorkRequestError(workReqId, true /* expFail */, "IllegalArgument");
        getWorkRequestLog(workReqId);

        /* To verify that the error message should not contain table ocid */
        ddl = "alter table " + tableName + "(drop id)";
        workReqId = alterTable(tableName, ddl, false);
        waitForStatus(workReqId, WorkRequest.Status.Failed);
        getWorkRequestError(workReqId, true /* expFail */, "IllegalArgument");
    }

    @Test
    public void testCancelWorkRequest() {
        final String tableName = "testCancelWorkRequest";
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(100)
                .maxWriteUnits(100)
                .maxStorageInGBs(1)
                .build();
        String ddl = genCreateTableDdl(tableName);
        createTable(tableName, ddl, limits);

        /* Create index */
        String workReqId = createIndex(tableName, "idx1",
                                       new String[]{"name"},
                                       false /* ifNotExists */,
                                       false /* wait */);
        /*
         * Cancel the work request of create index, expect to get
         * 404/CantCancelWorkRequest
         */
        DeleteWorkRequestRequest req = DeleteWorkRequestRequest.builder()
                .workRequestId(workReqId)
                .build();
        try {
            client.deleteWorkRequest(req);
            fail("Expect to get 404/CantCancelWorkRequest but not");
        } catch (BmcException ex) {
            assertEquals(404, ex.getStatusCode());
            assertEquals(ex.getServiceCode(),
                         ErrorCode.CANNOT_CANCEL_WORK_REQUEST.getErrorCode());
        }
    }

    private void getWorkRequestLog(String workRequestId) {
        ListWorkRequestLogsRequest req;
        ListWorkRequestLogsResponse res;
        WorkRequestLogEntry entry;

        /* Get workRequest Log */
        req = ListWorkRequestLogsRequest.builder()
                    .workRequestId(workRequestId)
                    .build();
        res = client.listWorkRequestLogs(req);
        assertNull(res.getOpcNextPage());
        assertNotNull(res.getWorkRequestLogEntryCollection().getItems());
        assertEquals(1, res.getWorkRequestLogEntryCollection()
                            .getItems().size());
        entry = res.getWorkRequestLogEntryCollection().getItems().get(0);
        assertNotNull(entry);
        assertNotNull(entry.getTimestamp());
    }

    private void getWorkRequestError(String workRequestId,
                                     boolean expFail,
                                     String expErrCode) {
        ListWorkRequestErrorsRequest req;
        ListWorkRequestErrorsResponse res;
        WorkRequestError entry;

        /* Get workRequest error */
        req = ListWorkRequestErrorsRequest.builder()
                    .workRequestId(workRequestId)
                    .build();
        res = client.listWorkRequestErrors(req);

        assertNull(res.getOpcNextPage());
        assertNotNull(res.getWorkRequestErrorCollection().getItems());
        if (expFail) {
            assertEquals(1, res.getWorkRequestErrorCollection()
                    .getItems().size());
            entry = res.getWorkRequestErrorCollection().getItems().get(0);
            assertNotNull(entry);
            if (expErrCode != null) {
                assertEquals(expErrCode, entry.getCode());
            }
            assertNotNull(entry.getMessage());
            /*
             * TODO: enable this after move proxy to 5.4.9
             * assertTrue(!entry.getMessage().contains("ocid1_nosqltable_"));
             */
            assertNotNull(entry.getTimestamp());
        } else {
            assertTrue(res.getWorkRequestErrorCollection()
                        .getItems().isEmpty());
        }
    }

    @Test
    public void testListWorkRequest() {
        final String tableName = "testListWorkRequest";

        List<String> workReqIds = new ArrayList<>();
        TableLimits limits = TableLimits.builder()
                .maxReadUnits(100)
                .maxWriteUnits(100)
                .maxStorageInGBs(1)
                .build();

        /* Executes 4 ddl operations for each table */
        String ddl = genCreateTableDdl(tableName);
        workReqIds.add(createTable(tableName, ddl, limits, false /* wait */));
        workReqIds.add(createIndex(tableName, "idxName",
                                   new String[] {"name"},
                                   false /* ifNotExists */,
                                   false /* wait */));

        ddl = genAlterTableDdl(tableName);
        workReqIds.add(alterTable(tableName, ddl, false /* wait */));
        workReqIds.add(dropIndex(tableName, "idxName", false /* wait */));

        List<String> reverseIds = new ArrayList<>(workReqIds);
        Collections.reverse(reverseIds);

        /* List workRequests */
        List<WorkRequestSummary> results;
        results = listWorkRequests(getCompartmentId(), 2,  4);
        assertEquals(4, results.size());
        int i = 0;
        for (WorkRequestSummary wrs : results) {
            assertEquals(reverseIds.get(i++), wrs.getId());
        }

        /* List workRequests again after the above workRequests complete */
        for (String workReqId : workReqIds) {
            waitForComplete(workReqId);
        }

        results = listWorkRequests(getCompartmentId(), 4, 4);
        assertEquals(4, results.size());
        i = 0;
        for (WorkRequestSummary wrs : results) {
            assertEquals(reverseIds.get(i++), wrs.getId());
            assertEquals(Float.valueOf(100), wrs.getPercentComplete());
            WorkRequestResource table = wrs.getResources().get(0);
            assertNotNull(table);
            assertTableOcid(table.getIdentifier());
        }
    }

    private List<WorkRequestSummary> listWorkRequests(String cmptId,
                                                      int limit,
                                                      int stopCount) {
        ListWorkRequestsRequest req;
        ListWorkRequestsResponse res;
        String nextPage = null;
        int count = 0;
        List<WorkRequestSummary> results = new ArrayList<>();
        while(true) {
            req = ListWorkRequestsRequest.builder()
                    .compartmentId(cmptId)
                    .page(nextPage)
                    .limit(limit)
                    .build();

            res = client.listWorkRequests(req);

            int num = res.getWorkRequestCollection().getItems().size();
            results.addAll(res.getWorkRequestCollection().getItems());
            count += num;
            nextPage = res.getOpcNextPage();
            if (nextPage == null || count >= stopCount) {
                break;
            }
        }
        return results;
    }

    private void checkWorkRequest(String workRequestId,
                                  String cmptId,
                                  String tableName,
                                  OperationType opType) {

        GetWorkRequestRequest req = GetWorkRequestRequest.builder()
                .workRequestId(workRequestId)
                .build();

        GetWorkRequestResponse res;
        while (true) {
            res = client.getWorkRequest(req);

            WorkRequest workReq = res.getWorkRequest();
            assertEquals(opType, workReq.getOperationType());
            assertEquals(cmptId, workReq.getCompartmentId());
            assertEquals(workRequestId, workReq.getId());
            assertNotNull(workReq.getTimeAccepted());

            List<WorkRequestResource> resources = workReq.getResources();
            assertNotNull(resources);
            assertEquals(1, resources.size());
            WorkRequestResource resource = resources.get(0);
            assertEquals("TABLE", resource.getEntityType());
            assertNotNull(resource.getIdentifier());
            assertTableOcid(resource.getIdentifier());
            assertNotNull(resource.getEntityUri());
            assertTrue(resource.getEntityUri().contains(tableName));
            Status status = workReq.getStatus();
            if (status == Status.InProgress) {
                assertEquals(ActionType.InProgress, resource.getActionType());
                assertNotNull(workReq.getTimeStarted());
                assertNull(workReq.getTimeFinished());
                assertTrue(workReq.getPercentComplete() < 100);
            } else if (status == Status.Succeeded ||
                       status == Status.Failed) {
                assertEquals(getActionType(opType), resource.getActionType());
                assertNotNull(workReq.getTimeStarted());
                assertNotNull(workReq.getTimeFinished());
                assertEquals(Float.valueOf(100f), workReq.getPercentComplete());
                break;
            }

            if (useCloudService) {
                /*
                 * In cloud test, sleep 250ms to avoid too frequent
                 * get-work-request calls
                 */
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private ActionType getActionType(OperationType opType) {
        switch(opType) {
        case CreateTable:
            return ActionType.Created;
        case DeleteTable:
            return ActionType.Deleted;
        case UpdateTable:
            return ActionType.Updated;
        default:
            fail("Unexpected OperationType: " + opType);
        }
        return null;
    }

    private static String genCreateTableDdl(String tableName) {
        return "create table if not exists " + tableName +
                "(id integer, name string, age integer, primary key(id))";
    }

    private static String genAlterTableDdl(String tableName) {
        return "alter table " + tableName + "(add status string)";
    }
}
