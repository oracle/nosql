/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.streamservice;

import static oracle.kv.impl.admin.MRTManager.CLEANING_PASS_INTERVAL_MS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.FaultException;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.streamservice.MRT.Manager;
import oracle.kv.impl.streamservice.MRT.Request;
import oracle.kv.impl.streamservice.MRT.Request.CreateRegion;
import oracle.kv.impl.streamservice.MRT.Request.CreateTable;
import oracle.kv.impl.streamservice.MRT.Request.DropRegion;
import oracle.kv.impl.streamservice.MRT.Request.DropTable;
import oracle.kv.impl.streamservice.MRT.Request.UpdateTable;
import oracle.kv.impl.streamservice.MRT.Response;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.table.TableAPI;

import com.sleepycat.je.utilint.StoppableThread;

/**
 * Multi-region table test service.
 */
public class MRTTestService extends Manager {

    private volatile boolean traceOn;

    private static final int DEFAULT_AGENT_GROUP_SIZE = 1;

    private static final NoSQLSubscriberId TEST_AGENT_ID =
        new NoSQLSubscriberId(1, 0);
    private static class MRTInfo {
        private final int metatdataSeqNum;

        MRTInfo(int metadataSeqNum) {
            this.metatdataSeqNum = metadataSeqNum;
        }
    }

    private final TableAPI tableAPI;
    private final Map<Integer, Region> regionMap = new HashMap<>();

    /**
     * Map of table ID -> tables that have been "processed". The info object
     * contains the table MD and the sequence number last seen for the table.
     */
    private final ConcurrentMap<Long, MRTInfo> mrTables =
        new ConcurrentHashMap<>();

    private RequestPollingThread pollingThread;

    /* If set the next request will respond with a ERROR response */
    private volatile boolean failRequest = false;

    public MRTTestService(TableAPI tableAPI, Logger logger) {
        super(logger);
        this.tableAPI = tableAPI;
        traceOn = false;
    }

    @Override
    protected short getMaxSerialVersion() {
        return SerialVersion.CURRENT;
    }

    public void failRequest() {
        failRequest = true;
    }

    public void turnOnTrace() {
        traceOn = true;
    }

    /* -- Request handling -- */

    public void startPolling() {
        if (pollingThread != null) {
            return;
        }
        pollingThread = new RequestPollingThread();
        pollingThread.start();
    }

    public void stopPolling() {
        if (pollingThread == null) {
            return;
        }
        pollingThread.shutdownThread(logger);
        pollingThread = null;
    }

    protected void trace(String msg) {
        if (!traceOn) {
            return;
        }
        System.err.println(msg);
    }

    private String prefix(int id) {
        return "[req=" + id + "] ";
    }

    /**
     * Handles a request. Returns a response to be posted or null.
     */
    private Response handleRequest(Request request) {
        /*
         * Check if the request has been handled and if so, skip it.
         */
        final Response response = getResponse(request.getRequestId());
        if (response != null) {
            return null;
        }

        if (failRequest) {
            failRequest = false;
            final Response resp = createErrorResponse(request.getRequestId(),
                                                      "test injected failure");
            trace("Fail response=" + resp);
            return resp;
        }

        final int id = request.getRequestId();
        trace(prefix(id) + "To process new request=" + request);
        Response resp = null;
        try {
            switch (request.getType()) {
                case CREATE_TABLE:
                    resp = handleCreateTable((CreateTable)request);
                    break;
                case UPDATE_TABLE:
                    resp = handleUpdateTable((UpdateTable)request);
                    break;
                case DROP_TABLE:
                    resp = handleDropTable((DropTable)request);
                    break;
                case CREATE_REGION:
                    resp = handleCreateRegion((CreateRegion)request);
                    break;
                case DROP_REGION:
                    resp = handleDropRegion((DropRegion)request);
                    break;
                default : fail("Unexpected message type " + request.getType());
            }
        } catch (Exception e) {
            resp = createErrorResponse(request.getRequestId(), e.getMessage());
        }
        return resp;
    }

    private Response handleCreateTable(CreateTable createTable) {
        final TableImpl table = createTable.getTable();
        checkMRT(table);

        /* Check if already seen this request */
        final MRTInfo info = mrTables.get(table.getId());
        if (info != null) {
            return createSuccResponse(createTable.getRequestId());
        }
        return newTable(createTable.getRequestId(), table,
                        createTable.getMetadataSeqNum());
    }

    private void checkMRT(TableImpl table) {
        if (!table.isMultiRegion()) {
            throw new IllegalStateException("Table " + table.getName() +
                                            " is not a multi-region table");
        }
    }

    private Response handleUpdateTable(UpdateTable updateTable) {
        final TableImpl table = updateTable.getTable();
        final long tableId = table.getId();

        /* If no longer a MRT - remove it. */
        if (!table.isMultiRegion()) {
            return removeMRT(updateTable.getRequestId(), tableId);
        }

        /* New table? */
        final MRTInfo oldInfo = mrTables.get(table.getId());
        if (oldInfo == null) {
            return newTable(updateTable.getRequestId(),
                            table, updateTable.getMetadataSeqNum());
        }

        /* Seen this request already? */
        if (oldInfo.metatdataSeqNum >= updateTable.getMetadataSeqNum()) {
            return createSuccResponse(updateTable.getRequestId());
        }

        /* Do update */
        final MRTInfo newInfo = new MRTInfo(updateTable.getMetadataSeqNum());
        mrTables.put(table.getId(), newInfo);
        return createSuccResponse(updateTable.getRequestId());
    }

    private Response handleDropTable(DropTable dropTable) {
        final long tableId = dropTable.getTableId();

        final MRTInfo oldInfo = mrTables.get(tableId);
        return oldInfo == null ? createSuccResponse(dropTable.getRequestId()) :
                                 removeMRT(dropTable.getRequestId(), tableId);
    }

     private Response newTable(int requestId,
                               TableImpl table, int metadataSeqNum) {
        final MRTInfo info = new MRTInfo(metadataSeqNum);
        mrTables.put(table.getId(), info);
        return createSuccResponse(requestId);
    }

    private Response removeMRT(int requestId, long tableId) {
        mrTables.remove(tableId);
        return createSuccResponse(requestId);
    }

    private Response handleCreateRegion(CreateRegion createRegion) {
        final Region region = createRegion.getRegion();
        regionMap.put(region.getId(), region);
        return createSuccResponse(createRegion.getRequestId());
    }

    private Response handleDropRegion(DropRegion dropRegion) {
        regionMap.remove(dropRegion.getRegionId());
        return createSuccResponse(dropRegion.getRequestId());
    }

    /**
     * Waits for the service to be ready.
     */
    public void waitForReady() {
        mrTables.clear();
        assertTrue("Waiting for multi-region service",
                   new PollCondition(500, 10000) {
                       @Override
                       protected boolean condition() {
                           try {
                               checkForReady();
                           } catch (FaultException fe) {
                               /* not ready */
                               return false;
                           }
                           return true;
                       }
                   }.await());
    }

    /**
     * Waits for the specified number of tables to be managed by the service.
     */
    public void waitForTables(int nTables) {
        assertTrue("Waiting for tables",
                   new PollCondition(500, 30000) {
                       @Override
                       protected boolean condition() {
                           return mrTables.size() == nTables;
                       }
                   }.await());
    }

    /**
     * Waits for all messages to be GCed.
     */
    public void waitForGC() {
        assertTrue("Waiting for response",
           new PollCondition(10 * 1000, CLEANING_PASS_INTERVAL_MS * 2) {
               @Override
               protected boolean condition() {
                   if (getRequestIterator(10, TimeUnit.SECONDS).hasNext()) {
                       trace("Request table is not empty");
                       return false;
                   }

                   if (getResponseIterator(0, 10, TimeUnit.SECONDS).hasNext()) {
                       trace("Response table is not empty");
                       return false;
                   }
                   return true;
               }
           }.await());
    }

    public void dumpRegions() {
        final String msg = "Regions:";
        logger.info(msg + regionMap.values().stream()
                                   .map(Region::toString)
                                   .collect(Collectors.toSet()));
    }

    public void dumpTables() {
        final StringBuilder sb = new StringBuilder("Requests:\n");
        final RequestIterator itr = getRequestIterator(10, TimeUnit.SECONDS);
        while (itr.hasNext()) {
            sb.append(itr.next().toString()).append("\n");
        }
        sb.append("Responses:\n");
        final ResponseIterator itr2 =
            getResponseIterator(0, 10, TimeUnit.SECONDS);
        while (itr2.hasNext()) {
            sb.append(itr2.next().toString()).append("\n");
        }
        logger.info(sb.toString());
    }

    /* -- From Manager -- */

    @Override
    protected TableAPI getTableAPI() {
        return tableAPI;
    }

    @Override
    protected void handleIOE(String error, IOException ioe) {
        logger.warning(error);
        throw new IllegalStateException(error + ": " + ioe.getMessage(), ioe);
    }

    private Response createSuccResponse(int reqId) {
        final Response ret = Response.createReqResp(reqId,
                                                    DEFAULT_AGENT_GROUP_SIZE);
        ret.addSuccResponse(TEST_AGENT_ID);
        return ret;
    }

    private Response createErrorResponse(int reqId, String msg) {
        final Response ret = Response.createReqResp(reqId,
                                                    DEFAULT_AGENT_GROUP_SIZE);
        ret.addFailResponse(TEST_AGENT_ID, msg);
        return ret;
    }

    /**
     * Thread polling for requests.
     */
    private class RequestPollingThread extends StoppableThread {

        private static final int THREAD_SOFT_SHUTDOWN_MS = 5000;

        private volatile boolean isShutdown = false;

        RequestPollingThread() {
            super("RequestPollingThread");
        }

        @Override
        public void run() {
            try {
                waitForReady();
                while (!isShutdown) {
                    final RequestIterator itr =
                                getRequestIterator(10, TimeUnit.SECONDS);
                    while (itr.hasNext() && !isShutdown) {
                        final Request request = itr.next();
                        final Response response = handleRequest(request);
                        if (response != null) {
                            postResponse(response, false /*overwrite*/);
                            trace(prefix(request.getRequestId()) +
                                  "Response posted=" + response);
                        }
                    }
                }
            } catch (Exception ex) {
                System.out.println("Unexpected exception: " + ex);
                ex.printStackTrace();
            }
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        protected synchronized int initiateSoftShutdown() {
            isShutdown = true;
            notifyAll();
            return THREAD_SOFT_SHUTDOWN_MS;
        }
    }
}

