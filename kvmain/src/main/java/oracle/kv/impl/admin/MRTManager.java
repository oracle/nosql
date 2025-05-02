/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.admin;

import static oracle.kv.KVVersion.PREREQUISITE_VERSION;
import static oracle.kv.impl.util.VersionUtil.compareMinorVersion;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.KVVersion;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.streamservice.MRT.Manager;
import oracle.kv.impl.streamservice.MRT.Request;
import oracle.kv.impl.streamservice.MRT.Request.CreateChild;
import oracle.kv.impl.streamservice.MRT.Request.CreateTable;
import oracle.kv.impl.streamservice.MRT.Request.DropChild;
import oracle.kv.impl.streamservice.MRT.Request.DropTable;
import oracle.kv.impl.streamservice.MRT.Request.UpdateTable;
import oracle.kv.impl.streamservice.MRT.Response;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.ShutdownThread;
import oracle.kv.table.TableAPI;
import oracle.kv.util.ErrorMessage;

/**
 * Multi-region table service manager.
 * This manager is used to handle messages to and from the multi-region service.
 * It is assumed that request {@literal IDs > 0} are plan IDs. Request
 * {@literal IDs <= 0} are reserved.
 */
public class MRTManager extends Manager {

    /* Public for unit test. */
    public static final int CLEANING_PASS_INTERVAL_MS = 60 * 1000;

    private final Admin admin;

    /*
     * Cached maximum version that the agent and store supports. In general the
     * maximum version is the lowest of the store version and the agent version.
     * It includes the store version so that messages are not written to the
     * request table that cannot be read by other nodes in the store.
     *
     * If the agent has not started, the maximum version will be the store
     * version. Note that this forces the first time agent to be at the store
     * version.
     *
     * Initialized to null (vs PREREQUISITE_VERSION) so that the min agent
     * version is set.
     */
    private volatile KVVersion maxVersion = null;

    /* Lock for updateVersion() */
    private final AtomicBoolean inUpdate = new AtomicBoolean(false);

    /**
     * Cached max serial version. It would be populated when it is used. Use
     * {@link #getMaxSerialVersion()} as it may change based on the store
     * and agent version.
     */
    private volatile short maxSerialVersion = Request.V1;

    private StreamServiceTableCleaner cleaner = null;

    /*
     * Flag to indicate that the request messages should be updated to Java
     * serialization if needed. The cleaner thread will check this when it
     * runs and will convert messages as needed. If the conversion is
     * complete the cleaner thread will clear the flag.
     */
    private boolean checkForRequestUpdate = false;

    /* True if shutdown has been called. */
    private volatile boolean shutdown = false;

    /* For unit test */
    static KVVersion forceAgentVersion = null;

    MRTManager(Admin admin) {
        super(admin.getLogger());
        this.admin = admin;
    }

    /**
     * Overridden for unit test.
     */
    @Override
    public KVVersion getAgentVersion() {
        return (forceAgentVersion != null) ? forceAgentVersion :
                                             super.getAgentVersion();
    }

    /**
     * Gets the max serial version that can be used to write to the
     * request/response table. If a message is version sensitive, the
     * value return should be used to determine the format.
     *
     * @return the max serial version that can be used in messages
     */
    @Override
    protected short getMaxSerialVersion() {
        /*
         * If the max is not up to current, see if the store and/or agent
         * has been upgraded.
         */
        if (maxSerialVersion < SerialVersion.CURRENT) {
            updateVersion(admin.getStoreVersion());
        }
        return maxSerialVersion;
    }

    /**
     * Posts a create table message.
     */
    public void postCreateMRT(int requestId, TableImpl table,  int seqNum) {
        postRequest(new CreateTable(requestId, table, seqNum));
    }

    /**
     * Posts a create child table message.
     */
    public void postCreateChildMRT(int requestId,
                                   TableImpl topTable,
                                   int seqNum,
                                   long tableId) {
        postRequest(new CreateChild(requestId, topTable, seqNum, tableId));
    }

    /**
     * Posts an update table message.
     */
    public void postUpdateMRT(int requestId, TableImpl table,  int seqNum) {
        postRequest(new UpdateTable(requestId, table, seqNum));
    }

    /**
     * Posts a drop table message.
     */
    public void postDropMRT(int requestId, TableImpl table, int seqNum) {
        postRequest(new DropTable(requestId, table, seqNum));
    }

    /**
     * Post a drop child table message.
     */
    public void postDropChildMRT(int requestId,
                                 long tableId,
                                 String tableName,
                                 int seqNum,
                                 TableImpl topTable) {
        postRequest(new DropChild(requestId, tableId, tableName, seqNum,
                                  topTable));
    }

    /**
     * Posts a create region message.
     */
    public void postCreateRegion(int requestId, Region region,  int seqNum) {
        postRequest(new Request.CreateRegion(requestId, region, seqNum));
    }

    /**
     * Posts a drop region message.
     */
    public void postDropRegion(int requestId, int regionId,  int seqNum) {
        postRequest(new Request.DropRegion(requestId, regionId, seqNum));
    }

    /**
     * Gets the max version supported by both the store and the MR agent. In other
     * words this is the maximum version that can be used to post messages
     * to the MR tables.
     * Gets the agent version. If the agent version is below the store version
     * the minimum agent version and the store version in the request table
     * may be updated. If the agent version is not known PREREQUISITE_VERSION
     * is returned.
     */
    public KVVersion getMaxVersion() {

        /*
         * If the cached agent version is not set, or below the store version
         * attempt to update.
         */
        final KVVersion storeVersion = admin.getStoreVersion();
        if ((maxVersion == null) ||
            (compareMinorVersion(maxVersion, storeVersion) < 0)) {
            updateVersion(storeVersion);
        }
        /*
         * If the cached value was not updated, which could happen if either
         * the attempt to read the request table got a FaultException, the
         * agent has not yet be started, or the agent has not yet been upgraded
         * to a version that can record this information there, then return the
         * prerequisite since the agent has to at least support that.
         */
        return maxVersion == null ? PREREQUISITE_VERSION : maxVersion;
    }

    /**
     * Updates the cached maximum version and the minimum agent version and
     * store version in the request table if possible. This method should
     * not be called with the object monitor held as there is deadlock risk.
     */
    public void updateVersion(KVVersion storeVersion) {
        assert !Thread.holdsLock(this);

        /*
         * This update could be made before any user tables are created so
         * check if the admin can write to system tables. If not exit.
         */
        if (!admin.systemTablesEnabled(false)) {
            return;
        }

        /*
         * We prevent more than one active call to updateVersion()
         * because it isn't necessary. Note that this guards setting both
         * maxVersion and maxSerialVersion.
         */
        if (!inUpdate.compareAndSet(false, true)) {
            return;
        }
        try {
            /* Done if the cached version is at least store version */
            if ((maxVersion != null) &&
                (compareMinorVersion(maxVersion, storeVersion) >= 0)) {
                return;
            }

            /*
             * Get the agent version, checking if it is at the required version
             * to support the store.
             */
            final KVVersion agentVersion = getAgentVersion();
            if ((agentVersion != null) &&
                (compareMinorVersion(agentVersion, PREREQUISITE_VERSION) < 0)) {
                logger.warning("Multi-region agent version " +
                               agentVersion.getNumericVersionString() +
                               " is earlier than the required minimum version" +
                               " and must be upgraded to " +
                               PREREQUISITE_VERSION.getNumericVersionString() +
                               " or above");
            }

            /*
             * If the agent has not started (agentVersion == null) use
             * storeVersion, otherwise use the minimum of the storeVersion
             * and the agentVersion.
             */
            final KVVersion newVersion =
                ((agentVersion == null) ||
                 (compareMinorVersion(agentVersion, storeVersion) > 0)) ?
                                                                storeVersion :
                                                                agentVersion;
            /*
             * Get the min agent version if set. If it's at or above the
             * new version, use it.
             */
            final KVVersion oldVersion = getMinAgentVersion();
            if ((oldVersion != null) &&
                (compareMinorVersion(oldVersion, newVersion) >= 0)) {
                maxVersion = oldVersion;
            } else {
                /*
                 * The old version is either null (not set), or less than
                 * new version Move forward and update the agent min version
                 * and the cached value.
                 */
                setStoreAndMinAgentVersions(storeVersion, newVersion);
                logger.info("Store version set to=" +
                            storeVersion.getNumericVersionString() +
                            ", min agent ver=" +
                            newVersion.getNumericVersionString());
                maxVersion = newVersion;
            }
            maxSerialVersion = SerialVersion.getMaxSerialVersion(maxVersion);

            /* set the flag to check for update and run the cleaner */
            checkForRequestUpdate = true;
            requestCleaning();
        } catch (FaultException fe) {
            /*
             * A fault exception can occur if the system table has not
             * yet been created, or there is a timeout, etc.
             */
            logger.info("Exception updating agent version: " + fe.getMessage());
        } finally {
            inUpdate.set(false);
        }
    }

    /**
     * Post a request. Start cleaning each time a request is posted.
     * The cleaner will run until the request can be removed.
     */
    private void postRequest(Request message) {
        super.postRequest(message, false /*overwrite*/);
        requestCleaning();
    }

    /**
     * Override for cleaning. If a response was received, start cleaning.
     */
    @Override
    public Response getResponse(int requestId) {
        final Response response = super.getResponse(requestId);
        if (response != null) {
            requestCleaning();
        }
        return response;
    }

    /**
     * Removes the request with the specified ID. Returns true if the request
     * existed and was deleted.
     */
    private boolean deleteRequest(int requestId) {
        logger.fine(() -> this + ": removing request for " + requestId);
        return execute(() ->
                           getTableAPI().delete(createRequestKey(requestId),
                                                null /*prevRow*/,
                                                WRITE_OPTIONS));
    }

    /**
     * Removes the response with the specified ID. Returns true if the response
     * existed and was deleted.
     */
    private boolean deleteResponse(int requestId) {
        logger.fine(() -> this + ": removing response for " + requestId);
        return execute(() ->
                           getTableAPI().delete(createResponseKey(requestId),
                                                null /*prevRow*/,
                                                WRITE_OPTIONS));
    }

    @Override
    protected TableAPI getTableAPI() {
        return admin.getInternalKVStore().getTableAPI();
    }

    @Override
    protected void handleIOE(String error, IOException ioe) {
        logger.warning(error);
        throw new CommandFaultException(this + ": " + error, ioe,
                                        ErrorMessage.NOSQL_5500, null);
    }

    private synchronized void requestCleaning() {
        if (shutdown || (cleaner != null) && cleaner.requestCleaning()) {
            return;
        }
        cleaner = new StreamServiceTableCleaner();
        cleaner.start();
    }

    /**
     * Shutdown the service.
     */
    synchronized void shutdown() {
        shutdown = true;
        if (cleaner != null) {
            cleaner.shutdown();
            cleaner = null;
        }
    }

    @Override
    protected boolean isShutdown() {
        return shutdown;
    }

    @Override
    public String toString() {
        return "MRTManager";
    }

    /**
     * Stream table cleaner thread. The cleaner will run as long as there
     * are request or response messages present in the tables.
     * A request can be deleted once the response for that request is complete,
     * that means, all agents have posted their responses, and the associated
     * plan is in a terminal state or pruned.
     * A response can be removed if the corresponding request is removed.
     * TODO - Once more than one stream service is supported this thread
     * should be pulled out into a general purpose thread for all services.
     */
    private class StreamServiceTableCleaner extends ShutdownThread {
        private volatile boolean cleaningRequested = true;

        StreamServiceTableCleaner() {
            super("StreamServiceTableCleaner");
        }

        /* -- From ShutdownThread -- */

        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        public void run() {
            logger.info("Starting request/response sys table cleaner=" + this);
            while (!isShutdown()) {
                updateRequestMessages();

                if (clean()) {
                    break;
                }
                cleaningWait();
            }
            logger.info(() -> "Exiting " + this);
        }

        /*
         * Makes a pass through the response messages looking for any that
         * can be removed. Returns true if all cleaning is done.
         */
        private boolean clean() {
            logger.info(() -> this + " clean");
            boolean pendingResponses = false;

            /*
             * See if any requests/responses be deleted. This can be done
             * if the corresponding plan is gone or in a terminal state.
             */
            final ResponseIterator itr = getResponseIterator(0 /*startId*/,
                                                             10L,
                                                             TimeUnit.SECONDS);
            while (itr.hasNext() && !isShutdown()) {
                if (!checkForDelete(itr.next())) {
                    pendingResponses = true;
                }
            }
            if (pendingResponses) {
                return false;
            }

            /*
             * If there are no pending responses, check if there are any
             * requests.
             */
             if (getRequestIterator(10L, TimeUnit.SECONDS).hasNext()) {
                 return false;
            }
            return checkForDone();
        }

        /*
         * Checks whether the specified response and corresponding request
         * messages can be deleted. Returns true if both messages are removed.
         *
         * Messages can be removed if their associated plan is terminal or has
         * been removed.
         */
        private boolean checkForDelete(Response response) {
            final int requestId = response.getRequestId();

            /*
             * Request IDs < 0 special cases and are never removed.
             */
            if (requestId < 0) {
                /* Return true to stop checking */
                return true;
            }

            final Plan plan = admin.getPlanById(requestId);
            /*
             * Can't remove messages if the plan is still around and in a
             * non-terminal state.
             */
            if ((plan != null) && !plan.getState().isTerminal()) {
                return false;
            }

            if (!response.isComplete()) {
                logger.fine(() -> "Response (reqId=" + response.getRequestId() +
                                  " is incomplete" +
                                  ", group size=" + response.getGroupSize() +
                                  ", #responses=" + response.getNumResponses());
                return false;
            }

            /* If the request is present remove it. */
            if (getRequest(requestId) != null) {

                /* If the removed failed, retry on next pass */
                if (!deleteRequest(requestId)) {
                    return false;
                }
            }

            /* Plan is gone, request is gone, safe to remove the response. */
            final boolean succ = deleteResponse(requestId);
            logger.finest(() -> "Response of reqId=" + requestId + " deleted" +
                                ", succ=" + succ);
            return succ;
        }

        /**
         * Updates the request messages if needed. A scan of the request
         * messages is made if checkForRequestUpdate is set. Will clear
         * checkForRequestUpdate if the update is completed, or not needed.
         */
        private void updateRequestMessages() {
            if (!checkForRequestUpdate) {
                return;
            }

            assert maxVersion != null;
            /* Scan the request table looking for messages to update */
            final RequestIterator itr = getRequestIterator(10L,
                                                           TimeUnit.SECONDS);
            while (itr.hasNext() && !isShutdown()) {
                final Request req = itr.next();
                /*
                 * Only some messages types used FastExternalizable for
                 * serialization.
                 */
                switch (req.getType()) {
                    case CREATE_TABLE :
                    case UPDATE_TABLE :
                    case DROP_TABLE :
                    case CREATE_CHILD :
                    case DROP_CHILD :
                    case CREATE_REGION :
                    case DROP_REGION :
                    case STORE_VERSION :
                        break;
                    default : throw new IllegalStateException("unreachable");
                }
            }
            /* We are here either because of done, or in shutdown */
            checkForRequestUpdate = isShutdown();
        }

        /*
         * Checks whether to exit when everything is done. If true is returned
         * a new request for cleaning has come in during cleaning.
         */
        private synchronized boolean checkForDone() {
            if (cleaningRequested) {
                cleaningRequested = false;
            } else {
                shutdown();
            }
            return isShutdown();
        }

        /**
         * Requests that cleaning be preformed. Returns true if the request was
         * successful. If false is returned a new cleaner thread needs to be
         * started.
         */
        synchronized boolean requestCleaning() {
            if (isShutdown()) {
                return false;
            }
            cleaningRequested = true;
            notifyAll();
            return true;
        }

        private synchronized void cleaningWait() {
            try {
                waitForMS(CLEANING_PASS_INTERVAL_MS);
            } catch (InterruptedException ie) {
                throw new IllegalStateException("Unexpected interrupt", ie);
            }
        }
    }
}
