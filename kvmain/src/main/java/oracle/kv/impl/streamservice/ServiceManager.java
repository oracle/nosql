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

package oracle.kv.impl.streamservice;

import static oracle.kv.impl.streamservice.MRT.Response.VERSION_REQUEST_ID;
import static oracle.kv.impl.systables.StreamServiceTableDesc.COL_REQUEST_ID;
import static oracle.kv.impl.systables.StreamServiceTableDesc.COL_SERVICE_TYPE;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.Version;
import oracle.kv.impl.streamservice.MRT.Request;
import oracle.kv.impl.streamservice.MRT.Response;
import oracle.kv.impl.streamservice.ServiceMessage.ServiceType;
import oracle.kv.impl.systables.StreamRequestDesc;
import oracle.kv.impl.systables.StreamResponseDesc;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.table.FieldRange;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.WriteOptions;

/**
 * Base class for constructing stream service managers.
 */
public abstract class ServiceManager<R extends ServiceMessage,
                                     S extends ServiceMessage> {

    /** rate limit logger sample interval */
    private static final int RL_LOG_PERIOD_MS = 60 * 1000;
    /** rate limit logger max # of objects */
    private static final int RL_LOG_MAX_OBJ = 1024;
    /** max attempts to read and write a message */
    private static final int MAX_ATTEMPTS = 1024;

    private static final ReadOptions READ_OPTIONS =
            new ReadOptions(Consistency.ABSOLUTE, 0, null);

    protected static final WriteOptions WRITE_OPTIONS =
            new WriteOptions(Durability.COMMIT_SYNC, 0, null);

    private static final int N_OP_RETRIES = 10;

    /*
     * Cached handles to the request and response tables. These fields should
     * only be accessed through the appropriate get* methods.
     */
    private Table requestTable;
    private Table responseTable;

    protected final Logger logger;
    /** rate limit logger */
    private final RateLimitingLogger<String> rlLogger;

    /** test hook of read-modify-write */
    private TestHook<Version> rmwBeforeTestHook = null;
    private TestHook<Version> rmwAfterTestHook = null;

    protected ServiceManager(Logger logger) {
        this.logger = logger;
        rlLogger =
            new RateLimitingLogger<>(RL_LOG_PERIOD_MS, RL_LOG_MAX_OBJ, logger);
    }

    /**
     * Gets the service type of this service.
     */
    protected abstract ServiceType getServiceType();

    /**
     * Returns the table API handle.
     */
    protected abstract TableAPI getTableAPI();

    /**
     * Returns the max serial version that can be used to write to the
     * request/response tables.  If a message is version sensitive, the
     * value return should be used to determine the format.
     *
     * @return the max serial version that can be used when posting messages
     */
    protected abstract short getMaxSerialVersion();

    /**
     * Returns a request messages from the specified row.
     */
    protected abstract R getRequestFromRow(Row row) throws IOException;

    /**
     * Returns a response messages from the specified row.
     */
    protected abstract S getResponseFromRow(Row row) throws IOException;

    /**
     * Handles IOExceptions thrown when converting a Row to a message. The error
     * is a case specific string. If this method returns the conversion will
     * return null.
     *
     * @param error the description of when the exception occured
     * @param ioe the exception
     */
    protected abstract void handleIOE(String error, IOException ioe);

    /**
     * Writes the specified service request. If overwrite is false and a request
     * with the same ID already exists it is not overwritten.
     */
    protected void postRequest(R message, boolean overwrite) {
        postMessage(message, getRequestTable(), overwrite);
    }

    /**
     * Writes the specified service response. If overwrite is false and a
     * response with the same ID already exists it is not overwritten.
     */
    protected void postResponse(S message, boolean overwrite) {
        postMessage(message, getResponseTable(), overwrite);
    }

    private void postMessage(ServiceMessage message,
                             Table table,
                             boolean overwrite) {
        final Row row;
        try {
            row = message.toRow(table, getMaxSerialVersion());
        } catch (IOException ioe) {
            handleIOE("exception generating row from " + message, ioe);
            return;
        }

        /* post message to request/response table with retry */
        final int reqId = message.getRequestId();
        int count = 0;
        final long start = System.currentTimeMillis();
        final String tb = table.getFullNamespaceName();
        while (count < MAX_ATTEMPTS) {
            try {
                /* write request */
                if (message instanceof Request) {
                    directWrite(row, overwrite);
                    return;
                }

                final Response resp = (Response) message;
                /* write version */
                if (resp.getType().equals(Response.Type.GROUP_AGENT_VERSION)) {
                    if (resp.hasResponseFromLead()) {
                        /*
                         * the agent version message is from lead
                         * agent, overwrite the existing row if any
                         */
                        directWrite(row, true);
                        return;
                    }

                    /* version from non-lead agent, do RMW */
                    if(readModifyWrite(reqId, row)) {
                        return;
                    }

                    /* fail to RMW, retry */
                    continue;
                }

                /* write other responses */
                if (readModifyWrite(reqId, row)) {
                    /* post response succeeded */
                    return;
                }

                rlLogger.log(reqId + tb, Level.FINE,
                             () -> lm(logPrefix(reqId) +
                                      "Cannot write to table=" + tb +
                                      ", will retry"));
            } catch (FaultException fe) {
                count++;
                String msg = logPrefix(reqId) + "Cannot post ";
                if (message instanceof Request) {
                    msg += "request";
                } else {
                    final Response resp = (Response) message;
                    msg += "response: type=" + resp.getType();
                }
                msg += ", message=" + message +
                       ", table=" + tb +
                       ", #attempts=" + count +
                       ", elapsedMs=" +
                       (System.currentTimeMillis() - start) +
                       ", error=" + fe;
                rlLogger.log(reqId + tb + fe, Level.INFO, lm(msg));
            }
        }

        if (count == MAX_ATTEMPTS) {
            final String msg = logPrefix(reqId) +
                               "After max attempts=" + MAX_ATTEMPTS +
                               ", cannot post message=" + message +
                               ", table=" + table.getFullNamespaceName() +
                               ", elapsedMs=" +
                               (System.currentTimeMillis() - start);
            logger.warning(lm(msg));
            /* client may retry on exception */
            throw new FaultException(msg, false);
        }
    }

    private void directWrite(Row row, boolean overwrite) {
        if (overwrite) {
            execute(() -> getTableAPI().put(row, null /*prevRow*/,
                                            WRITE_OPTIONS));
        } else {
            execute(() -> getTableAPI().putIfAbsent(row, null /*prevRow*/,
                                                    WRITE_OPTIONS));
        }
    }

    private boolean readModifyWrite(int reqId, Row row)  {
        final Row exist = readReqRespWithRetry(reqId, false/* response */);
        if (exist == null) {
            /* no existing row */
            directWrite(row, false);
            return true;
        }

        final Row merged = mergeRespRows(row, exist);
        if (merged == null) {
            /* cannot merge the rows, let caller retry */
            return false;
        }

        final Version version = exist.getVersion();
        assert TestHookExecute.doHookIfSet(rmwBeforeTestHook, version);
        final Version ret =
            execute(() -> getTableAPI().putIfVersion(merged,
                                                     version,
                                                     null/* prevRow */,
                                                     WRITE_OPTIONS));
        /* return false if conflict */
        if (ret == null) {
            assert TestHookExecute.doHookIfSet(rmwAfterTestHook, null);
            final String tb = exist.getTable().getFullNamespaceName();
            rlLogger.log(reqId + tb, Level.FINE,
                         () -> lm("Cannot write row to table=" + tb +
                                  " with ver=" + version));
            return false;
        }

        return true;
    }

    protected void setRWMTestHook(TestHook<Version> before,
                                  TestHook<Version> after) {
        rmwBeforeTestHook = before;
        rmwAfterTestHook = after;
    }

    /**
     * Merges a response row with another response row, returns a merged row
     * @param row a new response row that will merge with the existing one
     * @param exist existing response row
     * @return a merged row
     */
    private Row mergeRespRows(Row row, Row exist) {
        try {
            final Response resp = (Response) getResponseFromRow(row);
            final Response existResp = (Response) getResponseFromRow(exist);

            final int groupSz = resp.getGroupSize();
            final int existGrpSz = existResp.getGroupSize();
            if (groupSz == existGrpSz) {
                /* merge response rows */
                existResp.merge(resp);
                return existResp.toRow(getResponseTable(),
                                       getMaxSerialVersion());

            }
            /*
             * The existing row in table is persisted by another agent
             * with different group size. No merge is needed and just
             * return the row to overwrite existing one.
             */
            logger.info(lm("Skip merge because of mismatch" +
                           " group size, this=" + resp +
                           ", existing=" + existResp +
                           ". incoming=" + resp));
            return row;
        } catch (IOException exp) {
            logger.fine(() -> lm("Cannot convert a row from the response " +
                                 "table to response, error=" + exp));
            return null;
        }
    }

    /**
     * Gets the request for the specified ID if it exists. If no request is
     * found null is returned.
     */
    public R getRequest(int requestId) {
        final Row row = readReqRespWithRetry(requestId, true);
        try {
            return row == null ? null : getRequestFromRow(row);
        } catch (IOException ioe) {
            handleIOE("exception parsing request " + requestId +
                      " from " + getServiceType() + " service.", ioe);
        }
        return null;
    }

    /**
     * Gets the response for the specified ID if it exists. If no response is
     * found null is returned.
     */
    public S getResponse(int requestId) {
        final Row row = readReqRespWithRetry(requestId, false);
        try {
            return row == null ? null : getResponseFromRow(row);
        } catch (IOException ioe) {
            handleIOE("exception parsing response to " + requestId +
                      " from " + getServiceType() + " service.", ioe);
        }
        return null;
    }

    /**
     * Gets request or response row from the system table with retry
     * @param reqId    request id
     * @param request  true if request, false if response
     * @return row of request or response
     */
    protected Row readReqRespWithRetry(int reqId, boolean request) {
        final PrimaryKey pkey = request ? createRequestKey(reqId) :
            createResponseKey(reqId);
        final String tb = pkey.getTable().getFullNamespaceName();
        int count = 0;
        final long start = System.currentTimeMillis();
        while (count < MAX_ATTEMPTS) {
            try {
                return execute(() -> getTableAPI().get(pkey, READ_OPTIONS));
            } catch (FaultException fe) {
                count++;
                final String type = request ? "request" : "response";
                final String msg = logPrefix(reqId) +
                                   "Cannot read type=" + type +
                                   " from table=" + tb +
                                   ", #attempts=" + count +
                                   ", elapsedMs=" +
                                   (System.currentTimeMillis() - start) +
                                   ", error=" + fe;
                rlLogger.log(reqId + tb + fe, Level.INFO, lm(msg));
            }
        }

        /* unable to read after max retry */
        final String msg = logPrefix(reqId) +
                           "After max attempts=" + MAX_ATTEMPTS +
                           ", cannot read type=" +
                           (request ? "request" : "response") +
                           " from table=" + tb +
                           ", elapsedMs=" +
                           (System.currentTimeMillis() - start);
        logger.warning(lm(msg));
        return null;
    }

    /**
     * Returns the agent version from response table
     * @return the agent version, or null if the row does not exist
     * @throws IOException if unable to read the response table
     */
    public Response getResponseAgentVer() throws IOException {
        final Row row = readReqRespWithRetry(VERSION_REQUEST_ID, false);
        if (row == null) {
            /* not found */
            return null;
        }
        final Response ret = Response.getFromRow(row);
        if (!ret.getType().equals(Response.Type.GROUP_AGENT_VERSION)) {
            throw new IllegalArgumentException(
                "Row with request id=" + VERSION_REQUEST_ID +
                " is not a version response");
        }
        return ret;
    }

    /**
     * Creates a primary key for the request table. If {@literal requestID > 0}
     * then the request ID component of the key is set.
     */
    protected PrimaryKey createRequestKey(int requestId) {
        return createKey(getRequestTable(), requestId);
    }

    /**
     * Creates a primary key for the response table. If {@literal requestID >
     * 0} then the request ID component of the key is set.
     */
    protected PrimaryKey createResponseKey(int requestId) {
        return createKey(getResponseTable(), requestId);
    }

    /**
     * Creates a primary key for the specified table. If {@literal requestID !=
     * 0} then the request ID component of the key is set.
     */
    private PrimaryKey createKey(Table messageTable, int requestId) {
        final PrimaryKey key = messageTable.createPrimaryKey();
        key.put(COL_SERVICE_TYPE, getServiceType().ordinal());
        if (requestId != 0) {
            key.put(COL_REQUEST_ID, requestId);
        }
        return key;
    }

    /**
     * Throws FaultException if the manager is not ready.
     */
    public void checkForReady() {
        try {
            getRequestTable();
            getResponseTable();
        } catch (FaultException fe) {
            throw new FaultException("Multi-region service is not ready: " +
                                     fe.getMessage(), fe, false);
        }
    }

    /**
     * Gets the request table. Throws FaultException if the system table has
     * not been initialized.
     */
    protected synchronized Table getRequestTable() {
        if (requestTable == null) {
            requestTable = getTable(StreamRequestDesc.TABLE_NAME);
        }
        return requestTable;
    }

    /**
     * Gets the response table. Throws FaultException if the system table has
     * not been initialized.
     */
    protected synchronized Table getResponseTable() {
        if (responseTable == null) {
            responseTable = getTable(StreamResponseDesc.TABLE_NAME);
        }
        return responseTable;
    }

    private Table getTable(String tableName) {
        final Table table = getTableAPI().getTable(tableName);
        if (table != null) {
            return table;
        }
        throw new FaultException("Unable to acquire handle to system table " +
                                 tableName + " store may not be initialized",
                                 false /*isRemote*/);
    }

    /**
     * Executes the specified operation, retrying if possible.
     */
    protected <T> T execute(Supplier<T> op) {
        FaultException lastException = null;
        int nRetries = N_OP_RETRIES;
        T ret = null;
        while (nRetries > 0) {
            try {
                ret = op.get();
                break;
            } catch (FaultException e) {
                lastException = e;
            }
            if (isShutdown()) {
                throw new FaultException("manager shutdown", false);
            }
            nRetries--;
        }
        if (lastException != null) {
            throw lastException;
        }
        return ret;
    }

    /**
     * Returns true if the manager is shutdown. Manager subclasses can override
     * to abort retries on shutdown.
     */
    protected boolean isShutdown() {
        return false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * Base class for message iterators.
     */
    protected abstract class MessageIterator<T extends ServiceMessage>
                                                       implements Iterator<T> {
        private final TableIterator<Row> tableItr;

        /**
         * Constructs an iteration over the records in the message table
         * with the service type returned from getServiceType(). If
         * startId == 0, then the iteration is over all messages, otherwise the
         * iteration starts with the message whos requestID is equal to or
         * greater then startId.
         *
         * If messageTable is null the iterator will be empty.
         */
        protected MessageIterator(Table messageTable,
                                  int startId,
                                  long timeout,
                                  TimeUnit timeoutUnit) {
            /*
             * messageTable could be null if the manager is started before the
             * system tables are created. In that case create an empty iterator.
             */
            if (messageTable == null) {
                tableItr = null;
                return;
            }

            /*
             * If start ID is 0, start iterating at the first request/response
             * message. Messages with ID < 0, are intended for direct access
             * only.
             */
            final FieldRange range =
                                messageTable.createFieldRange(COL_REQUEST_ID);
            range.setStart((startId == 0) ? 1 : startId, true /*isInclusive*/);
            final MultiRowOptions getOptions = new MultiRowOptions(range);
            final TableIteratorOptions options =
                                   new TableIteratorOptions(Direction.FORWARD,
                                                            Consistency.ABSOLUTE,
                                                            timeout, timeoutUnit);

            tableItr = getTableAPI().tableIterator(createKey(messageTable, 0),
                                                   getOptions, options);
        }

        @Override
        public boolean hasNext() {
            return tableItr != null && tableItr.hasNext();
        }

        @Override
        public T next() {
            if (tableItr == null) {
                throw new NoSuchElementException();
            }
            try {
                Row row = tableItr.next();
                if (row == null) {
                   throw new IllegalStateException("Unexpected null row from" +
                                                   " table iterator");
                }
                T m = getMessage(row);
                if (m == null) {
                   throw new IllegalStateException("Unable to get message from" +
                                                   " row: " + row);
                }
                return m;
            } catch (IOException ioe) {
                handleIOE("exception iterating messages from " +
                          getServiceType() + " service.", ioe);
            }
            throw new NoSuchElementException();
        }

        /**
         * Get a service message from the specified row.
         */
        protected abstract T getMessage(Row row) throws IOException;

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    private String logPrefix(long requestId) {
        return "[reqId=" + requestId + "] ";
    }

    /**
     * Adds logger header
     *
     * @param msg logging msg
     * @return logging msg with header
     */
    private String lm(String msg) {
        return "[ServiceManager] " + msg;
    }
}
