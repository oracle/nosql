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

package oracle.kv.impl.rep.migration;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.migration.PartitionMigrations.MigrationRecord;
import oracle.kv.impl.rep.migration.PartitionMigrations.SourceRecord;
import oracle.kv.impl.rep.migration.TransferProtocol.TransferRequest;
import oracle.kv.impl.rep.migration.generation.PartitionMDException;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;

/**
 * Migration service. This object is registered with the JE service framework
 * and handles requests to migrate partitions from target nodes. When a
 * service request is received the details of the request are read from the
 * newly established channel and if valid, a MigrationSource thread is
 * started to handle the actual data movement.
 *
 * The initial request is the only message sent from the target to the source
 * node. After that all communication is from the source to the target.
 */
public class MigrationService implements Runnable {

    /* Name used to register with the JE service framework */
    public static final String SERVICE_NAME = "PartitionMigration";

    private final Logger logger;

    /* Wait indefinitely for somebody to request the service. */
    private static final long POLL_TIMEOUT = Long.MAX_VALUE;

    private final RepNode repNode;

    private final Params params;

    /* The maximum number of target streams which can run concurrently. */
    private final int concurrentSourceLimit;

    final MigrationManager manager;

    private ThreadFactory sourceThreadFactory = null;

    /*
     * Queue for the JE service framework. Channels for incoming requests are
     * placed on this queue by the framework, and are pulled in this thread's
     * run method.
     */
    private final BlockingQueue<DataChannel> queue =
                                new LinkedBlockingQueue<>();

    /**
     * Maps the partition Id with the migration source, concurrent access
     * must be synchronized
     */
    private final Map<PartitionId, MigrationSource> sourceMap = new HashMap<>();

    /* True if the service is accepting migration requests. */
    private volatile boolean enabled = false;

    /* Count of errors processing requests */
    private int requestErrors = 0;

    /* For unit tests */
    TestHook<DatabaseEntry> readHook;
    private TestHook<AtomicReference<Response>> responseHook;

    MigrationService(RepNode repNode, MigrationManager manager, Params params) {
        this.repNode = repNode;
        this.manager = manager;
        this.params = params;
        concurrentSourceLimit =
                        params.getRepNodeParams().getConcurrentSourceLimit();
        logger = LoggerUtils.getLogger(this.getClass(), params);
    }

    synchronized void getStatus(HashSet<PartitionMigrationStatus> status) {
        for (MigrationSource source : sourceMap.values()) {
            status.add(source.getStatus());
        }
    }

    synchronized PartitionMigrationStatus getStatus(PartitionId partitionId) {
        final MigrationSource source = sourceMap.get(partitionId);
        return (source == null) ? null : source.getStatus();
    }

    /**
     * Returns true if:
     * a) there are running sources
     * b) there are transfers which have completed sources (i.e reached #3 End
     * of Data in the ToO), but have not finished the entire ToO protocol.
     *
     * Guarding against (b) is necessary because it's possible for the ToO
     * operation to fail, causing the state of the source to revert back to
     * pre-transfer conditions, specifically the migrated partition is
     * re-instated on the source. Therefore it is important that no one else
     * change any state dependent on the migrated partition until completely
     * finished. [#24245]
     */
    synchronized boolean pendingSources() {
        if (getNumRunning() > 0) {
            return true;
        }

        /*
         * Though there are no sources running, we need to check if any
         * transfers have completed but the migration is still waiting
         * for ToO. These will appear as completed source records.
         */
        final PartitionMigrations migrations = manager.getMigrations();
        if (migrations == null) {
            return false;
        }
        final Iterator<MigrationRecord> itr = migrations.completed();
        while (itr.hasNext()) {
            if (itr.next() instanceof SourceRecord) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the migration service is enabled. The service is
     * enabled after a successful call to start(). It is disabled by
     * calling stop().
     */
    boolean isEnabled() {
        return enabled;
    }

    /**
     * Starts the service by registering with the JE service framework.
     */
    synchronized void start(ReplicatedEnvironment repEnv) {
        if (enabled) {
            throw new IllegalStateException("Service already started");
        }
        assert repEnv != null;

        final RepImpl repImpl = RepInternal.getRepImpl(repEnv);
        final com.sleepycat.je.rep.impl.node.RepNode jeRN =
                                (repImpl == null) ? null : repImpl.getRepNode();
        if (jeRN == null) {
            /*
             * Both getRepImpl() and getRepNode() will return null if the
             * replicated env was closed. In this case just return since the
             * service will eventually be stopped. A subsequent state transition
             * when the env is reopened will restart the service.
             */
            return;
        }
        final ServiceDispatcher dispatcher = jeRN.getServiceDispatcher();

        if (dispatcher.isRegistered(SERVICE_NAME)) {
            throw new IllegalStateException("Service already registered");
        }

        enabled = true;
        final Thread t = new KVThreadFactory(" migration service", logger).
                                                        newThread(this);
        dispatcher.register(dispatcher.new LazyQueuingService(SERVICE_NAME,
                                                              queue, t));
        logger.info("Migration service accepting requests.");
    }

    /**
     * Stops the service.
     *
     * @param shutdown true if the node is shutting down
     */
    synchronized void stop(boolean shutdown, boolean wait,
                           ReplicatedEnvironment repEnv) {
        assert repEnv != null;

        if (!enabled) {
            return;
        }
        enabled = false;

        /**
         * Since the rep node may be in an incomplete state during shutdown
         * do not attempt to cancel registration with the service dispatcher.
         */
        if (!shutdown) {
            final RepImpl repImpl = RepInternal.getRepImpl(repEnv);
            final com.sleepycat.je.rep.impl.node.RepNode jeRN =
                                (repImpl == null) ? null : repImpl.getRepNode();
            if (jeRN != null) {
                final ServiceDispatcher dispatcher = jeRN.getServiceDispatcher();

                if (dispatcher.isRegistered(SERVICE_NAME)) {
                    logger.log(Level.INFO, "Stopping {0}", this);

                    /* This will interrupt the service thread if needed */
                    dispatcher.cancel(SERVICE_NAME);
                }
            }
        }

        for (MigrationSource source : sourceMap.values()) {
            source.cancel(wait);
        }
        sourceMap.clear();
    }

    /**
     * Shuts down a source and waits for it to stop. This is used
     * when the admin needs to cleanup after a cancel or failure.
     *
     * @param partitionId
     * @param targetRGId
     */
    synchronized void cancel(PartitionId partitionId, RepGroupId targetRGId) {
        final MigrationSource source = sourceMap.get(partitionId);

        if ((source != null) &&
            (source.getTargetGroupId() == targetRGId.getGroupId())) {
            source.cancel(true);
            removeSource(partitionId);
        }
    }

    /**
     * Returns the migration source for the specified partition. If there
     * isn't a migration going on for that partition, null is returned.
     *
     * @param partitionId a partition ID
     * @return a migration source or null
     */
    synchronized MigrationSource getSource(PartitionId partitionId) {
        final MigrationSource source = sourceMap.get(partitionId);
        if (source == null) {
            return null;
        }
        /*
         * There exists a window between the time the MigrationSource is
         * created and put into the map, and the time when MigrationSource
         * thread starts up by executor. We can only remove MigrationSource
         * from the map after the migration is complete.
         */
        if (!source.isAlive() && source.isMigrationDone()) {
            sourceMap.remove(partitionId);
            logger.fine(() -> "Migration source removed since migration is " +
                              "done, pid=" + partitionId);
            return null;
        }
        return source;
    }

    /**
     * Returns a migration stream handler, the handler can be  either a NOOP
     * handler {@link MigrationStreamHandle#NOOP_HANDLER}, or a valid
     * migrating handle
     * @param pid partition id
     * @param txn transaction of the request
     * @param incremented set to true if the pending write counter is
     *                    incremented
     * @return a migration stream handler
     */
    public synchronized MigrationStreamHandle getMigrationHandle(
        PartitionId pid, Transaction txn, AtomicBoolean incremented) {
        final MigrationSource source = getSource(pid);
        if (source != null) {
            /* a valid handler is returned, no need to increment counter */
            return MigrationStreamHandle.getMigratingHandle(source, txn);
        }

        /* increment the counter within the same sync block of getSource() */
        final int pending = manager.incrNoopHandlerWrites(pid);
        incremented.set(true);
        logger.finest(() -> "[" + pid + "] Increase #pending " +
                            "writes=" + pending +
                            ", txn id=" + txn.getId());
        /* no migration source, use noop handler */
        return MigrationStreamHandle.NOOP_HANDLER;
    }

    private synchronized void removeSource(PartitionId partitionId) {
        sourceMap.remove(partitionId);
    }

    @Override
    public void run() {

        /* This thread is run the first time a service request comes in for the
         * migration service. Once started it will remain running until the
         * service is unregistered or canceled. If unregistered the thread will
         * be interrupted.
         */
        logger.log(Level.INFO, "Migration service thread started.");

        try {
            while (enabled) {
                DataChannel channel = null;
                try {
                    channel = queue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);

                    if (channel == RepUtils.CHANNEL_EOF_MARKER) {
                        logger.info("EOF marker - shutdown");
                        return;
                    }

                    if (channel != null) {
                        processRequest(channel);
                    }
                } catch (IOException ioe) {
                    closeChannel(channel);
                    logger.log(Level.INFO,
                               "IOException processing migration request: ",
                               ioe);
                } catch (InterruptedException ie) {
                    logger.info("Migration service interrupted");
                    return;
                }
            }
        } finally {
            logger.info("Migration service thread exit");
        }
    }

    /**
     * Closes the specified channel, logging any resulting exceptions.
     *
     * @param channel a channel
     */
    private void closeChannel(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ioe) {
                logger.log(Level.WARNING, "Exception during cleanup", ioe);
            }
        }
    }

    /**
     * Processes the initial service request. The migration details are read
     * from the channel and if valid a migration source thread is created
     * and started.
     *
     * @param channel a channel
     * @throws IOException resulting from operations on the channel
     */
    private void processRequest(DataChannel channel) throws IOException {

        final TransferRequest request = TransferRequest.read(channel);

        final AtomicReference<Response> hookedResponse =
                new AtomicReference<>();
        assert TestHookExecute.doHookIfSet(responseHook, hookedResponse);

        if ((responseHook != null) && (hookedResponse.get() != null)) {
            final Response response = hookedResponse.get();
            if (response.equals(Response.BUSY)) {
                reportBusy(concurrentSourceLimit, "Test busy", channel);
            } else {
                reportError(response, "Test error: " + response, channel);
            }
            return;
        }

        final PartitionId partitionId = new PartitionId(request.partitionId);

        final RepNodeId targetRNId = request.targetRNId;
        final int targetGroupId = targetRNId.getGroupId();
        /* If no target, we are only transferring data, not migrating */
        final boolean transferOnly =
            targetRNId.equals(TransferProtocol.TRANSFER_ONLY_TARGET);

        /* Check to make sure the requested partition is here */
        try {
            repNode.getPartitionDB(partitionId);
        } catch (IncorrectRoutingException ire) {

            /*
             * If the request is for an unknown partition, it may be due to
             * completed transfer, in which case the source should reset
             * and let the target try again later.
             */
            if (!transferOnly && checkForRestart(request)) {
                reportBusy(concurrentSourceLimit,
                           "Migration source resetting " + partitionId,
                           channel);
                return;
            }
            reportError(Response.UNKNOWN_SERVICE,
                        "Request for unknown: " + ire.getLocalizedMessage(),
                        channel);
            return;
        }

        /*
         * If the source doesn't know about the target rep group, report back
         * BUSY. Eventually the topology will be updated and we can proceed.
         * The check is necessary because once the migration is completed the
         * source needs to know about the target group in order to forward
         * requests there.
         */
        if (!transferOnly &&
            repNode.getTopology().get(new RepGroupId(targetGroupId)) == null) {
            reportBusy(0,
                       "Migration source needs updated topology, target " +
                       "group " + targetGroupId + " unknown", channel);
            return;
        }

        if (transferOnly) {
            logger.log(Level.FINE,
                       "Received transfer request for {0}",
                       new Object[]{partitionId});
        } else {
            logger.log(Level.FINE,
                       "Received migration request for {0} to {1}",
                       new Object[]{partitionId, targetGroupId});
        }

        synchronized (this) {

            if (!enabled || repNode.getTableManager().isBusyMaintenance()) {
                /* Report 0 streams until enabled */
                reportBusy(0,
                           "Migration source not enabled for " + partitionId,
                           channel);
                return;
            }

            /*
             * Limit the number of concurrent streams. The target will retry
             * until they can get in.
             */
            final int running = getNumRunning();
            if (running >= concurrentSourceLimit) {
                reportBusy(concurrentSourceLimit,
                           "Migration source busy. Number of streams= " +
                           running +
                           ", max= " + concurrentSourceLimit, channel);
                return;
            }

            MigrationSource source = sourceMap.get(partitionId);

            /*
             * If there is a source already running, try to cancel it and ask
             * the target to try again later.
             */
            if (source != null) {

                /*
                 * If the source is a transfer, cancel it regardless of
                 * the request.
                 *
                 * If the source is a migration, only cancel it if the
                 * request is also for a migration.
                 */
                if (source.isTransferOnly() || !transferOnly) {
                    source.cancel(false);
                }
                reportBusy(concurrentSourceLimit,
                           "Source for " + partitionId + " already running: " +
                           source.toString(), channel);
                return;
            }

            /*
             * Initialize the generation table now. It isn't needed until
             * the migration is complete, but it is better to catch a failure
             * now before the migration starts vs. at the end.
             */
            if (!transferOnly) {
                try {
                    manager.initializeGenerationTable();
                } catch (PartitionMDException ex) {
                    final String msg = "Reject migration request because of  " +
                                       "failure to ensure partition" +
                                       " generation db, reason: " +
                                       ex.getMessage();
                    /* let target retry */
                    reportBusy(concurrentSourceLimit, msg, channel);
                    return;
                }
            }

            source = new MigrationSource(channel, partitionId,
                                         request.targetRNId,
                                         repNode, this, params);
            sourceMap.put(partitionId, source);

            try {
                TransferRequest.writeACKResponse(channel);
            } catch (IOException ioe) {
                sourceMap.remove(partitionId);
                logger.warning("MigrationSource pid=" + partitionId +
                               " removed due to exception=" + ioe);
                closeChannel(channel);
                throw ioe;
            }

            if (sourceThreadFactory == null) {
                sourceThreadFactory =
                     new KVThreadFactory(" partition migration source", logger);
            }
            logger.log(Level.INFO, "Starting {0}", source);

            /* Start streaming K/Vs to target */
            sourceThreadFactory.newThread(source).start();
        }
    }

    private int getNumRunning() {
        assert Thread.holdsLock(this);

        /* First clean out any non-running sources */
        final Iterator<MigrationSource> itr = sourceMap.values().iterator();
        while (itr.hasNext()) {
            MigrationSource s = itr.next();
            /* remove only when migration thread exits */
            if (!s.isAlive() && s.isMigrationDone()) {
                itr.remove();
            }
        }
        return sourceMap.size();
    }

    /*
     * Checks to see if there is a source migration record for the specified
     * partition and target. If so, the record is removed and true is returned.
     */
    private boolean checkForRestart(TransferRequest request) {
        final PartitionMigrations migrations = manager.getMigrations();

        if (migrations == null) {
            return false;
        }

        final SourceRecord record = migrations.getSource(
            new PartitionId(request.partitionId));

        /*
         * If no record, or the target does not match, then no restart.
         * We can only restart if it is the same node making the new request.
         * We can't trust other nodes in the shard since network partition may
         * have more than one node acting as the master. We could incorrectly
         * restore the partition here (as a result of removing the migration
         * record) when it has moved to the new location. Very bad: [#25313].
         */
        if ((record == null)
            || !record.getTargetRNId().equals(request.targetRNId)) {
            return false;
        }
        try {
            logger.log(
                Level.INFO,
                () ->
                String.format(
                    "Migration source detected restart of record %s "
                    + "with %s, removing completed record",
                    record, request));
            /* migration will restart, need to update PGT */
            manager.removeRecord(record.getPartitionId(), record.getId(),
                                 true, true /* update PGT */);
        } catch (DatabaseException de) {
            logger.log(Level.WARNING, "Exception removing " + record, de);
        }
        return true;
    }

    /*
     * Reports a busy condition to the client. The message is also logged
     * at FINE, and the channel is closed.
     */
    private void reportBusy(int numStreams,
                            String message,
                            DataChannel channel) {
        requestErrors++;
        logger.log(Level.FINE, message);
        try {
            TransferRequest.writeBusyResponse(channel, numStreams, message);
        } catch (IOException ioe) {
            logger.log(Level.WARNING, "Exception sending busy response", ioe);
        }
        closeChannel(channel);
    }

    /*
     * Reports and error condition to the client. The message is also logged
     * at INFO, and the channel is closed.
     */
    private void reportError(Response response,
                             String message,
                             DataChannel channel) {
        assert response.equals(Response.FORMAT_ERROR) ||
               response.equals(Response.UNKNOWN_SERVICE) ||
               response.equals(Response.INVALID);

        requestErrors++;
        logger.log(Level.INFO, message);
        try {
            TransferRequest.writeErrorResponse(channel, response, message);
        } catch (IOException ioe) {
            logger.log(Level.WARNING, "Exception sending error response", ioe);
        }
        closeChannel(channel);
    }

    /* -- Unit test -- */

    void setReadHook(TestHook<DatabaseEntry> hook) {
        readHook = hook;
    }

    void setResponseHook(TestHook<AtomicReference<Response>> hook) {
        responseHook = hook;
    }

    @Override
    public String toString() {
        return "MigrationService[" + enabled + ", " + sourceMap.size() +
               ", " + requestErrors + "]";
    }

    /**
     * Unit test only
     */
    public int getRequestErrors() {
        return requestErrors;
    }
}
