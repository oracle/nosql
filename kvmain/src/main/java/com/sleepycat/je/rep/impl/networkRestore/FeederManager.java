/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.impl.networkRestore;

import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * Manages the multiple log file feeders that may be servicing requests from
 * multiple clients requesting log files.
 */
public class FeederManager extends StoppableThread {

    /*
     * The queue into which the ServiceDispatcher queues socket channels for
     * new Feeder instances.
     */
    private final BlockingQueue<DataChannel> channelQueue =
        new LinkedBlockingQueue<>();

    /*
     * Map indexed by the client id. Each Feeder adds itself to the Map when
     * its first created and removes itself when it exits.
     */
    final Map<Integer, FeederCreator> feederCreators =
        new ConcurrentHashMap<>();

    /*
     * Maps the client id to its Lease. Except for instantaneous overlaps,
     * a client will have an entry in either the feeders map or the leases
     * map, but not in both maps.
     */
    final Map<Integer, Lease> leases = new ConcurrentHashMap<>();

    /*
     * A cache of StatResponses to try minimize the recomputation of SHA256
     * hashes.
     */
    final Map<String, Protocol.FileInfoResp> statResponses =
        new ConcurrentHashMap<>();

    /* Implements the timer used to maintain the leases. */
    final Timer leaseTimer = new Timer(true);

    /* This node's name and internal id */
    final NameIdPair nameIdPair;

    /* Counts the number of times the lease was renewed. */
    public int leaseRenewalCount;

    /* The duration of leases. */
    long leaseDuration = DEFAULT_LEASE_DURATION;

    final ServiceDispatcher serviceDispatcher;

    /* Determines whether the feeder manager has been shutdown. */
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    final Logger logger;

    /**
     * Non-zero values override values used in FileFeeder protocol.
     */
    private volatile int testProtocolMaxVersion = 0;
    private volatile int testProtocolLogVersion = 0;

    /* Wait indefinitely for somebody to request the service. */
    private static final long POLL_TIMEOUT = Long.MAX_VALUE;

    /* Identifies the Feeder Service. */
    public static final String FEEDER_SERVICE = "LogFileFeeder";

    /*
     * Default duration of lease on DbBackup associated with the client. It's
     * five minutes.
     */
    private static final long DEFAULT_LEASE_DURATION = 5 * 60 * 1000;

    /**
     * Creates a FeederManager but does not start it.
     *
     * @param serviceDispatcher The service dispatcher with which the
     * FeederManager must register itself. It's null only in a test
     * environment.
     *
     * @param nameIdPair The node name and id  associated with the feeder
     *
     * @param envImpl the environment that will provide the log files
     */
    public FeederManager(ServiceDispatcher serviceDispatcher,
                         EnvironmentImpl envImpl,
                         NameIdPair nameIdPair) {

        super(envImpl, "Feeder Manager node: " + nameIdPair.getName());
        this.serviceDispatcher = serviceDispatcher;
        serviceDispatcher.register
            (serviceDispatcher.new
                 LazyQueuingService(FEEDER_SERVICE, channelQueue, this));
        this.nameIdPair = nameIdPair;
        logger = LoggerUtils.getLogger(getClass());
    }

    EnvironmentImpl getEnvImpl() {
        return envImpl;
    }

    /**
     * Returns the number of times the lease was actually renewed.
     */
    public int getLeaseRenewalCount() {
        return leaseRenewalCount;
    }

    /**
     * Returns the number of leases that are currently outstanding.
     */
    public int getLeaseCount() {
        return leases.size();
    }

    /**
     * Returns the number of feederCreators that are currently active with this node.
     * Note that active leases are included in this count, since it's expected
     * that the clients will try to reconnect.
     */
    public int getActiveFeederCount() {
        return feederCreators.size() + getLeaseCount();
    }

    public void setLeaseDuration(long leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    /**
     * Clears the cached checksum for a file when it may be overwritten
     * (e.g., entries may be erased).
     */
    public void clearedCachedFileChecksum(String fileName) {
        statResponses.remove(fileName);
    }

    /**
     * Used in testing to clear the cache in case the test is switching
     * between hash algorithms.
     */
    public void clearedCachedFileChecksums() {
        statResponses.clear();
    }

    /**
     * The dispatcher method that starts up new log file feeders.
     */
    @Override
    public void run() {
        try {
            while (true) {
                final DataChannel channel =
                    channelQueue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
                if (channel == RepUtils.CHANNEL_EOF_MARKER) {
                    LoggerUtils.info(logger, envImpl,
                                     "Log file Feeder manager soft shutdown.");
                    return;
                }
                new FeederCreator(this, channel, serviceDispatcher).start();
            }
        } catch (InterruptedException ie) {
            LoggerUtils.info
                (logger, envImpl, "Log file feeder manager interrupted");
        } catch (Exception e) {
            LoggerUtils.severe(logger, envImpl,
                               "unanticipated exception: " + e.getMessage() +
                               " " + LoggerUtils.getStackTraceForSevereLog(e));
            throw new EnvironmentFailureException
                (envImpl, EnvironmentFailureReason.UNCAUGHT_EXCEPTION, e);
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        LoggerUtils.fine
            (logger, envImpl, "Shutting down log file feeder manager");

        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        shutdownThread(logger);

        /* shutdown active feeder threads */
        for (FeederCreator feederCreator :
             new ArrayList<>(feederCreators.values())) {
            feederCreator.shutdown();
        }
        leaseTimer.cancel();
        /*
         * Terminate any outstanding leases, and close their associated open
         * dbbackups, so that the environment can be closed cleanly.
         */
        for (Lease l : new ArrayList<>(leases.values())) {
            DbBackup dbBackup = l.terminate();
            if (dbBackup != null && dbBackup.backupIsOpen()) {
                dbBackup.endBackup();
            }
        }
        serviceDispatcher.cancel(FEEDER_SERVICE);
        cleanup();
        LoggerUtils.fine(logger, envImpl,
                         "Shut down log file feeder manager completed");
    }

    public boolean isClientActive(final int clientId) {
        return feederCreators.containsKey(clientId);
    }

    /**
     * Perform forced shutdown of a single LogFileFeeder. Prevents renewal of
     * the lease, which will remove file protection held by its DbBackup.
     */
    public void shutdownClient(final int clientId) {
        try {
            final FeederCreator feederCreator = feederCreators.get(clientId);
            if (feederCreator != null) {
                feederCreator.preventLeaseRenewal();
                feederCreator.shutdown();
            }
        } catch (RuntimeException e) {
            logger.info(
                "Exception closing FeederCreator for clientId=" + clientId +
                " during NetworkRestore forced shutdown: " + e);
        }
    }

    @Override
    protected int initiateSoftShutdown() {
        /* Shutdown invoked from a different thread. */
        channelQueue.clear();
        /* Add special entry so that the channelQueue.poll operation exits. */
        channelQueue.add(RepUtils.CHANNEL_EOF_MARKER);
        return 0;
    }

    /**
     * Provides the lease mechanism used to maintain a handle to the DbBackup
     * object across Server client disconnects.
     */
    class Lease extends TimerTask {
        private final int id;
        private DbBackup dbBackup;

        public Lease(int id, long duration, DbBackup dbbackup) {
            super();
            this.dbBackup = dbbackup;
            this.id = id;
            Lease oldLease = leases.put(id, this);
            if (oldLease != null) {
                throw EnvironmentFailureException.unexpectedState
                    ("Found an old lease for node: " + id + ".  " +
                     "This can happen if network restore fails multiple " +
                     "times due to socket timeouts and there is only one " +
                     "available node that can service the request " +
                     "(such as in a RF=2 group).");
            }
            leaseTimer.schedule(this, duration);
        }

        @Override
        /* The timer went off, expire the lease if it hasn't been terminated */
        public synchronized void run() {
            if (dbBackup == null) {
                return;
            }
            dbBackup.endBackup();
            terminate();
        }

        /**
         * Fetches the leased DbBackup instance and terminates the lease.
         *
         * @return the dbBackup instance, if the lease hasn't already been
         * terminated
         */
        public synchronized DbBackup terminate() {
            if (dbBackup == null) {
                return null;
            }
            cancel();
            Lease l = leases.remove(id);
            assert(l == this);
            DbBackup saveDbBackup = dbBackup;
            dbBackup = null;
            return saveDbBackup;
        }
    }

    /**
     * @see StoppableThread#getLogger
     */
    @Override
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Non-zero value overrides {@link Protocol#MAX_VERSION}.
     */
    public void setTestProtocolMaxVersion(final int version) {
        testProtocolMaxVersion = version;
    }

    public int getTestProtocolMaxVersion() {
        return testProtocolMaxVersion;
    }

    /**
     * Non-zero value overrides {@link LogEntryType#LOG_VERSION}.
     */
    public void setTestProtocolLogVersion(final int version) {
        testProtocolLogVersion = version;
    }

    public int getTestProtocolLogVersion() {
        return testProtocolLogVersion;
    }
}
