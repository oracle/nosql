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
package com.sleepycat.je.rep.stream;

import static com.sleepycat.je.utilint.VLSN.FIRST_VLSN;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.cleaner.FileProtector.ProtectedFileSet;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequest;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequestType;
import com.sleepycat.je.rep.stream.BaseProtocol.RestoreRequest;
import com.sleepycat.je.rep.stream.BaseProtocol.StartStream;
import com.sleepycat.je.rep.stream.ReplicaSyncupReader.SkipGapException;
import com.sleepycat.je.rep.subscription.StreamAuthenticator;
import com.sleepycat.je.rep.txn.ReadonlyTxn;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Establish where the replication stream should start for a feeder and replica
 * pair. The Feeder's job is to send the replica the parts of the replication
 * stream it needs, so that the two can determine a common matchpoint.
 *
 * If a successful matchpoint is found the feeder learns where to start the
 * replication stream for this replica.
 */
public class FeederReplicaSyncup {

    /**
     * When replay is possible, ideally we would use a cost-based approach to
     * decide whether to use replay vs network restore [#26890]. For now at
     * least avoid a "large" replay for a new/replaced/wiped node, which is
     * detected when VLSN 1 is requested. The max number of VLSNs to replay
     * in this situation is calculated to be one minute of replay time:
     *   Avg replay rate is 20k ops/s == 40k vlsn/s (due to commit VLSNs)
     *   40k vlsn/s * 60s/min == 2400k vlsn/min
     */
    private static final int MAX_INITIAL_REPLAY = 2400 * 1000;

    /* Overrides MAX_INITIAL_REPLAY for testing. */
    private static volatile int testMaxInitialReplay;

    /* A test hook that is called after a syncup has started. */
    private static volatile TestHook<Feeder> afterSyncupStartedHook;

    /* A test hook that is called after a syncup has ended. */
    private static volatile TestHook<Feeder> afterSyncupEndedHook;

    private static TestHook<FeederReplicaSyncup> sendPingHook;

    private static TestHook<Object> pauseHook;

    private final Feeder feeder;
    private final RepNode repNode;
    private final NamedChannel namedChannel;
    private final Protocol protocol;
    private final VLSNIndex vlsnIndex;
    private final Logger logger;
    private FeederSyncupReader backwardsReader;

    public FeederReplicaSyncup(Feeder feeder,
                               NamedChannel namedChannel,
                               Protocol protocol) {
        this.feeder = feeder;
        this.repNode = feeder.getRepNode();
        logger = LoggerUtils.getLogger(getClass());
        this.namedChannel = namedChannel;
        this.protocol = protocol;
        this.vlsnIndex = repNode.getVLSNIndex();
    }

    /**
     * The feeder's side of the protocol. Find out where to start the
     * replication stream.
     *
     * @throws NetworkRestoreException if sync up failed and network store is
     * required
     * @throws ChecksumException if checksum validation failed
     */
    public void execute()
        throws DatabaseException, IOException,
               NetworkRestoreException, ChecksumException {

        final long startTime = TimeSupplier.currentTimeMillis();
        RepImpl repImpl = repNode.getRepImpl();
        LoggerUtils.info(logger, repImpl,
                         "Feeder-replica " +
                         feeder.getReplicaNameIdPair().getName() +
                         " syncup started. Feeder range: " +
                         repNode.getVLSNIndex().getRange());

        /*
         * Prevent the VLSNIndex range from being changed and protect all files
         * in the range. To search the index and read files within this range
         * safely, VLSNIndex.getRange must be called after syncupStarted.
         */
        final ProtectedFileSet protectedFileSet =
            repNode.syncupStarted(feeder.getReplicaNameIdPair());

        try {
            assert TestHookExecute.doHookIfSet(afterSyncupStartedHook, feeder);

            /*
             * Wait for the replica to start the syncup message exchange. The
             * first message will always be an EntryRequest. This relies on the
             * fact that a brand new group always begins with a master that has
             * a few vlsns from creating the nameDb that exist before a replica
             * syncup. The replica will never issue a StartStream before doing
             * an EntryRequest.
             *
             * The first entry request has three possible types of message
             * responses - EntryNotFound, AlternateMatchpoint, or Entry.
             */
            VLSNRange range = vlsnIndex.getRange();
            EntryRequest firstRequest =
                (EntryRequest) protocol.read(namedChannel);
            Message response = makeResponseToEntryRequest(range,
                                                          firstRequest,
                                                          true);

            protocol.write(response, namedChannel);

            /*
             * Now the replica may send one of three messages:
             * - a StartStream message indicating that the replica wants to
             * start normal operations
             * - a EntryRequest message if it's still hunting for a
             * matchpoint. There's the possibility that the new EntryRequest
             * asks for a VLSN that has been log cleaned, so check that we can
             * supply it.
             * - a RestoreRequest message that indicates that the replica
             * has given up, and will want a network restore.
             */

            long startVLSN = INVALID_VLSN;
            while (true) {
                Message message = protocol.read(namedChannel);
                if (logger.isLoggable(Level.FINEST)) {
                    LoggerUtils.finest(logger, repImpl,
                                       "Replica " +
                                       feeder.getReplicaNameIdPair() +
                                       " message op: " + message.getOp());
                }
                if (message instanceof StartStream) {
                    final StartStream startMessage = (StartStream) message;
                    startVLSN = startMessage.getVLSN();

                    /* set feeder filter */
                    final FeederFilter filter = startMessage.getFeederFilter();
                    if (filter != null) {
                        filter.setStartVLSN(startVLSN);
                        filter.setLogger(logger);
                        final String filterId =
                            feeder.getReplicaNameIdPair().getName() +
                            ", channel=" +
                            namedChannel.getChannel().getChannelId();
                        filter.setFeederFilterId(filterId);
                    }
                    feeder.setFeederFilter(filter);

                    /*
                     * skip security check if not needed, e.g., a replica in
                     * a secure store
                     */
                    if (!feeder.needSecurityChecks()) {
                        break;
                    }

                    final StreamAuthenticator auth =
                        namedChannel.getChannel().getStreamAuthenticator();
                    /* if security check is needed, auth cannot be null */
                    assert (auth != null);
                    /* remember table id strings of subscribed tables */
                    if (filter != null) {
                        auth.setTableIds(filter.getTableIds());
                    } else {
                        /* if no filter, subscribe all tables */
                        auth.setTableIds(null);
                    }
                    /* security check */
                    if (!auth.checkAccess()) {
                        final String err = "Replica " +
                                           feeder.getReplicaNameIdPair()
                                                 .getName() +
                                           " fails security check " +
                                           "in start stream syncup";
                        LoggerUtils.warning(logger, repImpl, err);

                        /* signal client */
                        feeder.makeSecurityCheckResponse(err);
                    }
                    break;
                } else if (message instanceof Protocol.SyncupPing) {
                    protocol.write(protocol.new SyncupPing(),namedChannel);
                } else if (message instanceof EntryRequest) {
                    response = makeResponseToEntryRequest
                        (range, (EntryRequest) message, false);
                    protocol.write(response, namedChannel);
                } else if (message instanceof RestoreRequest) {
                    throw answerRestore(range,
                                        ((RestoreRequest) message).getVLSN());
                } else if (message instanceof Protocol.DBIdRequest) {
                    final String dbName =
                        ((Protocol.DBIdRequest) message).getDbName();
                    response = makeResponseToDBIdRequest(repNode, dbName);
                    protocol.write(response, namedChannel);
                } else {
                    throw EnvironmentFailureException.unexpectedState
                        (repImpl,
                         "Expected StartStream or EntryRequest but got " +
                         message);
                }
            }

            LoggerUtils.info(logger, repImpl,
                             "Feeder-replica " +
                             feeder.getReplicaNameIdPair().getName() +
                             " start stream at VLSN: " + startVLSN);

            feeder.initMasterFeederSource(startVLSN);

        } finally {
            repNode.syncupEnded(protectedFileSet);
            assert TestHookExecute.doHookIfSet(afterSyncupEndedHook, feeder);
            assert TestHookExecute.doHookIfSet(sendPingHook,this);
            LoggerUtils.info
                (logger, repImpl,
                 String.format("Feeder-replica " +
                               feeder.getReplicaNameIdPair().getName() +
                               " syncup ended. Elapsed time: %,dms",
                               (TimeSupplier.currentTimeMillis() - startTime)));

        }
    }

    private static int getMaxInitialReplay() {
        return testMaxInitialReplay != 0 ?
            testMaxInitialReplay : MAX_INITIAL_REPLAY;
    }

    /** For testing. */
    public static void setTestMaxInitialReplay(int val) {
        testMaxInitialReplay = val;
    }

    /** For testing. */
    public static void setAfterSyncupStartedHook(TestHook<Feeder> hook) {
        afterSyncupStartedHook = hook;
    }

    /** For testing. */
    public static void setAfterSyncupEndedHook(TestHook<Feeder> hook) {
        afterSyncupEndedHook = hook;
    }

    public static void setSendPingHook(TestHook<FeederReplicaSyncup> hook) {
        sendPingHook = hook;
    }

    public static void setPauseHook(TestHook<Object> pauseHook) {
        FeederReplicaSyncup.pauseHook = pauseHook;
    }

    public void doPauseHook()
    {
        if (pauseHook != null) {
            backwardsReader.setTimesChecked(1000);
            pauseHook.doHook();
        }
    }

    private FeederSyncupReader setupReader(long startVLSN)
        throws DatabaseException, IOException {

        EnvironmentImpl envImpl = repNode.getRepImpl();
        int readBufferSize = envImpl.getConfigManager().
            getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        /*
         * A BackwardsReader for scanning the log file backwards.
         */
        long lastUsedLsn = envImpl.getFileManager().getLastUsedLsn();

        long firstVLSN = vlsnIndex.getRange().getFirst();
        long firstFile = vlsnIndex.getLTEFileNumber(firstVLSN);
        long finishLsn = DbLsn.makeLsn(firstFile, 0);
        return new FeederSyncupReader(envImpl,
                                      vlsnIndex,
                                      lastUsedLsn,
                                      readBufferSize,
                                      startVLSN,
                                      finishLsn,
                                      this);
    }

    /**
     * Ping messages will be sent in a fixed frequency during a long scanning
     * of log files to keep the channel alive
     * <p>
     * See FeederReader.sendPingMessage() and
     * FeederSyncupReader.sendPingMessage()
     */
    public void sendPingMessage() throws IOException {
        if (feeder.canSendPingMessage()) {
            protocol.write(protocol.new SyncupPing(), namedChannel);
            Message message = protocol.read(namedChannel);
            assert message instanceof Protocol.SyncupPing;
        }
    }

    /**
     * For unit test
     */
    public void responsePingMessage() throws IOException {
        if(feeder.canSendPingMessage()) {
            Message message = protocol.read(namedChannel);
            assert message instanceof Protocol.SyncupPing;
            protocol.write(protocol.new SyncupPing(), namedChannel);
        }
    }

    private Message makeResponseToDBIdRequest(RepNode rn, String dbName) {
        final DatabaseId dbId = getDBId(rn.getRepImpl(), dbName);
        return protocol.new DBIdResponse(dbId);
    }

    /**
     * Returns db id for given database
     *
     * @param repImpl replication env
     * @param dbName  db name
     *
     * @return th db id of given db
     * @throws DatabaseNotFoundException if db id not found
     */
    public static DatabaseId getDBId(RepImpl repImpl, String dbName)
        throws DatabaseNotFoundException {

        final DbTree dbTree = repImpl.getDbTree();

        /* need a locker to call getDbIdFromName() */
        final TransactionConfig txnConfig = new TransactionConfig();
        final Txn txn = new ReadonlyTxn(repImpl, txnConfig);
        final DatabaseId dbId;
        try {
            dbId = dbTree.getDbIdFromName(txn, dbName, null, false);
            txn.commit();
        } catch (Exception exp) {
            txn.abort();
            throw exp;
        }

        /* must find the db to proceed */
        if (dbId == null) {
            throw new DatabaseNotFoundException("Cannot find db id for " +
                                                "JE database " + dbName);
        }
        return dbId;
    }

    private Message makeResponseToEntryRequest(VLSNRange range,
                                               EntryRequest request,
                                               boolean isFirstResponse)
        throws IOException, ChecksumException {

        final long requestMatchpoint = request.getVLSN();
        final EntryRequestType type = request.getType();

        /* if NOW mode, return high end regardless of requested vlsn */
        if (type.equals(EntryRequestType.NOW)) {
            /*
             * VLSN range is not empty even without user data, so we can
             * always get a valid entry.
             */
            return protocol.new Entry(getMatchPtRecord(range.getLast()));
        }

        /* stream modes other than NOW */

        /*
         * The matchpoint must be in the VLSN range, or more specifically, in
         * the VLSN index so we can map the VLSN to the lsn in order to fetch
         * the associated log record.
         */
        if (range.getFirst() > requestMatchpoint) {
            /* request point is smaller than lower bound of range */
            if (type.equals(BaseProtocol.EntryRequestType.AVAILABLE)) {
                return protocol.new Entry(getMatchPtRecord(range.getFirst()));
            }

            /* default mode */
            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Matchpoint vlsn=" + requestMatchpoint +
                             " lower than range=" + range +
                             ", request type=" + type);
            return protocol.new EntryNotFound();
        }

        if (range.getLast() < requestMatchpoint) {
            /* request point is higher than upper bound of range */
            if (type.equals(EntryRequestType.AVAILABLE)) {
                return protocol.new Entry(getMatchPtRecord(range.getLast()));
            }

            /*
             * default mode:
             *
             * The matchpoint is after the last one in the range. We have to
             * suggest the lastSync entry on this node as an alternative. This
             * should only happen on the feeder's first response. For example,
             * suppose the feeder's range is vlsns 1-100. It's possible that
             * the exchange is as follows:
             *  1 - replica has 1-110, asks feeder for 110
             *  2 - feeder doesn't have 110, counters with 100
             *  3 - from this point on, the replica should only ask for vlsns
             *      that are <= the feeder's counter offer of 100
             * Guard that this holds true, because the feeder's log reader is
             * only set to search backwards; it does not expect to toggle
             * between forward and backwards.
             */
            assert backwardsReader == null :
              "Replica request for vlsn > feeder range should only happen " +
              "on the first exchange.";

            if (range.getLastSync() == NULL_VLSN) {
                /*
                 * We have no syncable entry at all. The replica will have to
                 * do a network restore.
                 */
                LoggerUtils.info(logger, repNode.getRepImpl(),
                                 "Matchpoint vlsn=" + requestMatchpoint +
                                 " higher than range=" + range +
                                 ", request type=" + type +
                                 ", no syncable vlsn");
                return protocol.new EntryNotFound();
            }

            if (isFirstResponse) {
                final OutputWireRecord lastSync =
                    getMatchPtRecord(range.getLastSync());
                assert lastSync != null :
                "Look for alternative, range=" + range;
                return protocol.new AlternateMatchpoint(lastSync);
            }

            throw EnvironmentFailureException.unexpectedState
                (repNode.getRepImpl(), "RequestMatchpoint=" +
                 requestMatchpoint + " range=" + range +
                 "should only happen on first response");
        }

        /*
         * When VLSN 1 is requested the feeder will respond with EntryNotFound
         * even when it has VLSN 1, if the replay range is "large". The
         * intention is to avoid long replays for new/replaced/wiped nodes.
         *
         * Note the optimization does not apply to the subscription.
         */
        if (!feeder.isSubscription() &&
            requestMatchpoint == FIRST_VLSN &&
            range.getLast() > getMaxInitialReplay()) {
            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Matchpoint vlsn=" + requestMatchpoint +
                             " inside range=" + range +
                             ", request type=" + type +
                             ", last vlsn greater than max init replay=" +
                             getMaxInitialReplay());
            return protocol.new EntryNotFound();
        }

        /* The matchpoint is within the range. Find it. */
        final OutputWireRecord matchRecord =
            getMatchPtRecord(requestMatchpoint);
        if (matchRecord == null) {
            throw EnvironmentFailureException.unexpectedState
                (repNode.getRepImpl(),
                 "Couldn't find matchpoint " + requestMatchpoint +
                 " in log. VLSN range=" + range);
        }

        return protocol.new Entry(matchRecord);
    }

    /* scan log backwards to find match point record */
    private OutputWireRecord getMatchPtRecord(long matchPointVLSN)
        throws IOException, ChecksumException {

        if (backwardsReader == null) {
            backwardsReader = setupReader(matchPointVLSN);

            doPauseHook();
        }
        OutputWireRecord feederRecord = null;
        try {
            feederRecord = backwardsReader.scanBackwards(matchPointVLSN);
        } catch (SkipGapException ske) {
            /*
             * The matchpoint VLSN is neither in GTEBucket nor in LTEBucket.
             * Set up a forwardReader from a LTELsn to find this LSN.
             * 
             * It is guranteed that LTEBucket will exist. So, a forward search
             * is guranteed to find the matchpoint VLSN if it exists in the
             * Log files.
             */

            int readBufferSize =
                    repNode.getRepImpl().getConfigManager()
                           .getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);

            FeederReader forwardReader =
                    new FeederReader(repNode.getRepImpl(), vlsnIndex,
                                     DbLsn.NULL_LSN, readBufferSize,this);

            try {
                forwardReader.initScan(matchPointVLSN);
                feederRecord = forwardReader.scanForwards(
                    matchPointVLSN, 0, false);
            } catch (InterruptedException e1) {
                ske.addSuppressed(e1);
                throw ske;
            }
        }
        return feederRecord;
    }

    public boolean isNamedChannelOpen() {
        return namedChannel.isOpen();
    }

    private NetworkRestoreException answerRestore(VLSNRange range,
                                                  long failedMatchpoint)
        throws IOException {

        Message response =
            protocol.new RestoreResponse(repNode.getLogProviders());
        protocol.write(response, namedChannel);

        return new NetworkRestoreException(failedMatchpoint,
                                           range.getFirst(),
                                           range.getLast(),
                                           feeder.getReplicaNameIdPair());
    }

    @SuppressWarnings("serial")
    static public class NetworkRestoreException extends Exception
        implements NotSerializable {

        /* The out-of-range vlsn that provoked the exception */
        private final long vlsn;
        private final long firstVLSN;
        private final long lastVLSN;

        /* The replica that made the request. */
        private final NameIdPair replicaNameIdPair;

        public NetworkRestoreException(long vlsn,
                                       long firstVLSN,
                                       long lastVLSN,
                                       NameIdPair replicaNameIdPair) {
            this.vlsn = vlsn;
            this.firstVLSN = firstVLSN;
            this.lastVLSN = lastVLSN;
            this.replicaNameIdPair = replicaNameIdPair;
        }

        @Override
        public String getMessage() {
           return ((firstVLSN <= vlsn) &&
                   (lastVLSN >= vlsn)) ?
                  ("Node: " + replicaNameIdPair +
                   " has an intervening checkpoint," +
                   " or an intervening gap in log files possibly" +
                   " implying the absence of a checkpoint, " +
                   " after the matchpoint at:" + vlsn +
                   " and cannot safely rollback to resume streaming"):
                  ("Matchpoint vlsn " + vlsn + " requested by node: " +
                   replicaNameIdPair + " was outside the VLSN range: " +
                   "[" + firstVLSN + "-" + lastVLSN + "]");
        }

        public long getVlsn() {
            return vlsn;
        }

        public NameIdPair getReplicaNameIdPair() {
            return replicaNameIdPair;
        }
    }
}
