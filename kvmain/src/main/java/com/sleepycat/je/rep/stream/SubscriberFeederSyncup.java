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

import static com.sleepycat.je.rep.stream.BaseProtocol.VERSION_8;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.BaseProtocol.AlternateMatchpoint;
import com.sleepycat.je.rep.stream.BaseProtocol.Entry;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryNotFound;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequestType;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Object to sync-up the Feeder and Subscriber to establish the VLSN from
 * which subscriber should should start stream log entries from feeder.
 */
public class SubscriberFeederSyncup {

    private final static int MIN_VER_PART_GEN_DB = VERSION_8;

    private final Logger logger;
    private final RepImpl repImpl;
    private final NamedChannel namedChannel;
    private final Protocol protocol;
    private final FeederFilter filter;
    private final EntryRequestType type;
    /* partition gen db name */
    private final String partGenDBName;

    /* partition gen db id */
    private DatabaseId partGenDBId;

    public SubscriberFeederSyncup(NamedChannel namedChannel,
                                  Protocol protocol,
                                  FeederFilter filter,
                                  RepImpl repImpl,
                                  EntryRequestType type,
                                  String partGenDBName,
                                  Logger logger) {
        this.namedChannel = namedChannel;
        this.protocol = protocol;
        this.filter = filter;
        this.repImpl = repImpl;
        this.type = type;
        this.partGenDBName = partGenDBName;
        this.logger = logger;

        partGenDBId = null;
    }

    /**
     * Execute sync-up to the Feeder.  Request Feeder to start a replication
     * stream from a start VLSN, if it is available. Otherwise return NULL
     * VLSN to subscriber.
     *
     * @param reqVLSN  VLSN requested by subscriber to stream log entries
     *
     * @return start VLSN from subscribe can stream log entries
     * @throws InternalException if fail to execute syncup
     */
    public long execute(long reqVLSN) throws InternalException {

        final long startTime = TimeSupplier.currentTimeMillis();

        LoggerUtils.info(logger, repImpl,
                         lm("Subscriber-Feeder=" +
                            namedChannel.getNameIdPair() + " syncup started" +
                            ", type=" + type + ", reqVLSN=" + reqVLSN +
                            ", filter=" + filter));

        try {
            /* query the start VLSN from feeder */
            final long startVLSN = getStartVLSNFromFeeder(reqVLSN);
            /* if part gen db name, query partition gen db id from feeder */
            if (partGenDBName != null && !partGenDBName.isEmpty()) {
                partGenDBId = getPartGenDBIdFromFeeder();
            }
            if (startVLSN != NULL_VLSN) {
                LoggerUtils.info(logger, repImpl,
                                 lm("Response from feeder=" +
                                    namedChannel.getNameIdPair() +
                                    ": the start vlsn=" + startVLSN +
                                    ", the requested vlsn=" + reqVLSN +
                                    ", send startStream request with filter=" +
                                    filter));

                /* start streaming from feeder if valid start VLSN */
                protocol.write(protocol.new StartStream(startVLSN, filter),
                               namedChannel);
            } else {
                LoggerUtils.info(logger, repImpl,
                                 lm("Unable to stream from feeder=" +
                                    namedChannel.getNameIdPair() +
                                    " from requested vlsn=" + reqVLSN));
            }
            return startVLSN;
        } catch (IllegalStateException | IOException e) {
            final String err = "Fail to sync-up with feeder, error=" + e;
            LoggerUtils.warning(logger, repImpl, lm(err));
            //TODO: downgrade to FINE if it no longer deserves INFO
            LoggerUtils.info(logger, repImpl, LoggerUtils.getStackTrace(e));
            throw new InternalException(err, e);
        } finally {
            LoggerUtils.info(
                logger, repImpl,
                lm(String.format("Subscriber to feeder=" +
                                 namedChannel.getNameIdPair() +
                                 " sync-up done, elapsed time=%,dms",
                                 TimeSupplier.currentTimeMillis() - startTime)));
        }
    }

    /**
     * Returns Partition Generation DB Id
     *
     * @return partition generation db id, null if not initialized
     */
    public DatabaseId getPartGenDBId() {
        return partGenDBId;
    }

    /**
     * Request a start VLSN from feeder. The feeder will return a valid
     * start VLSN, which can be equal to or earlier than the request VLSN,
     * or null if feeder is unable to service the requested VLSN.
     *
     * @param requestVLSN start VLSN requested by subscriber
     *
     * @return VLSN a valid start VLSN from feeder, or null if it unavailable
     * at the feeder
     * @throws IOException if unable to read message from channel
     * @throws IllegalStateException if the feeder sends an unexpected message
     */
    private long getStartVLSNFromFeeder(long requestVLSN)
            throws IOException, IllegalStateException {

        LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                           () -> lm("Subscriber send requested vlsn=" +
                                    requestVLSN + " to feeder=" +
                                    namedChannel.getNameIdPair()));

        /* ask the feeder for the requested VLSN. */
        protocol.write(protocol.new EntryRequest(requestVLSN, type),
                       namedChannel);

        /*
         * Expect the feeder to return one of following if type is
         * EntryRequestType.DEFAULT:
         *  a) not_found if the requested VLSN is too low
         *  b) the requested VLSN if the requested VLSN is found
         *  c) the alt match VLSN if the requested VLSN is too high
         *
         * If type is EntryRequestType.AVAILABLE:
         *  a) the lowest available VLSN if the requested VLSN is too low
         *  b) the requested VLSN if the requested VLSN is found
         *  c) the highest available VLSN if the request VLSN is too high

         * If type is EntryRequestType.NOW:
         *  a) always returns highest available VLSN
         */
        Message message = protocol.read(namedChannel);
        final long vlsn;
        /*
         * Could be a ping message since feeder is busy in scanning files
         */
        while(message instanceof Protocol.SyncupPing) {
            protocol.write(protocol.new SyncupPing(),namedChannel);
            message = protocol.read(namedChannel);
        }
        if (message instanceof Entry) {
            vlsn = ((Entry) message).getWireRecord().getVLSN();

            /* must be exact match for the default type */
            if (type.equals(EntryRequestType.DEFAULT)) {
                assert (vlsn == requestVLSN);
            }

            /* dump traces */
            if (vlsn == requestVLSN) {
                LoggerUtils.logMsg(
                    logger, repImpl, Level.FINEST,
                    () -> lm("Subscriber successfully requested vlsn=" +
                             requestVLSN + " from feeder=" +
                             namedChannel.getNameIdPair() +
                             ", request type=" + type));
            }

            if (vlsn < requestVLSN) {
                LoggerUtils.logMsg(
                    logger, repImpl, Level.FINEST,
                    () -> lm("Requested vlsn=" + requestVLSN +
                             " is not available from feeder=" +
                             namedChannel.getNameIdPair() +
                             " instead, start stream from a lowest " +
                             "available vlsn=" + vlsn +
                             ", request type=" + type));
            }

            if (vlsn > requestVLSN) {
                if (type.equals(EntryRequestType.NOW)) {
                    LoggerUtils.logMsg(
                        logger, repImpl, Level.FINEST,
                        () -> lm("Stream from highest available " +
                                 "vlsn from feeder=" +
                                 namedChannel.getNameIdPair() +
                                 ", vlsn=" + vlsn +
                                 ", request type:=" + type));
                } else {
                    LoggerUtils.logMsg(
                        logger, repImpl, Level.FINEST,
                        () -> lm("Requested vlsn=" + requestVLSN +
                                 " is not available from feeder=" +
                                 namedChannel.getNameIdPair() +
                                 " instead, start stream from a highest" +
                                 " available vlsn=" + vlsn +
                                 ", request type=" + type));
                }
            }

        } else if (message instanceof AlternateMatchpoint) {
            /* now and available type should not see alter match point */
            if (type.equals(EntryRequestType.NOW) ||
                type.equals(EntryRequestType.AVAILABLE)) {
                String msg = "Receive unexpected response " + message +
                             "from feeder " + namedChannel.getNameIdPair() +
                             ", request type: " + type;
                LoggerUtils.warning(logger, repImpl, lm(msg));
                throw new IllegalStateException(msg);
            }

            vlsn = ((AlternateMatchpoint) message).getAlternateWireRecord()
                                                   .getVLSN();
            /* must be an earlier VLSN */
            assert (vlsn < requestVLSN);
            LoggerUtils.logMsg(
                logger, repImpl, Level.FINEST,
                () -> lm("Feeder=" + namedChannel.getNameIdPair() +
                         " returns a valid start vlsn=" + vlsn +
                         " but earlier than requested vlsn=" +
                         requestVLSN + ", request type=" + type));

        } else  if (message instanceof EntryNotFound) {
            /* now and available type should not see not found */
            if (type.equals(EntryRequestType.NOW) ||
                type.equals(EntryRequestType.AVAILABLE)) {
                /*
                 * even for a brand new environment, the VLSN range at feeder
                 * is not empty so we should not see entry not found
                 */
                String msg = "Receive unexpected response=" + message +
                             " from feeder=" + namedChannel.getNameIdPair() +
                             ", requestVLSN=" + requestVLSN +
                             ", request type=" + type;
                LoggerUtils.warning(logger, repImpl, lm(msg));
                throw new IllegalStateException(msg);
            }

            vlsn = NULL_VLSN;
            LoggerUtils.logMsg(
                logger, repImpl, Level.FINEST,
                () -> lm("Feeder=" + namedChannel.getNameIdPair() +
                         " is unable to service the request vlsn=" +
                         requestVLSN + ", request type=" + type));
        } else {
            /* unexpected response from feeder */
            String msg = "Receive unexpected response=" + message +
                         "from feeder=" + namedChannel.getNameIdPair() +
                         ", request type=" + type;
            LoggerUtils.warning(logger, repImpl, lm(msg));
            throw new IllegalStateException(msg);
        }

        return vlsn;
    }

    /**
     * Requests the partition generation db id from feeder
     *
     * @return the partition generation db id
     *
     * @throws IOException if unable to read from channel
     * @throws IllegalStateException if receives unexpected response
     */
    private DatabaseId getPartGenDBIdFromFeeder()
        throws IOException, IllegalStateException {

        if(protocol.getVersion() < MIN_VER_PART_GEN_DB) {
            LoggerUtils.info(logger, repImpl,
                             lm("Client provides part gen db name while the " +
                                "server protocol (ver=" +
                                protocol.getVersion() +
                                ") is not upgraded to support it, min ver to " +
                                "support partition generation=" +
                                MIN_VER_PART_GEN_DB + ", ignore."));
            return null;
        }

        LoggerUtils.logMsg(
            logger, repImpl, Level.FINE,
            () -> lm("Subscriber sends request for partition generation " +
                     "db id for db=" + partGenDBName + " to feeder=" +
                     namedChannel.getNameIdPair()));

        /* ask the feeder for the db id. */
        protocol.write(protocol.new DBIdRequest(partGenDBName), namedChannel);

        /*
         * Since we have already passed the protocol negotiation, the feeder
         * shall support this protocol version and returns valid db id
         */
        final Message message = protocol.read(namedChannel);
        final DatabaseId id;
        if (message instanceof Protocol.DBIdResponse) {
            id = ((Protocol.DBIdResponse)message).getDbId();
            LoggerUtils.logMsg(
                logger, repImpl, Level.FINE,
                () -> lm("Subscriber successfully requested partition " +
                         "generation db id=" + id + " from feeder=" +
                         namedChannel.getNameIdPair()));
        } else {
            /* unexpected response from feeder */
            String msg = "Receive unexpected response=" + message +
                         "from feeder=" + namedChannel.getNameIdPair() +
                         ", request type=" + type;
            LoggerUtils.warning(logger, repImpl, lm(msg));
            throw new IllegalStateException(msg);
        }

        return id;
    }

    private String lm(String msg) {
        return "[SubscriberFeederSyncup-" + repImpl.getNodeName() + "] " + msg;
    }
}
