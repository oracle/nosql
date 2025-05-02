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

import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_ACK_MESSAGES;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_GROUPED_ACKS;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_GROUP_ACK_MESSAGES;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MAX_GROUPED_ACKS;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Level;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.beforeimage.BeforeImageOutputWireRecord;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.utilint.BinaryProtocol;
import com.sleepycat.je.rep.utilint.RepUtils.Clock;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongMaxStat;
import com.sleepycat.je.utilint.LongMaxZeroStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.VLSN;

/**
 * Defines the base protocol of messages used to set up a stream between
 * source and target.
 *
 * Note BaseProtocol represents a set of basic protocol operations intended to
 * be used by subclasses. For a complete description of message operations
 * used in JE HA protocol, please see the Protocol class in the same package.
 *
 * @see com.sleepycat.je.rep.stream.Protocol
 */
public abstract class BaseProtocol extends BinaryProtocol {

    /* --------------------------- */
    /* ---  protocol versions  --- */
    /* --------------------------- */

    /*
     * Note that the GROUP_ACK response message was introduced in version 5,
     * but is disabled by default via RepParams.REPLICA_ENABLE_GROUP_ACKS.
     *
     * It can be enabled when we can increase the protocol version number.
     */

    /* The default (highest) version supported by the Protocol code. */
    public static final int MAX_VERSION = 15;

    /* The minimum version we're willing to interact with. */
    static final int MIN_VERSION = 3;

    /**
     * Version added in JE 25.1.0 to support before images
     */
    public static final int VERSION_15 = 15;

    public static final JEVersion VERSION_15_JE_VERSION =
            new JEVersion("25.1.0");


    /**
     * Version added in JE 24.1.0 to support ping
     * message during sycnup, which keeps the channel
     * alive
     */
    public static final int VERSION_14 = 14;

    public static final JEVersion VERSION_14_JE_VERSION =
            new JEVersion("24.1.0");

    /**
     * Version added in JE 21.3.0 to support max clock skew check in heartbeat,
     * heartbeat response:
     * - sender timestamp
     */
    public static final int VERSION_13 = 13;

    public static final JEVersion VERSION_13_JE_VERSION =
        new JEVersion("21.3.0");

    /**
     * Version added in JE 20.2.0 to support following filter stat in heartbeat
     * messages:
     * - last feeder commit time
     * - vlsn of last operation processed by filter
     * - vlsn of last operation passing filter
     * - modification time of last operation processed by filter
     */
    public static final int VERSION_12 = 12;

    public static final JEVersion VERSION_12_JE_VERSION =
        new JEVersion("20.2.0");

    /**
     * Version added in JE 19.5.1 to support storing heartbeat IDs in heartbeat
     * messages.
     */
    public static final int VERSION_11 = 11;

    public static final JEVersion VERSION_11_JE_VERSION =
        new JEVersion("19.5.1");

    /*
     * Version added in JE 19.2.0 to support dynamic filter change in
     * subscription api
     */
    public static final int VERSION_10 = 10;

    public static final JEVersion VERSION_10_JE_VERSION =
        new JEVersion("19.2.0");

    /*
     * Version added in JE 18.3.4 to support return security check failure
     * response to stream client
     */
    public static final int VERSION_9 = 9;

    public static final JEVersion VERSION_9_JE_VERSION =
        new JEVersion("18.3.4");

    /*
     * Version added in JE 18.3.1 to support partition gen table db id look up
     */
    public static final int VERSION_8 = 8;

    public static final JEVersion VERSION_8_JE_VERSION =
        new JEVersion("18.3.1");

    /*
     * Version added in JE 7.5.6 to support entry request type
     */
    public static final int VERSION_7 = 7;
    public static final JEVersion VERSION_7_JE_VERSION =
        new JEVersion("7.5.6");

    /*
     * Version added in JE 6.4.10 to support generic feeder filtering
     */
    public static final int VERSION_6 = 6;
    public static final JEVersion VERSION_6_JE_VERSION =
        new JEVersion("6.4.10");

    /* Version added in JE 6.0.1 to support RepGroupImpl version 3. */
    public static final int VERSION_5 = 5;
    public static final JEVersion VERSION_5_JE_VERSION =
        new JEVersion("6.0.1");

    /*
     * Version in which HEARTBEAT_RESPONSE added a second field.  We can manage
     * without this optional additional information if we have to, we we can
     * still interact with the previous protocol version.  (JE 5.0.58)
     */
    static final int VERSION_4 = 4;
    public static final JEVersion VERSION_4_JE_VERSION =
        new JEVersion("5.0.58");

    /* Version added in JE 4.0.50 to address byte order issues. */
    static final int VERSION_3 = 3;
    public static final JEVersion VERSION_3_JE_VERSION =
        new JEVersion("4.0.50");

    /* ------------------------------------------ */
    /* ---  messages defined in base protocol --- */
    /* ------------------------------------------ */

    /* range of op codes allowed in subclasses, inclusively. */
    protected final static short MIN_MESSAGE_OP_CODE_IN_SUBCLASS = 1024;
    protected final static short MAX_MESSAGE_OP_CODE_IN_SUBCLASS = 2047;

    /*
     * Following ops are core replication stream post-handshake messages
     * defined in streaming protocol and are intended to be used in subclasses.
     *
     * Note these msg op codes inherit from original implementation of stream
     * protocol. Due to backward compatibility requirement, we keep them
     * unchanged and directly copy them here.
     */
    public final MessageOp ENTRY =
        new MessageOp((short) 101, Entry.class, (ByteBuffer buffer)->{
            return new Entry(buffer);
        });

    public final MessageOp START_STREAM =
        new MessageOp((short) 102, StartStream.class, (ByteBuffer buffer)->{
            return new StartStream(buffer);
        });

    public static final Class<? extends Message>
        HEARTBEAT_CLASS = Heartbeat.class;
    public final MessageOp HEARTBEAT =
        new MessageOp((short) 103, Heartbeat.class, (ByteBuffer buffer)->{
            return new Heartbeat(buffer);
        });

    public static final Class<? extends Message>
        HEARTBEAT_RESPONSE_CLASS = HeartbeatResponse.class;
    public final MessageOp HEARTBEAT_RESPONSE =
        new MessageOp((short) 104, HeartbeatResponse.class,
            (ByteBuffer buffer) -> { return new HeartbeatResponse(buffer);
        });

    public static final Class<? extends Message> COMMIT_CLASS = Commit.class;
    public final MessageOp COMMIT =
        new MessageOp((short) 105, Commit.class, (ByteBuffer buffer)->{
            return new Commit(buffer);
        });

    public static final Class<? extends Message> ACK_CLASS = Ack.class;
    public final MessageOp ACK =
        new MessageOp((short) 106, Ack.class, (ByteBuffer buffer)->{
            return new Ack(buffer);
        });

    public final MessageOp ENTRY_REQUEST =
        new MessageOp((short) 107, EntryRequest.class, (ByteBuffer buffer)->{
            return new EntryRequest(buffer);
        });

    public final MessageOp ENTRY_NOTFOUND =
        new MessageOp((short) 108, EntryNotFound.class, (ByteBuffer buffer)->{
            return new EntryNotFound(buffer);
        });

    public final MessageOp ALT_MATCHPOINT =
        new MessageOp((short) 109, AlternateMatchpoint.class ,
            (ByteBuffer buffer)->{ return new AlternateMatchpoint(buffer);
        });

    public final MessageOp RESTORE_REQUEST =
        new MessageOp((short) 110, RestoreRequest.class, (ByteBuffer buffer)->{
            return new RestoreRequest(buffer);
        });

    public final MessageOp RESTORE_RESPONSE =
        new MessageOp((short) 111, RestoreResponse.class,
            (ByteBuffer buffer)->{return new RestoreResponse(buffer);
        });

    public static final Class<? extends Message>
        SHUTDOWN_REQUEST_CLASS = ShutdownRequest.class;
    public final MessageOp SHUTDOWN_REQUEST =
        new MessageOp((short) 112, ShutdownRequest.class,
            (ByteBuffer buffer)->{ return new ShutdownRequest(buffer);
        });

    public static final Class<? extends Message>
        SHUTDOWN_RESPONSE_CLASS = ShutdownResponse.class;
    public final MessageOp SHUTDOWN_RESPONSE =
        new MessageOp((short) 113, ShutdownResponse.class,
            (ByteBuffer buffer)->{ return new ShutdownResponse(buffer);
        });

    public static final Class<? extends Message>
        GROUP_ACK_CLASS = GroupAck.class;
    public final MessageOp GROUP_ACK =
        new MessageOp((short) 114, GroupAck.class, (ByteBuffer buffer)->{
            return new GroupAck(buffer);
        });

    public final MessageOp ENTRY_WITH_BEFORE_IMAGE =
        new MessageOp((short) 115, Entry.class, (ByteBuffer buffer)->{
            return new Entry(buffer, true);
        });

    /* --------------------------- */
    /* --------  fields  --------- */
    /* --------------------------- */

    /** The log version of the format used to write log entries to the stream. */
    protected int streamLogVersion;

    /* Count of all singleton ACK messages. */
    protected final LongStat nAckMessages;

    /* Count of all group ACK messages. */
    protected final LongStat nGroupAckMessages;

    /* Sum of all acks sent via group ACK messages. */
    protected final LongStat nGroupedAcks;

    /* Max number of acks sent via a single group ACK message. */
    protected final LongMaxStat nMaxGroupedAcks;

    protected final RepImpl repImpl;

    /**
     * Whether to fix the log version for log entries received from JE 7.0.x
     * feeders that use log version 12 format but are incorrectly marked with
     * later log versions due to a bug ([#25222]).  The problem is that the
     * feeder supplies an entry in log version 12 (LOG_VERSION_EXPIRE_INFO)
     * format, but says it has a later log version.
     *
     * <p>This field is only set to true by the replica, which only reads and
     * writes it from the main Replica thread, so no synchronization is needed.
     */
    private boolean fixLogVersion12Entries = false;


    /** The maximum clock skew among sender and receiver. */
    private final long maxClockSkew;
    /** A skewed clock for testing. */
    private final Clock clock;

    /**
     * Returns a BaseProtocol object configured that implements the specified
     * (supported) protocol version.
     *
     * @param repImpl the node using the protocol
     *
     * @param nameIdPair name-id pair of the node using the protocol
     *
     * @param remote the remote node's name-id pair
     *
     * @param protocolVersion the version of the protocol that must be
     *        implemented by this object
     *
     * @param streamLogVersion the log version of the format used to write log
     *        entries
     *
     * @param protocolOps the message operations that make up this protocol
     *
     * @param checkValidity whether to check the message operations for
     *        validity.  Checks should be performed for new protocols, but
     *        suppressed for legacy ones.
     */
    BaseProtocol(final RepImpl repImpl,
                 final NameIdPair nameIdPair,
                 final NameIdPair remote,
                 final int protocolVersion,
                 final int streamLogVersion,
                 final MessageOp[] protocolOps,
                 final boolean checkValidity) {
        super(nameIdPair, remote, protocolVersion, repImpl);
        this.streamLogVersion = streamLogVersion;
        this.repImpl = repImpl;

        nAckMessages = new LongStat(stats, N_ACK_MESSAGES);
        nGroupAckMessages = new LongStat(stats, N_GROUP_ACK_MESSAGES);
        nGroupedAcks = new LongStat(stats, N_GROUPED_ACKS);
        nMaxGroupedAcks = new LongMaxZeroStat(stats, N_MAX_GROUPED_ACKS);
        initializeMessageOps(protocolOps, checkValidity);

        if (repImpl != null) {
            maxClockSkew = repImpl.getConfigManager()
                .getDuration(RepParams.MAX_CLOCK_DELTA);
            clock = new Clock(repImpl.getConfigManager()
                              .getDuration(RepParams.HEARTBEAT_CLOCK_SKEW));
        } else {
            maxClockSkew = 0;
            clock = new Clock(0);
        }
    }

    public int getStreamLogVersion() {
        return streamLogVersion;
    }

    /**
     * Invoked in cases where the stream log version is not known at the time
     * the protocol object is created and stream log version negotiations are
     * required to determine the version that will be used for log records sent
     * over the HA stream.
     *
     * @param streamLogVersion the maximum log version associated with stream
     * records
     */
    public void setStreamLogVersion(int streamLogVersion) {
        this.streamLogVersion = streamLogVersion;
    }

    /**
     * Returns whether log entries need their log versions fixed to work around
     * [#25222].
     */
    public boolean getFixLogVersion12Entries() {
        return fixLogVersion12Entries;
    }

    /**
     * Sets whether log entries need their log versions fixed to work around
     * [#25222].
     */
    public void setFixLogVersion12Entries(boolean value) {
        fixLogVersion12Entries = value;
    }

    /* ------------------------------------------------- */
    /* ---  message classes defined in base protocol --- */
    /* ------------------------------------------------- */

    /**
     * A message containing a log entry in the replication stream.
     */
    public class Entry extends Message {
        /*
         * The time when the entry was created. It's used for metrics. Note
         * that it's not serialized and sent across the wire.
         */
        private transient final long createNs = System.nanoTime();
        /*
         * InputWireRecord is set when this Message had been received at this
         * node. OutputWireRecord is set when this message is created for
         * sending from this node.
         */
        final protected InputWireRecord inputWireRecord;
        protected OutputWireRecord outputWireRecord;

        public Entry(final OutputWireRecord outputWireRecord) {
            inputWireRecord = null;
            this.outputWireRecord = outputWireRecord;
        }

        @Override
        public MessageOp getOp() {
            if (outputWireRecord instanceof BeforeImageOutputWireRecord) {
                return ENTRY_WITH_BEFORE_IMAGE;
            }
            return ENTRY;
        }

        /**
         * @return the System.nanoTime when the Entry was created.
         */
        public long getCreateNs() {
            return createNs;
        }

        @Override
        public ByteBuffer wireFormat() {
            final int bodySize = getWireSize();
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            writeOutputWireRecord(outputWireRecord, messageBuffer);
            messageBuffer.flip();
            if (bodySize + MESSAGE_HEADER_SIZE != messageBuffer.limit()) {
                throw EnvironmentFailureException.unexpectedState(
                    underflowMessage(bodySize, messageBuffer.limit()));
            }
            return messageBuffer;
        }

        private String underflowMessage(int calculatedSize,
                                        int sizeInBuffer) {
            return "Underflow when serializing wire record." +
                " calculatedSize=" + calculatedSize +
                " expectedSizeInBuffer=" +
                (calculatedSize + MESSAGE_HEADER_SIZE) +
                " actualSizeInBuffer=" + sizeInBuffer +
                " entrySize=" + outputWireRecord.header.getSize() +
                " entryType=" + outputWireRecord.header.getType() +
                " VLSN=" + outputWireRecord.header.getVLSN();
        }

        protected int getWireSize() {
            return outputWireRecord.getWireSize(streamLogVersion);
        }

        public Entry(final ByteBuffer buffer)
            throws DatabaseException {

            inputWireRecord =
                new InputWireRecord(repImpl, buffer, BaseProtocol.this);
        }

        public Entry(final ByteBuffer buffer, boolean hasBeforeImage)
            throws DatabaseException {

            inputWireRecord =
                new InputWireRecord(repImpl, buffer, BaseProtocol.this,
                                    hasBeforeImage);
        }

        public InputWireRecord getWireRecord() {
            return inputWireRecord;
        }

        public InputWireRecord getWireRecordWithBeforeImage() {
            return inputWireRecord;
        }

        @Override
        public String toString() {

            final StringBuilder sb = new StringBuilder();
            sb.append(super.toString());

            if (inputWireRecord != null) {
                sb.append(" ");
                sb.append(inputWireRecord);
            }

            if (outputWireRecord != null) {
                sb.append(" ");
                sb.append(outputWireRecord);
            }

            return sb.toString();
        }

        /* For unit test support */
        @Override
        public boolean match(Message other) {

            /*
             * This message was read in, but we need to compare it to a message
             * that was sent out.
             */
            if (outputWireRecord == null) {
                outputWireRecord = new OutputWireRecord(repImpl,
                                                        inputWireRecord);
            }
            return super.match(other);
        }

        /* True if the log entry is a TxnAbort or TxnCommit. */
        public boolean isTxnEnd() {
            final byte entryType = getWireRecord().getEntryType();
            return LogEntryType.LOG_TXN_COMMIT.equalsType(entryType) ||
                   LogEntryType.LOG_TXN_ABORT.equalsType(entryType);

        }
    }

    /**
     * StartStream indicates that the replica would like the feeder to start
     * the replication stream at the proposed vlsn.
     */
    public class StartStream extends VLSNMessage {
        private final FeederFilter feederFilter;

        StartStream(long startVLSN) {
            super(startVLSN);
            feederFilter = null;
        }

        StartStream(long startVLSN, FeederFilter filter) {
            super(startVLSN);
            feederFilter = filter;
        }

        public StartStream(ByteBuffer buffer) {
            super(buffer);

            /* Feeder filtering not supported before protocol version 6 */
            if (getVersion() < VERSION_6) {
                feederFilter = null;
                return;
            }

            final int length = LogUtils.readInt(buffer);
            if (length == 0) {
                /* no filter is provided by client */
                feederFilter = null;
                return;
            }

            /* reconstruct filter from buffer */
            final byte filterBytes[] =
                LogUtils.readBytesNoLength(buffer, length);
            final ByteArrayInputStream bais =
                new ByteArrayInputStream(filterBytes);
            ObjectInputStream ois = null;
            try {
                ois = new ObjectInputStream(bais);
                feederFilter = (FeederFilter) ois.readObject();
            } catch (ClassNotFoundException | IOException e) {
                logger.warning(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            } finally {
                if (ois != null) {
                    try {
                        ois.close();
                    } catch (IOException e) {
                        logger.finest("exception raised when closing the " +
                                      "object input stream object " +
                                      e.getLocalizedMessage());
                    }
                }
            }
        }

        public FeederFilter getFeederFilter() {
            return feederFilter;
        }

        @Override
        public ByteBuffer wireFormat() {
            /* Feeder filtering not supported before protocol version 6 */
            if (getVersion() < VERSION_6) {
                return super.wireFormat();
            }

            final int feederBufferSize;
            final byte[] filterBytes;

            if (feederFilter != null) {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = null;
                try {
                    oos = new ObjectOutputStream(baos);
                    oos.writeObject(feederFilter);
                    oos.flush();
                } catch (IOException e) {
                    logger.warning(e.getLocalizedMessage());
                    throw new IllegalStateException(e);
                } finally {
                    if (oos != null) {
                        try {
                            oos.close();
                        } catch (IOException e) {
                            logger.finest("exception raised when closing the " +
                                          "object output stream object " +
                                          e.getLocalizedMessage());
                        }
                    }
                }
                filterBytes = baos.toByteArray();
                feederBufferSize = filterBytes.length;
            } else {
                filterBytes = null;
                feederBufferSize = 0;
            }

            /* build message buffer */
            final int bodySize = wireFormatSize() + 4 + feederBufferSize;
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            /* write 8 bytes of VLSN */
            LogUtils.writeLong(messageBuffer, vlsn);
            /* write 4 bytes of feeder buf size */
            LogUtils.writeInt(messageBuffer, feederBufferSize);
            /* write feeder buffer */
            if (feederBufferSize > 0) {
                LogUtils.writeBytesNoLength(messageBuffer, filterBytes);
            }
            messageBuffer.flip();
            return messageBuffer;
        }

        @Override
        public MessageOp getOp() {
            return START_STREAM;
        }

        @Override
        public String toString() {
            String filterString = (feederFilter == null) ? "[no filtering]" :
                feederFilter.toString();

            return super.toString() + " " + filterString;
        }
    }

    public class Heartbeat extends Message {

        private final long masterNow;
        private final long currentTxnEndVLSN;
        private final int heartbeatId;
        /* last vlsn processed by filter */
        private final long lastFilterVLSN;
        /* last vlsn passing filter */
        private final long lastPassVLSN;
        /* last commit timestamp in Feeder */
        private final long lastCommitTimeMs;
        /* mod. time of last processed operation */
        private final long lastModTimeMs;

        public Heartbeat(long masterNow, long currentTxnEndVLSN,
                         int heartbeatId, long lastFilterVLSN,
                         long lastPassVLSN, long lastCommitTimeMs,
                         long lastModTimeMs) {
            this.masterNow = masterNow;
            this.currentTxnEndVLSN = currentTxnEndVLSN;
            this.heartbeatId = heartbeatId;
            this.lastFilterVLSN = lastFilterVLSN;
            this.lastPassVLSN = lastPassVLSN;
            this.lastCommitTimeMs = lastCommitTimeMs;
            this.lastModTimeMs = lastModTimeMs;
        }

        public Heartbeat(long masterNow, long currentTxnEndVLSN,
                         int heartbeatId) {
            this(masterNow, currentTxnEndVLSN, heartbeatId, INVALID_VLSN,
                 INVALID_VLSN, -1/* unused */, -1/* unused */);
        }

        @Override
        public MessageOp getOp() {
            return HEARTBEAT;
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(clock.currentTimeMillis());
        }

        private ByteBuffer wireFormat(long currentTimeMillis) {
            final int bodySz = getBodySize();
            final ByteBuffer messageBuffer = allocateInitializedBuffer(bodySz);
            LogUtils.writeLong(messageBuffer, masterNow);
            LogUtils.writeLong(messageBuffer, currentTxnEndVLSN);
            if (includeHeartbeatId()) {
                LogUtils.writeInt(messageBuffer, heartbeatId);
            }
            if (includeSubscriptionInfo()) {
                LogUtils.writeLong(messageBuffer, lastFilterVLSN);
                LogUtils.writeLong(messageBuffer, lastPassVLSN);
                LogUtils.writeLong(messageBuffer, lastCommitTimeMs);
                LogUtils.writeLong(messageBuffer, lastModTimeMs);
            }
            if (includeSenderTimestamp()) {
                LogUtils.writeLong(messageBuffer, currentTimeMillis);
            }

            messageBuffer.flip();
            return messageBuffer;
        }

        public Heartbeat(ByteBuffer buffer) {
            masterNow = LogUtils.readLong(buffer);
            currentTxnEndVLSN = LogUtils.readLong(buffer);
            if (includeHeartbeatId()) {
                heartbeatId = LogUtils.readInt(buffer);
            } else {
                heartbeatId = -1;
            }
            if (includeSubscriptionInfo()) {
                lastFilterVLSN = LogUtils.readLong(buffer);
                lastPassVLSN = LogUtils.readLong(buffer);
                lastCommitTimeMs = LogUtils.readLong(buffer);
                lastModTimeMs = LogUtils.readLong(buffer);
            } else {
                lastFilterVLSN = INVALID_VLSN;
                lastPassVLSN = INVALID_VLSN;
                lastCommitTimeMs = -1;
                lastModTimeMs = -1;
            }
            if (includeSenderTimestamp()) {
                ensureSenderTimestampValid(LogUtils.readLong(buffer));
            }
        }

        public long getMasterNow() {
            return masterNow;
        }

        public long getCurrentTxnEndVLSN() {
            return currentTxnEndVLSN;
        }

        public int getHeartbeatId() {
            return heartbeatId;
        }

        public long getLastFilterVLSN() {
            return lastFilterVLSN;
        }

        public long getLastPassVLSN() {
            return lastPassVLSN;
        }

        public long getLastModTimeMs() {
            return lastModTimeMs;
        }

        public long getLastCommitTimeMs() {
            return lastCommitTimeMs;
        }

        @Override
        public String toString() {
            return super.toString() + " masterNow=" + masterNow +
                   " currentCommit=" + currentTxnEndVLSN +
                   " heartbeatId=" + heartbeatId +
                   " lastFilterVLSN=" +
                   (lastFilterVLSN == INVALID_VLSN ?
                       "no feeder filter" : lastFilterVLSN) +
                   " lastPassVLSN=" +
                   (lastPassVLSN == INVALID_VLSN ?
                       "no feeder filter" : lastPassVLSN) +
                   " lastCommitTimeMs=" + lastCommitTimeMs +
                   " lastModTimeMs=" + lastModTimeMs;

        }

        protected int getBodySize() {
            int bodySize = 8 * 2 /* masterNow + currentTxnEndVLSN */;
            if (includeHeartbeatId()) {
                bodySize += 4; /* heartbeat id */
            }
            if (includeSubscriptionInfo()) {
                bodySize += 8; /* filter VLSN */
                bodySize += 8; /* last pass VLSN */
                bodySize += 8; /* last commit timestamp */
                bodySize += 8; /* last op modification timestamp */
            }
            if (includeSenderTimestamp()) {
                bodySize += 8; /* sender timestamp */
            }
            return bodySize;
        }

        private boolean includeHeartbeatId() {
            return getVersion() >= VERSION_11;
        }

        private boolean includeSubscriptionInfo() {
            return getVersion() >= VERSION_12;
        }

        private boolean includeSenderTimestamp() {
            return getVersion() >= VERSION_13;
        }

        @Override
        public boolean match(Message other) {
            if (!(other instanceof Heartbeat)) {
                return false;
            }
            final long currentTimeMillis = TimeSupplier.currentTimeMillis();
            return Arrays.equals(
                wireFormat(currentTimeMillis).array().clone(),
                ((Heartbeat) other)
                .wireFormat(currentTimeMillis).array().clone());
        }

    }

    public class HeartbeatResponse extends Message {

        /* The latest commit/abort VLSN on the replica/arbiter/subscriber. */
        private final long txnEndVLSN;
        private final int heartbeatId;
        private final long localDurableVLSN;

        public HeartbeatResponse(long ackedVLSN, int heartbeatId) {
            super();
            this.txnEndVLSN = ackedVLSN;
            this.heartbeatId = heartbeatId;
            this.localDurableVLSN = NULL_VLSN;
        }

        public HeartbeatResponse(long ackedVLSN, int heartbeatId, long localDurableVLSN) {
            super();
            this.txnEndVLSN = ackedVLSN;
            this.heartbeatId = heartbeatId;
            this.localDurableVLSN = localDurableVLSN;
        }

        public HeartbeatResponse(ByteBuffer buffer) {
            /*
             * syncupVLSN field was used for the now defunct CBVLSN feature.
             * It can be removed when the protocol version is incremented.
             */
            LogUtils.readLong(buffer);

            txnEndVLSN = includeTxnEndVLSN()
                ?  LogUtils.readLong(buffer) : VLSN.NULL_VLSN;
            heartbeatId = includeHeartbeatId()
                ?  LogUtils.readInt(buffer) : -1;
            localDurableVLSN = includeLocalDurableVLSN()
                ?  LogUtils.readLong(buffer) : VLSN.NULL_VLSN;
            if (includeSenderTimestamp()) {
                ensureSenderTimestampValid(LogUtils.readLong(buffer));
            }
        }

        @Override
        public MessageOp getOp() {
            return HEARTBEAT_RESPONSE;
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(clock.currentTimeMillis());
        }

        private ByteBuffer wireFormat(long currentTimeMillis) {
            final int bodySize = getBodySize();

            ByteBuffer messageBuffer = allocateInitializedBuffer(bodySize);
            LogUtils.writeLong(messageBuffer, NULL_VLSN /*oldSyncupVLSN*/);
            if (includeTxnEndVLSN()) {
                LogUtils.writeLong(messageBuffer, txnEndVLSN);
            }
            if (includeHeartbeatId()) {
                LogUtils.writeInt(messageBuffer, heartbeatId);
            }
            if(includeLocalDurableVLSN()) {
                LogUtils.writeLong(messageBuffer, localDurableVLSN);
            }
            if (includeSenderTimestamp()) {
                LogUtils.writeLong(messageBuffer, currentTimeMillis);
            }
            messageBuffer.flip();
            return messageBuffer;
        }

        public long getTxnEndVLSN() {
            return txnEndVLSN;
        }

        public long getLocalDurableVLSN() {
            return localDurableVLSN;
        }

        public int getHeartbeatId() {
            return heartbeatId;
        }

        @Override
        public String toString() {
            return super.toString() +
                " txnEndVLSN=" + txnEndVLSN +
                " heartbeatId=" + heartbeatId;
        }

        protected int getBodySize() {
            int bodySize = 8;
            if (includeTxnEndVLSN()) {
                bodySize += 8; /* txn end vlsn */
            }
            if (includeHeartbeatId()) {
                bodySize += 4; /* heartbeat id */
            }
            if (includeLocalDurableVLSN()) {
                bodySize += 8; /* local durable vlsn */
            }
            if (includeSenderTimestamp()) {
                bodySize += 8; /* sender timestamp */
            }
            return bodySize;
        }

        private boolean includeTxnEndVLSN() {
            return getVersion() >= VERSION_4;
        }

        private boolean includeHeartbeatId() {
            return getVersion() >= VERSION_11;
        }

        private boolean includeSenderTimestamp() {
            return getVersion() >= VERSION_13;
        }

        private boolean includeLocalDurableVLSN() {
            return getVersion() >= VERSION_14;
        }

        @Override
        public boolean match(Message other) {
            if (!(other instanceof HeartbeatResponse)) {
                return false;
            }
            final long currentTimeMillis = TimeSupplier.currentTimeMillis();
            return Arrays.equals(
                wireFormat(currentTimeMillis).array().clone(),
                ((HeartbeatResponse) other)
                .wireFormat(currentTimeMillis).array().clone());
        }
    }

    /**
     * Message of a commit op
     */
    public class Commit extends Entry {
        private final boolean needsAck;
        private final SyncPolicy replicaSyncPolicy;

        public Commit(final boolean needsAck,
                      final SyncPolicy replicaSyncPolicy,
                      final OutputWireRecord wireRecord) {
            super(wireRecord);
            this.needsAck = needsAck;
            this.replicaSyncPolicy = replicaSyncPolicy;
        }

        @Override
        public MessageOp getOp() {
            return COMMIT;
        }

        @Override
        public ByteBuffer wireFormat() {
            final int bodySize = super.getWireSize() +
                                 1 /* needsAck */ +
                                 1 /* replica sync policy */;
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            messageBuffer.put((byte) (needsAck ? 1 : 0));
            messageBuffer.put((byte) replicaSyncPolicy.ordinal());
            writeOutputWireRecord(outputWireRecord, messageBuffer);
            messageBuffer.flip();
            return messageBuffer;
        }

        public Commit(final ByteBuffer buffer)
            throws DatabaseException {

            this(getByteNeedsAck(buffer.get()),
                 getByteReplicaSyncPolicy(buffer.get()),
                 buffer);
        }

        private Commit(final boolean needsAck,
                       final SyncPolicy replicaSyncPolicy,
                       final ByteBuffer buffer)
            throws DatabaseException {

            super(buffer);
            this.needsAck = needsAck;
            this.replicaSyncPolicy = replicaSyncPolicy;
        }

        public boolean getNeedsAck() {
            return needsAck;
        }

        public SyncPolicy getReplicaSyncPolicy() {
            return replicaSyncPolicy;
        }
    }

    /**
     * Message of an ack op
     */
    public class Ack extends Message {

        private final long txnId;

        private final long localDurableVLSN;

        public Ack(long txnId) {
            super();
            this.txnId = txnId;
            this.localDurableVLSN = NULL_VLSN;
            nAckMessages.increment();
        }

        public Ack(long txnId, long localDurableVLSN) {
            super();
            this.txnId = txnId;
            this.localDurableVLSN = localDurableVLSN;
            nAckMessages.increment();
        }

        @Override
        public MessageOp getOp() {
            return ACK;
        }

        @Override
        public ByteBuffer wireFormat() {
            int bodySize = 8;
            if (includeLocalDurableVLSN()) {
                bodySize += 8;
            }
            ByteBuffer messageBuffer = allocateInitializedBuffer(bodySize);
            LogUtils.writeLong(messageBuffer, txnId);
            if (includeLocalDurableVLSN()) {
                LogUtils.writeLong(messageBuffer, localDurableVLSN);
            }
            messageBuffer.flip();
            return messageBuffer;
        }

        public Ack(ByteBuffer buffer) {
            txnId = LogUtils.readLong(buffer);
            localDurableVLSN = includeLocalDurableVLSN()
                    ?  LogUtils.readLong(buffer) : VLSN.NULL_VLSN;
        }

        public long getTxnId() {
            return txnId;
        }

        public long getLocalDurableVLSN() {
            return localDurableVLSN;
        }

        @Override
        public String toString() {
            return super.toString() + " txn " + txnId;
        }

        private boolean includeLocalDurableVLSN() {
            return getVersion() >= VERSION_14;
        }
    }

    /**
     * A replica node asks a feeder for the log entry at this VLSN.
     */
    public class EntryRequest extends VLSNMessage {

        private final EntryRequestType type;

        EntryRequest(long matchpoint) {
            super(matchpoint);
            type = EntryRequestType.DEFAULT;
        }

        EntryRequest(long matchpoint, EntryRequestType type) {
            super(matchpoint);
            this.type = type;
        }

        public EntryRequest(ByteBuffer buffer) {
            super(buffer);

            /* entry request type not supported before protocol version 7 */
            if (getVersion() < VERSION_7) {
                type = EntryRequestType.DEFAULT;
                return;
            }

            final int i = LogUtils.readInt(buffer);
            type = EntryRequestType.values()[i];
        }

        public EntryRequestType getType() {
            return type;
        }

        @Override
        public ByteBuffer wireFormat() {

            /* type not supported before protocol version 7 */
            if (getVersion() < VERSION_7) {
                return super.wireFormat();
            }

            /* build message buffer */
            final int bodySize = wireFormatSize();
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            /* write 8 bytes of VLSN */
            LogUtils.writeLong(messageBuffer, vlsn);
            /* write 4 bytes of type */
            LogUtils.writeInt(messageBuffer, type.ordinal());
            messageBuffer.flip();
            return messageBuffer;
        }

        @Override
        public int wireFormatSize() {
            /* type not supported before protocol version 7 */
            if (getVersion() < VERSION_7) {
                return super.wireFormatSize();
            }

            return super.wireFormatSize() + 4;
        }

        @Override
        public MessageOp getOp() {
            return ENTRY_REQUEST;
        }

        @Override
        public String toString() {
            return "entry request vlsn: " + super.toString() +
                   ", type: " + type;
        }
    }

    /**
     * Type of entry request sent to feeder
     *
     * RV: VLSN requested by client
     * LOW: low end of available VLSN range in vlsn index
     * HIGH: high end of available VLSN range in vlsn index
     *
     * The DEFAULT mode is used by existing replication stream consumer e.g.
     * replica, arbiter, secondary nodes, etc, while the others are only used
     * in subscription (je.rep.subscription).
     *
     * {@literal
     * -------------------------------------------------------------------
     *     MODE      | RV < LOW  |   RV in [LOW, HIGH] | RV > HIGH
     * -------------------------------------------------------------------
     *  DEFAULT      | NOT_FOUND |   REQUESTED ENTRY   | ALT MATCH POINT
     *  AVAILABLE    |   LOW     |   REQUESTED ENTRY   | HIGH
     *  NOW          |   HIGH    |   HIGH              | HIGH
     * }
     */
    public enum EntryRequestType {
        DEFAULT,
        AVAILABLE,
        NOW
    }

    /**
     * Response when the EntryRequest asks for a VLSN that is below the VLSN
     * range covered by the Feeder.
     */
    public class EntryNotFound extends Message {

        public EntryNotFound() {
        }

        public EntryNotFound(@SuppressWarnings("unused") ByteBuffer buffer) {
            super();
        }

        @Override
        public MessageOp getOp() {
            return ENTRY_NOTFOUND;
        }
    }

    public class AlternateMatchpoint extends Message {

        private final InputWireRecord alternateInput;
        private OutputWireRecord alternateOutput = null;

        AlternateMatchpoint(final OutputWireRecord alternate) {
            alternateInput = null;
            this.alternateOutput = alternate;
        }

        @Override
        public MessageOp getOp() {
            return ALT_MATCHPOINT;
        }

        @Override
        public ByteBuffer wireFormat() {
            final int bodySize = alternateOutput.getWireSize(streamLogVersion);
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            writeOutputWireRecord(alternateOutput, messageBuffer);
            messageBuffer.flip();
            return messageBuffer;
        }

        public AlternateMatchpoint(final ByteBuffer buffer)
            throws DatabaseException {
            alternateInput =
                new InputWireRecord(repImpl, buffer, BaseProtocol.this);
        }

        public InputWireRecord getAlternateWireRecord() {
            return alternateInput;
        }

        /* For unit test support */
        @Override
        public boolean match(Message other) {

            /*
             * This message was read in, but we need to compare it to a message
             * that was sent out.
             */
            if (alternateOutput == null) {
                alternateOutput =
                    new OutputWireRecord(repImpl, alternateInput);
            }
            return super.match(other);
        }
    }

    /**
     * Request from the replica to the feeder for sufficient information to
     * start a network restore.
     */
    public class RestoreRequest extends VLSNMessage {

        RestoreRequest(long failedMatchpoint) {
            super(failedMatchpoint);
        }

        public RestoreRequest(ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        public MessageOp getOp() {
            return RESTORE_REQUEST;
        }
    }

    /**
     * Response when the replica needs information to instigate a network
     * restore. The message contains a set of nodes that could be used as the
     * basis for a NetworkBackup so that the request node can become current
     * again.
     *
     * <p>The protocol also sends an unused long value, previously the
     * GlobalCBVLSN, that is always NULL_VLSN.</p>
     */
    public class RestoreResponse extends SimpleMessage {
        private final RepNodeImpl[] logProviders;

        public RestoreResponse(RepNodeImpl[] logProviders) {
            this.logProviders = logProviders;
        }

        public RestoreResponse(ByteBuffer buffer) {
            /*
             * This field was used for the now defunct CBVLSN feature.
             * It can be removed when the protocol version is incremented.
             */
            LogUtils.readLong(buffer);

            logProviders = getRepNodeImplArray(buffer);
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(NULL_VLSN, logProviders);
        }

        /* Add support for RepNodeImpl arrays. */

        @Override
        protected void putWireFormat(final ByteBuffer buffer,
                                     final Object obj) {
            if (obj.getClass() == RepNodeImpl[].class) {
                putRepNodeImplArray(buffer, (RepNodeImpl[]) obj);
            } else {
                super.putWireFormat(buffer, obj);
            }
        }

        @Override
        protected int wireFormatSize(final Object obj) {
            if (obj.getClass() == RepNodeImpl[].class) {
                return getRepNodeImplArraySize((RepNodeImpl[]) obj);
            }
            return super.wireFormatSize(obj);
        }

        private void putRepNodeImplArray(final ByteBuffer buffer,
                                         final RepNodeImpl[] ra) {
            LogUtils.writeInt(buffer, ra.length);
            final int groupFormatVersion = getGroupFormatVersion();
            for (final RepNodeImpl node : ra) {
                putByteArray(
                    buffer,
                    RepGroupImpl.serializeBytes(node, groupFormatVersion));
            }
        }

        private RepNodeImpl[] getRepNodeImplArray(final ByteBuffer buffer) {
            final RepNodeImpl[] ra = new RepNodeImpl[LogUtils.readInt(buffer)];
            final int groupFormatVersion = getGroupFormatVersion();
            for (int i = 0; i < ra.length; i++) {
                ra[i] = RepGroupImpl.deserializeNode(
                    getByteArray(buffer), groupFormatVersion);
            }
            return ra;
        }

        private int getRepNodeImplArraySize(RepNodeImpl[] ra) {
            int size = 4; /* array length */
            final int groupFormatVersion = getGroupFormatVersion();
            for (final RepNodeImpl node : ra) {
                size += (4 /* Node size */ +
                         RepGroupImpl.serializeBytes(node, groupFormatVersion)
                             .length);
            }
            return size;
        }

        /**
         * Returns the RepGroupImpl version to use for the currently configured
         * protocol version.
         */
        private int getGroupFormatVersion() {
            return (getVersion() < VERSION_5) ?
                RepGroupImpl.FORMAT_VERSION_2 :
                RepGroupImpl.MAX_FORMAT_VERSION;
        }

        @Override
        public MessageOp getOp() {
            return RESTORE_RESPONSE;
        }

        RepNodeImpl[] getLogProviders() {
            return logProviders;
        }
    }

    /**
     * Message used to shutdown a node
     */
    public class ShutdownRequest extends SimpleMessage {
        /* The time that the shutdown was initiated on the master. */
        private final long shutdownTimeMs;

        public ShutdownRequest(long shutdownTimeMs) {
            super();
            this.shutdownTimeMs = shutdownTimeMs;
        }

        @Override
        public MessageOp getOp() {
            return SHUTDOWN_REQUEST;
        }

        public ShutdownRequest(ByteBuffer buffer) {
            shutdownTimeMs = LogUtils.readLong(buffer);
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(shutdownTimeMs);
        }

        public long getShutdownTimeMs() {
            return shutdownTimeMs;
        }
    }

    /**
     * Message in response to a shutdown request.
     */
    public class ShutdownResponse extends Message {

        public ShutdownResponse() {
            super();
        }

        @Override
        public MessageOp getOp() {
            return SHUTDOWN_RESPONSE;
        }

        public ShutdownResponse(@SuppressWarnings("unused") ByteBuffer buffer) {
        }
    }

    public class GroupAck extends Message {

        private final long txnIds[];
        private final long localDurableVLSN;

        public GroupAck(long txnIds[]) {
            super();
            this.txnIds = txnIds;
            this.localDurableVLSN = NULL_VLSN;
            nGroupAckMessages.increment();
            nGroupedAcks.add(txnIds.length);
            nMaxGroupedAcks.setMax(txnIds.length);
        }

        public GroupAck(long txnIds[], long localDurableVLSN) {
            super();
            this.txnIds = txnIds;
            this.localDurableVLSN = localDurableVLSN;
            nGroupAckMessages.increment();
            nGroupedAcks.add(txnIds.length);
            nMaxGroupedAcks.setMax(txnIds.length);
        }

        @Override
        public MessageOp getOp() {
            return GROUP_ACK;
        }

        @Override
        public ByteBuffer wireFormat() {

            final int bodySize = getBodySize();

            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);

            putLongArray(messageBuffer, txnIds);
            if (includeLocalDurableVLSN()) {
                LogUtils.writeLong(messageBuffer, localDurableVLSN);
            }
            messageBuffer.flip();

            return messageBuffer;
        }

        public GroupAck(ByteBuffer buffer) {
            txnIds = readLongArray(buffer);
            localDurableVLSN = includeLocalDurableVLSN()
                    ?  LogUtils.readLong(buffer) : VLSN.NULL_VLSN;
        }

        public long[] getTxnIds() {
            return txnIds;
        }

        public long getLocalDurableVLSN() {
            return localDurableVLSN;
        }

        @Override
        public String toString() {
            return super.toString() + " txn " + Arrays.toString(txnIds);
        }

        private boolean includeLocalDurableVLSN() {
            return getVersion() >= VERSION_14;
        }

        protected int getBodySize() {
            int bodySize = 4 + 8 * txnIds.length;
            if (includeLocalDurableVLSN()) {
                bodySize += 8; /* local durable vlsn */
            }
            return bodySize;
        }
    }

    /**
     * Base class for messages which contain only a VLSN
     */
    protected abstract class VLSNMessage extends Message {
        protected final long vlsn;

        VLSNMessage(long vlsn) {
            super();
            this.vlsn = vlsn;
        }

        public VLSNMessage(ByteBuffer buffer) {
            long vlsnSequence = LogUtils.readLong(buffer);
            vlsn = vlsnSequence;
        }

        @Override
        public ByteBuffer wireFormat() {
            int bodySize = wireFormatSize();
            ByteBuffer messageBuffer = allocateInitializedBuffer(bodySize);
            LogUtils.writeLong(messageBuffer, vlsn);
            messageBuffer.flip();
            return messageBuffer;
        }

        int wireFormatSize() {
            return 8;
        }

        long getVLSN() {
            return vlsn;
        }

        @Override
        public String toString() {
            return super.toString() + " " + vlsn;
        }
    }

    /**
     * Base class for all protocol handshake messages.
     */
    protected abstract class HandshakeMessage extends SimpleMessage {
    }

    /**
     * Version broadcasts the sending node's protocol version.
     */
    protected abstract class ProtocolVersion extends HandshakeMessage {
        private final int version;

        @SuppressWarnings("hiding")
        private final NameIdPair nameIdPair;

        public ProtocolVersion(int version) {
            super();
            this.version = version;
            this.nameIdPair = BaseProtocol.this.nameIdPair;
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(version, nameIdPair);
        }

        public ProtocolVersion(ByteBuffer buffer) {
            version = LogUtils.readInt(buffer);
            nameIdPair = getNameIdPair(buffer);
        }

        /**
         * @return the version
         */
        protected int getVersion() {
            return version;
        }

        /**
         * The nodeName of the sender
         *
         * @return nodeName
         */
        protected NameIdPair getNameIdPair() {
            return nameIdPair;
        }
    }

    /* ---------------------------------------- */
    /* ---  end of message class definition --- */
    /* ---------------------------------------- */

    /**
     * Write an entry output wire record to the message buffer using the write
     * log version format and increment nEntriesWrittenOldVersion if the entry
     * format was changed.
     */
    protected void writeOutputWireRecord(final OutputWireRecord record,
                                         final ByteBuffer messageBuffer) {
        final boolean changedFormat =
            record.writeToWire(messageBuffer, streamLogVersion);
        if (changedFormat) {
            nEntriesWrittenOldVersion.increment();
        }
    }

    /**
     * Initializes message ops and check if is valid within allocated range
     *
     * @param protocolOps  ops to be initialized
     * @param checkValidity true if check validity of op code
     */
    protected void initializeMessageOps(MessageOp[] protocolOps,
                                      boolean checkValidity) {

        if (checkValidity && protocolOps != null) {
            /* Check if op code is valid before initialization */
            for (MessageOp op : protocolOps) {
                if (!isValidMsgOpCode(op.getOpId())) {
                    throw EnvironmentFailureException.unexpectedState
                        ("Op id: " + op.getOpId() +
                         " is out of allowed range inclusively [" +
                         MIN_MESSAGE_OP_CODE_IN_SUBCLASS + ", " +
                         MAX_MESSAGE_OP_CODE_IN_SUBCLASS + "]");
                }
            }
        }
        initializeMessageOps(protocolOps);
    }

    /**
     * Returns whether the byte value specifies that an acknowledgment is
     * needed.
     */
    private static boolean getByteNeedsAck(final byte needsAckByte) {
        switch (needsAckByte) {
            case 0:
                return false;
            case 1:
                return true;
            default:
                throw EnvironmentFailureException.unexpectedState(
                    "Invalid bool ordinal: " + needsAckByte);
        }
    }

    /**
     * Check if protocol that is being used can support sending ping message,
     * which requires the version to be greater than VERSION_14
     */
    public boolean supportSyncupPingMessage() {
        return getVersion() >= VERSION_14;
    }

    public boolean includeLocalDurableVLSN() {
        return getVersion() >= VERSION_14;
    }

    /** Checks if op code defined in subclass fall in pre-allocated range */
    private static boolean isValidMsgOpCode(short opId) {

        return (opId <= MAX_MESSAGE_OP_CODE_IN_SUBCLASS) &&
               (opId >= MIN_MESSAGE_OP_CODE_IN_SUBCLASS);
    }

    /** Returns the sync policy specified by the argument. */
    private static SyncPolicy getByteReplicaSyncPolicy(
        final byte syncPolicyByte) {

        for (final SyncPolicy p : SyncPolicy.values()) {
            if (p.ordinal() == syncPolicyByte) {
                return p;
            }
        }
        throw EnvironmentFailureException.unexpectedState(
            "Invalid sync policy ordinal: " + syncPolicyByte);
    }

    /* Writes array of longs into buffer */
    private void putLongArray(ByteBuffer buffer, long[] la) {
        LogUtils.writeInt(buffer, la.length);

        for (long l : la) {
            LogUtils.writeLong(buffer, l);
        }
    }

    /* Reads array of longs from buffer */
    private long[] readLongArray(ByteBuffer buffer) {
        final long la[] = new long[LogUtils.readInt(buffer)];

        for (int i = 0; i < la.length; i++) {
            la[i] = LogUtils.readLong(buffer);
        }

        return la;
    }

    /**
     * Verifies the sender timestamp.
     *
     * We can only verify that the sender timestamp is not too far in the
     * future. The sender will verify our response so that skew on both
     * direction is verified.
     *
     * We only log the issue instead of thrown an exception.  The decision is
     * to adhere to the principle of HA as the top priority. Since we do not
     * know which node is at fault, we may invalidate a master which is
     * actually healthy and cause disruption to availability. TODO: ideally, we
     * would want to invalidate the culprit node if we can gain enough
     * confidence over the situation. This, however, would require multiple
     * validation across time and multiple nodes.
     */
    private void ensureSenderTimestampValid(long timestamp) {
        final long currTime = TimeSupplier.currentTimeMillis();
        if (timestamp > currTime + maxClockSkew) {
            if (repImpl != null) {
                LoggerUtils.logMsg(
                    repImpl.getReplicationWarningLogger(),
                    repImpl,
                    EnvironmentFailureReason.TIME_OUT_OF_SYNC.toString(),
                    Level.WARNING,
                    String.format(
                        "Sender timestamp %s too far in the future, " +
                        "currentTime=%s, maxClockSkew=%s",
                        timestamp, currTime, maxClockSkew));
            }
        }
    }
}
