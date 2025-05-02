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

import static com.sleepycat.je.log.LogEntryType.LOG_VERSION_EXPIRE_INFO;
import static com.sleepycat.je.rep.impl.RepParams.GROUP_NAME;
import static com.sleepycat.je.rep.impl.RepParams.MAX_CLOCK_DELTA;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicaConnectRetryException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.stream.Protocol.CacheSizeResponse;
import com.sleepycat.je.rep.stream.Protocol.DuplicateNodeReject;
import com.sleepycat.je.rep.stream.Protocol.FeederJEVersions;
import com.sleepycat.je.rep.stream.Protocol.FeederProtocolVersion;
import com.sleepycat.je.rep.stream.Protocol.JEVersionsReject;
import com.sleepycat.je.rep.stream.Protocol.NodeGroupInfoOK;
import com.sleepycat.je.rep.stream.Protocol.NodeGroupInfoReject;
import com.sleepycat.je.rep.stream.Protocol.SNTPResponse;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.BinaryProtocol.ProtocolException;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.utilint.RepUtils.Clock;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Implements the Replica side of the handshake protocol between the Replica
 * and the Feeder. The FeederReplicaHandshake class takes care of the other
 * side.
 *
 * @see <a href="https://sleepycat.oracle.com/pmwiki/pmwiki.php?n=JEHomePage.ReplicaFeederHandshake">FeederReplicaHandshake</a>
 */
public class ReplicaFeederHandshake {

    /* The rep node (server or replica) */
    private final RepImpl repImpl;
    private final Clock clock;
    private final int groupFormatVersion;
    private final NamedChannel namedChannel;

    private final NameIdPair replicaNameIdPair;

    /* Established during the negotiation. */
    private NameIdPair feederNameIdPair;

    private final NodeType nodeType;

    private final RepGroupImpl repGroup;

    private Protocol protocol = null;

    
    /* The JE software versions in use by the Feeder */
    private FeederJEVersions feederJEVersions;

    /*
     * The time to wait between retries in case of DuplicateNodeReject.
     */
    static final int REPLICA_CONNECTION_RETRY_SLEEP_MS = 1000;
    static final int REPLICA_CONNECTION_RETRIES = 10;

    /*
     * Used during testing: A non-zero value overrides the actual log version.
     */
    private static volatile int testCurrentLogVersion = 0;

    /**
     * Used during testing: A non-zero value overrides the actual protocol
     * version.
     */
    private static volatile int testCurrentProtocolVersion = 0;

    /* Fields used to track clock skew wrt the feeder. */
    private long clockDelay = Long.MAX_VALUE;
    private long clockDelta = Long.MAX_VALUE;
    private static int CLOCK_SKEW_MAX_SAMPLE_SIZE = 5;
    private static final long CLOCK_SKEW_MIN_DELAY_MS = 2;
    private final int maxClockDelta;

    /**
     * If the nodeType is an Arbiter, the SNTPRequest message
     * is sent but the clock skew is not checked.
     */
    private final boolean checkClockSkew;

    private final Logger logger;

    /**
     * An instance of this class is created with each new handshake preceding
     * the setting up of a connection.
     *
     * @param conf  handshake configuration with feeder
     */
    public ReplicaFeederHandshake(ReplicaFeederHandshakeConfig conf) {
        this.repImpl = conf.getRepImpl();
        this.namedChannel = conf.getNamedChannel();
        this.repGroup = conf.getGroup();
        this.groupFormatVersion = repGroup.getFormatVersion();
        this.nodeType = conf.getNodeType();

        replicaNameIdPair = conf.getNameIdPair();
        this.clock = conf.getClock();
        if (nodeType.isArbiter()) {
            maxClockDelta = Integer.MAX_VALUE;
            checkClockSkew = false;
        } else {
            maxClockDelta =
                repImpl.getConfigManager().getDuration(MAX_CLOCK_DELTA);
            checkClockSkew = true;
        }
        logger = LoggerUtils.getLogger(getClass());
    }

    /** Get the current log version, supporting a test override. */
    private static int getCurrentLogVersion() {
        return (testCurrentLogVersion != 0) ?
            testCurrentLogVersion :
            LogEntryType.LOG_VERSION;
    }

    /**
     * Set the current log version to a different value, for testing.
     * Specifying {@code 0} reverts to the standard value.
     *
     * @param testLogVersion the testing log version or {@code 0}
     */
    public static void setTestLogVersion(int testLogVersion) {
        testCurrentLogVersion = testLogVersion;
    }

    /** Get the current JE version, supporting a test override. */
    private JEVersion getCurrentJEVersion() {
        return repImpl.getCurrentJEVersion();
    }

    /** Get the current protocol version, supporting a test override. */
    private int getCurrentProtocolVersion() {
        if (testCurrentProtocolVersion != 0) {
            return testCurrentProtocolVersion;
        }
        return Protocol.getJEVersionProtocolVersion(getCurrentJEVersion());
    }

    /**
     * Set the current protocol version to a different value, for testing.
     * Specifying {@code 0} reverts to the standard value.
     */
    public static void setTestProtocolVersion(final int testProtocolVersion) {
        testCurrentProtocolVersion = testProtocolVersion;
    }

    /**
     * Returns the minJEVersion of the group, or null if unknown (in
     * protocol versions &lt; VERSION_7).
     *
     * Note that this method and the FeederJEVersions message is currently
     * unused, but is left in place for possible future use. It was used
     * previously for the now defunct GlobalCBVLSN.
     */
    public JEVersion getFeederMinJEVersion() {
        return feederJEVersions.getMinJEVersion();
    }

    /**
     * Negotiates a protocol that both the replica and feeder can support.
     *
     * @return the common protocol
     *
     * @throws IOException
     */
    private Protocol negotiateProtocol()
        throws IOException,
        ReplicaConnectRetryException {

        final Protocol defaultProtocol =
            Protocol.getProtocol(repImpl, replicaNameIdPair, clock,
                                 getCurrentProtocolVersion(),
                                 groupFormatVersion);
        /* Send over the latest version protocol this replica can support. */
        defaultProtocol.write(defaultProtocol.new ReplicaProtocolVersion(),
                              namedChannel);
        /*
         * Returns the highest level the feeder can support, or the version we
         * just sent, if it can support that version
         */
        Message message = defaultProtocol.read(namedChannel);
        if (message instanceof DuplicateNodeReject) {

            /* When the feeder messages with DuplicateNodeReject to the replica
             * , for the first try we throw retry exception, so the node 
             * does not need to restart and wait for the feeder to kill 
             * the channel, so the handshake to be successful in the another 
             * try with feeder. 
             * Any more dup messages recieved from the feeder will be thrown 
             * a EFE
             *
             */   
            String errMsg = "Duplicate Node Rejection as a replica with " + 
                "the name: " + replicaNameIdPair + " is already active " +
                "with the Feeder:" + feederNameIdPair; 
            throw new ReplicaConnectRetryException(errMsg, 
                    REPLICA_CONNECTION_RETRIES,
                    REPLICA_CONNECTION_RETRY_SLEEP_MS);
        }

        FeederProtocolVersion feederVersion =
            ((FeederProtocolVersion) message);
        feederNameIdPair = feederVersion.getNameIdPair();
        Protocol configuredProtocol =
            Protocol.get(repImpl,
                         replicaNameIdPair,
                         feederNameIdPair,
                         clock, feederVersion.getVersion(),
                         getCurrentProtocolVersion(), groupFormatVersion);
        LoggerUtils.fine(logger, repImpl,
                         "Feeder id: " + feederVersion.getNameIdPair() +
                         "Response message: " + feederVersion.getVersion());
        namedChannel.setNameIdPair(feederNameIdPair);
        LoggerUtils.fine(logger, repImpl,
                         "Channel Mapping: " + feederNameIdPair + " is at " +
                         namedChannel.getChannel());

        if (configuredProtocol == null) {
            /* Include JE version information [#22541] */
            throw new EnvironmentFailureException
                (repImpl,
                 EnvironmentFailureReason.PROTOCOL_VERSION_MISMATCH,
                 "Incompatible protocol versions. " +
                 "Protocol version: " + feederVersion.getVersion() +
                 " introduced in JE version: " +
                 Protocol.getProtocolJEVersion(feederVersion.getVersion()) +
                 " requested by the Feeder: " + feederNameIdPair +
                 " is not supported by this Replica: " + replicaNameIdPair +
                 " with protocol version: " + defaultProtocol.getVersion() +
                 " introduced in JE version: " +
                 Protocol.getProtocolJEVersion(defaultProtocol.getVersion()));
        }
        return configuredProtocol;
    }

    /**
     * Executes the replica side of the handshake.
     * @throws ProtocolException
     */
    public Protocol execute()
        throws IOException,
               ProtocolException,
               ReplicaConnectRetryException {

        LoggerUtils.info(logger, repImpl,
                         "Replica-feeder handshake start");

        /* First negotiate the protocol, then use it. */
        protocol = negotiateProtocol();

        /* Ensure that software versions are compatible. */
        verifyVersions();

        /**
         * Note whether log entries with later log versions need to be
         * converted to log version 12 to work around [#25222].
         */
        if (feederJEVersions.getLogVersion() == LOG_VERSION_EXPIRE_INFO) {
            protocol.setFixLogVersion12Entries(true);
        }

        /*
         * Now perform the membership information validation part of the
         * handshake
         */
        verifyMembership();

        checkClockSkew();

        checkCacheSize();

        LoggerUtils.info(logger, repImpl,
                         "Replica-feeder " + feederNameIdPair.getName() +
                         " handshake completed.");
        return protocol;
    }

    /**
     * Checks software and log version compatibility.
     */
    private void verifyVersions()
        throws IOException {

        final int replicaLogVersion = getCurrentLogVersion();
        protocol.write(protocol.new
                       ReplicaJEVersions(getCurrentJEVersion(),
                                         replicaLogVersion),
                       namedChannel);
        Message message = protocol.read(namedChannel);
        if (message instanceof JEVersionsReject) {
            /* The software version is not compatible with the Feeder. */
            throw new EnvironmentFailureException
                (repImpl,
                 EnvironmentFailureReason.HANDSHAKE_ERROR,
                 " Feeder: " + feederNameIdPair + ". " +
                 ((JEVersionsReject) message).getErrorMessage());
        }

        /*
         * Save the version information in case we want to use it as the basis
         * for further compatibility checking in future.
         */
        feederJEVersions = (FeederJEVersions) message;

        final byte feederLogVersion = feederJEVersions.getLogVersion();
        LoggerUtils.logMsg(logger, repImpl,
                           Level.INFO,
                           "Replica current log version:" + replicaLogVersion +
                           " Feeder log version: " + feederLogVersion);

        if (feederLogVersion > replicaLogVersion) {
            throw new EnvironmentFailureException(
                repImpl,
                EnvironmentFailureReason.HANDSHAKE_ERROR,
                " Feeder: " + feederNameIdPair + ". " +
                "Feeder log version " + feederLogVersion +
                " is not known to the replica, whose current log version is " +
                replicaLogVersion);
        }
    }

    /**
     * Exchange membership information messages.
     */
    private void verifyMembership()
        throws IOException {

        DbConfigManager configManager = repImpl.getConfigManager();
        String groupName = configManager.get(GROUP_NAME);

        Message message = protocol.new
            NodeGroupInfo(groupName,
                          repGroup.getUUID(),
                          replicaNameIdPair,
                          repImpl.getHostName(),
                          repImpl.getPort(),
                          nodeType,
                          repImpl.isDesignatedPrimary(),
                          getCurrentJEVersion());
        protocol.write(message, namedChannel);

        message = protocol.read(namedChannel);

        if (message instanceof NodeGroupInfoReject) {
            NodeGroupInfoReject reject = (NodeGroupInfoReject) message;
            throw new EnvironmentFailureException
                (repImpl,
                 EnvironmentFailureReason.HANDSHAKE_ERROR,
                 " Feeder: " + feederNameIdPair + ". " +
                 reject.getErrorMessage());
        }

        if (!(message instanceof NodeGroupInfoOK)) {
            throw new EnvironmentFailureException
                (repImpl,
                 EnvironmentFailureReason.HANDSHAKE_ERROR,
                 " Feeder: " + feederNameIdPair + ". " +
                 "Protocol error. Unexpected response " + message);
        }
        final NodeGroupInfoOK nodeGroupInfoOK = (NodeGroupInfoOK) message;
        if (repGroup.hasUnknownUUID()) {
            /* Correct the initial UUID */
            repGroup.setUUID(nodeGroupInfoOK.getUUID());
        }
        if (nodeType.hasTransientId()) {
            /* Update the transient node's ID */
            replicaNameIdPair.update(nodeGroupInfoOK.getNameIdPair());
        }
    }

    /**
     * Checks for clock skew wrt the current feeder. It's important that the
     * clock skew be within an acceptable range so that replica can meet any
     * time based consistency requirements requested by transactions. The
     * intent of this check is to draw the attention of the application or the
     * administrators to the skew, not correct it.
     * <p>
     * The scheme implemented below is a variation on the scheme used by <a
     * href="http://tools.ietf.org/html/rfc2030">SNTP</a> protocol. The Feeder
     * plays the role of the SNTP server and the replica the role of the client
     * in this situation. The mechanism used here is rough and does not
     * guarantee the detection of a clock skew, especially since it's a one
     * time check done each time a connection is re-established with the
     * Feeder. The clocks could be in sync at the time of this check and drift
     * apart over the lifetime of the connection. It's also for this reason
     * that we do not store the skew value and make compensations using it when
     * determining replica consistency.
     * <p>
     * Replications nodes should therefore ensure that they are using NTP or a
     * similar time synchronization service to keep time on all the replication
     * nodes in a group in sync.
     * <p>
     *
     * @throws IOException
     * @throws EnvironmentFailureException
     * @throws ProtocolException
     */
    private void checkClockSkew()
        throws IOException,
               ProtocolException {

        boolean isLast = false;
        int sampleCount = 0;
        do {
            if (checkClockSkew) {
                /* Iterate until we have a value that's good enough. */
                isLast = (++sampleCount >= CLOCK_SKEW_MAX_SAMPLE_SIZE) ||
                         (clockDelay <= CLOCK_SKEW_MIN_DELAY_MS);
            } else {
                isLast = true;
            }

            protocol.write(protocol.new SNTPRequest(isLast), namedChannel);
            SNTPResponse response = protocol.read(namedChannel,
                                                  SNTPResponse.class);
            if (response.getDelay() < clockDelay) {
                clockDelay = response.getDelay();
                clockDelta = response.getDelta();
            }

        } while (!isLast);

        if (!checkClockSkew) {
            return;
        }

        LoggerUtils.logMsg
            (logger, repImpl,
             (Math.abs(clockDelta) >= maxClockDelta) ?
             Level.SEVERE : Level.FINE,
             "Round trip delay: " + clockDelay + " ms. " + "Clock delta: " +
             clockDelta + " ms. " + "Max permissible delta: " +
             maxClockDelta + " ms.");

        if (Math.abs(clockDelta) >= maxClockDelta) {
            throw new EnvironmentFailureException
                (repImpl,
                 EnvironmentFailureReason.TIME_OUT_OF_SYNC,
                 "Clock delta: " + clockDelta + " ms. " +
                 "between Feeder: " + feederNameIdPair.getName() +
                 " and this Replica exceeds max permissible delta: " +
                 maxClockDelta + " ms.");
        }
    }

    /**
     * Checks if the cache sizes between the two are the same, and if not logs
     * a warning.  Mismatching cache sizes is not an error, but can lead to a
     * lagging replica if its cache size is not large enough to efficiently
     * handle the workload.
     */
    private void checkCacheSize() throws ProtocolException, IOException {
        /* Feeder does not support cache check, so do nothing. */
        if (protocol.getVersion() < Protocol.VERSION_10) {
            return;
        }

        protocol.write(protocol.new CacheSizeRequest(repImpl), namedChannel);

        final CacheSizeResponse info =
            protocol.read(namedChannel, CacheSizeResponse.class);
        final long replicaCacheSize = repImpl.getMemoryBudget().getMaxMemory();

        if (info.getCacheSize() != replicaCacheSize
            || info.getOffHeapSize() != 0) {
            LoggerUtils.logMsg(logger, repImpl, Level.WARNING,
                "Mismatched cache sizes, replica:" + replicaCacheSize
                + " feeder: " + info.getCacheSize()
                + " replica off-heap: " + 0
                + " feeder off-heap: " + info.getOffHeapSize());
        }
    }
}
