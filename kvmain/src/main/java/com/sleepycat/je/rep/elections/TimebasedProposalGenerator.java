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

package com.sleepycat.je.rep.elections;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Proposer.ProposalParser;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.utilint.RepUtils.Clock;

/**
 * Generates a unique sequence of ascending proposal numbers that is unique
 * across all machines.
 *
 * Each proposal number is built as the concatenation of the following components:
 *
 * ms time (8 bytes) | machineId (16 bytes) | locally unique Id (4 bytes)
 *
 * The ms time supplies the increasing number and the IP address is a number
 * unique across machines.
 *
 * The machineId is generated as described below.
 *
 * The locally unique Id is used to allow for multiple unique proposal
 * generators in a single process.
 */
public class TimebasedProposalGenerator {

    /*
     * A number that is unique for all instances of the TimeBasedGenerator on
     * this machine.
     */
    private final int locallyUniqueId;
    private static final AtomicInteger uniqueIdGenerator = new AtomicInteger(1);

    /*
     * Tracks the time (in ms) used to generate the previous proposal
     * preventing the creation of duplicate proposals.  Synchronize on this
     * instance when accessing this field.
     */
    private long prevProposalTime;

    /* lambda that returns the latest proposal we know about based upon past
     * elections results. Used to detect and account for clock skews.
     */
    private final Supplier<Proposal> lastWinningProposal;

    private final Clock clock;
    private long maxClockSkew;
    /*
     * A unique ID for this JVM, using a hex representation of the IP address
     * XOR'ed with a random value. If the IP address cannot be determined,
     * a secure random number is generated and used instead. The risk of
     * collision is very low since the number of machines in a replication
     * group is typically small, in the 10s at most.
     */
    private static final String machineId;

    /* Allow for 16 byte ipv6 addresses. */
    final static int ADDRESS_WIDTH =32;
    final static int UID_WIDTH = 8;

    /*
     * Initialize machineId, do it just once to minimize latencies in the face
     * of misbehaving networks that slow down calls to getLocalHost()
     */
    static  {

        InetAddress localHost;
        try {
            localHost = java.net.InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            /*
             * Likely a misconfigured machine if it could not determine
             * localhost.
             */
            localHost = null;
        }
        byte[] localAddress = null;
        if (localHost != null) {
            localAddress = localHost.getAddress();

            if (localHost.isLoopbackAddress()) {
                /* Linux platforms return a loopback address, examine the
                 * interfaces individually for a suitable address.
                 */
                localAddress = null;
                try {
                    for (Enumeration<NetworkInterface> interfaces =
                        NetworkInterface.getNetworkInterfaces();
                        interfaces.hasMoreElements();) {
                        for (Enumeration<InetAddress> addresses =
                            interfaces.nextElement().getInetAddresses();
                            addresses.hasMoreElements();) {
                            InetAddress ia = addresses.nextElement();
                            if (! (ia.isLoopbackAddress() ||
                                   ia.isAnyLocalAddress() ||
                                   ia.isMulticastAddress())) {
                                /* Found one, any one of these will do. */
                                localAddress = ia.getAddress();
                                break;
                            }
                        }
                    }
                } catch (SocketException e) {
                    /* Could not get the network interfaces, give up */
                }
            }
        }

        if (localAddress != null) {
            /*
             * Convert the address to a positive integer, XOR it with a
             * random value of the right size, and format in hex
             */
            final BigInteger addrVal = new BigInteger(1, localAddress);
            final BigInteger randVal =
                new BigInteger(ADDRESS_WIDTH * 4, new SecureRandom());
            machineId = String.format("%0" + ADDRESS_WIDTH + "x",
                                      addrVal.xor(randVal));
        } else {
            /*
             * If the localAddress is null, this host is likely disconnected,
             * or localHost is misconfigured, fall back to using just a secure
             * random number.
             */
            final BigInteger randVal =
                new BigInteger(ADDRESS_WIDTH * 4, new SecureRandom());
            machineId = String.format("%0" + ADDRESS_WIDTH + "x", randVal);
        }
    }

    /**
     * Creates an instance with an application-specified locally (machine wide)
     * unique id, e.g. a port number, or a combination of a pid and some other
     * number.
     *
     * @param locallyUniqueId the machine wide unique id
     *
     * @param lastWinningProposal lambda that returns the latest proposal we
     * know about, or null if we don't know of any. It's used as the basis for
     * ensuring that the next proposal ranks higher than lastWinningProposal in
     * the presence of clock skews.
     *
     * @param skew deliberate skew in ms introduced for testing
     *
     */
    private TimebasedProposalGenerator(int locallyUniqueId,
                                       Supplier<Proposal> lastWinningProposal,
                                       int skew) {
        this.locallyUniqueId = locallyUniqueId;
        this.lastWinningProposal = lastWinningProposal;
        clock = new Clock(skew);
        prevProposalTime = clock.currentTimeMillis();
    }

    /* Only used in testing. */
    public TimebasedProposalGenerator() {
        this(uniqueIdGenerator.getAndIncrement(), () -> null, 0);
        this.maxClockSkew = 2000;
    }

    /**
     * Constructor defaulting the unique id so it's merely unique within the
     * process.
     */
    public TimebasedProposalGenerator(Supplier<Proposal> lastWinningProposal,
                                      long maxClockSkew,
                                      int skew) {
        this(uniqueIdGenerator.getAndIncrement(), lastWinningProposal, skew);
        this.maxClockSkew = maxClockSkew;
    }

    /**
     * Returns the next Proposal greater than all previous proposals returned
     * on this machine.
     *
     * @return the next unique proposal
     */
    public Proposal nextProposal() {
        long proposalTime = clock.currentTimeMillis();
        final Proposal prevKnownProposal = lastWinningProposal.get();
        final long prevKnownProposalTimeMs =
            (prevKnownProposal != null) ? prevKnownProposal.getTimeMs() : 0;
        synchronized (this) {
            if (proposalTime <= prevProposalTime) {
                /* Proposals are moving faster than the clock. */
                proposalTime = ++prevProposalTime;
            }
            prevProposalTime = proposalTime;

            /* Now check it against times we may have from other nodes to make
             * sure we are generating a newer proposal.
             */
            if ((prevKnownProposalTimeMs != 0) &&
                (proposalTime < prevKnownProposalTimeMs)) {
                final long skew = prevKnownProposalTimeMs - proposalTime;
                if (skew > maxClockSkew) {
                    String msg =
                        "Clock skew (relatively slow)" + skew + " ms " +
                        " exceeds max permissible clock skew " +
                        maxClockSkew + "ms";
                    throw new IllegalStateException(msg);
                }
                /* Advance the proposal time. */
                proposalTime = (prevKnownProposalTimeMs + 1);
                prevProposalTime = proposalTime;
            }
        }

        return new StringProposal(proposalTime, machineId, locallyUniqueId);
    }

    /**
     * Returns the parser used to convert wire representations into Proposal
     * instances.
     *
     * @return a ProposalParser
     */
    public static ProposalParser getParser() {
        return StringProposal.getParser();
    }

    /**
     * Implements the Proposal interface for a string based proposal. The
     * string is a hex representation of the Proposal.
     */
    private static class StringProposal implements Proposal {

        private final long timestamp;
        private final String machineId;
        private final String locallyUniqueId;

        /* The canonical proposal parser. */
        private static ProposalParser theParser = new ProposalParser() {
                @Override
                public Proposal parse(String wireFormat) {
                    return ((wireFormat == null) || ("".equals(wireFormat))) ?
                        null :
                        new StringProposal(wireFormat);
                }
            };

        private StringProposal(long timestamp,
                               String machineId,
                               int locallyUniqueId) {
            this.timestamp = timestamp;
            this.machineId = machineId;
            this.locallyUniqueId = String.format("%08x", locallyUniqueId);
        }


        StringProposal(String proposal) {
            Objects.requireNonNull(proposal, "proposal");
            this.timestamp = Long.parseLong(
                proposal.substring(0, TIMESTAMP_WIDTH), 16);
            this.machineId = proposal.substring(
                TIMESTAMP_WIDTH, TIMESTAMP_WIDTH + ADDRESS_WIDTH);
            this.locallyUniqueId = proposal.substring(
                TIMESTAMP_WIDTH + ADDRESS_WIDTH);
        }

        @Override
        public String wireFormatWithoutTimestamp() {
            return String.format("%s%s", machineId, locallyUniqueId);
        }

        @Override
        public long getTimeMs() {
        	return timestamp;
        }

        @Override
        public int compareTo(Proposal otherProposal) {
            if (!(otherProposal instanceof StringProposal)) {
                throw new IllegalArgumentException(String.format(
                    "StringProposal cannot compare to %s",
                    otherProposal));
            }
            final StringProposal that = (StringProposal) otherProposal;
            if (timestamp != that.timestamp) {
                return Long.compare(timestamp, that.timestamp);
            }
            if (!machineId.equals(that.machineId)) {
                return machineId.compareTo(that.machineId);
            }
            return locallyUniqueId.compareTo(that.locallyUniqueId);
        }

        @Override
        public String toString() {
            return String.format(
                String.format("%s:%%s:%%s[%%s]", TIMESTAMP_FORMAT),
                timestamp, machineId, locallyUniqueId,
                MasterTerm.logString(timestamp));
        }

        private static ProposalParser getParser() {
            return theParser;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, machineId, locallyUniqueId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof StringProposal)) {
                return false;
            }
            final StringProposal that = (StringProposal) obj;
            return (timestamp == that.timestamp)
                && machineId.equals(that.machineId)
                && locallyUniqueId.equals(that.locallyUniqueId);
        }
    }
}
