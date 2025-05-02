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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.util.logging.Level;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.elections.Acceptor.SuggestionGenerator.Ranking;
import com.sleepycat.je.rep.elections.Proposer.DefaultFormattedProposal;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Accept;
import com.sleepycat.je.rep.elections.Protocol.Propose;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.TextProtocol.InvalidMessageException;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.node.ElectionStates;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.MasterTransfer;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Plays the role of Acceptor in the consensus algorithm. It runs in its
 * own thread listening for and responding to messages sent by Proposers.
 */
public class Acceptor extends ElectionAgentThread {

    /** Used to return suggestions in response to Propose requests. */
    private final SuggestionGenerator suggestionGenerator;

    /** Identifies the Acceptor Service. */
    public static final String SERVICE_NAME = "Acceptor";

    private final ElectionsConfig config;
    private final ElectionStates electionStates;
    private final PrePromiseHook prePromiseHook;

    private final StatGroup statistics;
    private final IntStat nProposeAcceptorAccepted;
    private final IntStat nProposeAcceptorIgnored;
    private final IntStat nProposeAcceptorRejected;
    private final IntStat nAcceptAcceptorAccepted;
    private final IntStat nAcceptAcceptorRejected;

    /**
     * Creates an Acceptor
     */
    public Acceptor(Protocol protocol,
                    ElectionsConfig config,
                    ElectionStates electionStates,
                    PrePromiseHook prePromiseHook,
                    SuggestionGenerator suggestionGenerator) {

        super(config.getRepImpl(), protocol,
              "Acceptor Thread " + config.getNameIdPair().getName());
        this.config = config;

        this.electionStates = electionStates;
        this.prePromiseHook = prePromiseHook;
        this.suggestionGenerator = suggestionGenerator;
        this.statistics = new StatGroup(AcceptorStatDefinition.GROUP_NAME,
                                        AcceptorStatDefinition.GROUP_DESC);
        this.nProposeAcceptorRejected =
            new IntStat(statistics, AcceptorStatDefinition.
                    PROPOSE_ACCEPTOR_REJECTED);
        this.nProposeAcceptorAccepted =
            new IntStat(statistics, AcceptorStatDefinition.
                    PROPOSE_ACCEPTOR_ACCEPTED);
        this.nProposeAcceptorIgnored =
            new IntStat(statistics, AcceptorStatDefinition.
                    PROPOSE_ACCEPTOR_IGNORED);
        this.nAcceptAcceptorAccepted =
            new IntStat(statistics, AcceptorStatDefinition.
                    ACCEPT_ACCEPTOR_ACCEPTED);
        this.nAcceptAcceptorRejected =
            new IntStat(statistics, AcceptorStatDefinition.
                    ACCEPT_ACCEPTOR_REJECTED);
    }

    /**
     * The Acceptor thread body.
     */
    @Override
    public void run() {
        final ServiceDispatcher serviceDispatcher =
            config.getServiceDispatcher();
        serviceDispatcher.register(SERVICE_NAME, channelQueue);
        LoggerUtils.logMsg
            (logger, envImpl, formatter, Level.INFO, "Acceptor started");
        DataChannel channel = null;
        try {
            while (true) {
                channel = serviceDispatcher.takeChannel
                    (SERVICE_NAME, protocol.getReadTimeout());

                if (channel == null) {
                    /* A soft shutdown. */
                    return;
                }

                BufferedReader in = null;
                PrintWriter out = null;
                try {
                    in = new BufferedReader(
                        new InputStreamReader(
                            Channels.newInputStream(channel)));
                    out = new PrintWriter(
                        Channels.newOutputStream(channel), true);
                    final String requestLine = in.readLine();
                    if (requestLine == null) {
                        LoggerUtils.logMsg(logger, envImpl,
                                           formatter, Level.INFO,
                                           "Acceptor: EOF on request");
                        continue;
                    }
                    RequestMessage requestMessage = null;
                    try {
                        requestMessage = protocol.parseRequest(requestLine);
                    } catch (InvalidMessageException ime) {
                        protocol.processIME(channel, ime);
                        continue;
                    }
                    if (requestMessage.getOp() == protocol.SHUTDOWN) {
                        break;
                    }
                    final MasterTransfer masterTransfer =
                        (config.getRepNode() != null) ?
                        config.getRepNode().getActiveTransfer() : null;
                    if (masterTransfer != null) {
                        final String msg =
                            "Acceptor ignoring request due to active master " +
                            "transfer initiated at:" + formatter.
                            getDate(masterTransfer.getStartTime());
                        LoggerUtils.logMsg(logger, envImpl,
                                           formatter, Level.INFO, msg);
                        continue;
                    }

                    ResponseMessage responseMessage = null;
                    if (requestMessage.getOp() == protocol.PROPOSE) {
                        responseMessage = process((Propose) requestMessage);
                    } else if (requestMessage.getOp() == protocol.ACCEPT) {
                        responseMessage = process((Accept) requestMessage);
                    } else {
                        LoggerUtils.logMsg(logger, envImpl,
                                           formatter, Level.INFO,
                                           "Acceptor unrecognized request: " +
                                           requestLine);
                        continue;
                    }

                    /*
                     * The acceptor may choose to ignore this message when it is
                     * not ready.
                     */
                    if (responseMessage == null) {
                        continue;
                    }

                    /*
                     * The request message may be of an earlier version. If so,
                     * this node transparently read the older version. JE only
                     * throws out InvalidMesageException when the version of
                     * the request message is newer than the current protocol.
                     * To avoid sending a repsonse that the requester cannot
                     * understand, we send a response in the same version as
                     * that of the original request message.
                     */
                    responseMessage.setSendVersion
                        (requestMessage.getSendVersion());
                    out.println(responseMessage.wireFormat());
                } catch (IOException e) {
                    LoggerUtils.logMsg
                        (logger, envImpl, formatter, Level.INFO,
                         "Acceptor IO error on socket: " + e.getMessage());
                    continue;
                } finally {
                    Utils.cleanup(logger, envImpl, formatter, channel, in, out);
                    cleanup();
                }
            }
        } catch (InterruptedException e) {
            if (isShutdown()) {
                /* Treat it like a shutdown, exit the thread. */
                return;
            }
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                               "Acceptor unexpected interrupted");
            throw EnvironmentFailureException.unexpectedException(e);
        } finally {
            serviceDispatcher.cancel(SERVICE_NAME);
            cleanup();
        }
    }

    /**
     * Returns the current Acceptor statistics.
     */
    public StatGroup getAcceptorStats() {
        return statistics;
    }

    /**
     * Responds to a Propose request.
     *
     * @param propose the request proposal
     *
     * @return the response: a Promise if the request was accepted, a Reject if
     * reject and {@code null} if ignore.
     */
    ResponseMessage process(Propose propose) {

        final Proposal proposal = propose.getProposal();
        final PrePromiseResult prePromiseResult =
            prePromiseHook.promise(proposal);

        switch (prePromiseResult.getResultType()) {
            case IGNORE:
                nProposeAcceptorIgnored.increment();
                return null;

            case REJECT:
                nProposeAcceptorRejected.increment();
                /* Reject if pre-promise hook says so. */
                final Proposal rejectCause = prePromiseResult.getRejectCause();
                logInfo(String.format(
                    "Proposal %s rejected due to a promised proposal %s",
                    proposal, electionStates.getPromisedProposal()));
                return protocol.new Reject(rejectCause);

            case PROCEED:
                nProposeAcceptorAccepted.increment();
                break;
        }

        final Proposal promisedProposal = electionStates.getPromisedProposal();
        if (!promisedProposal.equals(proposal)) {
            throw new IllegalStateException(String.format(
                "ElectionStates promisedProposal not set correctly, " +
                "expected %s, got %s",
                proposal, promisedProposal));
        }

        final Value suggestedValue = suggestionGenerator.get(promisedProposal);
        final Ranking suggestionRanking =
            suggestionGenerator.getRanking(promisedProposal);
        logInfo("Promised: " + promisedProposal +
                " Suggested Value: " + suggestedValue +
                " Suggestion Ranking: " + suggestionRanking);
        return protocol.new Promise(promisedProposal,
                                    electionStates.getAcceptedValue(),
                                    suggestedValue,
                                    suggestionRanking,
                                    config.getElectionPriority(),
                                    config.getLogVersion(),
                                    JEVersion.CURRENT_VERSION);
    }

    /**
     * Responds to Accept request
     *
     * @param accept the request
     * @return an Accepted or Reject response as appropriate.
     */
    ResponseMessage process(Accept accept) {
        final Proposal promisedProposal = electionStates.getPromisedProposal();
        if (!electionStates.setAccept(accept)) {
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                               "Reject Accept: " + accept.getProposal() +
                               " Promised proposal: " + promisedProposal);
            nAcceptAcceptorRejected.increment();
            return protocol.new Reject(promisedProposal);
        }
        final Value acceptedValue = electionStates.getAcceptedValue();
        logInfo("Promised: " + promisedProposal + " Accepted: " +
                accept.getProposal() + " Value: " + acceptedValue);
        nAcceptAcceptorAccepted.increment();
        return protocol.new Accepted(accept.getProposal(), acceptedValue);
    }

    public interface SuggestionGenerator {

        /**
         * Used to generate a suggested value for use by a Proposer. It's a
         * hint. The proposal argument may be used to freeze values like the
         * VLSN number from advancing (if they were used in the ranking) until
         * an election has completed.
         *
         * @param proposal the Proposal for which the value is being suggested.
         *
         * @return the suggested value.
         */
        abstract Value get(Proposal proposal);

        /**
         * The importance associated with the above suggestion. Acceptors have
         * to agree on a common system for ranking importance so that the
         * relative importance of different suggestions can be meaningfully
         * compared.
         *
         * @param proposal the proposal associated with the ranking
         *
         * @return the importance of the suggestion as a number
         */
        abstract Ranking getRanking(Proposal proposal);

        /**
         * A description of the ranking used when comparing Promises to pick a
         * Master.
         */
        class Ranking implements Comparable<Ranking> {
            /* The dtvlsn is only used for comparisons with older pre term
             * promises.
             */
            final long dtvlsn;
            final long vlsn;

            /* The term value, or MasterTerm.PRETERM_TERM if the ranking was
             * by a pre term node.
             */
            final long masterTerm;

            final long id;

            /* It's guaranteed that any valid Ranking will rank above the MIN */
            static Ranking MIN =
                new Ranking(0,
                            1,
                            NameIdPair.NULL_NODE_ID,
                            MasterTerm.MIN_TERM);

            static Ranking UNINITIALIZED = new Ranking();

            public Ranking(long dtvlsn, long vlsn, long id, long masterTerm) {
                this.dtvlsn = dtvlsn;
                this.vlsn = vlsn;
                this.id = id;
                MasterTerm.check(masterTerm);
                this.masterTerm = masterTerm;
            }

            public Ranking(long dtvlsn, long vlsn, long masterTerm) {
                this.dtvlsn = dtvlsn;
                this.vlsn = vlsn;
                this.id = NameIdPair.NULL_NODE_ID;
                MasterTerm.check(masterTerm);
                this.masterTerm = masterTerm;
            }

            /**
             * Special purpose internal constructor used to generate the
             * UNINITIALIZED value.
             */
            private Ranking() {
                this.dtvlsn = Long.MIN_VALUE;
                this.vlsn = Long.MIN_VALUE;
                this.id = NameIdPair.NULL_NODE_ID;
                this.masterTerm = MasterTerm.NULL;
            }

            /**
             * Returns true if the Ranking was generated by a pre master term
             * node.
             */
            public boolean isPreTerm() {
                return masterTerm == MasterTerm.PRETERM_TERM;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + (int) (dtvlsn ^ (dtvlsn >>> 32));
                result = prime * result + (int) (id ^ (id >>> 32));
                result = prime * result
                    + (int) (masterTerm ^ (masterTerm >>> 32));
                result = prime * result + (int) (vlsn ^ (vlsn >>> 32));
                return result;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                Ranking other = (Ranking) obj;
                if (dtvlsn != other.dtvlsn) {
                    return false;
                }
                if (id != other.id) {
                    return false;
                }

                /* Only compare master term if the neither object is pre term */
                if ((!other.isPreTerm() && !isPreTerm()) &&
                    (masterTerm != other.masterTerm)) {
                    return false;
                }

                if (vlsn != other.vlsn) {
                    return false;
                }
                return true;
            }

            @Override
            public String toString() {
                return "dtvlsn:" + dtvlsn + " vlsn:" + vlsn + " id:" + id +
                       " master term:" + MasterTerm.logString(masterTerm);
            }

            /**
             * The compareTo falls back to the use of dtvlsns when a hybrid
             * (pre/post term) comparison is called for. Such hybrid
             * comparisons should only happen during transient situations as a
             * shard is being upgraded to the new master term software.
             *
             * It's worth noting here that under rare circumstances, under a
             * suitable network partitioning scenario compounded by additional
             * failures, the (in-memory) dtvlsn for a pre term node can be
             * higher than the dtvlsn for a post term node, which could result
             * in an incorrect master being chosen, while the group was in a
             * hybrid pre/post term state. If failure testing proves that this
             * is not a rare event, we can make changes to fall back to all
             * dtvlsn based ranking while the group is in this hybrid state.
             *
             * For details please review the thread:
             * https://proddev-database.slack.com/archives/C6Z2BNJ2C/\
             * p1627418774271900?thread_ts=1626379213.229200&cid=C6Z2BNJ2C
             */
            @Override
            public int compareTo(Ranking o) {
                final int result = (o.isPreTerm() || isPreTerm()) ?
                    Long.compare(dtvlsn, o.dtvlsn) :
                    Long.compare(masterTerm, o.masterTerm);

                return (result != 0) ? result : Long.compare(vlsn, o.vlsn);
            }
        }
    }

    /**
     * Invoked on the event that a proposal is being promised. The callback
     * method is invoked before the listener makes a promise.
     */
    public static interface PrePromiseHook {

        /**
         * Notifies of the proposal.
         *
         * @param proposal the proposal to be promised
         * @return the pre-promise result
         */
        PrePromiseResult promise(Proposal proposal);
    }

    /**
     * Represents the pre-promise result type.
     */
    public enum PrePromiseResultType {

        /**
         * The acceptor is not ready to make a promise and thus should ignore
         * the proposal promise request. Currently this means the election
         * continuation barrier has not passed yet.
         */
        IGNORE,

        /**
         * The acceptor should reject the promise. For example, we have already
         * learned the master of a higher term.
         */
        REJECT,

        /**
         * There is no objection from the pre-promise hook for the acceptor can
         * make the promise.
         */
        PROCEED
    }

    /**
     * Represents a pre-promise result.
     */
    public static class PrePromiseResult {

        private static final PrePromiseResult IGNORE_RESULT =
            new PrePromiseResult(PrePromiseResultType.IGNORE, null);
        private static final PrePromiseResult PROCEED_RESULT =
            new PrePromiseResult(PrePromiseResultType.PROCEED, null);

        /** The result type. */
        private final PrePromiseResultType resultType;
        /** The equivalent higher term proposal for rejection. */
        private final Proposal rejectCause;

        private PrePromiseResult(PrePromiseResultType resultType,
                                 Proposal rejectCause) {
            this.resultType = resultType;
            this.rejectCause = rejectCause;
        }

        public PrePromiseResultType getResultType() {
            return resultType;
        }

        public Proposal getRejectCause() {
            return rejectCause;
        }

        /**
         * Creates an IGNORE result.
         */
        public static PrePromiseResult createIgnore() {
            return IGNORE_RESULT;
        }

        /**
         * Creates a REJECT result.
         */
        public static PrePromiseResult createReject(long term) {
            return new PrePromiseResult(
                PrePromiseResultType.REJECT,
                new DefaultFormattedProposal(term));
        }

        /**
         * Creates a PROCEED result.
         */
        public static PrePromiseResult createProceed() {
            return PROCEED_RESULT;
        }
    }

    /**
     * Default implementation of PrePromiseHook with ElecitonStates.
     */
    public static class DefaultPrePromiseHook
        implements PrePromiseHook {

        protected final ElectionStates electionStates;

        public DefaultPrePromiseHook(ElectionStates electionStates) {
            this.electionStates = electionStates;
        }

        @Override
        public PrePromiseResult promise(Proposal proposal) {
            if (!electionStates.setPromised(proposal)) {
                return PrePromiseResult.createReject(
                    electionStates.getPromisedTerm());
            }
            return PrePromiseResult.createProceed();
        }
    }
}
