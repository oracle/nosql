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

import static com.sleepycat.je.rep.impl.RepParams.ELECTIONS_OPEN_TIMEOUT;
import static com.sleepycat.je.rep.impl.RepParams.ELECTIONS_READ_TIMEOUT;

import java.util.logging.Level;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.elections.Acceptor.SuggestionGenerator.Ranking;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Proposer.ProposalParser;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.TextProtocol;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * {@literal
 * Defines the request/response messages used in the implementation of
 * elections.
 *
 * From Proposer to Acceptor:
 *     Propose -> Promise | Reject
 *     Accept -> Accepted | Reject
 *
 * From Proposer initiator to Learners:
 *    Result -> none
 *
 * The following exchange is not part of the elections process, but is used
 * by nodes and utilities that are attempting to find the master.
 *
 *    MasterQuery -> MasterQueryResponse | None
 * }
 */
public class Protocol extends TextProtocol {

    /* Protocol version string. Format: <major version>.<minor version> */
    /* It's used to ensure compatibility across versions. */
    private static final String VERSION = "2.0";

    /* An instance of ProposalParser used to de-serialize proposals */
    private final ProposalParser proposalParser;

    /* An instance of ValueParser used to de-serialize values */
    private final ValueParser valueParser;

    /* Request Operations */
    public final MessageOp PROPOSE;
    public final MessageOp ACCEPT;
    public final MessageOp RESULT;
    public final MessageOp MASTER_QUERY;
    public final MessageOp SHUTDOWN;

    /* Response operations */
    public final MessageOp REJECT;
    public final MessageOp PROMISE;
    public final MessageOp ACCEPTED;
    public final MessageOp MASTER_QUERY_RESPONSE;

    /**
     * Creates an instance of the Protocol.
     *
     * @param proposalParser parses a string into a Proposal object.
     * @param valueParser parses a string into a Value object.
     * @param groupName the name of the group running the election process.
     * @param nameIdPair a unique identifier for this election participant.
     */
    public Protocol(ProposalParser proposalParser,
                    ValueParser valueParser,
                    String  groupName,
                    NameIdPair nameIdPair,
                    RepImpl repImpl,
                    DataChannelFactory channelFactory) {

        /* Request operations */
        super(VERSION, groupName, nameIdPair, repImpl, channelFactory);

        PROPOSE = new MessageOp("P", Propose.class, (line, tokens) -> {
            return new Propose(line, tokens);
        });
        ACCEPT = new MessageOp("A", Accept.class, (line, tokens) -> {
            return new Accept(line, tokens);
        });
        RESULT = new MessageOp("RE", Result.class, (line, tokens) -> {
            return new Result(line, tokens);
        });
        MASTER_QUERY = new MessageOp("MQ", MasterQuery.class,
            (line, tokens) -> {return new MasterQuery(line, tokens);
        });
        SHUTDOWN = new MessageOp("X", Shutdown.class, (line, tokens) -> {
            return new Shutdown(line, tokens);
        });

        REJECT = new MessageOp("R", Reject.class, (line, tokens) -> {
            return new Reject(line, tokens);
        });
        PROMISE = new MessageOp("PR", Promise.class, (line, tokens) -> {
            return new Promise(line, tokens);
        });
        ACCEPTED = new MessageOp("AD", Accepted.class, (line, tokens) -> {
            return new Accepted(line, tokens);
        });
        MASTER_QUERY_RESPONSE = new MessageOp("MQR", MasterQueryResponse.class,
            (line, tokens) -> { return new MasterQueryResponse(line, tokens);
        });

        initializeMessageOps(new MessageOp[] {
                PROPOSE,
                ACCEPT,
                RESULT,
                MASTER_QUERY,
                SHUTDOWN,

                REJECT,
                PROMISE,
                ACCEPTED,
                MASTER_QUERY_RESPONSE,
        });
        this.proposalParser = proposalParser;
        this.valueParser = valueParser;

        setTimeouts(repImpl, ELECTIONS_OPEN_TIMEOUT, ELECTIONS_READ_TIMEOUT);
    }

    /**
     * Promise response message. It's sent in response to a Propose message.
     *
     * Note that the "minor" (introduced in 7.1.3) and "id"
     * (introduced in 18.3.1) part of the suggestion ranking is always tagged on
     * to the end of the promise request payload. Older pre 7.1.3
     * (or pre 18.3.1) nodes will ignore extra tokens at the end, since they do
     * not know about the minor and/or id component of the ranking. This node
     * will use it if it's present and otherwise use a value of zero for the
     * minor (VLSN) component and NameIdPair.NULL.id value for the id (node id)
     * component.
     *
     * So when comparing rankings across old and new nodes, we are effectively
     * comparing a Ranking(VLSN, Long.MIN_VALUE) with a Ranking(DTVLSN, VLSN),
     * resulting in suboptimal election results (from a dtvlsn perspective)
     * while an upgrade is in progress, that is, it will tend to favor the
     * older node. But this inaccuracy will vanish once all nodes have been
     * upgraded.
     *
     * The Ranking id field is used if the response is from an Arbiter. The
     * Arbiter's Ranking id field is set to the node identifier corresponding
     * to the VLSN/DTVLSN. The RN's Ranking id is the node id of the node.
     * The id field is used so an Arbiters VLSN/DTVLSN will not rank higher than
     * the value from the RN with that node id.
     *
     * With the introduction of master terms in 21.3 a Promise contains the
     * masterTerm value so that Promises can be ranked using the master term.
     * Details at {@link Ranking#compareTo}.
     */
    public class Promise extends ResponseMessage {
        private Proposal highestProposal = null;
        private Value acceptedValue = null;
        private Value suggestion = null;

        /**
         * The major and minor components of the Ranking represent the DTVLSN
         * and the latest VLSN respectively.
         */
        private final Ranking suggestionRanking;
        private final int priority;
        private int logVersion;
        private JEVersion jeVersion;

        public Promise(Proposal highestProposal,
                       Value value,
                       Value suggestion,
                       Ranking suggestionRanking,
                       int priority,
                       int logVersion,
                       JEVersion jeVersion) {
            this.highestProposal = highestProposal;
            this.acceptedValue = value;
            this.suggestion = suggestion;
            this.suggestionRanking = suggestionRanking ;
            this.priority = priority;
            this.logVersion = logVersion;
            this.jeVersion = jeVersion;
        }

        /**
         * Creates a Promise from the tokens. Note that the number of tokens
         * can vary based upon the version of the software that generated the
         * Promise. Specifically the new masterTerm token is absent from
         * versions preceding 21.3. The deserialization code handles the
         * missing value in the response payload by setting the masterTerm to
         * the distinguished value: {@link MasterTerm#PRETERM_TERM} to help
         * support version compatibility.
         *
         * @param responseLine the response text that was parsed into tokens
         * @param tokens the tokens resulting from the parsed responseLine
         * @throws InvalidMessageException
         */
        public Promise(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
            highestProposal = proposalParser.parse(nextPayloadToken());
            acceptedValue = valueParser.parse(nextPayloadToken());
            suggestion = valueParser.parse(nextPayloadToken());
            String weight = nextPayloadToken();
            long dtvlsn = "".equals(weight) ?
                Ranking.UNINITIALIZED.dtvlsn : Long.parseLong(weight);
            long masterTerm = MasterTerm.PRETERM_TERM;

            long vlsn = Ranking.UNINITIALIZED.vlsn;
            long nodeId = NameIdPair.NULL_NODE_ID;
            priority = Integer.parseInt(nextPayloadToken());
            if (getMajorVersionNumber(sendVersion) > 1) {
                logVersion = Integer.parseInt(nextPayloadToken());
                jeVersion = new JEVersion(nextPayloadToken());
                if (hasMoreTokens()) {
                    /*
                     * The tie breaker is appended to the end by newer versions
                     * of JE nodes >= version 7.1.3
                     */
                    vlsn = Long.parseLong(nextPayloadToken());
                }
                if (hasMoreTokens()) {
                    /*
                     * The node id of the VLSN is appended to the end by newer
                     * versions of JE nodes >= version 7.1.4
                     */
                    nodeId = Long.parseLong(nextPayloadToken());
                }


                if (hasMoreTokens()) {
                    /* The master term */
                    masterTerm =  Long.parseLong(nextPayloadToken());
                    MasterTerm.check(masterTerm);
                }

            }
            suggestionRanking = new Ranking(dtvlsn, vlsn, nodeId, masterTerm);
        }

        @Override
        public MessageOp getOp() {
            return PROMISE;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + getOuterType().hashCode();
            result = prime * result
                    + ((acceptedValue == null) ? 0 : acceptedValue.hashCode());
            result = prime
                    * result
                    + ((highestProposal == null) ? 0
                            : highestProposal.hashCode());
            result = prime * result + priority;
            result = prime * result
                    + ((suggestion == null) ? 0 : suggestion.hashCode());
            result = prime * result + suggestionRanking.hashCode();

            if (getMajorVersionNumber(sendVersion) > 1) {
                result += prime* result + logVersion + jeVersion.hashCode();
            }

            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (!super.equals(obj)) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            Promise other = (Promise) obj;
            if (!getOuterType().equals(other.getOuterType())) {
                return false;
            }

            if (acceptedValue == null) {
                if (other.acceptedValue != null) {
                    return false;
                }
            } else if (!acceptedValue.equals(other.acceptedValue)) {
                return false;
            }

            if (highestProposal == null) {
                if (other.highestProposal != null) {
                    return false;
                }
            } else if (!highestProposal.equals(other.highestProposal)) {
                return false;
            }

            if (priority != other.priority) {
                return false;
            }

            if (getMajorVersionNumber(sendVersion) > 1) {
                if (logVersion != other.logVersion) {
                    return false;
                }

                if (jeVersion.compareTo(other.jeVersion) != 0) {
                    return false;
                }
            }

            if (suggestion == null) {
                if (other.suggestion != null) {
                    return false;
                }
            } else if (!suggestion.equals(other.suggestion)) {
                return false;
            }

            if (!suggestionRanking.equals(other.suggestionRanking)) {
                return false;
            }

            return true;
        }

        /**
         * Serializes the Promise into its wire format
         *
         * Note that the masterTerm token is written at the end of the request.
         * Pre master term nodes who deserialize the Promise will ignore
         * this token.
         */
        @Override
        public String wireFormat() {
            String line =
                wireFormatPrefix() +
                SEPARATOR +
                ((highestProposal != null) ?
                 highestProposal.wireFormat() :
                 "") +
                SEPARATOR +
                ((acceptedValue != null) ? acceptedValue.wireFormat() : "") +
                SEPARATOR +
                ((suggestion != null) ?  suggestion.wireFormat() : "") +
                SEPARATOR +
                ((suggestionRanking.dtvlsn == Long.MIN_VALUE) ?
                 "" :
                 Long.toString(suggestionRanking.dtvlsn)) +
                 SEPARATOR +
                 priority;

           if (getMajorVersionNumber(sendVersion) > 1) {
              line += SEPARATOR + logVersion +
                      SEPARATOR + jeVersion.toString() +
                      SEPARATOR + Long.toString(suggestionRanking.vlsn) +
                      SEPARATOR + Long.toString(suggestionRanking.id) +
                      SEPARATOR + Long.toString(suggestionRanking.masterTerm);
           }

           return line;
        }

        Proposal getHighestProposal() {
            return highestProposal;
        }

        Value getAcceptedValue() {
            return acceptedValue;
        }

        Value getSuggestion() {
            return suggestion;
        }

        Ranking getSuggestionRanking() {
            return suggestionRanking;
        }

        int getPriority() {
            return priority;
        }

        int getLogVersion() {
            return logVersion;
        }

        JEVersion getJEVersion() {
            return jeVersion;
        }

        private Protocol getOuterType() {
            return Protocol.this;
        }
    }

    /**
     * Response to a successful Accept message.
     */
    public class Accepted extends ResponseMessage {
        private final Proposal proposal;
        private final Value value;

        Accepted(Proposal proposal, Value value) {
            assert(proposal!= null);
            assert(value != null);
            this.proposal = proposal;
            this.value = value;
        }

        public Accepted(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
            proposal = proposalParser.parse(nextPayloadToken());
            value  = valueParser.parse(nextPayloadToken());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result
                    + ((proposal == null) ? 0 : proposal.hashCode());
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof Accepted)) {
                return false;
            }
            final Accepted other = (Accepted) obj;
            if (proposal == null) {
                if (other.proposal != null) {
                    return false;
                }
            } else if (!proposal.equals(other.proposal)) {
                return false;
            }
            if (value == null) {
                if (other.value != null) {
                    return false;
                }
            } else if (!value.equals(other.value)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
            return ACCEPTED;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR + proposal.wireFormat() +
                SEPARATOR + value.wireFormat();
        }

        public Value getValue() {
            return value;
        }

        public Proposal getProposal() {
            return proposal;
        }
    }

    /**
     * The response to a Master Query request. It simply repackages the
     * Accepted response.
     */
    public class MasterQueryResponse extends Accepted {

        MasterQueryResponse(Proposal proposal, Value value) {
            super(proposal, value);
        }

        public MasterQueryResponse(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
        }
        @Override
        public MessageOp getOp() {
            return MASTER_QUERY_RESPONSE;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }
    }

    /**
     * Reject response to a message.
     */
    public class Reject extends ResponseMessage {
        private final Proposal higherProposal;

        Reject(Proposal higherProposal) {
            this.higherProposal = higherProposal;
        }

        public Reject(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
            higherProposal = proposalParser.parse(nextPayloadToken());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result +
                ((higherProposal == null) ? 0 : higherProposal.hashCode());

            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof Reject)) {
                return false;
            }
            final Reject other = (Reject) obj;
            if (higherProposal == null) {
                if (other.higherProposal != null) {
                    return false;
                }
            } else if (!higherProposal.equals(other.higherProposal)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
            return REJECT;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR + higherProposal.wireFormat();
        }

        Proposal getHigherProposal() {
            return higherProposal;
        }
    }

    /**
     * Propose request used in Phase 1 of Paxos
     */
    public class Propose extends RequestMessage {
        protected final Proposal proposal;

        Propose(Proposal proposal) {
            this.proposal = proposal;
        }

        public Propose(String requestLine, String[] tokens)
            throws InvalidMessageException {

            super(requestLine, tokens);
            proposal = proposalParser.parse(nextPayloadToken());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result
                    + ((proposal == null) ? 0 : proposal.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof Propose)) {
                return false;
            }
            final Propose other = (Propose) obj;
            if (proposal == null) {
                if (other.proposal != null) {
                    return false;
                }
            } else if (!proposal.equals(other.proposal)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
            return PROPOSE;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR +  proposal.wireFormat();
        }

        public Proposal getProposal() {
            return proposal;
        }
    }

    public class Shutdown extends RequestMessage {

        public Shutdown() {}

        public Shutdown(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
        }

        @Override
        public MessageOp getOp() {
            return SHUTDOWN;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix();
        }

    }

    /**
     * Accept request issued in Phase 2 of paxos.
     */
    public class Accept extends Propose {
        private final Value value;

        Accept(Proposal proposal, Value value) {
            super(proposal);
            this.value = value;
        }

        public Accept(String requestLine, String[] tokens)
            throws InvalidMessageException {

            super(requestLine, tokens);
            value = valueParser.parse(nextPayloadToken());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof Accept)) {
                return false;
            }
            final Accept other = (Accept) obj;
            if (value == null) {
                if (other.value != null) {
                    return false;
                }
            } else if (!value.equals(other.value)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
            return ACCEPT;
        }

        @Override
        public String wireFormat() {
            return super.wireFormat() + SEPARATOR + value.wireFormat();
        }

        public Value getValue() {
            return value;
        }
    }

    /**
     * Used to inform Learners of a "chosen value".
     */
    public class Result extends Accept {

        Result(Proposal proposal, Value value) {
            super(proposal, value);
        }

        public Result(String requestLine, String[] tokens)
            throws InvalidMessageException {
            super(requestLine, tokens);
        }

        @Override
        public MessageOp getOp() {
            return RESULT;
        }
    }

    /**
     * Used to query the Learner for a current master
     */
    public class MasterQuery extends RequestMessage {

        public MasterQuery() {
        	logUnknownSender();
        }

        public MasterQuery(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
            logUnknownSender();
        }

        @Override
        public MessageOp getOp() {
            return MASTER_QUERY;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix();
        }

        private void logUnknownSender() {
			int senderId = getSenderId();
			if ((senderId != Integer.MIN_VALUE && senderId < -1)) {
				LoggerUtils.logMsg(logger, repImpl, formatter, Level.SEVERE,
						getClass().getName() +
						" An unknown Node querying for Master. Sender id:" +
						senderId + "  " + LoggerUtils.getStackTrace());
			}
		}
        @Override
        public String toString() {
            return getOp() + " " + getMessagePrefix() + " " + wireFormat();
        }
    }

    /* Represents a Value in Paxos. */
    public interface Value extends WireFormatable  {
    }

    public interface ValueParser {
        /**
         * Converts the wire format back into a Value
         *
         * @param wireFormat String representation of a Value
         *
         *
         * @return the de-serialized Value
         *
         */
        abstract Value parse(String wireFormat);
    }

    /**
     * A String based value implementation used as the "default" Value
     */
    public static class StringValue extends StringFormatable implements Value {

        StringValue() {
            super(null);
        }

        public StringValue(String s) {
            super(s);
        }

        @Override
        public String toString() {
            return "Value:" + s;
        }

        public String getString() {
            return s;
        }
    }
}
