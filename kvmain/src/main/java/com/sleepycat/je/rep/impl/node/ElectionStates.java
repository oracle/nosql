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

package com.sleepycat.je.rep.impl.node;

import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Proposer.DefaultFormattedProposal;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Accept;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.json_simple.JsonKey;
import com.sleepycat.json_simple.JsonObject;

/**
 * Encapsulates the election states.
 *
 * <p>
 * In particular, this class encapsulates the acceptor states includes the
 * promised proposal, the accept proposal and value which controls the acceptor
 * logic and also needs to be persisted as per the Paxos protocol.  However, in
 * the special cases of arbiters and unit tests, the election states do not
 * need to be persisted. Therefore, we put the acceptor logic here and the
 * persist logic in ElectionStateContinuation.
 */
public class ElectionStates {

    public static final JsonKey PROMISED_PROPOSAL_KEY = () -> "promisedProposal";
    public static final JsonKey ACCEPTED_PROPOSAL_KEY = () -> "acceptedProposal";
    public static final JsonKey ACCEPTED_VALUE_KEY = () -> "acceptedValue";

    /**
     * The promised proposal, {@code null} if not available.  A new value can
     * only be accepted if the associated proposal is later than promised.
     */
    protected Proposal promisedProposal = null;
    /**
     * The proposal associated with the accepted value, {@code null} if not
     * available. An accepted value should be returned for the promise if it is
     * of the same term. TODO: currently unused. The accepted value, although
     * returned, is not used by the proposer.
     */
    protected Proposal acceptedProposal = null;
    /**
     * The the accepted value, {@code null} if not available. The value should
     * be returned for a promise if of the same term.
     */
    protected Value acceptedValue = null;

    /**
     * Constructs the state with all fields set to {@code null}. This
     * constructor is called by the ElectionStateContinuation and should be
     * further initialized by calling the init method.
     */
    protected ElectionStates() {
    }

    /**
     * Constructs the state by setting the promised proposal to the minimum
     * state. This constructor is used by arbiters and unit tests. The
     * constructor argument is just used for method overriding.
     */
    public ElectionStates(@SuppressWarnings("unused") byte ignored) {
        this.promisedProposal =
            new DefaultFormattedProposal(TimeSupplier.currentTimeMillis());
    }

    /**
     * Initializes from the persisted json object.
     */
    protected void init(JsonObject obj) {
        ensureHoldLock();
        this.promisedProposal = getProposal(obj, PROMISED_PROPOSAL_KEY);
        this.acceptedProposal = getProposal(obj, ACCEPTED_PROPOSAL_KEY);
        this.acceptedValue = getValue(obj, ACCEPTED_VALUE_KEY);
    }

    /**
     * Sets the latest promised proposal. Do nothing if it is not later than
     * the current promised.
     *
     * @return {@code true} if successfully set
     */
    public synchronized boolean setPromised(Proposal proposal) {
        ensureInitialized();
        if (proposal == null) {
            return false;
        }
        if (promisedProposal.compareTo(proposal) <= 0) {
            promisedProposal = proposal;
            return true;
        }
        return false;
    }

    protected void ensureInitialized() {
        if (promisedProposal == null) {
            throw new IllegalStateException(String.format(
                "Unexpected promised proposal not inited, i.e., " +
                "ElectionStateContinuation#init not called"));
        }
    }

    /**
     * Sets the latest accepted proposal and value. Do nothing if it is not
     * later than the current promised (note: not the current accepted).
     *
     * @return {@code true} if successfully set
     */
    public synchronized boolean setAccept(Accept accept) {
        ensureInitialized();
        final Proposal proposal = accept.getProposal();
        final Value value = accept.getValue();
        if ((proposal == null) || (value == null)) {
            return false;
        }
        if (promisedProposal.compareTo(proposal) <= 0) {
            acceptedProposal = proposal;
            acceptedValue = value;
            return true;
        }
        return false;
    }

    /**
     * Returns the promised term.
     * @return the promised term
     */
    public synchronized long getPromisedTerm() {
        if (promisedProposal == null) {
            return MasterTerm.MIN_TERM;
        }
        return promisedProposal.getTimeMs();
    }

    /**
     * Returns the promised proposal.
     * @return the promised proposal
     */
    public synchronized Proposal getPromisedProposal() {
        return promisedProposal;
    }

    /**
     * Returns the accepted value, {@code null} if not available.
     *
     * TODO: It is not correct to just use the persisted acceptedValue because
     * we are not looking at which term it is for. Currently the acceptedValue
     * is not used so this is still OK. Will implement with [KVSTORE-1197] part
     * 3 when we allow multiple rounds of the election for a term.
     */
    public synchronized Value getAcceptedValue() {
        return acceptedValue;
    }

    @Override
    public String toString() {
        return String.format(
            "promisedProposal=%s, " +
            "acceptedProposal=%s, " +
            "acceptedValue=%s",
            promisedProposal,
            acceptedProposal,
            acceptedValue);
    }

    /**
     * Ensures we hold the synchronization lock of this object.
     */
    protected void ensureHoldLock() {
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException(
                "Expected held the ElectionState object lock");
        }
    }

    /**
     * Returns a proposal of the provided key from the json object.
     */
    public static Proposal getProposal(JsonObject obj, JsonKey key) {
        final String proposalString = obj.getString(key);
        if (proposalString == null) {
            return null;
        }
        final Proposal proposal = new DefaultFormattedProposal(proposalString);
        return proposal;
    }

    /**
     * Returns a value of the provided key from the json object.
     */
    public static Value getValue(JsonObject obj, JsonKey key) {
        final String valueString = obj.getString(key);
        if (valueString == null) {
            return null;
        }
        final MasterValue value = new MasterValue(valueString);
        return value;
    }
}

