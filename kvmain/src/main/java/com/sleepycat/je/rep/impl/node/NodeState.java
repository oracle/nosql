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

import static com.sleepycat.je.rep.ReplicatedEnvironment.State.DETACHED;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.UNKNOWN;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.elections.MasterObsoleteException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Tracks the state of the ReplicatedEnvironement.
 *
 * NodeState encapsulates the current replicator state, and the ability to wait
 * for state transition and fire state change notifications.
 */
public class NodeState {
    /** The rep impl whose state is being tracked. */
    private final RepImpl repImpl;

    /** The application registered state change listener for this node. */
    private StateChangeListener stateChangeListener = null;

    /** The state change event that resulted in the current state. */
    private StateChangeEvent stateChangeEvent = null;
    private final AtomicReference<ReplicatedEnvironment.State> currentState =
        new AtomicReference<>(ReplicatedEnvironment.State.DETACHED);

    /** Non null when the state is Master, null otherwise. */
    private volatile MasterIdTerm masterIdTerm;

    /**
     * Latest MasterIdTerm when this node is/was the master. This value can be
     * used to obtain the previous MasterIdTerm when this node is no longer the
     * master.
     */
    private final AtomicReference<MasterIdTerm> latestThisMasterIdTerm =
        new AtomicReference<>();

    /**
     * Minimum allowed term for the future state change. The value is always
     * monotonically increasing.  The value is updated for every state
     * transition as well as when acceptor promises for a new term.  When
     * transitting to either master or replica, the new term must be equal to
     * or larger than this min term. Detached and Unknown state has a null term
     * and hence transition to these states bypasses the checks.
     */
    private final AtomicLong allowedNextTerm = new AtomicLong(0);

    private final Logger logger;
    private final NameIdPair nameIdPair;

    public NodeState(NameIdPair nameIdPair,
                     RepImpl repImpl) {

        this.nameIdPair = nameIdPair;
        this.repImpl = repImpl;
        logger = LoggerUtils.getLogger(getClass());
    }

    synchronized public
        void setChangeListener(StateChangeListener stateChangeListener){
        this.stateChangeListener = stateChangeListener;
    }

    synchronized public StateChangeListener getChangeListener() {
        return stateChangeListener;
    }

    /* A convenience method to capture the idiom */
    synchronized public void changeToUnknownAndNotify() {
        changeAndNotify(UNKNOWN, NameIdPair.NULL, MasterTerm.NULL,
                        (e) -> {throw new IllegalStateException(e);});
    }

    /* A convenience method to capture the idiom */
    synchronized public void changeToDetachedAndNotify() {
        changeAndNotify(DETACHED, NameIdPair.NULL, MasterTerm.NULL,
                        (e) -> {throw new IllegalStateException(e);});
    }

    synchronized public void changeAndNotify(
        ReplicatedEnvironment.State state,
        NameIdPair masterNameId,
        long masterTerm,
        Consumer<MasterObsoleteException> handler)
    {

        try {
            changeAndNotify(state, masterNameId, masterTerm);
        } catch (MasterObsoleteException moe) {
            handler.accept(moe);
        }
    }

    /**
     * Change to a new node state and release any threads waiting for a state
     * transition.
     */
    synchronized public void changeAndNotify(ReplicatedEnvironment.State state,
                                             NameIdPair masterNameId,
                                             long masterTerm)
        throws MasterObsoleteException
    {

        changeAndNotify(state, masterNameId, masterTerm, true);
    }

    /**
     * Change to a new arbiter node state and release any threads waiting for a
     * state transition without any checking on the validity of the term.
     */
    synchronized public void arbiterChangeAndNotify(
        ReplicatedEnvironment.State state)
    {

        try{
            changeAndNotify(state, NameIdPair.NULL, MasterTerm.NULL, false);
        } catch (MasterObsoleteException e) {
            throw new IllegalStateException(
                "Unexpected master obsolete without check", e);
        }
    }

    /**
     * Change to a new node state and release any threads waiting for a state
     * transition.
     */
    synchronized public void changeAndNotify(ReplicatedEnvironment.State state,
                                             NameIdPair masterNameId,
                                             long masterTerm,
                                             boolean ensureTermValid)
        throws MasterObsoleteException
    {
        if (ensureTermValid) {
            ensureTransitTermValid(state, masterTerm, allowedNextTerm.get());
        }

        ReplicatedEnvironment.State newState = state;
        ReplicatedEnvironment.State oldState = currentState.getAndSet(state);
        masterIdTerm = currentState.get().isMaster() ?
           new MasterIdTerm(nameIdPair.getId(), masterTerm) :
           null;
        latestThisMasterIdTerm.getAndUpdate(
            (old) -> {
                if (!state.isMaster()) {
                    /* Keeps the old value if new state is not the master. */
                    return old;
                }
                if ((old != null) && (old.term > masterTerm)) {
                    throw new IllegalStateException(
                        "Should already checked new term is larger");
                }
                return new MasterIdTerm(masterNameId.getId(), masterTerm);
        });
        stateChangeEvent = new StateChangeEvent(state, masterNameId);

        updateAllowedNextTerm(masterTerm);

        LoggerUtils.info(logger, repImpl, String.format(
            "State change from %s to %s with masterId=%s, masterTerm=%s",
            oldState, newState, masterNameId,
            MasterTerm.logString(masterTerm)));

        if (stateChangeListener != null
            /*
             * Do not expose redundant state transition to the listeners. As
             * specified in the ReplicatedEnvironment.State, transitions
             * usually go through UNKNOWN state by reconnecting. A redundant
             * transition is possible due to master stays the same but the term
             * is bumped. Since term is a concept that is transparent to
             * applications, there is little benefit in exposing this
             * transition. The only benefit I can think of right now is so that
             * the applications can reason about the performance impact
             * according to this additional transition. However, we can expose
             * this through stats. Since the existing listeners may depend on
             * this no-redundant transition behavior, currently I decided not
             * expose this.
             */
            && (!oldState.equals(newState))) {
            try {
                stateChangeListener.stateChange(stateChangeEvent);
            } catch (Exception e) {
                LoggerUtils.severe(logger, repImpl,
                                   "State Change listener exception" +
                                   e.getMessage() +
                                   LoggerUtils.getStackTraceForSevereLog(e));
                throw new EnvironmentFailureException
                    (repImpl, EnvironmentFailureReason.LISTENER_EXCEPTION, e);
            }
        }

        /* Make things obvious in thread dumps */
        Thread.currentThread().setName(currentState + " " + nameIdPair);
    }

    /**
     * Returns {@code true} if the newly learned master value is not obsolete.
     *
     * After this check, the node will use the learned result to establish
     * connection. We must ensure that the associated term is greater or equal
     * to the allowed next term.
     *
     * We already made a check in the Learner.preLearnHook so that, in the
     * common case, the countDownLatch should not be triggered when a lower
     * term result is learned. There could be a race, however, that after the
     * preLearnHook is passed and before we connect, it made a new promise.
     * This is OK since the CommitFreezeLatch will freeze the replay, but it is
     * less optimal. Therefore, we throw MasterObsoleteException here so that
     * the RepNode can continue onto the next loop with the higher term.
     */
    private void ensureTransitTermValid(ReplicatedEnvironment.State state,
                                        long nextTerm,
                                        long allowed)
        throws MasterObsoleteException
    {
        if (state.equals(DETACHED) || state.equals(UNKNOWN)) {
            if (nextTerm != MasterTerm.NULL) {
                throw new IllegalStateException(String.format(
                    "Detachted or unknown mode " +
                    "has a non-null term value: %016x",
                    nextTerm));
            }
            return;
        }
        if (nextTerm == MasterTerm.NULL) {
            throw new IllegalStateException(String.format(
                "Master or replica mode " +
                "does not provide a term value: %016x",
                nextTerm));
        }
        LoggerUtils.fine(
            logger, repImpl,
            String.format(
                "Ensure master term valid. " +
                "Allowed term is %s. " +
                "The learned result is for term %s.",
                MasterTerm.logString(allowed),
                MasterTerm.logString(nextTerm)));
        if (nextTerm < allowed) {
            throw new MasterObsoleteException(nextTerm, allowed);
        }
    }

    /**
     * Updates the allowedNextTerm.
     *
     * Ensures that the allowed value is monotonically increasing. The provided
     * value may not be monotonically increasing for acceptor promises. Even
     * though the acceptor checks the allowed value before promise, we do not
     * bother to synchronize and prevent race.
     */
    public void updateAllowedNextTerm(long value) {
        allowedNextTerm.updateAndGet((v) -> Math.max(v, value));
    }

    public long getAllowedNextTerm() {
        return allowedNextTerm.get();
    }

    synchronized public ReplicatedEnvironment.State getRepEnvState() {
        return currentState.get();
    }

    /**
     * Returns the MasterIdTerm pair if the node is the master, null if
     * the current state is not master.
     */
    public MasterIdTerm getMasterIdTerm() {
        return masterIdTerm;
    }

    /**
     * Returns the previous Id and term when this node is the master.
     */
    public MasterIdTerm getPreviousMasterIdTerm() {
        return latestThisMasterIdTerm.get();
    }

    synchronized public StateChangeEvent getStateChangeEvent() {
        return stateChangeEvent;
    }
}
