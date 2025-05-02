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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.rep.elections.MasterObsoleteException;
import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.util.TimeSupplier;

/**
 * Ensures that a VLSN is not advanced at this node while an election is in
 * progress. Note that this is difficult, if not impossible to achieve
 * efficiently in a distributed environment across the entire group, when
 * communications may not always be reliable. So, the implementation really
 * represents a good faith effort to freeze the VLSN. JE HA itself should be
 * able to make forward progress in the event of such a failure.
 *
 * The class coordinates three threads: the acceptor, the learner, and the
 * replay thread. There is exactly one instance of each thread per replication
 * node, so it coordinates the activity of these three threads.
 *
 * The typical serialized sequence of calls is therefore:
 *
 * latch.freeze() -- invoked in response to a Promise by an Acceptor
 * latch.vlsnEvent() -- one or more of them in response to ongoing election
 * latch.awaitThaw() -- by the replica thread waiting for the freeze to lift
 *
 * Both vlsnEvent() and awaitThaw() are NOPs in the absence of a freeze.
 *
 * @see <a href="https://sleepycat.oracle.com/trac/wiki/ElectionsImplementation#FreezingVLSNs">Freezing VLSNs</a>
 */
public class CommitFreezeLatch {

    /*
     * The following fields are accessed in the synchronization block of this
     * object.
     */

    /** The latest proposal that freezes the replay. */
    private Proposal promisedProposal = null;
    /** The latest learned proposal. */
    private Proposal learnedProposal = null;
    /** The latest learned nameIdPair which we will connect to next. */
    private NameIdPair learnedNameIdPair = null;
    /* The latch used for waiting. */
    private CountDownLatch latch = null;
    /* The end time of the freeze. */
    private long freezeEnd = 0;

    /*
     * The following fields do not need synchronization block for thread
     * safety.
     */

    /* Statistics */
    private int freezeCount = 0;
    private int awaitTimeoutCount = 0;
    private int awaitElectionCount = 0;

    private volatile long timeOut = DEFAULT_LATCH_TIMEOUT;

    private final static long DEFAULT_LATCH_TIMEOUT = 5000; // ms

    public int getAwaitTimeoutCount() {
        return awaitTimeoutCount;
    }

    public int getAwaitElectionCount() {
        return awaitElectionCount;
    }

    public int getFreezeCount() {
        return freezeCount;
    }

    public long getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(long timeOut) {
        this.timeOut = timeOut;
    }

    /**
     * Initiates or extends a freeze on a VLSN in response to a new election
     * that is in progress. It's invoked by the Acceptor thread.
     *
     * @param proposal identifies the election that is provoking the freeze
     */
    public synchronized void freeze(Proposal proposal) {
        if ((promisedProposal != null)
            && (proposal.compareTo(promisedProposal)) <= 0) {
            // Older proposal ignore it.
            return;
        }
        if (latch != null) {
            /* Enable waiters who will reacquire the new latch below. */
            latch.countDown();
        }
        latch = new CountDownLatch(1);
        promisedProposal = proposal;
        freezeEnd = TimeSupplier.currentTimeMillis() + timeOut;
        return;
    }

    /**
     * Invoked by the Learner thread whenever it receives an election result.
     * The freeze on the VLSN is only lifted if the proposal associated with
     * the event is current, that is, it represents a proposal that is newer
     * than the one used to establish the freeze.
     *
     * @param proposal identifies the election that just concluded
     */
    public synchronized void vlsnEvent(Proposal proposal,
                                       Value value) {
        if (promisedProposal == null) {
            // No VLSN to unfreeze
            return;
        }
        if (proposal.compareTo(this.promisedProposal) >= 0) {
            learnedProposal = proposal;
            learnedNameIdPair = ((MasterValue) value).getNameId();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    /**
     * Clears the latch freeing any waiters.
     */
    public synchronized void clearLatch() {
        if (latch != null) {
            latch.countDown();
        }
        latch = null;
        freezeEnd = 0;
    }

    /**
     * Waits for an ongoing election to finish before acking an entry.
     *
     * This method is invoked by the Replay thread before replaying a commit
     * log entry in the current sequence:
     *
     * (1) invokes this method and waits
     * (2) writes the entry to the log, together with its vlsn
     * (3) sends an ack to the master
     *
     * We freeze the replay in response to acceptor promises for two purposes:
     *
     * (i) When an acceptor makes a promise, the promise includes a VLSN for
     * comparison to choose the new master. We do not want the VLSN to advance
     * during election which will result in an inaccurate comparison.
     *
     * (ii) Once the acceptor makes the promise with vlsn V for a higher term
     * T' larger than the current term T of master M, we cannot send any ack
     * for V' greater than V to M.
     *
     * To make the guarantee for (ii), a simple solution would be that we throw
     * MasterObsoleteException at step (2) so that the replay thread exits
     * before even sending the ack for V. The RepNode thread will then start a
     * new election and continue with a new valid master. In the common case,
     * however, the master is not frequently changed and the re-election might
     * just be the result of a network hiccup. In such cases, we want the
     * replay to simply continue. Correctness is still guaranteed by checking
     * among latest proposals and master values.
     *
     * Timeout is treated the same as that the master is obsolete.
     *
     * Note that the latch must be re-initialized after a return from this
     * await method.
     *
     * @param currTerm the current term for the master
     * @param currMaster the nameIdPair for the current master
     *
     * @throws MasterObsoleteException if the caller should treat the
     * completion as a master change and not continue the replay
     * @throws InterruptedException
     */
    public void awaitThaw(long currTerm, NameIdPair currMaster)
        throws MasterObsoleteException, InterruptedException {

        CountDownLatch awaitLatch;
        long awaitTimeout;

        synchronized (this) {
            /*
             * Copy out the values of interest to use outside the
             * synchronization block.
             */
            awaitLatch = latch;
            /*
             * Deals with the common case that we do not have an onging
             * election first.
             */
            if (awaitLatch == null) {
                ensureSafeToContinue(currTerm, currMaster);
                return;
            }
            /* Ongoing election, prepare. */
            awaitTimeout = this.freezeEnd - TimeSupplier.currentTimeMillis();
        }

        freezeCount++;

        boolean done = awaitLatch.await(awaitTimeout, TimeUnit.MILLISECONDS);

        /*
         * We are waiting for the ongoing election to be done. After this wait
         * successfully completes, it is always safe to continue after the
         * freeze is lifted. This is because the vlsn we will ack afterwards is
         * the one we sent with the promise.  It might not be safe to ack the
         * next entry, but that entry will be checked with ensureSafeToContinue.
         */
        synchronized (this) {
            if (done) {
                awaitElectionCount++;
                clearLatch();
                return;
            }
            if (this.freezeEnd - TimeSupplier.currentTimeMillis() <= 0) {
                /* freeze end was not extended, timeout. */
                awaitTimeoutCount++;
                clearLatch();
                return;
            }
        }
        /* Re-acquire the new latch and wait for the extended timeout. */
        awaitThaw(currTerm, currMaster);
    }

    /**
     * Ensures whether we can simply continue the replay when we do not
     * encounter a freeze, throws exception otherwise.
     *
     * We should not continue the replay stream if we have made a higher term
     * promise.
     *
     * This could be a redundant check because the ReplayThread checks for
     * masterStatus before invoking awaitThaw and sending back an ack, but it
     * is safer to add an independent check here so that we do not rely on the
     * external synchronization and execution orders.
     */
    private void ensureSafeToContinue(long currTerm, NameIdPair currMaster)
        throws MasterObsoleteException
    {
        assert Thread.holdsLock(this);
        if (promisedProposal == null) {
            /*
             * No promise yet. Sending ack for this entry is safe.
             */
            return;
        }
        final long promisedTerm = promisedProposal.getTimeMs();
        if (promisedTerm <= currTerm) {
            /*
             * The most common case when the saved promisedProposal is of the
             * current (==) or previous (<) term. We may see a previous term
             * promise when the node did not participate the current term
             * election in time. Sending ack for this entry is safe.
             */
            return;
        }
        final long learnedTerm =
            (learnedProposal == null) ? -1 : learnedProposal.getTimeMs();
        if ((promisedTerm <= learnedTerm)
            && (learnedNameIdPair.equals(currMaster))) {
            /*
             * A likely case that the new master of a higher term is the same
             * as the current. Sending ack for this entry is safe.
             */
            return;
        }
        if (promisedTerm > learnedTerm) {
            /*
             * We exited the previous freeze without learning the election
             * result of the correct term, i.e., timed out. It is not safe to
             * ack this entry.
             */
            throw new MasterObsoleteException(currTerm, promisedTerm);
        }
        /*
         * We exited the previous freeze learning about a different master. It
         * is not safe to ack this entry.
         */
        throw new MasterObsoleteException(currMaster, learnedNameIdPair);
    }
}
