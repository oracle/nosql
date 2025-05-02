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

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.elections.Elections;
import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Proposer.WinningProposal;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.utilint.RepUtils.ExceptionAwareBlockingQueue;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TracerFormatter;

/**
 * A Master Transfer operation.
 * <p>
 * Each Master Transfer operation uses a separate instance of this class.
 * There is usually no more than one instance in the lifetime of a master node,
 * because if the transfer succeeds, the old master node environment becomes
 * invalid and must be closed.  However, if an operation times out, another
 * operation can try again later.  Or, a second operation can "forcibly"
 * supersede an existing operation in progress.
 *
 * @see ReplicatedEnvironment#transferMaster(Set, int, TimeUnit)
 */
public class MasterTransfer {
    /*
     * Phase 2 is expected to complete in a very short period. Since txns are
     * blocked during phase 2 we need an upper bound on its duration. We should
     * not wait until deadlineTimeMs since it typically represents a very long
     * period (KSV specifies 5 minutes). Note that this phase 2 timeout is much
     * longer than the time that should be needed and is only meant to account
     * for unforeseen circumstances (e.g., network failures) or bugs.
     */
    private final static int PHASE_2_TIMEOUT_MS = 10 * 1000;

    /* For overriding phase 2 timeout in tests. */
    private static long phase2TimeoutOverrideMs = 0L;

    final private Set<String> replicas;
    final private long startTimeMs;
    final private long timeout;
    final private long deadlineTimeMs;
    final private RepNode repNode;
    final private Map<String, Long> readyReplicas;
    /* Why the master transfer is being performed. */
    final private String reason;
    volatile private CountDownLatch blocker;

    /**
     * Queue which communicates key events of interest from Feeders regarding
     * the progress of their efforts to catch up with the end of the log.  The
     * existence of this object signifies that (1) the owning Master Transfer
     * object is viable (hasn't been superseded by a later, "forcing" MT
     * operation); and (2) we have not yet discovered a winner.  Once we have
     * chosen a winner we disallow any future attempt to supersede this
     * operation.
     *
     * @see #abort
     * See RepNode.setUpTransfer
     */
    private ExceptionAwareBlockingQueue<VLSNProgress> eventQueue;

    final private Logger logger = LoggerUtils.getLogger(getClass());

    MasterTransfer(
        Set<String> replicas, long timeout, String reason, RepNode repNode) {
        this.replicas = replicas;
        this.timeout = timeout;
        startTimeMs = TimeSupplier.currentTimeMillis();
        deadlineTimeMs = startTimeMs + timeout;
        this.reason = (reason == null ? "unknown" : reason);
        this.repNode = repNode;

        LoggerUtils.info(
            logger, repNode.getRepImpl(),
            String.format(
                "Start Master Transfer, reason=(%s), timeout=%d msec, "
                + "targeting %s",
                reason, timeout, Arrays.toString(replicas.toArray())));
        readyReplicas = new HashMap<>(replicas.size());
        eventQueue = new ExceptionAwareBlockingQueue<>
            (repNode.getRepImpl(), new VLSNProgress(INVALID_VLSN, null, true));
    }

    /**
     * Aborts an existing, in-progress Master Transfer operation, if it hasn't
     * reached the point of no return.
     *
     * @return true, if the operation was cancelled, false if it's too late for
     * a clean cancellation.
     */
    synchronized public boolean abort(Exception e) {
        assert (e != null);
        final ExceptionAwareBlockingQueue<VLSNProgress> queue = getQueue();
        if (queue != null) {
            queue.releasePoll(e);
        }
        return true;
    }

    /**
     * Accepts a Progress event and posts it to our queue for processing by the
     * Master Transfer operation thread.
     */
    synchronized public void noteProgress(VLSNProgress p) {
        final ExceptionAwareBlockingQueue<VLSNProgress> queue = getQueue();
        if (queue != null) {
            queue.add(p);
        }
    }

    /**
     * Informs this Master Transfer operation that the named Feeder is shutting
     * down, because its replica connection has been lost.  This of course
     * means that we can't expect this Feeder to soon catch up with our VLSN.
     * In particular, if we have reached Phase 2 on the strength of the
     * progress of only this one Feeder, then we must revert back to Phase 1.
     * <p>
     * Actually all we do here is post a special kind of "progress" event to
     * our queue; it gets processed for real in the {@code chooseReplica()}
     * thread, along with all the other events.
     *
     * @see #chooseReplica
     */
    void giveUp(String replicaNodeName) {
        noteProgress(VLSNProgress.makeFeederDeathEvent(replicaNodeName));
    }

    synchronized private ExceptionAwareBlockingQueue<VLSNProgress> getQueue() {
        return eventQueue;
    }

    /**
     * Performs the core processing of a Master Transfer operation.  We first
     * wait for one of the candidate target replica nodes to become completely
     * synchronized.  We then send a message to all nodes in the group
     * (including ourselves) announcing which node is to become the new
     * master.
     * <p>
     * If the operation fails we release any transaction commit/abort threads
     * that may have been blocked during phase 2 of the wait.  However, in the
     * success case the release of any such transaction threads is done as a
     * natural by-product of the transition of the environment from master to
     * replica status.
     * <p>
     * For more information on the algorithm see
     * {@link ReplicatedEnvironment#transferMaster(Set, int, TimeUnit)}
     */
    String transfer() {
        boolean winnerAnnounced = false;
        boolean done = false;
        try {
            String result = chooseReplica();
            done = true;
            synchronized (this) {
                eventQueue = null;
            }
            winnerAnnounced = annouceWinner(result);
            return result;
        } catch (MasterTransferFailureException e) {
            LoggerUtils.warning(logger, repNode.getRepImpl(),
                                "Master Transfer operation failed: " + e);
            throw e;
        } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(repNode.getRepImpl(), ie);
        } finally {
            eventQueue = null;
            if (!(done && winnerAnnounced) && blocker != null) {
                blocker.countDown();
            }
        }
    }

    /**
     * Prepares for a Master Transfer operation by waiting for one of the
     * nominated candidate target replica nodes to catch up with the master,
     * in two phases, as described in
     * {@link ReplicatedEnvironment#transferMaster(Set, int, TimeUnit)}.
     * <p>
     * This method works by observing events generated by Feeder threads and
     * passed to us via a queue.
     * <p>
     * For more information on the algorithm see
     * {@link ReplicatedEnvironment#transferMaster(Set, int, TimeUnit)}
     *
     * @return the node name of the first replica to complete phase 2 of the
     * preparation.
     *
     * @throws MasterTransferFailureException if the operation times out.
     */
    private String chooseReplica() throws InterruptedException {
        final ExceptionAwareBlockingQueue<VLSNProgress> queue = getQueue();
        final FeederManager feederManager = repNode.feederManager();

        // if repNode is not AuthoritativeMaster, then dont start transfer
        if (!repNode.isAuthoritativeMaster()) {
            throw new MasterTransferFailureException(
                    "Master is no more authoritative");
        }
        for (String nodeName : replicas) {
            final Feeder feeder = feederManager.getFeeder(nodeName);
            if (feeder != null) {
                feeder.setMasterTransfer(this);
            }
        }

        /*
         * Phase 1 could last a long time, if all of our candidate replicas are
         * still catching up (or not even connected); so we allow new
         * transactions to be written.  But once we get to phase 2 we block
         * commit/abort operations for a final (quicker) catch-up.  Thus we can
         * tell whether we're in phase 2 by whether we have a non-null blocker.
         *
         * The specified deadline applies to phase 1 until phase 2 starts, and
         * then it is changed to reflect the expectation that phase 2 is short.
         * The endVLSN is determined after blocking at the start of phase 2,
         * since we must ensure that a replica reaches the last commit/abort.
         */
        long endVLSN = NULL_VLSN;
        long deadlineMs = deadlineTimeMs;
        String result = null;
        for (;;) {
            final long pollTimeout = deadlineMs - TimeSupplier.currentTimeMillis();
            final VLSNProgress event =
                queue.pollOrException(pollTimeout, TimeUnit.MILLISECONDS);
            if (event == null) {
                throw new MasterTransferFailureException(
                    (blocker == null)
                        ? getPhase1TimeoutMsg()
                        : getPhase2TimeoutMsg());
            }
            if (event.isNodeNotMasterEvent()) {
                throw new MasterTransferFailureException("Master Transfer" +
                    " Failed as repNode is not authoritative");
            }
            Level level = Level.INFO;
            if (event.isFeederDeathEvent()) {
                readyReplicas.remove(event.replicaNodeName);
                if (blocker != null && readyReplicas.isEmpty()) {

                    /*
                     * Must revert back to phase 1.  The latch will still
                     * exist, because we've passed it to repImpl; and this is
                     * exactly what we want, so that blocked txns can proceed,
                     * and new ones won't get blocked for now.
                     */
                    blocker.countDown();
                    blocker = null;
                    endVLSN = NULL_VLSN;
                    deadlineMs = deadlineTimeMs;
                }
            } else if (blocker == null) {   /* phase 1 */
                assert readyReplicas.isEmpty();
                readyReplicas.put(event.replicaNodeName, event.vlsn);
                blocker = new CountDownLatch(1);
                repNode.getRepImpl().blockTxnCompletion(blocker);
                deadlineMs = TimeSupplier.currentTimeMillis() + getPhase2TimeoutMs();
                endVLSN = repNode.getCurrentTxnEndVLSN();
                /*
                 * >= comparison, here and below, since currentTxnEndVLSN can
                 * lag the latest txnEndVLSN actually written to the log.
                 */
                if (event.getVLSN() >= endVLSN) {
                    result = event.replicaNodeName;
                }
            } else {            /* phase 2 */
                if (event.getVLSN() >= endVLSN) {
                    result = event.replicaNodeName;
                } else {

                    /*
                     * The present VLSN does not match the ultimate target
                     * VLSN, so we're not done yet.  Since there could be a few
                     * events of this type, only log all of them at the
                     * {@code FINE} level.
                     */
                    readyReplicas.put(event.replicaNodeName, event.vlsn);
                    level = Level.FINE;
                }
            }

            /* Emit log message after the fact */
            LoggerUtils.logMsg(logger, repNode.getRepImpl(), level,
                               "Master Transfer progress: " +
                               event.replicaNodeName + ", " + event.vlsn +
                               ", phase: " + (blocker == null ? 1 : 2) +
                               ", endVLSN: " + endVLSN);
            if (result != null) {
                return result;
            }
        }
    }

    /**
     * Broadcasts a fake election result message.  This does a couple things:
     * (1) prods the chosen replica to become the new master; and (2) forces
     * the old master to notice and shut down with a master-replica transition
     * exception.
     */
    private boolean annouceWinner(String nodeName) {
        final RepGroupImpl group = repNode.getGroup();
        RepNodeImpl node = group.getNode(nodeName);
        MasterValue newMaster = new MasterValue
            (node.getSocketAddress().getHostName(),
             node.getSocketAddress().getPort(),
             node.getNameIdPair());
        final Elections elections = repNode.getElections();
        final Learner learner = elections.getLearner();
        final Proposal proposal =
            repNode.getElections().getProposer().nextProposal();

        Learner.informLearners
            (group.getAllLearnerSockets(),
             new WinningProposal(proposal, newMaster, null),
             elections.getProtocol(),
             elections.getThreadPool(),
             elections.getLogger(),
             repNode.getRepImpl(),
             null);
        /*
         * Proposal was accepted at this node, it will transition to replica.
         */
        return proposal.compareTo(learner.getCurrentProposal()) == 0;
    }

    /**
     * Enables the given {@code Feeder} to contribute to this Master Transfer
     * operation.  Called from the {@code FeederManager} when a new {@code
     * Feeder} is established during the time when a Master Transfer operation
     * is already in progress.
     */
    void addFeeder(Feeder f) {
        String name = f.getReplicaNameIdPair().getName();
        if (replicas.contains(name)) {
            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Add node " + name +
                             " to existing Master Transfer");
            f.setMasterTransfer(this);
        }
    }

    public long getStartTime() {
        return startTimeMs;
    }

    /**
     * Log various information about the master transfer after it ends.
     *
     * @param success true if the transfer succeeded, false otherwise.
     * @param failureReason the reason the master transfer failed, if it did
     * fail.
     */
    public void logMasterTransferEnd(boolean success, String failureReason) {
        final DateFormat format = TracerFormatter.makeDateFormat();
        final StringBuilder sb = new StringBuilder("Master Transfer ended in");
        if (success) {
            sb.append(" success.");
        } else {
            sb.append(" failure because: " + failureReason);
        }

        sb.append("  Previous master: " + repNode.getName());
        sb.append("  Current master: "
            + (repNode.getMasterName() == null
            ? "unknown" : repNode.getMasterName()));

        final long endTime = TimeSupplier.currentTimeMillis();
        final long duration = endTime - startTimeMs;
        sb.append("  Started at: " + format.format(new Date(startTimeMs)));
        sb.append("  Ended at: " + format.format(new Date(endTime)));
        sb.append("  Transfer took " + duration + " ms");
        sb.append("  Reason for master transfer: " + reason);
        LoggerUtils.info(logger, repNode.getRepImpl(), sb.toString());
    }

    /**
     * Generates a detailed error message for the case when the operation times
     * out without reaching phase 2.
     */
    private String getPhase1TimeoutMsg() {
        return "Timed out during phase 1: did not receive any acks for " +
            timeout + " ms" + extraTimeoutInfo();
    }

    private String getPhase2TimeoutMsg() {
        return "Timed out during phase 2: received acks but a replica did" +
            " not catch up within " + getPhase2TimeoutMs() + " ms" +
            extraTimeoutInfo();
    }

    private String extraTimeoutInfo() {
        final DateFormat format = TracerFormatter.makeDateFormat();

        return "\nTransfer started=" + format.format(new Date(startTimeMs)) +
            ", current time=" + format.format(new Date(startTimeMs)) +
            ", current time=" +
            format.format(new Date(TimeSupplier.currentTimeMillis())) +
            ", master's VLSN=" + repNode.getCurrentTxnEndVLSN() +
            repNode.dumpAckFeederState();
    }

    /**
     * An event of interest in the pursuit of our goal of completing the Master
     * Transfer.  Generally it indicates that the named replica has received
     * and processed the transaction identified by the given VLSN.  As a
     * special case, an event representing the death of a Feeder is represented
     * by a {@code null} VLSN.
     */
     public static class VLSNProgress {

        final long vlsn;
        final String replicaNodeName;
        final boolean isNodeMaster;
        public VLSNProgress(long vlsn, String replicaNodeName,
                     boolean isNodeMaster) {
            this.vlsn = vlsn;
            this.replicaNodeName = replicaNodeName;
            this.isNodeMaster = isNodeMaster;
        }

        static VLSNProgress makeFeederDeathEvent(String nodeName) {
            return new VLSNProgress(INVALID_VLSN, nodeName, true);
        }

        long getVLSN() {
            assert vlsn != INVALID_VLSN;
            return vlsn;
        }

        boolean isFeederDeathEvent() {
            return vlsn == INVALID_VLSN;
        }

        boolean isNodeNotMasterEvent() {
            return isNodeMaster == false;
        }
    }

    public static long getPhase2TimeoutMs() {
        return (phase2TimeoutOverrideMs != 0) ?
            phase2TimeoutOverrideMs : PHASE_2_TIMEOUT_MS;
    }

    /** For overriding phase 2 timeout in tests. */
    public static void setPhase2TimeoutOverrideMs(long val) {
        phase2TimeoutOverrideMs = val;
    }
}
