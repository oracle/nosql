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

import com.sleepycat.je.rep.elections.Acceptor;
import com.sleepycat.je.rep.elections.Acceptor.PrePromiseResult;
import com.sleepycat.je.rep.elections.Acceptor.PrePromiseResultType;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Invoked for the event of the acceptor making a promise for a proposal.
 *
 * <p>This hook maintains one of the core invariants for the log replication
 * protocol: once a promise of a higher term is made, no ack will be sent for
 * lower term.  The hook is the core piece of the code for the interaction
 * between the acceptor promise and other components to maintain the above
 * invariant.
 *
 * <p>
 * The hook routine does the following things:
 * <ol>
 * <li>Verifies that the node has not transit to either master or replica of a
 * term greater than or equal to the current proposal.</li>
 * <li>Persist this new proposal to be promised.</li>
 * <li>Marks to stop streaming from the current term if it is lower.</li>
 * <li>Marks to stop establish connection to the master of lower term.</li>
 * </ol>
 *
 * <p>
 * For a replica, when a promise is being made, the stream replay is stopped
 * with CommitFreezeLatch. The replay is either resumed after necessary check
 * or it breaks out of the loop and starts to connect to the new master or
 * starts a new election.
 *
 * <p>
 * For a master, masterStatus.assertSync is checked in the
 * Feeder#runResponseLoop so that if a new master is learned, the master breaks
 * out of the loop and stops streaming. This still does not guarantee the
 * invariants during the time a promise is made and the new master is learned.
 * I think this is still OK: it is not possible for both that a different
 * master M2 to obtain promise from the current authoritative master M1 and
 * that M1 continues to receive majority acks. For M2 to obtain promise from M1
 * which is the current authoratitive master and then elected the new master,
 * M2 must have entries of higher term than M1. Therefore, M2 or another
 * master, in the previous election of a higher term than M1, must obtain
 * majority acks from a quorum. This quorum will have overlap with the ack
 * quorum of M1. But this is not possible.
 */
public class ElectionPrePromiseHook
    extends Acceptor.DefaultPrePromiseHook {

    /** Test hook called before promise. For unit testing */
    public static volatile TestHook<RepNode> testHook;

    private final RepNode repNode;

    public ElectionPrePromiseHook(RepNode repNode) {
        super(repNode.getElectionStateContinuation());
        this.repNode = repNode;
    }

    @Override
    public PrePromiseResult promise(Proposal proposal) {
        assert TestHookExecute.doHookIfSet(testHook, repNode);
        final ElectionStatesContinuation electionStatesContinuation =
            repNode.getElectionStateContinuation();
        if (!electionStatesContinuation.pastContinuationBarrier()) {
            return PrePromiseResult.createIgnore();
        }

        final long proposedTerm = proposal.getTimeMs();
        /* Default check against election states. */
        final PrePromiseResult defaultCheckResult = super.promise(proposal);
        if (!defaultCheckResult.getResultType()
            .equals(PrePromiseResultType.PROCEED)) {
            return defaultCheckResult;
        }

        /*
         * Ensure proposal greater or equal to allowed. This check overlaps
         * with the above election state check w.r.t. the promised terms, but
         * it also checks against learned terms.
         */
        final long allowedNextTerm = repNode.getNodeState().getAllowedNextTerm();
        if (proposedTerm < allowedNextTerm) {
            return PrePromiseResult.createReject(allowedNextTerm);
        }

        /*
         * Marks to stop streaming for lower term. This will stop any Replica
         * stream from ack lower term. See Replay and
         * CommitFreezeLatch.awaitThaw.
         */
        repNode.getVLSNFreezeLatch().freeze(proposal);

        /* Marks to stop establish connection for lower term. */
        repNode.getNodeState().updateAllowedNextTerm(proposedTerm);

        return PrePromiseResult.createProceed();
    }
}
