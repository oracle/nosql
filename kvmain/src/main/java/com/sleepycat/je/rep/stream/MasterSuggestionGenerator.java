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

import static com.sleepycat.je.utilint.VLSN.UNINITIALIZED_VLSN;

import com.sleepycat.je.rep.elections.Acceptor;
import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.RepNode;

/**
 * A Basic suggestion generator.
 *
 * A more sophisticated version may contact other replica nodes to see if it
 * has sufficient connectivity to implement the commit policy in effect for
 * the Replication Group. KIS for now.
 */
public class MasterSuggestionGenerator
    implements Acceptor.SuggestionGenerator {

    private final RepNode repNode;

    /* Determines whether to use pre-emptive ranking to make this
     * node the Master during the next election */
    private boolean forceAsMaster = false;

    /* Used during a forced election to guarantee this proposal as a winner. */
    private static final Ranking PREMPTIVE_RANKING =
        new Ranking(Long.MAX_VALUE, Long.MAX_VALUE, MasterTerm.MAX_TERM);

    /* The ranking used to ensure that a current auth master is reselected. */
    private static final Ranking MASTER_RANKING =
        new Ranking(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1, MasterTerm.MAX_TERM);

    public MasterSuggestionGenerator(RepNode repNode) {
        this.repNode = repNode;
    }

    @Override
    public Value get(Proposal proposal) {
        /* Suggest myself as master */
        return new MasterValue(repNode.getHostName(),
                               repNode.getPort(),
                               repNode.getNameIdPair());
    }

    @Override
    public Ranking getRanking(Proposal proposal) {
        if (forceAsMaster) {
            return PREMPTIVE_RANKING;
        }

        if (repNode.isAuthoritativeMaster()) {
            return MASTER_RANKING;
        }

        final long dtvlsn = repNode.getDTVLSN();
        final long vlsn = repNode.getVLSNIndex().getRange().getLast();

        if (dtvlsn == UNINITIALIZED_VLSN) {
            /*
             * In a preDTVLSN stream segment on a postDTVLSN replica. No
             * DTVLSN information as yet.
             */
            return new Ranking(vlsn, 0, repNode.getNodeId(),
                               MasterTerm.PRETERM_TERM);
        }

        return new Ranking(dtvlsn, vlsn,
                           repNode.getNodeId() /* This node */,
                           repNode.getMasterIdTerm().term);
    }

    /**
     * This entry point is for testing only.
     *
     * It will submit a Proposal with a premptive ranking so that it's
     * guaranteed to be the selected as the master at the next election.
     *
     * @param force determines whether the forced proposal is in effect
     */
    public void forceMaster(boolean force) {
        this.forceAsMaster = force;
    }
}
