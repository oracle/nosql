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

import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Invoked for the event of the learner learning an election result.
 */
public class ElectionPreLearnHook implements Learner.PreLearnHook {

    /** Test hook called before promise. For unit testing */
    public static volatile TestHook<RepNode> testHook;

    private final RepNode repNode;

    public ElectionPreLearnHook(RepNode repNode) {
        this.repNode = repNode;
    }

    @Override
    public boolean learn(Proposal proposal) {
        assert TestHookExecute.doHookIfSet(testHook, repNode);
        final long allowedNextTerm = repNode.getNodeState().getAllowedNextTerm();
        if (proposal.getTimeMs() < allowedNextTerm) {
            /* A lower term, note to ignore. */
            return false;
        }
        return true;
    }
}
