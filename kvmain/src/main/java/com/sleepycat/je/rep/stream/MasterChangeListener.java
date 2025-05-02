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

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;

import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.MasterTransfer.VLSNProgress;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * The Listener registered with Elections to learn about new Masters
 */
public class MasterChangeListener implements Learner.Listener {

    private final RepNode repNode;
    private final Logger logger;

    public MasterChangeListener(RepNode repNode) {
        this.repNode = repNode;
        logger = LoggerUtils.getLogger(getClass());
    }

    /**
     * Implements the Listener protocol. The method should not have any
     * operations that might wait, since notifications are single threaded.
     */
    @Override
    public void notify(Proposal proposal, Value value) {

        MasterValue masterValue = ((MasterValue) value);

        /*
         * Sets the master status first before thaw the vlsn freeze. Otherwise,
         * there could be a race in Replica loop such that the replay is
         * continued after an unfreeze not noticing the master should be
         * changed.
         */
        final MasterStatus masterStatus = repNode.getMasterStatus();
        final boolean inSync = masterStatus.setGroupMaster
            (masterValue.getHostName(),
             masterValue.getPort(),
             masterValue.getNameId(), proposal);

        /* If there is a master change in the shard 
         * then stop the master transfer as it is no more master.
         */         
        if (!masterStatus.isNodeMaster()) {
             if (repNode.getActiveTransfer() != null) {
                 repNode.getActiveTransfer().noteProgress(
                            new VLSNProgress(INVALID_VLSN, null, false));
             }  
        }

        /*
         * Bumps the term if we directly sync the masterStatus.
         *
         * Thread-safety discussion:
         * 1. MasterStatus group info is only updated here in the learner
         * thread except shutdown. Therefore, the update info, sync and node
         * state change here is serialized. Furthermore, the
         * RepNode.preLearnHook ignores lower term values. Therefore, we are
         * always observing the latest term here.
         * 2. Sync and node change happens both here and in RepNode loop. The
         * non-atomicity between sync and node change will cause these
         * operations being interleaved. We can still claim the guarantee of
         * serializability among these operations:
         * (1) We only directly sync and bump term here if the master stay the
         * same, therefore, there must be a master status update and sync
         * before the direct sync here that choose the same master.
         * (2) There can be master status updates only after the bump term here
         * due to 1.
         * (3) According to (1) and (2), the concurrent state change happens in
         * the RepNode loop must be a lower term transition to the same master.
         * If that transition happens after the one here, then there is no
         * problem. Otherwise, that transition will observe a
         * MasterObsoleteException and loop again.
         */
        if (inSync && masterStatus.isNodeMaster()) {
            repNode.getNodeState().
                changeAndNotify(ReplicatedEnvironment.State.MASTER,
                                masterStatus.getNodeMasterNameId(),
                                masterStatus.getNodeMasterIdTerm().term,
                                (moe) -> {
                                    final String msg = String.
                                        format("Unexpected master obsolete: %s",
                                               moe);
                                    throw new IllegalStateException(msg, moe);
                                });
        }

        final StringBuilder sb = new StringBuilder();
        sb.append(String.format("Master changed to %s at term %s",
                                masterValue.getNameId().getName(),
                                MasterTerm.logString(proposal.getTimeMs())));
        sb.append(", group status: ");
        final Set<RepNodeImpl> members =
            repNode.getGroup().getAllMembers(null);
        if (members == null || members.size() == 0) {
            sb.append("empty.");
        } else {
            sb.append(
                members.stream()
                .map((n) ->
                     String.format(
                         "%s:%s%s",
                         n.getName(), n.getType(),
                         (n.isRemoved() ? "removed" : "")))
                .collect(Collectors.joining(", ")));
            sb.append(".");
        }
        LoggerUtils.info(logger, repNode.getRepImpl(), sb.toString());

        /*
         * Release the freeze on the replay stream. The stream continues ack
         * for the current VLSN.  It then checks for the assertSync on the next
         * entry and re-connects to the new master if not inSync. Since we sync
         * directly for the same master, the stream will simply continue in
         * this case.
         */
        repNode.getVLSNFreezeLatch().vlsnEvent(proposal, value);
    }
}
