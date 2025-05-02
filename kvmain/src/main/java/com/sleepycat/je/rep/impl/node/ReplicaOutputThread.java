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

import java.io.IOException;

import com.sleepycat.je.rep.impl.RepImpl;

public class ReplicaOutputThread extends ReplicaOutputThreadBase {
    private final RepNode repNode;

    ReplicaOutputThread(RepImpl repImpl) {
        super(repImpl);
        repNode = repImpl.getRepNode();
    }

    @Override
    public void writeReauthentication() {
    }

    @Override
    public void writeFilterChange() {
    }

    @Override
    public void writeHeartbeat(Long txnId) throws IOException {

        final Replica replica= repNode.getReplica();

        if ((txnId == null) && (replica.getTestDelayMs() > 0)) {
            return;
        }

        final int heartbeatId = (txnId != null) ?
            replica.getMasterHeartbeatId() : -1;
        protocol.write(protocol.new HeartbeatResponse(
                           replica.getTxnEndVLSN(),
                           heartbeatId,
                           repNode.getLocalDurableVLSN()),
                       replicaFeederChannel);
    }
}
