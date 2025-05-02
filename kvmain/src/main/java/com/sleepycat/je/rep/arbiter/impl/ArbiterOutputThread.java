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
package com.sleepycat.je.rep.arbiter.impl;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.ReplicaOutputThreadBase;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.stream.Protocol;

/**
 * The ArbiterOutputThread reads transaction identifiers
 * from the outputQueue and writes a acknowledgment
 * response to to the network channel. Also used
 * to write responses for heart beat messages.
 */
public class ArbiterOutputThread extends ReplicaOutputThreadBase {
    private final ArbiterVLSNTracker vlsnTracker;
    private final ArbiterAcker arbiterAcker;

    public ArbiterOutputThread(ArbiterAcker arbiterAcker,
                               RepImpl repImpl,
                               BlockingQueue<Long> outputQueue,
                               Protocol protocol,
                               DataChannel replicaFeederChannel,
                               ArbiterVLSNTracker vlsnTracker) {
        super(repImpl, outputQueue, protocol, replicaFeederChannel);
        this.arbiterAcker = arbiterAcker;
        this.vlsnTracker = vlsnTracker;
    }

    public void writeHeartbeat(Long txnId) throws IOException {
        long vlsn = vlsnTracker.get();
        final int heartbeatId = (txnId != null) ?
            arbiterAcker.getMasterHeartbeatId() :
            -1;
        protocol.write(protocol.new HeartbeatResponse(vlsn, heartbeatId),
                       replicaFeederChannel);
    }

    @Override
    public void writeReauthentication() {
    }

    @Override
    public void writeFilterChange() {
    }
}
