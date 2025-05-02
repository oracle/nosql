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

package com.sleepycat.je;

import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.rep.txn.MasterTxn;

/**
 * AsyncAckHandler defines the handlers used to process transaction commit
 * related replica acknowledgments. The method {@link #onQuorumAcks(MasterTxn)}
 * is invoked on successful acknowledgment and
 * {@link #onException(MasterTxn, Exception)} on failure; exactly one of these
 * methods is invoked for every transaction. The handler is not invoked if it
 * is associated with a transaction that does not make any changes or if the
 * {@link ReplicaAckPolicy} does not require acknowledgments.
 * <p>
 * The primary motivation for async ack processing is to free up the
 * application thread as soon the local commit is successful and not hold on to
 * thread resources while waiting for replica acknowledgments. For transactions
 * that make small changes, e.g. an insert, delete, update, the time spent
 * waiting for an acknowledgment, ~500us on average, dominates the time spent
 * executing the transaction, typically 50-100us. So the application thread
 * will spend most of its time idling, while it waits for an ack quorum. Async
 * ack processing frees up the application thread to process the next request
 * immediately and process the acks in a different thread when they become
 * available. So fewer threads (10x reduction) are needed in total to process
 * comparable workloads. Reduced thread context switching and better use of the
 * processor cache, result in significantly lower write request latency,
 * permitting higher throughput at comparable latency. ycsb tests with KV (6
 * request threads) show a 29% increase in throughput, with lower latency
 * (relative to sync), for insert workloads and a 12% increase for a 50%, mixed
 * workload, again with lower request latency.
 * <p>
 * In order to minimize the handlers impact on replication performance, the
 * handler's methods should not block. They should do minimal processing,
 * handing off work to other threads if necessary and return promptly.
 * <p>
 * The handler, if it's shared across multiple transactions, must be reentrant.
 * It's up to the user of the interface to make the appropriate tradeoffs for
 * their application.
 * <p>
 * Implementation note:
 * <p>
 * With async ack processing, there is a small difference in when changes
 * become visible to the application: The changes become visible after the
 * local commit (when locks are released), rather than after the quorum
 * acknowledgment. That is, the changes typically become visible (to a
 * concurrent request) slightly sooner.
 * <p>
 * Consider the following timeline associated with a write request:
 * <ul>
 * <li>t1 - request issued at client
 * <li>t2 - local commit at master, following modifications based on the request
 * <li>t3 - master gets a quorum of acks
 * <li>t4 - response is received at the client
 * </ul>
 * With sync acks the locks are released at t3 by the RN application thread,
 * and the changes are made visible. Other RN threads (processing concurrent
 * uncoordinated requests) can see these changes at any time t > t3, possibly
 * before the client has read the response.
 * <p>
 * With async acks, the locks are released at t2, and control is returned to
 * the RN application thread. The async handler is invoked at t3, and the
 * response is subsequently sent back to the client.
 * <p>
 * Note that in both sync and async cases the change can be visible before the
 * response is received by the client. It's just that in the async case this
 * time window is slightly larger. This difference is indistinguishable from
 * the client's viewpoint, since it can see this behavior today with sync ack
 * processing.
 *
 * @since 23.1
 */
public interface AsyncAckHandler {

    /**
     * The onQuorumAcks callback notifies the application that the
     * {@link ReplicaAckPolicy} associated with a transaction's
     * {@link Durability durability} have been satisfied. The transaction must
     * be configured to use this handler via
     * {@link TransactionConfig#setAsyncAckHandler}.
     * <p>
     * The method is only invoked if the transaction's acknowledgments are
     * received within the smaller of {@link TransactionConfig#getTxnTimeout
     * transaction timeout} or RepParams.REPLICA_ACK_TIMEOUT. If the quorum of
     * acknowledgments is not satisfied within this period,
     * {@link #onException} is invoked, with an InsufficientAcksException,
     * argument.
     * <p>
     * Due to the async nature of the callback, it's possible (especially when
     * the RNs are hosted on the same machine) that this callback is invoked
     * before the return from the commit() call in the application's thread.
     * The application must be robust wrt such non-intuitive call sequences.
     *
     * @param txn the Master txn associated with the quorum acknowledgment
     */
    void onQuorumAcks(MasterTxn txn);

    /**
     * The onException callback notifies the application that a problem was
     * encountered while waiting for the transaction's acknowledgments.
     * <p>
     * It's typically used to report that the acknowledgments were not received
     * within the transaction's specified
     * {@link TransactionConfig#getTxnTimeout transaction timeout} period, but
     * is loosely typed to allow for other exceptions in future.
     *
     * @param txn The transaction associated with the handler
     * @param exception the exception describing the nature of the problem
     */
    void onException(MasterTxn txn, Exception exception);
}
