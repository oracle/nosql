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
package com.sleepycat.je.rep;

/**
 * An asynchronous mechanism for tracking the {@link
 * ReplicatedEnvironment.State State} of the replicated environment and
 * choosing how to route database operations.  {@code State} determines which
 * operations are currently permitted on the node. For example, only the {@link
 * ReplicatedEnvironment.State#MASTER MASTER} node can execute write
 * operations.
 * <p>
 * The Listener is registered with the replicated environment using {@link
 * ReplicatedEnvironment#setStateChangeListener(StateChangeListener)}.  There
 * is at most one Listener associated with the actual environment (not an
 * {@link com.sleepycat.je.Environment} handle) at any given instance in time.
 * <p>
 * {@literal See} the <a
 * href="{@docRoot}/../examples/je/rep/quote/package-summary.html">
 * examples</a> for information on different approaches toward routing
 * database operations and an example of using the StateChangeListener.
 * @see <a href="{@docRoot}/../ReplicationGuide/replicawrites.html">Managing
 * Write Requests at a Replica</a>
 */
public interface StateChangeListener {

    /**
     * The notification method. It is initially invoked when the {@code
     * StateChangeListener} is first associated with the {@code
     * ReplicatedEnvironment} via the {@link
     * ReplicatedEnvironment#setStateChangeListener(StateChangeListener)}
     * method and subsequently each time there is a state change.
     * <p>
     * This method should do the minimal amount of work, queuing any resource
     * intensive operations for processing by another thread before returning
     * to the caller, so that it does not unduly delay the other housekeeping
     * operations performed by the internal thread which invokes this method.
     *
     * @param stateChangeEvent the new state change event
     * @throws RuntimeException Any uncaught exceptions will result in the
     * shutdown of the ReplicatedEnvironment.
     */
   public void stateChange(StateChangeEvent stateChangeEvent)
       throws RuntimeException;
}
