/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.util;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.impl.node.NameIdPair;

import oracle.kv.impl.topo.ResourceId;

/**
 * Base class for threads which manage replicated environment state changes.
 */
public abstract class StateTracker extends ShutdownThread {

    /**
     * The queue used to hold state transitions. Note that only the latest
     * transition is of interest. Consequently, as an optimization, earlier
     * entries can be discarded whenever that's convenient.
     *
     * The queue must at least be of length 2 so that it can hold the last
     * state transition and the EOQ marker
     */
    private final BlockingQueue<StateChangeEvent> stateTransitions =
        new ArrayBlockingQueue<>(2);

    /**
     * The End Of Queue marker associated with the
     * <code>stateTransitions</code> queue.
     */
    private static final StateChangeEvent EOQ_DETACHED_STATE_MARKER =
        new StateChangeEvent(ReplicatedEnvironment.State.DETACHED,
                             NameIdPair.NULL);

    /**
     * The service associated with the state changes.
     */
    protected final ResourceId serviceId;

    protected final Logger logger;

    /**
     * Creates the StateTracker thread.
     *
     * @param name the simple class name of the thread
     * @param serviceId the service ID whose state is to be tracked
     * @param logger the logger to be used
     * @param exceptionHandler exception handler
     */
    protected StateTracker(String name, ResourceId serviceId, Logger logger,
                           UncaughtExceptionHandler exceptionHandler) {
        super(null, exceptionHandler, name + "-serviceId-" + serviceId);
        this.serviceId = serviceId;
        this.logger = logger;
    }

    /**
     * Returns true if there are no more state change events in the queue.
     *
     * @return true if there are no more state change events in the queue
     */
    protected boolean isEmpty() {
        return stateTransitions.isEmpty();
    }

    /**
     * Invoked by the RN whenever the HA listener notifies it of an HA state
     * change. Since the listener thread must not be held up, the method merely
     * queues up the event for async processing by the Manager's thread.
     *
     * Note that this method is invoked sequentially given the semantics of the
     * HA state listener.
     *
     * @param stateChangeEvent the state change event
     */
    public void noteStateChange(StateChangeEvent stateChangeEvent) {

        /*
         * Must not miss state change events. Only the latest state matters.
         */
        while (true) {
            /* retry until the element is in the queue. */
            try {
                stateTransitions.add(stateChangeEvent);
                logger.log(Level.INFO, "{0} queue added:{1}",  // TODO - FINE
                          new Object[]{getName(), stateChangeEvent.getState()});
                return;
            } catch (IllegalStateException e) {

                /*
                 * No space in queue, remove an element and try again, only the
                 * latest state change matters.
                 */
                try {
                    final StateChangeEvent rem = stateTransitions.remove();
                    if (rem == EOQ_DETACHED_STATE_MARKER) {

                        /*
                         * Events that come after EOQ can be ignored. Stick the
                         * EOQ back in the queue.
                         */
                        stateChangeEvent = EOQ_DETACHED_STATE_MARKER;
                        continue;
                    }
                    logger.log(Level.INFO, "{0} entry removed:{1}", // TODO - FINE
                               new Object[]{getName(), rem});
                } catch (NoSuchElementException nsee) {
                    /* Consumed by the run() thread */
                }
            }
        }
    }

    /**
     * The loop which processes state change requests.
     */
    @Override
    public void run() {
        boolean interrupted = false;

        try {
            runInternal();
        } catch (ThreadInterruptedException | InterruptedException tie) {
            /* Expected as part of a hard shutdown. */
            interrupted = true;
        } finally {
            logger.log(Level.INFO, "{0} thread exited: {1}{2}",
                       new Object[]{getName(), serviceId,
                                    interrupted ? " Interrupted. " : ""});
        }
    }

    private void runInternal()
        throws InterruptedException {

        logger.log(Level.INFO, "{0} thread start: {1}",
                   new Object[]{getName(), serviceId});

        while (!isShutdown()) {
            final StateChangeEvent sce = stateTransitions.take();

            if (!isEmpty()) {

                /*
                 * Skip this state change event, if a new state transition
                 * supersedes this one.
                 */
                continue;
            }
            doNotify(sce);

            if (sce == EOQ_DETACHED_STATE_MARKER) {
                break;
            }
        }
    }

    protected abstract void doNotify(StateChangeEvent sce)
        throws InterruptedException;

    @Override
    protected int initiateSoftShutdown() {
        final long limitMs = System.currentTimeMillis() +
                             DEFAULT_SHUTDOWN_TIMEOUT_MS;

        /* Place a special EOQ marker */
        noteStateChange(EOQ_DETACHED_STATE_MARKER);

        /* Return the amount of time left to wait, or -1 if none */
        final long waitMs = limitMs - System.currentTimeMillis();
        return (waitMs <= 0) ? -1 : (int)waitMs;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
