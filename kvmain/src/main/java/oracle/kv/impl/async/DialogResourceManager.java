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

package oracle.kv.impl.async;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.async.perf.DialogResourceManagerPerfTracker;
import oracle.kv.impl.util.ObjectUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Manages resources associated with dialogs, in particular limiting the total
 * number of active dialogs.
 *
 * <h1>The Throttling Problem</h1>
 * <p>We would like to do throttling on the server so that the number of
 * dialogs it handles do not exceeds a quota. There are two aspects of this
 * problem which needs to be addressed:
 * <ol>
 * <li>Quota sharing strategy, i.e., how the server quota is shared among
 * multiple connections (clients), and</li>
 * <li>Throttling behavior, i.e., how the server should react when the number
 * of dialogs exceeds the quota. This aspect of the problem is out of the scope
 * for this object. Simply put, when the server reaches the capacity, it should
 * stop accepting new dialogs. This would create a back pressure all the way to
 * the client. </li>
 * </ol>
 *
 * <h2>Quota Sharing Strategies</h2>
 * <p>Intuitively, the strategy should consider two aspects:
 * <ol>
 * <li>Utilization. Given the server capacity C, a better sharing strategy is
 * the one that have the number of concurrent dialogs closer to C.
 * </li>
 * <li>Fairness. Fairness is not well defined, but intuitively we do not want
 * two connections with similar desire having drastically different quota.
 * </li>
 * </ol>
 *
 * <h3>A Static Strategy</h3>
 * <p>A static but simple strategy for quota sharing is that the server
 * specifies two values: a connection quota, which is the maximum amount of
 * connections the server will handle, and a per connection quota, which is the
 * maximum amount of dialogs each connection can handle.
 *
 * <p>The drawback of this strategy is under-utilization: if the number of
 * connection varies with a large range, the per-connection quota has to be set
 * to a under-estimated value. The per-connection quota is implemented with
 * {@link AsyncOption#DLG_REMOTE_MAXDLGS} and it does not support dynamically
 * adjusting quota due to additional complexity.
 *
 * <h3>A Periodic Adjustment Strategy</h3>
 * <p>Another strategy works by periodically (e.g., every 100ms) computes the
 * quota for each connection. Each connection register a handle to the manager.
 * The overall quota equals to the summation of handle quota plus a global
 * surplus. The global surplus is used for overall quota adjustment, e.g., a
 * negative global surplus means the server is reducing its capacity. The
 * periodic adjustment intends to compute the quota for the next period based
 * on history quota usage for the connections.
 *
 * <p>The adjustment computation can improve server utilization by assigning
 * more permits to connections that use more in the past. The assumption is
 * that the history stats of the connections are representitive for future
 * workload pattern. Whatever fairness definition can be ensured as well.
 *
 * <p>One computation heuristic works as follows. The quota can be represented
 * as permits. The computation can consists of two phases: a donation phase,
 * where each handle donates permits, and a distribution phase, where the
 * donated permits are re-distributed. The permits are of two types: reserved
 * and surplus. The handle can collect a moving average count of the reserved
 * permits which is used the expected value of the future reserved. The
 * expected surplus of a connection is the total minus the expected reserved.
 * During the donation phase, each handle donates 10% of the total permits plus
 * the expected surplus. During the distribution phase, the donated permits are
 * given out to the handles without a surplus if there is any such handle,
 * otherwise, to all the handles.
 *
 * <p>This heuristic intends to avoid under utilization by letting handles with
 * surplus donate more. It intends to ensure a certain level of fairness by
 * letting handles with more permits donate more. The drawback is that the
 * strategy relies on the prediction of expected reserved permits for a
 * connection. There can be pathological cases where the prediction does not
 * work. The heuristic computations are not easy to reason about the effect as
 * well.
 *
 * <h3>A Greedy Strategy</h3>
 * <p>A greedy strategy works by using a global counter, representing the
 * number of available permits, and a global waiting queue. When a connection
 * handle wants to create a new dialog, it decrement counter to acquire a
 * permit if the counter is positive, otherwise, the handle is added into the
 * waiting queue. When a dialog is finished, the handle releases the permit,
 * i.e.,  increment the counter and another handle from the queue can attempt
 * to decrement the counter again.
 *
 * <p>Server utilization is no longer an issue with the greedy strategy since
 * the only limit in the system for starting dialogs is the overall quota.
 * Fairness depends on the waiting queue manipulation. This strategy is adopted
 * due to its simplicity and the guarantee of no server under-utilization.
 *
 * <p>One additional concern with this strategy is that the implementation
 * requires a global counter which can become a global synchronization point.
 * However, such global synchronization point may be fine for a atomic integer
 * since the bandwidth of such memory data structure is two orders of
 * magnitudes (10^6 vs 10^4) larger than the required the throughput of the KV
 * store (see https://arxiv.org/abs/1305.5800 for more information).
 *
 * <h4>Fairness with the Greedy Strategy</h4>
 * <p>Fairness is a concern only when the server is over utilized and the
 * counter is non-positive. In such case, fairness is determined by how newly
 * released permits are assigned to the handles in the waiting queue. We adopt
 * the following mechanism,
 * <ul>
 * <li>Each handle can appear only once inside the waiting queue</li>
 * <li>The waiting queue is maintained in a FIFO manner and the "barging"
 * behavior of a handle acquiring a permits when there are other handles in the
 * queue is not allowed.</li>
 * </ul>
 * With this mechanism, we can achive what I call a "throughput fairness", such
 * that longer-lasting dialogs can start more dialogs. This can be illustrated
 * as follows:
 * <pre>
 * T 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16
 *    a  a
 *    a  a
 *    a  a
 *    b  b  b  b  b  b
 *          a  a
 *          b  b  b  b  b  b
 *          a  a
 *                b  b  b  b  b  b
 *                a  a
 *                      b  b  b  b  b  b
 *                      a  a
 *                            b  b  b  b  b  b
 *                            a  a
 * </pre>
 * <p>The x-axis represents time in seconds. The capacity of the server is a
 * maximum of four concurrent dialogs. There are two connections a and b.
 * Dialogs from connection a takes two seconds, b six seconds, including
 * acquiring the permit, reading the frames, executing the request and
 * releasing the permit. Assuming both connections have infinite number of
 * dialogs ready to start, the above mechanism ensures that dialogs are started
 * from both connections in turn. In steady state, at any time, there are 3
 * dialogs from b but 1 from a.
 *
 * <p>Such can be the desired fairness behavior for many use cases. If not, a
 * weighted mechanism can be applied so that the number of handles in the
 * waiting queue are different based on the connection and fairness
 * requirement.
 */
public class DialogResourceManager {

    /*
     * The current number of available permits. A handle needs to decrement
     * this count to reserve permit before starting dialogs. The count cannot
     * go negative if there is no permit number change. If the value is
     * negative, then we have been asked to reduce the number of permits, and
     * are waiting for reserved permits to be freed.
     *
     * TODO: maybe use the @Contended annotation to prevent false sharing. See
     * JEP 142 and JDK-8140687.
     */
    private final AtomicInteger availableNumPermits;
    /*
     * The FIFO queue for the handles that failed to acquire resources.
     *
     * To ensure fairness, we need to ensure every handle can appear at most
     * once in the queue, otherwise, procedures that made more try-acquire
     * attempts can obtain more permits. This at-most-once behavior is achieved
     * with the Handle#waiting flag.
     */
    private final Queue<Handle> waitingHandles =
        new ConcurrentLinkedQueue<>();
    /* The perf tracker */
    private final DialogResourceManagerPerfTracker perfTracker;

    /* Modifications to the following fields must acquire the object lock. */

    /*
     * Total number of permits, which is the sum of the permits reserved (by
     * the handles) and available permits (i.e., availableNumPermits). Note
     * that this value will be less than the number of reserved permits if
     * availableNumPermits is negative.
     */
    private volatile int totalNumPermits;
    /*
     * The notify callback for when the number of permits has been adjusted to
     * match the value specified in a call to setNumPermits.
     */
    private @Nullable NotePermitsAdjusted notePermitsAdjusted = null;

    /**
     * Constructor with a total number of permits for testing.
     */
    public DialogResourceManager(int n) {
        this(n, new DialogResourceManagerPerfTracker());
    }

    /**
     * Constructor with a total number of permits.
     */
    public DialogResourceManager(
        int n, DialogResourceManagerPerfTracker perfTracker) {

        synchronized(this) {
            this.totalNumPermits = n;
        }
        this.availableNumPermits = new AtomicInteger(n);
        this.perfTracker = perfTracker;
    }

    /**
     * Sets the total number of permits.
     *
     * The latest of the notePermitsAdjusted callbacks set by this method (if
     * not overriden by a later one) will eventually be invoked when the number
     * of total permits is adjusted to the set value.
     */
    public void setNumPermits(int n, NotePermitsAdjusted callback) {
        final int inc;
        synchronized(this) {
            inc = n - totalNumPermits;
            totalNumPermits = n;
            notePermitsAdjusted = callback;
        }
        /*
         * We can invoke the callback only when the available number of permits
         * is non-negative, otherwise, the actual total number of permits will
         * exceeds the set value.
         */
        final int avail = availableNumPermits.addAndGet(inc);
        if (avail >= 0) {
            synchronized(this) {
                if (notePermitsAdjusted != null) {
                    notePermitsAdjusted.onComplete(totalNumPermits);
                    notePermitsAdjusted = null;
                }
            }
        }
        perfTracker.update(avail, totalNumPermits);
    }

    /**
     * Gets the number of available permits.
     */
    public int getNumAvailablePermits() {
        return availableNumPermits.get();
    }

    /**
     * A callback to notify the permits are done adjusting.
     *
     * When reducing the total number of permits, it cannot take effect
     * immediately and the upper layer can see, for a period, the number of
     * permits reserved exceeding the newly-set total. It will evetually reduce
     * to the set number when permits are gradually freed. A callback can be
     * registered to notify the upper layer when that happens.
     *
     * Increasing the total number of permits can take effect immediately. For
     * the upper layer to have a unified code path, the callback will be
     * invoked as well.
     */
    public interface NotePermitsAdjusted {

        /**
         * Called when the total number of permits equals to the set value.
         *
         * @param n the number of total permits
         */
        void onComplete(int n);
    }

    /**
     * Creates a handle and registers a callbcak that should be invoked if the
     * handle was blocked but is now ready to reserve permits.
     */
    public Handle createHandle(String id, HandleCallback callback) {
        return new Handle(id, callback);
    }

    /**
     * Handles reserving and freeing permits.
     *
     * This object assumes that the upper layer will make sure each reserve is
     * always paired with one and only one free.
     */
    public class Handle {

        private final String id;
        /** Whether this handle is waiting to receive a permit. */
        private final AtomicBoolean waiting = new AtomicBoolean(false);
        private final HandleCallback callback;

        public Handle(String id, HandleCallback callback) {
            ObjectUtil.checkNull("callback", callback);
            this.id = id;
            this.callback = callback;
        }

        /**
         * Reserves a permit for this handle or adds it to the waiting handles
         * queue.
         *
         * If there are already other handles waiting to acquire permits, this
         * handle cannot reserve a permit until its predecessors do. A
         * "barging" behavior of immediately acquire the permits is not allowed
         * for fairness.
         *
         * @return if successfully reserved a permit for this handle
         */
        public boolean reserve() {
            onWait();
            while (true) {
                /*
                 * Reserve permits according to the number of handles in the
                 * queue and the available number of permits. There might be a
                 * race when a new handle is added to the waiting queue after
                 * we get the size and somehow the handle cannot reserve a
                 * permit even if there are available permits. In that case,
                 * that waiting handle will not be notified until the next time
                 * a handle reserves or frees a permit. This is equivalent to a
                 * decrease of the total number of permits resulting in a
                 * slight server under-utilization. However, this is OK since
                 * it is rare and it is not a correctness issue.
                 */
                final int available = availableNumPermits.get();
                if (available == 0) {
                    perfTracker.update(available, totalNumPermits);
                    return false;
                }
                final int size = waitingHandles.size();
                final int nreserved = Math.min(available, size);
                final int remaining = available - nreserved;
                if (!availableNumPermits.compareAndSet(available, remaining)) {
                    continue;
                }
                perfTracker.update(remaining, totalNumPermits);
                return drainWaitingHandles(nreserved);
            }
        }

        /**
         * Drains the waitingHandles with the reserved number of permits,
         * returning a boolean for whether this handle was given a permit, and
         * notifying other handles of their permits as needed.
         *
         * Delivers one permit for each handle.
         *
         * @return {@code true} if this handle is given a permit
         */
        private boolean drainWaitingHandles(int nreserved) {
            boolean selfDelivered = false;
            try {
                while (true) {
                    if (nreserved <= 0) {
                        return selfDelivered;
                    }
                    final Handle h = waitingHandles.poll();
                    if (h == null) {
                        return selfDelivered;
                    }
                    nreserved--;
                    final boolean thisHandle = (h == this);
                    h.onReady(!thisHandle);
                    if (thisHandle) {
                        selfDelivered = true;
                    }
                }
            } finally {
                /* Puts back the reserved permits that are not delivered */
                availableNumPermits.getAndAdd(nreserved);
            }
        }

        /**
         * Frees one permits.
         */
        public void free() {
            free(1);
        }

        /**
         * Frees the specified number of permits.
         *
         * Invokes the permit adjustment callback if present and the available
         * number of permits is larger than 0.
         *
         * Invokes the handle callbacks to notify a permit is reserved for it.
         */
        public void free(int n) {
            if (n < 0) {
                throw new IllegalStateException("Freeing a negative number");
            }
            if (n == 0) {
                return;
            }
            final int available = availableNumPermits.get();
            /* If we were asked to reduce the total number of permits */
            if (available < 0) {
                final int inc = n;
                final int p = availableNumPermits.getAndUpdate(
                    (v) -> (v > 0) ? v : Math.min(0, v + inc));
                /* Check if still reducing permits after we freed permits */
                if (p + n < 0) {
                    perfTracker.update(0, totalNumPermits);
                    return;
                }
                /*
                 * No longer reducing permits, i.e., p + n = v + inc >= 0. We
                 * can notify the upper layer that permission adjustment is
                 * done.
                 */
                synchronized(this) {
                    if (notePermitsAdjusted != null) {
                        notePermitsAdjusted.onComplete(totalNumPermits);
                        notePermitsAdjusted = null;
                    }
                }
                /*
                 * If we still have some permits left after we fill in the
                 * total permits deficit, set n to the surplus value and invoke
                 * callbacks on the waiting handles next.
                 */
                n = (p > 0) ? n : n + p;
            }

            /* Freeing the permits */
            while (n > 0) {
                final Handle h = waitingHandles.poll();
                if (h == null) {
                    break;
                }
                /*
                 * If someone is waiting. No need to increment the
                 * availableNumPermits, just invoke the callback on the handle.
                 */
                n--;
                h.onReady(true);
            }
            /*
             * No one is waiting or used up freed permits. If no one is
             * waiting, just increment the counter. The same race as described
             * in the reserve() method can happen as well that a new handle is
             * added to the waiting queue after our poll. This is OK for the
             * same reason that it is rare and it is not a correctness issue.
             *
             * If someone is still waiting, but we used up freed permits, amd
             * there are still available permits, then some race happens during
             * the reserve(). The effect is the same as described above as
             * well.
             */
            if (n > 0) {
                perfTracker.update(
                    availableNumPermits.addAndGet(n), totalNumPermits);
            } else {
                perfTracker.update(
                    availableNumPermits.get(), totalNumPermits);
            }
        }

        /**
         * Called when the handle needs to wait.
         *
         * Do not queue the handle if it is already in.
         */
        private void onWait() {
            if (waiting.compareAndSet(false, true)) {
                waitingHandles.add(this);
            }
        }

        /**
         * Dequeues the handle when a permit has been reserve for it.
         *
         * Notifies the handle or otherwise the handle itself is in the process
         * of reserving the permit.
         */
        private void onReady(boolean needNotify) {
            if (!waiting.get()) {
                throw new IllegalStateException(String.format(
                    "Handle %s was not queued for waiting", id));
            }
            waiting.set(false);
            if (needNotify) {
                callback.onPermitsReserved(1);
            }
        }
    }

    /**
     * Notifies the handler that some permits have been reserved for a handle.
     */
    public interface HandleCallback {

        /**
         * Called when some permits have been reserved for the handle.
         *
         * @param n the number of permits reserved
         */
        void onPermitsReserved(int n);
    }
}
