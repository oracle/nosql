/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.sklogger.measure;

import java.util.HashMap;
import java.util.Map;

import oracle.nosql.common.jss.JsonSerializable;

/**
 * Produces a measurement based on a history and a memory-less result function.
 *
 * <p>This class assumes that the result produced by the history only depends
 * on the beginning and the end of a history.
 *
 * <p>Concrete measurement classes implements by extending this class and
 * implements {@link CurrentHistory} and {@link StartHistory} interfaces.
 * Except for {@link CurrentHistory#observe}, all methods from these two
 * interfaces are executed inside a synchronization block of this class during
 * the obtain call. Inaccuracy resulting from not synchronizing between observe
 * and obtain can usually be ignored.
 */
public abstract class MemorylessHistoryMeasure<
    S, R extends JsonSerializable,
    CH extends MemorylessHistoryMeasure.CurrentHistory<S, R, CH, SH>,
    SH extends MemorylessHistoryMeasure.StartHistory<S, R, CH, SH>>
    implements Observer<S>, MeasureElement<R> {

    /*
     * A map of histories each of which contains the start-time history of a
     * watcher.
     */
    protected final Map<String, SH> startHistoryMap = new HashMap<>();

    /**
     * Returns the current history.
     */
    protected abstract CH getCurrent();

    /**
     * Creates a new start history.
     */
    protected abstract SH createStartHistory();

    /**
     * Contains the current history.
     */
    public interface CurrentHistory<
        S, R,
        CH extends CurrentHistory<S, R, CH, SH>,
        SH extends StartHistory<S, R, CH, SH>>
        extends Observer<S> {
        /**
         * Returns the measurement result which is obtained by comparing with
         * the start history.
         */
        R compare(SH startHistory);
    }

    /**
     * Contains the start-time history.
     */
    public interface StartHistory<
        S, R,
        CH extends CurrentHistory<S, R, CH, SH>,
        SH extends StartHistory<S, R, CH, SH>> {

        /**
         * Sets the start history to current.
         */
        void set(CH currentHistory);

    }

    @Override
    public void observe(S newState) {
        getCurrent().observe(newState);
    }

    @Override
    public synchronized R obtain(String watcherName, boolean clear) {
        final SH startHistory =
            startHistoryMap.computeIfAbsent(
                watcherName, k -> createStartHistory());
        final CH current = getCurrent();
        final R result = current.compare(startHistory);
        if (clear) {
            startHistory.set(current);
        }
        return result;
    }
}
