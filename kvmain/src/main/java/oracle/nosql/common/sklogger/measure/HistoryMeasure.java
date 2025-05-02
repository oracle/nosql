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
import java.util.function.Supplier;

import oracle.nosql.common.jss.JsonSerializable;

/**
 * Produces a measurement based on state history.
 *
 * <p>An naive approach for keeping history for measurement for each watcher is
 * to update the history of each watcher for each observation, getting a O(w)
 * complexity for observe and O(1) complexity for obtain.  Since observe is
 * more frequent, this class adopts a better approach that has a O(1)
 * complexity for observe and O(w) complexity for obtain.
 *
 * <p>This approach divides the history into two consecutive segments: (1) a
 * past history starting from the start-time of a watcher until the last
 * obtain time of any watcher and (2) a recent history starting from the last
 * obtain time of any watcher until present. We update the recent history on
 * observe and updates the past history for all watchers on obtain.
 *
 * <p>Concrete measurement classes implements by extending this class and
 * implements {@link RecentHistory} and {@link PastHistory} interfaces. Except
 * for {@link RecentHistory#observe}, all methods from these two interfaces are
 * executed inside a synchronization block of this class during obtain.
 * Inaccuracy resulting from not synchronizing between observe and obtain can
 * usually be ignored.
 */
public class HistoryMeasure<
    S, R extends JsonSerializable,
    RH extends HistoryMeasure.RecentHistory<S, R, RH, PH>,
    PH extends HistoryMeasure.PastHistory<S, R, RH, PH>>
    implements Observer<S>, MeasureElement<R> {

    /*
     * Contains the recent history between the latest obtain on any watcher to
     * the present
     */
    protected final RH historySinceAnyCollect;
    /* The supplier which creates a past history */
    protected final Supplier<PH> pastHistorySupplier;
    /*
     * A map of histories each of which contains the state since the watcher's
     * starting time (last clear) until the latest clear on any watcher.
     */
    protected final Map<String, PH> historyUntilAnyCollectMap =
        new HashMap<>();
    /*
     * A past history contains the state since the beginning of the measure
     * until the latest clear on any watcher. Used to initialize a newly
     * created past history.
     */
    protected final PH pastHistoryUntilAnyCollect;

    /**
     * Constructs the history measure.
     */
    public HistoryMeasure(RH recentHistory,
                          Supplier<PH> pastHistorySupplier) {
        this.historySinceAnyCollect = recentHistory;
        this.pastHistorySupplier = pastHistorySupplier;
        this.pastHistoryUntilAnyCollect = pastHistorySupplier.get();
    }

    /**
     * Contains the recent history.
     */
    public interface RecentHistory<
        S, R,
        RH extends RecentHistory<S, R, RH, PH>,
        PH extends PastHistory<S, R, RH, PH>>
        extends Observer<S> {
        /**
         * Resets the recent history after a obtain.
         */
        void reset();

        /**
         * Computes the measurement result with the past history.
         */
        R computeWith(PH pastHistory);
    }

    /**
     * Contains the past history.
     */
    public interface PastHistory<
        S, R,
        RH extends RecentHistory<S, R, RH, PH>,
        PH extends PastHistory<S, R, RH, PH>> {
        /**
         * Resets the past history after a obtain.
         */
        void reset();

        /**
         * Combines with the recent history.
         */
        void combine(RH recentHistory);

        /**
         * Inherits from another past history.
         */
        void inherit(PH other);
    }

    @Override
    public void observe(S newState) {
        historySinceAnyCollect.observe(newState);
    }

    @Override
    public synchronized R obtain(String watcherName, boolean clear) {
        final PH historyUntilAnyCollect =
            historyUntilAnyCollectMap.computeIfAbsent(
                watcherName, k -> {
                    final PH past = pastHistorySupplier.get();
                    past.inherit(pastHistoryUntilAnyCollect);
                    return past;
                });
        /*
         * Combine the history of before and after any reset and applies
         * the metric function.
         */
        final R result =
            historySinceAnyCollect.computeWith(historyUntilAnyCollect);
        if (clear) {
            /*
             * Combine the recent history into the past history of all the
             * watchers (except for the current watcher).
             */
            historyUntilAnyCollectMap.entrySet().stream().
                filter(e ->
                       (watcherName == null) ?
                       (watcherName != e.getKey()) :
                       !watcherName.equals(e.getKey())).
                forEach(e -> e.getValue().combine(historySinceAnyCollect));
            pastHistoryUntilAnyCollect.combine(historySinceAnyCollect);
            /*
             * Do resets for the watcher's past history and the global recent
             * history.
             */
            historyUntilAnyCollect.reset();
            historySinceAnyCollect.reset();
        }
        return result;
    }
}

