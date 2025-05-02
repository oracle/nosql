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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import oracle.nosql.common.jss.JsonSerializable;

/**
 * Probes the current system state.
 *
 * <p>A state probe obtains measurement of a current state. The measurement
 * results for all watchers are always the same. A caller frequently supplies
 * the observed state.
 *
 * @see CallbackStateProbe
 */
public class StateProbe<S, R extends JsonSerializable>
    implements Observer<S>, MeasureElement<R> {

    /* The current state */
    protected final AtomicReference<S> currentState;
    /* The result funciton */
    private final Function<S, R> resultFunction;

    /**
     * Constructs the state probe.
     */
    public StateProbe(Function<S, R> resultFunction) {
        this(null, resultFunction);
    }

    public StateProbe(S defaultState,
                      Function<S, R> resultFunction) {
        this.currentState = new AtomicReference<>(defaultState);
        this.resultFunction = resultFunction;
    }

    @Override
    public void observe(S newState) {
        currentState.set(newState);
    }

    @Override
    public synchronized R obtain(String watcherName, boolean clear) {
        return resultFunction.apply(currentState.get());
    }
}

