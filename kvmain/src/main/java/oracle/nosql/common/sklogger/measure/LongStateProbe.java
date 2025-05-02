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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.jss.JsonSerializable;
import oracle.nosql.common.jss.ObjectNodeSerializable.LongField;


/**
 * Probes the current system state.
 *
 * <p>A state probe obtains measurement of a current state. The measurement
 * results for all watchers are always the same. A caller frequently supplies
 * the observed state.
 *
 * @see CallbackStateProbe
 */
public class LongStateProbe<R extends JsonSerializable>
    implements Observer<Long>, MeasureElement<R> {

    /* The current state */
    protected final AtomicLong currentState;
    /* The result funciton */
    private final LongFunction<R> resultFunction;

    /**
     * Constructs the state probe.
     */
    public LongStateProbe(LongFunction<R> resultFunction) {
        this(0, resultFunction);
    }

    public LongStateProbe(long defaultState,
                          LongFunction<R> resultFunction) {
        this.currentState = new AtomicLong(defaultState);
        this.resultFunction = resultFunction;
    }

    @Override
    public void observe(Long newState) {
        currentState.set(newState);
    }

    public long getValue() {
        return currentState.get();
    }

    public long incrValue() {
        return currentState.incrementAndGet();
    }

    public long incrValue(long delta) {
        return currentState.addAndGet(delta);
    }

    public long decrValue() {
        return currentState.decrementAndGet();
    }

    public long decrValue(long delta) {
        return currentState.addAndGet(-delta);
    }

    @Override
    public synchronized R obtain(String watcherName, boolean clear) {
        return resultFunction.apply(currentState.get());
    }

    /**
     * A convenient result class.
     */
    public static class Result implements JsonSerializable {

        public static final Result DEFAULT = new Result(0);

        private final long value;

        public Result(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public JsonNode toJson() {
            return JsonUtils.createJsonNode(value);
        }

        @Override
        public boolean isDefault() {
            return value == LongField.DEFAULT;
        }
    }
}

