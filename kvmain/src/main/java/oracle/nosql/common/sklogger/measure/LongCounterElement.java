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

import java.util.concurrent.atomic.LongAdder;

import oracle.nosql.common.sklogger.measure.MemorylessHistoryMeasure.CurrentHistory;
import oracle.nosql.common.sklogger.measure.MemorylessHistoryMeasure.StartHistory;

/**
 * An element that measures the value change of a long counter over a period of
 * time.
 *
 * @see ThroughputElement for a throughput-oriented element.
 */
public class LongCounterElement
    extends MemorylessHistoryMeasure<Long,
                                     LongValue,
                                     LongCounterElement.Current,
                                     LongCounterElement.Start> {

    private final Current current = new Current();

    @Override
    protected Current getCurrent() {
        return current;
    }

    @Override
    protected Start createStartHistory() {
        return new Start();
    }

    protected static class Current
            implements CurrentHistory<Long, LongValue, Current, Start> {

        private final LongAdder count = new LongAdder();

        @Override
        public void observe(Long value) {
            count.add(value);
        }

        @Override
        public LongValue compare(Start start) {
            return new LongValue(count.sum() - start.count);
        }
    }

    protected static class Start
            implements StartHistory<Long, LongValue, Current, Start> {

        private long count = 0;

        @Override
        public void set(Current current) {
            count = current.count.sum();
        }
    }
}
