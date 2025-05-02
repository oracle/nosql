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

package oracle.kv.impl.async.perf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.measurement.ConciseStats;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;
import oracle.nosql.common.sklogger.measure.LongValueStats;
import oracle.nosql.common.sklogger.measure.LongValueStatsField;

/**
 * A collection of NioChannelThreadPool perf metrics.
 */
public class NioChannelThreadPoolPerf extends ObjectNodeSerializable {

    /** The field map. */
    public static final Map<String, Field<?, NioChannelThreadPoolPerf>>
        FIELDS = new HashMap<>();
    /** The start timestamp of the perf collection interval. */
    public static final String INTERVAL_START =
        LongField.create(FIELDS, "intervalStart", (p) -> p.intervalStart);
    /** The end timestamp of the perf collection interval. */
    public static final String INTERVAL_END =
        LongField.create(FIELDS, "intervalEnd", (p) -> p.intervalEnd);
    public static final String VALUE =
        JsonSerializableField.create(
            FIELDS, "value",
            (p) -> p.value,
            (p, d) -> new PerfValue(p),
            PerfValue.VALUE_DEFAULT);

    /** The default. */
    public static final NioChannelThreadPoolPerf DEFAULT =
        new NioChannelThreadPoolPerf(
            LongField.DEFAULT, LongField.DEFAULT, PerfValue.VALUE_DEFAULT);

    /* Fields */

    private final long intervalStart;
    private final long intervalEnd;
    private final PerfValue value;

    public NioChannelThreadPoolPerf(long intervalStart,
                                    long intervalEnd,
                                    LongValueStats numAliveExecutors,
                                    int numUnresponsiveExecutors,
                                    List<NioChannelExecutorPerf> executorPerfs)
    {
        this(intervalStart, intervalEnd,
             new PerfValue(numAliveExecutors,
                           numUnresponsiveExecutors,
                           executorPerfs));
    }

    public NioChannelThreadPoolPerf(long intervalStart,
                                    long intervalEnd,
                                    PerfValue perf)
    {
        this.intervalStart = intervalStart;
        this.intervalEnd = intervalEnd;
        this.value = perf;
    }

    public NioChannelThreadPoolPerf(JsonNode payload) {
        this(readField(FIELDS, payload, INTERVAL_START),
             readField(FIELDS, payload, INTERVAL_END),
             readField(FIELDS, payload, VALUE));
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }

    public int getNumExecutorPerfs() {
        return value.executorPerfs.size();
    }

    public int getNumHandlerPerfs() {
        return value.executorPerfs.stream()
            .mapToInt((e) -> e.getHandlerPerfs().size()).sum();
    }

    /**
     * The perf value excluding interval start and end.
     */
    public static class PerfValue extends ObjectNodeSerializable
    {
        /** The default. */
        public static final PerfValue VALUE_DEFAULT =
            new PerfValue(LongValueStats.DEFAULT,
                          IntegerField.DEFAULT,
                          FilteredListField.getDefault());
        /** The field map. */
        public static final Map<String, Field<?, PerfValue>>
            VALUE_FIELDS = new HashMap<>();
        /** The perf metrics of the number of alive executors. */
        public static final String NUM_ALIVE_EXECUTORS =
            LongValueStatsField.create(
                VALUE_FIELDS, "numAliveExecutors",
                (p) -> p.numAliveExecutors);
        /** The number of the unresponsive executors. */
        public static final String NUM_UNRESPONSIVE_EXECUTORS =
            IntegerField.create(
                VALUE_FIELDS, "numUnresponsiveExecutors",
                (p) -> p.numUnresponsiveExecutors);
        /* The executor perf metrics. */
        public static final String EXECUTOR_PERFS =
            FilteredListField.create(
                VALUE_FIELDS, "executorPerfs",
                (p) -> p.executorPerfs,
                NioChannelExecutorPerf::new);


        private final LongValueStats numAliveExecutors;
        private final int numUnresponsiveExecutors;
        private final List<NioChannelExecutorPerf> executorPerfs;

        private PerfValue(
            LongValueStats numAliveExecutors,
            int numUnresponsiveExecutors,
            List<NioChannelExecutorPerf> executorPerfs)
        {
            this.numAliveExecutors = numAliveExecutors;
            this.numUnresponsiveExecutors = numUnresponsiveExecutors;
            this.executorPerfs = executorPerfs;
        }

        private PerfValue(JsonNode payload) {
            this(readField(VALUE_FIELDS, payload, NUM_ALIVE_EXECUTORS),
                 readField(VALUE_FIELDS, payload, NUM_UNRESPONSIVE_EXECUTORS),
                 readField(VALUE_FIELDS, payload, EXECUTOR_PERFS));
        }

        @Override
        public ObjectNode toJson() {
            return writeFields(VALUE_FIELDS, this);
        }
    }

    /* Methods to return other metrics objects. */

    /**
     * Returns the summary of the executors and their associated handlers.
     */
    public NioChannelExecutorSummarizedPerf
        getNioChannelExecutorSummarizedPerf()
    {
        return new NioChannelExecutorSummarizedPerf(value.executorPerfs);
    }

    /**
     * Returns the ConciseStats to serialize with the stats framework.
     */
    public Stats getConciseStats() {
        return new Stats(this);
    }

    public static class Stats implements ConciseStats, Serializable {

        private static final long serialVersionUID = 1L;

        private transient NioChannelThreadPoolPerf perf;
        private final String serialized;

        private Stats(NioChannelThreadPoolPerf perf) {
            this.perf = perf;
            this.serialized = perf.toString();
        }

        private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {

            in.defaultReadObject();
            this.perf = new NioChannelThreadPoolPerf(
                JsonUtils.parseJsonObject(serialized));
        }

        @Override
        public long getStart() {
            return perf.intervalStart;
        }

        @Override
        public long getEnd() {
            return perf.intervalEnd;
        }

        @Override
        public String getFormattedStats() {
            return perf.value.toJson().toString();
        }

        public NioChannelThreadPoolPerf getPerf() {
            return perf;
        }
    }

}

