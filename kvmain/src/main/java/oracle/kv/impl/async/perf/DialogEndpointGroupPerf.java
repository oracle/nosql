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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.measurement.ConciseStats;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;

/**
 * A collection of dialog perf metrics associated with the endpoint group.
 */
public class DialogEndpointGroupPerf extends ObjectNodeSerializable {

    /** The field map. */
    public static final Map<String, Field<?, DialogEndpointGroupPerf>>
        FIELDS = new HashMap<>();
    /** The start timestamp of the perf collection interval. */
    public static final String INTERVAL_START =
        LongField.create(FIELDS, "intervalStart", (p) -> p.intervalStart);
    /** The end timestamp of the perf collection interval. */
    public static final String INTERVAL_END =
        LongField.create(FIELDS, "intervalEnd", (p) -> p.intervalEnd);
    /** The perf metrics of the endpoint group. */
    public static final String VALUE =
        JsonSerializableField.create(
            FIELDS, "value",
            (p) -> p.value,
            (p, d) -> new PerfValue(p),
            PerfValue.VALUE_DEFAULT);

    /** The default. */
    public static final DialogEndpointGroupPerf DEFAULT =
        new DialogEndpointGroupPerf(
            LongField.DEFAULT, LongField.DEFAULT, PerfValue.VALUE_DEFAULT);

    private final long intervalStart;
    private final long intervalEnd;
    private final PerfValue value;

    public DialogEndpointGroupPerf(
        long intervalStart,
        long intervalEnd,
        Map<String, DialogEndpointPerf> creatorPerfs,
        Map<String, DialogEndpointPerf> responderPerfs,
        DialogResourceManagerPerf resourceManagerPerf,
        IOBufferPoolUsagePerf ioBufferPoolUsagePerf)
    {
        this(intervalStart, intervalEnd,
             new PerfValue(creatorPerfs, responderPerfs,
                           resourceManagerPerf, ioBufferPoolUsagePerf));
    }

    public DialogEndpointGroupPerf(
        long intervalStart,
        long intervalEnd,
        PerfValue perf)
    {
        this.intervalStart = intervalStart;
        this.intervalEnd = intervalEnd;
        this.value = perf;
    }

    public DialogEndpointGroupPerf(JsonNode payload) {
        this(readField(FIELDS, payload, INTERVAL_START),
             readField(FIELDS, payload, INTERVAL_END),
             readField(FIELDS, payload, VALUE));
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }

   @Override
   public boolean isDefault() {
       return value.isDefault();
   }

    /**
     * The perf value excluding interval start and end.
     */
    public static class PerfValue extends ObjectNodeSerializable
    {
        /** The default. */
        public static final PerfValue VALUE_DEFAULT =
            new PerfValue(FilteredMapField.getDefault(),
                          FilteredMapField.getDefault(),
                          DialogResourceManagerPerf.DEFAULT,
                          IOBufferPoolUsagePerf.DEFAULT);
        /** The field map. */
        public static final Map<String, Field<?, PerfValue>>
            VALUE_FIELDS = new HashMap<>();
        /** The dialog perf metrics for creator endpoints. */
        public static final String CREATOR_PERFS =
            FilteredMapField.create(
                VALUE_FIELDS, "creatorPerfs", (v) -> v.creatorPerfs,
                DialogEndpointPerf::new);
        /** The dialog perf metrics for responder endpoints. */
        public static final String RESPONDER_PERFS =
            FilteredMapField.create(
                VALUE_FIELDS, "responderPerfs", (v) -> v.responderPerfs,
                DialogEndpointPerf::new);
        /**
         * The dialog resource manager perf metrics, incluing the dialog permit
         * usage metrics.
         */
        public static final String RESOURCE_MANAGER_PERF =
            JsonSerializableField.create(
                VALUE_FIELDS, "resourceManagerPerf",
                (v) -> v.resourceManagerPerf,
                (p, d) -> new DialogResourceManagerPerf(p),
                DialogResourceManagerPerf.DEFAULT);
        /**
         * The io buffer pool usage perf metrics, including the channel input,
         * message and channel output buffer pool usage.
         */
        public static final String IO_BUFFER_POOL_USAGE_PERF =
            JsonSerializableField.create(
                VALUE_FIELDS, "IOBufferPoolUsagePerf",
                (v) -> v.ioBufferPoolUsagePerf,
                (p, d) -> new IOBufferPoolUsagePerf(p),
                IOBufferPoolUsagePerf.DEFAULT);

        private final Map<String, DialogEndpointPerf> creatorPerfs;
        private final Map<String, DialogEndpointPerf> responderPerfs;
        private final DialogResourceManagerPerf resourceManagerPerf;
        private final IOBufferPoolUsagePerf ioBufferPoolUsagePerf;

        private PerfValue(
            Map<String, DialogEndpointPerf> creatorPerfs,
            Map<String, DialogEndpointPerf> responderPerfs,
            DialogResourceManagerPerf resourceManagerPerf,
            IOBufferPoolUsagePerf ioBufferPoolUsagePerf)
        {
            this.creatorPerfs = creatorPerfs;
            this.responderPerfs = responderPerfs;
            this.resourceManagerPerf = resourceManagerPerf;
            this.ioBufferPoolUsagePerf = ioBufferPoolUsagePerf;
        }

        private PerfValue(JsonNode payload) {
            this(readField(VALUE_FIELDS, payload, CREATOR_PERFS),
                 readField(VALUE_FIELDS, payload, RESPONDER_PERFS),
                 readField(VALUE_FIELDS, payload, RESOURCE_MANAGER_PERF),
                 readField(VALUE_FIELDS, payload, IO_BUFFER_POOL_USAGE_PERF));
        }

        @Override
        public ObjectNode toJson() {
            return writeFields(VALUE_FIELDS, this);
        }
    }

    public int getNumEndpointPerfs() {
        return value.creatorPerfs.size() + value.responderPerfs.size();
    }

    /* Methods to return other metrics objects. */

    /**
     * Returns the summarized perf object.
     */
    public DialogEndpointSummarizedPerf getDialogEndpointSummarizedPerf() {
        return new DialogEndpointSummarizedPerf(
            value.creatorPerfs, value.responderPerfs);
    }

    /**
     * Returns the ConciseStats to serialize with the stats framework.
     */
    public Stats getConciseStats() {
        return new Stats(this);
    }

    public static class Stats implements ConciseStats, Serializable {

        private static final long serialVersionUID = 1L;

        private transient DialogEndpointGroupPerf perf;
        private final String serialized;

        private Stats(DialogEndpointGroupPerf perf) {
            this.perf = perf;
            this.serialized = perf.toString();
        }

        private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {

            in.defaultReadObject();
            this.perf = new DialogEndpointGroupPerf(
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

        public DialogEndpointGroupPerf getPerf() {
            return perf;
        }
    }

    /**
     * Returns the EndpointGroupMetrics.
     */
    @SuppressWarnings("deprecation")
    public oracle.kv.stats.EndpointGroupMetrics getEndpointGroupMetrics() {
        return new oracle.kv.stats.EndpointGroupMetrics() {
            @Override
            public List<oracle.kv.stats.EndpointMetrics>
                getEndpointMetricsList()
            {
                final List<oracle.kv.stats.EndpointMetrics> ret =
                    new ArrayList<>();
                value.creatorPerfs.values()
                    .forEach((p) -> ret.add(p.getEndpointMetrics()));
                value.responderPerfs.values()
                    .forEach((p) -> ret.add(p.getEndpointMetrics()));
                return null;
            }
            @Override
            public oracle.kv.stats.EndpointMetrics
                getEndpointMetrics(String address)
            {
                DialogEndpointPerf perf;
                perf = value.creatorPerfs.get(address);
                if (perf != null) {
                    return perf.getEndpointMetrics();
                }
                perf = value.responderPerfs.get(address);
                if (perf != null) {
                    return perf.getEndpointMetrics();
                }
                return null;
            }
            @Override
            public String getFormattedStats() {
                return toJson().toString();
            }
            @Override
            public String getSummarizedStats() {
                return getDialogEndpointSummarizedPerf().toJson().toString();
            }
        };
    }
}
