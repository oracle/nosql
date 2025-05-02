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

package oracle.nosql.common.sklogger;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.jss.JsonSerializable;
import oracle.nosql.common.sklogger.StatsData.Type;

/**
 * A metric, and all of its samples.
 */
public class MetricFamilySamples<R extends JsonSerializable>
    implements JsonSerializable, Serializable {

    private static final long serialVersionUID = 1L;

    private final long reportTimeMs;
    private final String name;
    private final Type type;
    private final List<String> labelNames;
    private final List<Sample<R>> samples;

    public MetricFamilySamples(String name,
                               Type type,
                               List<String> labelNames,
                               List<Sample<R>> samples) {
        this.name = name;
        this.type = type;
        this.labelNames = labelNames;
        this.samples = samples;
        this.reportTimeMs = System.currentTimeMillis();
    }

    public long getReportTimeMs() {
        return reportTimeMs;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public List<String> getLabelNames() {
        return labelNames;
    }

    public List<Sample<R>> getSamples() {
        return samples;
    }

    /**
     * A single Sample.
     */
    public static class Sample<R extends JsonSerializable>
        extends FlatMeasureGroup.WrappedResult<R>
        implements Serializable {

        private static final long serialVersionUID = 1L;

        /* Old constructor and fields, keep for compatibility. */
        public final List<String> labelValues;
        public final R dataValue;
        public Sample(List<String> labelValues, R dataValue) {
            super("", createUnknownLabelNames(labelValues),
                  labelValues.toArray(new String[0]),
                  dataValue);
            this.labelValues = labelValues;
            this.dataValue = dataValue;
        }

        private static List<String> createUnknownLabelNames(
            List<String> labelValues) {
            final String[] labelNames = new String[labelValues.size()];
            Arrays.fill(labelNames, "");
            return Arrays.asList(labelNames);
        }

        /**
         * Constructs from the wrapped result.
         */
        public Sample(FlatMeasureGroup.WrappedResult<R> wrapped) {
            super(wrapped.getName(),
                  wrapped.getLabelNames(),
                  wrapped.getLabel(),
                  wrapped.getResult());
            this.labelValues = Arrays.asList(wrapped.getLabel());
            this.dataValue = wrapped.getResult();
        }
    }

    @Override
    public ArrayNode toJson() {
        return samples.stream().
            collect(JsonUtils::createArrayNode,
                    (a, s) -> a.add(s.toJson()),
                    ArrayNode::addAll);
    }

    @Override
    public boolean isDefault() {
        return samples.isEmpty();
    }
}
