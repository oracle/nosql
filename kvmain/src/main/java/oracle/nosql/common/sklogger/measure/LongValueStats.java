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
import java.util.function.Function;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;

/**
 * The statistics (count, min, max, average, 95th and 99th) of a long value.
 */
public class LongValueStats extends LongValueStatsBase {

    /** The field map. */
    public static final Map<String, Field<?, LongValueStats>> FIELDS =
        new HashMap<>();
    /** The total count, including the overflow occurrences. */
    public static final String COUNT =
        LongField.create(FIELDS, "count", LongValueStats::getCount);

    /** The default LongValueStats result. */
    public static final LongValueStats DEFAULT =
        new LongValueStats(LongField.DEFAULT,
                           LongField.DEFAULT,
                           LongField.DEFAULT,
                           LongField.DEFAULT,
                           LongField.DEFAULT,
                           LongField.DEFAULT,
                           LongField.DEFAULT);

    private final long count;

    public LongValueStats(long[] histogram,
                          Function<Integer, Long> upperBoundFunction) {
        this(computeStats(histogram, upperBoundFunction));
    }

    private LongValueStats(HistogramSummary histogramSummary) {
        super(histogramSummary);
        this.count = histogramSummary.count;
    }

    public LongValueStats(LongValueStats other) {
        super(other);
        this.count = other.count;
    }

    public LongValueStats(long count,
                          long min,
                          long max,
                          long average,
                          long percent95,
                          long percent99,
                          long overflowCount) {
        super(min, max, average, percent95, percent99, overflowCount);
        this.count = count;
    }

    public LongValueStats(JsonNode payload) {
        super(payload);
        this.count = readField(FIELDS, payload, COUNT);
    }

    public long getCount() {
        return count;
    }

    @Override
    public ObjectNode toJson() {
        final ObjectNode result = super.toJson();
        writeFields(FIELDS, result, this);
        return result;
    }

    @Override
    public boolean isDefault() {
        return super.isDefault() && (count == LongField.DEFAULT);
    }
}
