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

/**
 * StatsData is the root of all monitor stats class.
 */
public interface StatsData {
    // convenience constants
    /**
     * Number of nanoseconds in a second.
     */
    final double NANOSECONDS_PER_SECOND = 1E9;
    /**
     * Number of milliseconds in a second.
     */
    final double MILLISECONDS_PER_SECOND = 1E3;

    /*
     * Stats common properties and field keys that will be used when process
     * and/or log stats.
     */
    // common properties
    final String SHARD = "shard";
    final String RESOURCE = "resource";
    final String COMPONENT_ID = "componentId";
    // RateMetric fields
    final String DURATION_NAME = "duration";
    // Counter fields
    final String COUNT_NAME = "count";
    final String THROUGHPUT_NAME = "throughputPerSecond";
    // Gauge fields
    final String GAUGE_NAME = "value";
    // Quantile fields
    final String QUANTILE_NAME = "quantile";
    // SizeQuantile fields
    final String SUM_NAME = "sum";
    // PerfQuantile fields
    final String OPERATION_COUNT_NAME = "operationCount";
    final String REQUEST_COUNT_NAME = "requestCount";
    final String OVERFLOW_COUNT_NAME = "overflowCount";
    final String OPERATION_MIN_NAME = "operationMin";
    final String OPERATION_MAX_NAME = "operationMax";
    final String OPERATION_AVG_NAME = "operationAvg";
    // Histogram fields
    final String HISTOGRAM_NAME = "histogram";

    final String DELIMITER = "_";


    /**
     * @return The name of the stats.
     */
    String getStatsName();

    /**
     * StatsData types.
     */
    enum Type {
        COUNTER, LONG_GAUGE, CUSTOM_GAUGE, SIZE_QUANTILE, PERF_QUANTILE,
        HISTOGRAM, EVENT, UNTYPED,
    }
}
