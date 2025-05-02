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

package oracle.kv.stats;

/**
 * Describes the statistics of a numeric metric.
 *
 * This interface is deprecated. Use Json instead.
 *
 * We are deprecaring this interface corresponding to the effort of using
 * {@link oracle.nosql.common.jss.JsonSerializable} for performance metrics.
 * This effort intends to solve a code maintainance problem that performance
 * metrics data are frequently being added/removed while they also need to
 * maintain backward-compatibility. For example, this class describes a numeric
 * data, but the interface is too restricted and subject to change. We may want
 * to add a median value sometime; we may remove getPercent95 for metrics that
 * does not collect that data; we may add duration and throughput value for
 * throughput-like metrics. Every time we want to make such change it is an API
 * change and breaks backward compatibility.
 *
 * Moving forward, we deprecate this interface and users of this interface
 * should use {@linkplain oracle.nosql.common.json.JsonNode json} or {@code
 * JsonSerializable} classes to expose performance data. For example, a Json
 * object (with fields of "count", "min", "max", etc.) can be used in the place
 * of objects of this class. Alternatively, the {@code LatencyElement.Result}
 * can be used for latency values which provides a {@code toJson} method to
 * serialize to json. As an example, the {@code ServiceAgentMetrics} currently
 * using this interface should itself be changed into a Json object or
 * implement {@code JsonSerializable}.
 *
 * Please see oracle/kv/impl/async/perf/package-info.java for more detail.
 *
 * @hidden Until we make the async metrics public
 * @deprecated since 22.3
 */
@Deprecated
public interface MetricStats {

    /**
     * Returns the count.
     */
    long getCount();

    /**
     * Returns the minimum.
     */
    long getMin();

    /**
     * Returns the maximum.
     */
    long getMax();

    /**
     * Returns the average.
     */
    long getAverage();

    /**
     * Returns the 95th percentile.
     */
    long getPercent95();

    /**
     * Returns the 99th percentile.
     */
    long getPercent99();
}
