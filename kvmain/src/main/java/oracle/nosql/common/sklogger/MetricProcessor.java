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

import oracle.nosql.common.jss.JsonSerializable;
import oracle.nosql.common.sklogger.measure.MeasureResultProcessor;

/**
 * Implement this interface to handle metricFamily collected from registered
 * metrics.
 */
public interface MetricProcessor extends MeasureResultProcessor {

    /**
     * Returns the name.
     */
    String getName();

    /**
     * Processes a metric family sample.
     *
     * The method is added for compability reason. Previously the class can
     * only process MetricFamilySamples.
     */
    default void process(MetricFamilySamples<?> result) {
        process((JsonSerializable) result);
    }

    /* Adds a default which process nothing */
    @Override
    default void process(JsonSerializable result) {
        throw new UnsupportedOperationException();
    }
}
