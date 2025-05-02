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

package oracle.kv.impl.measurement;

/**
 * Measurements are statistics, event notifications, and other types of
 * instrumented data. The distinguishing characteristic of Measurements is
 * that they can be can be rolled up, or aggregated.
 *
 * Measurement rollup is used to support a monitoring system that permits
 * filtering and aggregation of instrumented data.
 */
public interface Measurement {
    /* Return the MeasurementType id for this measurement. */
    public int getId();

    /* 
     * The period of time covered by this measurement. Used mainly when
     * measurements have to be discarded, or pruned, so we know what has been
     * lost.
     */
    public long getStart();
    public long getEnd();
}