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

package oracle.kv.impl.monitor;

import java.util.Set;

import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.topo.ResourceId;

/**
 * A view selects specified types of monitoring information.
 */
public interface View {
    
    /* Views have names that can be presented in the UI. */
    public String getName();

    /**
     * Get the set of metric types that are targeted by this view.
     */
    public Set<MeasurementType> getTargetMetricTypes();

    /**
     * Let the view know that there is new information.
     */
    public void applyNewInfo(ResourceId resourceId, Measurement measurement);

    /**
     * Release resources.
     */
    void close();
}
