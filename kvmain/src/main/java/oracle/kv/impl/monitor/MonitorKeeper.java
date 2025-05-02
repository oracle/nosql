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

import oracle.kv.impl.topo.ResourceId;

/**
 * An interface to specify a class that holds a reference to the Monitor.
 * Ordinarily it is the Admin that keeps this reference, but in testing it is
 * useful to create a proxy for the Admin that doesn't carry all Admin's
 * baggage along with it.
 */

public interface MonitorKeeper {
    /**
     * Get the instance of Monitor associated with the implementing class.
     */
    Monitor getMonitor();

    /**
     * Return the latency ceiling associated with the given RepNode.
     */
    int getLatencyCeiling(ResourceId rnid);

    /**
     * Return the throughput floor associated with the given RepNode.
     */
    int getThroughputFloor(ResourceId rnid);
}

