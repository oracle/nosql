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
 * ViewListeners are informed when new data is available in the view. They are
 * meant to be fairly simple and stateless, support local access, and are often
 * used for testing.
 *
 * See Monitor.java for a discussion of how ViewListeners and {@link Tracker}
 * interact.
 */
public interface ViewListener<T> {

    /**
     * @param newData newly computed results, which haven't been previously
     * announced.
     */
    public void newInfo(ResourceId resourceId, T newData);
}