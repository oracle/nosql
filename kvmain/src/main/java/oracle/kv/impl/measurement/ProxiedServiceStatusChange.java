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

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FormatUtils;

/**
 * Used by a third party service (usually the SNA) to report a status change
 * experienced by another service (usually a RepNode).  This is useful when the
 * RepNode is shutting down or handling errors and may not be up to provide its
 * own service status.
 */
public class ProxiedServiceStatusChange extends ServiceStatusChange {

    private static final long serialVersionUID = 1L;

    /* The resource that experienced the change. */
    private final ResourceId target;

    /**
     * @param target the resource that experienced the change.
     * @param newStatus the new service status.
     */
    public ProxiedServiceStatusChange(ResourceId target,
                                      ServiceStatus newStatus) {
        super(newStatus);
        this.target = target;
    }

    @Override
    public ResourceId getTarget(ResourceId reportingResource) {
        return target;
    }

    @Override
    public String toString() {
        return target + ": Service status: " + newStatus + " " +
            FormatUtils.formatDateTime(now);
    }
}
