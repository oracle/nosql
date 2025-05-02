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

import java.io.Serializable;

import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FormatUtils;

/**
 * A notification that a {@link ConfigurableService} has had a service status
 * change.
 */
public class ServiceStatusChange implements Measurement, Serializable {

    private static final long serialVersionUID = 1L;
    protected final ServiceStatus newStatus;
    protected long now;

    public ServiceStatusChange(ServiceStatus newStatus) {
        this(newStatus, System.currentTimeMillis());
    }

    /** Supply time, for testing */
    public ServiceStatusChange(ServiceStatus newStatus, long now) {
        this.newStatus = newStatus;
        this.now = now;
    }

    @Override
    public int getId() {
        return Metrics.SERVICE_STATUS.getId();
    }

    public ServiceStatus getStatus() {
        return newStatus;
    }

    /**
     * @return the timeStamp
     */
    public long getTimeStamp() {
        return now;
    }

    public void updateTime() {
        now = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "Service status: " + newStatus + " " +
            FormatUtils.formatDateTime(now);
    }

    @Override
    public long getStart() {
        return now;
    }

    @Override
    public long getEnd() {
        return now;
    }

    /**
     * Return the resourceId of the service that experienced this status
     * change. In subclasses, this may not be the same as the service that
     * reported the change.
     */
    public ResourceId getTarget(ResourceId reportingResource) {
        return reportingResource;
    }
}
