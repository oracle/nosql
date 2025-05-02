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

package oracle.kv.impl.monitor.views;

import java.io.Serializable;

import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * Expresses a change in status at a remote service.
 */
public class ServiceChange implements Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * The service which reported the change. This may not be the service
     * that experienced the change.
     */
    private final ResourceId reportingId;

    /* The service which experienced the status change. */
    private final ResourceId originalId;

    /* The timestamp when the change first took effect. */
    private final long changeTime;

    private final ServiceStatus current;

    public ServiceChange(ResourceId reportingId, ServiceStatusChange change) {
        this.reportingId = reportingId;
        this.originalId = change.getTarget(reportingId);
        this.changeTime = change.getTimeStamp();
        this.current = change.getStatus();
    }

    public long getChangeTime() {
        return changeTime;
    }

    public ServiceStatus getStatus() {
        return current;
    }

    /**
     * @return the resource which experienced the change.
     */
    public ResourceId getTarget() {
        return originalId;
    }

    /**
     * @return the resource which reported the status change. This may
     * not be the resource which experienced the change.
     */
    public ResourceId getReporter() {
        return reportingId;
    }

    /**
     * For now, use the status severity. In the future, we could add
     * additional factors.
     */
    public int getSeverity() {
        return current.getSeverity();
    }

    /**
     * For now, use the status's needAlert. In the future, we could add
     * additional factors.
     */
    public boolean isNeedsAlert() {
        return current.needsAlert();
    }

    @Override
    public String toString() {
        return "ServiceChange[" +
            "reportingId=" + reportingId +
            " originalId=" + originalId +
            " changeTime=" + changeTime +
            " current=" + current + "]";
    }
}
