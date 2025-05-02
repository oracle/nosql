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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.View;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Tracks changes in the service status of KV components.
 */
public class ServiceStatusView implements View {

    private final Logger logger;
    private final Set<ViewListener<ServiceStatusChange>> listeners;

    public ServiceStatusView(AdminServiceParams params) {
        logger = LoggerUtils.getLogger(this.getClass(), params);
        this.listeners = new HashSet<ViewListener<ServiceStatusChange>>();
    }

    @Override
    public String getName() {
        return Monitor.INTERNAL_STATUS_CHANGE_VIEW;
    }

    @Override
    public Set<MeasurementType> getTargetMetricTypes() {
        return Collections.singleton(Metrics.SERVICE_STATUS);
    }

    @Override
    public synchronized void applyNewInfo(ResourceId resourceId,
                                          Measurement m) {

        ServiceStatusChange change = (ServiceStatusChange) m;
        for (ViewListener<ServiceStatusChange> listener : listeners) {
            listener.newInfo(resourceId, change);
        }
    }

    public synchronized void addListener(ViewListener<ServiceStatusChange> l) {
        logger.finest(getName() + " added listener " + l);
        listeners.add(l);
    }

    public synchronized
        void removeListener(ViewListener<ServiceStatusChange> l) {
        listeners.remove(l);
    }

    @Override
    public void close() {
        /* Nothing to do. */
    }
}