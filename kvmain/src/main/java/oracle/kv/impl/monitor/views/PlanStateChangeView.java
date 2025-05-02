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
import oracle.kv.impl.admin.plan.PlanProgress;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.View;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Tracks changes in the service status of KV components.
 */
public class PlanStateChangeView implements View {

    private final Logger storewideLogger;
    private final Set<ViewListener<PlanStateChange>> stateListeners;
    private final Set<ViewListener<PlanProgress>> progressListeners;

    public PlanStateChangeView(AdminServiceParams params) {
        stateListeners = new HashSet<ViewListener<PlanStateChange>>();
        progressListeners = new HashSet<ViewListener<PlanProgress>>();
        storewideLogger =
            LoggerUtils.getStorewideViewLogger(this.getClass(), params);
    }

    @Override
    public String getName() {
        return Monitor.PLAN_STATE_VIEW;
    }

    @Override
    public Set<MeasurementType> getTargetMetricTypes() {
        return Collections.singleton(Metrics.PLAN_STATE);
    }

    @Override
    public synchronized void applyNewInfo(ResourceId resourceId,
                                          Measurement m) {

        /* Display the state change in the storewide log. */
        storewideLogger.info("[" + resourceId + "] " + m);

        if (m instanceof PlanStateChange) {
            PlanStateChange change = (PlanStateChange) m;
            /* Distribute the state change to listeners. */
            for (ViewListener<PlanStateChange> listener : stateListeners) {
                listener.newInfo(resourceId, change);
            }
        }
        if (m instanceof PlanProgress) {
            PlanProgress progress = (PlanProgress) m;
            /* Distribute the progress to listeners. */
            for (ViewListener<PlanProgress> listener : progressListeners) {
                listener.newInfo(resourceId, progress);
            }
        }
    }

    public synchronized
        void addPlanStateListener(ViewListener<PlanStateChange> l) {
        stateListeners.add(l);
    }

    public synchronized
        void addPlanProgressListener(ViewListener<PlanProgress> l) {
        progressListeners.add(l);
    }

    @Override
    public void close() {
        /* Nothing to do. */
    }
}
