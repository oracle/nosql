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
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.View;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * This MonitorView accepts miscellaneous information from all services and
 * funnels it to a single Logger which saves the information in the
 * kvstore-wide logging file.
 */
public class GeneralInfoView implements View {

    private final Logger logger;

    public GeneralInfoView(AdminServiceParams params) {
        logger = LoggerUtils.getStorewideViewLogger(this.getClass(), params);
    }

    @Override
    public String getName() {
        return Monitor.INTERNAL_GENERAL_INFO_VIEW;
    }

    @Override
    public Set<MeasurementType> getTargetMetricTypes() {
        return Collections.singleton(Metrics.PRUNED);
    }

    @Override
    public void applyNewInfo(ResourceId resourceId, Measurement m) {
        logger.info("[" + resourceId + "] " + m);
    }

    @Override
    public void close() {
      /* Nothing to do */
    }
}
