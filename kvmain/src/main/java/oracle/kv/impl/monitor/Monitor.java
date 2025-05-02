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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.PlanProgress;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.measurement.LoggerMessage;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.views.CSVFileView;
import oracle.kv.impl.monitor.views.GeneralInfoView;
import oracle.kv.impl.monitor.views.LogTracker;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.PerfTracker;
import oracle.kv.impl.monitor.views.PerfView;
import oracle.kv.impl.monitor.views.PlanStateChangeTracker;
import oracle.kv.impl.monitor.views.PlanStateChangeView;
import oracle.kv.impl.monitor.views.ServiceStatusTracker;
import oracle.kv.impl.monitor.views.ServiceStatusView;
import oracle.kv.impl.monitor.views.StorewideLoggingView;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The Monitor module lives within the AdminService and is the locus of control
 * for all monitoring services. The general flow is as follows:
 *
 * Data arrives at the monitor in one of two ways:
 * (a) It's collected from remote services by the Collector, which polls
 *     those services. The Collector collects monitoring information from all
 *     monitor agents and stores it in the History. The Collector owns a thread
 *     pool which is responsible for periodic polling.
 * (b) It's sent to the monitor by services local to the AdminService process.
 *
 * The Monitor has a set of views which have subscribed to particular types of
 * data. The View may deposit the data directly into some destination, like a
 * text file, or to a ViewListener, which simply provides a callback for that
 * data. The data may also be sent to a Tracker, which stores the data, and
 * makes it available to a set of remote TrackerListeners. The Tracker ensures
 * that each TrackerListener sees a complete stream of data, although the
 * TrackerListeners may be refreshing data at different points. The Tracker
 * must also bound its storage, and prune data that has already been viewed, or
 * will overflow the Tracker storage.
 *
 *    Collector
 *     |     |
 *     v     v
 *    pull  pull         Modules within AdminService can also generate
 *    from  from                 monitoring info
 *    agent agent                      |
 *         |                           |
 *         v                           v
 *   +-----------------------------------------------------------------------+
 *   |Monitoring data is dispatched to the views that subscribe to that type |
 *   |of data                                                                |
 *   +-----------------------------------------------------------------------+
 *     |                      |               |          |                    |
 *     v                      v               v          v                    v
 *   ViewA                  ViewB            ViewC
 *     |                      |                 |
 *     v                      v
 *  some destination,    a ViewListener   a ViewListener/Tracker
 *  like a log file                        |             |
 *                                         v             v
 *                                  TrackerListener    TrackerListener
 *                                   (may be remote)    (may be remote)
 *
 * ViewListeners and Trackers are rather similar and in fact, currently all
 * classes that implement Tracker also implement ViewListener. Vanilla view
 * listeners that are not trackers are generally used by tests, whereas
 * Trackers are more complex and support access by multiple, remote clients.
 */

public class Monitor implements ParameterListener {
    /* Names of predefined views. */
    public static final String INTERNAL_STATUS_CHANGE_VIEW =
        "Internal_ServiceStateChanges";
    public static final String INTERNAL_STOREWIDE_LOGGING_VIEW =
        "Internal_LoggingMessages";
    public static final String INTERNAL_GENERAL_INFO_VIEW =
        "Internal_GeneralInfo";
    public static final String INTERNAL_STATS_FILE_VIEW =
        "Internal_StatsCSVFileGenerator";

    public static final String GUI_SERVICE_STATUS = "Service Status";
    public static final String PERF_FILE_VIEW = "Performance Stats";
    public static final String PLAN_STATE_VIEW = "Plan State";

    /* The Monitor lives within an AdminInstance. */
    private final Collector collector;

    private final AdminServiceParams params;
    private final AdminId adminId;
    private final MonitorKeeper admin;
    private final Logger logger;
    private final LoginManager loginMgr;

    /*
     * The general view collection, and particular views that we need to
     * keep a reference to for other purposes.
     */
    private final Set<View> views;
    private ServiceStatusView changeView;
    private StorewideLoggingView loggingView;
    private LogTracker logTracker;
    private ServiceStatusTracker serviceStatusTracker;
    private PerfView perfView;
    private PerfTracker perfTracker;
    private PlanStateChangeView planView;
    private PlanStateChangeTracker planStateChangeTracker;

    /* These views are interested in this kind of metric. */
    private final MetricToViewMap metricToViewMap;

    public Monitor(AdminServiceParams params, MonitorKeeper admin,
                   LoginManager loginMgr) {

        this.admin = admin;
        this.params = params;
        this.loginMgr = loginMgr;
        adminId = params.getAdminParams().getAdminId();

        logger = LoggerUtils.getLogger(this.getClass(), params);
        views = Collections.synchronizedSet(new HashSet<View>());
        collector = new Collector(this);
        metricToViewMap = new MetricToViewMap();
        createPredefinedViews();
    }

    /**
     * Register agents for any existing services in the kvstore. The Monitor may
     * be coming up after the kvstore has been created. Ideally this would be
     * called within the Monitor constructor, but is done outside instead
     * because of initialization ordering dependencies within the Admin.
     */
    public void setupExistingAgents(Topology topo) {
        for (StorageNode sn : topo.getStorageNodeMap().getAll()) {
            registerAgent(sn.getHostname(),
                          sn.getRegistryPort(),
                          sn.getStorageNodeId());
        }

        for (RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                StorageNode sn = topo.get(rn.getStorageNodeId());
                registerAgent(sn.getHostname(),
                              sn.getRegistryPort(),
                              rn.getResourceId());
            }
            for (ArbNode an : rg.getArbNodes()) {
                StorageNode sn = topo.get(an.getStorageNodeId());
                registerAgent(sn.getHostname(),
                              sn.getRegistryPort(),
                              an.getResourceId());
            }
        }
    }

    /**
     * The Admin must register KVStore components which support monitoring.
     */
    public void registerAgent(String snHostname,
                              int snRegistryPort,
                              ResourceId agentId) {
        collector.registerAgent(snHostname, snRegistryPort, agentId);
    }

    /**
     * The Admin must unregister KVStore components which have been removed.
     */
    public void unregisterAgent(ResourceId agentId) {
        collector.unregisterAgent(agentId);
    }

    /** Shutdown all background threads. */
    public void shutdown() {
        for (View v : views) {
            v.close();
        }

        collector.shutdown();
        logTracker.shutdown();
        serviceStatusTracker.shutdown();
        perfTracker.shutdown();
        planStateChangeTracker.shutdown();
    }

    private void createPredefinedViews() {
        /*
         * Internal views.
         */

        /* Status changes are needed by the plan executor. */
        this.changeView = new ServiceStatusView(params);
        this.serviceStatusTracker = new ServiceStatusTracker(params);
        changeView.addListener(serviceStatusTracker);
        views.add(changeView);

        /*
         * Accumulate all service logging messages into a single storewide
         * console.
         */
        this.loggingView = new StorewideLoggingView(params);
        this.logTracker = new LogTracker(logger);
        loggingView.addListener(logTracker);
        views.add(loggingView);

        /*
         * Some non-logger message service output is added to the logs.
         */
        views.add(new GeneralInfoView(params));

        /* Create .csv file to hold performance stats. */
        if (params.getAdminParams().createCSV()) {
            views.add(new CSVFileView(params));
        }

        // TODO, use param
        this.perfView = new PerfView(params, admin);
        this.perfTracker = new PerfTracker(params);
        perfView.addListener(perfTracker);
        views.add(perfView);

        /* Plan state changes are shown in the GUI. */
        this.planView = new PlanStateChangeView(params);
        this.planStateChangeTracker = new PlanStateChangeTracker();
        planView.addPlanStateListener(planStateChangeTracker);
        views.add(planView);

        for (View v: views) {
            metricToViewMap.addView(v);
        }
    }

    /** For unit test support. */
    void setLoggerHook(TestHook<LoggerMessage> hook) {
        loggingView.setTestHook(hook);
    }

    /**
     * Add a view to the Monitor.
     */
    public void addView(View v) {
        logger.finest("Adding view " + v.getName());
        metricToViewMap.addView(v);
    }

    /** For unit test support */
    public void trackStatusChange(ViewListener<ServiceStatusChange> l) {
        changeView.addListener(l);
    }

    /** For unit test support */
    public void trackPerfChange(ViewListener<PerfEvent> p) {
        perfView.addListener(p);
    }

    public void trackPlanStateChange(ViewListener<PlanStateChange> p) {
        planView.addPlanStateListener(p);
    }

    public void trackPlanProgress(ViewListener<PlanProgress> p) {
        planView.addPlanProgressListener(p);
    }

    public ServiceStatusTracker getServiceChangeTracker() {
        return serviceStatusTracker;
    }

    public PerfTracker getPerfTracker() {
        return perfTracker;
    }

    public LogTracker getLogTracker() {
        return logTracker;
    }

    public PlanStateChangeTracker getPlanStateChangeTracker() {
        return planStateChangeTracker;
    }

    /**
     * Publish monitoring information generated on the Admin process. In all
     * other processes, monitoring information is collected through a
     * MonitorAgent.
     */
    public void publish(Measurement p) {
        metricToViewMap.submit(adminId, p);
    }

    public void publish(ResourceId resourceId,
                        List<Measurement> measurements) {
        if (measurements.size() == 0) {
            return;
        }
        for (Measurement m: measurements) {
            metricToViewMap.submit(resourceId, m);
        }
    }

    /**
     * Have the monitor fetch monitoring information from all agents now.
     * For unit testing, to support debugging, and eventually to support a GUI
     * based get-info-now button.
     */
    public void collectNow() {
        collector.collectNow();
    }

    public void collectNow(ResourceId resource) {
        collector.collectNow(resource);
    }

    public AdminServiceParams getParams() {
        return params;
    }

    /**
     * Associate metric types with views that are registered to receive them.
     */
    static class MetricToViewMap {

        private final ConcurrentMap<Integer, Set<View>> metricMap;

        /**
         * Currently, views are predefined and are registered at Monitor
         * startup.
         */
        MetricToViewMap() {
            metricMap = new ConcurrentHashMap<Integer, Set<View>>();
        }

        private void addView(View v) {
            for (MeasurementType  metricType : v.getTargetMetricTypes()) {
                Set<View> existing =  metricMap.get(metricType.getId());
                if (existing == null) {
                    existing = new HashSet<View>();
                    metricMap.put(metricType.getId(), existing);
                }
                existing.add(v);
            }
        }

        public void removeView(View v) {
            for (Set<View> viewset : metricMap.values()) {
                final Iterator<View> iter = viewset.iterator();
                while (iter.hasNext()) {
                    final View view = iter.next();
                    if (view.getClass() == v.getClass()) {
                        iter.remove();
                    }
                }
            }
        }

        void submit(ResourceId resourceId, Measurement m) {
            Set<View> views = metricMap.get(m.getId());
            if (views != null) {
                for (View view : views) {
                    view.applyNewInfo(resourceId, m);
                }
            }
        }
    }

    /**
     * Add or remove CSV View
     */
    private void changeCSVView(boolean createCSV) {
        if (createCSV) {
            /* adding */
            logger.info("Adding CSV view");
            View view = new CSVFileView(params);
            views.add(view);
            addView(view);
        } else {
            /* removing */
            logger.info("Removing CSV view");
            final Iterator<View> iter = views.iterator();
            while (iter.hasNext()) {
                final View view = iter.next();
                if (view instanceof CSVFileView) {
                    metricToViewMap.removeView(view);
                    view.close();
                    iter.remove();
                }
            }
        }
    }

    private void changePollPeriod(long pollMillis) {
        collector.resetAgents(pollMillis);
    }

    /**
     * Replace the existing storewide log with new parameters.
     */
    private void changeStorewideView() {
        loggingView.close();
        views.remove(loggingView);
        loggingView = new StorewideLoggingView(params);
        loggingView.addListener(logTracker);
        views.add(loggingView);
    }

    /**
     * New parameters are available.  These are the items affected in the
     * monitor and related classes:
     *
     *  If createCSV has changed, add or remove view.
     *
     *  If monitor poll period has changed, need to re-create agents in
     *  Collector with the new timing.
     *
     *  Change AdminParams in params so they'll be picked up by PerfView for
     *  changes in latency ceiling/floor values.
     */
    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {

        logger.info("newParameters called for Monitor");
        /* Use wrapper class for accessors */
        AdminParams oldParams = new AdminParams(oldMap);
        params.setAdminParams(new AdminParams(newMap));
        AdminParams newParams = params.getAdminParams();

        if (oldParams.createCSV() != newParams.createCSV()) {
            changeCSVView(newParams.createCSV());
        }
        if (oldParams.getPollPeriodMillis() !=
            newParams.getPollPeriodMillis()) {
            changePollPeriod(newParams.getPollPeriodMillis());
        }

        if ((oldParams.getLogFileCount() != newParams.getLogFileCount()) ||
            (oldParams.getLogFileLimit() != newParams.getLogFileLimit())) {
            changeStorewideView();
        }
    }

    /* For use in error and usage messages. */
    public String getStorewideLogName() {
        return loggingView.getStorewideLogName();
    }

    LoginManager getLoginManager() {
        return loginMgr;
    }

    /**
     * For unit test support.
     */
    int getNumCollectorAgents() {
        return collector.getNumAgents();
    }
}
