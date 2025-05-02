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

package oracle.kv.impl.sna.collector;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.nosql.common.sklogger.SkLogger;

/**
 * SN collector service that will start all agents to collect monitor data.
 * Currently, two agents are registered and started. Agents are the active
 * entities that fetch metric data at prescribed times. The two current agents
 * are the JMX agent, which reads the jmx mbean periodically, and the ping
 * agent, which calls ping on the running service to determine health.
 *
 * Agents funnel collected data and collector recorders, which route and
 * possibly transform the data and send it to a destination. The defacto
 * recorder is the FileRecorder, which logs this information in a format that
 * that can be consumed by monitoring systems. It is also possible to add
 * a custom destination by specifying an optional collector recorder as a
 * dynamically loaded class.
 */
public class CollectorService {

    private final List<CollectorAgent> agents;
    private final CollectorRecorder fileRecorder;
    private final AtomicReference<CollectorRecorder> optionalRecorder;
    private boolean shutdown;
    private final GlobalParamsListener globalParamsListener;
    private final SNParamsListener snParamsListener;
    private final Logger snaLogger;

    /*
     * Keep a reference to the snp and gp because they would be needed as
     * parameters to an optional recorder
     */
    private StorageNodeParams storageNodeParams;
    private GlobalParams globalParams;

    public CollectorService(StorageNodeParams snp,
                            GlobalParams gp,
                            SecurityParams sp,
                            LoginManager loginManager,
                            Logger snaLogger) {
        this.snaLogger = snaLogger;
        this.globalParams = gp;
        this.storageNodeParams = snp;

        /*
         * Create all recorders. These are the modules that take collected
         * info and transform and transport them to a destination.
         * - the fileRecorder is mandatory
         * - an additional recorder may be dynamically loaded and created
         */
        fileRecorder = new FileCollectorRecorder(snp, gp);
        optionalRecorder = new AtomicReference<>();
        setOptionalRecorder(snp, gp);

        /* Register all agents */
        agents = new ArrayList<CollectorAgent>();
        PingCollectorAgent pingAgent =
            new PingCollectorAgent(gp, snp, loginManager, fileRecorder,
                                   optionalRecorder, snaLogger);

        JMXCollectorAgent metricAgent =
             new JMXCollectorAgent(snp, sp, fileRecorder, optionalRecorder,
                                   snaLogger);
        agents.add(pingAgent);
        agents.add(metricAgent);

        if (gp.getCollectorEnabled()) {
            for (CollectorAgent agent : agents) {
                agent.start();
            }
        }
        shutdown = false;
        globalParamsListener = new GlobalParamsListener();
        snParamsListener = new SNParamsListener();
    }

    /**
     * Dynamically load and create the specified collector recorder.
     * Return old CollectorRecorder if a new CollectorRecorder is set.
     */
    private CollectorRecorder setOptionalRecorder(StorageNodeParams snp,
                                                  GlobalParams gp) {

        CollectorRecorder current = optionalRecorder.get();
        String recorderClassName = gp.getCollectorRecorder();
        if (recorderClassName == null) {
            optionalRecorder.set(null);
            return current;
        }


        String classname = recorderClassName.trim();
        if ((classname == null) || (classname.length() == 0)) {
            optionalRecorder.set(null);
            return current;
        }

        /*
         * An optional recorder was specified in the params, but there's already
         * an existing one.
         */
        if (current != null &&
            current.getClass().getName().equals(classname)) {
            return null; /* CollectorRecorder isn't changed. */
        }

        /* An optional recorder was specified, and it's different */
        Class<?> cl;
        try {
            cl = Class.forName(classname);
            Class<? extends CollectorRecorder> recorderClass =
                cl.asSubclass(CollectorRecorder.class);
            Constructor<? extends CollectorRecorder> ctor =
                recorderClass.getDeclaredConstructor
                (StorageNodeParams.class,
                 GlobalParams.class,
                 SkLogger.class);
            SkLogger useLogger = null;
            if (snaLogger != null) {
                useLogger = new SkLogger(snaLogger);
            }
            CollectorRecorder recorder = ctor.newInstance(snp, gp, useLogger);
            optionalRecorder.set(recorder);
            return current;
        } catch (Exception e) {
            snaLogger.log(Level.SEVERE, "Couldn't create " + classname, e);
        }
        return null; /* Failed to set the new CollectorRecorder. */
    }

    public synchronized void updateParams(GlobalParams newGlobalParams,
                                          StorageNodeParams newSNParams) {
        if (shutdown) {
            /* Collector service has been shutdown, don't update parameters. */
            return;
        }
        if (newGlobalParams != null) {
            snaLogger.fine("Collector service: new global params: " +
                newGlobalParams.getMap().toString());
            globalParams = newGlobalParams;
        }
        if (newSNParams != null) {
            snaLogger.fine("Collector service: new SN params: " +
                newSNParams.getMap().toString());
            storageNodeParams = newSNParams;
        }

        /*
         * There are two assumptions to optional Recorder:
         * 1. Recorder record() and close() method should be synchronized
         *    make sure we don't close Recorder that is doing record()
         * 2. No params are expected to change in the optional Recoder as
         *    we aren't calling optionalRecorder.updateParams();
         */

        /* Is there a new optional recorder? */
        final CollectorRecorder oldOptional =
            setOptionalRecorder(storageNodeParams, globalParams);

        for (CollectorAgent agent : agents) {
            agent.updateParams(newGlobalParams, newSNParams);
        }

        /*
         * Close the old optional recorder after all agents updated to new
         * optional recorder.
         */
        if (oldOptional != null) {
            oldOptional.close();
        }
    }

    public synchronized void shutdown() {
        shutdown = true;
        for (CollectorAgent agent : agents) {
            agent.stop();
        }

        /*
         * Shutdown recorders after agents are closed, since agents use
         * recorders
         */
        fileRecorder.close();
        CollectorRecorder recorder = optionalRecorder.get();
        if (recorder != null) {
            recorder.close();
        }
    }

    public ParameterListener getGlobalParamsListener() {
        return globalParamsListener;
    }

    public ParameterListener getSNParamsListener() {
        return snParamsListener;
    }

    /**
     * Global parameter change listener.
     */
    private class GlobalParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            GlobalParams newGlobalParams = new GlobalParams(newMap);
            updateParams(newGlobalParams, null /* newSNParams */);
        }
    }

    /**
     * StorageNode parameter change listener.
     */
    private class SNParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            StorageNodeParams newSNParams = new StorageNodeParams(newMap);
            updateParams(null /* newGlobalParams */, newSNParams);
        }
    }
}
