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

package oracle.kv.impl.util;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Tracks service level state changes, so that it can be conveniently
 * monitored and tested.
 */
public class ServiceStatusTracker {

    private ServiceStatusChange current =
        new ServiceStatusChange(ServiceStatus.INITIAL);

    /**
     * The listeners that are invoked on each update.
     */
    private final List<Listener> listeners;

    /**
     * The logger used to log service state changes, or null.
     */
    private Logger logger;

    /**
     * A special listener that detects crashes, or null.
     */
    private ServiceCrashDetector crashDetector;

    /**
     * Create a ServiceStatusTracker when the MonitorAgent repository isn't
     * yet available.
     */
    public ServiceStatusTracker(Logger logger) {
        this.logger = logger;
        listeners = new LinkedList<Listener>();
    }

    /**
     * Create a ServiceStatusTracker and attach a MonitorAgent listener.
     */
    public ServiceStatusTracker
        (Logger logger, AgentRepository agentRepository) {
        this(logger);
        addListener(new StatusMonitor(agentRepository));
    }

    /**
     * Returns the instantaneous service status
     */
    public ServiceStatus getServiceStatus() {
        return current.getStatus();
    }

    /**
     * Reset logger
     */
    public synchronized void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Add a new listener
     */
    public synchronized void addListener(Listener listener) {
        listeners.add(listener);
    }

    /**
     * Add a listener that connects this status tracker to the monitor agent.
     */
    public synchronized void addListener(AgentRepository agentRepository) {
        listeners.add(new StatusMonitor(agentRepository));
    }

    /**
     * Add a new listener and send the current status.
     */
    public synchronized
        void addListenerSendCurrent(Consumer<ServiceStatusChange> updater) {

        ServiceStatusChange s = new ServiceStatusChange(current.getStatus());
        updater.accept(s);
        listeners.add((prevStatus, newStatus) -> updater.accept(newStatus));
    }

    /**
     * Add a service crash detector so that crashes can be reported as part of
     * service status change logging, or null to disable detection.
     */
    public synchronized
        void setCrashDetector(ServiceCrashDetector crashDetector)
    {
        this.crashDetector = crashDetector;
    }

    /**
     * Updates the service status if it has changed. Upon a change it invokes
     * any listeners that may be registered to listen to such changes.
     */
    public synchronized void update(ServiceStatus newStatus) {
        update(newStatus, null);
    }

    /**
     * Updates the service status if it has changed, providing an optional
     * reason. Upon a change it invokes any listeners that may be registered to
     * listen to such changes.
     */
    public synchronized void update(ServiceStatus newStatus, String reason) {
        if (current.getStatus().equals(newStatus)) {
            current.updateTime();
            return;
        }

        final ServiceStatusChange prev = current;
        current = new ServiceStatusChange(newStatus);
        boolean startingAfterCrash = false;
        if (crashDetector != null) {
            crashDetector.update(prev, current);
            if (crashDetector.getStartingAfterCrash()) {
                startingAfterCrash = true;
            }
        }

        if (logger != null) {
            LoggerUtils.logServiceStatusEvent(logger, prev.getStatus(),
                                              newStatus, startingAfterCrash,
                                              reason);
        }

        for (Listener listener : listeners) {
            listener.update(prev, current);
        }

    }

    /**
     * A Listener used for monitoring and test purposes.
     */
    public interface Listener {
        void update(ServiceStatusChange prevStatus,
                    ServiceStatusChange newStatus);
    }

    /**
     * Sends status changes to the monitor agent.
     */
    private class StatusMonitor implements Listener {

        private final AgentRepository monitorAgentBuffer;

        StatusMonitor(AgentRepository monitorAgentBuffer) {
            this.monitorAgentBuffer = monitorAgentBuffer;
        }

        @Override
        public void update(ServiceStatusChange prevStatus,
                           ServiceStatusChange newStatus) {
            monitorAgentBuffer.add(newStatus);
        }
    }
}
