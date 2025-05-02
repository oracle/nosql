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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Tracks service status to provide a monitoring health indicator. Note that
 * multiple threads may be simultaneously inserting new changes.
 */
public class ServiceStatusTracker
    extends Tracker<ServiceChange> implements ViewListener<ServiceStatusChange> {

    /* Run the pruner modulo this. */
    public static final int PRUNE_FREQUENCY = 40;

    /* Keep status information for each resource. */
    private final Map<ResourceId, ServiceChange> serviceHealth;
    /* Also keep track of the last logged status, to avoid repeated messages. */
    private final Map<ResourceId, ServiceChange> lastLoggedEvent;

    private final Logger logger;
    private int newInfoCounter = 0;

    /* For unit test usage only. */
    public ServiceStatusTracker() {
        this(null);
    }

    public ServiceStatusTracker(AdminServiceParams params) {
        /*
         * Use a concurrent hash map so we can read the service health safely
         * while it's being updated. However, updates need to be synchronized
         * so that an earlier service change from a laggard thread won't wipe
         * out a later change.
         */
        serviceHealth =
            new ConcurrentHashMap<ResourceId, ServiceChange>();
        lastLoggedEvent =
            new ConcurrentHashMap<ResourceId, ServiceChange>();

        if (params == null) {
            /* Should only be for unit test usage */
            logger =
                LoggerUtils.getLogger(this.getClass(), "ServiceStatusTracker");
        } else {
            logger =
                LoggerUtils.getLogger(this.getClass(), params);
        }
    }

    /**
     * Register a new change. Update the health map only if this change is
     * newer than a previous change, and does not obscure past information.
     */
    @Override
    public void newInfo(ResourceId rId, ServiceStatusChange change) {

        ServiceChange event = new ServiceChange(rId, change);
        EventAction result;

        synchronized (this) {
            if (newInfoCounter++ % PRUNE_FREQUENCY == 0) {
                prune();
            }

            ResourceId targetId = event.getTarget();
            ServiceChange old = serviceHealth.get(targetId);
            ServiceChange oldLogged = lastLoggedEvent.get(targetId);

            result = shouldUpdate(old, oldLogged, event);
            if (result.shouldMap) {
                serviceHealth.put(targetId, event);
                long syntheticTimestamp =
                    getSyntheticTimestamp(event.getChangeTime());
                queue.add
                    (new EventHolder<ServiceChange>
                     (syntheticTimestamp, event,
                      true /* Status changes are always recordable. */));
            }

            /* Remember the last logged event */
            if (result.shouldLog) {
                lastLoggedEvent.put(targetId, event);
            }

        }

        if (result.shouldMap) {
            notifyListeners();
        }

        if (result.shouldLog) {
            logger.info("[" + rId + "] " + change);
        }
    }

    /**
     * Determine whether this new event should be logged, and if it should be
     * shown in the health map and queue. The log should display all unique
     * events, even if they come out of order. The health map and queue should
     * consider time, and only display timely, current events.
     */
    private EventAction shouldUpdate(ServiceChange previous,
                                     ServiceChange previousLogged,
                                     ServiceChange newEvent) {

        /* This is the first status for this service, so use the new event. */
        if (previous == null) {
            return EventAction.LOG_AND_MAP;
        }

        /*
         * This "new" event is actually older than the one that is already
         * tracked. Log it, but don't map it.
         */
        if (previous.getChangeTime() > newEvent.getChangeTime()) {
            return EventAction.LOG_DONT_MAP;
        }

        /*
         * This "new" event is an unreachable status, manufactured by
         * the monitor. If the old status was terminal and more conclusive,
         * don't supercede it with an UNREACHABLE status, because that would
         * lose information. But make an exception for the STOPPING state so it
         * doesn't seem that the SN is stuck forever in STOPPING mode.
         */
        if ((newEvent.getStatus() == ServiceStatus.UNREACHABLE) &&
            (previous.getStatus().isTerminalState()) &&
            (previous.getStatus() != ServiceStatus.STOPPING)) {
            if (previousLogged.getStatus() == ServiceStatus.UNREACHABLE) {
                /* We already logged this state; don't report it again. */
                return EventAction.DONT_LOG_OR_MAP;
            }
            return EventAction.LOG_DONT_MAP;
        }

        /*
         * This new event is the same status as the old event, and isn't a
         * real change.
         */
        if (newEvent.getStatus() == previous.getStatus()) {
            return EventAction.DONT_LOG_OR_MAP;
        }

        return EventAction.LOG_AND_MAP;
    }

    /**
     * Get the current status for all resources.
     */
    public Map<ResourceId, ServiceChange> getStatus() {
        return new HashMap<ResourceId, ServiceChange>(serviceHealth);
    }

    /**
     * Get a list of events that have occurred since the given time.
     */
    @Override
    protected synchronized
        RetrievedEvents<ServiceChange> doRetrieveNewEvents(long pointInTime) {

        List<EventHolder<ServiceChange>> values =
        	new ArrayList<EventHolder<ServiceChange>>();

        long syntheticStampOfLastRecord = pointInTime;
        for (EventHolder<ServiceChange> se : queue) {
            if (se.getSyntheticTimestamp() > pointInTime) {
                values.add(se);
                syntheticStampOfLastRecord = se.getSyntheticTimestamp();
            }
        }

        return new RetrievedEvents<ServiceChange>
        	(syntheticStampOfLastRecord, values);
    }

    private static class EventAction {
        final boolean shouldLog;
        final boolean shouldMap;

        static EventAction LOG_AND_MAP = new EventAction(true, true);
        static EventAction LOG_DONT_MAP = new EventAction(true, false);
        static EventAction DONT_LOG_OR_MAP = new EventAction(false, false);

        private EventAction(boolean shouldLog, boolean shouldMap) {
            this.shouldLog = shouldLog;
            this.shouldMap = shouldMap;
        }
    }
}
