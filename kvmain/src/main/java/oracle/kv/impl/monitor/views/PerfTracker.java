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
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 *
 */
public class PerfTracker extends Tracker<PerfEvent>
	implements ViewListener<PerfEvent> {

    public final static int CHUNK_SIZE = 15;

    /* Keep status information for each resource. */
    private final Map<ResourceId, PerfEvent> resourcePerf;
    private final Logger perfFileLogger;
    private int headerCounter;

    public PerfTracker(AdminServiceParams params) {
        this(LoggerUtils.getPerfFileLogger(PerfTracker.class,
                                           params.getGlobalParams(),
                                           params.getStorageNodeParams()));
    }

    /** Specify the logger explicitly, for testing */
    public PerfTracker(Logger logger) {
        resourcePerf = new ConcurrentHashMap<ResourceId, PerfEvent>();
        perfFileLogger = logger;
        headerCounter = 1;
    }

    @Override
    public void newInfo(ResourceId rId, PerfEvent p) {

        synchronized (this) {
            /* Print a header, and prune the queue, every now and then. */
            if (--headerCounter == 0) {
                headerCounter = CHUNK_SIZE;
                perfFileLogger.info(PerfEvent.HEADER);
                prune();
            }

            /* log into the perf stat file. */
            perfFileLogger.info(p.getColumnFormatted());

            /* Save in a map, for later perusal by the UI. */
            resourcePerf.put(rId, p);
            long syntheticTimestamp = getSyntheticTimestamp(p.getChangeTime());
            queue.add(new EventHolder<PerfEvent>
                      (syntheticTimestamp, p,
                       p.needsAlert())); /* alertable == recordable. */
        }
        notifyListeners();
    }

    /**
     * Get the current performance for all resources, for display.
     */
    public Map<ResourceId, PerfEvent> getPerf() {
        return new HashMap<ResourceId, PerfEvent>(resourcePerf);
    }

    /**
     * Get a list of events that have occurred since the given time.
     */
    @Override
    protected synchronized
        RetrievedEvents<PerfEvent> doRetrieveNewEvents(long pointInTime) {

        List<EventHolder<PerfEvent>> values =
            new ArrayList<EventHolder<PerfEvent>>();

        long syntheticStampOfLastRecord = pointInTime;
        for (EventHolder<PerfEvent> pe : queue) {
            if (pe.getSyntheticTimestamp() > pointInTime) {
                values.add(pe);
                syntheticStampOfLastRecord = pe.getSyntheticTimestamp();
            }
        }

        return
            new RetrievedEvents<PerfEvent>(syntheticStampOfLastRecord, values);
    }
}
