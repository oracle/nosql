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
import java.util.List;

import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;

/**
 * Tracks plan state changes.  The UI uses this to dynamically update a listing
 * of plan states.
 */
public class PlanStateChangeTracker
    extends Tracker<PlanStateChange> implements ViewListener<PlanStateChange> {

    public static final int PRUNE_FREQUENCY = 40;

    private int newInfoCounter = 0;

    @Override
    public void newInfo(ResourceId rId, PlanStateChange psc) {
        synchronized (this) {
            if (newInfoCounter++ % PRUNE_FREQUENCY == 0) {
                prune();
            }

            long syntheticTimestamp = getSyntheticTimestamp(psc.getTime());

            queue.add
                (new EventHolder<PlanStateChange>
                 (syntheticTimestamp, psc,
                 false /* Plans states are never recordable. */));
        }
        notifyListeners();
    }

    /**
     * Get a list of events that have occurred since the given time.
     */
    @Override
    protected synchronized
        RetrievedEvents<PlanStateChange> doRetrieveNewEvents(long since) {

        List<EventHolder<PlanStateChange>> values =
            new ArrayList<EventHolder<PlanStateChange>>();

        long syntheticStampOfLastRecord = since;
        for (EventHolder<PlanStateChange> psc : queue) {
            if (psc.getSyntheticTimestamp() > since) {
                values.add(psc);
                syntheticStampOfLastRecord = psc.getSyntheticTimestamp();
            }
        }

        return new RetrievedEvents<PlanStateChange>(syntheticStampOfLastRecord,
                                                    values);
    }
}
