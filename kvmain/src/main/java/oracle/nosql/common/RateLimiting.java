/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple way used to limit the rate at which events related to a
 * specific object are handled to at most once within the configured time
 * period. The effect of the rate limited event is to sample events associated
 * with the object.
 *
 * @param <T> the type of the object associated with the event.
 */
public class RateLimiting<T> {
    /**
     * Contains the objects that had events last handled for them and the
     * associated time that it was last handled.
     */
    private final Map<T, Long> handledEvents;

    /**
     *  The event sampling period.
     */
    private final int eventSamplePeriodMs;

    /* The number of events that were actually handled. */
    private long limitedMessageCount = 0;

    /**
     * Constructs a configured RateLimiting Instance.
     *
     * @param eventSamplePeriodMs used to compute the max rate of
     *         1 event/eventSamplePeriodMs
     * @param maxObjects the max number of MRU objects to track
     */
    @SuppressWarnings("serial")
    public RateLimiting(final int eventSamplePeriodMs,
                        final int maxObjects) {

        this.eventSamplePeriodMs = eventSamplePeriodMs;

        handledEvents = new LinkedHashMap<T,Long>(9) {
            @Override
            protected boolean
            removeEldestEntry(Map.Entry<T, Long> eldest) {

              return size() > maxObjects;
            }
          };
    }

    /* For testing */
    public long getLimitedMessageCount() {
        return limitedMessageCount;
    }


    /* For testing */
    int getMapSize() {
        return handledEvents.size();
    }

    /**
     * Return true if the object has not already been handled in the current
     * time interval.
     *
     * @param object the object associated with the event
     */
    public boolean isHandleable(T object) {
        if (object == null) {
            return true;
        }

        final long now = System.currentTimeMillis();
        synchronized(handledEvents) {
            final Long timeMs = handledEvents.get(object);
            if ((timeMs == null) ||
                (now > (timeMs + eventSamplePeriodMs))) {
                limitedMessageCount++;
                handledEvents.put(object, now);
                return true;
            }
            return false;
        }
    }
}
