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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
     * associated time that it was last handled. Indexed by the object from
     * caller, the pair consists of 1) the timestamp of the last time the
     * object is handled and 2) the number of times the object is not handled
     * since the last time it is handled. Since the Pair is not modifiable at
     * the mean time, AtomicLong is used to make it modifiable and avoid create
     * new objects each time it is updated
     */
    private final Map<T, Pair<AtomicLong, AtomicLong>> handledEvents;

    /**
     *  The event sampling period.
     */
    private final int eventSamplePeriodMs;

    /* The number of events that were actually handled. */
    private long limitedMessageCount = 0;

    /*
     * The total number of suppressed events. This counter is not reset when
     * individual suppressed event counters in the map are reset.
     */
    private long numSuppressed = 0;

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

        handledEvents = new LinkedHashMap<T,Pair<AtomicLong, AtomicLong>>(9) {
            @Override
            protected boolean
            removeEldestEntry(Map.Entry<T, Pair<AtomicLong, AtomicLong>>
                                  eldest) {
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
        return getHandleable(object).first();
    }

    /**
     * Return a pair of values indicating the object is handled in the
     * current time interval and the number of times it is not handled since
     * the last time it is handled.
     *
     * @param object the object associated with the event
     * @return a pair of values. The first is true if the object is handled,
     * false otherwise. The second is the number of times the object is not
     * handled since the last time it is handled.
     */
    Pair<Boolean, Long> getHandleable(T object) {
        if (object == null) {
            return new Pair<>(true, 0L);
        }
        final long now = System.currentTimeMillis();
        synchronized (handledEvents) {
            final Pair<AtomicLong, AtomicLong> pair =
                handledEvents.computeIfAbsent(
                    object, u -> new Pair<>(new AtomicLong(0),
                                            new AtomicLong(0)));
            final long timeMs = pair.first().get();
            if ((timeMs == 0L) || (now > (timeMs + eventSamplePeriodMs))) {
                limitedMessageCount++;
                /* reset time stamp and counter */
                pair.first().set(now);
                return new Pair<>(true, pair.second().getAndSet(0L));
            }
            numSuppressed++;
            /* increment the counter */
            return new Pair<>(false, pair.second().incrementAndGet());
        }
    }

    /**
     * Unit test only
     *
     * Returns the number of suppressed objects, or 0 if the object is null or
     * the entry indexed by the object does not exist
     * @param object the object associated with the event
     * @return the number of suppressed objects or 0.
     */
    long getNumSuppressed(T object) {
        if (object == null || !handledEvents.containsKey(object)) {
            return 0;
        }
        return handledEvents.get(object).second().get();
    }
    /**
     * Unit test only
     *
     * Returns the number of suppressed objects without resetting the counters.
     */
    long getNumSuppressed() {
        return numSuppressed;
    }
}
