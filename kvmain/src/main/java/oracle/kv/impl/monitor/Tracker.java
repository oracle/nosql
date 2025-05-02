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

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A Tracker stores incoming monitor data and makes it available to callers
 * that request updates. The Tracker is responsible for storing enough of a
 * backlog of data so that callers will see a complete stream of data. It does
 * this by tracking requests and assuming that a request for a later time means
 * the data for an earlier request has been processed and can be removed.
 *
 * Trackers are related to ViewListeners. Classes that extend Tracker generally
 * implement ViewListeners. However, there are ViewListeners that do not extend
 * tracker, because they are meant for simpler, local, single threaded access.
 *
 * To support compatibility with releases earlier than 21.2, the tracker
 * supports notifying listeners of changes. These listeners may be remote to
 * the AdminService/Monitor and will independently request data. To avoid the
 * need to support RMI stub replacement, the notification capability was
 * deprecated in the 21.2 release, and callers should now always poll for
 * updates.
 */
public abstract class Tracker<T> {

    /*
     * Maximum size of event queue.
     */
    public static final int QUEUE_MAX = 5000;

    /**
     * Maximum age in milliseconds of items to keep in the event queue if they
     * have been supplied in response to a retrieveNewEvents call but have not
     * been acknowledged as processed by a subsequent retrieveNewEvents with a
     * higher "since" parameter value.
     */
    public static final long QUEUE_UNACKED_MAX_AGE_MS = 10 * 60 * 1000;

    /*
     * Keep a queue of events for delivery to listeners.
     */
    protected final List<EventHolder<T>> queue;

    /**
     * The latest value of a since parameter specified in a call to
     * retrieveNewEvents, or 0 if the method has not been called.
     */
    private long latestSinceTimestamp;

    private long lastTimestampGiven = 0;
    private final List<TrackerListener> listeners;
    private final NotifierThread notifier;

    protected Tracker() {
        listeners = new ArrayList<TrackerListener>();
        notifier = new NotifierThread();
        notifier.start();
        queue = new ArrayList<EventHolder<T>>();
    }

    protected List<TrackerListener> getListeners() {
        return listeners;
    }

    /*
     * Prune the event queue when there is obsolete information or queue
     * size exceed.
     */
    protected void prune() {

        long interesting = getEarliestInterestingTimeStamp();

        while (!queue.isEmpty()) {
            EventHolder<T> pe = queue.get(0);
            if (pe.getSyntheticTimestamp() > interesting) {
                /* Stop if we've reached the earliest interesting timestamp. */
                break;
            }
            queue.remove(0);
        }

        /*
         * Guarantee that the queue is never larger than a certain max number
         * of entries. If that number is exceeded, then we discard guarantees
         * about keeping messages until all listeners have indicated that they
         * are no longer interesting.
         */
        int currentSize = queue.size();
        if (currentSize > QUEUE_MAX) {
            for (int i = 0; i < (currentSize - QUEUE_MAX); i++) {
                queue.remove(0);
            }
        }
    }

    public synchronized void registerListener(TrackerListener tl) {
        listeners.add(tl);
    }

    public synchronized void removeListener(TrackerListener tl) {
        listeners.remove(tl);
    }

    /** Shutdown threads. */
    public void shutdown() {
        notifier.shutdown();
    }

    /*
     * The caller of notifyListeners might be holding arbitrary
     * synchronization monitors, which might be the same monitors
     * wanted by the listener implementations.  Also, listeners
     * can be Remote interfaces.  For both reasons, we don't want
     * to hold any monitors when invoking the listener's notification
     * method.  To that end, we have this simple thread to invoke
     * the listeners asynchronously.
     */
    private class NotifierThread extends Thread {
        boolean notificationNeeded = false;
        private boolean shutdown;

        public NotifierThread() {
            setDaemon(true); /* Prevent its hanging the process on exit. */
        }

        @Override
        public void run() {
            while (true) {
                synchronized(this) {
                    while (notificationNeeded == false) {
                        if (shutdown) {
                            return;
                        }
                        try {
                            wait();
                        } catch (Exception e) {
                            /* Just try again. */
                        }
                    }
                    notificationNeeded = false;
                }
                /* Call the listeners with no monitors held. */
                asyncNotifyListeners();
            }
        }

        /* Let the thread know there is a notification to be performed. */
        public void kick() {
            synchronized(this) {
                notificationNeeded = true;
                notify();
            }
        }

        synchronized void shutdown() {
            shutdown = true;
            notify();
        }
    }

    protected void notifyListeners() {
        notifier.kick();
    }

    private void asyncNotifyListeners() {

        /*
         * Copy the list because the method notifyOfNewEvents may remove an
         * item from the original list by calling removeListener, resulting in
         * the list's being modified while we're iterating over it.
         */
        List<TrackerListener> myListeners;
        synchronized (this) {
            myListeners = new ArrayList<TrackerListener>(listeners);
        }

        /*
         * Don't hold the monitor when notifying the listeners.
         */
        final Iterator<TrackerListener> iter = myListeners.iterator();
        while (iter.hasNext()) {
            final TrackerListener tl = iter.next();
            try {
                tl.notifyOfNewEvents();
            } catch (RemoteException re) {
                /* There's a problem with this listener; just get rid of it. */
                iter.remove();
            }
        }
    }

    /**
     * Return the earliest timestamp for which all later entries should be
     * retained.
     */
    protected synchronized long getEarliestInterestingTimeStamp() {

        /*
         * Always retain items after lastTimestampGiven, since, except for the
         * short time after start up when retrieveNewEvents hasn't been called,
         * we know they are items that have not been processed yet. Items as
         * old or older than lastTimestampGiven but newer than
         * latestSinceTimestamp are ones that we've returned but may not have
         * been processed yet. Retain them for a while just in case the caller
         * crashes before processing them and needs them again.
         */
        final long unackedTimestamp =
            Math.max(latestSinceTimestamp,
                     getCurrentTimeMillis() - QUEUE_UNACKED_MAX_AGE_MS);
        return Math.min(lastTimestampGiven, unackedTimestamp);
    }

    protected long getSyntheticTimestamp(long naturalTimestamp) {

        long syntheticTimestamp = naturalTimestamp;

        if (lastTimestampGiven < syntheticTimestamp) {
            lastTimestampGiven = syntheticTimestamp;
        } else {
            lastTimestampGiven++;
            syntheticTimestamp = lastTimestampGiven;
        }
        return syntheticTimestamp;
    }

    /**
     * Returns the current time in milliseconds. Can be overridden for testing.
     */
    protected long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Get a list of events that have occurred since the given time, and track
     * the latest requested since timestamp.
     */
    public final RetrievedEvents<T> retrieveNewEvents(long since) {
        final RetrievedEvents<T> events = doRetrieveNewEvents(since);
        if (since > latestSinceTimestamp) {
            latestSinceTimestamp = since;
        }
        return events;
    }

    /** Implement retrieving new events. */
    abstract protected RetrievedEvents<T> doRetrieveNewEvents(long since);

    /**
     * When deciding, for the purpose of retrieveNewEvents, whether a given log
     * record is later or earlier than another, we use the synthetic timestamp
     * in EventHolder, which is guaranteed to be different from any other log
     * record's timestamp.  When creating such a timestamp, we examine the
     * unique timestamp of the most recently created record.  If that value is
     * less than the natural timestamp of the new record, we use the new
     * record's natural timestamp as its unique timestamp.  Otherwise, we
     * increment the most recently given unique timestamp, and use that as a
     * synthetic timestamp for the new record.  This guarantees that no log
     * records will be lost, when the client asks for log records added since
     * the timestamp of the last record previously delivered, even if new
     * records have natural timestamps that are less than or equal to that of
     * the last previously delivered record.  Since the synthetic timestamps
     * are near in value to the natural timestamps, if not identical, it also
     * works to ask for events since a particular natural timestamp, without
     * reference to the last record delivered.
     */
    public static class EventHolder<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private long syntheticTimestamp;
        private T event;
        boolean recordable;

        public EventHolder(long syntheticTimestamp, T event,
                           boolean recordable) {
            this.syntheticTimestamp = syntheticTimestamp;
            this.event = event;
            this.recordable = recordable;
        }

        public long getSyntheticTimestamp() {
            return syntheticTimestamp;
        }

        public T getEvent() {
            return event;
        }

        public boolean isRecordable() {
            return recordable;
        }

        @Override
        public String toString() {
            return "EventHolder[" +
                "syntheticTimestamp=" + syntheticTimestamp +
                " event=" + event +
                " recordable=" + recordable + "]";
        }
    }

    /**
     * We have to communicate the synthetic timestamp of the last retrieved
     * event to the caller, which will (optionally) use this timestamp as the
     * "since" value when requesting the next set of updates.  We use an object
     * of this class to return this value along with the list of LogRecords
     * retrieved.
     */
    public static class RetrievedEvents<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private long syntheticTimestampOfLastEvent;
        private List<EventHolder<T>> events;

        public RetrievedEvents(long syntheticStampOfLastEvent,
                               List<EventHolder<T>> events) {
            this.syntheticTimestampOfLastEvent = syntheticStampOfLastEvent;
            this.events = events;
        }

        public long getLastSyntheticTimestamp() {
            return syntheticTimestampOfLastEvent;
        }

        public int size() {
            return events.size();
        }

        public List<T> getEvents() {
            List<T> values = new ArrayList<T>();
            for (EventHolder<T> se : events) {
                values.add(se.getEvent());
            }
            return values;
        }

        public List<EventHolder<T>> getRecordableEvents() {
            List<EventHolder<T>> values = new ArrayList<EventHolder<T>>();
            for (EventHolder<T> se : events) {
                if (se.isRecordable()) {
                    values.add(se);
                }
            }
            return values;
        }
    }
}
