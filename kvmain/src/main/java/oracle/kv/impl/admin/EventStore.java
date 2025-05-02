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

package oracle.kv.impl.admin;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminDatabase.DB_TYPE;
import oracle.kv.impl.admin.AdminStores.AdminStore;
import oracle.kv.impl.admin.AdminStores.AdminStoreCursor;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.criticalevent.CriticalEvent.EventKey;
import oracle.kv.impl.admin.criticalevent.CriticalEvent.EventType;
import oracle.kv.impl.util.SerializationUtil;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/*
 * Store for critical events.
 */
public class EventStore extends AdminStore {

    public static EventStore getReadOnlyInstance(Logger logger,
                                                 Environment env) {
        return new EventStore(logger, env, true, Long.MAX_VALUE);
    }

    private final AdminDatabase<EventKey, CriticalEvent> eventDb;
    private long items;
    private long maxEvents = 0;

    EventStore(Logger logger,
               Environment env,
               boolean readOnly,
               long maxEvents) {
        super(logger);
        eventDb = new AdminDatabase<EventKey, CriticalEvent>(DB_TYPE.EVENTS,
                                                             logger, env,
                                                             readOnly) {
            @Override
            protected DatabaseEntry keyToEntry(EventKey key) {
                /*
                 * The key generated is the same as the original DPL
                 * key. This was done to retain the sort order.
                 */
                final DatabaseEntry keyEntry = new DatabaseEntry();
                final TupleOutput to = new TupleOutput();
                to.writeLong(key.getSyntheticTimestamp());
                to.writeByte(0);
                to.writeChars(key.getCategory());
                TupleBinding.outputToEntry(to, keyEntry);
                return keyEntry;
            }};
            this.maxEvents = maxEvents;
            this.items = eventDb.count();
    }

    /**
     * Retrieve a single event from the database, using the given key.
     */
    CriticalEvent getEvent(Transaction txn, String eventId) {
        return getEvent(txn, EventKey.fromString(eventId));
    }

    /**
     * Retrieve a list of events matching the given criteria.
     */
    List<CriticalEvent> getEvents(Transaction txn,
                                  long startTime, long endTime,
                                  EventType type) {
        final EventKey startKey = new EventKey(startTime, type);
        final EventKey endKey =
                    new EventKey(endTime == 0 ? Long.MAX_VALUE : endTime, type);
        return getEvents(txn, startKey, endKey, type);
    }

    public EventCursor getEventCursor() {
        return new EventCursor(eventDb.openCursor(null)) {

            @Override
            protected CriticalEvent entryToObject(DatabaseEntry key,
                                                  DatabaseEntry value) {
                return SerializationUtil.getObject(value.getData(),
                                                   CriticalEvent.class);
            }
        };
    }

    CriticalEvent getEvent(Transaction txn, EventKey key) {
        return eventDb.get(txn, key,
                           LockMode.DEFAULT, CriticalEvent.class);
    }

    List<CriticalEvent> getEvents(Transaction txn,
                                  EventKey startKey,
                                  EventKey endKey,
                                  EventType type) {
        final List<CriticalEvent> events = new ArrayList<>();
        final DatabaseEntry keyEntry = eventDb.keyToEntry(startKey);
        final DatabaseEntry endEntry = eventDb.keyToEntry(endKey);
        final DatabaseEntry valueEntry = new DatabaseEntry();

        try (final Cursor cursor = eventDb.openCursor(txn)) {
            OperationStatus status =
                            cursor.getSearchKeyRange(keyEntry, valueEntry,
                                                     LockMode.DEFAULT);
            while (status == OperationStatus.SUCCESS) {
                if (eventDb.compareKeys(keyEntry, endEntry) > 0) {
                    break;
                }
                final CriticalEvent event =
                           SerializationUtil.getObject(valueEntry.getData(),
                                                       CriticalEvent.class);
                if (type == EventType.ALL ||
                    type.equals(event.getEventType())) {
                    events.add(event);
                }
                status = cursor.getNext(keyEntry, valueEntry,
                                        LockMode.DEFAULT);
            }
        }
        return events;
    }

    void putEvent(Transaction txn, CriticalEvent event) {
        eventDb.put(txn, event.getKey(), event, false);
        items++;
    }

    /**
     * Expire older events from the persistent store.
     */
    void ageStore(Transaction txn, long pruningAge) {

        final long expiry = new Date().getTime() - pruningAge;
        final DatabaseEntry keyEntry = 
            eventDb.keyToEntry(new EventKey(0L, EventType.ALL));
        final DatabaseEntry endEntry = 
            eventDb.keyToEntry(new EventKey(expiry, EventType.ALL));
        final DatabaseEntry valueEntry = new DatabaseEntry();
        try (final Cursor cursor = eventDb.openCursor(txn)) {
            OperationStatus status =
                            cursor.getSearchKeyRange(keyEntry, valueEntry,
                                                     LockMode.RMW);
            while (status == OperationStatus.SUCCESS) {
                if (eventDb.compareKeys(keyEntry, endEntry) > 0) {
                    break;
                }
                cursor.delete();
                items--;
                status = cursor.getNext(keyEntry, valueEntry,
                                        LockMode.RMW);
            }
        }

        if (items > maxEvents && maxEvents > 0) {
            long numToRemove = items - maxEvents;
            long removed = 0;
            try (final Cursor cursor = eventDb.openCursor(txn)) {
                while(cursor.getNext(keyEntry, valueEntry, LockMode.RMW)
                      == OperationStatus.SUCCESS) {
                    cursor.delete();
                    items--;
                    removed++;
                    if (removed >= numToRemove) {
                        break;
                    }

                }

            }
        }
    }

    @Override
    public void close() {
        eventDb.close();
    }

    public void setMaxEvents(long maxEvents) {
        this.maxEvents = maxEvents;
    }

    /**
     * A cursor class to facilitate the scan of the event store.
     */
    public abstract static class EventCursor
                extends AdminStoreCursor<EventKey, CriticalEvent> {

        private EventCursor(Cursor cursor) {
            super(cursor);
        }
     }
}
