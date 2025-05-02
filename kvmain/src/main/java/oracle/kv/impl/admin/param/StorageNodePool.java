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

package oracle.kv.impl.admin.param;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * A StorageNodePool is just a set of StorageNodes identified by their
 * ResourceIds, which can be considered for deployment by the Planner.
 */
public class StorageNodePool implements Serializable, Iterable<StorageNodeId> {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final Set<StorageNodeId> snIds;
    private transient ReadWriteLock rwl = new ReentrantReadWriteLock();

    public StorageNodePool(String name) {
        this.name = name;
        snIds = new TreeSet<>();
    }

    public String getName() {
        return name;
    }

    public List<StorageNodeId> getList() {
        return new ArrayList<>(snIds);
    }

    public void add(StorageNodeId snId) {
        Lock w = rwl.writeLock();
        try {
            w.lock();
            if (snIds.contains(snId)) {
                /* return silently for idempotency */
                return;
            }
            snIds.add(snId);
        } finally {
            w.unlock();
        }
    }

    public void remove(StorageNodeId snId) {
        Lock w = rwl.writeLock();
        try {
            w.lock();
            if (!snIds.contains(snId)) {
                /* return silently for idempotency */
                return;
            }
            snIds.remove(snId);
        } finally {
            w.unlock();
        }
    }

    public void clear() {
        Lock w = rwl.writeLock();
        try {
            w.lock();
            snIds.clear();
        } finally {
            w.unlock();
        }
    }

    /**
     * Calling freeze() on this StorageNodePool will prevent it from being
     * modified until thaw() is called.
     */
    public void freeze() {
        rwl.readLock().lock();
    }

    public void thaw() {
        rwl.readLock().unlock();
    }

    /**
     * This iterator will loop over and over the storage node list, and will
     * never return null.
     */
    public Iterator<StorageNodeId> getLoopIterator() {
        return new LoopIterator(this);
    }

    public int size() {
        return snIds.size();
    }

    public boolean contains(StorageNodeId snid) {
        return snIds.contains(snid);
    }

    @Override
    public Iterator<StorageNodeId> iterator() {
        return snIds.iterator();
    }

    /**
     * Sets transient fields after being read from the DB.
     */
    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        rwl = new ReentrantReadWriteLock();
    }

    /**
     * This iterator will loop over the storage node list, and will never
     * return null.
     */
    private static class LoopIterator implements Iterator<StorageNodeId> {
        private Iterator<StorageNodeId> listIter ;
        private final StorageNodePool pool;

        private LoopIterator(StorageNodePool pool) {

            if (pool.snIds.isEmpty()) {
                throw new IllegalCommandException
                    ("Storage Node Pool " + pool.name + " is empty.");
            }
            this.pool = pool;
            listIter = pool.snIds.iterator();
        }

        @Override
        public boolean hasNext() {
            /* This loops, so there's always next item. */
            return true;
        }

        @Override
        public StorageNodeId next() {
            if (!listIter.hasNext()) {
                listIter = pool.snIds.iterator();
            }
            return listIter.next();
        }

        @Override
        public void remove() {
            /* We shouldn't be modifying the StorageNodePool. */
            throw new UnsupportedOperationException
                ("Intentionally unsupported");
        }
    }
}
