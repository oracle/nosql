/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.utilint;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Predicate;

import com.sleepycat.je.txn.Txn;

/**
 * SimpleTxnMap provides a customized (but limited functionality) map that's
 * well suited to the tracking of open transactions. Transactions are entered
 * into this map when they are first created, referenced while they are alive
 * via their transaction id and subsequently, removed upon commit or abort. So
 * the map access pattern for each transaction looks like the sequence:
 *
 * put [get]* remove
 *
 * For JE applications, like KVS, transactions can pass through this map at the
 * rate of 30K to 60K transactions/sec, so the map needs to process these
 * operations efficiently.
 *
 * This map tries to be efficient for the put, get, remove operations by:
 *
 * 1) Avoiding any memory allocation for the typical: put, get, remove
 * sequence. In contrast, a heap entry uses 24 bytes for each entry plus 16
 * bytes for the long object argument when using compressed oops. It could be
 * that the heap storage could be replaced by stack storage for the long object
 * argument since it's a downward lexical funarg, but I don't know if the jvm
 * does such analysis.
 *
 * 2) Having a very short instruction code path for the typical case.
 *
 * The data structure used here is very simple, and consists of two maps.
 *
 * 1) An array based map indexed by the low bits of the transaction id.
 *
 * 2) A regular java Map
 *
 * The array based map is the preferred location for map entries. If the slot
 * associated with the transaction id is occupied, we fall back to the the java
 * Map.
 *
 * So the best case behavior is as if the map were implemented entirely as an
 * array and the worst case is that we will do an extra integer mask, array
 * index and compare operation before we resort to using the java Map. Given
 * the behavior of transactions, we expect that the vast majority of the
 * operations will be implemented by the array map.
 *
 * This class provides a minimal subset of the operations provided by Map. All
 * methods are synchronized. This works well for replica replay in conjunction
 * with a jvm's thread biased locking strategy, but we may need explicit locks
 * for other usage.
 *
 * @param <T> the type of Txn object stored as values in the map
 */
public class SimpleTxnMap<T extends Txn>  {

    /* The low order bit mask used to mask the transaction Id */
    private final int cacheMask;

    /*
     * The preferred array map.
     *
     * Invariant: The txn with a given id can be in exactly one of the maps,
     * never in both.
     *
     */
    private final Txn arrayMap[];

    /* The number of entries in just the array map. */
    private int arrayMapSize = 0;

    /* The backup map. */
    private final HashMap<Long,T> backupMap = new HashMap<>();

    public SimpleTxnMap(int arrayMapSize) {
        if (Integer.bitCount(arrayMapSize) != 1) {
            throw new IllegalArgumentException("argument:" + arrayMapSize +
                                               " must be a power of two");
        }
        arrayMap = new Txn[arrayMapSize];
        cacheMask = arrayMapSize - 1;
    }

    /**
     * Adds or updates a txn in the map. We do not normally expect to be
     * called with a txn that is already in the map, but it occasionally
     * happens, for example, during mastership changes.
     */
    public synchronized void put(T txn) {
        final long txnId = txn.getId();
        int i = (int)txn.getId() & cacheMask;
        final Txn cachedTxn = arrayMap[i];
        if (cachedTxn == null) {
            /* Free slot use it. */
            arrayMap[i] = txn;
            arrayMapSize++;
            return;
        }

        if (cachedTxn.getId() == txn.getId()) {
            /* Update an existing txn. */
            arrayMap[i] = txn;
            return;
        }

        /* Array slot occupied by a transaction, fall back to the map. */
        backupMap.put(txnId, txn);
    }

    /**
     * Interface used to create Txns when required by a method.
     *
     * @param <T> the type of Txn being created
     */
    public interface  TxnFactory<T extends Txn> {
        T create(long txnId);
    };

    /**
     * Like put() above, except that the put() is done only if the txnId does
     * not already exist in the map, using the factory method to create the new
     * Txn.
     *
     * The implicit get/put operations are atomic, so no additional
     * synchronization is necessary.
     *
     * @param txnId the txnId used as the key
     * @param factory the txn factory used to create the txn if the txnId was
     * missing from the map
     *
     * @return the the existing or newly generated txn; the return value is
     * never null.
     */
    @SuppressWarnings("unchecked")
    public final synchronized T computeIfAbsent(long txnId,
                                                TxnFactory<T> factory) {
        final int index = (int)txnId & cacheMask;
        T cachedTxn = (T)arrayMap[index];
        if ((cachedTxn != null) && (cachedTxn.getId() == txnId)) {
            /* Assert instead of ISE, since it's a high overhead check. */
            assert ! backupMap.containsKey(txnId);
            return cachedTxn;
        }
        if (cachedTxn != null) {
            /* Already a txn with a different txnid in the slot.
             * Must go into backup, if it's not already there. */
            return backupMap.
                computeIfAbsent(txnId, (Long tid) -> factory.create(tid));
        }
        /* Could be in backup. */
        cachedTxn = backupMap.get(txnId);
        if (cachedTxn != null) {
            return cachedTxn;
        }

        /* Not in local cache or backup */
        assert arrayMap[index] == null;
        arrayMapSize++;
        return (T) (arrayMap[index] = factory.create(txnId)) ;
    }

    synchronized public T get(long txnId) {
        @SuppressWarnings("unchecked")
        T cachedTxn = (T)arrayMap[(int)txnId & cacheMask];
        if ((cachedTxn != null) && (cachedTxn.getId() == txnId)) {
            assert ! backupMap.containsKey(txnId);
            return cachedTxn;
        }
        return backupMap.get(txnId);
    }

    /**
     * Removes the txn with that key, if it exists.
     *
     * @return the Txn that was removed, or empty if it did not exist.
     */
    synchronized public T remove(long txnId) {
        final int i = (int)txnId & cacheMask;
        @SuppressWarnings("unchecked")
        T cachedTxn = (T)arrayMap[i];
        if ((cachedTxn != null) && (cachedTxn.getId() == txnId)) {
            arrayMap[i] = null;
            arrayMapSize--;
            assert ! backupMap.containsKey(txnId);
            return cachedTxn;
        }

        /*
         * Array slot empty, or occupied by a different transaction,
         * check backup.
         */
        return backupMap.remove(txnId);
    }

    public synchronized int size() {
        return backupMap.size() + arrayMapSize;
    }

    public synchronized boolean isEmpty() {
        return size() == 0;
    }

    /**
     * The methods below are not used in critical paths and are not optimized.
     * They they are O(n) complexity. We can revisit with change in usage.
     */

    synchronized public void clear() {
        backupMap.clear();
        Arrays.fill(arrayMap, null);
        arrayMapSize = 0;
    }

    /**
     * Returns a new map containing the current snapshot of transactions in
     * this map.
     */
    synchronized public Map<Long, T> getMap() {
        final Map<Long, T> map = new HashMap<>(backupMap);
        for (Object element : arrayMap) {
            @SuppressWarnings("unchecked")
            final T txn = (T)element;
            if (txn != null) {
                T old = map.put(txn.getId(), txn);
                assert old == null;
            }
        }

        return map;
    }

    /**
     * Returns the txns in the Map that match the predicate. Note that the
     * results are approximate. Specifically, they do not ensure memory
     * consistent results. The intent of this method is to minimize contention
     * with ongoing put/remove operations.
     *
     * This entrypoint should only be used in situations where the caller can
     * accommodate such results.
     *
     * @return txns that match the predicate
     */
    public Collection<Txn> filterTxns(Predicate<Txn> predicate) {
        Collection<Txn> filterResults = null;

        /*
         * No synchronization when accessing IVs and array to avoid contention.
         * Array elements may be stale.
         */
        if (arrayMapSize > 0) {
            for (Txn t : arrayMap) {
                if ((t != null) && predicate.test(t)) {
                    if (filterResults == null) {
                        filterResults = new LinkedList<>();
                    }
                    filterResults.add(t);
                }
            }
        }

        /*
         * Must synchronize to avoid accessing the hash table while it's in an
         * inconsistent state.
         */
        synchronized (this) {
            for (T t : backupMap.values()) {
                if (predicate.test(t)) {
                    if (filterResults == null) {
                        filterResults = new LinkedList<>();
                    }
                    filterResults.add(t);
                }
            }
        }
        return (filterResults == null) ?
            Collections.emptyList() : filterResults;
    }


    /**
     * For test use only
     */
    public HashMap<Long, T> getBackupMap() {
        return backupMap;
    }
}
