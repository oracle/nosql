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

package oracle.kv.impl.api.parallelscan;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.ParallelScanIterator;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.ops.MultiKeyIterate;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKey;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.ops.StoreIterate;
import oracle.kv.impl.api.ops.StoreKeysIterate;
import oracle.kv.query.ExecuteOptions;

/**
 * Implementation of a scatter-gather storeIterator or storeKeysIterator. The
 * iterator will access the store by partitions.
 * {@code PartitionStream} will use to read a single partition.
 */
public class ParallelScan {

    /* Prevent construction */
    private ParallelScan() {}

    /*
     * The entrypoint to ParallelScan from KVStoreImpl.storeKeysIterate.
     */
    public static ParallelScanIterator<Key>
        createParallelKeyScan(final KVStoreImpl storeImpl,
                              final Direction direction,
                              final int batchSize,
                              final Key parentKey,
                              final KeyRange subRange,
                              final Depth depth,
                              final Consistency consistency,
                              final long timeout,
                              final TimeUnit timeoutUnit,
                              final StoreIteratorConfig sic,
                              final boolean excludeTombstones)
        throws FaultException {

        if (direction == null) {
            throw new IllegalArgumentException("direction must not be null");
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final byte[] parentKeyBytes =
            (parentKey != null) ?
                    storeImpl.getKeySerializer().toByteArray(parentKey) : null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange = storeImpl.getKeySerializer().restrictRange
            (parentKey, subRange);

        final StoreIteratorParams parallelKeyScanSIP =
            new StoreIteratorParams(direction,
                                    batchSize,
                                    parentKeyBytes,
                                    useRange,
                                    depth,
                                    consistency,
                                    timeout,
                                    timeoutUnit,
                                    excludeTombstones);

        ExecuteOptions options = new ExecuteOptions();
        options.setTimeout(timeout, timeoutUnit);
        options.setMaxConcurrentRequests(sic.getMaxConcurrentRequests());

        return new PartitionScanIterator<Key>(storeImpl,
                                              options,
                                              parallelKeyScanSIP) {
            @Override
            protected MultiKeyIterate generateGetterOp(byte[] resumeKey) {
                return new StoreKeysIterate
                    (storeIteratorParams.getParentKeyBytes(),
                     storeIteratorParams.getSubRange(),
                     storeIteratorParams.getDepth(),
                     storeIteratorParams.getPartitionDirection(),
                     storeIteratorParams.getBatchSize(),
                     resumeKey,
                     storeIteratorParams.getExcludeTombstones());
            }

            @Override
            protected void convertResult(Result result, List<Key> elementList) {

                final List<ResultKey> byteKeyResults = result.getKeyList();

                int cnt = byteKeyResults.size();
                if (cnt == 0) {
                    assert (!result.hasMoreElements());
                    return;
                }
                for (int i = 0; i < cnt; i += 1) {
                    final byte[] entry = byteKeyResults.get(i).getKeyBytes();
                    elementList.add(storeImpl.getKeySerializer().
                                    fromByteArray(entry));
                }
            }

            @Override
            protected int compare(Key one, Key two) {
                return one.compareTo(two);
            }
        };
    }

    /*
     * The entrypoint to ParallelScan from KVStoreImpl.storeIterate. The
     * iterator returned via with method will iterate over all of the partitions
     * in the store.
     */
    public static ParallelScanIterator<KeyValueVersion>
        createParallelScan(final KVStoreImpl storeImpl,
                           final Direction direction,
                           final int batchSize,
                           final Key parentKey,
                           final KeyRange subRange,
                           final Depth depth,
                           final Consistency consistency,
                           final long timeout,
                           final TimeUnit timeoutUnit,
                           final StoreIteratorConfig sic,
                           final boolean excludeTombstones) {
        return createParallelScan(storeImpl,
                                  direction,
                                  batchSize,
                                  parentKey,
                                  subRange,
                                  depth,
                                  consistency,
                                  timeout,
                                  timeoutUnit,
                                  sic,
                                  null,
                                  excludeTombstones);
    }

    /*
     * The entrypoint to ParallelScan from KVStoreImpl.storeIterate. The
     * iterator returned via with method will iterate over just the partitions
     * in the specified set of partitions.
     */
    public static ParallelScanIterator<KeyValueVersion>
        createParallelScan(final KVStoreImpl storeImpl,
                           final Direction direction,
                           final int batchSize,
                           final Key parentKey,
                           final KeyRange subRange,
                           final Depth depth,
                           final Consistency consistency,
                           final long timeout,
                           final TimeUnit timeoutUnit,
                           final StoreIteratorConfig sic,
                           final Set<Integer> partitions,
                           final boolean excludeTombstones)
        throws FaultException {

        if (direction == null) {
            throw new IllegalArgumentException("direction must not be null");
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final byte[] parentKeyBytes =
            (parentKey != null) ?
            storeImpl.getKeySerializer().toByteArray(parentKey) :
            null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange = storeImpl.getKeySerializer().restrictRange
            (parentKey, subRange);

        final StoreIteratorParams parallelScanSIP =
            new StoreIteratorParams(direction,
                                    batchSize,
                                    parentKeyBytes,
                                    useRange,
                                    depth,
                                    consistency,
                                    timeout,
                                    timeoutUnit,
                                    partitions,
                                    excludeTombstones);

        ExecuteOptions options = new ExecuteOptions();
        options.setTimeout(timeout, timeoutUnit);
        options.setMaxConcurrentRequests(sic.getMaxConcurrentRequests());

        return new PartitionScanIterator<KeyValueVersion>(
            storeImpl, options, parallelScanSIP) {

            @Override
            protected MultiKeyIterate generateGetterOp(byte[] resumeKey) {
                return new StoreIterate(storeIteratorParams.getParentKeyBytes(),
                                        storeIteratorParams.getSubRange(),
                                        storeIteratorParams.getDepth(),
                                    storeIteratorParams.getPartitionDirection(),
                                        storeIteratorParams.getBatchSize(),
                                        resumeKey,
                                        storeIteratorParams.
                                            getExcludeTombstones());
            }

            @Override
            protected void convertResult(Result result,
                                         List<KeyValueVersion> elementList) {

                final List<ResultKeyValueVersion> byteKeyResults =
                    result.getKeyValueVersionList();

                int cnt = byteKeyResults.size();
                if (cnt == 0) {
                    assert (!result.hasMoreElements());
                    return;
                }
                for (int i = 0; i < cnt; i += 1) {
                    final ResultKeyValueVersion entry = byteKeyResults.get(i);
                    KeySerializer keySerializer = storeImpl.getKeySerializer();
                    elementList.add(KVStoreImpl.createKeyValueVersion(
                        keySerializer.fromByteArray(entry.getKeyBytes()),
                        entry.getValue(),
                        entry.getVersion(),
                        entry.getExpirationTime(),
                        entry.getCreationTime(),
                        entry.getModificationTime()));
                }
            }

            @Override
            protected int compare(KeyValueVersion one, KeyValueVersion two) {
                return one.getKey().compareTo(two.getKey());
            }
        };
    }
}
