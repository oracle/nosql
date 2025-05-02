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

package oracle.kv.hadoop;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KeyValueVersion;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.parallelscan.ParallelScan;
import oracle.kv.impl.security.util.KVStoreLogin;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @hidden
 */
abstract class KVRecordReaderBase<K, V> extends RecordReader<K, V> {

    protected KVStoreImpl kvstore;
    protected KeyValueVersion current;

    private KVInputSplit kvInputSplit;

    /* List of remaining partitions to read from */
    private List<Set<Integer>> partitionSets;

    /* Initial number of partitions */
    private int startNPartitionSets;

    /* The current iterator */
    private Iterator<KeyValueVersion> iter;

    /**
     * Called once at initialization.
     * @param split the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

        if (kvstore != null) {
            close();
        }

        kvInputSplit = (KVInputSplit)split;
        final String kvStoreName = kvInputSplit.getKVStoreName();
        final String[] kvHelperHosts = kvInputSplit.getKVHelperHosts();
        final String kvStoreSecurityFile =
                         kvInputSplit.getKVStoreSecurityFile();
        final KVStoreConfig storeConfig =
            new KVStoreConfig(kvStoreName, kvHelperHosts);
        storeConfig.setSecurityProperties(
            KVStoreLogin.createSecurityProperties(kvStoreSecurityFile));
        kvstore = (KVStoreImpl)KVStoreFactory.getStore(storeConfig);

        /* For backward compatibility, make the single partition into a list */
        final int singlePartId = kvInputSplit.getKVPart();

        partitionSets = (singlePartId == 0) ?
                kvInputSplit.getPartitionSets() :
                Collections.singletonList(Collections.singleton(singlePartId));
        startNPartitionSets = partitionSets.size();
    }

    /**
     * Read the next key, value pair.
     * @return true if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            if (iter == null) {
                iter = getNextIterator();
            }

            while (iter != null) {
                if (iter.hasNext()) {
                    current = iter.next();
                    return true;
                }
                current = null;
                iter = getNextIterator();
            }
            return false;
        } catch (Exception E) {
            // Do we have to do anything better than just return false?
            System.out.println("KVRecordReaderBase " + this + " caught: " + E);
            E.printStackTrace();
            return false;
        }
    }

    private Iterator<KeyValueVersion> getNextIterator() {
        if (partitionSets.isEmpty()) {
            return null;
        }

        final Set<Integer> partitions = partitionSets.remove(0);
        assert partitions.size() > 0;
        return (partitions.size() == 1) ?
                 kvstore.partitionIterator(kvInputSplit.getDirection(),
                                           kvInputSplit.getBatchSize(),
                                           (Integer) partitions.toArray()[0],
                                           kvInputSplit.getParentKey(),
                                           kvInputSplit.getSubRange(),
                                           kvInputSplit.getDepth(),
                                           kvInputSplit.getConsistency(),
                                           kvInputSplit.getTimeout(),
                                           kvInputSplit.getTimeoutUnit()) :
                 ParallelScan.createParallelScan(kvstore,
                                                 kvInputSplit.getDirection(),
                                                 kvInputSplit.getBatchSize(),
                                                 kvInputSplit.getParentKey(),
                                                 kvInputSplit.getSubRange(),
                                                 kvInputSplit.getDepth(),
                                                 kvInputSplit.getConsistency(),
                                                 kvInputSplit.getTimeout(),
                                                 kvInputSplit.getTimeoutUnit(),
                                                 new StoreIteratorConfig(),
                                                 partitions,
                                                 /* excludeTombstones */
                                                 true);
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data
     * read
     */
    @Override
    public float getProgress() {
        return (partitionSets == null) ? 0 :
                    (float)(startNPartitionSets - partitionSets.size()) /
                                                    (float)startNPartitionSets;
    }

    /**
     * Close the record reader.
     */
    @Override
    public void close() {
        kvstore.close();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + startNPartitionSets + ", " +
               getProgress() + "]";
    }
}
