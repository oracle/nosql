/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import oracle.kv.BulkWriteOptions;
import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.EntryStream;
import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValue;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.Operation;
import oracle.kv.OperationFactory;
import oracle.kv.OperationResult;
import oracle.kv.ParallelScanIterator;
import oracle.kv.ReturnValueVersion;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.query.Statement;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.stats.KVStats;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TableAPI;
import org.reactivestreams.Publisher;

/** A mock implementation of KVStore. */
public class MockStore implements KVStore {

    @Override
    public ValueVersion get(Key key) {
        return null;
    }

    @Override
    public ValueVersion get(Key key,
                            Consistency consistency,
                            long timeout,
                            TimeUnit timeoutUnit) {
        return null;
    }

    @Override
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth) {
        return new TreeMap<Key, ValueVersion>();
    }

    @Override
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth,
                                                 Consistency consistency,
                                                 long timeout,
                                                 TimeUnit timeoutUnit) {
        return new TreeMap<Key, ValueVersion>();
    }

    @Override
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth) {
        return new TreeSet<Key>();
    }

    @Override
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth,
                                       Consistency consistency,
                                       long timeout,
                                       TimeUnit timeoutUnit) {
        return new TreeSet<Key>();
    }

    @Override
    public Iterator<KeyValueVersion> multiGetIterator(Direction direction,
                                                      int batchSize,
                                                      Key parentKey,
                                                      KeyRange subRange,
                                                      Depth depth) {
        return Collections.<KeyValueVersion>emptyList().iterator();
    }

    @Override
    public Iterator<KeyValueVersion> multiGetIterator(Direction direction,
                                                      int batchSize,
                                                      Key parentKey,
                                                      KeyRange subRange,
                                                      Depth depth,
                                                      Consistency consistency,
                                                      long timeout,
                                                      TimeUnit timeoutUnit) {
        return Collections.<KeyValueVersion>emptyList().iterator();
    }

    @Override
    public Iterator<Key> multiGetKeysIterator(Direction direction,
                                              int batchSize,
                                              Key parentKey,
                                              KeyRange subRange,
                                              Depth depth) {
        return Collections.<Key>emptyList().iterator();
    }

    @Override
    public Iterator<Key> multiGetKeysIterator(Direction direction,
                                              int batchSize,
                                              Key parentKey,
                                              KeyRange subRange,
                                              Depth depth,
                                              Consistency consistency,
                                              long timeout,
                                              TimeUnit timeoutUnit) {
        return Collections.<Key>emptyList().iterator();
    }

    @Override
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize) {
        return Collections.<KeyValueVersion>emptyList().iterator();
    }

    @Override
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize,
                                                   Key parentKey,
                                                   KeyRange subRange,
                                                   Depth depth) {
        return Collections.<KeyValueVersion>emptyList().iterator();
    }

    @Override
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize,
                                                   Key parentKey,
                                                   KeyRange subRange,
                                                   Depth depth,
                                                   Consistency consistency,
                                                   long timeout,
                                                   TimeUnit timeoutUnit) {
        return Collections.<KeyValueVersion>emptyList().iterator();
    }

    private class MockStoreParallelScanIterator<K>
        implements ParallelScanIterator<K> {

        private final Iterator<K> iter = Collections.<K>emptyList().iterator();

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public K next() {
            return iter.next();
        }

        @Override
        public void remove() {
            iter.remove();
        }

        @Override
        public void close() {
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return null;
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return null;
        }
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(Direction direction,
                      int batchSize,
                      Key parentKey,
                      KeyRange subRange,
                      Depth depth,
                      Consistency consistency,
                      long timeout,
                      TimeUnit timeoutUnit,
                      StoreIteratorConfig storeIteratorConfig) {
        return new MockStoreParallelScanIterator<KeyValueVersion>();
    }

    @Override
    public Iterator<Key> storeKeysIterator(Direction direction,
                                                       int batchSize) {
        return Collections.<Key>emptyList().iterator();
    }

    @Override
    public Iterator<Key> storeKeysIterator(Direction direction,
                                                       int batchSize,
                                                       Key parentKey,
                                                       KeyRange subRange,
                                                       Depth depth) {
        return Collections.<Key>emptyList().iterator();
    }

    @Override
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize,
                                           Key parentKey,
                                           KeyRange subRange,
                                           Depth depth,
                                           Consistency consistency,
                                           long timeout,
                                           TimeUnit timeoutUnit) {
        return Collections.<Key>emptyList().iterator();
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(Direction direction,
                          int batchSize,
                          Key parentKey,
                          KeyRange subRange,
                          Depth depth,
                          Consistency consistency,
                          long timeout,
                          TimeUnit timeoutUnit,
                          StoreIteratorConfig storeIteratorConfig) {
        return new MockStoreParallelScanIterator<Key>();
    }

    @Override
    public Version put(Key key,
                       Value value) {
        return null;
    }

    @Override
    public Version put(Key key,
                       Value value,
                       ReturnValueVersion prevValue,
                       Durability durability,
                       long timeout,
                       TimeUnit timeoutUnit) {
        return null;
    }

    @Override
    public Version putIfAbsent(Key key,
                               Value value) {
        return null;
    }

    @Override
    public Version putIfAbsent(Key key,
                               Value value,
                               ReturnValueVersion prevValue,
                               Durability durability,
                               long timeout,
                               TimeUnit timeoutUnit) {
        return null;
    }

    @Override
    public Version putIfPresent(Key key,
                                Value value) {
        return null;
    }

    @Override
    public Version putIfPresent(Key key,
                                Value value,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit) {
        return null;
    }

    @Override
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion) {
        return null;
    }

    @Override
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit) {
        return null;
    }

    @Override
    public boolean delete(Key key) { return false; }

    @Override
    public boolean delete(Key key,
                          ReturnValueVersion prevValue,
                          Durability durability,
                          long timeout,
                          TimeUnit timeoutUnit) {
        return false;
    }

    @Override
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion) {
        return false;
    }

    @Override
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion,
                                   ReturnValueVersion prevValue,
                                   Durability durability,
                                   long timeout,
                                   TimeUnit timeoutUnit) {
        return false;
    }

    @Override
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth) {
        return 0;
    }

    @Override
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth,
                           Durability durability,
                           long timeout,
                           TimeUnit timeoutUnit) {
        return 0;
    }

    @Override
    public List<OperationResult> execute(List<Operation> operations) {
        return Collections.emptyList();
    }

    @Override
    public List<OperationResult> execute(List<Operation> operations,
                                         Durability durability,
                                         long timeout,
                                         TimeUnit timeoutUnit) {
        return Collections.emptyList();
    }

    @Override
    public OperationFactory getOperationFactory() {
        return MockOperationFactory.INSTANCE;
    }

    private static class MockOperationFactory implements OperationFactory {
        private static final MockOperationFactory INSTANCE =
            new MockOperationFactory();
        @Override
        public Operation createPut(Key key, Value value) { return null; }
        @Override
        public Operation createPut(Key key,
                                   Value value,
                                   ReturnValueVersion.Choice prevReturn,
                                   boolean abortIfUnsuccessful) {
            return null;
        }
        @Override
        public Operation createPutIfAbsent(Key key, Value value) {
            return null;
        }
        @Override
        public Operation createPutIfAbsent(Key key,
                                           Value value,
                                           ReturnValueVersion.Choice prevReturn,
                                           boolean abortIfUnsuccessful) {
            return null;
        }
        @Override
        public Operation createPutIfPresent(Key key, Value value) {
            return null;
        }
        @Override
        public Operation createPutIfPresent(Key key,
                                            Value value,
                                            ReturnValueVersion.Choice prevReturn,
                                            boolean abortIfUnsuccessful) {
            return null;
        }
        @Override
        public Operation createPutIfVersion(Key key,
                                            Value value,
                                            Version version) {
            return null;
        }
        @Override
        public Operation createPutIfVersion(Key key,
                                            Value value,
                                            Version version,
                                            ReturnValueVersion.Choice prevReturn,
                                            boolean abortIfUnsuccessful) {
            return null;
        }
        @Override
        public Operation createDelete(Key key) { return null; }
        @Override
        public Operation createDelete(Key key,
                                      ReturnValueVersion.Choice prevReturn,
                                      boolean abortIfUnsuccessful) {
            return null;
        }
        @Override
        public Operation createDeleteIfVersion(Key key, Version version) {
            return null;
        }
        @Override
        public Operation createDeleteIfVersion
            (Key key,
             Version version,
             ReturnValueVersion.Choice prevReturn,
             boolean abortIfUnsuccessful) {
            return null;
        }
    }

    @Override
    public void close() { }

    @Override
    public KVStats getStats(boolean clear) {
        /* TODO: Add mock */
        return null;
    }

    @Override
    public KVStats getStats(String watcherName, boolean clear) {
        /* TODO: Add mock */
        return null;
    }

    @Override
    public void login(LoginCredentials creds) {
    }

    @Override
    public void logout() {
    }

    @Override
    public Version putLOB(Key key,
                             InputStream lobStream,
                             Durability durability,
                             long timeout,
                             TimeUnit timeoutUnit)
        throws IOException,
               ConcurrentModificationException {
        return null;
    }

    @Override
    public boolean deleteLOB(Key key,
                             Durability durability,
                             long timeout,
                             TimeUnit timeoutUnit)
        throws ConcurrentModificationException {
        return false;
    }

    @Override
    public InputStreamVersion getLOB(Key key,
                                     Consistency consistency,
                                     long timeout,
                                     TimeUnit timeoutUnit)
        throws ConcurrentModificationException {
        return null;
    }

    @Override
    public Version putLOBIfAbsent(Key lobKey,
                                  InputStream lobStream,
                                  Durability durability,
                                  long chunkTimeout,
                                  TimeUnit timeoutUnit)
        throws IOException,
               ConcurrentModificationException {
        return null;
    }

    @Override
    public Version putLOBIfPresent(Key lobKey,
                                   InputStream lobStream,
                                   Durability durability,
                                   long chunkTimeout,
                                   TimeUnit timeoutUnit)
        throws IOException,
               ConcurrentModificationException {
        return null;
    }

    @Override
    public TableAPI getTableAPI() {
        return null;
    }

    @Override
    public Version appendLOB(Key lobKey,
                             InputStream lobAppendStream,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit)
        throws IOException,
               ConcurrentModificationException {
        return null;
    }

    @Override
    public ExecutionFuture execute(String statement) throws FaultException,
            IllegalArgumentException {
        return execute(statement, null);
    }

    @Override
    public ExecutionFuture execute(String statement, ExecuteOptions options)
        throws IllegalArgumentException {
        return null;
    }

    @Override
    public ExecutionFuture execute(char[] statement, ExecuteOptions options)
        throws IllegalArgumentException {
        return null;
    }

    @Override
    public Publisher<RecordValue> executeAsync(String statement,
                                               ExecuteOptions options) {
        return null;
    }

    @Override
    public Publisher<RecordValue> executeAsync(Statement statement,
                                               ExecuteOptions options) {
        return null;
    }

    @Override
    public StatementResult executeSync(String statement) throws FaultException,
            IllegalArgumentException {
        return executeSync(statement, null);
    }

    @Override
    public StatementResult executeSync(String statement,
        ExecuteOptions options)
        throws IllegalArgumentException {
        return null;
    }

    @Override
    public StatementResult executeSync(char[] statement,
        ExecuteOptions options)
        throws IllegalArgumentException {
        return null;
    }

    @Override
    public ExecutionFuture getFuture(byte[] futureBytes)
            throws IllegalArgumentException {
        return null;
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(Iterator<Key> parentKeyIterator,
                      int batchSize,
                      KeyRange subRange,
                      Depth depth,
                      Consistency consistency,
                      long timeout,
                      TimeUnit timeoutUnit,
                      StoreIteratorConfig storeIteratorConfig) {
        return null;
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(Iterator<Key> parentKeyIterator,
                          int batchSize,
                          KeyRange subRange,
                          Depth depth,
                          Consistency consistency,
                          long timeout,
                          TimeUnit timeoutUnit,
                          StoreIteratorConfig storeIteratorConfig) {
        return null;
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(List<Iterator<Key>> parentKeyIterators,
                      int batchSize,
                      KeyRange subRange,
                      Depth depth,
                      Consistency consistency,
                      long timeout,
                      TimeUnit timeoutUnit,
                      StoreIteratorConfig storeIteratorConfig) {
        return null;
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(List<Iterator<Key>> parentKeyIterators,
                          int batchSize,
                          KeyRange subRange,
                          Depth depth,
                          Consistency consistency,
                          long timeout,
                          TimeUnit timeoutUnit,
                          StoreIteratorConfig storeIteratorConfig) {
        return null;
    }

    @Override
    public void put(List<EntryStream<KeyValue>> streams,
                    BulkWriteOptions bulkWriteOptions) {
    }

    @Override
    public PreparedStatement prepare(final String query)
            throws FaultException, IllegalArgumentException {
        throw new IllegalStateException("Not implemented.");
    }

    @Override
    public PreparedStatement prepare(final String query, ExecuteOptions options)
            throws FaultException, IllegalArgumentException {
        throw new IllegalStateException("Not implemented.");
    }

    @Override
    public PreparedStatement prepare(final char[] statement,
        ExecuteOptions options) {
        throw new IllegalStateException("Not implemented.");
    }

    @Override
    public StatementResult executeSync(Statement statement) {

        return executeSync(statement, null);
    }

    @Override
    public StatementResult executeSync(Statement statement,
                                       ExecuteOptions options) {
        return null;
    }
}
