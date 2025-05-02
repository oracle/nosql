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

package oracle.kv;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import oracle.kv.lob.KVLargeObject;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.query.Statement;
import oracle.kv.stats.KVStats;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TableAPI;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * KVStore is the handle to a store that is running remotely. To create a
 * connection to a store, request a KVStore instance from
 * {@link KVStoreFactory#getStore KVStoreFactory.getStore}.
 *
 * <p>
 * A handle has thread, memory and network resources associated with it.
 * Consequently, the {@link KVStore#close} method must be invoked to free up
 * the resources when the application is done using the KVStore.
 * <p>
 * To minimize resource allocation and deallocation overheads, it's best to
 * avoid repeated creation and closing of handles to the same store. For
 * example, creating and closing a handle around each KVStore operation, would
 * incur large resource allocation overheads resulting in poor application
 * performance.
 * </p>
 * <p>
 * A handle permits concurrent operations, so a single handle is sufficient to
 * access a KVStore in a multi-threaded application. The creation of multiple
 * handles incurs additional resource overheads without providing any
 * performance benefit.
 * </p>
 * <p>
 * Iterators returned by methods of this interface can only be used safely by
 * one thread at a time unless synchronized externally.
 *
 * <p>There are two groups of common failure types that result in throwing
 * instances of {@link RuntimeException} from methods that have these failures
 * in common: read failure exceptions, and write failure exceptions.
 * These are described below.</p>
 * <h2><a id="readExceptions">Read Exceptions</a></h2>
 *
 * <p>Read operations are executed by the {@code get} and {@code
 * iterator} methods, and their related iteration calls. Examples are
 * {@link KVStore#get get}, {@link KVStore#multiGet multiGet} and
 * {@link KVStore#storeIterator storeIterator}. In addition,
 *  the {@link TableAPI} interface has read methods, such as
 * {@link TableAPI#get table get},
 * {@link TableAPI#tableIterator tableIterator}, etc. All read methods may
 * experience the following failures:</p>
 *
 * <ul>
 * <li>{@link FaultException} is the superclass of several read exceptions:
 *   <ul>
 *     <li>{@link ConsistencyException} is thrown if the specified
 *     {@link Consistency} could not be met, within the allowed timeout period.
 *     </li>
 *     <li>{@link RequestTimeoutException} is thrown when a request cannot be
 *     processed because the configured timeout interval is exceeded.
 *     </li>
 *     <li>{@link RequestLimitException} is thrown when a request cannot be
 *     processed because it would exceed the maximum number of active requests
 *     for a node.
 *     </li>
 *   </ul>
 * </li>
 * <li> Read operations may also fail with a
 * <a href="#securityExceptions">security exception</a> or
 * <a href="#handleExceptions">stale handle exception</a> as described below.
 * </li>
 * </ul>
 * <h2><a id="writeExceptions">Write Exceptions</a></h2>
 * <p>Write operations are executed by methods that modify data, such as {@link
 * KVStore#put put}, {@link KVStore#delete delete}, the other {@code put} and
 * {@code delete} variants, {@link KVStore#execute execute}, etc.
 * In addition, the {@link TableAPI} interface has data modification methods,
 * such as {@link TableAPI#put table put},{@link TableAPI#delete table delete},
 * etc.
 * All write methods may experience the following failures:</p>
 *
 * <ul>
 * <li>{@link FaultException} is the superclass of several write exceptions:
 *   <ul>
 *     <li>{@link DurabilityException} is thrown if the specified
 *     {@link Durability} could not be met, within the allowed timeout period.
 *     </li>
 *     <li>{@link RequestTimeoutException} is thrown when a request cannot be
 *     processed because the configured timeout interval is exceeded.
 *     </li>
 *     <li>{@link RequestLimitException} is thrown when a request cannot be
 *     processed because it would exceed the maximum number of active requests
 *     for a node.
 *     </li>
 *   </ul>
 * </li>
 * <li> Write operations may also fail with a
 * <a href="#securityExceptions">security exception</a> or
 * <a href="#handleExceptions">stale handle exception</a> as described below.
 * </li>
 * </ul>
 *
 * <h2><a id="securityExceptions">Security Exceptions</a></h2>
 * <p>There are a number of exceptions related to security that can be thrown
 * by any method that attempts to access data. Data access methods all throw
 * instances of {@link KVSecurityException}, which is the superclass of all
 * security exceptions. Common security exceptions are:</p>
 *
 * <ul>
 * <li>{@link AuthenticationFailureException} is thrown when invalid credentials
 * are passed to an authentication operation.
 * </li>
 * <li>{@link AuthenticationRequiredException} is thrown when a secured
 * operation is attempted and the client is not currently authenticated.
 * </li>
 * <li>{@link UnauthorizedException} is thrown when an authenticated user
 * attempts to perform an operation for which they are not authorized.
 * </li>
 * </ul>
 * <h2><a id="handleExceptions">Stale Handle Exceptions</a></h2>
 * <p>Any method that attempts to access data may also throw a {@link
 * StaleStoreHandleException}, which indicates that the store handle has become
 * invalid. The application should close and reopen the handle.
 * <h2><a id="asyncThreadModel">Thread Model for Asynchronous Execution</a></h2>
 * <p>This and the {@link TableAPI} interface provide asynchronous execution
 * methods such as {@link #executeAsync(String, ExecuteOptions)}.  These async
 * methods are non-blocking in that they return without waiting for any
 * events such as network read and write, or security handshake. The actual
 * handling of such events happens inside an internal thread pool which has a
 * fixed number of threads. These async methods return widely-accepted
 * asynchronous flow-control or computation classes, namely, the {@link
 * Publisher} and {@link CompletableFuture}, from which user-supplied actions
 * are triggered after the execution results are avaiable. We implement these
 * interfaces in a way such that user-supplied actions will be performed by a
 * thread in the internal thread pool. Therefore, these actions must be
 * non-blocking to avoid interfering with internal event processing. This
 * requirement corresponds to those defined in the async classes (see Subsriber
 * Rule 2 in
 * <a href="https://github.com/reactive-streams/reactive-streams-jvm/blob/master/README.md">
 * Reactive Streams </a> and the policies for implementing {@code
 * CompletionStage} in {@link CompletableFuture}). If the triggered method
 * needs to perform a blocking action, use a separate executor to perform the
 * action. For example:
 * <pre>
 * store.executeAsync(statement, null)
 *     .subscribe(new Subscriber&lt;RecordValue&gt;() {
 *         ... ...
 *         &#64;Override public void onSubscribe(Subscription s) {
 *             ... ...
 *             // Uses ForkJoinPool.commonPool for the blocking action
 *             CompletableFuture&lt;Void&gt; cf =
 *                 CompletableFuture.runAsync(
 *                    () -&gt; doBlockingAction());
 *             cf.thenRun(() -&gt; afterBlockingAction());
 *             ... ...
 *         }
 *         ... ...
 *     });
 * </pre>
 */
public interface KVStore extends KVLargeObject, Closeable {

    /**
     * Get the value associated with the key.
     *
     * <p>The {@link KVStoreConfig#getConsistency default consistency} and
     * {@link KVStoreConfig#getRequestTimeout default request timeout} are
     * used.</p>
     *
     * @param key the key used to lookup the key/value pair.
     *
     * @return the value and version associated with the key, or null if no
     * associated value was found.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public ValueVersion get(Key key);

    /**
     * Get the value associated with the key.
     *
     * @param key the key used to lookup the key/value pair.
     *
     * @param consistency determines the consistency associated with the read
     * used to lookup the value.  If null, the {@link
     * KVStoreConfig#getConsistency default consistency} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return the value and version associated with the key, or null if no
     * associated value was found.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public ValueVersion get(Key key,
                            Consistency consistency,
                            long timeout,
                            TimeUnit timeoutUnit);

    /**
     * Returns the descendant key/value pairs associated with the
     * <code>parentKey</code>. The <code>subRange</code> and the
     * <code>depth</code> arguments can be used to further limit the key/value
     * pairs that are retrieved. The key/value pairs are fetched within the
     * scope of a single transaction that effectively provides serializable
     * isolation.
     *
     * <p>This API should be used with caution since it could result in an
     * OutOfMemoryError, or excessive GC activity, if the results cannot all be
     * held in memory at one time. Consider using the {@link
     * KVStore#multiGetIterator} version instead.</p>
     *
     * <p>This method only allows fetching key/value pairs that are descendants
     * of a parentKey that has a <em>complete</em> major path.  To fetch the
     * descendants of a parentKey with a partial major path, use {@link
     * #storeIterator} instead.</p>
     *
     * <p>The {@link KVStoreConfig#getConsistency default consistency} and
     * {@link KVStoreConfig#getRequestTimeout default request timeout} are
     * used.</p>
     *
     * @param parentKey the parent key whose "child" KV pairs are to be
     * fetched.  It must not be null.  The major key path must be complete.
     * The minor key path may be omitted or may be a partial path.
     *
     * @param subRange further restricts the range under the parentKey to
     * the minor path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are returned.  If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @return a SortedMap of key-value pairs, one for each key selected, or an
     * empty map if no keys are selected.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth);

    /**
     * Returns the descendant key/value pairs associated with the
     * <code>parentKey</code>. The <code>subRange</code> and the
     * <code>depth</code> arguments can be used to further limit the key/value
     * pairs that are retrieved. The key/value pairs are fetched within the
     * scope of a single transaction that effectively provides serializable
     * isolation.
     *
     * <p>This API should be used with caution since it could result in an
     * OutOfMemoryError, or excessive GC activity, if the results cannot all be
     * held in memory at one time. Consider using the {@link
     * KVStore#multiGetIterator} version instead.</p>
     *
     * <p>This method only allows fetching key/value pairs that are descendants
     * of a parentKey that has a <em>complete</em> major path.  To fetch the
     * descendants of a parentKey with a partial major path, use {@link
     * #storeIterator} instead.</p>
     *
     * @param parentKey the parent key whose "child" KV pairs are to be
     * fetched.  It must not be null.  The major key path must be complete.
     * The minor key path may be omitted or may be a partial path.
     *
     * @param subRange further restricts the range under the parentKey to
     * the minor path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are returned.  If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @param consistency determines the read consistency associated with the
     * lookup of the child KV pairs.  If null, the {@link
     * KVStoreConfig#getConsistency default consistency} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return a SortedMap of key-value pairs, one for each key selected, or an
     * empty map if no keys are selected.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth,
                                                 Consistency consistency,
                                                 long timeout,
                                                 TimeUnit timeoutUnit);

    /**
     * Returns the descendant keys associated with the <code>parentKey</code>.
     * <p>
     * This method is almost identical to {@link #multiGet(Key, KeyRange,
     * Depth)}. It differs solely in the type of its return value: It returns a
     * SortedSet of keys instead of returning a SortedMap representing
     * key-value pairs.
     * </p>
     *
     * @see #multiGet(Key, KeyRange, Depth)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth);

    /**
     * Returns the descendant keys associated with the <code>parentKey</code>.
     * <p>
     * This method is almost identical to {@link #multiGet(Key, KeyRange,
     * Depth, Consistency, long, TimeUnit)}. It differs solely in the type of
     * its return value: It returns a SortedSet of keys instead of returning a
     * SortedMap representing key-value pairs.
     * </p>
     *
     * @see #multiGet(Key, KeyRange, Depth, Consistency, long, TimeUnit)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth,
                                       Consistency consistency,
                                       long timeout,
                                       TimeUnit timeoutUnit);

    /**
     * The iterator form of {@link #multiGet(Key, KeyRange, Depth)}.
     *
     * <p>The iterator permits an ordered traversal of the descendant key/value
     * pairs associated with the <code>parentKey</code>. It's useful when the
     * result is too large to fit in memory. Note that the result is not
     * transactional and the operation effectively provides read-committed
     * isolation. The implementation batches the fetching of KV pairs in the
     * iterator, to minimize the number of network round trips, while not
     * monopolizing the available bandwidth.</p>
     *
     * <p>This method only allows fetching key/value pairs that are descendants
     * of a parentKey that has a <em>complete</em> major path.  To fetch the
     * descendants of a parentKey with a partial major path, use {@link
     * #storeIterator} instead.</p>
     *
     * <p>The iterator does not support the remove method.</p>
     *
     * <p>The {@link KVStoreConfig#getConsistency default consistency} and
     * {@link KVStoreConfig#getRequestTimeout default request timeout} are
     * used.</p>
     *
     * @param direction specifies the order in which records are returned by
     * the iterator.  Only {@link Direction#FORWARD} and {@link
     * Direction#REVERSE} are supported by this method.
     *
     * @param batchSize the suggested number of keys to fetch during each
     * network round trip.  If only the first or last key-value pair is
     * desired, passing a value of one (1) is recommended.  If zero, an
     * internally determined default is used.
     *
     * @param parentKey the parent key whose "child" KV pairs are to be
     * fetched.  It must not be null.  The major key path must be complete.
     * The minor key path may be omitted or may be a partial path.
     *
     * @param subRange further restricts the range under the parentKey to
     * the minor path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are returned.  If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @return an iterator that permits an ordered traversal of the descendant
     * key/value pairs.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<KeyValueVersion> multiGetIterator(Direction direction,
                                                      int batchSize,
                                                      Key parentKey,
                                                      KeyRange subRange,
                                                      Depth depth);

    /**
     * The iterator form of {@link #multiGet(Key, KeyRange, Depth, Consistency,
     * long, TimeUnit)}.
     *
     * <p>The iterator permits an ordered traversal of the descendant key/value
     * pairs associated with the <code>parentKey</code>. It's useful when the
     * result is too large to fit in memory. Note that the result is not
     * transactional and the operation effectively provides read-committed
     * isolation. The implementation batches the fetching of KV pairs in the
     * iterator, to minimize the number of network round trips, while not
     * monopolizing the available bandwidth.</p>
     *
     * <p>This method only allows fetching key/value pairs that are descendants
     * of a parentKey that has a <em>complete</em> major path.  To fetch the
     * descendants of a parentKey with a partial major path, use {@link
     * #storeIterator} instead.</p>
     *
     * <p>The iterator does not support the remove method.</p>
     *
     * <p>The {@link KVStoreConfig#getConsistency default consistency} and
     * {@link KVStoreConfig#getRequestTimeout default request timeout} are
     * used.</p>
     *
     * @param direction specifies the order in which records are returned by
     * the iterator.  Only {@link Direction#FORWARD} and {@link
     * Direction#REVERSE} are supported by this method.
     *
     * @param batchSize the suggested number of keys to fetch during each
     * network round trip.  If only the first or last key-value pair is
     * desired, passing a value of one (1) is recommended.  If zero, an
     * internally determined default is used.
     *
     * @param parentKey the parent key whose "child" KV pairs are to be
     * fetched.  It must not be null.  The major key path must be complete.
     * The minor key path may be omitted or may be a partial path.
     *
     * @param subRange further restricts the range under the parentKey to
     * the minor path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are returned.  If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @param consistency determines the read consistency associated with the
     * lookup of the child KV pairs.  If null, the {@link
     * KVStoreConfig#getConsistency default consistency} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return an iterator that permits an ordered traversal of the descendant
     * key/value pairs.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<KeyValueVersion> multiGetIterator(Direction direction,
                                                      int batchSize,
                                                      Key parentKey,
                                                      KeyRange subRange,
                                                      Depth depth,
                                                      Consistency consistency,
                                                      long timeout,
                                                      TimeUnit timeoutUnit);

    /**
     * The iterator form of {@link #multiGetKeys(Key, KeyRange, Depth)}.
     * <p>
     * This method is almost identical to {@link #multiGetIterator(Direction,
     * int, Key, KeyRange, Depth)}. It differs solely in the type of its return
     * value: it returns an iterator over keys instead of key/value pairs.
     * </p>
     *
     * @see #multiGetIterator(Direction, int, Key, KeyRange, Depth)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<Key> multiGetKeysIterator(Direction direction,
                                              int batchSize,
                                              Key parentKey,
                                              KeyRange subRange,
                                              Depth depth);

    /**
     * The iterator form of {@link #multiGetKeys(Key, KeyRange, Depth,
     * Consistency, long, TimeUnit)}.
     * <p>
     * This method is almost identical to {@link #multiGetIterator(Direction,
     * int, Key, KeyRange, Depth, Consistency, long, TimeUnit)}. It differs
     * solely in the type of its return value: it returns an iterator over keys
     * instead of key/value pairs.
     * </p>
     *
     * @see #multiGetIterator(Direction, int, Key, KeyRange, Depth,
     * Consistency, long, TimeUnit)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<Key> multiGetKeysIterator(Direction direction,
                                              int batchSize,
                                              Key parentKey,
                                              KeyRange subRange,
                                              Depth depth,
                                              Consistency consistency,
                                              long timeout,
                                              TimeUnit timeoutUnit);

    /**
     * Return an Iterator which iterates over all key/value pairs in unsorted
     * order.
     *
     * <p>Note that the result is not transactional and the operation
     * effectively provides read-committed isolation. The implementation
     * batches the fetching of KV pairs in the iterator, to minimize the number
     * of network round trips, while not monopolizing the available
     * bandwidth.</p>
     *
     * <p>The iterator does not support the remove method.</p>
     *
     * <p>The {@link KVStoreConfig#getConsistency default consistency} and
     * {@link KVStoreConfig#getRequestTimeout default request timeout} are
     * used.</p>
     *
     * @param direction specifies the order in which records are returned by
     * the iterator.  Currently only {@link Direction#UNORDERED} is supported
     * by this method.
     *
     * @param batchSize the suggested number of keys to fetch during each
     * network round trip.  If only the first or last key-value pair is
     * desired, passing a value of one (1) is recommended.  If zero, an
     * internally determined default is used.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize);

    /**
     * Return an Iterator which iterates over all key/value pairs (or the
     * descendants of a parentKey, or those in a KeyRange) in unsorted order.
     *
     * <p>Note that the result is not transactional and the operation
     * effectively provides read-committed isolation. The implementation
     * batches the fetching of KV pairs in the iterator, to minimize the number
     * of network round trips, while not monopolizing the available
     * bandwidth.</p>
     *
     * <p>This method only allows fetching key/value pairs that are descendants
     * of a parentKey that is null or has a <em>partial</em> major path.  To
     * fetch the descendants of a parentKey with a complete major path, use
     * {@link #multiGetIterator} instead.</p>
     *
     * <p>The iterator does not support the remove method.</p>
     *
     * <p>The {@link KVStoreConfig#getConsistency default consistency} and
     * {@link KVStoreConfig#getRequestTimeout default request timeout} are
     * used.</p>
     *
     * @param direction specifies the order in which records are returned by
     * the iterator.  Currently only {@link Direction#UNORDERED} is supported
     * by this method.
     *
     * @param batchSize the suggested number of keys to fetch during each
     * network round trip.  If only the first or last key-value pair is
     * desired, passing a value of one (1) is recommended.  If zero, an
     * internally determined default is used.
     *
     * @param parentKey the parent key whose "child" KV pairs are to be
     * fetched.  It may be null to fetch all keys in the store.  If non-null,
     * the major key path must be a partial path and the minor key path must be
     * empty.
     *
     * @param subRange further restricts the range under the parentKey to
     * the major path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are returned.  If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize,
                                                   Key parentKey,
                                                   KeyRange subRange,
                                                   Depth depth);

    /**
     * Return an Iterator which iterates over all key/value pairs (or the
     * descendants of a parentKey, or those in a KeyRange) in unsorted order.
     *
     * <p>Note that the result is not transactional and the operation
     * effectively provides read-committed isolation. The implementation
     * batches the fetching of KV pairs in the iterator, to minimize the number
     * of network round trips, while not monopolizing the available
     * bandwidth.</p>
     *
     * <p>This method only allows fetching key/value pairs that are descendants
     * of a parentKey that is null or has a <em>partial</em> major path.  To
     * fetch the descendants of a parentKey with a complete major path, use
     * {@link #multiGetIterator} instead.</p>
     *
     * <p>The iterator does not support the remove method.</p>
     *
     * <p>The {@link KVStoreConfig#getConsistency default consistency} and
     * {@link KVStoreConfig#getRequestTimeout default request timeout} are
     * used.</p>
     *
     * @param direction specifies the order in which records are returned by
     * the iterator.  Currently only {@link Direction#UNORDERED} is supported
     * by this method.
     *
     * @param batchSize the suggested number of keys to fetch during each
     * network round trip.  If only the first or last key-value pair is
     * desired, passing a value of one (1) is recommended.  If zero, an
     * internally determined default is used.
     *
     * @param parentKey the parent key whose "child" KV pairs are to be
     * fetched.  It may be null to fetch all keys in the store.  If non-null,
     * the major key path must be a partial path and the minor key path must be
     * empty.
     *
     * @param subRange further restricts the range under the parentKey to
     * the major path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are returned.  If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @param consistency determines the read consistency associated with the
     * lookup of the child KV pairs.  Version-based consistency may not be used.
     * If null, the {@link KVStoreConfig#getConsistency default consistency} is
     * used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize,
                                                   Key parentKey,
                                                   KeyRange subRange,
                                                   Depth depth,
                                                   Consistency consistency,
                                                   long timeout,
                                                   TimeUnit timeoutUnit);

    /**
     * Return an Iterator which iterates over all key/value pairs (or the
     * descendants of a parentKey, or those in a KeyRange). A non-null
     * storeIteratorConfig argument causes a multi-threaded parallel scan on
     * multiple Replication Nodes to be performed.
     *
     * <p>The result is not transactional and the operation effectively
     * provides read-committed isolation. The implementation batches the
     * fetching of KV pairs in the iterator, to minimize the number of network
     * round trips, while not monopolizing the available bandwidth.</p>
     *
     * <p>This method only allows fetching key/value pairs that are descendants
     * of a parentKey that is null or has a <em>partial</em> major path.  To
     * fetch the descendants of a parentKey with a complete major path, use
     * {@link #multiGetIterator} instead.</p>
     *
     * <p>The iterator is not thread safe. It does not support the remove
     * method.</p>
     *
     * <p>The {@link KVStoreConfig#getConsistency default consistency} and
     * {@link KVStoreConfig#getRequestTimeout default request timeout} are
     * used.</p>
     *
     * @param direction specifies the order in which records are returned by
     * the iterator.
     *
     * @param batchSize the suggested number of keys to fetch during each
     * network round trip. If zero, an internally determined default is used.
     *
     * @param parentKey the parent key whose "child" KV pairs are to be
     * fetched. It may be null to fetch all keys in the store. If non-null,
     * the major key path must be a partial path and the minor key path must be
     * empty.
     *
     * @param subRange further restricts the range under the parentKey to
     * the major path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are returned.  If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @param consistency determines the read consistency associated with the
     * lookup of the child KV pairs.  Version-based consistency may not be used.
     * If null, the {@link KVStoreConfig#getConsistency default consistency} is
     * used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @param storeIteratorConfig specifies the configuration for parallel
     * scanning across multiple Replication Nodes. If this is null, an
     * IllegalArgumentException is thrown.
     *
     * @throws IllegalArgumentException if the storeIteratorConfig argument is
     * null.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(Direction direction,
                      int batchSize,
                      Key parentKey,
                      KeyRange subRange,
                      Depth depth,
                      Consistency consistency,
                      long timeout,
                      TimeUnit timeoutUnit,
                      StoreIteratorConfig storeIteratorConfig);

    /**
     * Return an Iterator which iterates over all keys in unsorted order.
     * <p>
     * This method is almost identical to {@link #storeIterator(Direction,
     * int)}. It differs solely in the type of its return value: it returns an
     * iterator over keys instead of key/value pairs.
     * </p>
     *
     * @see #storeIterator(Direction, int)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize);

    /**
     * Return an Iterator which iterates over all keys (or the descendants of a
     * parentKey, or those in a KeyRange) in unsorted order.
     * <p>
     * This method is almost identical to {@link #storeIterator(Direction,
     * int, Key, KeyRange, Depth)}. It differs solely in the type of its return
     * value: it returns an iterator over keys instead of key/value pairs.
     * </p>
     *
     * @see #storeIterator(Direction, int)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize,
                                           Key parentKey,
                                           KeyRange subRange,
                                           Depth depth);

    /**
     * Return an Iterator which iterates over all keys (or the descendants of a
     * parentKey, or those in a KeyRange) in unsorted order.
     * <p>
     * This method is almost identical to {@link #storeIterator(Direction, int,
     * Key, KeyRange, Depth, Consistency, long, TimeUnit)}. It differs solely
     * in the type of its return value: it returns an iterator over keys
     * instead of key/value pairs.
     * </p>
     *
     * @see #storeIterator(Direction, int, Key, KeyRange, Depth, Consistency,
     * long, TimeUnit)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize,
                                           Key parentKey,
                                           KeyRange subRange,
                                           Depth depth,
                                           Consistency consistency,
                                           long timeout,
                                           TimeUnit timeoutUnit);

    /**
     * Return an Iterator which iterates over all keys (or the descendants of a
     * parentKey, or those in a KeyRange). A non-null storeIteratorConfig
     * argument causes a multi-threaded parallel scan on multiple Replication
     * Nodes to be performed.
     * <p>
     * This method is almost identical to {@link #storeIterator(Direction, int,
     * Key, KeyRange, Depth, Consistency, long, TimeUnit,
     * StoreIteratorConfig)} but differs solely in the type of its return
     * value (keys instead of key/value pairs).
     * </p>
     *
     * @see #storeIterator(Direction, int, Key, KeyRange, Depth, Consistency,
     * long, TimeUnit, StoreIteratorConfig)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     */
    public ParallelScanIterator<Key>
        storeKeysIterator(Direction direction,
                          int batchSize,
                          Key parentKey,
                          KeyRange subRange,
                          Depth depth,
                          Consistency consistency,
                          long timeout,
                          TimeUnit timeoutUnit,
                          StoreIteratorConfig storeIteratorConfig);

    /**
     * Returns an Iterator which iterates over all key/value pairs matching the
     * keys supplied by iterator (or the descendants of a parentKey, or those
     * in a KeyRange).
     *
     * <p>
     * The result is not transactional and the operation effectively provides
     * read-committed isolation. The implementation batches the fetching of KV
     * pairs in the iterator, to minimize the number of network round trips,
     * while not monopolizing the available bandwidth. Batches are fetched in
     * parallel across multiple Replication Nodes, the degree of parallelism is
     * controlled by the storeIteratorConfig argument.
     *
     * <p>
     * The {@link KVStoreConfig#getConsistency default consistency} and
     * {@link KVStoreConfig#getRequestTimeout default request timeout} are
     * used.
     * </p>
     *
     * @param parentKeyIterator it yields a sequence of parent keys. The major
     * key path must be complete. The minor key path may be omitted or may be a
     * partial path. The iterator implementation need not be thread safe.
     *
     * @param batchSize the suggested number of keys to fetch during each
     * network round trip. If zero, an internally determined default is used.
     *
     * @param subRange further restricts the range under the parentKey to the
     * major path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are returned. If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @param consistency determines the read consistency associated with the
     * lookup of the child KV pairs. Version-based consistency may not be used.
     * If null, the {@link KVStoreConfig#getConsistency default consistency} is
     * used.
     *
     * @param timeout is an upper bound on the time interval for processing
     * each batch. A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @param storeIteratorConfig specifies the configuration for parallel
     * scanning across multiple Replication Nodes. If this is null, an
     * internally determined default is used.
     *
     * @return an iterator that contains an entry for each key that is present
     * in the store and matches the criteria specified by the
     * <code>parentKeyIterator</code>, <code>subRange</code> and
     * <code>depth</code> constraints. If the <code>parentKeyIterator</code>
     * yields duplicate keys, the KeyValueVersion associated with the duplicate
     * keys will be returned at least once and potentially multiple times. The
     * implementation makes an effort to minimize these duplicate values but
     * the exact number of repeated KeyValueVersion instances is not defined by
     * the implementation, since weeding out such duplicates can be resource
     * intensive.
     *
     * @throws IllegalArgumentException if the storeIteratorConfig argument is
     * null.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     *
     * @since 3.4
     */
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(Iterator<Key> parentKeyIterator,
                      int batchSize,
                      KeyRange subRange,
                      Depth depth,
                      Consistency consistency,
                      long timeout,
                      TimeUnit timeoutUnit,
                      StoreIteratorConfig storeIteratorConfig);


    /**
     * Return an Iterator which iterates over all keys matching the keys
     * supplied by iterator (or the descendants of a parentKey, or those in a
     * KeyRange).
     *
     * <p>
     * This method is almost identical to {@link #storeIterator(Iterator, int,
     * KeyRange, Depth, Consistency, long, TimeUnit, StoreIteratorConfig)}
     * but differs solely in the type of its return value (keys instead of
     * key/value pairs).
     * </p>
     *
     * @see #storeIterator(Iterator, int, KeyRange, Depth, Consistency,
     * long, TimeUnit, StoreIteratorConfig)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     *
     * @since 3.4
     */
    public ParallelScanIterator<Key>
        storeKeysIterator(Iterator<Key> parentKeyIterator,
                          int batchSize,
                          KeyRange subRange,
                          Depth depth,
                          Consistency consistency,
                          long timeout,
                          TimeUnit timeoutUnit,
                          StoreIteratorConfig storeIteratorConfig);

    /**
     * Returns an Iterator which iterates over all key/value pairs matching the
     * keys supplied by iterators (or the descendants of a parentKey, or those
     * in a KeyRange).
     *
     * <p>
     * Except for the difference in the type of the first argument:
     * <code>parentKeyIterators</code>, which is a list of iterators instead of
     * a single iterator, this method is identical to the overloaded {@link
     * #storeIterator(Iterator, int, KeyRange, Depth, Consistency, long,
     * TimeUnit, StoreIteratorConfig)} method. One or more of the iterators in
     * the <code>parentKeyIterators</code> list may be read in parallel to
     * maximize input throughput, the element of key iterator list should be
     * non null.
     * </p>
     *
     * @see #storeIterator(Iterator, int, KeyRange, Depth, Consistency,
     * long, TimeUnit, StoreIteratorConfig)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     *
     * @since 3.4
     */
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(List<Iterator<Key>> parentKeyIterators,
                      int batchSize,
                      KeyRange subRange,
                      Depth depth,
                      Consistency consistency,
                      long timeout,
                      TimeUnit timeoutUnit,
                      StoreIteratorConfig storeIteratorConfig);

    /**
     * Return an Iterator which iterates over all keys matching the keys
     * supplied by iterators (or the descendants of a parentKey, or those in a
     * KeyRange).
     *
     * <p>
     * Except for the difference in the type of the first argument:
     * <code>parentKeyIterators</code>, which is a list of iterators instead of
     * a single iterator, this method is identical to the overloaded
     * {@link #storeKeysIterator(Iterator, int, KeyRange, Depth, Consistency,
     * long, TimeUnit, StoreIteratorConfig)} method. One or more of the
     * iterators in the <code>parentKeyIterators</code> list may be read in
     * parallel to maximize input throughput, the element of key iterator list
     * should be non null.
     * </p>
     *
     * @see #storeKeysIterator(Iterator, int, KeyRange, Depth, Consistency, long,
     * TimeUnit, StoreIteratorConfig)
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     *
     * @since 3.4
     */
    public ParallelScanIterator<Key>
        storeKeysIterator(List<Iterator<Key>> parentKeyIterators,
                          int batchSize,
                          KeyRange subRange,
                          Depth depth,
                          Consistency consistency,
                          long timeout,
                          TimeUnit timeoutUnit,
                          StoreIteratorConfig storeIteratorConfig);

    /**
     * Put a key/value pair, inserting or overwriting as appropriate.
     *
     * <p>The {@link KVStoreConfig#getDurability default durability} and {@link
     * KVStoreConfig#getRequestTimeout default request timeout} are used.</p>
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was inserted or updated and the (non-null) version of the
     *   new KV pair is returned.  There is no way to distinguish between an
     *   insertion and an update when using this method signature.
     *   <li>
     *   The KV pair was not guaranteed to be inserted or updated successfully,
     *   and one of the exceptions listed below is thrown.
     * </ol>
     *
     * @param key the key part of the key/value pair.
     *
     * @param value the value part of the key/value pair.
     *
     * @return the version of the new value.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public Version put(Key key,
                       Value value);

    /**
     * Put a key/value pair, inserting or overwriting as appropriate.
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was inserted or updated and the (non-null) version of the
     *   new KV pair is returned.  The {@code prevValue} parameter may be
     *   specified to determine whether an insertion or update was performed.
     *   If a non-null previous value or version is returned then an update was
     *   performed, otherwise an insertion was performed.  The previous value
     *   or version may also be useful for other application specific reasons.
     *   <li>
     *   The KV pair was not guaranteed to be inserted or updated successfully,
     *   and one of the exceptions listed below is thrown.  The {@code
     *   prevValue} parameter, if specified, is left unmodified.
     * </ol>
     *
     * @param key the key part of the key/value pair
     *
     * @param value the value part of the key/value pair.
     *
     * @param prevValue a {@link ReturnValueVersion} object to contain the
     * previous value and version associated with the given key, or null if
     * they should not be returned.  The version and value in this object are
     * set to null if a previous value does not exist, or the {@link
     * ReturnValueVersion.Choice} specifies that they should not be returned.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return the version of the new value.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public Version put(Key key,
                       Value value,
                       ReturnValueVersion prevValue,
                       Durability durability,
                       long timeout,
                       TimeUnit timeoutUnit);

    /**
     * Put a key/value pair, but only if no value for the given key is present.
     *
     * <p>The {@link KVStoreConfig#getDurability default durability} and {@link
     * KVStoreConfig#getRequestTimeout default request timeout} are used.</p>
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was inserted and the (non-null) version of the new KV pair
     *   is returned.
     *   <li>
     *   The KV pair was not inserted because a value was already present with
     *   the given key; null is returned and no exception is thrown.
     *   <li>
     *   The KV pair was not guaranteed to be inserted successfully and one of
     *   the exceptions listed below is thrown.
     * </ol>
     *
     * @param key the key part of the key/value pair.
     *
     * @param value the value part of the key/value pair.
     *
     * @return the version of the new value, or null if an existing value is
     * present and the put is unsuccessful.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public Version putIfAbsent(Key key,
                               Value value);

    /**
     * Put a key/value pair, but only if no value for the given key is present.
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was inserted and the (non-null) version of the new KV pair
     *   is returned.  The {@code prevValue} parameter, if specified, will
     *   contain a null previous value and version.
     *   <li>
     *   The KV pair was not inserted because a value was already present with
     *   the given key; null is returned and no exception is thrown.  The
     *   {@code prevValue} parameter, if specified, will contain a non-null
     *   previous value and version.  The previous value and version may be
     *   useful for application specific reasons; for example, if an update
     *   will be performed next in this case.
     *   <li>
     *   The KV pair was not guaranteed to be inserted successfully and one of
     *   the exceptions listed below is thrown.  The {@code prevValue}
     *   parameter, if specified, is left unmodified.
     * </ol>
     *
     * @param key the key part of the key/value pair.
     *
     * @param value the value part of the key/value pair.
     *
     * @param prevValue a {@link ReturnValueVersion} object to contain the
     * previous value and version associated with the given key, or null if
     * they should not be returned.  The version and value in this object are
     * set to null if a previous value does not exist, or the {@link
     * ReturnValueVersion.Choice} specifies that they should not be returned.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return the version of the new value, or null if an existing value is
     * present and the put is unsuccessful.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public Version putIfAbsent(Key key,
                               Value value,
                               ReturnValueVersion prevValue,
                               Durability durability,
                               long timeout,
                               TimeUnit timeoutUnit);

    /**
     * Put a key/value pair, but only if a value for the given key is present.
     *
     * <p>The {@link KVStoreConfig#getDurability default durability} and {@link
     * KVStoreConfig#getRequestTimeout default request timeout} are used.</p>
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was updated and the (non-null) version of the new KV pair
     *   is returned.
     *   <li>
     *   The KV pair was not updated because no existing value was present with
     *   the given key; null is returned and no exception is thrown.
     *   <li>
     *   The KV pair was not guaranteed to be updated successfully and one of
     *   the exceptions listed below is thrown.
     * </ol>
     *
     * @param key the key part of the key/value pair.
     *
     * @param value the value part of the key/value pair.
     *
     * @return the version of the new value, or null if no existing value is
     * present and the put is unsuccessful.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public Version putIfPresent(Key key,
                                Value value);

    /**
     * Put a key/value pair, but only if a value for the given key is present.
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was updated and the (non-null) version of the new KV pair
     *   is returned.  The {@code prevValue} parameter, if specified, will
     *   contain a non-null previous value and version.  The previous value and
     *   version may be useful for application specific reasons.
     *   <li>
     *   The KV pair was not updated because no existing value was present with
     *   the given key; null is returned and no exception is thrown.  The
     *   {@code prevValue} parameter, if specified, will contain a null
     *   previous value and version.
     *   <li>
     *   The KV pair was not guaranteed to be updated successfully and one of
     *   the exceptions listed below is thrown.  The {@code prevValue}
     *   parameter, if specified, is left unmodified.
     * </ol>
     *
     * @param key the key part of the key/value pair.
     *
     * @param value the value part of the key/value pair.
     *
     * @param prevValue a {@link ReturnValueVersion} object to contain the
     * previous value and version associated with the given key, or null if
     * they should not be returned.  The version and value in this object are
     * set to null if a previous value does not exist, or the {@link
     * ReturnValueVersion.Choice} specifies that they should not be returned.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return the version of the new value, or null if no existing value is
     * present and the put is unsuccessful.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public Version putIfPresent(Key key,
                                Value value,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit);

    /**
     * Put a key/value pair, but only if the version of the existing value
     * matches the matchVersion argument. Used when updating a value to ensure
     * that it has not changed since it was last read.
     *
     * <p>The {@link KVStoreConfig#getDurability default durability} and {@link
     * KVStoreConfig#getRequestTimeout default request timeout} are used.</p>
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was updated and the (non-null) version of the new KV pair
     *   is returned.
     *   <li>
     *   The KV pair was not updated because no existing value was present with
     *   the given key, or the version of the existing KV pair did not equal
     *   the given {@code matchVersion} parameter; null is returned and no
     *   exception is thrown.
     *   <li>
     *   The KV pair was not guaranteed to be updated successfully and one of
     *   the exceptions listed below is thrown.
     * </ol>
     *
     * @param key the key part of the key/value pair.
     *
     * @param value the value part of the key/value pair.
     *
     * @param matchVersion the version to be matched.
     *
     * @return the version of the new value, or null if the matchVersion
     * parameter does not match the existing value (or no existing value is
     * present) and the put is unsuccessful.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion);

    /**
     * Put a key/value pair, but only if the version of the existing value
     * matches the matchVersion argument. Used when updating a value to ensure
     * that it has not changed since it was last read.
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was updated and the (non-null) version of the new KV pair
     *   is returned.  The {@code prevValue} parameter, if specified, will
     *   contain a non-null previous value and version.  The previous value may
     *   be useful for application specific reasons.
     *   <li>
     *   The KV pair was not updated because no existing value was present with
     *   the given key, or the version of the existing KV pair did not equal
     *   the given {@code matchVersion} parameter; null is returned and no
     *   exception is thrown.  The {@code prevValue} parameter may be specified
     *   to determine whether the failure was due to a missing KV pair or a
     *   version mismatch.  If a null previous value or version is returned
     *   then the KV pair was missing, otherwise a version mismatch occurred.
     *   The previous value or version may also be useful for other application
     *   specific reasons.
     *   <li>
     *   The KV pair was not guaranteed to be updated successfully and one of
     *   the exceptions listed below is thrown.  The {@code prevValue}
     *   parameter, if specified, is left unmodified.
     * </ol>
     *
     * @param key the key part of the key/value pair.
     *
     * @param value the value part of the key/value pair.
     *
     * @param matchVersion the version to be matched.
     *
     * @param prevValue a {@link ReturnValueVersion} object to contain the
     * previous value and version associated with the given key, or null if
     * they should not be returned.  The version and value in this object are
     * set to null if a previous value does not exist, or the {@link
     * ReturnValueVersion.Choice} specifies that they should not be returned,
     * or the matchVersion parameter matches the existing value and the put is
     * successful.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return the version of the new value, or null if the matchVersion
     * parameter does not match the existing value (or no existing value is
     * present) and the put is unsuccessful.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit);

    /**
     * Delete the key/value pair associated with the key.
     *
     * <p>Deleting a key/value pair with this method does not automatically
     * delete its children or descendant key/value pairs.  To delete children
     * or descendants, use {@link #multiDelete multiDelete} instead.</p>
     *
     * <p>The {@link KVStoreConfig#getDurability default durability} and {@link
     * KVStoreConfig#getRequestTimeout default request timeout} are used.</p>
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was deleted and true is returned.
     *   <li>
     *   The KV pair was not deleted because no existing value was present with
     *   the given key; false is returned and no exception is thrown.
     *   <li>
     *   The KV pair was not guaranteed to be deleted successfully and one of
     *   the exceptions listed below is thrown.
     * </ol>
     *
     * @param key the key used to lookup the key/value pair.
     *
     * @return true if the delete is successful, or false if no existing value
     * is present.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public boolean delete(Key key);

    /**
     * Delete the key/value pair associated with the key.
     *
     * <p>Deleting a key/value pair with this method does not automatically
     * delete its children or descendant key/value pairs.  To delete children
     * or descendants, use {@link #multiDelete multiDelete} instead.</p>
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was deleted and true is returned.  The {@code prevValue}
     *   parameter, if specified, will contain a non-null previous value and
     *   version.  The previous value may be useful for application specific
     *   reasons.
     *   <li>
     *   The KV pair was not deleted because no existing value was present with
     *   the given key; false is returned and no exception is thrown.  The
     *   {@code prevValue} parameter, if specified, will contain a null
     *   previous value and version.
     *   <li>
     *   The KV pair was not guaranteed to be deleted successfully and one of
     *   the exceptions listed below is thrown.  The {@code prevValue}
     *   parameter, if specified, is left unmodified.
     * </ol>
     *
     * @param key the key used to lookup the key/value pair.
     *
     * @param prevValue a {@link ReturnValueVersion} object to contain the
     * previous value and version associated with the given key, or null if
     * they should not be returned.  The version and value in this object are
     * set to null if a previous value does not exist, or the {@link
     * ReturnValueVersion.Choice} specifies that they should not be returned.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return true if the delete is successful, or false if no existing value
     * is present.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public boolean delete(Key key,
                          ReturnValueVersion prevValue,
                          Durability durability,
                          long timeout,
                          TimeUnit timeoutUnit);

    /**
     * Delete a key/value pair, but only if the version of the existing value
     * matches the matchVersion argument. Used when deleting a value to ensure
     * that it has not changed since it was last read.
     *
     * <p>Deleting a key/value pair with this method does not automatically
     * delete its children or descendant key/value pairs.  To delete children
     * or descendants, use {@link #multiDelete multiDelete} instead.</p>
     *
     * <p>The {@link KVStoreConfig#getDurability default durability} and {@link
     * KVStoreConfig#getRequestTimeout default request timeout} are used.</p>
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was deleted and true is returned.
     *   <li>
     *   The KV pair was not deleted because no existing value was present with
     *   the given key, or the version of the existing KV pair did not equal
     *   the given {@code matchVersion} parameter; false is returned and no
     *   exception is thrown.  There is no way to distinguish between a missing
     *   KV pair and a version mismatch when using this method signature.
     *   <li>
     *   The KV pair was not guaranteed to be deleted successfully and one of
     *   the exceptions listed below is thrown.
     * </ol>
     *
     * @param key the key to be used to identify the key/value pair.
     *
     * @param matchVersion the version to be matched.
     *
     * @return true if the deletion is successful.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion);

    /**
     * Delete a key/value pair, but only if the version of the existing value
     * matches the matchVersion argument. Used when deleting a value to ensure
     * that it has not changed since it was last read.
     *
     * <p>Deleting a key/value pair with this method does not automatically
     * delete its children or descendant key/value pairs.  To delete children
     * or descendants, use {@link #multiDelete multiDelete} instead.</p>
     *
     * <p>Possible outcomes when calling this method are:
     * <ol>
     *   <li>
     *   The KV pair was deleted and true is returned.  The {@code prevValue}
     *   parameter, if specified, will contain a non-null previous value and
     *   version.  The previous value may be useful for application specific
     *   reasons.
     *   <li>
     *   The KV pair was not deleted because no existing value was present with
     *   the given key, or the version of the existing KV pair did not equal
     *   the given {@code matchVersion} parameter; false is returned and no
     *   exception is thrown.  The {@code prevValue} parameter may be specified
     *   to determine whether the failure was due to a missing KV pair or a
     *   version mismatch.  If a null previous value or version is returned
     *   then the KV pair was missing, otherwise a version mismatch occurred.
     *   The previous value or version may also be useful for other application
     *   specific reasons.
     *   <li>
     *   The KV pair was not guaranteed to be deleted successfully and one of
     *   the exceptions listed below is thrown.  The {@code prevValue}
     *   parameter, if specified, is left unmodified.
     * </ol>
     *
     * @param key the key to be used to identify the key/value pair.
     *
     * @param matchVersion the version to be matched.
     *
     * @param prevValue a {@link ReturnValueVersion} object to contain the
     * previous value and version associated with the given key, or null if
     * they should not be returned.  The version and value in this object are
     * set to null if a previous value does not exist, or the {@link
     * ReturnValueVersion.Choice} specifies that they should not be returned,
     * or the matchVersion parameter matches the existing value and the delete
     * is successful.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return true if the deletion is successful.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion,
                                   ReturnValueVersion prevValue,
                                   Durability durability,
                                   long timeout,
                                   TimeUnit timeoutUnit);

    /**
     * Deletes the descendant Key/Value pairs associated with the
     * <code>parentKey</code>. The <code>subRange</code> and the
     * <code>depth</code> arguments can be used to further limit the key/value
     * pairs that are deleted.
     *
     * <p>The {@link KVStoreConfig#getDurability default durability} and {@link
     * KVStoreConfig#getRequestTimeout default request timeout} are used.</p>
     *
     * @param parentKey the parent key whose "child" KV pairs are to be
     * deleted.  It must not be null.  The major key path must be complete.
     * The minor key path may be omitted or may be a partial path.
     *
     * @param subRange further restricts the range under the parentKey to
     * the minor path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are deleted.  If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @return a count of the keys that were deleted.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth);

    /**
     * Deletes the descendant Key/Value pairs associated with the
     * <code>parentKey</code>. The <code>subRange</code> and the
     * <code>depth</code> arguments can be used to further limit the key/value
     * pairs that are deleted.
     *
     * @param parentKey the parent key whose "child" KV pairs are to be
     * deleted.  It must not be null.  The major key path must be complete.
     * The minor key path may be omitted or may be a partial path.
     *
     * @param subRange further restricts the range under the parentKey to
     * the minor path components in this subRange. It may be null.
     *
     * @param depth specifies whether the parent and only children or all
     * descendants are deleted.  If null, {@link Depth#PARENT_AND_DESCENDANTS}
     * is implied.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * operation.  A best effort is made not to exceed the specified limit. If
     * zero, the {@link KVStoreConfig#getRequestTimeout default request
     * timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return a count of the keys that were deleted.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth,
                           Durability durability,
                           long timeout,
                           TimeUnit timeoutUnit);

    /**
     * This method provides an efficient and transactional mechanism for
     * executing a sequence of operations associated with keys that share the
     * same Major Path. The efficiency results from the use of a single network
     * interaction to accomplish the entire sequence of operations. If the
     * underlying store instance has only one partition, as is the case with
     * {@link oracle.kv.KVLocal} the shared Major Path constraint is relaxed as
     * in that environment all keys can be accessed in a single transaction.
     * <p>
     * The operations passed to this method are created using an {@link
     * OperationFactory}, which is obtained from the {@link
     * #getOperationFactory} method.
     * </p>
     * <p>
     * All the {@code operations} specified are executed within the scope of a
     * single transaction that effectively provides serializable isolation.
     * The transaction is started and either committed or aborted by this
     * method.  If the method returns without throwing an exception, then all
     * operations were executed atomically, the transaction was committed, and
     * the returned list contains the result of each operation.
     * </p>
     * <p>
     * If the transaction is aborted for any reason, an exception is thrown.
     * An abort may occur for two reasons:
     * </p>
     * <ol>
     *   <li>An operation or transaction results in an exception that is
     *   considered a fault, such as a durability or consistency error, a
     *   failure due to message delivery or networking error, etc. A {@link
     *   FaultException} is thrown.</li>
     *   <li>An individual operation returns normally but is unsuccessful as
     *   defined by the particular operation (e.g., a delete operation for a
     *   non-existent key) <em>and</em> {@code true} was passed for the {@code
     *   abortIfUnsuccessful} parameter when the operation was created using
     *   the {@link OperationFactory}.  An {@link OperationExecutionException}
     *   is thrown, and the exception contains information about the failed
     *   operation.</li>
     * </ol>
     * <p>
     * Operations are not executed in the sequence they appear in the {@code
     * operations} list, but are rather executed in an internally defined
     * sequence that prevents deadlocks.  Additionally, if there are two
     * operations for the same key, their relative order of execution is
     * arbitrary; this should be avoided.
     * </p>
     *
     * <p>The {@link KVStoreConfig#getDurability default durability} and {@link
     * KVStoreConfig#getRequestTimeout default request timeout} are used.</p>
     *
     * @param operations the list of operations to be performed. Note that all
     * operations in the list must specify keys with the same Major Path unless
     * the store is single partition.
     *
     * @return the sequence of results associated with the operation. There is
     * one entry for each Operation in the operations argument list.  The
     * returned list is in the same order as the operations argument list.
     *
     * @throws OperationExecutionException if an operation is not successful as
     * defined by the particular operation (e.g., a delete operation for a
     * non-existent key) <em>and</em> {@code true} was passed for the {@code
     * abortIfUnsuccessful} parameter when the operation was created using the
     * {@link OperationFactory}.
     *
     * @throws IllegalArgumentException if operations is null or empty, or not
     * all operations operate on keys with the same Major Path and the store is
     * not single partition, or more than one operation has the same Key.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public List<OperationResult> execute(List<Operation> operations)
        throws OperationExecutionException;

    /**
     * This method provides an efficient and transactional mechanism for
     * executing a sequence of operations associated with keys that share the
     * same Major Path. The efficiency results from the use of a single network
     * interaction to accomplish the entire sequence of operations. If the
     * underlying store instance has only one partition, as is the case with
     * {@link oracle.kv.KVLocal} the shared Major Path constraint is relaxed as
     * in that environment all keys can be accessed in a single transaction.
     * <p>
     * The operations passed to this method are created using an {@link
     * OperationFactory}, which is obtained from the {@link
     * #getOperationFactory} method.
     * </p>
     * <p>
     * All the {@code operations} specified are executed within the scope of a
     * single transaction that effectively provides serializable isolation.
     * The transaction is started and either committed or aborted by this
     * method.  If the method returns without throwing an exception, then all
     * operations were executed atomically, the transaction was committed, and
     * the returned list contains the result of each operation.
     * </p>
     * <p>
     * If the transaction is aborted for any reason, an exception is thrown.
     * An abort may occur for two reasons:
     * </p>
     * <ol>
     *   <li>An operation or transaction results in an exception that is
     *   considered a fault, such as a durability or consistency error, a
     *   failure due to message delivery or networking error, etc. A {@link
     *   FaultException} is thrown.</li>
     *   <li>An individual operation returns normally but is unsuccessful as
     *   defined by the particular operation (e.g., a delete operation for a
     *   non-existent key) <em>and</em> {@code true} was passed for the {@code
     *   abortIfUnsuccessful} parameter when the operation was created using
     *   the {@link OperationFactory}.  An {@link OperationExecutionException}
     *   is thrown, and the exception contains information about the failed
     *   operation.</li>
     * </ol>
     * <p>
     * Operations are not executed in the sequence they appear in the {@code
     * operations} list, but are rather executed in an internally defined
     * sequence that prevents deadlocks.  Additionally, if there are two
     * operations for the same key, their relative order of execution is
     * arbitrary; this should be avoided.
     * </p>
     *
     * @param operations the list of operations to be performed. Note that all
     * operations in the list must specify keys with the same Major Path unless
     * the store is single partition.
     *
     * @param durability the durability associated with the transaction used to
     * execute the operation sequence.  If null, the {@link
     * KVStoreConfig#getDurability default durability} is used.
     *
     * @param timeout is an upper bound on the time interval for processing the
     * set of operations.  A best effort is made not to exceed the specified
     * limit. If zero, the {@link KVStoreConfig#getRequestTimeout default
     * request timeout} is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return the sequence of results associated with the operation. There is
     * one entry for each Operation in the operations argument list.  The
     * returned list is in the same order as the operations argument list.
     *
     * @throws OperationExecutionException if an operation is not successful as
     * defined by the particular operation (e.g., a delete operation for a
     * non-existent key) <em>and</em> {@code true} was passed for the {@code
     * abortIfUnsuccessful} parameter when the operation was created using the
     * {@link OperationFactory}.
     *
     * @throws IllegalArgumentException if operations is null or empty, or not
     * all operations operate on keys with the same Major Path and the store is
     * not single partition, or more than one operation has the same Key.
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     */
    public List<OperationResult> execute(List<Operation> operations,
                                         Durability durability,
                                         long timeout,
                                         TimeUnit timeoutUnit)
        throws OperationExecutionException;

    /**
     * Loads Key/Value pairs supplied by special purpose streams into the store.
     * The bulk loading of the entries is optimized to make efficient use of
     * hardware resources. As a result, this operation can achieve much higher
     * throughput when compared with single Key/Value put APIs.
     * <p>
     * Entries are supplied to the loader by a list of EntryStream instances.
     * Each stream is read sequentially, that is, each EntryStream.getNext() is
     * allowed to finish before the next operation is issued. The load
     * operation typically reads from these streams in parallel as determined
     * by {@link BulkWriteOptions#getStreamParallelism}.
     * </p>
     * <p>
     * If an entry is associated with a key that's already present in the store,
     * the {@link EntryStream#keyExists} method is invoked on it and the entry
     * is not loaded into the store by this method; the
     * {@link EntryStream#keyExists} method may of course choose to do so
     * itself, if the values differ.
     * </p>
     * <p>
     * If the key is absent, a new entry is created in the store, that is, the
     * load operation has putIfAbsent semantics. The putIfAbsent semantics
     * permit restarting a load of a stream that failed for some reason.
     * </p>
     * <p>
     * The collection of streams defines a partial insertion order, with
     * insertion of Key/Value pairs containing the same major key within a
     * stream being strictly ordered, but with no ordering constraints being
     * imposed on keys across streams, or for different keys within the same
     * stream.
     * </p>
     * <p>
     * The behavior of the bulk put operation with respect to duplicate entries
     * contained in different streams is thus undefined. If the duplicate
     * entries are just present in a single stream, then the first entry will
     * be inserted (if it's not already present) and the second entry and
     * subsequent entries will result in the invocation of the
     * {@link EntryStream#keyExists} method. If duplicates exist across
     * streams, then the first entry to win the race is inserted and subsequent
     * duplicates will result in {@link EntryStream#keyExists} being invoked on
     * them.
     * </p>
     * <p>
     * Load operations tend to be long running. In order to facilitate
     * resumption of such a long running operation due to a process or client
     * machine failure, the application may use to checkpoint its progress at
     * granularity of a stream, using the
     * {@link oracle.kv.EntryStream#completed} method. The javadoc for this
     * method provides further details.
     * </p>
     * <p>
     * Exceptions encountered during the reading of streams result in the put
     * operation being terminated and the first such exception being thrown
     * from the put method.
     * </p>
     * <p>
     * Exceptions encountered when inserting a Key/Value pair into the store
     * result in the {@link EntryStream#catchException} being invoked.
     * </p>
     * @param streams the streams that supply the Key/Value pairs to be
     * inserted. Elements of stream list must not be null.
     *
     * @param bulkWriteOptions non-default arguments controlling the behavior
     * the bulk write operations
     *
     * @since 4.0
     */
    public void put(List<EntryStream<KeyValue>> streams,
                    BulkWriteOptions bulkWriteOptions);

    /**
     * Returns a factory that is used to creation operations that can be passed
     * to {@link #execute execute}.
     */
    public OperationFactory getOperationFactory();

    /**
     * Close the K/V Store handle and release resources.  If the K/V Store
     * is secure, this also logs out of the store.
     */
    @Override
    public void close();

    /**
     * Returns the statistics related to the K/V Store client.
     *
     * @param clear If set to true, configure the statistics operation to
     * reset statistics after they are returned.
     *
     * @return The K/V Store client side statistics.
     */
    public KVStats getStats(boolean clear);

    /**
     * Returns the statistics related to the K/V Store client.
     *
     * <p>Returns the statistics associated with the specified {@code
     * watcherName}. Statistics obtained for different watcher names are
     * independent of each other.
     *
     * @param watcherName the watcher name with respect to the statistics
     * @param clear if true, configure the statistics operation to reset
     * statistics after they are returned
     *
     * @return the K/V Store client side statistics
     *
     * @hidden Until per-watcher stats are made public
     */
    public KVStats getStats(String watcherName, boolean clear);

    /**
     * Returns an instance of the TableAPI interface for the store.
     */
    public TableAPI getTableAPI();

    /**
     * Updates the login credentials for this store handle.
     * This should be called in one of two specific circumstances.
     * <ul>
     * <li>
     * If the application has caught an
     * {@link AuthenticationRequiredException},
     * this method will attempt to re-establish the authentication of this
     * handle against the store, and the credentials provided must be for
     * the store handle's currently logged-in user.
     * </li>
     * <li>
     * If this handle is currently logged out, login credentials for any user
     * may be provided, and the login, if successful, will associate the
     * handle with that user.
     * </li>
     * </ul>
     * @param creds the login credentials to associate with the store handle
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded.
     *
     * @throws AuthenticationFailureException if the LoginCredentials
     * do not contain valid credentials.
     *
     * @throws IllegalArgumentException if the LoginCredentials are null
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason.
     *
     * @since 3.0
     */
    public void login(LoginCredentials creds)
        throws RequestTimeoutException, AuthenticationFailureException,
               FaultException, IllegalArgumentException;

    /**
     * Logout the store handle.  After calling this method, the application
     * should not call methods on this handle other than close() before first
     * making a successful call to login().  Calls to other methods will result
     * in AuthenticationRequiredException being thrown.
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded.
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason.
     *
     * @since 3.0
     */
    public void logout()
        throws RequestTimeoutException, FaultException;

    /**
     * Executes an SQL statement. The statement may be a DDL or DML statement.
     * DDL statements can be used to create or modify tables, indices, users
     * and roles. DML statements may be read-only SQL queries that extract data
     * from tables, or SQL statements that insert, update, or delete table rows.
     * <p>
     * For DDL statements, the operation is asynchronous and may not be finished
     * when the method returns. An {@link ExecutionFuture} instance is returned
     * which extends {@link java.util.concurrent.Future} and can be used to get
     * information about the status of the operation, or to await completion of
     * the operation.  For example:
     * <pre>
     * // Create a table
     * ExecutionFuture future = null;
     * try {
     *     future = store.execute
     *          ("CREATE TABLE users (" +
     *           "id INTEGER, " +
     *           "firstName STRING, " +
     *           "lastName STRING, " +
     *           "age INTEGER, " +
     *           "PRIMARY KEY (id))");
     * } catch (IllegalArgumentException e) {
     *     System.out.println("The statement is invalid: " + e);
     * } catch (FaultException e) {
     *     System.out.println("There is a transient problem, retry the " +
     *                          "operation: " + e);
     * }
     * // Wait for the operation to finish
     * StatementResult result = future.get();
     * </pre>
     * <p>
     * If the store is currently executing another DDL operation that is the
     * logical equivalent of the action specified by the statement, the method
     * will return an ExecutionFuture that serves as a handle to that operation,
     * rather than starting a new invocation of the command. The caller can use
     * the ExecutionFuture to await the completion of the operation. For
     * example:
     * <pre>
     *   // process A starts an index creation
     *   ExecutionFuture futureA =
     *       store.execute("CREATE INDEX age ON users(age)");
     *
     *   // process B starts the same index creation. If the index creation is
     *   // still running in the cluster, futureA and futureB will refer to
     *   // the same operation
     *   ExecutionFuture futureB =
     *       store.execute("CREATE INDEX age ON users(age)");
     * </pre>
     * <p>
     * For DML statements, the method initiates the execution and returns a
     * {@link ExecutionFuture} that is already in the "done" state. However,
     * no query results have been produced yet. Obtaining the query results
     * must be done by iterating over the result set via the
     * {@link StatementResult#iterator}. For example:
     * <pre>
     * // Execute a query
     * ExecutionFuture future = null;
     * try {
     *     future = store.execute("select id, lastName from users where age &gt; 30");
     * } catch (IllegalArgumentException e) {
     *     System.out.println("The statement is invalid: " + e);
     * } catch (FaultException e) {
     *     System.out.println("There is a transient problem, retry the " +
     *                          "operation: " + e);
     * }
     * // The following get() call will return immediately, because the future
     * // is done already.
     * assert(futue.isDone());
     * StatementResult resultSet = future.get();
     * // iterate over the query result set
     * TableIterator&lt;RecordValue&gt; ither = resultSet.iterator();
     * while (iter.hasNext()) {
     *     RecordValue rec = iter.next();
     *     System.out.println(rec);
     * }
     * resultSet.close();
     * </pre>
     * <p>
     * Note that, in a secure store, creating and modifying table and index
     * definitions may require a level of system privileges over and beyond
     * that required for reads and writes of table records.
     * <br>
     * See the documentation for a full description of the supported statements.
     * <p>
     * Note that this method supersedes oracle.kv.table.TableAPI.execute.
     *
     * @param statement must follow valid SQL syntax.
     *
     * @throws IllegalArgumentException if statement is not valid or cannot be executed
     * because of a syntactic or semantic error.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     * @see <a href="#writeExceptions">Write exceptions</a>
     *
     * @since 3.3
     */
    ExecutionFuture execute(String statement);

    /**
     * This method is equivalent to {@link #execute(String)}, but it allows its
     * users to specify execution options that override the defaults.
     *
     * @param statement must follow valid SQL syntax.
     * @param options options that override the defaults.
     *
     * @throws IllegalArgumentException if statement is not valid or cannot be executed
     * because of a syntactic or semantic error.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     * @see <a href="#writeExceptions">Write exceptions</a>
     *
     * @since 4.0
     */
    ExecutionFuture execute(String statement, ExecuteOptions options);

    /**
     * This method is equivalent to {@link #execute(String, ExecuteOptions)},
     * but the statement is given as a char[].
     * <p>Note: Use this method when statement may contain passwords. It is
     * safe to clean the contents of the statement char array after the call.
     * </p>
     *
     * @since 4.5
     */
    ExecutionFuture execute(char[] statement, ExecuteOptions options);

    /**
     * Synchronously execute an SQL statement. This is a convenience method
     * that is equivalent to:
     * <pre>
     * ExecutionFuture future = execute(statement);
     * return future.get();
     * </pre>
     *
     * @param statement must follow valid SQL syntax.
     * @throws IllegalArgumentException if statement is not valid or cannot be executed
     * because of a syntactic or semantic error.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     * @see <a href="#writeExceptions">Write exceptions</a>
     *
     * @see #execute(String)
     *
     * @since 3.3
     */
    StatementResult executeSync(String statement);

    /**
     * Synchronously execute an SQL statement. This is a convenience method
     * that is equivalent to:
     * <pre>
     * ExecutionFuture future = execute(statement, options);
     * return future.get();
     * </pre>
     *
     * @param statement must follow valid SQL syntax.
     * @param options options that override the defaults.
     * @throws IllegalArgumentException if statement is not valid or cannot be executed
     * because of a syntactic or semantic error.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     * @see <a href="#writeExceptions">Write exceptions</a>
     *
     * @since 4.0
     */
    StatementResult executeSync(String statement, ExecuteOptions options);

    /**
     * Synchronously execute an SQL statement. This is a convenience method
     * that is equivalent to:
     * <pre>
     * ExecutionFuture future = execute(statement, options);
     * return future.get();
     * </pre>
     * <p>Note: Use this method when statement may contain passwords. It is
     * safe to clean the contents of the statement char array after the call.
     * </p>
     *
     * @since 4.5
     */
    StatementResult executeSync(char[] statement, ExecuteOptions options);

    /**
     * Synchronously execute a prepared SQL statement provided as a
     * {@link PreparedStatement} or {@link BoundStatement}.
     * <p>
     * The method has the same semantics as {@link #executeSync(String)}, but
     * takes a prepared statement as input, whereas
     * {@link #executeSync(String)} internally compiles/prepares its input
     * statement text.
     * <p>
     * Use this method if you want to execute the same query multiple times.
     * If the query contains external variables, these variables must be bound
     * to specific values before each execution.
     *
     * @param statement The prepared statement to be executed.
     * @return Returns the result of the statement.
     * @throws IllegalArgumentException if the statement fails to be executed
     * because of a semantic problem with the query.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     * @see <a href="#writeExceptions">Write exceptions</a>
     *
     * @since 4.0
     */
    StatementResult executeSync(Statement statement);

    /**
     * Synchronously execute a prepared SQL statement provided as a
     * {@link PreparedStatement} or {@link BoundStatement}.
     * <p>
     * The method has the same semantics as
     * {@link #executeSync(String, ExecuteOptions)}, but takes a prepared
     * statement as input, whereas {@link #executeSync(String, ExecuteOptions)}
     * internally compiles/prepares its input statement text.
     * <p>
     * Use this method if you want to execute the same query multiple times.
     * If the query contains external variables, these variables must be bound
     * to specific values before each execution.
     *
     * @param statement The prepared statement to be executed.
     * @param options Execution options.
     * @return Returns the result of the statement.
     *
     * @throws IllegalArgumentException if the statement fails to be executed
     * because of a semantic problem with the query.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     * @see <a href="#writeExceptions">Write exceptions</a>
     *
     * @since 4.0
     */
    StatementResult executeSync(Statement statement, ExecuteOptions options);

    /**
     * Returns a publisher that can be used to subscribe to the results of the
     * asynchronous execution of an SQL statement. DDL statements are not
     * supported. The subscription supplied to the subscriber will implement
     * {@link ExecutionSubscription}. The publisher may only be used for a
     * single subscription.
     *
     * <p>The subscription can be used to iterate over the results of the
     * query, to wait for the query to complete, to close the query, or to
     * check the status of the query.
     *
     * <p>For example:
     * <pre>
     * // Query a table
     * store.executeAsync("SELECT * FROM users", null)
     *     .subscribe(new Subscriber&lt;RecordValue&gt;() {
     *         &#64;Override public void onSubscribe(Subscription s) {
     *             s.request(1000);
     *         }
     *         &#64;Override public void onNext(RecordValue record) {
     *             System.out.println(record);
     *         }
     *         &#64;Override public void onComplete() {
     *             System.out.println("Done");
     *         }
     *         &#64;Override public void onError(Throwable exception) {
     *             exception.printStackTrace();
     *         }
     *     });
     * </pre>
     *
     * <p>This method only supports data access (DML) operations.  If the
     * statement is a data definition (DDL) or administrative operation, the
     * {@link Subscriber#onError} method will be called with an {@link
     * UnsupportedOperationException}.
     *
     * <p>See the documentation for a full description of the supported
     * DML statements.
     *
     * <p>If {@link Publisher#subscribe} is called more than once on the
     * publisher, {@link Subscriber#onError} will be called with an {@link
     * IllegalStateException}.
     *
     * <p>If statement execution fails, {@link Subscriber#onError} will be
     * called with one of the following exceptions:
     *
     * <ul>
     * <li> {@link IllegalArgumentException} - if the statement is not valid or
     * cannot be executed because of a syntactic or semantic error
     *
     * <li> {@link FaultException} - for one of the standard <a
     * href="../KVStore.html#readExceptions">read exceptions</a> or <a
     * href="../KVStore.html#writeExceptions">write exceptions</a>
     * </ul>
     *
     * @param statement statement using Table syntax
     *
     * @param options options that override the defaults
     *
     * @return a publisher for subscribing to the operation
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     *
     * @see <a href="#asyncThreadModel">Thread model for asynchronous
     * execution</a>
     *
     * @since 19.5
     */
    Publisher<RecordValue> executeAsync(String statement,
                                        ExecuteOptions options);

    /**
     * Returns a publisher that can be used to subscribe to the results of the
     * asynchronous execution of a table statement, either a {@link
     * PreparedStatement} or a {@link BoundStatement}. DDL statements are not
     * supported. The subscription supplied to the subscriber will implement
     * {@link ExecutionSubscription}. The publisher may only be used for a
     * single subscription.
     *
     * <p>The subscription can be used to iterate over the results of the
     * query, to wait for the query to complete, to close the query, or to
     * check the status of the query.
     *
     * <p>For example:
     * <pre>
     * // Query a table
     * Statement statement = store.prepare("SELECT * FROM users");
     * store.executeAsync(statement, null)
     *     .subscribe(new Subscriber&lt;RecordValue&gt;() {
     *         &#64;Override public void onSubscribe(Subscription s) {
     *             s.request(1000);
     *         }
     *         &#64;Override public void onNext(RecordValue record) {
     *             System.out.println(record);
     *         }
     *         &#64;Override public void onComplete() {
     *             System.out.println("Done");
     *         }
     *         &#64;Override public void onError(Throwable exception) {
     *             exception.printStackTrace();
     *         }
     *     });
     * </pre>
     *
     * <p>This method only supports data access (DML) operations.  If the
     * statement is a data definition (DDL) or administrative operation, the
     * {@link Subscriber#onError} method will be called with an {@link
     * UnsupportedOperationException}.
     *
     * <p>See the documentation for a full description of the supported
     * DML statements.
     *
     * <p>If {@link Publisher#subscribe} is called more than once on the
     * publisher, {@link Subscriber#onError} will be called with an {@link
     * IllegalStateException}.
     *
     * <p>If statement execution fails, {@link Subscriber#onError} will be
     * called with one of the following exceptions:
     *
     * <ul>
     * <li> {@link IllegalArgumentException} - if the statement is not valid or
     * cannot be executed because of a syntactic or semantic error
     *
     * <li> {@link FaultException} - for one of the standard <a
     * href="../KVStore.html#readExceptions">read exceptions</a> or <a
     * href="../KVStore.html#writeExceptions">write exceptions</a>
     * </ul>
     *
     * @param statement statement to be executed
     *
     * @param options options that override the defaults
     *
     * @return a publisher for subscribing to the operation
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     *
     * @see <a href="#writeExceptions">Write exceptions</a>
     *
     * @see <a href="#asyncThreadModel">Thread model for asynchronous
     * execution</a>
     *
     * @since 19.5
     */
    Publisher<RecordValue> executeAsync(Statement statement,
                                        ExecuteOptions options);

    /**
     * Obtain a handle onto a previously issued DDL operation, using a
     * serialized version of the ExecutionFuture obtained from
     * {@link ExecutionFuture#toByteArray}. Does not result in any
     * communication with the server. The user must call the appropriate
     * methods on the reconstituted future to get up to date status.
     * For example:
     * <pre>
     * // futureBytes can be saved and used later to recreate an
     * // ExecutionFuture instance
     * ExecutionFuture laterFuture = store.getFuture(futureBytes);
     *
     * // laterFuture doesn't have any status yet. Call ExecutionFuture.get
     * // or updateStatus to communicate with the server and get new
     * // information
     * StatementResult laterResult = laterFuture.get();
     * </pre>
     * <p>
     * Values created with either the current or earlier releases can be used
     * with this method, but values created by later releases are not
     * guaranteed to be compatible.
     *
     * @return an ExecutionFuture representing a previously issued DDL
     * operation
     *
     * @throws IllegalArgumentException if futureBytes isn't a valid
     * representation of an ExecutionFuture instance.
     *
     * @since 3.3
     */
    ExecutionFuture getFuture(byte[] futureBytes)
        throws IllegalArgumentException;

    /**
     * Compiles a query into an execution plan.
     *
     * @param statement The statement to be compiled.
     *
     * @return Returns the prepared statement of the query.
     *
     * @throws IllegalArgumentException if statement is not valid.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     *
     * @since 4.0
     */
    PreparedStatement prepare(String statement);

    /**
     * @hidden
     *
     * Compiles a query into an execution plan using the supplied options
     *
     * @param statement The statement to be compiled.
     *
     * @param options ExecuteOptions to use if not null.
     *
     * @return Returns the prepared statement of the query.
     *
     * @throws IllegalArgumentException if statement is not valid.
     *
     * @see <a href="#readExceptions">Read exceptions</a>
     *
     * @since 4.4
     */
    PreparedStatement prepare(String statement, ExecuteOptions options);

    /**
     * @hidden
     *
     * Compiles a query into an execution plan using the supplied options. The
     * method has the same semantics as {@link #prepare(String, ExecuteOptions)}
     *
     * <p>Note: Use this method when statement may contain passwords. It is
     * safe to clean the contents of the statement char array after
     * {@link #execute(char[], ExecuteOptions)} call.</p>
     *
     * @since 4.5
     */
    PreparedStatement prepare(char[] statement, ExecuteOptions options);
}
