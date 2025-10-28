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

package oracle.kv.pubsub;

import java.util.Set;

import oracle.kv.stats.SubscriptionMetrics;

import org.reactivestreams.Subscription;

/**
 * A NoSQL subscription to the source kvstore. It is created by an instance of
 * NoSQLPublisher with given subscription configuration.
 * <p>
 * A NoSQL subscription has real resources (threads, network connections,
 * iterator state etc.) associated with it. These resources are released when
 * the subscription is canceled via {@link #cancel}.
 */
public interface NoSQLSubscription extends Subscription {

    /* override original reactive stream subscription interface */

    /**
     * Streams a fixed number operations from source kvstore interested to
     * the subscribed tables.
     */
    @Override
    void request(long n);

    /**
     * Clean up and free resources; terminates shard streams in particular.
     */
    @Override
    void cancel();

    /* additional interface functions introduced by NoSQLSubscription */

    /**
     * Returns ID of the subscriber that created the subscription.
     *
     * @return ID of the subscriber that created the subscription
     */
    NoSQLSubscriberId getSubscriberId();

    /**
     * Returns a current position in the stream. All elements before this
     * position have been delivered to the Subscriber via
     * {@link NoSQLSubscriber#onNext}, and delivering the element for
     * the current position to {@code onNext} is either about to begin,
     * underway, or also complete.
     *
     * <p>Although this position can be used by a subsequent subscriber to
     * resume the stream from this point forwards, effectively resuming an
     * earlier subscription, it is preferable either to use the stream position
     * returned by {@link #getOptimizedPosition(StreamPosition)}, or to call
     * {@link #doCheckpoint(StreamPosition, boolean)} and specify false for
     * {@code exact}. Both of those approaches use a later position if
     * possible, and therefore should increase the odds that the checkpoint
     * will be available to resume. When the position returned by this
     * method or {@link #getOptimizedPosition(StreamPosition)} is used to do
     * checkpoint, the caller needs to ensure all earlier operations before
     * the position have been processed, and it is safe to resume the stream
     * from the position.
     *
     * @return current stream position
     */
    StreamPosition getCurrentPosition();

    /**
     * Gets the last checkpoint stored in kv store for the given subscription.
     * <p>
     * If {@link #doCheckpoint(StreamPosition)} or,
     * {@link #doCheckpoint(StreamPosition, boolean)} is called with {@code
     * exact} is set to true is called to checkpoint, it returns the supplied
     * stream position if the checkpoint is successful.
     * <p>
     * If {@link #doCheckpoint(StreamPosition, boolean)} is called with
     * {@code exact} is set to false, a higher stream position can be used to
     * checkpoint and therefore it may return a higher stream position
     * than the supplied one.
     *
     * @return the last checkpoint associated with that subscription, or null
     * if this subscription does not have any persisted checkpoint in kvstore.
     */
    StreamPosition getLastCheckpoint();

    /**
     * Returns true if the subscription has been canceled.
     *
     * @return  true if the subscription has been canceled, false otherwise.
     */
    boolean isCanceled();

    /**
     * Do subscription checkpoint. This is equivalent to calling
     * {@link #doCheckpoint(StreamPosition, boolean)} with {@code exact} is
     * set to true. The checkpoint will be made to the source
     * kvstore. The checkpoint from a subscription is stored in a table
     * dedicated to this particular subscription. Each row in the table
     * represents a checkpoint for a shard. Each row is inserted when the
     * checkpoint is made for the first time, and updated when subsequent
     * checkpoint is made.
     * <p>
     * Note the checkpoint is an asynchronous call. When called, it creates a
     * separate thread to do the check to kvstore, and it itself instantly
     * returns to caller. The result of checkpoint will be signaled to
     * subscriber via {@link NoSQLSubscriber#onCheckpointComplete}.
     * <p>
     * The caller needs to ensure all operations up to the given stream
     * position have been processed and it is safe to resume stream from the
     * given stream position and no operation will be missing after resumption.
     * <p>
     * Unless the application has its own need to store an exact checkpoint
     * position, in most cases it is preferable to call {@link
     * #doCheckpoint(StreamPosition, boolean)} and specify false for {@code
     * exact}, or store the value returned by {@link #getOptimizedPosition},
     * both of which may be able to checkpoint a higher position.
     * <p>
     * It is illegal to call this method concurrently for a subscription. The
     * method should be called only after
     * {@link NoSQLSubscriber#onCheckpointComplete} is called in the previous
     * call of this method, which indicates the previous checkpoint is done.
     * Otherwise {@link SubscriptionFailureException} will be raised.
     *
     * @param streamPosition the stream position to checkpoint
     */
    void doCheckpoint(StreamPosition streamPosition);

    /**
     * Do subscription checkpoint at the given stream position or a stream
     * position later than the given position selected by the Streams API.
     * The checkpoint will be made to the source kvstore like
     * {@link #doCheckpoint(StreamPosition)}. If {@code exact} is
     * true, the checkpoint will be made at exact {@code streamPosition}.
     * Otherwise, Streams API will use a stream position returned by
     * {@link #getOptimizedPosition(StreamPosition)} to checkpoint.
     *
     * @param streamPosition the stream position to checkpoint
     * @param exact true if checkpoint at exact given position, false if use
     *              an optimized stream position to checkpoint.
     */
    void doCheckpoint(StreamPosition streamPosition, boolean exact);

    /**
     * Returns a stream position that represents the same operations supplied
     * to {@link NoSQLSubscriber#onNext}, but one that is optimized for
     * checkpoints. The returned position is equal or later than the given
     * position for all replication groups. Applications should use the results
     * of calling this method when saving a stream position as a checkpoint.
     * The benefit of later position is that a later position is more likely to
     * be available when the stream resumes.
     *
     * @param streamPosition the stream position to checkpoint
     * @return an optimized stream position
     */
    StreamPosition getOptimizedPosition(StreamPosition streamPosition);

    /**
     * Returns the subscription metrics {@link SubscriptionMetrics}
     *
     * @return the subscription metrics
     */
    SubscriptionMetrics getSubscriptionMetrics();

    /**
     * Adds a table to the set of subscribed tables for a running subscription.
     * The subscription will apply the change to every shard in kvstore. This
     * method is asynchronous and will return immediately. The {@link
     * NoSQLSubscriber#onChangeResult(StreamPosition, Throwable)} method will
     * be called when the change is complete. If it fails,
     * {@link NoSQLSubscriber#onChangeResult(StreamPosition, Throwable)} will
     * be called when the subscription does not need terminate, and
     * {@link NoSQLSubscriber#onError(Throwable)} will be called when the
     * subscription need terminate. Calling this method does not block the
     * running subscription.
     *
     * @param tableName the name of the table to subscribe, which is either an
     *                  non-prefixed name that specifies a table in the default
     *                  namespace, or a name with the namespace prefix and a
     *                  colon followed by the table name.
     */
    void subscribeTable(String tableName);

    /**
     * Adds a subscribed table to the running subscription, and specify if to
     * stream transaction. This method is identical to
     * {@link #subscribeTable(String)}, except that user can specify if the
     * writes to the subscribed table should be streamed as transactions or not.
     * @param tableName the name of the table to subscribe, which is either
     *                  an non-prefixed name that specifies a table in the
     *                  default namespace, or a name with the namespace
     *                  prefix and a colon followed by the table name.
     * @param streamTxn true if to stream transactions, false to stream write
     *                 operations in {@link StreamOperation}.
     */
    void subscribeTable(String tableName, boolean streamTxn);

    /**
     * Removes a table from the set of subscribed tables for a running
     * subscription. The subscription will apply the change to every shard in
     * kvstore. This method is asynchronous and will return immediately.The
     * {@link NoSQLSubscriber#onChangeResult(StreamPosition, Throwable)}
     * method will be called when the change is complete. If it fails,
     * {@link NoSQLSubscriber#onChangeResult(StreamPosition, Throwable)} will
     * be called when the subscription does not need terminate, and
     * {@link NoSQLSubscriber#onError(Throwable)} will be called when the
     * subscription need terminate. Calling this method does not block the
     * running subscription.
     *
     * @param tableName the name of the table to subscribe, which is either an
     *                  non-prefixed name that specifies a table in the default
     *                  namespace, or a name with the namespace prefix and a
     *                  colon followed by the table name.
     */
    void unsubscribeTable(String tableName);

    /**
     * Returns the set of currently subscribed tables. If the subscription is
     * configured to stream all user tables, returns null.
     *
     * @return the set of currently subscribed tables, or null.
     *
     * @throws SubscriptionFailureException if the subscription is canceled.
     */
    Set<String> getSubscribedTables() throws SubscriptionFailureException;
}
