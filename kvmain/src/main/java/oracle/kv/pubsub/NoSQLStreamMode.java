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

/**
 * Subscription stream mode used to configure the starting point for a NoSQL
 * subscription.
 */
public enum NoSQLStreamMode {

    /**
     * Start the stream from the latest available stream position. Any existing
     * checkpoint or specified start stream position will be ignored.
     *
     * <p><b>Example use case</b></p>
     *
     * A user creates a new table "User" and wants to stream all updates made
     * to that table after the subscription is created. She can configure the
     * subscription with:
     *
     * <pre>
     * final NoSQLSubscriptionConfig subscriptionConfig =
     *     new NoSQLSubscriptionConfig.Builder("MyCheckpointTable")
     *     .setSubscribedTables("User")
     *     .setStreamMode(NoSQLStreamMode.FROM_NOW)
     *     .build();
     * </pre>
     *
     * In order to receive all updates made to the table, the user should
     * create the subscription before any updates are made to the table. If
     * not, updates made to the table before the subscription is created may
     * be missed.
     */
    FROM_NOW,

    /**
     * Start the stream from the specified start stream position, signaling an
     * exception if the requested position is not available.  Any existing
     * checkpoint will be ignored.
     *
     * <p>If the requested start position is not available on the master for
     * any shard, the subscription will fail and the publisher will signal
     * {@link SubscriptionInsufficientLogException} to the subscriber via
     * {@link NoSQLSubscriber#onError}.
     *
     * <p>If the requested start position is later than the currently available
     * position on the master for any shard, then the stream will start from
     * the highest position available on the master for that shard.  This
     * situation can happen in unusual cases where the store has performed a
     * hard rollback.
     *
     * <p><b>Example use case</b></p>
     *
     * A user wants to stream all updates made to table "User" starting from a
     * given stream position, and needs to be notified with an exception if the
     * requested stream position is not available. She can configure the
     * subscription with:
     *
     * <pre>
     * final NoSQLStreamPosition position = ...; // given stream position
     * final NoSQLSubscriptionConfig subscriptionConfig =
     *     new NoSQLSubscriptionConfig.Builder("MyCheckpointTable")
     *     .setSubscribedTables("User")
     *     .setStreamMode(NoSQLStreamMode.FROM_EXACT_STREAM_POSITION)
     *     .setStartStreamPosition(position)
     *     .build();
     * </pre>
     */
    FROM_EXACT_STREAM_POSITION,

    /**
     * Start the stream from the specified start stream position, using the
     * next available position for shards where the requested position is
     * not available.  Any existing checkpoint will be ignored.
     *
     * <p>This mode is the same as {@link #FROM_EXACT_STREAM_POSITION} except
     * that the stream will start from the next position available on the
     * master rather than signaling an exception for any shards where the
     * requested start position is not available.
     *
     * <p>If the requested start position is later than the currently available
     * position on the master for any shard, then the stream will start from
     * the highest position available on the master for that shard.  This
     * situation can happen in unusual cases where the store has performed a
     * hard rollback.
     *
     * <p><b>Example use case</b></p>
     *
     * A user wants to stream all updates made to table "User" starting from a
     * given stream position, but can tolerate missing updates if the requested
     * stream position is not available.  She can configure the subscription
     * with:
     *
     * <pre>
     * final NoSQLStreamPosition position = ...; // given stream position
     * final NoSQLSubscriptionConfig subscriptionConfig =
     *     new NoSQLSubscriptionConfig.Builder("MyCheckpointTable")
     *     .setSubscribedTables("User")
     *     .setStreamMode(NoSQLStreamMode.FROM_STREAM_POSITION)
     *     .setStartStreamPosition(position)
     *     .build();
     * </pre>
     */
    FROM_STREAM_POSITION,

    /**
     * Start the stream from the last checkpoint saved in the checkpoint table,
     * signaling an exception if the checkpoint position is not available. Any
     * specified start stream position will be ignored.
     *
     * <p>If the checkpoint table does not exist and the publisher is allowed
     * to create a new checkpoint table for the user, as specified by {@link
     * NoSQLSubscriptionConfig.Builder#setCreateNewCheckpointTable}, then the
     * checkpoint table will be created and the stream will start from the
     * latest position available on the master for each shard.  If the
     * checkpoint table does not exist and the publisher is not allowed to
     * create a new checkpoint table, the subscription will fail and the
     * publisher will signal {@link SubscriptionFailureException} to the
     * subscriber via {@link NoSQLSubscriber#onError}.
     *
     * <p>If the checkpoint position is not available on the master for any
     * shard, the subscription will fail and the publisher will signal {@link
     * SubscriptionInsufficientLogException} to the subscriber via {@link
     * NoSQLSubscriber#onError}.
     *
     * <p>If the checkpoint position is later than the currently available
     * position on the master for any shard, then the stream will start from
     * the highest position available on the master for that shard.  This
     * situation can happen in unusual cases where the store has performed a
     * hard rollback.
     *
     * <p><b>Example use case</b></p>
     *
     * A user wants to stream all updates made to table "User" from the last
     * checkpoint, and needs to be notified with an exception if the checkpoint
     * position is not available.  She can configure the subscription with:
     *
     * <pre>
     * final NoSQLSubscriptionConfig subscriptionConfig =
     *     new NoSQLSubscriptionConfig.Builder("MyCheckpointTable")
     *     .setSubscribedTables("User")
     *     .setStreamMode(NoSQLStreamMode.FROM_EXACT_CHECKPOINT)
     *     .build();
     * </pre>
     */
    FROM_EXACT_CHECKPOINT,

    /**
     * Start the stream from the last checkpoint saved in the checkpoint table,
     * using the next available position for shards where the checkpoint
     * position is not available.  Any specified start stream position will be
     * ignored.
     *
     * <p>This mode is same as {@link #FROM_EXACT_CHECKPOINT} except that the
     * stream will start from the next position available on the master
     * rather than signaling an exception for any shards where the checkpoint
     * position is not available.
     *
     * <p>If the checkpoint start position is later than the currently
     * available position on the master for any shard, then the stream will
     * start from the highest position available on the master for that shard.
     * This situation can happen in unusual cases where the store has performed
     * a hard rollback.
     *
     * <p><b>Example use case</b></p>
     *
     * A user wants to stream all updates made to table "User" from the latest
     * saved checkpoint, but can tolerate missing updates if the checkpoint
     * stream position is not available.  She can configure the subscription
     * with:
     *
     * <pre>
     * final NoSQLSubscriptionConfig subscriptionConfig =
     *     new NoSQLSubscriptionConfig.Builder("MyCheckpointTable")
     *     .setSubscribedTables("User")
     *     .setStreamMode(NoSQLStreamMode.FROM_CHECKPOINT)
     *     .build();
     * </pre>
     */
    FROM_CHECKPOINT;

    /**
     * @hidden
     * Returns true if the stream mode needs a checkpoint
     */
    public boolean needCheckpoint() {
        return this.equals(FROM_CHECKPOINT) ||
               this.equals(FROM_EXACT_CHECKPOINT);
    }
}
