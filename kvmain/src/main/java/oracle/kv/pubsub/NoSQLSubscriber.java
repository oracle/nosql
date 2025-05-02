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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * The subscriber interface is to be implemented by the application. The
 * NoSQLSubscriber interface defines additional methods that are described
 * below along with details of NoSQL-specific considerations for the existing
 * methods.
 * <p>
 * Implementation of NOSQLSubscriber should follow the Subscriber rules in the
 * <a href="https://github.com/reactive-streams/reactive-streams-jvm/tree/v1.0.0#specification">Reactive
 * Streams Specification</a>.
 */
public interface NoSQLSubscriber extends Subscriber<StreamOperation> {

    /* override the reactive stream subscription interface */

    /**
     * Invoked after the {@link NoSQLPublisher} has successfully established
     * contact with the store using helper hosts. When this method is called by
     * NoSQLPublisher, the argument is an implementation of {@link
     * NoSQLSubscription}.
     *
     * @see NoSQLSubscription#request
     */
    @Override
    void onSubscribe(Subscription s);

    /**
     * Signals the next NoSQL Operation.
     * <p>
     * The sequence of onNext calls represents the stream of changes, both
     * updates and deletions, made to the store.  The order of calls to onNext
     * for a given key represents the exact order of the operations on that key
     * performed on the store. There are no guarantees about the order of
     * operations across different keys in the stream.
     * <p>
     * If a shard is down, events associated with rows stored on that shard
     * could be arbitrarily delayed until the shard comes back up again and the
     * NoSQL Publisher can establish a shard stream to it.
     */
    @Override
    void onNext(StreamOperation t);

    /**
     * Signals an unrecoverable error in subscription.
     * <p>
     * There are many potential sources of error that the publisher may signal
     * via this method. One of them is worth special mention: the publisher may
     * invoke this method if it finds that it cannot resume the stream from
     * {@link NoSQLSubscriptionConfig#getInitialPosition} because the relevant
     * logs are no longer available at one of the shards.
     */
    @Override
    void onError(Throwable t);

    /**
     * Signals the completion of a subscription
     * <p>
     * Note streaming from kvstore table is unbounded by nature since unbounded
     * updates can be applied to a table. Thus onComplete() will never be
     * called in Stream API. User of Stream API shall implement this method
     * as no-op and any no-op implementation will be ignored.
     */
    @Override
    void onComplete();

    /* additional interface introduced by NoSQLSubscriber */

    /**
     * Invoked by the NoSQL publisher when creating a Subscription.  The
     * implementation of this method should return a configuration that
     * identifies the desired position for starting streaming, the tables whose
     * data should be streamed, and other configuration information.
     * <p>
     * An ill configuration or null return will cancel the subscription and
     * the publisher will release all resources allocated for that subscription.
     *
     * @return the configuration for creating the subscription
     */
    NoSQLSubscriptionConfig getSubscriptionConfig();

    /**
     * Signals a warning during subscription.
     * <p>
     * A call to this method warns the user of a potential issue that does not
     * yet represent a disruption in service, for example, a warning that a
     * shard was not available for an extended period of time. Note that
     * onWarn, unlike onError, does not terminate the flow of signals. It's
     * used to warn the subscriber that the publisher's functioning is
     * impaired, but not as yet fatally and some exception-specific action
     * could be taken to restore the health of the Publisher.
     */
    void onWarn(Throwable t);

    /**
     * Signals when a previously requested checkpoint is completed.
     * If checkpoint fails for any reason, the subscription will skip this
     * checkpoint for certain shards and continue streaming. The subscription
     * will try the next checkpoint when it comes.
     * <p>
     * Note that the stream position checkpoint may not be the one originally
     * supplied if
     * {@link NoSQLSubscription#doCheckpoint(StreamPosition, boolean)} is
     * called to do checkpoint with {@code exact} is set to false.
     *
     * @param streamPosition  the stream position in checkpoint
     * @param failureCause    null if checkpoint succeeds, otherwise the cause
     *                        of checkpoint failure.
     */
    void onCheckpointComplete(StreamPosition streamPosition,
                              Throwable failureCause);

    /**
     * Signals when a request to add or remove a table from the set of
     * subscribed tables is completed. If the change was successful, this
     * method will be called with a non-null stream position that represents
     * the first stream position for which the change has taken effect. This
     * method will be called before any entries for that stream position are
     * provided to a call to {@link #onNext}. If the change was unsuccessful
     * but the subscription is still active, this method will be
     * called with a non-null exception that describes the cause of the
     * failure. If the change caused the subscription to be canceled, this
     * method will not be called, and the {@link #onError} method will be
     * called instead.
     * <p>
     * In particular, this method will be called with a
     * {@link SubscriptionChangeNotAppliedException} whose {@link
     * SubscriptionChangeNotAppliedException#getReason reason} is one of:
     * <ul>
     * <li> {@link
     * SubscriptionChangeNotAppliedException.Reason#TOO_MANY_PENDING_CHANGES TOO_MANY_PENDING_CHANGES}
     * <li> {@link
     * SubscriptionChangeNotAppliedException.Reason#SUBSCRIPTION_CANCELED SUBSCRIPTION_CANCELED}
     * <li> {@link
     * SubscriptionChangeNotAppliedException.Reason#SUBSCRIPTION_ALL_TABLES SUBSCRIPTION_ALL_TABLES}
     * <li> {@link
     * SubscriptionChangeNotAppliedException.Reason#TABLE_ALREADY_SUBSCRIBED TABLE_ALREADY_SUBSCRIBED}
     * <li> {@link
     * SubscriptionChangeNotAppliedException.Reason#TABLE_NOT_SUBSCRIBED TABLE_NOT_SUBSCRIBED}
     * <li> {@link
     * SubscriptionChangeNotAppliedException.Reason#CHANGE_TIMEOUT}
     * </ul>
     * <p>
     * This method will be called with {@link
     * SubscriptionTableNotFoundException} if the table does not exist.
     * <p>
     * If the subscription is unable to apply a requested change to all shards,
     * the subscription will be canceled and the {@code onError} method will be
     * called with a {@link SubscriptionFailureException}.
     * <p>
     * The default implementation does nothing.
     *
     * @param streamPosition the effective stream position of the change, or
     *                       null if the change failed
     * @param failureCause   null if the change was applied successfully,
     *                       otherwise cause of the failure
     * @see NoSQLSubscription#subscribeTable
     * @see NoSQLSubscription#unsubscribeTable
     */
    default void onChangeResult(StreamPosition streamPosition,
                                Throwable failureCause) {
    }

    /**
     * @hidden
     *
     * When a stream is configured to use external checkpoint, this
     * method returns the last checkpoint made by stream application, or null
     * if no checkpoint has been performed. If stream is not configured to
     * use external checkpoint, calling this method would
     * throwUnsupportedOperationException.
     * <p>
     * If a stream is configured to use external checkpoint from Stream
     * application via
     * {@link NoSQLSubscriptionConfig.Builder#setUseExternalCheckpointForElasticity()}
     * in order to ensure the order correctness during elastic operations
     * performed in the kvstore, application need to implement this method to
     * return the last checkpoint made by the Streams application. The
     * checkpoint returned must ensure that all stream operations up to the
     * checkpoint have been persisted or consumed, and the stream would not
     * have to resend these operations in the case of failure. Streams API
     * would use the checkpoint to coordinate the streams from different
     * shards to ensure that the order of writes made to a primary key during
     * elastic operations in kvstore would be preserved, either in the case
     * of single subscriber or multiple sharded subscribers.
     * <p>
     * Please note if this method returns an incorrect checkpoint, e.g.,
     * not all stream operations have been persisted or consumed up to the
     * checkpoint, it may cause out-of-order persistence or consumption of
     * the writes to a primary key in elastic operations. In addition, if
     * this method does not return any valid checkpoint timely, e.g., null
     * for certain shards, Streams API may hold internal streams from those
     * shards and consequently block delivery of stream operations from them.
     *
     * @throws UnsupportedOperationException if stream is configured to use
     * external checkpoint to handle elastic operations in kvstore while this
     * method is not implemented.
     *
     * @return external checkpoint from Stream application, or null if no
     * checkpoint is made
     */
    default StreamPosition getExternalLastCheckpoint() {
        final NoSQLSubscriptionConfig conf = getSubscriptionConfig();
        if (conf == null || !conf.getUseExtCkptForElasticity()) {
            throw new UnsupportedOperationException(
                "Subscription not configured to use external checkpoint");
        }

        throw new UnsupportedOperationException(
            "Returning external checkpoint method is not implemented");
    }
}
