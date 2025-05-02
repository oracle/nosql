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

package oracle.kv.impl.xregion.agent;

import java.util.logging.Logger;

import oracle.kv.impl.pubsub.NoSQLSubscriptionImpl;
import oracle.kv.pubsub.NoSQLSubscriber;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.StreamPosition;

import org.reactivestreams.Subscription;

/**
 * Object represents the base class of region agent subscriber.
 */
abstract public class BaseRegionAgentSubscriber implements NoSQLSubscriber {

    /* private logger */
    protected final Logger logger;

    /* parent inbound stream to use this subscriber */
    protected final RegionAgentThread parent;

    /* configuration of the subscription */
    protected final NoSQLSubscriptionConfig config;

    /* cause of subscription failure */
    protected volatile Throwable cause;

    /* the stream position of last checkpoint, or null if no checkpoint */
    protected volatile StreamPosition ckptPos;

    /* error if fail to change the stream, null otherwise */
    private volatile Throwable changeResultExp;

    /* true if the change result is ready */
    private volatile boolean changeResultReady;

    /* the effective position of last successful change stream, or null */
    private volatile StreamPosition effectivePos;

    /* inbound stream instance */
    private volatile NoSQLSubscription subscription;

    /* true if the subscription is successful */
    private volatile boolean succ;

    public BaseRegionAgentSubscriber(RegionAgentThread agentThread,
                                     NoSQLSubscriptionConfig config,
                                     Logger logger) {
        this.parent = agentThread;
        this.config = config;
        this.logger = logger;

        cause = null;
    }

    @Override
    public void onSubscribe(Subscription s) {
        subscription = (NoSQLSubscription) s;
        subscription.request(getInitialRequestSize());
        succ = true;
    }

    @Override
    public void onComplete() {
        /* never called */
        assert (false);
    }

    @Override
    public NoSQLSubscriptionConfig getSubscriptionConfig() {
        return config;
    }

    @Override
    public void onChangeResult(StreamPosition pos, Throwable exp) {
        changeResultReady = true;
        effectivePos = pos;
        changeResultExp = exp;
    }

    /**
     * Returns the parent region agent thread
     *
     * @return parent region agent thread
     */
    public RegionAgentThread getParent() {
        return parent;
    }

    /**
     * Returns the number of operations to request in {@link #onSubscribe}.
     */
    public long getInitialRequestSize() {
        return Long.MAX_VALUE;
    }

    /**
     * Called when the agent is being shutdown, to do cleanup.
     */
    abstract public void shutdown();

    /**
     * Returns true if the change result is ready to consume, false otherwise.
     *
     * @return true if the change result is ready to consume, false otherwise.
     */
    public boolean isChangeResultReady() {
        return changeResultReady;
    }

    /**
     * Returns true if subscription is successfully established, or false
     * otherwise.
     *
     * @return true if subscription is successfully established, or false
     * otherwise.
     */
    public boolean isSubscriptionSucc() {
        return succ;
    }

    /**
     * Gets the handle of subscription, or null if subscription is not
     * established.
     *
     * @return the handle of subscription, or null.
     */
    public NoSQLSubscription getSubscription() {
        return subscription;
    }

    /**
     * Returns the active subscription within subscriber if exists, or null
     * otherwise.
     */
    protected NoSQLSubscriptionImpl getActiveSubscription() {
        if (subscription != null && !subscription.isCanceled()) {
            /* has an active subscription */
            return (NoSQLSubscriptionImpl) subscription;
        }
        /* no active subscription */
        return null;
    }

    /**
     * Gets the cause of subscription failure signaled via
     * {@link #onError(Throwable)}, or null if subscription is running or
     * shutdown without failure.
     *
     * @return the cause of subscription failure or null.
     */
    public Throwable getFailure() {
        return cause;
    }

    /**
     * Clears previous change result
     */
    public void clearChangeResult() {
        changeResultReady = false;
        effectivePos = null;
        changeResultExp = null;
    }

    /**
     * Returns the cause of change result failure, or null if change is
     * successful.
     *
     * @return cause of change result failure, or null.
     */
    public Throwable getChangeResultExp() {
        return changeResultExp;
    }

    /**
     * Returns the stream position that change is effective, or null if
     * change fails.
     *
     * @return the stream position that change is effective, or null.
     */
    public StreamPosition getEffectivePos() {
        return effectivePos;
    }

    /**
     * Gets the last checkpoint stream position
     * @return the last checkpoint stream position
     */
    public StreamPosition getLastCkpt() {
        return ckptPos;
    }
}
