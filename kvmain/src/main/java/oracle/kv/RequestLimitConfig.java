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

import java.io.Serializable;

/**
 * Describes how requests may be limited so that one or more nodes with long
 * service times don't end up consuming all available threads in the KVS
 * client.
 *
 * @see KVStoreConfig#setRequestLimit
 * @see RequestLimitException
 */
public class RequestLimitConfig implements Serializable {

    private static final long serialVersionUID = 1;

    /**
     * The default maximum number of active requests.
     */
    public static final int DEFAULT_MAX_ACTIVE_REQUESTS = 100;

    /**
     * The default threshold for activating request limiting, as a percentage
     * of the requested maximum active requests.
     */
    public static final int DEFAULT_REQUEST_THRESHOLD_PERCENT = 90;

    /**
     * The default limit on the number of requests, as a percentage of
     * the requested maximum active requests.
     */
    public static final int DEFAULT_NODE_LIMIT_PERCENT = 80;

    /**
     *  The maximum number of active requests permitted by the KV client
     */
    private final int maxActiveRequests;

    /**
     *  The threshold at which request limiting is activated.
     */
    private final int requestThresholdPercent;

    /**
     * The above threshold expressed as a number of requests.
     */
    private final int requestThreshold;

    /**
     * The limit that must not be exceeded once the above threshold has been
     * crossed.
     */
    private final int nodeLimitPercent;

    /**
     * The above limit expressed as a number of requests.
     */
    private final int nodeLimit;

    private static final RequestLimitConfig defaultRequestLimitConfig =
        new RequestLimitConfig(DEFAULT_MAX_ACTIVE_REQUESTS,
                               DEFAULT_REQUEST_THRESHOLD_PERCENT,
                               DEFAULT_NODE_LIMIT_PERCENT);

    /**
     * Creates a request limiting configuration.
     *
     * The request limiting mechanism is only activated when the number of
     * active requests exceeds the threshold specified by the parameter
     * <code>requestThresholdPercent</code>. Both the threshold and limit
     * parameters below are expressed as a percentage of <code>
     * maxActiveRequests</code>.
     * <p>
     * When the mechanism is active the number of active requests to a node is
     * not allowed to exceed <code>nodeRequestLimitPercent</code>. Any new
     * requests that would exceed this limit are rejected and a <code>
     * RequestLimitException</code> is thrown.
     * <p>
     * For example, consider a configuration with maxActiveRequests=10,
     * requestThresholdPercent=80 and nodeLimitPercent=50. If 8 requests are
     * already active at the client, and a 9th request is received that would
     * be directed at a node which already has 5 active requests, it would
     * result in a <code>RequestLimitException</code> being thrown. If only
     * 7 requests were active at the client, the 8th request would be directed
     * at the node with 5 active requests and the request would be processed
     * normally.
     *
     * @param maxActiveRequests the maximum number of active requests permitted
     * by the KV client. This number is typically derived from the maximum
     * number of threads that the client has set aside for processing requests.
     * The default is 100. Note that the KVStore does not actually enforce this
     * maximum directly. It only uses this parameter as the basis for
     * calculating the requests limits to be enforced at a node.
     *
     * @param requestThresholdPercent the threshold computed as a percentage of
     * <code>maxActiveRequests</code> at which requests are limited. The
     * default is 90.
     *
     * @param nodeLimitPercent determines the maximum number of active requests
     * that can be associated with a node when the request limiting mechanism
     * is active. The default is 80.
     *
     * @throws IllegalArgumentException if any argument is 0 or less, if {@code
     * requestThresholdPercent} is greater than 100, or if {@code
     * nodeLimitPercent} is greater than {@code requestThresholdPercent}
     */
    public RequestLimitConfig(int maxActiveRequests,
                              int requestThresholdPercent,
                              int nodeLimitPercent) {

        if (maxActiveRequests <= 0) {
            throw new IllegalArgumentException("maxActiveRequests: " +
                                               maxActiveRequests +
                                               " must be positive");
        }
        this.maxActiveRequests = maxActiveRequests;

        if (requestThresholdPercent <= 0) {
            throw new IllegalArgumentException(
                "requestThresholdPercent: " + requestThresholdPercent +
                " must be positive");
        } else if (requestThresholdPercent > 100) {
            throw new IllegalArgumentException("requestThresholdPercent: " +
                                               requestThresholdPercent +
                                               " cannot exceed 100");
        }
        this.requestThresholdPercent = requestThresholdPercent;
        requestThreshold =
            (int) (maxActiveRequests * (requestThresholdPercent / 100.0));

        if (nodeLimitPercent <= 0) {
            throw new IllegalArgumentException(
                "nodeLimitPercent: " + nodeLimitPercent +
                " must be positive");
        } else if (nodeLimitPercent > requestThresholdPercent) {
            String msg = "nodeLimitPercent: " + nodeLimitPercent +
            " cannot exceed requestThresholdPercent: " +
            requestThresholdPercent;
            throw new IllegalArgumentException(msg);
        }
        this.nodeLimitPercent = nodeLimitPercent;
        nodeLimit = (int) (maxActiveRequests * (nodeLimitPercent / 100.0));
    }

    /**
     * Returns the maximum number of active requests permitted by the KVS
     * client.
     * <p>
     * The default value is 100.
     */
    public int getMaxActiveRequests() {
        return maxActiveRequests;
    }

    /**
     * Returns the percentage used to compute the active request threshold
     * above which the request limiting mechanism is activated.
     * <p>
     * The default value is 90.
     */
    public int getRequestThresholdPercent() {
        return requestThresholdPercent;
    }

    /**
     * Returns the threshold number of requests above which the request
     * limiting mechanism is activated.
     */
    public int getRequestThreshold() {
        return requestThreshold;
    }

    /**
     * Returns the percentage used to compute the maximum number of requests
     * that may be active at a node.
     * <p>
     * The default value is 80.
     */
    public int getNodeLimitPercent() {
        return nodeLimitPercent;
    }

    /**
     * Returns the maximum number of requests that may be active at a node.
     */
    public int getNodeLimit() {
        return nodeLimit;
    }

    @Override
    public String toString() {
        return String.format("maxActiveRequests=%d," +
                             " requestThresholdPercent=%d%%," +
                             " nodeLimitPercent=%d%%",
                             maxActiveRequests,
                             requestThresholdPercent,
                             nodeLimitPercent);
    }

    /**
     * Returns an instance that uses the default values.
     */
    public static RequestLimitConfig getDefault() {
        return defaultRequestLimitConfig;
    }
}
