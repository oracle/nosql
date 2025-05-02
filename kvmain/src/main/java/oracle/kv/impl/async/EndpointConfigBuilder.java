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

package oracle.kv.impl.async;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.security.ssl.SSLControl;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A builder to build the endpoint configuration.
 */
public class EndpointConfigBuilder {

    /**
     * Allow overriding defaults by setting a system property with the
     * specified prefix followed by the option name.
     */
    private static final String DEFAULT_OVERRIDE_PROPERTY_PREFIX =
        "oracle.kv.async.endpoint.config.default.";

    private static final Map<AsyncOption<?>, Object> optionDefaults =
        new HashMap<AsyncOption<?>, Object>();

    static {
        optionDefault(AsyncOption.SO_KEEPALIVE, null);
        optionDefault(AsyncOption.SO_LINGER, null);
        optionDefault(AsyncOption.SO_RCVBUF, null);
        optionDefault(AsyncOption.SO_REUSEADDR, false);
        optionDefault(AsyncOption.SO_SNDBUF, null);
        optionDefault(AsyncOption.TCP_NODELAY, true);
        /*
         * Do not limit the number of concurrent dialogs. Upper layer uses its
         * own mechanism to limit concurrent requests with RequestLimitConfig.
         * Server side may also set a proper value, but perhaps in most cases
         * does not care.
         */
        optionDefault(AsyncOption.DLG_LOCAL_MAXDLGS, Integer.MAX_VALUE);
        optionDefault(AsyncOption.DLG_REMOTE_MAXDLGS, Integer.MAX_VALUE);
        /*
         * Set the frame length limit to 2k, since we expect in common case the
         * key-value pair size would be at most in the range of 1k. The TCP
         * packet size seems not matter too much as in the busy cases, we are
         * sending batches of frames altogether and it does not seem this frame
         * length would affect the number of TCP packets.
         */
        optionDefault(AsyncOption.DLG_LOCAL_MAXLEN, 2048);
        optionDefault(AsyncOption.DLG_REMOTE_MAXLEN, 2048);
        /*
         * Set the dialog content length to Integer.MAX_VALUE, since before
         * async we did not limit such length and so the limit is the array
         * size which is the max of Integer.
         */
        optionDefault(AsyncOption.DLG_LOCAL_MAXTOTLEN, Integer.MAX_VALUE);
        optionDefault(AsyncOption.DLG_REMOTE_MAXTOTLEN, Integer.MAX_VALUE);
        /* The upper layer will override it with a connect timeout. */
        optionDefault(AsyncOption.DLG_CONNECT_TIMEOUT, 10000);
        /* The upper layer will override it with a read timeout. */
        optionDefault(AsyncOption.DLG_HEARTBEAT_TIMEOUT, 10);
        optionDefault(AsyncOption.DLG_HEARTBEAT_INTERVAL, 1000);
        /*
         * Default the value to twice the heartbeat timeout. The value is used
         * for clean up unused connections, while the heartbeat timeout is used
         * to detect unstable connection. The upper layer should set it to a
         * value larger than the heartbeat timeout.
         */
        optionDefault(AsyncOption.DLG_IDLE_TIMEOUT, 20000);
        /*
         * The batching parameters are used to balance dialog fairness and
         * latency. Just default them to some reasonable values. The larger the
         * BATCHSZ, the more fair but longer latency. The product of BATCHSZ
         * and NBATCH decides the number of frames to write but yielding the
         * thread to do other works.
         */
        optionDefault(AsyncOption.DLG_FLUSH_BATCHSZ, 64);
        optionDefault(AsyncOption.DLG_FLUSH_NBATCH, 4);
    }

    private final Map<AsyncOption<?>, Object> options =
        new HashMap<AsyncOption<?>, Object>();
    private @Nullable SSLControl sslControl = null;
    private @Nullable Object configId = null;

    /**
     * Creates an instance of this class.
     */
    public EndpointConfigBuilder() {
    }

    /**
     * Sets the endpoint option.
     *
     * @param option the option
     * @param value the value
     * @return this builder
     *
     */
    public <T>
        EndpointConfigBuilder option(AsyncOption<T> option, T value) {

        options.put(option, value);
        return this;
    }

    /**
     * Sets the sslControl.
     *
     * @param sslCtrl the ssl control info
     * @return the builder
     */
    public EndpointConfigBuilder sslControl(SSLControl sslCtrl) {

        this.sslControl = sslCtrl;
        return this;
    }

    /**
     * Sets the configuration ID, which can be used to control sharing of
     * endpoints.
     *
     * @param cfgId the configuration ID
     * @return the builder
     */
    public EndpointConfigBuilder configId(Object cfgId) {
        configId = cfgId;
        return this;
    }

    /**
     * Builds the configuration.
     */
    public EndpointConfig build() {
        return new EndpointConfig(options, optionDefaults, sslControl,
                                  configId);
    }

    /**
     * Sets the endpoint option default value.
     */
    public static <T> void optionDefault(AsyncOption<T> option, T value) {
        final String propertyOverride = System.getProperty(
            DEFAULT_OVERRIDE_PROPERTY_PREFIX + option.name());
        if (propertyOverride != null) {
            value = option.parseStringValue(propertyOverride);
        }
        optionDefaults.put(option, value);
    }

    /**
     * Returns the endpoint option default value.
     *
     * @param option the option
     * @return the default value
     * @throws IllegalArgumentException if the option has no default value
     */
    public static <T> T getOptionDefault(AsyncOption<T> option) {
        if (!optionDefaults.containsKey(option)) {
            throw new IllegalArgumentException(
                "Option has no default value: " + option);
        }
        return option.type().cast(optionDefaults.get(option));
    }
}
