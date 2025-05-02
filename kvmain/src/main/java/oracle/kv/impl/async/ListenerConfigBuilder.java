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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.util.HashMap;
import java.util.Map;

/**
 * A builder to build the listener configuration.
 */
public class ListenerConfigBuilder {

    /**
     * Allow overriding defaults by setting a system property with the
     * specified prefix followed by the option name.
     */
    private static final String DEFAULT_OVERRIDE_PROPERTY_PREFIX =
        "oracle.kv.async.listener.config.default.";

    private static final Map<AsyncOption<?>, Object> optionDefaults =
        new HashMap<AsyncOption<?>, Object>();

    static {
        optionDefault(AsyncOption.SO_RCVBUF, null);
        optionDefault(AsyncOption.SO_REUSEADDR, true);
        optionDefault(AsyncOption.SSO_BACKLOG, 0);
        optionDefault(
            AsyncOption.DLG_ACCEPT_MAX_ACTIVE_CONNS, Integer.MAX_VALUE);
        optionDefault(
            AsyncOption.DLG_CLEAR_BACKLOG_INTERVAL, 100 /* 100 ms */);
    }

    private ListenerPortRange portRange = ListenerPortRange.ANY_PORT_RANGE;
    private String hostName = "0.0.0.0";
    private final Map<AsyncOption<?>, Object> options =
        new HashMap<AsyncOption<?>, Object>();
    private EndpointConfigBuilder endpointConfigBuilder =
        new EndpointConfigBuilder()

        /*
         * Use the listener socket's value for SO_REUSEADDR for the accepted
         * channel, which is what sockets and NIO channels do by default. If we
         * have SO_REUSEADDR enabled for the server socket, we can still run
         * into a conflict on Linux with a previous socket in TIME_WAIT state
         * if the server-side socket is configured with SO_REUSEADDR disabled.
         */
        .option(AsyncOption.SO_REUSEADDR,
                getOptionDefault(AsyncOption.SO_REUSEADDR));

    /**
     * Creates an instance of this class.
     */
    public ListenerConfigBuilder() {
    }

    /**
     * Sets the port range.
     *
     * @param range the port range
     * @return this builder
     */
    public ListenerConfigBuilder portRange(ListenerPortRange range) {
        portRange = range;
        return this;
    }

    /**
     * Sets the listening host name. If not called, defaults to "0.0.0.0",
     * which specifies using any address to listen on all TCP networks.
     *
     * @param newHostName the listening host name
     * @return this builder
     */
    public ListenerConfigBuilder hostName(String newHostName) {
        hostName = checkNull("newHostName", newHostName);
        return this;
    }

    /**
     * Sets the listener option.
     *
     * <p>For the SO_REUSEADDR option, also sets the option on the endpoint
     * config builder.
     *
     * @param option the option
     * @param value the value
     * @return this builder
     */
    public <T>
        ListenerConfigBuilder option(AsyncOption<T> option, T value) {

        options.put(option, value);
        if (option == AsyncOption.SO_REUSEADDR) {
            endpointConfigBuilder.option(option, value);
        }
        return this;
    }

    /**
     * Sets the builder for endpoint configuration.
     *
     * @param builder the endpoint configuration builder
     * @return this builder
     */
    public ListenerConfigBuilder
        endpointConfigBuilder(EndpointConfigBuilder builder) {

        endpointConfigBuilder = builder;
        return this;
    }

    /**
     * Returns the builder for the endpoint configuration.
     *
     * @return the endpoint builder
     */
    public EndpointConfigBuilder getEndpointConfigBuilder() {
        return endpointConfigBuilder;
    }

    /**
     * Builds the configuration.
     */
    public ListenerConfig build() {
        return new ListenerConfig(portRange, hostName, options, optionDefaults,
                                  endpointConfigBuilder.build());
    }

    /**
     * Sets the listener option default value.
     */
    private static <T> void optionDefault(AsyncOption<T> option, T value) {
        final String propertyOverride = System.getProperty(
            DEFAULT_OVERRIDE_PROPERTY_PREFIX + option.name());
        if (propertyOverride != null) {
            value = option.parseStringValue(propertyOverride);
        }
        optionDefaults.put(option, value);
    }

    /**
     * Returns the listener option default value.
     */
    private static <T> T getOptionDefault(AsyncOption<T> option) {
        if (!optionDefaults.containsKey(option)) {
            throw new IllegalArgumentException(
                "Option has no default value: " + option);
        }
        return option.type().cast(optionDefaults.get(option));
    }
}
