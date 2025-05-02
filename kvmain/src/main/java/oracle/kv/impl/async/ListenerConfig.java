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

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The configuration of a listener.
 */
public class ListenerConfig {

    private final ListenerPortRange portRange;
    private final String hostName;
    private final Map<AsyncOption<?>, Object> options =
        new HashMap<AsyncOption<?>, Object>();
    private final Map<AsyncOption<?>, Object> optionDefaults =
        new HashMap<AsyncOption<?>, Object>();
    private final EndpointConfig endpointConfig;

    ListenerConfig(ListenerPortRange portRange,
                   String hostName,
                   Map<AsyncOption<?>, Object> options,
                   Map<AsyncOption<?>, Object> optionDefaults,
                   EndpointConfig endpointConfig) {
        checkNull("portRange", portRange);
        checkNull("hostName", hostName);
        checkNull("options", options);
        checkNull("optionDefaults", optionDefaults);
        checkNull("endpointConfig", endpointConfig);
        this.portRange = portRange;
        this.hostName = hostName;
        this.options.putAll(options);
        this.optionDefaults.putAll(optionDefaults);
        this.endpointConfig = endpointConfig;
    }

    /**
     * Gets the port range.
     *
     * @return the port range.
     */
    public ListenerPortRange getPortRange() {
        return portRange;
    }

    /**
     * Gets the listening host name. If the value is "0.0.0.0", which is the
     * default, then the value represent an "any address" host, meaning that
     * listening should occur on all TCP networks. Specify the {@link
     * NetworkAddress#UNIX_DOMAIN_PREFIX} prefix on the hostname to use Unix
     * domain sockets.
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Gets the listener option value.
     *
     * @return the listener option
     */
    public <T> T getOption(AsyncOption<T> option) {
        if (!optionDefaults.containsKey(option)) {
            throw new IllegalArgumentException(
                    String.format(
                        "Not a suitable option for listener: %s", option));
        }
        Object value = options.get(option);
        if (value == null) {
            value = optionDefaults.get(option);
        }
        return option.type().cast(value);
    }

    /**
     * Gets endpoint configuration for accepted a connection.
     *
     * @return the endpoint configuration
     */
    public EndpointConfig getEndpointConfig() {
        return endpointConfig;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (! (obj instanceof ListenerConfig)) {
            return false;
        }

        ListenerConfig that = (ListenerConfig) obj;
        return (this.portRange.equals(that.portRange) &&
                this.options.equals(that.options) &&
                this.optionDefaults.equals(that.optionDefaults) &&
                this.endpointConfig.equals(that.endpointConfig));
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int hash = portRange.hashCode();
        hash = prime * hash + hostName.hashCode();
        hash = prime * hash + options.hashCode();
        hash = prime * hash + optionDefaults.hashCode();
        hash = prime * hash + endpointConfig.hashCode();
        return hash;
    }

    @Override
    public String toString() {
        return super.toString() + "[" +
            "portRange=" + portRange +
            " hostName=" + hostName +
            " options=" + options +
            " optionDefaults=" + optionDefaults +
            " endpointConfig=" + endpointConfig + "]";
    }
}
