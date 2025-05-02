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
import static oracle.kv.impl.util.ObjectUtil.safeEquals;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.test.TestStatus;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The configuration for an endpoint and its connection.
 */
public class EndpointConfig {

    private final Map<AsyncOption<?>, Object> options =
        new HashMap<AsyncOption<?>, Object>();
    private final Map<AsyncOption<?>, Object> optionDefaults =
        new HashMap<AsyncOption<?>, Object>();
    private final @Nullable SSLControl sslControl;

    /**
     * A unique identifier for this endpoint to allow callers to control
     * sharing endpoints with other callers.  Use null to represent the generic
     * identity that is not attempting to restrict sharing.
     */
    private final @Nullable Object configId;

    EndpointConfig(Map<AsyncOption<?>, Object> options,
                   Map<AsyncOption<?>, Object> optionDefaults,
                   @Nullable SSLControl sslControl,
                   @Nullable Object configId) {
        checkNull("options", options);
        checkNull("optionDefaults", optionDefaults);
        this.options.putAll(options);
        this.optionDefaults.putAll(optionDefaults);

        /*
         * For tests, make sure to use SO_REUSEADDR to avoid problems with
         * quickly reusing server socket addresses.
         */
        if (TestStatus.isActive()) {
            this.optionDefaults.put(AsyncOption.SO_REUSEADDR, true);
        }
        this.sslControl = sslControl;
        this.configId = configId;
    }

    /**
     * Gets the channel options.
     *
     * @return the channel options
     */
    public <T> T getOption(AsyncOption<T> option) {
        if (!optionDefaults.containsKey(option)) {
            throw new IllegalArgumentException(
                    String.format(
                        "Not a suitable option for endpoint: %s", option));
        }
        Object value = options.get(option);
        if (value == null) {
            value = optionDefaults.get(option);
        }
        return option.type().cast(value);
    }

    /**
     * Gets the ssl configuration.
     *
     * @return the ssl configuration, {@code null} if ssl is not required
     */
    public @Nullable SSLControl getSSLControl() {
        return sslControl;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (! (obj instanceof EndpointConfig)) {
            return false;
        }

        EndpointConfig that = (EndpointConfig) obj;
        return (this.options.equals(that.options) &&
                this.optionDefaults.equals(that.optionDefaults) &&
                safeEquals(this.sslControl, that.sslControl) &&
                safeEquals(this.configId, that.configId));
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int hash = options.hashCode();
        hash = prime * hash + optionDefaults.hashCode();
        final SSLControl sc = sslControl;
        if (sc != null) {
            hash = prime * hash + sc.hashCode();
        }
        final Object ci = configId;
        if (ci != null) {
            hash = prime * hash + ci.hashCode();
        }
        return hash;
    }

    @Override
    public String toString() {
        return String.format(
            "EndpointConfig[%s%s%s]",
            options,
            (sslControl == null) ? "" : " sslControl=" + sslControl,
            (configId == null) ? "" : " configId=" + configId);
    }
}
