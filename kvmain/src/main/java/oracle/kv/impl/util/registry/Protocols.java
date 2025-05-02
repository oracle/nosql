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

package oracle.kv.impl.util.registry;

import oracle.kv.KVStoreConfig;

/** Which network protocols to use for calls to the store. */
public enum Protocols {

    /** Use only RMI. */
    RMI_ONLY(true /* rmi */, false /* async */),

    /** Use only Async. */
    ASYNC_ONLY(false /* rmi */, true /* async */),

    /** Use RMI or Async. */
    RMI_AND_ASYNC(true /* rmi */, true /* async */);

    private final boolean rmi;
    private final boolean async;

    private Protocols(boolean rmi, boolean async) {
        this.rmi = rmi;
        this.async = async;
    }

    /**
     * Gets the protocols associated with the specified store configuration.
     *
     * @param config the store configuration
     * @return the associated protocols
     */
    public static Protocols get(KVStoreConfig config) {
        /*
         * Use RMI if async is disabled, regardless of the RMI setting. There
         * always needs to be some protocol available, so it is just a question
         * of which one to use if both are disabled. Because the ability to
         * disabling RMI is not public, give the public request to disable
         * async priority.
         */
        if (!config.getUseAsync()) {
            return RMI_ONLY;
        }
        return config.getUseRmi() ? RMI_AND_ASYNC : ASYNC_ONLY;
    }

    /**
     * Gets the protocols enabled by default.
     *
     * @return the default protocols
     */
    public static Protocols getDefault() {
        /* Use RMI if async is disabled, regardless of the RMI setting */
        if (!KVStoreConfig.getDefaultUseAsync()) {
            return RMI_ONLY;
        }
        return KVStoreConfig.getDefaultUseRmi() ? RMI_AND_ASYNC : ASYNC_ONLY;
    }

    /** Whether to use async. */
    public boolean useAsync() {
        return async;
    }

    /** Whether to use RMI. */
    public boolean useRmi() {
        return rmi;
    }
}

