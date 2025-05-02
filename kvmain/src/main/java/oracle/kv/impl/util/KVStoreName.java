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

package oracle.kv.impl.util;

/**
 * Manages information about the store name of services launched in the current
 * JVM.
 */
public class KVStoreName {

    private KVStoreName() { }

    /** The latest store name or null. Has default access for testing. */
    static volatile String latestKVStoreName;

    /**
     * Notes the store name associated with this service. Every service should
     * call this method when it discovers the name of the store that it is
     * associated with.
     *
     * @param name the kvstore name
     */
    public static void noteKVStoreName(String name) {
        if (name != null) {
            latestKVStoreName = name;
        }
    }

    /**
     * Returns the name of the store associated with the most recently started
     * ConfigurableService. Returns null if no service has registered the name
     * of its kvstore.
     *
     * @return the KVStore name or null
     * @see ConfigurableService#noteKVStoreName
     */
    public static String getKVStoreName() {
        return latestKVStoreName;
    }
}
