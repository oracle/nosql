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

/**
 * Control whether to use asynchronous services.  This class is separate so
 * that it can be used by clients even if other async classes are not
 * available.
 */
public class AsyncControl {

    protected AsyncControl() {
        throw new AssertionError();
    }

    /**
     * The name of the system property used to set {@link #serverUseAsync}.
     */
    public static final String SERVER_USE_ASYNC = "oracle.kv.async.server";

    /**
     * Whether to enable async by default.
     */
    private static final boolean serverUseAsyncDefault = true;

    /**
     * If true, enables support for async requests on the server side,
     * including sharing listening operations with the dialog layer.
     */
    public static volatile boolean serverUseAsync =
        (System.getProperty(SERVER_USE_ASYNC) == null) ?
        serverUseAsyncDefault :
        Boolean.getBoolean(SERVER_USE_ASYNC);
}
