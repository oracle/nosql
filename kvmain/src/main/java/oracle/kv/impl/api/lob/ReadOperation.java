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

package oracle.kv.impl.api.lob;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Key;
import oracle.kv.impl.api.KVStoreImpl;

/**
 * Superclass of all LOB read operations
 */
public class ReadOperation extends Operation {

    protected final Consistency consistency;

    public ReadOperation(KVStoreImpl kvsImpl,
                         Key appLobKey,
                         Consistency consistency,
                         long chunkTimeout,
                         TimeUnit timeoutUnit) {

        super(kvsImpl, appLobKey, chunkTimeout, timeoutUnit);

        this.consistency = (consistency == null) ?
            kvsImpl.getDefaultConsistency() : consistency;
    }

    public Consistency getConsistency() {
        return consistency;
    }
}
