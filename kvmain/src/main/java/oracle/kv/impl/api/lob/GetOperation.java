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

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Key;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.lob.KVLargeObject.LOBState;

/**
 * Implements the LOB get operation
 */
public class GetOperation extends ReadOperation {

    GetOperation(KVStoreImpl kvsImpl,
                 Key appLobKey,
                 Consistency consistency,
                 long chunkTimeout,
                 TimeUnit timeoutUnit) {

        super(kvsImpl, appLobKey, consistency, chunkTimeout, timeoutUnit);
    }

    InputStreamVersion execute() {

        final ValueVersion appValueVersion =
                kvsImpl.get(appLOBKey, consistency,
                            chunkTimeoutMs, TimeUnit.MILLISECONDS);

        if (appValueVersion == null) {
            return null;
        }

        internalLOBKey = valueToILK(appValueVersion.getValue());

        final ValueVersion metadataVV =
            initMetadata(consistency, LOBState.COMPLETE);

        final long lastSuperChunkId = lobProps.getLastSuperChunkId();
        if ((lastSuperChunkId > 1) &&
            (consistency instanceof Consistency.Version)) {
            String msg = "Version consistency cannot be used to read " +
            		"a LOB striped across more than one partition. " +
            		"This LOB is striped across " + lastSuperChunkId +
            		"partitions. Use a different consistency policy";
            throw new IllegalArgumentException(msg);
        }

        final InputStream inputStream = new
            ChunkEncapsulatingInputStream(this,
                                          metadataVV.getVersion());
        return new InputStreamVersion(inputStream,
                                      appValueVersion.getVersion());
    }
}
