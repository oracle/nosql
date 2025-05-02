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

package oracle.kv.impl.api.query;

import java.util.Set;

import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.query.ExecuteOptions;

public interface InternalStatement {

    StatementResult executeSync(KVStoreImpl store,
                                ExecuteOptions options);

    StatementResult executeSyncShards(
        KVStoreImpl store,
        ExecuteOptions options,
        Set<RepGroupId> shards);
}
