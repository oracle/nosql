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

package oracle.kv.impl.api.ops;

import java.util.List;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link NOP}.
 */
class NOPHandler extends InternalOperationHandler<NOP> {

    NOPHandler(OperationHandler handler) {
        super(handler, OpCode.NOP, NOP.class);
    }

    @Override
    Result execute(NOP op,
                   Transaction txn,
                   PartitionId partitionId) {
        return new Result.NOPResult();
    }

    @Override
    List<KVStorePrivilege> getRequiredPrivileges(NOP op) {
        /* NOP needs no privilege */
        return emptyPrivilegeList;
    }
}
