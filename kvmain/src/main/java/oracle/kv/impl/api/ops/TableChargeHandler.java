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

import com.sleepycat.je.Transaction;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.topo.PartitionId;

import java.util.List;

/**
 * Server handler for {@link TableCharge}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * | TableCharge   |   N/A  |N/A|      Read units      |           0           |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
public class TableChargeHandler
        extends InternalOperationHandler<TableCharge> {

    TableChargeHandler(OperationHandler handler) {
        super(handler, OpCode.TABLE_CHARGE, TableCharge.class);
    }

    @Override
    Result execute(TableCharge op,
                   Transaction txn,
                   PartitionId partitionId) {
        op.addReadUnits(op.getReadUnits());
        return new Result.TableChargeResult();
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(TableCharge op) {
        return emptyPrivilegeList;
    }
}
