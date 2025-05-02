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

import oracle.kv.impl.api.ops.InternalOperation.OpCode;

/**
 * Base server handler for subclasses of TableIterateOperation.
 */
abstract class TableIterateOperationHandler<T extends TableIterateOperation>
        extends MultiTableOperationHandler<T> {

    TableIterateOperationHandler(OperationHandler handler,
                                 OpCode opCode,
                                 Class<T> operationType) {
        super(handler, opCode, operationType);
    }

    static boolean exceedsMaxReadKB(TableIterateOperation op,
                                    int currentReadBytes) {
        if (op.getMaxReadKB() > 0) {
            return (op.getReadKB() +
                    op.getReadKBToAdd(currentReadBytes)) >
                op.getMaxReadKB();
        }
        return false;
    }
}
