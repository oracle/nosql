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

import java.util.Collections;
import java.util.List;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.SystemPrivilege;

/**
 * Base server handler for subclasses of MultiKeyIterate.
 */
abstract class MultiKeyIterateHandler<T extends MultiKeyIterate>
        extends MultiKeyOperationHandler<T> {

    MultiKeyIterateHandler(OperationHandler handler,
                           OpCode opCode,
                           Class<T> operationType) {
        super(handler, opCode, operationType);
    }

    @Override
    List<? extends KVStorePrivilege> schemaAccessPrivileges() {
        return SystemPrivilege.schemaReadPrivList;
    }

    @Override
    List<? extends KVStorePrivilege> generalAccessPrivileges() {
        return SystemPrivilege.readOnlyPrivList;
    }

    @Override
    public List<? extends KVStorePrivilege>
        tableAccessPrivileges(long tableId) {
        return tableReadPrivileges(tableId);
    }

    @Override
    public List<? extends KVStorePrivilege>
        namespaceAccessPrivileges(String namespace) {
        return Collections.singletonList(
            new NamespacePrivilege.ReadInNamespace(namespace));
    }
}
