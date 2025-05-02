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
import oracle.kv.impl.security.TablePrivilege;

/**
 * Base server handler for {@link Delete} and its subclasses.
 */
abstract class BasicDeleteHandler<T extends Delete>
        extends SingleKeyOperationHandler<T> {

    BasicDeleteHandler(OperationHandler handler,
                       OpCode opCode,
                       Class<T> operationClass) {
        super(handler, opCode, operationClass);
    }

    @Override
    List<? extends KVStorePrivilege> schemaAccessPrivileges() {
        return SystemPrivilege.schemaWritePrivList;
    }

    @Override
    List<? extends KVStorePrivilege> generalAccessPrivileges() {
        return SystemPrivilege.writeOnlyPrivList;
    }

    @Override
    public List<? extends KVStorePrivilege>
        tableAccessPrivileges(long tableId) {
        return Collections.singletonList(
            new TablePrivilege.DeleteTable(tableId));
    }

    @Override
    public List<? extends KVStorePrivilege>
    namespaceAccessPrivileges(String namespace) {
        return Collections.singletonList(
            new NamespacePrivilege.DeleteInNamespace(namespace));
    }
}
