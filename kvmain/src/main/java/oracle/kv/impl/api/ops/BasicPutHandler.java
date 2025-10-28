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

import static oracle.kv.impl.security.KVStorePrivilegeLabel.DELETE_TABLE;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.WriteOptions;

/**
 * Base server handler for {@link Put} and its subclasses.
 */
abstract class BasicPutHandler<T extends Put>
        extends SingleKeyOperationHandler<T> {

    BasicPutHandler(OperationHandler handler,
                    OpCode opCode,
                    Class<T> operationType) {
        super(handler, opCode, operationType);
    }

    @Override
    void verifyDataAccess(T op)
        throws UnauthorizedException {

        /*
         * Check if the operation has a valid TTL. If so, this operation is
         * an implicit delete and requires DELETE_TABLE privilege.
         *
         * The check for null handles older clients that may not include a TTL.
         */
        if (op.getTTL() != null && op.getTTL().getValue() != 0) {
            super.verifyDataAccess(op, EnumSet.of(DELETE_TABLE));
        } else {
            super.verifyDataAccess(op);
        }
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
            new TablePrivilege.InsertTable(tableId));
    }

    @Override
    public List<? extends KVStorePrivilege>
    namespaceAccessPrivileges(String namespace) {
        return Collections.singletonList(
            new NamespacePrivilege.InsertInNamespace(namespace));
    }

    static OperationResult putEntry(Cursor cursor,
                                    DatabaseEntry keyEntry,
                                    DatabaseEntry dataEntry,
                                    com.sleepycat.je.Put op,
                                    WriteOptions jeOptions) {
        try {
            return cursor.put(keyEntry, dataEntry, op, jeOptions);
        } catch (IllegalArgumentException iae) {
            throw new WrappedClientException(iae);
        }
    }

    /*
     * Returns true if the server needs to copy MR counters. This is only
     * used by classes in this hierarchy. In this case MR counters may
     * be defined in schema in "normal" tables or in a JSON Collection
     * table.
     */
    protected static boolean getHasMRCounters(TableImpl table) {
        return table.hasAnyMRCounters();
    }
}
