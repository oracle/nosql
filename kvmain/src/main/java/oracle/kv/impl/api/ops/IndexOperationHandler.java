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

import oracle.kv.FaultException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.InternalOperationHandler.PrivilegedTableAccessor;
import oracle.kv.impl.api.ops.MultiTableOperationHandler.TargetTableAccessChecker;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.table.Index;
import oracle.kv.table.Table;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

/**
 * Base server handler for subclasses of {@link IndexOperation}.
 */
public abstract class IndexOperationHandler<T extends IndexOperation>
        extends InternalOperationHandler<T>
    implements PrivilegedTableAccessor {

    IndexOperationHandler(OperationHandler handler,
                          OpCode opCode,
                          Class<T> operationType) {
        super(handler, opCode, operationType);
    }

    public IndexScanner getIndexScanner(T op,
                                        Transaction txn,
                                        CursorConfig cursorConfig,
                                        LockMode lockMode,
                                        boolean keyOnly,
                                        boolean moveAfterResumeKey) {
        final Table table = getTable(op);
        return new IndexScanner(op,
                                txn,
                                getSecondaryDatabase(
                                    ((TableImpl)table).getInternalNamespace(),
                                    table.getFullName(),
                                    op.getIndexName()),
                                getIndex(op,
                                         ((TableImpl)table).getInternalNamespace(),
                                         table.getFullName()),
                                op.getIndexRange(),
                                op.getResumeSecondaryKey(),
                                op.getResumePrimaryKey(),
                                moveAfterResumeKey,
                                cursorConfig,
                                lockMode,
                                keyOnly);
    }

    String getTableName(T op) {
        long id = op.getTargetTables().getTargetTableId();
        Table table = getRepNode().getTable(id);
        if (table == null) {
            throw new MetadataNotFoundException
                ("Cannot access table.  It may not exist, id: " + id,
                 operationHandler.getTableMetadataSeqNum());
        }
        return table.getFullName();
    }

    Table getTable(T op) {
        long id = op.getTargetTables().getTargetTableId();
        Table table = getRepNode().getTable(id);
        if (table == null) {
            throw new MetadataNotFoundException
                ("Cannot access table.  It may not exist, id: " + id,
                 operationHandler.getTableMetadataSeqNum());
        }
        return table;
    }

    IndexImpl getIndex(T op, String namespace, String tableName) {
        final Index index =
            getRepNode().getIndex(namespace, op.getIndexName(), tableName);
        if (index == null) {
            throw new MetadataNotFoundException
                ("Cannot find index " + op.getIndexName() + " in table "
                 + tableName, operationHandler.getTableMetadataSeqNum());
        }

        return (IndexImpl) index;
    }

    public SecondaryDatabase getSecondaryDatabase(String namespace,
                                                  String tableName,
                                                  String indexName) {
        final SecondaryDatabase db =
            getRepNode().getIndexDB(namespace,
                                    indexName,
                                    tableName);
        if (db == null) {
            throw new MetadataNotFoundException("Cannot find index database: " +
                indexName + ", " + tableName,
                operationHandler.getTableMetadataSeqNum());
        }
        return db;
    }

    public void verifyTableAccess(T op)
        throws UnauthorizedException, FaultException {
        verifyTableAccess(op.getTargetTables());
    }

    public void verifyTableAccess(TargetTables tables)
        throws UnauthorizedException, FaultException {

        if (ExecutionContext.getCurrent() == null) {
            return;
        }

        new TargetTableAccessChecker(operationHandler, this, tables).
            checkAccess();
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(T op) {
        /*
         * Checks the basic privilege for authentication here, and leave the
         * the table access checking in {@code verifyTableAccess()}.
         */
        return SystemPrivilege.usrviewPrivList;
    }

    @Override
    public List<? extends KVStorePrivilege>
        tableAccessPrivileges(long tableId) {
        return Collections.singletonList(
            new TablePrivilege.ReadTable(tableId));
    }

    static boolean exceedsMaxReadKB(IndexOperation op) {
        if (op.getMaxReadKB() > 0) {
            return op.getReadKB() > op.getMaxReadKB();
        }
        return false;
    }

    @Override
    public List<? extends KVStorePrivilege>
    namespaceAccessPrivileges(String namespace) {
        return Collections.singletonList(
            new NamespacePrivilege.ReadInNamespace(namespace));
    }
}
