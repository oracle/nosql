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

import java.util.EnumSet;
import java.util.List;

import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.InternalOperationHandler.Keyspace.KeyspaceType;
import oracle.kv.impl.api.ops.InternalOperationHandler.PrivilegedTableAccessor;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.security.AccessCheckUtils;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;

/**
 * Base server handler for {@link SingleKeyOperation} subclasses.
 */
abstract class SingleKeyOperationHandler<T extends SingleKeyOperation>
        extends InternalOperationHandler<T>
        implements PrivilegedTableAccessor {

    SingleKeyOperationHandler(OperationHandler operationHandler,
                              OpCode opCode,
                              Class<T> operationType) {
        super(operationHandler, opCode, operationType);
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(T op) {
        final KeyspaceType keyspace =
            Keyspace.identifyKeyspace(op.getKeyBytes());
        switch (keyspace) {
        case PRIVATE:
            return SystemPrivilege.internalPrivList;
        case SCHEMA:
            return schemaAccessPrivileges();
        case GENERAL:
            /*
             * Checks only the basic privilege for authentication, and let
             * finer-grain table access checking be performed in execution of
             * each operation.
             */
            return SystemPrivilege.usrviewPrivList;
        default:
            throw new AssertionError();
        }
    }

    /**
     * Verifies the data access in general keyspace for operation.  If the
     * tableId for the operation is non-zero, table existence will be checked
     * first. Then if the security is enabled, the legitimacy of data access
     * will be checked further:
     * <p>
     * 1. if tableId is non-zero, the access privileges on the table specified
     * by the id are needed;<br>
     * 2. if tableId is zero but the key falls in a table's keyspace, the
     * access privileges on the table are needed;<br>
     * 3. if tableId is zero and the key is not in any table keyspace, the
     * access privileges on general keyspace are needed;
     * <p>
     * Here we only checks if current session has the required access
     * privileges for efficiency, since the authentication checking and subject
     * identification have been done while processing the request.
     *
     * @param op the operation
     * @throws UnauthorizedException if the permission check for data access
     * fails.
     */
    void verifyDataAccess(T op)
        throws UnauthorizedException {

        verifyDataAccess(op, null);
    }

    /**
     * Verifies the data access in general keyspace for operation.  If the
     * tableId for the operation is non-zero, table existence will be checked
     * first. Then if the security is enabled, the legitimacy of data access
     * will be checked further:
     * <p>
     * 1. if tableId is non-zero, the access privileges on the table specified
     * by the id are needed;<br>
     * 2. if tableId is zero but the key falls in a table's keyspace, the
     * access privileges on the table are needed;<br>
     * 3. if tableId is zero and the key is not in any table keyspace, the
     * access privileges on general keyspace are needed;
     * <p>
     * If it's a table operation and requires additional table privileges to
     * perform, we also check if current session has required access privileges.
     *
     * Here we only check if current session has the required access
     * privileges for efficiency, since the authentication checking and subject
     * identification have been done while processing the request.
     *
     * @param op the operation
     * @param tablePrivs the additional required table privileges. Note that the
     * table privileges specified should be able to imply the same privileges
     * in the generalAccessPrivileges of the operation.
     * @throws UnauthorizedException if the permission check for data access
     * fails.
     */
    void verifyDataAccess(T op, EnumSet<KVStorePrivilegeLabel> tablePrivs)
        throws UnauthorizedException {

        TableImpl accessedTable = null;

        /* For table operation, check if table exists */
        final long tableId = op.getTableId();
        if (tableId != 0) {
            accessedTable = getAndCheckTable(tableId);
        }

        /*
         * Non-secure kvstore,  or if the key is not in general keyspace, or
         * has full access privileges on general keyspace, just skip further
         * permission checking.
         */
        final ExecutionContext exeCtx = ExecutionContext.getCurrent();
        if (exeCtx == null || !Keyspace.isGeneralAccess(op.getKeyBytes())) {
            return;
        }

        if (exeCtx.hasAllPrivileges(generalAccessPrivileges())) {
            /* Verify if accessing system tables via key-value API */
            if (tableId == 0) {
                accessedTable = findTableByKeyBytes(op.getKeyBytes());
            }

            if (accessedTable != null) {
                verifySystemTableAccess(accessedTable);
            }
            return;
        }

        /* Operation via key-value API, checks if it is in any table keyspace */
        if (tableId == 0) {
            accessedTable = findTableByKeyBytes(op.getKeyBytes());

        } else {
            /* Verify the table id that was passed in */
            if (accessedTable != null) {
                verifyTargetTable(op, accessedTable);
            }
        }

        /*
         * Not in table keyspace, checking fails since current session does
         * not have full access privilege on general keyspace.
         */
        if (accessedTable == null) {
            throw new UnauthorizedException(
                "Insufficient access rights granted");
        }

        /*
         * Table keyspace, checks ownership first
         */
        if (AccessCheckUtils.currentUserOwnsResource(accessedTable)) {
            return;
        }

        /* Check the privileges on the namespace */
        if ( !accessedTable.isSystemTable() && exeCtx.hasAllPrivileges(
            namespaceAccessPrivileges(accessedTable.getInternalNamespace()))) {

            /*
             * The operation may require additional privileges, verify if
             * current session has the required privileges.
             */
            if (tablePrivs != null) {
                verifyTablePrivileges(accessedTable, tablePrivs);
            }
            return;
        }

        /*
         * Do a first check here to fail quickly if the basic table access
         * privileges are not met.
         */
        if (!exeCtx.hasAllPrivileges(
                tableAccessPrivileges(accessedTable.getId()))) {
            throw new UnauthorizedException(
                "Insufficient access rights granted on table, id: " +
                accessedTable.getId() +
                " name: " + accessedTable.getFullNamespaceName()
            );
        }

        /* Ensure at least read privileges on all parent tables */
        TableImpl parent = (TableImpl) accessedTable.getParent();
        while (parent != null) {
            final long pTableId = parent.getId();
            final TablePrivilege parentReadPriv =
                new TablePrivilege.ReadTable(pTableId);

            if (!exeCtx.hasPrivilege(parentReadPriv) &&
                !exeCtx.hasAllPrivileges(tableAccessPrivileges(pTableId))) {
                throw new UnauthorizedException(
                    "Insufficient access rights on parent table, id: " +
                    pTableId);
            }
            parent = (TableImpl) parent.getParent();
        }

        /*
         * Current session may have general access on tables, verify system
         * table access.
         */
        verifySystemTableAccess(accessedTable);

        /*
         * The operation may require additional privileges, verify if
         * current session has the required privileges.
         */
        if (tablePrivs != null) {
            verifyTablePrivileges(accessedTable, tablePrivs);
        }
    }

    void reserializeResultValue(SingleKeyOperation op, ResultValueVersion rvv) {
        reserializeResultValue(op, rvv, operationHandler);

    }

    /**
     * Reserializes the ResultValueVersion.valueBytes if needed, update the
     * valueBytes of ResultValueVersion if re-serialization is needed and
     * done.
     */
    static void reserializeResultValue(SingleKeyOperation op,
                                       ResultValueVersion rvv,
                                       OperationHandler operationHandler) {

        if (op.getTableId() == 0 || rvv == null) {
            return;
        }
        byte[] valBytes = operationHandler.reserializeToOldValue(
            op.getTableId(), op.getKeyBytes(), rvv.getValueBytes(),
            op.getOpSerialVersion());

        rvv.setValueBytes(valBytes);
    }

    /**
     * Verifies that the table Id that was passed in matches the target table
     * in the key.
     *
     * This method is used as a part of table access checking. For performance
     * reasons, the target table id is specified in the request. The target
     * table in the request can be forged by a malicious user and must
     * therefore be verified. This method performs the verification.
     *
     * @param op the operation
     *
     * @param tableIdTable the table identified by the tableId
     */
    private void verifyTargetTable(T op, TableImpl tableIdTable) {

        final TableImpl keyTargetTable = findTableByKeyBytes(op.getKeyBytes());
        if (keyTargetTable == null) {
            throw new UnauthorizedException("Key does not identify a table." +
                " Expected to find table id:" + tableIdTable.getId() +
                "(" + tableIdTable.getFullName() + ")");
        }

        if (keyTargetTable.getId() != tableIdTable.getId()) {
            throw new UnauthorizedException(
                "Request table id:" + tableIdTable.getId() +
                "differs from table id in key:" +
                    keyTargetTable.getId() +
                    "(" + keyTargetTable.getFullName() + ")");
        }
    }

    /**
     * Verify current access is allowed if given table is a system table.
     */
    private void verifySystemTableAccess(TableImpl table) {
        if (table.isSystemTable() && !allowAccessSystemTables()) {
            throw new UnauthorizedException(
                "Operation is not permitted on system tables");
        }
    }

    /**
     * Returns the required privileges for Avro schema keyspace access.
     */
    abstract List<? extends KVStorePrivilege> schemaAccessPrivileges();

    /**
     * Returns the required privileges for accessing full general keyspaces
     * except the schema keyspace and server private keyspace.
     */
    abstract List<? extends KVStorePrivilege> generalAccessPrivileges();
}
