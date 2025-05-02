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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.InternalOperationHandler.Keyspace.KeyAccessChecker;
import oracle.kv.impl.api.ops.InternalOperationHandler.Keyspace.KeyspaceType;
import oracle.kv.impl.api.ops.InternalOperationHandler.PrivilegedTableAccessor;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;

/**
 * A base server handler for subclasses of MultiKeyOperation.
 *
 * Throughput calculation for both iterators
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * | MultiKeyOper..|  N/A   | - | sum of record sizes  |           0           |
 * +---------------------------------------------------------------------------+
 */
abstract class MultiKeyOperationHandler<T extends MultiKeyOperation>
        extends InternalOperationHandler<T>
        implements PrivilegedTableAccessor {

    private static final KVAuthorizer UNIVERSAL_AUTHORIZER =
        new KVAuthorizer() {
            @Override
            public boolean allowAccess(DatabaseEntry keyEntry) {
                return true;
            }

            @Override
            public boolean allowFullAccess() {
                return true;
            }
        };

    MultiKeyOperationHandler(OperationHandler handler,
                             OpCode opCode,
                             Class<T> operationType) {
        super(handler, opCode, operationType);
    }

    /**
     * Iterate and return the next batch.
     *
     * @return the key at which to resume the iteration, or null if complete.
     */
    boolean iterate(MultiKeyOperation op,
                    Transaction txn,
                    PartitionId partitionId,
                    byte[] parentKey,
                    boolean majorPathComplete,
                    KeyRange subRange,
                    Depth depth,
                    Direction direction,
                    int batchSize,
                    byte[] resumeKey,
                    CursorConfig cursorConfig,
                    final List<ResultKeyValueVersion> results,
                    final KVAuthorizer auth,
                    final boolean excludeTombstones) {

        Scanner scanner = new Scanner(op, txn,
                                      partitionId,
                                      getRepNode(),
                                      parentKey,
                                      majorPathComplete,
                                      subRange,
                                      depth,
                                      direction,
                                      resumeKey,
                                      true, /*moveAfterResumeKey*/
                                      cursorConfig,
                                      LockMode.DEFAULT,
                                      false); /* not key-only */

        DatabaseEntry keyEntry = scanner.getKey();
        DatabaseEntry dataEntry = scanner.getData();
        Cursor cursor = scanner.getCursor();
        boolean moreElements;

        /*
         * When doing a scan that includes the parent key the parent is handled
         * separately from the descendants. The parent key does not make a
         * valid resume key, so if the batch size is 1, increase it to ensure
         * that the parent key is not the resume key.
         */
        if (resumeKey == null && batchSize == 1) {
            ++batchSize;
        }
        try {
            while ((moreElements = scanner.next()) == true) {
                if (!auth.allowAccess(keyEntry)) {

                    /*
                     * The requestor is not permitted to see this entry,
                     * so silently skip it.
                     */
                    continue;
                }
                if (excludeTombstones && scanner.getResult().isTombstone()) {
                    /*
                     * This entry is a tombstone, so skip it.
                     */
                    continue;
                }

                final ResultValueVersion valVers =
                    makeValueVersion(cursor, dataEntry, scanner.getResult());

                results.add(new ResultKeyValueVersion(
                                keyEntry.getData(),
                                valVers.getValueBytes(),
                                valVers.getVersion(),
                                valVers.getExpirationTime(),
                                valVers.getModificationTime(),
                                false /* isTombstone */));

                if (batchSize != 0 && results.size() >= batchSize) {
                    break;
                }
            }
        } finally {
            scanner.close();

        }
        return moreElements;
    }

    /**
     * Iterate and return the next batch.
     *
     * @return the key at which to resume the iteration, or null if complete.
     */
    boolean iterateKeys(MultiKeyOperation op,
                        Transaction txn,
                        PartitionId partitionId,
                        byte[] parentKey,
                        boolean majorPathComplete,
                        KeyRange subRange,
                        Depth depth,
                        Direction direction,
                        int batchSize,
                        byte[] resumeKey,
                        CursorConfig cursorConfig,
                        final List<ResultKey> results,
                        final KVAuthorizer auth,
                        final boolean excludeTombstones) {

        Scanner scanner = new Scanner(op, txn,
                                      partitionId,
                                      getRepNode(),
                                      parentKey,
                                      majorPathComplete,
                                      subRange,
                                      depth,
                                      direction,
                                      resumeKey,
                                      true, /*moveAfterResumeKey*/
                                      cursorConfig,
                                      LockMode.DEFAULT,
                                      true); /* key-only */

        DatabaseEntry keyEntry = scanner.getKey();
        boolean moreElements;
        /*
         * When doing a scan that includes the parent key the parent is handled
         * separately from the descendants. The parent key does not make a
         * valid resume key, so if the batch size is 1, increase it to ensure
         * that the parent key is not the resume key.
         */
        if (resumeKey == null && batchSize == 1) {
            ++batchSize;
        }
        try {
            while ((moreElements = scanner.next()) == true) {
                if (!auth.allowAccess(keyEntry)) {

                    /*
                     * The requestor is not permitted to see this entry,
                     * so silently skip it.
                     */
                    continue;
                }
                if (!scanner.getCurrent() && excludeTombstones) {
                    /*
                     * This entry is a tombstone, so skip it.
                     */
                    continue;
                }
                op.addReadBytes(getStorageSize(scanner.getCursor()));
                addKeyResult(results,
                             keyEntry.getData(),
                             scanner.getExpirationTime());
                if (batchSize != 0 && results.size() >= batchSize) {
                    break;
                }
            }
        } finally {
            scanner.close();

        }
        return moreElements;
    }

    /**
     * When the parent key is null or too short to determine the keyspace it
     * may access, returns a KVAuthorizer instance that will determine on a
     * per-KV entry basis whether entries are visible to the user. This
     * implements a policy that allows callers to iterate over the store
     * without generating errors if they come across something that they aren't
     * allowed access to.
     */
    KVAuthorizer checkPermission(T op) {
        return checkPermission(op.getParentKey());
    }

    KVAuthorizer checkPermission(byte[] key) {

        /* If security is not enabled, returns a fully accessible checker */
        if (ExecutionContext.getCurrent() == null) {
            return UNIVERSAL_AUTHORIZER;
        }

        final Set<KeyAccessChecker> checkers = new HashSet<>();

        /* Checks if access server private keyspace is legal */
        if (!isInternalRequestor()) {
            if (key == null ||
                Keyspace.mayBePrivateAccess(key)) {
                checkers.add(Keyspace.privateKeyAccessChecker);
            }
        }

        /* Checks if access Avro schema keyspace is legal */
        if (!hasSchemaAccessPrivileges()) {
            if (key == null ||
                Keyspace.mayBeSchemaAccess(key)) {
                checkers.add(Keyspace.schemaKeyAccessChecker);
            }
        }

        /*
         * Checks if access general keyspace is legal.  When building the
         * execution context, the possibility of accessing exact private or
         * schema keyspace has been check. So at here, we know that we will
         * possibly access the general keyspace. If users do not have a full
         * access to general keyspace, the table access checker will be added
         * to allow table-specific permission checking.
         */
        if (!hasGeneralAccessPrivileges()) {
            checkers.add(new TableAccessChecker(operationHandler, this));
        } else {
            /*
             * Check if accessing system tables is legal when current execution
             * context has general access privileges. Only read access is
             * allowed for client request.
             */
            checkers.add(new SysTableAccessChecker(operationHandler));
        }

        if (checkers.isEmpty()) {
            /*
             * Entries either cannot possible fall into the server private and
             * schema key space, or else we have an legal keyspace requestor,
             * so each access is guaranteed to be authorized.
             */
            return UNIVERSAL_AUTHORIZER;
        }

        /*
         * We have a user-level requestor, and either the parent key is null or
         * the parent key is not null, but is too short to be sure that no
         * illegal access will result, so entries could possibly fall into the
         * server private or schema key space.  Return an authorizer that will
         * check keys on each access.
         */
        return new KeyspaceAccessAuthorizer(checkers);
    }

    /**
     * Checks whether the requestor has required privileges for schema access.
     */
    boolean hasSchemaAccessPrivileges() {
        final ExecutionContext currentContext = ExecutionContext.getCurrent();
        if (currentContext == null) {
            return true;
        }
        return currentContext.hasAllPrivileges(schemaAccessPrivileges());
    }

    /**
     * Checks whether the requestor has required privileges for general access.
     */
    boolean hasGeneralAccessPrivileges() {
        final ExecutionContext currentContext = ExecutionContext.getCurrent();
        if (currentContext == null) {
            return true;
        }
        return currentContext.hasAllPrivileges(generalAccessPrivileges());
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(T op) {
        /*
         * If the parent key exactly falls in schema, private or general
         * keyspace, we can quickly check whether the user has the required
         * privilege.
         */
        final byte[] parentKey = op.getParentKey();
        if (parentKey != null) {
            final KeyspaceType keyspace = Keyspace.identifyKeyspace(parentKey);
            switch (keyspace) {
            case PRIVATE:
                return SystemPrivilege.internalPrivList;
            case SCHEMA:
                return schemaAccessPrivileges();
            case GENERAL:
                if (!Keyspace.mayBePrivateAccess(parentKey) &&
                    !Keyspace.mayBeSchemaAccess(parentKey)) {

                    /* Access exactly the general space, checks only the basic
                     * privilege for authentication, and let per-key checking
                     * be performed in each iteration.
                     */
                    return SystemPrivilege.usrviewPrivList;
                }
                break;
            default:
                throw new AssertionError();
            }
        }
        /*
         * The key is null or is too short to determine, we just check the
         * basic authentication here and defer the per-key checking to
         * iteration.
         */
        return SystemPrivilege.usrviewPrivList;
    }

    /**
     * Returns the required privileges for Avro schema keyspace access.
     */
    abstract List<? extends KVStorePrivilege> schemaAccessPrivileges();

    /**
     * Returns the required privileges for accessing the whole store keyspace
     * outside the schema and the server private keyspaces.
     */
    abstract List<? extends KVStorePrivilege> generalAccessPrivileges();

    /**
     * An authorizer for checking the permission of keyspace access of KVStore.
     * Currently, we implement the checking for the server-private keyspace,
     * the Avro schema keyspace, the table keyspace and non-table general
     * keyspace.
     */
    private static class KeyspaceAccessAuthorizer implements KVAuthorizer {

        private final Set<KeyAccessChecker> keyCheckers;

        /**
         * Constructs a KeyspaceAccessAuthorizer
         *
         * @param keyCheckers set of KeyAccessCheckers.
         */
        private KeyspaceAccessAuthorizer(Set<KeyAccessChecker> keyCheckers) {
            this.keyCheckers = keyCheckers;
        }

        @Override
        public boolean allowAccess(DatabaseEntry keyEntry) {
            final byte[] key = keyEntry.getData();
            for (final KeyAccessChecker checker : keyCheckers) {
                if (!checker.allowAccess(key)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean allowFullAccess() {
            return false;
        }
    }

    static void addKeyResult(List<ResultKey> results,
                             byte[] key,
                             long expirationTime) {
        results.add(new ResultKey(key, expirationTime));
    }
}
