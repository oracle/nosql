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

import static oracle.kv.Value.Format.isTableFormat;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.UnauthorizedException;
import oracle.kv.Version;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.RequestHandlerImpl.RequestContext;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.InternalOperationHandler.Keyspace.KeyAccessChecker;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.security.AccessCheckUtils;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.systables.TableMetadataDesc;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.RecordVersion;
import com.sleepycat.je.utilint.VLSN;

/**
 * Base class for implementing server-side handling of individual operations.
 * Each concrete instance executes operations for instances of a single
 * InternalOperation class.  Because a single handler is used to execute
 * multiple operations concurrently, handlers should not store per-operation
 * data.
 *
 * @param <T> the type of the associated internal operation
 */
public abstract class InternalOperationHandler<T extends InternalOperation> {
    /** ReadOption that has DEFAULT lockmode and excludes tombstones. */
    public final static ReadOptions DEFAULT_EXCLUDE_TOMBSTONES =
        LockMode.DEFAULT.toReadOptions().clone().setExcludeTombstones(true);

    /** ReadOption that has DEFAULT lockmode and excludes tombstones. */
    final static ReadOptions DEFAULT_INCLUDE_TOMBSTONES =
        LockMode.DEFAULT.toReadOptions().clone().setExcludeTombstones(false);


    /** ReadOption that has RMW lockmode and excludes tombstones. */
    public final static ReadOptions RMW_EXCLUDE_TOMBSTONES =
        LockMode.RMW.toReadOptions().clone().setExcludeTombstones(true);

    /**
     * An empty, immutable privilege list, used when an operation is requested
     * that does not require authentication.
     */
    static final List<KVStorePrivilege> emptyPrivilegeList =
        Collections.emptyList();

    /** Used to avoid fetching data for a key-only operation. */
    static final DatabaseEntry NO_DATA = new DatabaseEntry();
    static {
        NO_DATA.setPartial(0, 0, true);
    }

    /** Used as an empty value DatabaseEntry */
    private static final DatabaseEntry EMPTY_DATA =
        new DatabaseEntry(new byte[0]);

    /** Same key comparator as used for KV keys */
    static final Comparator<byte[]> KEY_BYTES_COMPARATOR =
        new Key.BytesComparator();

    /**
     * The overall operation handler.
     */
    final OperationHandler operationHandler;

    /**
     * The associated opcode.
     */
    final OpCode opCode;

    /**
     * The type of the operation associated with this handler.
     */
    private final Class<T> operationType;

    InternalOperationHandler(OperationHandler operationHandler,
                             OpCode opCode,
                             Class<T> operationType) {
        this.operationHandler = operationHandler;
        this.opCode = opCode;
        this.operationType = operationType;
    }

    public OperationHandler getOperationHandler() {
        return operationHandler;
    }

    OpCode getOpCode() {
        return opCode;
    }

    Class<T> getOperationType() {
        return operationType;
    }

    /**
     * Returns true by default to release read locks after each read
     * operation, rather than holding them all until the end of the txn.
     *
     * <p>Should be overridden to return false for operations that must retain
     * read locks until the end of the transaction -- this is repeatable-read
     * isolation. For example, see {@link MultiGetHandler#getReadCommitted}
     * and {@link MultiGetTableOperationHandler#getReadCommitted}.</p>
     *
     * <p>Note that:
     * - Write locks are always held until the end of the txn.
     * - Reading with LockMode.RMW gets a write lock, not a read lock.
     * - Read locks are always held while a cursor is positioned on a record
     *   (unless read-uncommitted is used and no lock is acquired).</p>
     *
     * <p>Because of the above, read-locks do not normally need to be held
     * until the end of the txn, i.e., repeatable-read isolation is not
     * needed. That is why this method returns true by default.</p>
     *
     * <p>It is a common mistake to assume that read-committed must be used
     * to avoid reading uncommitted data. Rather, repeatable-read isolation
     * (which is a _stronger_ form of isolation than read-committed) is the
     * default, and uncommitted data is read if only read-uncommitted mode is
     * explicitly specified. These terms are confusing, but they are the
     * ANSI standard terms.</p>
     */
    public boolean getReadCommitted() {
        return true;
    }

    /**
     * Execute the operation on the given repNode.
     *
     * @param op the operation to execute
     * @param txn the transaction to use for the operation
     * @param partitionId the partition ID of RN owning the request data
     * @return the result of execution
     * @throws UnauthorizedException if an attempt is made to access restricted
     * resources
     */
    abstract Result execute(T op,
                            Transaction txn,
                            PartitionId partitionId)
        throws UnauthorizedException;

    /**
     * Checks whether the input key has the server internal key prefix as
     * a prefix.  That is, does the key reference something that is definitely
     * within the server internal keyspace?
     */
    boolean isInternalRequestor() {
        final ExecutionContext currentContext = ExecutionContext.getCurrent();
        if (currentContext == null) {
            return true;
        }
        return currentContext.hasPrivilege(SystemPrivilege.INTLOPER);
    }

    /**
     * Returns an immutable list of privilege required to execute the
     * operation.  The required privileges depend on the operation and the
     * keyspace it accessing.
     *
     * @param op the operation
     * @return the list of privileges for the operation
     */
    abstract List<? extends KVStorePrivilege> getRequiredPrivileges(T op);

    Logger getLogger() {
        return operationHandler.getLogger();
    }

    public RepNode getRepNode() {
        return operationHandler.getRepNode();
    }

    Version getVersion(Cursor cursor) {
        return operationHandler.getVersion(cursor);
    }

    ResultValueVersion makeValueVersion(Cursor c,
                                        DatabaseEntry dataEntry,
                                        OperationResult result) {
        return operationHandler.makeValueVersion(c, dataEntry, result);
    }

    ResultValueVersion makeValueVersion(Cursor c, DatabaseEntry dataEntry) {
        return operationHandler.makeValueVersion(c, dataEntry);
    }

    protected TableImpl getAndCheckTable(final long tableId) {
        return operationHandler.getAndCheckTable(tableId);
    }

    TableImpl findTableByKeyBytes(byte[] keyBytes) {
        return operationHandler.findTableByKeyBytes(keyBytes);
    }

    /**
     * Creates JE write options with given TTL arguments.
     */
    static com.sleepycat.je.WriteOptions makeOption(TimeToLive ttl,
                                                    boolean updateTTL) {
        int ttlVal = ttl != null ? (int) ttl.getValue() : 0;
        TimeUnit ttlUnit = ttl != null ? ttl.getUnit() : null;
        return new com.sleepycat.je.WriteOptions()
            .setTTL(ttlVal, ttlUnit)
            .setUpdateTTL(updateTTL);
    }

    /**
     * Creates JE write options with given expiration time
     */
    static com.sleepycat.je.WriteOptions makeExpirationTimeOption(
        long expiration, boolean updateTTL) {
        return new com.sleepycat.je.WriteOptions()
            .setExpirationTime(expiration, null)
            .setUpdateTTL(updateTTL);
    }

    /**
     * Using the cursor at the prior version of the record and the data of the
     * prior version, return the requested previous value and/or version.
     * <p>
     * The {@link #getVersion(Cursor) access via cursor} for version data is
     * skipped for performance reason unless required by the given return
     * choice.
     * <p>
     * Expiration time is included if either version or value is requested but
     * not included for the choice of NONE.
     *
     * @param cursor cursor positioned on prior state of data
     * @param prevData previous data
     * @param prevValue choice of which state to attach and
     * the value+version to return as result
     * @param result result of looking up the previous state of the record.
     * Carries expiration time for the record. It is extremely unlikely to
     * be null, but handle that, just in case.
     */
    void getPrevValueVersion(Cursor cursor,
                             DatabaseEntry prevData,
                             ReturnResultValueVersion prevValue,
                             OperationResult result) {

        long expirationTime = (result != null ? result.getExpirationTime() : 0);
        long modificationTime = (result != null ?
            result.getModificationTime() : 0);
        switch (prevValue.getReturnChoice()) {
        case VALUE:
            assert !prevData.getPartial();
            prevValue.setValueVersion(prevData.getData(),
                                      null,
                                      expirationTime,
                                      modificationTime,
                                      getStorageSize(cursor));
            break;
        case VERSION:
            /**
             * JE does not gurantee to return the modification time
             * if value is not reqested, so to be consistent,
             * here always return 0 for the modification time  */
            prevValue.setValueVersion(null,
                                      getVersion(cursor),
                                      expirationTime,
                                      0L,
                                      -1);
            break;
        case ALL:
            assert !prevData.getPartial();
            prevValue.setValueVersion(prevData.getData(),
                                      getVersion(cursor),
                                      expirationTime,
                                      modificationTime,
                                      getStorageSize(cursor));
            break;
        case NONE:
            prevValue.setValueVersion(null, null, 0L, 0L, -1);
            break;
        default:
            throw new IllegalStateException
                (prevValue.getReturnChoice().toString());
        }
    }

    static ResultValueVersion getBeforeUpdateInfo(Choice choice,
                                                  Cursor cursor,
                                                  OperationHandler handler,
                                                  DatabaseEntry prevData,
                                                  OperationResult result) {

        long expirationTime = (result != null ? result.getExpirationTime() : 0);
        long modificationTime = (result != null ?
                                 result.getModificationTime(): 0);

        switch (choice) {
        case VALUE:
            assert !prevData.getPartial();
            return new ResultValueVersion(prevData.getData(),
                                          null,
                                          expirationTime,
                                          modificationTime,
                                          getStorageSize(cursor));
        case VERSION:
            /**
             * JE does not gurantee to return the modification time
             * if value is not reqested, so to be consistent,
             * here always return 0 for the modification time  */
            return new ResultValueVersion(null,
                                          handler.getVersion(cursor),
                                          expirationTime,
                                          0L,
                                          -1);
        case ALL:
            assert !prevData.getPartial();
            return new ResultValueVersion(prevData.getData(),
                                          handler.getVersion(cursor),
                                          expirationTime,
                                          modificationTime,
                                          getStorageSize(cursor));
        case NONE:
            return new ResultValueVersion(null, null, 0L, 0L, -1);
        default:
            throw new IllegalStateException(choice.toString());
        }
    }

    /**
     * Returns whether the Version of the record at the given cursor position
     * matches the given Version.
     */
    boolean versionMatches(Cursor cursor, Version matchVersion) {

        final RepNodeId repNodeId = getRepNode().getRepNodeId();
        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);

        /* First try without forcing an LN fetch. */
        RecordVersion recVersion =
            cursorImpl.getCurrentVersion(false /*fetchLN*/);

        /* The LSN may match, in which case we don't need the VLSN. */
        if (matchVersion.samePhysicalVersion(repNodeId, recVersion.getLSN())) {
            return true;
        }

        /* Try the VLSN if it is resident and available. */
        long vlsn = recVersion.getVLSN();
        if (VLSN.isNull(vlsn)) {

            /* The VLSN is not resident. Force a fetch and try again. */
            recVersion = cursorImpl.getCurrentVersion(true /*fetchLN*/);
            vlsn = recVersion.getVLSN();
            if (VLSN.isNull(vlsn)) {
                throw new IllegalStateException(
                    "VLSN was null after fetching LN");
            }
        }

        if (!matchVersion.sameLogicalVersion(vlsn)) {
            return false;
        }

        /*
         * Make sure that the version is for the same shard in case the record
         * was moved by partition migration
         */
        return operationHandler.getRepNodeUUID().equals(
            matchVersion.getRepGroupUUID());
    }

    /**
     * Create a DatabaseEntry for the value bytes.  If the value is
     * empty, as indicated by a single zero or 1 byte in the array, use an empty
     * DatabaseEntry to allow JE to optimize the empty value.
     *
     * Both 0, 1 and 2 need to be accepted. See Value.writeFastExternal() where
     * it is writing with Format of NONE or TABLE, TABLE_V1
     */
    static DatabaseEntry valueDatabaseEntry(byte[] value) {

        if (value.length == 1 && (value[0] == 0 || isTableFormat(value[0]))) {
            return EMPTY_DATA;
        }
        return new DatabaseEntry(value);
    }

    /**
     * As above, but reuse the supplied DatabaseEntry. Callers be sure to use
     * the returned value as it may not be the same as the one passed in.
     */
    DatabaseEntry valueDatabaseEntry(DatabaseEntry dbEntry,
                                     byte[] value) {

        if (value.length == 1 && (value[0] == 0 || isTableFormat(value[0]))) {
            return EMPTY_DATA;
        }

        dbEntry.setData(value);
        return dbEntry;
    }

    /**
     * Determine if current request can access system tables.
     */
    static boolean allowAccessSystemTables() {
        final ExecutionContext currentContext = ExecutionContext.getCurrent();
        if (currentContext == null) {
            return true;
        }
        final RequestContext reqContext =
             (RequestContext) currentContext.operationContext();
        final Request request = reqContext.getRequest();

        /* Only check write operation against system table */
        if (!request.isWrite()) {
            return true;
        }

        /*
         * Check if caller is permitted to write system tables, which is true
         * for internal nodes as well as for users granted this privilege.
         */
        return currentContext.hasPrivilege(SystemPrivilege.WRITE_SYSTEM_TABLE);
    }

    public static final int MIN_READ = 1024;

    /**
     * Returns the estimated disk storage size for the record at the
     * specified cursor's current position.
     */
    public static int getStorageSize(Cursor cursor) {
        return DbInternal.getCursorImpl(cursor).getStorageSize();
    }

    public static int getIndexStorageSize(Cursor cursor) {
        return DbInternal.getCursorImpl(cursor).getIndexStorageSize();
    }

    /**
     * Returns the number of index writes performed when writing the record at
     * the specified cursor. This method assumes the cursor is at the position
     * of the last write.
     */
    static int getNIndexWrites(Cursor cursor) {
        return DbInternal.getCursorImpl(cursor).getNSecondaryWrites();
    }


    protected TimeToLive getTombstoneTTL() {
        return getRepNode().getRepNodeParams().getTombstoneTTL();
    }

    /**
     * A common interface for those operations need to access table keyspace,
     * which defining the privileges needed.
     */
    interface PrivilegedTableAccessor {
        /**
         * Returns the needed privileges accessing the table specified by the
         * id.
         *
         * @param tableId table id
         * @return a list of required privileges
         */
        List<? extends KVStorePrivilege> tableAccessPrivileges(long tableId);

        /**
         * Returns the needed privileges accessing the namespace specified by
         * the namespace param.
         *
         * @param namespace the accessed namespace
         * @return a list of required privileges
         */
        List<? extends KVStorePrivilege> namespaceAccessPrivileges(String
            namespace);
    }

    /**
     * A class to help identify the keyspace which operation is accessing.
     */
    static class Keyspace {

        /**
         * The encoding of the prefix of the server-private keyspace(///),
         * which is a subset of the "internal" keyspace (//) used by the client
         * to store the Avro schema.
         */
        private static final byte[] PRIVATE_KEY_PREFIX =
            //new byte[] { Key.BINARY_COMP_DELIM, Key.BINARY_COMP_DELIM };
            new byte[] { 0, 0 };

        /**
         * The encoding of the prefix of the Avro schema keyspace(//sch/),
         * which is used by the client to store the Avro schema.
         */
        private static final byte[] AVRO_SCHEMA_KEY_PREFIX =
            new byte[] { 0, 0x73, 0x63, 0x68 }; /* Keybytes of "//sch" */

        static interface KeyAccessChecker {
            boolean allowAccess(byte[] key);
        }

        static final KeyAccessChecker privateKeyAccessChecker =
            new KeyAccessChecker() {
            @Override
            public boolean allowAccess(byte[] key) {
                return !Keyspace.isPrivateAccess(key);
            }
        };

        static final KeyAccessChecker schemaKeyAccessChecker =
            new KeyAccessChecker() {
            @Override
            public boolean allowAccess(byte[] key) {
                return !Keyspace.isSchemaAccess(key);
            }
        };

        enum KeyspaceType { PRIVATE, SCHEMA, GENERAL }

        static KeyspaceType identifyKeyspace(byte[] key) {
            if (key == null || key.length == 0) {
                throw new IllegalArgumentException(
                    "Key bytes may not be null or empty");
            }

            /* Quick return for non-internal keyspace */
            if (key[0] == 0) {
                if (isSchemaAccess(key)) {
                    return KeyspaceType.SCHEMA;
                }
                if (isPrivateAccess(key)) {
                    return KeyspaceType.PRIVATE;
                }
            }
            /* Other cases are regarded as general keyspace */
            return KeyspaceType.GENERAL;
        }

        /**
         * Checks whether the input key has the Avro schema keyspace as a
         * prefix. That is, does the key matches exactly the "//sch" or has the
         * major component of "//sch"?
         */
        static boolean isSchemaAccess(byte[] key) {
            if (key.length < AVRO_SCHEMA_KEY_PREFIX.length) {
                return false;
            }
            for (int i = 0; i < AVRO_SCHEMA_KEY_PREFIX.length; i++) {
                if (key[i] != AVRO_SCHEMA_KEY_PREFIX[i]) {
                    return false;
                }
            }
            /* Key is exactly the "//sch" */
            if (key.length == AVRO_SCHEMA_KEY_PREFIX.length) {
                return true;
            }
            /* Key has "//sch" as its partial or complete major component */
            final byte endingByte = key[AVRO_SCHEMA_KEY_PREFIX.length];
            return endingByte == (byte) 0xff /* Path delimiter */ ||
                   endingByte == (byte) 0x00; /* Component delimiter */
        }

        /**
         * Checks whether the input key is a prefix of the schema key space.
         * That is, does the key reference something that is may be within the
         * schema key space?
         */
        static boolean mayBeSchemaAccess(byte[] key) {
            if (key.length < AVRO_SCHEMA_KEY_PREFIX.length) {
                for (int i = 0; i < key.length; i++) {
                    if (key[i] != AVRO_SCHEMA_KEY_PREFIX[i]) {
                        return false;
                    }
                }
                return true;
            }
            for (int i = 0; i < AVRO_SCHEMA_KEY_PREFIX.length; i++) {
                if (key[i] != AVRO_SCHEMA_KEY_PREFIX[i]) {
                    return false;
                }
            }
            /* Key is exactly the "//sch" */
            if (key.length == AVRO_SCHEMA_KEY_PREFIX.length) {
                return true;
            }
            /* Key has "//sch" as its partial or complete major component */
            final byte endingByte = key[AVRO_SCHEMA_KEY_PREFIX.length];
            return endingByte == (byte) 0xff /* Path delimiter */ ||
                   endingByte == (byte) 0x00; /* Component delimiter */
        }

        /**
         * Checks whether the input key has the server private keyspace as a
         * prefix.  That is, does the key reference something that is
         * definitely within the server private keyspace?
         */
        static boolean isPrivateAccess(byte[] key) {
            if (key.length < PRIVATE_KEY_PREFIX.length) {
                return false;
            }
            for (int i = 0; i < PRIVATE_KEY_PREFIX.length; i++) {
                if (key[i] != PRIVATE_KEY_PREFIX[i]) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Checks whether the input key is a prefix of the server private key
         * space.  That is, does the key reference something that is may be
         * within the server private keyspace?
         */
        static boolean mayBePrivateAccess(byte[] key) {
            if (key.length < PRIVATE_KEY_PREFIX.length) {
                for (byte element : key) {
                    if (element != PRIVATE_KEY_PREFIX.length) {
                        return false;
                    }
                }
                return true;
            }
            for (int i = 0; i < PRIVATE_KEY_PREFIX.length; i++) {
                if (key[i] != PRIVATE_KEY_PREFIX.length) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Checks whether the input key is outside both the server private
         * keyspace and the schema keyspace.
         */
        static boolean isGeneralAccess(byte[] key) {
            return !isPrivateAccess(key) && !isSchemaAccess(key);
        }
    }

    /**
     * A per-key access checker for possible table keyspaces.  Access to a key
     * is allowed if and only if the key falls in a table's keyspace and the
     * current user has access privileges on that table.
     */
    static class TableAccessChecker implements KeyAccessChecker {

        private final PrivilegedTableAccessor tableAccessor;
        final OperationHandler operationHandler;

        /**
         * A set caching the tables which have been verified to be accessible.
         */
        private final Set<Long> accessibleTables = new HashSet<Long>();

        TableAccessChecker(OperationHandler operationHandler,
                           PrivilegedTableAccessor tableAccessor) {
            this.operationHandler = operationHandler;
            this.tableAccessor = tableAccessor;
        }

        @Override
        public boolean allowAccess(byte[] key) {
            if (!Keyspace.isGeneralAccess(key)) {
                return true;
            }

            final TableImpl possibleTable =
                operationHandler.findTableByKeyBytes(key);
            /* Not accessing table, returns false */
            if (possibleTable == null) {
                return false;
            }

            return internalCheckTableAccess(possibleTable);
        }

        boolean internalCheckTableAccess(TableImpl table) {
            final ExecutionContext exeCtx = ExecutionContext.getCurrent();
            if (exeCtx == null) {
                return true;
            }

            if (accessibleTables.contains(table.getId())) {
                return true;
            }

            if (!AccessCheckUtils.currentUserOwnsResource(table)) {
                /* Check the privileges on the namespace */
                if (!exeCtx.hasAllPrivileges(
                    tableAccessor.namespaceAccessPrivileges(
                        table.getInternalNamespace()))) {

                    if (!exeCtx.hasAllPrivileges(
                            tableAccessor.tableAccessPrivileges(table.getId()))) {
                        return false;
                    }

                    /* Ensure at least read privileges on all parent tables. */
                    TableImpl parent = (TableImpl) table.getParent();
                    while (parent != null) {
                        final long pTableId = parent.getId();
                        /*
                         * One of the parent table is accessible, exits the loop
                         * and returns true according to induction
                         */
                        if (accessibleTables.contains(pTableId)) {
                            break;
                        }

                        final TablePrivilege parentReadPriv =
                            new TablePrivilege.ReadTable(pTableId);
                        if (!exeCtx.hasPrivilege(parentReadPriv) &&
                            !exeCtx.hasAllPrivileges(
                                tableAccessor.tableAccessPrivileges(pTableId))) {
                            return false;
                        }
                        parent = (TableImpl) parent.getParent();
                    }
                }

                if (table.isSystemTable() && !allowAccessSystemTables()) {
                    return false;
                }
            }

            /* Caches the verified table */
            accessibleTables.add(table.getId());
            return true;
        }
    }

    /**
     * Returns privilege for the specified table ID. If the ID is for the table
     * metadata system table, the USRVIEW privilege is required. Otherwise,
     * READ_TABLE(tableId) privilege is required.
     */
    protected final List<? extends KVStorePrivilege> tableReadPrivileges(long tableId) {
        /*
         * Special case the table metadata table since it is necessary for the
         * client to read to get table metadata. If more system tables need
         * special casing, a more efficient scheme to identify the table may be
         * needed.
         */
        return tableId == TableMetadataDesc.METADATA_TABLE_ID ?
                SystemPrivilege.usrviewPrivList :
                Collections.singletonList(
                        new TablePrivilege.ReadTable(tableId));
    }

    /**
     * A per-key access checker for possible system tables. Client write access
     * to a key is not allowed if the key falls in a system table key space.
     */
    static class SysTableAccessChecker implements KeyAccessChecker {

        final OperationHandler operationHandler;

        SysTableAccessChecker(OperationHandler operationHandler) {
            this.operationHandler = operationHandler;
        }

        @Override
        public boolean allowAccess(byte[] key) {
            if (!Keyspace.isGeneralAccess(key)) {
                return true;
            }

            final ExecutionContext exeCtx = ExecutionContext.getCurrent();
            if (exeCtx == null) {
                return true;
            }

            final TableImpl table = operationHandler.findTableByKeyBytes(key);
            if (table != null && table.isSystemTable() &&
                !allowAccessSystemTables()) {
                return false;
            }
            return true;
        }
    }
}
