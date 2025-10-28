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
import java.util.UUID;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.UnauthorizedException;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.SortableString;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.RecordVersion;
import com.sleepycat.je.rep.impl.RepImpl;

/**
 * Handles all user level operations, e.g. get/put/delete, etc., by using the
 * associated InternalOperationHandler.  Each operation should register an
 * instance of InternalOperationHandler in the createOperationHandlers method
 * below.
 */
public class OperationHandler {

    /** Timeout for getting a valid ReplicatedEnvironment. */
    private static final int ENV_TIMEOUT_MS = 5000;

    /**
     * Used in case we want to change the default cursor configuration.
     */
    public static final CursorConfig CURSOR_DEFAULT = CursorConfig.DEFAULT;

    /** This ReplicatedEnvironment node. */
    private final RepNode repNode;

    /** The RepNode's UUID. */
    private UUID repNodeUUID;

    /**
     * The logger is available to classes to log notable situations to the
     * server node's (RepNode) log.
     */
    private final Logger logger;

    /** Handlers for individual operations. */
    private final InternalOperationHandler<?>[] operationHandlers =
        new InternalOperationHandler<?>[OpCode.VALUES_COUNT];

    /**
     * Provides the interface by which key-value entries can be selectively
     * hidden from view.
     */
    interface KVAuthorizer {
        /**
         * Should access to the entry be allowed?
         */
        boolean allowAccess(DatabaseEntry keyEntry);

        /**
         * Should access to the entry be allowed and whether the current session
         * has given table privileges?
         *
         * Note that the table privileges specified should also imply the same
         * privileges in the generalAccessPrivileges of the operation using
         * this authorizer.
         */
        boolean allowAccess(DatabaseEntry keyEntry,
                            EnumSet<KVStorePrivilegeLabel> tablePrivs);
    }

    public OperationHandler(RepNode repNode, RepNodeService.Params params) {

        this.repNode = repNode;

        logger = LoggerUtils.getLogger(this.getClass(), params);
        assert logger != null;

        createOperationHandlers();
    }

    /**
     * Gets the logger for this instance.
     */
    public Logger getLogger() {
        return logger;
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
    public Result execute(InternalOperation op,
                          Transaction txn,
                          PartitionId partitionId)
        throws UnauthorizedException {
        return executeCast(getHandler(op.getOpCode()), op, txn, partitionId);
    }

    /**
     * Returns the handler for the specified opcode.
     *
     * @param opCode the opcode
     * @return the associated handler
     */
    public InternalOperationHandler<?> getHandler(OpCode opCode) {
        return operationHandlers[opCode.ordinal()];
    }

    /** Do a type-safe cast and then execute. */
    private static <T extends InternalOperation>
    Result executeCast(InternalOperationHandler<T> handler,
                       InternalOperation op,
                       Transaction txn,
                       PartitionId partitionId) {
        return handler.execute(
            handler.getOperationType().cast(op), txn, partitionId);
    }

    /**
     * Returns an immutable list of the privileges required to execute the
     * operation.  The required privileges depend on the operation and the
     * keyspace it is accessing.
     *
     * @param op the operation
     * @return the list of privileges for the operation
     */
    public List<? extends KVStorePrivilege> getRequiredPrivileges(
        InternalOperation op) {

        return getRequiredPrivilegesCast(getHandler(op.getOpCode()), op);
    }

    /** Do a type-safe cast and then execute. */
    private static <T extends InternalOperation>
        List<? extends KVStorePrivilege> getRequiredPrivilegesCast(
            InternalOperationHandler<T> handler, InternalOperation op) {
        return handler.getRequiredPrivileges(
            handler.getOperationType().cast(op));
    }

    /**
     * Creates handlers for each type of operation.  This method needs to
     * instantiate a handler for each opcode.
     */
    private void createOperationHandlers() {
        register(new NOPHandler(this));
        register(new GetHandler(this));
        register(new MultiGetHandler(this));
        register(new MultiGetKeysHandler(this));
        register(new MultiGetIterateHandler(this));
        register(new MultiGetKeysIterateHandler(this));
        register(new StoreIterateHandler(this));
        register(new StoreKeysIterateHandler(this));
        register(new PutHandler(this));
        register(new PutIfAbsentHandler(this));
        register(new PutIfPresentHandler(this));
        register(new PutIfVersionHandler(this));
        register(new DeleteHandler(this));
        register(new DeleteIfVersionHandler(this));
        register(new MultiDeleteHandler(this));
        register(new ExecuteHandler(this));
        register(new MultiGetTableHandler(this));
        register(new MultiGetTableKeysHandler(this));
        register(new TableIterateHandler(this));
        register(new TableKeysIterateHandler(this));
        register(new IndexIterateHandler(this));
        register(new IndexKeysIterateHandler(this));
        register(new MultiDeleteTableHandler(this));
        register(new MultiGetBatchIterateHandler(this));
        register(new MultiGetBatchKeysIterateHandler(this));
        register(new MultiGetBatchTableHandler(this));
        register(new MultiGetBatchTableKeysHandler(this));
        register(new PutBatchHandler(this));
        register(new TableQueryHandler(this, OpCode.QUERY_SINGLE_PARTITION));
        register(new TableQueryHandler(this, OpCode.QUERY_MULTI_PARTITION));
        register(new TableQueryHandler(this, OpCode.QUERY_MULTI_SHARD));
        register(new GetIdentityHandler(this));
        register(new PutResolveHandler(this));
        register(new TableChargeHandler(this));

        /* Check that every opcode has a handler */
        for (final OpCode opCode : OpCode.values()) {
            final InternalOperationHandler<?> handler = getHandler(opCode);
            if (handler == null) {
                throw new IllegalStateException(
                    "Missing InternalOperationHandler for " + opCode);
            }
        }
    }

    /**
     * Register a new operation handler, throwing an exception if a handler has
     * already been registered for the handler's opcode.
     */
    private void register(InternalOperationHandler<?> handler) {
        final int index = handler.getOpCode().ordinal();
        if (operationHandlers[index] != null) {
            throw new IllegalStateException(
                "Handler already present for " + handler.getOpCode());
        }
        operationHandlers[index] = handler;
    }

    public RepNode getRepNode() {
        return repNode;
    }

    /**
     * Returns the Version of the record at the given cursor position.
     * Note: The method is public because it is also called from query code.
     */
    public Version getVersion(Cursor cursor) {

        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);

        /*
         * Although the LN will normally be resident, since we just wrote it
         * with the cursor, we pass fetchLN=true to handle the rare case that
         * the LN is evicted after being written.
         */
        final RecordVersion recVersion =
            cursorImpl.getCurrentVersion(true /*fetchLN*/);

        return new Version(getRepNodeUUID(),
                           recVersion.getVLSN(),
                           repNode.getRepNodeId(),
                           recVersion.getLSN());
    }

    /**
     * Get the rep node UUID lazily, since opening the environment is not
     * possible in the constructor.
     */
    public UUID getRepNodeUUID() {

        /* Fast unsynchronized check almost always good. */
        if (repNodeUUID != null) {
            return repNodeUUID;
        }

        synchronized (this) {

            if (repNodeUUID != null) {
                return repNodeUUID;
            }

            final RepImpl repImpl = repNode.getEnvImpl(ENV_TIMEOUT_MS);
            if (repImpl == null) {
                throw new FaultException("Unable to get ReplicatedEnvironment " +
                    "after " + ENV_TIMEOUT_MS + " ms",
                    true /*isRemote*/);
            }
            repNodeUUID = repImpl.getUUID();
            assert repNodeUUID != null;
            return repNodeUUID;
        }
    }

    /**
     * Returns a ResultValueVersion for the given data and the version at the
     * given cursor position.
     */
    ResultValueVersion makeValueVersion(Cursor c,
                                        DatabaseEntry dataEntry,
                                        OperationResult result) {
        return new ResultValueVersion(
            dataEntry.getData(),
            getVersion(c),
            result != null ? result.getExpirationTime() : 0,
            result != null ? result.getCreationTime() : 0,
            result != null ? result.getModificationTime() : 0,
            InternalOperationHandler.getStorageSize(c));
    }

    ResultValueVersion makeValueVersion(Cursor c, DatabaseEntry dataEntry) {
        return makeValueVersion(c, dataEntry, null);
    }

    /**
     * Verifies that the table exists.  If it does not an exception
     * is thrown.  This method is called by single-key methods such as get
     * and put when they are operating in a table's key space.  It prevents
     * clients who are not yet aware of a table's removal from doing operations
     * on the now-removed table.
     */
    TableImpl getAndCheckTable(final long tableId) {
        if (tableId == 0) {
            return null;
        }
        TableImpl table = getTable(tableId);
        if (table == null) {
            throw new MetadataNotFoundException(
                "Cannot access table.  It may not exist, id: " + tableId,
                getTableMetadataSeqNum());
        }
        return table;
    }

    /**
     * Sets the before image TTL in JE {@link WriteOptions}
     * @param jeOptions JE write options
     * @param tableId table id
     */
    void setBeforeImageTTL(WriteOptions jeOptions, long tableId) {
        if (tableId <= 0) {
            /* not valid table id, no table associated w/ the op */
            return;
        }
        final TableImpl table = getAndCheckTable(tableId);
        setBeforeImageTTLFromTable(jeOptions, table);

    }

    /**
     * Sets the before image TTL in JE write options from table instance
     * @param jeOptions je write options
     * @param table table instance
     */
    private void setBeforeImageTTLFromTable(WriteOptions jeOptions,
                                            TableImpl table) {
        final TimeToLive beforeImgTTL = table.getBeforeImageTTL();
        if (beforeImgTTL != null) {
            /* before image is enabled for the table */
            jeOptions.saveBeforeImage((int) beforeImgTTL.getValue(),
                                      beforeImgTTL.getUnit());
        }
    }

    /**
     * Returns the table associated with the table id, or null if the
     * table with the id does not exist.
     */
    TableImpl getTable(long tableId) {
        return repNode.getTable(tableId);
    }

    TableImpl findTableByKeyBytes(byte[] keyBytes) {
        if (keyBytes == null || keyBytes.length == 0) {
            return null;
        }
        final Key fullKey = Key.fromByteArray(keyBytes);
        final String firstComponent = fullKey.getMajorPath().get(0);
        final long possibleTableId;

        TableImpl topLevelTable = null;
        /* Try match a normal table */
        try {
            possibleTableId =
                SortableString.longFromSortable(firstComponent);
            topLevelTable = repNode.getTable(possibleTableId);
        } catch (IllegalArgumentException iae) {
            /*
             * If string is not a legal table idString whose length is supposed
             * to be no greater than 10 (base 128 encoding), or an unexpected
             * character is found there, an IAE will be thrown. Fall through
             * and try to match a r2compat table.
             */
        }

        /* No normal table, try match a r2-compat table */
        if (topLevelTable == null) {
            topLevelTable = repNode.getR2CompatTable(firstComponent);
        }
        if (topLevelTable != null) {
            return topLevelTable.findTargetTable(keyBytes);
        }
        return null;
    }

    /**
     * Attempts to reserialize the new value format to older value format for
     * a row if the operation serial version is older than current serial
     * version.
     *
     * Returns the value bytes in older format if re-serialization is done,
     * otherwise returns back the given value bytes unchanged.
     */
    byte[] reserializeToOldValue(long tableId,
                                 byte[] keyBytes,
                                 byte[] valBytes,
                                 short opSerialVersion) {
        assert(tableId != 0);
        TableImpl table = getTable(tableId);
        if (table == null) {
            throw new IllegalStateException("Table not found:" + tableId);
        }
        return table.reserializeToOldValue(keyBytes, valBytes, opSerialVersion);
    }

    byte[] reserializeToOldValue(byte[] keyBytes,
                                 byte[] valBytes,
                                 short opSerialVersion) {

        assert(keyBytes != null);
        TableImpl table = findTableByKeyBytes(keyBytes);
        if (table == null) {
            throw new IllegalStateException("Invalid key for a table");
        }
        return table.reserializeToOldValue(keyBytes, valBytes, opSerialVersion);
    }


    /**
     * Returns the sequence number associated with the table metadata at the RN.
     */
    int getTableMetadataSeqNum() {
        return repNode.getMetadataSeqNum(MetadataType.TABLE);
    }
}
