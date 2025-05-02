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

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.UnauthorizedException;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.SequenceImpl.SGAttributes;
import oracle.kv.impl.api.table.SequenceImpl.SGAttrsAndValues;
import oracle.kv.impl.api.table.SequenceImpl.SGValues;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.systables.SGAttributesTableDesc;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.WriteOptions;

import com.sleepycat.je.Transaction;

public class GetIdentityHandler extends
    SingleKeyOperationHandler<GetIdentityAttrsAndValues>{

    /*Max number of tries to update the system table for sequence generators.*/
    public static final int MAX_SG_TRIES = 1000;
    private static final ReadOptions readOp =
        new ReadOptions(Consistency.ABSOLUTE, 0, null);
    private static final WriteOptions writeOp =
        new WriteOptions(Durability.COMMIT_SYNC, 0, null);

    GetIdentityHandler(OperationHandler handler) {
        super(handler, OpCode.GET_IDENTITY, GetIdentityAttrsAndValues.class);
    }

    /**
     * Gets the attributes and new values of an identity column if needed.
     *
     * When this GetIdentityHandler is executed, it will first get the
     * up-to-date attributes from the system table and update the client's
     * attribute cache if the client needs the attributes, or there is a new
     * version of the attributes. Then it will check if the client needs new
     * sequences. If new sequences are needed, it will update the system table
     * and give the client a new batch of sequence numbers.
     */
    @Override
    Result execute(GetIdentityAttrsAndValues op,
                   Transaction txn,
                   PartitionId partitionId)
        throws UnauthorizedException {

        KVStoreImpl store = (KVStoreImpl) getRepNode().getKVStore();
        if (store == null) {
            throw new FaultException("Failed to get KVStore.", true);
        }

        TableAPIImpl api = store.getTableAPIImpl();
        Table table = api.getTable(SGAttributesTableDesc.TABLE_NAME);

        if (table == null) {
            throw new FaultException("Table not found: " + op.getSgName(),
                true);
        }

        PrimaryKey pk = table.createPrimaryKey();
        pk.put(SGAttributesTableDesc.COL_NAME_SGTYPE,
               SGAttributesTableDesc.SGType.INTERNAL.name());
        pk.put(SGAttributesTableDesc.COL_NAME_SGNAME, op.getSgName());
        Result getResult = api.getInternal((RowSerializer)pk, readOp);
        op.addReadBytes(getResult.getReadKB());
        op.addWriteBytes(getResult.getWriteKB(), 0, null, 0);
        Row identityRow = api.processGetResult(getResult, (PrimaryKeyImpl)pk);

        if (identityRow == null) {
            throw new FaultException("No sequence found for : " +
                op.getSgName(), true);
        }

        SGAttributes attributes = new SGAttributes(identityRow);
        SGAttrsAndValues result = new SGAttrsAndValues();

        if (op.getNeedAttributes() || op.getCurVersion() == -1 ||
            (op.getCurVersion() != attributes.getVersion())) {

            result.setAttributes(attributes);
        }

        if (op.getNeedNextSequence() || op.getCurVersion() == -1 ||
            (op.getCurVersion() != attributes.getVersion())) {

            Type dataType = Type.valueOf(identityRow
                .get(SGAttributesTableDesc.COL_NAME_DATATYPE).asString().get());

            SGValues<?> newValues = SGValues.newInstance(dataType,
                attributes.getIncrementValue());
            incrementSequence(identityRow, attributes, op.getClientCacheSize(),
                              newValues, table, api, op);
            result.setValueCache(newValues);
        }

        return new Result.GetIdentityResult(getOpCode(), op.getReadKB(),
                                            op.getWriteKB(), result);
    }

    /**
     * Increment the sequence number
     */
    private void incrementSequence(Row row,
                                   SGAttributes identityDef,
                                   int clientIdentityCacheSize,
                                   SGValues<?> newCacheValues,
                                   Table table,
                                   TableAPIImpl api,
                                   GetIdentityAttrsAndValues op) {
        boolean isCycle = row.get(SGAttributesTableDesc.COL_NAME_CYCLE)
            .asBoolean().get();
        Version version = row.getVersion();
        BigDecimal currentValue = row
            .get(SGAttributesTableDesc.COL_NAME_CURRENTVALUE).asNumber().get();

        BigDecimal increment = new BigDecimal(identityDef.getIncrementValue());
        boolean positive = increment.compareTo(BigDecimal.ZERO) > 0;

        BigDecimal cacheSize = clientIdentityCacheSize > 0
            ? new BigDecimal(clientIdentityCacheSize)
            : new BigDecimal(identityDef.getCacheValue());

        BigDecimal max = identityDef.getMaxValue();
        BigDecimal min = identityDef.getMinValue();
        BigDecimal startValue = identityDef.getStartValue();

        /*
         * The current_value could be concurrently updated by another
         * thread/node, retry until reaching MAX_SG_TRIES.
         */

        for (int i = 0; i < MAX_SG_TRIES; i++) {
            /* Recycle the current value when it exceeds max or min value */

            if (positive) {
                if (max != null && currentValue.compareTo(max) > 0) {
                    if (!isCycle) {
                        throw new WrappedClientException
                            (new IllegalArgumentException("Current value " +
                            "cannot exceed max value or no more available " +
                            "values in the sequence."));
                    }
                    /* Recycle from start value */
                    currentValue = startValue;
                }
            } else {
                if (min != null && currentValue.compareTo(min) < 0) {
                    if (!isCycle) {
                        throw new WrappedClientException
                            (new IllegalArgumentException("Current value " +
                            "cannot exceed min value or no more available " +
                            "values in the sequence."));
                    }
                    /* Recycle from start value */
                    currentValue = startValue;
                }
            }


            BigDecimal nextValue = currentValue.
                add(cacheSize.subtract(BigDecimal.ONE).
                multiply(increment));
            if (positive) {
                if ((max != null && nextValue.compareTo(max) > 0)) {
                    BigDecimal remainder = (max.subtract(currentValue)).
                        remainder(increment);
                    nextValue = max.subtract(remainder);
                }
            } else {
                if ((min != null && nextValue.compareTo(min) < 0)) {
                    BigDecimal remainder = (min.subtract(currentValue)).
                        remainder(increment);
                    nextValue = min.subtract(remainder);
                }

            }

            newCacheValues.update(currentValue, nextValue);

            BigDecimal newCurrentValue = nextValue.add(increment);
            row.put(SGAttributesTableDesc.COL_NAME_CURRENTVALUE,
                    FieldValueFactory.createNumber(newCurrentValue));


            ReturnRow prevRow = table.createReturnRow(ReturnRow.Choice.ALL);
            Result result = api.getStore().executeRequest(
                api.makePutRequest(OpCode.PUT_IF_VERSION,
                                   (RowSerializer) row, prevRow,
                                   writeOp, null, version));
            op.addReadBytes(result.getReadKB());
            op.addWriteBytes(result.getWriteKB(), 0, null, 0);

            /*
             * Use putIfVersion API to make sure the queried current value is
             * not updated in another thread
             */
            Version curVersion = api.processPutResult(result,
                                                      (RowImpl)row, prevRow);
            if (curVersion != null) {
                return;
            }

            /* Retry if the version does not match the previous one */
            version = prevRow.getVersion();
            currentValue = prevRow
                .get(SGAttributesTableDesc.COL_NAME_CURRENTVALUE).asNumber()
                .get();
        }

        throw new FaultException("Reached max retries to write sequence.",
                                 true);
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

    @Override
    List<? extends KVStorePrivilege> schemaAccessPrivileges() {
        return SystemPrivilege.schemaWritePrivList;
    }

    @Override
    List<? extends KVStorePrivilege> generalAccessPrivileges() {
        return SystemPrivilege.writeOnlyPrivList;
    }
}
