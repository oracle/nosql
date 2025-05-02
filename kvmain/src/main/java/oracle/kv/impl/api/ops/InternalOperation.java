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

import static oracle.kv.impl.api.ops.InternalOperationHandler.MIN_READ;
import static oracle.kv.impl.api.ops.ResourceTracker.RW_BLOCK_SIZE;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Operation.Type;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.StatType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.TimeToLive;

/**
 * Represents an operation that may be performed on the store.  Each operation
 * should define a new {@link OpCode} constant below and register a handler in
 * the {@link OperationHandler} class.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class InternalOperation implements FastExternalizable {

    /**
     * The serialVersion used when the operation is sent to the server. It is
     * set when "this" is deserialized at the server and it is used by the
     * server to know if the operation is coming from an older client so that
     * the server can convert the new value format to old value format.
     */
    private final short opSerialVersion;

    /**
     * Enable add the read bytes number to cachedReadKB rather than adding to
     * throughput tracking immediately in addReadBytes(), flushReadBytes(boolean)
     * method can be used to add the cachedReadKB to throughput tracking or
     * clear the cachedReadKB and ignore the cached read cost.
     */
    private transient boolean enableCacheReadBytes;
    /**
     * The cachedReadKB records the sum of cached read bytes rounded up to KB.
     */
    private transient int cachedReadKB;

    /**
     * An enumeration listing all available OpCodes of Operations for the
     * data store.
     *
     * WARNING: To avoid breaking serialization compatibility, the order of the
     * values must not be changed and new values must be added at the end.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public enum OpCode implements FastExternalizable, StatType {

        NOP(0) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new NOP(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {
                return new Result.NOPResult(in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return true;
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.NOP_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.NOP_CUM;
            }
        },

        GET(1) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new Get(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.GetResult(this,
                                            readKB, writeKB,
                                            in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.GetResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.GET_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.GET_CUM;
            }
        },

        MULTI_GET(2) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGet(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.IterateResult(this,
                                                readKB, writeKB,
                                                in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.IterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_CUM;
            }
        },

        MULTI_GET_KEYS(3) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGetKeys(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.KeysIterateResult(this,
                                                    readKB, writeKB,
                                                    in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.KeysIterateResult);
            }


            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_KEYS_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_KEYS_CUM;
            }
        },

        MULTI_GET_ITERATE(4) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGetIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.IterateResult(this,
                                                readKB, writeKB,
                                                in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.IterateResult);
            }


            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_ITERATOR_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_ITERATOR_CUM;
            }
        },

        MULTI_GET_KEYS_ITERATE(5) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGetKeysIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.KeysIterateResult(this,
                                                    readKB, writeKB,
                                                    in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.KeysIterateResult);
            }


            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_KEYS_ITERATOR_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_KEYS_ITERATOR_CUM;
            }
        },

        STORE_ITERATE(6) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new StoreIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.IterateResult(this,
                                                readKB, writeKB,
                                                in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.IterateResult);
            }


            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.STORE_ITERATOR_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.STORE_ITERATOR_CUM;
            }
        },

        STORE_KEYS_ITERATE(7) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new StoreKeysIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.KeysIterateResult(this,
                                                    readKB, writeKB,
                                                    in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.KeysIterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.STORE_KEYS_ITERATOR_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.STORE_KEYS_ITERATOR_CUM;
            }
        },

        PUT(8) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new Put(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.PutResult(this,
                                            readKB, writeKB,
                                            in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.PutResult);
            }

            @Override
            public Type getExecuteType() {
                return Type.PUT;
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.PUT_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.PUT_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        PUT_IF_ABSENT(9) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new PutIfAbsent(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.PutResult(this,
                                            readKB, writeKB,
                                            in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.PutResult);
            }

            @Override
            public Type getExecuteType() {
                return Type.PUT_IF_ABSENT;
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.PUT_IF_ABSENT_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.PUT_IF_ABSENT_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        PUT_IF_PRESENT(10) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new PutIfPresent(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.PutResult(this,
                                            readKB, writeKB,
                                            in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.PutResult);
            }

            @Override
            public Type getExecuteType() {
                return Type.PUT_IF_PRESENT;
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.PUT_IF_PRESENT_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.PUT_IF_PRESENT_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        PUT_IF_VERSION(11) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new PutIfVersion(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.PutResult(this,
                                            readKB, writeKB,
                                            in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.PutResult);
            }

            @Override
            public Type getExecuteType() {
                return Type.PUT_IF_VERSION;
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.PUT_IF_VERSION_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.PUT_IF_VERSION_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        DELETE(12) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new Delete(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.DeleteResult(this,
                                               readKB, writeKB,
                                               in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.DeleteResult);
            }

            @Override
            public Type getExecuteType() {
                return Type.DELETE;
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.DELETE_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.DELETE_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        DELETE_IF_VERSION(13) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new DeleteIfVersion(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.DeleteResult(this,
                                               readKB, writeKB,
                                               in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.DeleteResult);
            }

            @Override
            public Type getExecuteType() {
                return Type.DELETE_IF_VERSION;
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.DELETE_IF_VERSION_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.DELETE_IF_VERSION_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        MULTI_DELETE(14) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiDelete(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.MultiDeleteResult(this,
                                                    readKB, writeKB,
                                                    in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.MultiDeleteResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_DELETE_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_DELETE_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        EXECUTE(15) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new Execute(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.ExecuteResult(this,
                                                readKB, writeKB,
                                                in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.ExecuteResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.EXECUTE_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.EXECUTE_CUM;
            }
        },

        MULTI_GET_TABLE(16) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGetTable(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.IterateResult(this,
                                                readKB, writeKB,
                                                in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.IterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_CUM;
            }
        },

        MULTI_GET_TABLE_KEYS(17) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGetTableKeys(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.KeysIterateResult(this,
                                                    readKB, writeKB,
                                                    in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.KeysIterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_CUM;
            }
        },

        TABLE_ITERATE(18) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new TableIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.IterateResult(this,
                                                readKB, writeKB,
                                                in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.IterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.STORE_ITERATOR_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.STORE_ITERATOR_CUM;
            }
        },

        TABLE_KEYS_ITERATE(19) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new TableKeysIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.KeysIterateResult(this,
                                                    readKB, writeKB,
                                                    in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.KeysIterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.STORE_KEYS_ITERATOR_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.STORE_KEYS_ITERATOR_CUM;
            }
        },

        INDEX_ITERATE(20) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new IndexIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.IndexRowsIterateResult(this,
                                                         readKB, writeKB,
                                                         in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.IndexRowsIterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.INDEX_ITERATOR_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.INDEX_ITERATOR_CUM;
            }
        },

        INDEX_KEYS_ITERATE(21) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new IndexKeysIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.IndexKeysIterateResult(this,
                                                         readKB, writeKB,
                                                         in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.IndexKeysIterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.INDEX_KEYS_ITERATOR_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.INDEX_KEYS_ITERATOR_CUM;
            }
        },

        MULTI_DELETE_TABLE(22) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiDeleteTable(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.MultiDeleteResult(this,
                                                    readKB, writeKB,
                                                    in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.MultiDeleteResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_DELETE_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_DELETE_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        MULTI_GET_BATCH(23) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGetBatchIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.BulkGetIterateResult(this,
                                                       readKB, writeKB,
                                                       in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.BulkGetIterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_BATCH_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_BATCH_CUM;
            }
        },

        MULTI_GET_BATCH_KEYS(24) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGetBatchKeysIterate(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.BulkGetKeysIterateResult(this,
                                                           readKB, writeKB,
                                                           in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.BulkGetKeysIterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_BATCH_KEYS_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_BATCH_KEYS_CUM;
            }
        },

        MULTI_GET_BATCH_TABLE(25) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGetBatchTable(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.BulkGetIterateResult(this,
                                                       readKB, writeKB,
                                                       in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.BulkGetIterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_BATCH_TABLE_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_BATCH_TABLE_CUM;
            }
        },

        MULTI_GET_BATCH_TABLE_KEYS(26) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new MultiGetBatchTableKeys(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.BulkGetKeysIterateResult(this,
                                                           readKB, writeKB,
                                                           in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.BulkGetKeysIterateResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.MULTI_GET_BATCH_TABLE_KEYS_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.MULTI_GET_BATCH_TABLE_KEYS_CUM;
            }
        },

        PUT_BATCH(27) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new PutBatch(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.PutBatchResult(this,
                                                 readKB, writeKB,
                                                 in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.PutBatchResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.PUT_BATCH_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.PUT_BATCH_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        /*
         * Various query operations are separated in order to provide more
         * informative statistics to users, separating operations in terms of
         * 1. single-partition
         * 2. multi (all) partitions
         * 3. multi (all) shards
         * When updating operations are implemented, additional stats and
         * OpCodes will be added for those queries.
         */
        QUERY_SINGLE_PARTITION(28) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new TableQuery(this, in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.QueryResult(this,
                                              readKB, writeKB,
                                              in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.QueryResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.QUERY_SINGLE_PARTITION_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.QUERY_SINGLE_PARTITION_CUM;
            }
        },

        QUERY_MULTI_PARTITION(29) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new TableQuery(this, in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.QueryResult(this,
                                              readKB, writeKB,
                                              in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.QueryResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.QUERY_MULTI_PARTITION_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.QUERY_MULTI_PARTITION_CUM;
            }
        },

        QUERY_MULTI_SHARD(30) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new TableQuery(this, in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.QueryResult(this,
                                              readKB, writeKB,
                                              in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.QueryResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.QUERY_MULTI_SHARD_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.QUERY_MULTI_SHARD_CUM;
            }
        },

        GET_IDENTITY(31) {
            @Override
            InternalOperation readOperation(DataInput in,
                                            short serialVersion)
                throws IOException {
                return new GetIdentityAttrsAndValues(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB,
                                     int writeKB,
                                     short serialVersion)
                throws IOException {
                return new Result.GetIdentityResult(this, readKB, writeKB, in,
                                                    serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.GetIdentityResult);
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.GET_IDENTITY_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.GET_IDENTITY_CUM;
            }

        },

        PUT_RESOLVE(32) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                throws IOException {

                return new PutResolve(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                throws IOException {

                return new Result.PutResult(this,
                                            readKB, writeKB,
                                            in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return (result instanceof Result.PutResult);
            }

            //TODO: Currently PutResolve is not an execute type. Will support
            //it in the future.

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.PUT_RESOLVE_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.PUT_RESOLVE_CUM;
            }

            @Override
            protected boolean isWrite() {
                return true;
            }
        },

        TABLE_CHARGE(33) {
            @Override
            InternalOperation readOperation(DataInput in, short serialVersion)
                    throws IOException {

                return new TableCharge(in, serialVersion);
            }

            @Override
            public Result readResult(DataInput in,
                                     int readKB, int writeKB,
                                     short serialVersion)
                    throws IOException {
                return new Result.NOPResult(in, serialVersion);
            }

            @Override
            public boolean checkResultType(Result result) {
                return true;
            }

            @Override
            public short requiredVersion() {
                return SerialVersion.TABLE_MD_IN_STORE_VERSION;
            }

            @Override
            public PerfStatType getIntervalMetric() {
                return PerfStatType.NOP_INT;
            }

            @Override
            public PerfStatType getCumulativeMetric() {
                return PerfStatType.NOP_CUM;
            }
        };

        private static final OpCode[] VALUES = values();
        public static final int VALUES_COUNT = VALUES.length;

        private OpCode(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        abstract InternalOperation readOperation(DataInput in,
                                                 short serialVersion)
            throws IOException;

        public abstract Result readResult(DataInput in,
                                          int readKB, int writeKB,
                                          short serialVersion)
            throws IOException;

        public abstract boolean checkResultType(Result result);

        public Type getExecuteType() {
            throw new RuntimeException("Not an execute op: " + this);
        }

        public abstract PerfStatType getIntervalMetric();
        public abstract PerfStatType getCumulativeMetric();

        /**
         * The lowest serial version supported by this operation.  Operations
         * that require a higher minimum version should override this method.
         */
        public short requiredVersion() {
            return SerialVersion.MINIMUM;
        }

        /*
         * Returns true if the operation performs a write. (Write ops must
         * override this method.)
         */
        protected boolean isWrite() {
            return false;
        }

        /**
         * Reads this object from the input stream.
         */
        public static OpCode readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readUnsignedByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "unknown opcode: " + ordinal);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code unsigned byte}) <i>value</i> &#47;*
         *      {@link #NOP}=0,
         *      {@link #GET}=1,
         *      {@link #MULTI_GET}=2,
         *      {@link #MULTI_GET_KEYS}=3,
         *      {@link #MULTI_GET_ITERATE}=4,
         *      {@link #MULTI_GET_KEYS_ITERATE}=5,
         *      {@link #STORE_ITERATE}=6,
         *      {@link #STORE_KEYS_ITERATE}=7,
         *      {@link #PUT}=8,
         *      {@link #PUT_IF_ABSENT}=9,
         *      {@link #PUT_IF_PRESENT}=10,
         *      {@link #PUT_IF_VERSION}=11,
         *      {@link #DELETE}=12,
         *      {@link #DELETE_IF_VERSION}=13,
         *      {@link #MULTI_DELETE}=14,
         *      {@link #EXECUTE}=15,
         *      {@link #MULTI_GET_TABLE}=16,
         *      {@link #MULTI_GET_TABLE_KEYS}=17,
         *      {@link #TABLE_ITERATE}=18,
         *      {@link #TABLE_KEYS_ITERATE}=19,
         *      {@link #INDEX_ITERATE}=20,
         *      {@link #INDEX_KEYS_ITERATE}=21,
         *      {@link #MULTI_DELETE_TABLE}=22,
         *      {@link #MULTI_GET_BATCH}=23,
         *      {@link #MULTI_GET_BATCH_KEYS}=24,
         *      {@link #MULTI_GET_BATCH_TABLE}=25,
         *      {@link #MULTI_GET_BATCH_TABLE_KEYS}=26,
         *      {@link #PUT_BATCH}=27,
         *      {@link #QUERY_SINGLE_PARTITION}=28,
         *      {@link #QUERY_MULTI_PARTITION}=29,
         *      {@link #QUERY_MULTI_SHARD}=30 *&#47;
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    /* Throughput tracking (server side only) */

    /*
     * The resource tracker. If != null then tracking is enabled for this
     * operation.
     */
    private transient ResourceTracker tracker = null;

    /*
     * If true the read operation was made with absolute consistency or
     * the operation was a write.
     */
    private transient boolean isAbsolute = false;

    /*
     * The operation's read and write throughput in KB. If throughput tracking
     * is enabled (setResourceTracker has been called) these fields are used
     * to maintain a running total for this operation.
     */
    private transient int readKB = 0;
    private transient int writeKB = 0;

    /*
     * The amount of time (in msec) that the client is willing to wait
     * for a response from this operation. The value is a copy from
     * Request.timeoutMs
     */
    protected transient int timeoutMs;

    /**
     * All Operations must have an opcode associated with them.
     */
    private final OpCode opCode;

    /**
     * Assigns the opcode to the operation
     *
     * @param opCode
     */
    public InternalOperation(OpCode opCode) {
        this.opCode = checkNull("opCode", opCode);
        /*
         * Initialized to the client's version, but it's not used at the client
         * at all. It will be set to its "real" value by the deserializing
         * constructor below.
         */
        this.opSerialVersion = SerialVersion.CURRENT;
    }

    /**
     * FastExternalizable constructor.  Subclasses must call this constructor
     * before reading additional elements.
     *
     * The OpCode was read by readFastExternal.
     */
    InternalOperation(OpCode opCode,
                      @SuppressWarnings("unused") DataInput in,
                      short serialVersion) {

        this.opCode = checkNull("opCode", opCode);
        opSerialVersion = serialVersion;
    }

    /**
     * FastExternalizable factory for all InternalOperation subclasses.
     */
    public static InternalOperation readFastExternal(DataInput in,
                                                     short serialVersion)
        throws IOException {

        final OpCode op = OpCode.readFastExternal(in, serialVersion);
        return op.readOperation(in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link OpCode}) {@link #getOpCode opCode}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        opCode.writeFastExternal(out, serialVersion);
    }

    /**
     * Get this operation's opCode.
     *
     * @return the OpCode
     */
    public OpCode getOpCode() {
        return opCode;
    }

    /**
     * Returns true if this operation performs a read. Note that
     * some write operations also do reads, so performsRead() and
     * performsWrite() may both return true.
     *
     * The default implementation returns true (the common case), subclasses
     * should override as needed.
     *
     * @return true of this operation performs a read
     */
    public boolean performsRead() {
        return true;
    }

    /**
     * Returns true if this operation performs a write.
     *
     * The default implementation returns false (the common case), subclasses
     * should override as needed.
     *
     * @return true of this operation performs a write
     */
    public boolean performsWrite() {
        /* assert assures performsWrite is overriden for deletes */
        assert !isDelete();
        return false;
    }

    /**
     * Returns true if this operation is a delete operation.
     *
     * The default implementation returns false (the common case), subclasses
     * should override as needed.
     *
     * @return true of this operation is a delete operation
     */
    public boolean isDelete() {
        return false;
    }

    /**
     * Returns the table ID, or 0 if this is not a table operation.
     *
     * @return table ID of the operation or 0
     */
    public long getTableId() {
        return 0L;
    }

    public long[] getTableIds() {
        return null;
    }

    public boolean isTableOp() {
        return getTableId() != 0 || getTableIds() != null;
    }

    /**
     * Sets the resource tracker for this operation. If set, all read and
     * write throughput occurring during the operation are added to the
     * tracker.
     */
    public void setResourceTracker(ResourceTracker tracker,
                                   Consistency consistency) {
        assert this.tracker == null;
        this.tracker = tracker;
        this.isAbsolute = opCode.isWrite() ||
                          Consistency.ABSOLUTE.equals(consistency);
    }

    /**
     * Sets the resource tracker for this operation based on the specified
     * operation.
     */
    public void setResourceTracker(InternalOperation op) {
        this.tracker = op.tracker;
        this.isAbsolute = op.isAbsolute;
    }

    public boolean isAbsolute() {
        return isAbsolute;
    }

    /**
     * Gets the total KB read during this operation. If tracking was not
     * enabled for the operation 0 is returned.
     *
     * @return the total KB read
     */
    public int getReadKB() {
        return readKB;
    }

    /**
     * Gets the total KB written during this operation. If tracking was not
     * enabled for the operation 0 is returned.
     *
     * @return the total KB written
     */
    public int getWriteKB() {
        return writeKB;
    }

    public void addReadKB(int kb) {
        readKB += kb;
    }

    public void addWriteKB(int kb) {
        writeKB += kb;
    }

    /**
     * Records the specified number of bytes read. If tracking is enabled the
     * number of bytes is added to the read throughput tracking for this
     * operation. If tracking is not enabled the call is a noop.
     *
     * If the caching read cost is enabled add the read bytes rounded up to KB
     * to cachedReadKB rather than adding to read throughput tracking.
     *
     * @param bytes number of bytes read
     */
    public void addReadBytes(int bytes) {
        if (tracker != null) {
            if (enableCacheReadBytes) {
                cachedReadKB += toKBytes(bytes);
                return;
            }
            readKB += tracker.addReadBytes(bytes, isAbsolute);
        }
    }

    void addReadUnits(int units) {
        if (tracker != null) {
            tracker.addReadUnits(units);
        }
    }

    /**
     * This method beginAddReadBytes() is used to enable caching the read bytes
     * number in addReadBytes(), the cached readKB can be added to read
     * throughput tracking or discard using flushReadBytes(boolean).
     *
     * The beginAddReadBytes() and flushReadBytes() can be used to delay adding
     * the read bytes number to throughput tracking, finally add or discard all
     * the cached read bytes numbers.
     */
    void beginAddReadBytes() {
        enableCacheReadBytes = true;
        /* Clear the cache to discard the values not flushed yet. */
        cachedReadKB = 0;
    }

    /**
     * This method is to flush the cachedReadKB. If {@code add} is true, add the
     * cachedReadKB to throughput tracking, otherwise ignore and reset the
     * cachedReadKB.
     *
     * @param add set to true to add the cachedReadKB to read throughput
     * tracking.
     */
    void flushReadBytes(boolean add) {
        if (cachedReadKB == 0) {
            enableCacheReadBytes = false;
            return;
        }
        if (add) {
            readKB += tracker.addReadBytes(cachedReadKB * 1024, isAbsolute);
        }
        cachedReadKB = 0;
        enableCacheReadBytes = false;
    }

    /**
     * Records an empty return. This implementation adds MIN_READ to the
     * read bytes.
     */
    void addEmptyReadCharge() {
        addReadBytes(MIN_READ);
    }

    /**
     * Records MIN_READ to the read bytes.
     */
    public void addMinReadCharge() {
        addReadBytes(MIN_READ);
    }

    /**
     * Records the specified number of bytes written. If tracking is enabled the
     * number of bytes is added to the write throughput tracking for this
     * operation.
     *
     * Also adds the specified change in size to the tracker for the given
     * partition if the partition ID is not null.
     *
     * If tracking is not enabled the call is a noop.
     *
     * @param bytes number of bytes written
     * @param nIndexWrites the number of indexes (secondary DBs) which were
     * updated associated with the operation
     * @param partitionId the partition of the operation or null
     * @param deltaBytes the change in storage size
     */
    public void addWriteBytes(int bytes,
                              int nIndexWrites,
                              PartitionId partitionId,
                              int deltaBytes) {
        if (tracker != null) {
            writeKB += tracker.addWriteBytes(bytes, nIndexWrites, partitionId,
                                             deltaBytes);
        }
    }

    /**
     * Converts the specified number of bytes up to a number of Kbyte blocks
     */
    private int toKBytes(int bytes) {
        if (bytes == 0) {
            return 0;
        }
        int roundedKB = bytes / RW_BLOCK_SIZE;
        if ((bytes % RW_BLOCK_SIZE) != 0) {
            roundedKB++;
        }
        return roundedKB;
    }

    public int getReadKBToAdd(int bytes) {
        if (tracker != null) {
            return tracker.getReadKBToAdd(bytes, isAbsolute);
        }
        return 0;
    }

    public int getTimeout() {
        return timeoutMs;
    }

    public void setTimeout(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    /**
     * Overridden by non-LOB write operations to ensure that the key does
     * not have the LOB suffix currently in effect.
     *
     * @param lobSuffixBytes the byte representation of the LOB suffix in
     * effect
     *
     * @return null if the check passes, or the key bytes if it fails
     */
    public byte[] checkLOBSuffix(byte[] lobSuffixBytes) {
        return null;
    }

    /**
     * Returns a string describing this operation.
     *
     * @return the opcode of this operation
     */
    @Override
    public String toString() {
        return opCode.name();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof InternalOperation)) {
            return false;
        }
        final InternalOperation other = (InternalOperation) obj;
        return opCode == other.opCode;
    }

    @Override
    public int hashCode() {
        return opCode.hashCode();
    }

    /**
     * Writes a TimeToLive instance to the output stream in the format
     * documented by {@link TimeToLive#writeFastExternal}, and including
     * additional information if UnsupportedOperationException needs to be
     * thrown.
     */
    public static void writeTimeToLive(DataOutput out,
                                       short serialVersion,
                                       TimeToLive ttl,
                                       String operationName)
        throws IOException {

        writeTimeToLive(out, serialVersion, TimeToLive.getTTLValue(ttl),
                        TimeToLive.getTTLUnit(ttl), operationName);
    }

    /**
     * Writes a TTL value to the output stream in the format documented by
     * {@link TimeToLive#writeFastExternal}, and including additional
     * information if UnsupportedOperationException is thrown.
     */
    @SuppressWarnings("unused")
    public static void writeTimeToLive(DataOutput out,
                                       short serialVersion,
                                       int ttlVal,
                                       TimeUnit ttlUnit,
                                       String operationName)
        throws IOException {

        TimeToLive.writeFastExternal(out, serialVersion, ttlVal, ttlUnit);
    }

    /**
     * Returns the serial version of the operation coming from client.
     */
    short getOpSerialVersion() {
        return opSerialVersion;
    }

    /**
     * Common code to throw UnsupportedOperationException when a newer client
     * attempts to perform an operation against a server that does not support
     * it.  There is other common code in Request.writeExternal that does the
     * same thing on a per-operation basis.  This code is called when the
     * operation has conditional parameters that were added in a later version.
     * For example, Get, Put, Delete and their variants added a table id in V4.
     */
    @SuppressWarnings("unused")
    private static void throwVersionRequired(short serverVersion,
                                             short requiredVersion,
                                             String operationName) {
        throw new UnsupportedOperationException
            ("Attempting an operation that is not supported by " +
             "the server version.  Server version is " +
             SerialVersion.getKVVersion(serverVersion).getNumericVersionString()
             + ", required version is " +
             SerialVersion.getKVVersion(
                 requiredVersion).getNumericVersionString() +
             ", operation is " + operationName);
    }
}
