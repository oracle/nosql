/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.protocol;

import java.io.IOException;

import oracle.kv.Value;
import oracle.kv.impl.api.table.TableKey;
import oracle.nosql.proxy.RequestException;

/**
 * Protocol constants
 */
public class Protocol {

    public static final short V1 = 1;

    public static final short V2 = 2;

    /**
     * Add mode to table limits to support on demand capacity tables,
     * add durability, modtime, and ability to create free tables.
     */
    public static final short V3 = 3;

    public static final short V4 = 4;

    public static final short V5 = 5;

    /**
     * Serial version of the protocol. This changes as features and semantics
     * are changed or added
     *  4:
     *    o change to NSON
     *  5:
     *    o change to add serial version in the response header for clients
     *    o changed semantics of conditional put/delete to return existing row
     *    on success (not just failure) if requested
     *
     */
    public static final short SERIAL_VERSION = V5;
    public static final String SERIAL_VERSION_STRING = "5";

    public static final short QUERY_V1 = 1;

    public static final short QUERY_V2 = 2;

    /* Added query name in QueryRequest */
    public static final short QUERY_V4 = 4;

    /* Changes to VirtualScan, related to inner joins */
    public static final short QUERY_V5 = 5;

    /**
     * Serial version of the sub-protocol related to queries
     */
    public static final short CURRENT_QUERY_VERSION = 5;

    public static enum OpCode {
        DELETE(0),
        DELETE_IF_VERSION(1),
        GET(2),
        PUT(3),
        PUT_IF_ABSENT(4),
        PUT_IF_PRESENT(5),
        PUT_IF_VERSION(6),
        QUERY(7),
        PREPARE(8),
        WRITE_MULTIPLE(9),
        MULTI_DELETE(10),
        GET_TABLE(11),
        GET_INDEXES(12),
        GET_TABLE_USAGE(13),
        LIST_TABLES(14),
        TABLE_REQUEST(15),
        SCAN(16),   /* not supported as of 5.4 */
        INDEX_SCAN(17), /* not supported as of 5.4 */
        CREATE_TABLE(18),
        ALTER_TABLE(19),
        DROP_TABLE(20),
        CREATE_INDEX(21),
        DROP_INDEX(22),
        /* added in V2 */
        SYSTEM_REQUEST(23),
        SYSTEM_STATUS_REQUEST(24),

        /* added for rest API */
        CHANGE_COMPARTMENT(25),
        GET_INDEX(26),
        SUMMARIZE(27),
        LIST_WORKREQUESTS(28),
        GET_WORKREQUEST(29),
        GET_WORKREQUEST_LOGS(30),
        GET_WORKREQUEST_ERRORS(31),
        CANCEL_WORKREQUEST(32),

        /* added for MR table */
        ADD_REPLICA(33),
        DROP_REPLICA(34),
        GET_REPLICA_STATS(35),
        INTERNAL_DDL(36),
        INTERNAL_STATUS(37);

        private static final OpCode[] VALUES = values();
        OpCode(int code) {
            if (code != ordinal()) {
                throw new IllegalArgumentException("Wrong op code");
            }
        }

        public static OpCode getOP(int code) throws IOException {
            try {
                return VALUES[code];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException("unknown opcode: " + code);
            }
        }

        public static boolean isPut(int code) {
            return code >= 3 && code <= 6;
        }

        public static boolean isDel(int code) {
            return code <= 1;
        }

        public static boolean isTMOperation(OpCode op) {
            if (op == LIST_TABLES ||
                op == CREATE_TABLE ||
                op == ALTER_TABLE ||
                op == DROP_TABLE ||
                op == CHANGE_COMPARTMENT ||
                op == GET_TABLE ||
                op == GET_TABLE_USAGE ||
                op == CREATE_INDEX ||
                op == DROP_INDEX ||
                op == ADD_REPLICA ||
                op == DROP_REPLICA ||
                op == INTERNAL_DDL ||
                op == INTERNAL_STATUS ||
                op == GET_REPLICA_STATS) {
                return true;
            }
            return false;
        }

        public static boolean isWorkRequestOp(OpCode op) {
            if (op.ordinal() >= LIST_WORKREQUESTS.ordinal() &&
                op.ordinal() <= CANCEL_WORKREQUEST.ordinal()) {
                return true;
            }
            return false;
        }

        public static boolean isDataOp(OpCode op) {
            if (op == DELETE ||
                op == DELETE_IF_VERSION ||
                op == GET ||
                op == PUT ||
                op == PUT_IF_ABSENT ||
                op == PUT_IF_PRESENT ||
                op == PUT_IF_VERSION ||
                op == WRITE_MULTIPLE ||
                op == MULTI_DELETE ||
                op == QUERY ||
                op == PREPARE ||
                op == SUMMARIZE ||
                op == SCAN ||
                op == INDEX_SCAN) {
                return true;
            }
            return false;
        }

        public static boolean isDdlOp(OpCode op) {
            return (op == OpCode.CREATE_TABLE ||
                    op == OpCode.ALTER_TABLE ||
                    op == OpCode.DROP_TABLE ||
                    op == OpCode.CHANGE_COMPARTMENT ||
                    op == OpCode.ADD_REPLICA ||
                    op == OpCode.DROP_REPLICA ||
                    op == OpCode.CREATE_INDEX ||
                    op == OpCode.DROP_INDEX);
        }
    }

    /*
     * Response error codes (must be non-zero)
     */
    public static final int NO_ERROR = 0;

    /*
     * Error code range constants
     */
    public static final int USER_ERROR_BEGIN = 1;
    public static final int USER_ERROR_END = 49;
    public static final int THROTTLING_ERROR_BEGIN = 50;
    public static final int THROTTLING_ERROR_END = 99;
    public static final int SERVER_ERROR_BEGIN = 100;
    public static final int SERVER_RETRYABLE_ERROR_BEGIN = SERVER_ERROR_BEGIN;
    public static final int SERVER_RETRYABLE_ERROR_END = 124;
    public static final int SERVER_OTHER_ERROR_BEGIN = 125;

    /*
     * Error codes for user-generated errors, range from 1 to 50(exclusive).
     * These include illegal arguments, exceeding size limits for some objects,
     * resource not found, etc.
     */
    public static final int UNKNOWN_OPERATION = 1;
    public static final int TABLE_NOT_FOUND = 2;
    public static final int INDEX_NOT_FOUND = 3;
    public static final int ILLEGAL_ARGUMENT = 4;
    public static final int ROW_SIZE_LIMIT_EXCEEDED = 5;
    public static final int KEY_SIZE_LIMIT_EXCEEDED = 6;
    public static final int BATCH_OP_NUMBER_LIMIT_EXCEEDED = 7;
    public static final int REQUEST_SIZE_LIMIT_EXCEEDED = 8;
    public static final int TABLE_EXISTS = 9;
    public static final int INDEX_EXISTS = 10;
    public static final int INVALID_AUTHORIZATION = 11;
    public static final int INSUFFICIENT_PERMISSION = 12;
    public static final int RESOURCE_EXISTS = 13;
    public static final int RESOURCE_NOT_FOUND = 14;
    public static final int TABLE_LIMIT_EXCEEDED = 15;
    public static final int INDEX_LIMIT_EXCEEDED = 16;
    public static final int BAD_PROTOCOL_MESSAGE = 17;
    public static final int EVOLUTION_LIMIT_EXCEEDED = 18;
    public static final int TABLE_DEPLOYMENT_LIMIT_EXCEEDED = 19;
    public static final int TENANT_DEPLOYMENT_LIMIT_EXCEEDED = 20;
    /* added in V2 */
    public static final int OPERATION_NOT_SUPPORTED = 21;
    public static final int ETAG_MISMATCH = 22;
    public static final int CANNOT_CANCEL_WORK_REQUEST = 23;
    /* added in V3 */
    public static final int UNSUPPORTED_PROTOCOL = 24;
    public static final int INVALID_RETRY_TOKEN = 25;
    /* added in V4 */
    public static final int TABLE_NOT_READY = 26;
    public static final int UNSUPPORTED_QUERY_VERSION = 27;
    /* added in V5 */
    public static final int RECOMPILE_QUERY = 28;

    /*
     * Error codes for user throttling, range from 50 to 100(exclusive).
     */
    public static final int READ_LIMIT_EXCEEDED = 50;
    public static final int WRITE_LIMIT_EXCEEDED = 51;
    public static final int SIZE_LIMIT_EXCEEDED = 52;
    public static final int OPERATION_LIMIT_EXCEEDED = 53;

    /*
     * Error codes for server issues, range from 100 to 150(exclusive).
     */

    /*
     * Retry-able server issues, range from 100 to 125(exclusive).
     * These are internal problems, presumably temporary, and need to be sent
     * back to the application for retry.
     */
    public static final int REQUEST_TIMEOUT = 100;
    public static final int SERVER_ERROR = 101;
    public static final int SERVICE_UNAVAILABLE = 102;
    public static final int SECURITY_INFO_UNAVAILABLE = 104;
    /* added in V2 */
    public static final int RETRY_AUTHENTICATION = 105;

    /*
     * Other server issues, begin from 125.
     * These include server illegal state, unknown server error, etc.
     * They might be retry-able, or not.
     */
    public static final int UNKNOWN_ERROR = 125;
    public static final int ILLEGAL_STATE = 126;

    /*
     * Return true if the errorCode means a user-generated error.
     */
    public static boolean isUserFailure(int errorCode) {
        return errorCode >= USER_ERROR_BEGIN &&
               errorCode <= USER_ERROR_END;
    }

    /*
     * Return true if the errorCode means a failure caused by user limits.
     */
    public static boolean isUserThrottling(int errorCode) {
        return errorCode >= THROTTLING_ERROR_BEGIN &&
               errorCode <= THROTTLING_ERROR_END;
    }

    /*
     * Return true if the errorCode means a server failure.
     */
    public static boolean isServerFailure(int errorCode) {
        return errorCode >= SERVER_ERROR_BEGIN;
    }

    /*
     * Return true if the errorCode means a retry-able server failure.
     */
    public static boolean isServerRetryableFailure(int errorCode) {
        return errorCode >= SERVER_RETRYABLE_ERROR_BEGIN &&
               errorCode <= SERVER_RETRYABLE_ERROR_END;
    }

    /*
     * The max number of table usage records returned in getTableUsage response
     * with size limit of REQUEST_SIZE_LIMIT, see below for the table usage
     * record layout and max size of each field:
     *
     *  |tenantId|tableName|numberOfRecords|usageRecord1|...|usageRecordN|
     *
     *      tenantId: 128, TableImpl.MAX_NAMESPACE_LENGTH
     *      tableName: 256, TableImpl.MAX_ID_LENGTH
     *      numberOfRecords: 5, packed int
     *      usageRecord: (9 + 8 * 5), 1 packed long + 8 packed ints.
     *
     * So the number limit is approximately calculated as:
     *
     *  numberLimit = (REQUEST_SIZE_LIMIT(default 2MB)
     *                   - 128(tenantId)
     *                   - 256(tableName)
     *                   - 5(numberOfRecords)) / 49(record size)
     *              = 42791
     */
    public static final int TABLE_USAGE_NUMBER_LIMIT = 42791;

    /* TODO: may compute max number of records with max request size? */
    public static final int REPLICA_STATS_LIMIT = 1000;

    public static void checkKeySize(TableKey key, int keySizeLimit) {
        if (keySizeLimit < 0) {
            return;
        }

        /*
         * The total size of key components excluding the components of tableID.
         */
        int keySize = key.getKeySize(true /* skipTableId */);
        if (keySize > keySizeLimit) {
            throw new RequestException(KEY_SIZE_LIMIT_EXCEEDED,
                                       "Primary key of " + keySize +
                                       " exceeded the limit of " +
                                       keySizeLimit);
        }
    }

    public static void checkValueSize(Value value, int valueSizeLimit) {
        if (valueSizeLimit < 0) {
            return;
        }

        final int valueSize = value.getValue().length;
        if (valueSize > valueSizeLimit) {
            throw new RequestException(ROW_SIZE_LIMIT_EXCEEDED,
                                       "Value size of " + valueSize +
                                       " exceeded the limit of " +
                                       valueSizeLimit);
        }
    }
}
