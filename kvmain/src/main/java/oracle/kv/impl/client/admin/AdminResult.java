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

package oracle.kv.impl.client.admin;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import oracle.kv.StatementResult;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TableIterator;

/**
 * AdminResult packages information about a ddl statement that is either
 * currently executing, or has completed. The AdminResult is returned from the
 * ExecutionFuture, so the AdminResult is the vehicle used to convey this
 * information to the API caller. It's basically a reformulation of the
 * ExecutionInfo class, which packages the info delivered from the Admin
 * service to client.
 *
 * In other words,
 *
 * {@literal
 *     kvclient <-- ExecutionInfo -- kvstore
 *
 *     application <-- AdminResult/StatementResult -- kvclient
 * }
 *
 * NOTE: until query work is fully supported StatementResult does not
 * extend {@literal Iterable<RecordValue>} so it must be added here so that
 * explicit casts in test code can work correctly. Query DML.
 */
class AdminResult implements StatementResult {

    private static TableIterator<RecordValue> EMPTY_ITERATOR = new
        EmptyIterator();

    private final boolean success;
    private final int planId;
    private final String info;
    private final String jsonInfo;
    private final String errorMessage;
    private final boolean isDone;
    private final boolean isCancelled;
    private final String result;

    AdminResult(int planId,
                ExecutionInfo executionInfo,
                boolean isDone,
                boolean isCancelled) {
        this.planId = planId;
        this.isDone = isDone;
        this.isCancelled = isCancelled;
        if (executionInfo == null) {
            if (planId == 0) {

                /*
                 * This is a no-execute operation, nothing more needs to be
                 * done.
                 */
                this.success = true;
                this.info =
                    "The statement did not require any additional execution";
            } else {
                /*
                 * This operation is in progress, but no execution info is
                 * available because the future was created by the Thrift
                 * proxy.
                 */
                this.success = false;
                this.info = null;
            }
            this.errorMessage = null;
            this.jsonInfo = null;
            this.result = null;
        } else {
            /* We have info from the server. */
            this.success = executionInfo.isSuccess();
            this.info = executionInfo.getInfo();
            this.jsonInfo = executionInfo.getJSONInfo();
            this.errorMessage = executionInfo.getErrorMessage();
            this.result = executionInfo.getResult();
        }
    }

    @Override
    public boolean isSuccessful() {
        return success;
    }

    @Override
    public int getPlanId() {
        return planId;
    }

    @Override
    public String getInfo() {
        return info;
    }

    @Override
    public String getInfoAsJson() {
        return jsonInfo;
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "AdminResult [success=" + success + ", planId=" + planId
            + ",\ninfo=" + info + ", \njsonInfo=" + jsonInfo
            + ",\nerrorMessage=" + errorMessage + ", isDone=" + isDone
            + ", isCancelled=" + isCancelled + ", result=" + result
            + "]";
    }

    @Override
    public boolean isDone() {
        return isDone;
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public String getResult() {
        return result;
    }

    @Override
    public Kind getKind() {
        return Kind.DDL;
    }

    @Override
    public void close() {

    }

    @Override
    public RecordDef getResultDef() {
        return null;
    }

    @Override
    public TableIterator<RecordValue> iterator() {
        return EMPTY_ITERATOR;
    }

    public static class EmptyIterator implements TableIterator<RecordValue> {

        @Override
        public void close() {}

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return Collections.emptyList();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return Collections.emptyList();
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public RecordValue next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {}
    }
}
