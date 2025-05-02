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

package oracle.kv.impl.pubsub;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.pubsub.StreamOperation;
import oracle.kv.txn.TransactionIdImpl;

/**
 * Object represents a transaction in NoSQL Stream
 */
public class StreamTxnEvent implements StreamOperation.TransactionEvent {

    private static final int MAX_NUM_OPS_IN_TXN = 1024 * 1024;

    private final SequenceId sequenceId;
    private final TransactionIdImpl txnId;
    private final TransactionType type;
    private final List<StreamOperation> ops;

    StreamTxnEvent(TransactionIdImpl txnId,
                   SequenceId sequenceId,
                   boolean commit,
                   List<StreamOperation> list) {
        this.txnId = txnId;
        this.sequenceId = sequenceId;
        this.ops = new ArrayList<>();
        this.type = commit ? TransactionType.COMMIT : TransactionType.ABORT ;
        list.forEach(this::addOp);
    }

    @Override
    public SequenceId getSequenceId() {
        return sequenceId;
    }

    @Override
    public int getRepGroupId() {
        return txnId.getShardId();
    }

    @Override
    public long getTableId() {
        throw new IllegalArgumentException("Not supported in transaction");
    }

    @Override
    public String getFullTableName() {
        throw new IllegalArgumentException("Not supported in transaction");
    }

    @Override
    public String getTableName() {
        throw new IllegalArgumentException("Not supported in transaction");
    }

    @Override
    public int getRegionId() {
        throw new IllegalArgumentException("Not supported in transaction");
    }

    @Override
    public long getLastModificationTime() {
        throw new IllegalArgumentException(
            "Last modification time not supported in transaction");
    }

    @Override
    public long getExpirationTime() {
        throw new IllegalArgumentException(
            "Expiration time not supported in transaction");
    }

    @Override
    public String toJsonString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(txnId)
          .append("[seq=").append(sequenceId).append("]")
          .append("[type=").append(type).append("]")
          .append("[#ops=").append(ops.size()).append("]")
          .append("\n");
        for (int idx = 0; idx < ops.size(); idx++) {
            final StreamOperation op = ops.get(idx);
            sb.append("\t")
              .append("[op=").append(idx).append("]")
              .append(op.toJsonString())
              .append("\n");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "Txn [seq=" + sequenceId +
               ", type=" + type +
               ", txn id=" + txnId +
               ", #ops="+ ops.size() + "]";
    }

    @Override
    public Type getType() {
        return Type.TRANSACTION;
    }

    @Override
    public PutEvent asPut() {
        throw new IllegalArgumentException("This operation is not a put");
    }

    @Override
    public DeleteEvent asDelete() {
        throw new IllegalArgumentException("This operation is not a delete");
    }

    @Override
    public TransactionEvent asTransaction() {
        return this;
    }

    @Override
    public TransactionIdImpl getTransactionId() {
        return txnId;
    }

    @Override
    public TransactionType getTransactionType() {
        return type;
    }

    @Override
    public long getNumOperations() {
        return ops.size();
    }

    @Override
    public List<StreamOperation> getOperations() {
        return ops;
    }

    private void addOp(StreamOperation op) {
        if (op.getType().equals(Type.TRANSACTION)) {
            throw new IllegalArgumentException(
                "Nested transaction is not supported");
        }
        if (!op.getType().equals(Type.PUT) &&
            !op.getType().equals(Type.DELETE)) {
            throw new IllegalArgumentException(
                "Unsupported operation in transaction");
        }
        if (ops.size() == MAX_NUM_OPS_IN_TXN) {
            throw new IllegalStateException(
                "Number of operations in transaction cannot be greater" +
                " than " + MAX_NUM_OPS_IN_TXN);
        }
        ops.add(op);
    }
}
