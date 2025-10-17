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

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.Consistency;
import oracle.kv.Key;
import oracle.kv.Operation;
import oracle.kv.OperationFactory;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * An Execute operation performs a sequence of put and delete operations.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class Execute extends InternalOperation {

    /**
     * The operations to execute.
     */
    private final List<OperationImpl> ops;

    /**
     * Table operations include the table id.  0 means no table.
     */
    private final long tableId;

    /**
     * Constructs an execute operation.
     */
    public Execute(List<OperationImpl> ops, long tableId) {
        super(OpCode.EXECUTE);
        checkNull("ops", ops);
        ops.forEach(op -> checkNull("Operation list element", op));
        this.ops = ops;
        this.tableId = tableId;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    Execute(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.EXECUTE, in, serialVersion);
        final int opsSize = readNonNullSequenceLength(in);
        ops = new ArrayList<>(opsSize);
        for (int i = 0; i < opsSize; i += 1) {
            ops.add(new OperationImpl(in, serialVersion));
        }
        tableId = in.readLong();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link InternalOperation}) {@code super}
     * <li> ({@link SerializationUtil#writeNonNullCollection non-null
     *      collection}) {@link #getOperations ops}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeNonNullCollection(out, serialVersion, ops);
        out.writeLong(tableId);
    }

    public List<OperationImpl> getOperations() {
        return ops;
    }

    @Override
    public long getTableId() {
        return tableId;
    }

    @Override
    public boolean performsRead() {
        for (OperationImpl op : ops) {
            if (op.getInternalOp().performsRead()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean performsWrite() {
        return true;
    }

    @Override
    public boolean isDelete() {
        for (OperationImpl op : ops) {
            if (op.getInternalOp().isDelete()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setResourceTracker(ResourceTracker tracker,
                                     Consistency consistency) {
        super.setResourceTracker(tracker, consistency);
        for (OperationImpl op : ops) {
            op.getInternalOp().setResourceTracker(tracker, consistency);
        }
    }

    @Override
    public String toString() {
        return super.toString() + " Ops: " + ops;
    }

    /**
     * Implementation of Operation, the unit of work for the execute() method,
     * and wrapper for the corresponding SingleKeyOperation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public static class OperationImpl
        implements Operation, FastExternalizable {

        private final Key key; /* Not serialized. */
        private final boolean abortIfUnsuccessful;
        private final SingleKeyOperation internalOp;

        OperationImpl(Key key,
                      boolean abortIfUnsuccessful,
                      SingleKeyOperation internalOp) {
            checkNull("internalOp", internalOp);
            this.key = key;
            this.abortIfUnsuccessful = abortIfUnsuccessful;
            this.internalOp = internalOp;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        OperationImpl(DataInput in, short serialVersion)
            throws IOException {

            key = null;
            abortIfUnsuccessful = in.readBoolean();
            internalOp = (SingleKeyOperation)
                InternalOperation.readFastExternal(in, serialVersion);
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link DataOutput#writeBoolean boolean}) {@link
         *      #getAbortIfUnsuccessful abortIfUnsuccessful}
         * <li> ({@link SingleKeyOperation}) {@link #getInternalOp internalOp}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeBoolean(abortIfUnsuccessful);
            internalOp.writeFastExternal(out, serialVersion);
        }

        public SingleKeyOperation getInternalOp() {
            return internalOp;
        }

        /**
         * Because the Key is not serialized, this method will always throw an
         * IllegalStateException on the service-side of the RMI interface.
         * Internally, SingleKeyOperation.getKeyBytes should be called instead
         * of getKey, which is only intended for use by the client.
         */
        @Override
        public Key getKey() {
            if (key == null) {
                throw new IllegalStateException();
            }
            return key;
        }

        @Override
        public Operation.Type getType() {
            return internalOp.getOpCode().getExecuteType();
        }

        @Override
        public boolean getAbortIfUnsuccessful() {
            return abortIfUnsuccessful;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public static List<OperationImpl> downcast(List<Operation> ops) {
            /* Downcast: all Operations are OperationImpls. */
            return (List) ops;
        }

        @Override
        public String toString() {
            return String.format("Execute(%s)", internalOp);
        }
    }

    /**
     * OperationFactory implementation which also supports creating
     * table operations.
     */
    public static class OperationFactoryImpl implements OperationFactory {

        private final KeySerializer keySerializer;

        public OperationFactoryImpl(KeySerializer keySerializer) {
            this.keySerializer = keySerializer;
        }

        @Override
        public Operation createPut(Key key, Value value) {
            return createPut(key, value, null, false, 0L);
        }

        @Override
        public Operation createPut(Key key,
                                   Value value,
                                   ReturnValueVersion.Choice prevReturn,
                                   boolean abortIfUnsuccessful) {
            return createPut(key, value, prevReturn, abortIfUnsuccessful, 0L);
        }

        public Operation createPut(Key key,
                                   Value value,
                                   ReturnValueVersion.Choice prevReturn,
                                   boolean abortIfUnsuccessful,
                                   long tableId) {
            return new OperationImpl
                (key, abortIfUnsuccessful,
                 new Put(keySerializer.toByteArray(key), value,
                         (prevReturn != null) ? prevReturn :
                                                ReturnValueVersion.Choice.NONE,
                         tableId));
        }

        @Override
        public Operation createPutIfAbsent(Key key, Value value) {
            return createPutIfAbsent(key, value, null, false, 0L);
        }

        @Override
        public Operation
            createPutIfAbsent(Key key,
                              Value value,
                              ReturnValueVersion.Choice prevReturn,
                              boolean abortIfUnsuccessful) {
            return createPutIfAbsent(key, value,
                                     prevReturn, abortIfUnsuccessful, 0L);
        }

        public Operation
            createPutIfAbsent(Key key,
                              Value value,
                              ReturnValueVersion.Choice prevReturn,
                              boolean abortIfUnsuccessful,
                              long tableId) {
            return new OperationImpl
                (key, abortIfUnsuccessful,
                 new PutIfAbsent(keySerializer.toByteArray(key), value,
                                 (prevReturn != null) ?
                                                 prevReturn :
                                                 ReturnValueVersion.Choice.NONE,
                                 tableId));
        }

        @Override
        public Operation createPutIfPresent(Key key, Value value) {
            return createPutIfPresent(key, value, null, false, 0L);
        }

        @Override
        public Operation
            createPutIfPresent(Key key,
                               Value value,
                               ReturnValueVersion.Choice prevReturn,
                               boolean abortIfUnsuccessful) {
            return createPutIfPresent(key, value,
                                      prevReturn, abortIfUnsuccessful, 0L);
        }

        public Operation
            createPutIfPresent(Key key,
                               Value value,
                               ReturnValueVersion.Choice prevReturn,
                               boolean abortIfUnsuccessful,
                               long tableId) {
            return new OperationImpl
                (key, abortIfUnsuccessful,
                 new PutIfPresent(keySerializer.toByteArray(key), value,
                                  (prevReturn != null) ?
                                  prevReturn :
                                  ReturnValueVersion.Choice.NONE,
                                  tableId));
        }

        @Override
        public Operation createPutIfVersion(Key key,
                                            Value value,
                                            Version version) {
            return createPutIfVersion(key, value, version, null, false, 0L);
        }

        @Override
        public Operation
            createPutIfVersion(Key key,
                               Value value,
                               Version version,
                               ReturnValueVersion.Choice prevReturn,
                               boolean abortIfUnsuccessful) {
            return createPutIfVersion(key, value, version,
                                      prevReturn, abortIfUnsuccessful, 0L);
        }

        public Operation
            createPutIfVersion(Key key,
                               Value value,
                               Version version,
                               ReturnValueVersion.Choice prevReturn,
                               boolean abortIfUnsuccessful,
                               long tableId) {
            return new OperationImpl
                (key, abortIfUnsuccessful,
                 new PutIfVersion(keySerializer.toByteArray(key), value,
                                  (prevReturn != null) ?
                                  prevReturn :
                                  ReturnValueVersion.Choice.NONE,
                                  version,
                                  tableId));
        }

        @Override
        public Operation createDelete(Key key) {
            return createDelete(key, null, false);
        }

        @Override
        public Operation createDelete(Key key,
                                      ReturnValueVersion.Choice prevReturn,
                                      boolean abortIfUnsuccessful) {
            return createDelete(key, prevReturn, abortIfUnsuccessful, 0L,
                                false /* doTombstone */, null /* rowMetadata */);
        }

        public Operation createDelete(Key key,
                                      ReturnValueVersion.Choice prevReturn,
                                      boolean abortIfUnsuccessful,
                                      long tableId,
                                      boolean doTombstone,
                                      String rowMetadata) {
            return new OperationImpl
                (key, abortIfUnsuccessful,
                 new Delete(keySerializer.toByteArray(key),
                            (prevReturn != null) ?
                            prevReturn :
                            ReturnValueVersion.Choice.NONE,
                            tableId,
                            doTombstone,
                            rowMetadata));
        }

        @Override
        public Operation createDeleteIfVersion(Key key, Version version) {
            return createDeleteIfVersion(key, version, null, false);
        }

        @Override
        public Operation
            createDeleteIfVersion(Key key,
                                  Version version,
                                  ReturnValueVersion.Choice prevReturn,
                                  boolean abortIfUnsuccessful) {
            return createDeleteIfVersion(key, version, prevReturn,
                                         abortIfUnsuccessful, 0L,
                                         false /* doTombstone */,
                                         null /* rowMetadata */);
        }

        public Operation
            createDeleteIfVersion(Key key,
                                  Version version,
                                  ReturnValueVersion.Choice prevReturn,
                                  boolean abortIfUnsuccessful,
                                  long tableId,
                                  boolean doTombstone,
                                  String rowMetadata) {
            return new OperationImpl
                (key, abortIfUnsuccessful,
                 new DeleteIfVersion(keySerializer.toByteArray(key),
                                     (prevReturn != null) ?
                                     prevReturn :
                                     ReturnValueVersion.Choice.NONE,
                                     version,
                                     tableId,
                                     doTombstone,
                                     rowMetadata));
        }
    }
}
