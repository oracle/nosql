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
import static oracle.kv.impl.util.SerialVersion.CREATION_TIME_VER;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_14;
import static oracle.kv.impl.util.SerializationUtil.toDeserializedForm;
import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullCollection;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationResult;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldDefSerialization;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FieldValueSerialization;
import oracle.kv.impl.api.table.SequenceImpl.SGAttrsAndValues;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.FieldValue;

/**
 * The result of running a request.  Result may contain a return value of the
 * request.  It may also contain an error, an update to some topology
 * information, or information about how the request was satisfied (such as the
 * forwarding path it took).
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class Result
    implements OperationResult, FastExternalizable {

    /* The number of KB read and written during this operation */
    private final int readKB;
    private final int writeKB;

    /**
     * The OpCode determines the result type for deserialization, and may be
     * useful for matching to the request OpCode.
     */
    private final OpCode opCode;

    /*
     * The TableMatadata sequential number of RN on which the request is
     * executed.
     */
    private int metadataSeqNum;

    /**
     * Constructs a request result that contains a value resulting from an
     * operation.
     */
    private Result(OpCode op, int readKB, int writeKB) {
        opCode = op;
        assert op.checkResultType(this) :
        "Incorrect type " + getClass().getName() + " for " + op;
        this.readKB = readKB;
        this.writeKB = writeKB;
        this.metadataSeqNum = 0;
    }

    /** Copy constructor used to implement deserializedForm */
    private Result(Result other) {
        this(other.opCode, other.readKB, other.writeKB);
    }

    /**
     * FastExternalizable constructor.  Subclasses must call this constructor
     * before reading additional elements.
     *
     * The OpCode was read by readFastExternal.
     */
    Result(OpCode op,  int readKB, int writeKB,
           @SuppressWarnings("unused") DataInput in,
           @SuppressWarnings("unused") short serialVersion) {

        this(op, readKB, writeKB);
    }

    /**
     * FastExternalizable factory for all Result subclasses.
     */
    public static Result readFastExternal(DataInput in, short serialVersion)
        throws IOException {

        final OpCode op = OpCode.readFastExternal(in, serialVersion);
        final int readKB = in.readInt();
        final int writeKB = in.readInt();
        final int seqNum = in.readInt();
        final Result result =
            op.readResult(in, readKB, writeKB, serialVersion);
        result.setMetadataSeqNum(seqNum);
        return result;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link OpCode}) <i>opCode</i>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        opCode.writeFastExternal(out, serialVersion);
        out.writeInt(readKB);
        out.writeInt(writeKB);
        out.writeInt(metadataSeqNum);
    }

    /**
     * Gets the boolean result for all operations.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    @Override
    public abstract boolean getSuccess();

    /**
     * Returns the number of KB read during this operation. It may include
     * records read from the store but not returned in this result.
     * @return the number of KB read during this operation
     */
    public int getReadKB() {
        return readKB;
    }

    /**
     * Returns the number of KB written during this operation.
     * @return the number of KB written during this operation
     */
    public int getWriteKB() {
        return writeKB;
    }

    /**
     * Sets the metadata sequence number.
     */
    public void setMetadataSeqNum(int seqNum) {
        metadataSeqNum = seqNum;
    }

    /**
     * Returns a metadata sequence number if the operation was a table
     * operation. If the operation was for a specific table, the sequence
     * number for that table is returned. If the operation was not for a
     * specific table, or a batch operation, the sequence number of the overall
     * metadata is returned.
     *
     * The sequence number is what is present in the RN that handled the
     * operation.
     *
     * @return a metadata sequence number
     */
    public int getMetadataSeqNum() {
        return metadataSeqNum;
    }

    /**
     * Get the primary-index key to be used as the starting point for
     * the primary index scan that will produce the next result set.
     */
    public byte[] getPrimaryResumeKey() {
       throw new IllegalStateException(
           "result of type: " + getClass() +
           " does not contain a primary resume key");
    }

    /**
     * Get the secondary-index key to be used as the starting point for
     * the secondary index scan that will produce the next result set.
     */
    public byte[] getSecondaryResumeKey() {
       throw new IllegalStateException(
           "result of type: " + getClass() +
           " does not contain a secondary resume key");
    }

    /**
     * Gets the current Value result of a Get, Put or Delete operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    @Override
    public Value getPreviousValue() {
        throw new IllegalStateException
            ("result of type: " + getClass() + " does not contain a Value");
    }

    /**
     * Gets the current Version result of a Get, Put or Delete operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    @Override
    public Version getPreviousVersion() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a previous Version");
    }

    /**
     * Gets the new Version result of a Put operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    @Override
    public Version getNewVersion() {
        throw new IllegalStateException("result of type: " + getClass() +
                                        " does not contain a new Version");
    }

    /**
     * Gets the new expiration time a Put operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    @Override
    public long getNewExpirationTime() {
        throw new IllegalStateException("result of type: " + getClass() +
                                        " does not contain an " +
                                        "expiration time");
    }

    @Override
    public long getPreviousExpirationTime() {
        throw new IllegalStateException("result of type: " + getClass() +
                                        " does not contain a " +
                                        "previous expiration time");
    }

    @Override
    public long getPreviousCreationTime() {
        throw new IllegalStateException("result of type: " + getClass() +
            " does not contain a previous creation time");
    }

    @Override
    public long getPreviousModificationTime() {
        throw new IllegalStateException("result of type: " + getClass() +
                                        " does not contain a " +
                                        "previous modification time");
    }

    @Override
    public long getNewCreationTime() {
        throw new IllegalStateException("result of type: " + getClass() +
            " does not contain a " +
            "new creation time");
    }

    @Override
    public long getNewModificationTime() {
        throw new IllegalStateException("result of type: " + getClass() +
            " does not contain a " +
            "new modification time");
    }

    @Override
    public int getNewStorageSize() {
        throw new IllegalStateException("result of type: " + getClass() +
                                        " does not contain a " +
                                        "new storage size");
    }

    @Override
    public int getPreviousStorageSize() {
        throw new IllegalStateException("result of type: " + getClass() +
                                        " does not contain a " +
                                        "previous storage size");
    }

    @Override
    public FieldValue getGeneratedValue() {
        throw new IllegalStateException("result of type: " + getClass() +
                                        " does not contain a " +
                                        "generated value");
    }

    /**
     * Gets the int result of a MultiDelete operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public int getNDeletions() {
        throw new IllegalStateException
            ("result of type: " + getClass() + " does not contain a boolean");
    }

    /**
     * Gets the OperationExecutionException result of an Execute operation, or
     * null if no exception should be thrown.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public OperationExecutionException
        getExecuteException(@SuppressWarnings("unused") List<Operation> ops) {

        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain an OperationExecutionException");
    }

    /**
     * Gets the OperationResult list result of an Execute operation, or null if
     * an OperationExecutionException should be thrown.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public List<OperationResult> getExecuteResult() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a ExecuteResult");
    }

    /**
     * Gets the ResultKeyValueVersion list result of an iterate operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public List<ResultKeyValueVersion> getKeyValueVersionList() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a ResultKeyValueVersion list");
    }

    /**
     * Gets the key list result of an iterate-keys operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public List<ResultKey> getKeyList() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a key list");
    }

    /**
     * Gets the ResultIndexKeys list result of a table index keys
     * iterate operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public List<ResultIndexKeys> getIndexKeyList() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a ResultIndexKeys list");
    }

    /**
     * Gets the ResultIndexRows list result of a table index row
     * iterate operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public List<ResultIndexRows> getIndexRowList() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a ResultIndexRows list");
    }

    /**
     * Gets the ResultRecord list of a query operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     *
     * @since ???
     */
    public List<FieldValueImpl> getQueryResults() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " is not a query result");
    }

    /**
     * Gets the has-more-elements result of an iterate or iterate-keys
     * operation. True returned if the iteration is complete.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public boolean hasMoreElements() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain an iteration result");
    }

    /**
     * Gets the parent key index to start from in multi-get-batch or
     * multi-get-batch-keys operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public int getResumeParentKeyIndex() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a resume parent key index");
    }

    /**
     * The number of records returned or processed as part of this operation.
     * Single operations only apply to one record, but the multi, iterate, or
     * execute operations will work on multiple records, and should override
     * this to provide the correct number of operations.
     */
    public int getNumRecords() {
        return 1;
    }

    @SuppressWarnings("unused")
    public void setGeneratedValue(FieldValue value) {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not allow setGeneratedValue");
    }

    @Override
    public String toString() {
        return "Result[" + opCode +
            " numRecords=" + getNumRecords() +
            " readKB=" + readKB +
            " writeKB=" + writeKB + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Result)) {
            return false;
        }
        final Result other = (Result) obj;
        return (readKB == other.readKB) &&
            (writeKB == other.writeKB) &&
            (opCode == other.opCode) &&
            (metadataSeqNum == other.metadataSeqNum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readKB, writeKB, opCode, metadataSeqNum);
    }

    /**
     * The result of a Get operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public static class GetResult extends ValueVersionResult {

        public GetResult(OpCode opCode,
                         int readKB, int writeKB,
                         ResultValueVersion valueVersion) {
            super(opCode, readKB, writeKB, valueVersion);
        }

        /** Constructor to implement deserializedForm */
        private GetResult(GetResult other, short serialVersion) {
            super(other, serialVersion);
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        GetResult(OpCode opCode,
                  int readKB, int writeKB,
                  DataInput in, short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);
        }

        @Override
        public boolean getSuccess() {
            return getPreviousValue() != null;
        }

        @Override
        public GetResult deserializedForm(short serialVersion) {
            return new GetResult(this, serialVersion);
        }
    }

    /**
     * The result of a Put operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public static class PutResult extends ValueVersionResult {

        private final Version newVersion;  /* of the new record */
        private final long newExpirationTime; /* of the new record */
        // There is creationTime in prevValue, they should both be the same.
        private final long newCreationTime;
        private final long newModificationTime;
        private final int newStorageSize;
        private final int shard;

        /*
         * Added in 18.3 to say, in the case of SQL UPSERT, whether the
         * row was updated (true) or inserted (false).
         */
        private final boolean wasUpdate;

        /*
         * Added in 19.3 to return a generated value for an identity column.
         * This value is never serialized; it is local to the client and
         * set after the operation has completed.
         */
        private FieldValue generatedValue;

        /**
         * Constructs a result with required arguments.
         *
         * @param opCode code for operation that produced this result
         * @param prevVal prior value, can be null
         * @param version the new version and expiration time of the record
         * being put. May be null if the operation failed.
         */
        PutResult(OpCode opCode,
                  int readKB,
                  int writeKB,
                  ResultValueVersion prevVal,
                  Version version,
                  long expTime,
                  boolean wasUpdate,
                  long creationTime,
                  long modificationTime,
                  int storageSize,
                  int shard) {

            super(opCode, readKB, writeKB, prevVal);

            this.wasUpdate = wasUpdate;
            newVersion = version;
            newExpirationTime = expTime;
            newCreationTime = creationTime;
            newModificationTime = modificationTime;
            newStorageSize = storageSize;
            this.shard = shard;
        }

        /** Constructor to implement deserializedForm */
        private PutResult(PutResult other, short serialVersion) {
            super(other, serialVersion);
            wasUpdate = other.wasUpdate;
            newVersion = other.newVersion;
            newExpirationTime = other.newExpirationTime;
            newCreationTime = other.newCreationTime;
            newModificationTime = other.newModificationTime;
            newStorageSize = other.newStorageSize;
            shard = other.shard;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        PutResult(OpCode opCode,
                  int readKB,
                  int writeKB,
                  DataInput in,
                  short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);
            if (in.readByte() != 0) {
                newVersion =  Version.createVersion(in, serialVersion);
            } else {
                newVersion = null;
            }
            newExpirationTime = readTimestamp(in, serialVersion);
            wasUpdate = in.readBoolean();
            newModificationTime = readTimestamp(in, serialVersion);

            newStorageSize = in.readInt();
            shard = in.readInt();
            if (serialVersion >= CREATION_TIME_VER) {
                newCreationTime = in.readLong();
            } else {
                newCreationTime = 0;
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link ValueVersionResult}) {@code super}
         * <li> ({@link SerializationUtil#writeFastExternalOrNull Version or
         *      null} {@link #getNewVersion newVersion}
         * <li> <i>[Optional]</i> ({@link DataOutput#writeBoolean boolean})
         *      <i>whether expirationTime is present</i>
         * <li> <i>[Optional]</i> ({@link DataOutput#writeLong long}) {@link
         *      #getNewExpirationTime newExpirationTime}
         * <li> ({@link DataOutput#writeBoolean boolean})
         *      {@link #getWasUpdate wasUpdate}
         * <li> <i>[Optional]</i>({@link DataOutput#writeBoolean boolean})
         *      <i>whether newModificationTime is present</i>
         * <li> <i>[Optional]</i>({@link DataOutput#writeLong long})
         *      {@link #getNewModificationTime newModificationTime}
         * <li> <i>[Contingent on serialVersion]</i>({@link DataOutput#writeLong long})
         *      {@link #getNewCreationTime creationTime}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
            super.writeFastExternal(out, serialVersion);
            writeFastExternalOrNull(out, serialVersion, newVersion);

            writeTimestamp(out,
                           newExpirationTime,
                           serialVersion);
            out.writeBoolean(wasUpdate);
            writeTimestamp(out,
                           newModificationTime,
                           serialVersion);

            out.writeInt(newStorageSize);
            out.writeInt(shard);
            if (serialVersion >= CREATION_TIME_VER) {
                out.writeLong(newCreationTime);
            }
        }

        @Override
        public boolean getSuccess() {
            return newVersion != null;
        }

        @Override
        public Version getNewVersion() {
            return newVersion;
        }

        @Override
        public long getNewExpirationTime() {
            return newExpirationTime;
        }

        @Override
        public FieldValue getGeneratedValue() {
            return generatedValue;
        }

        @Override
        public void setGeneratedValue(FieldValue value) {
            generatedValue = value;
        }

        public boolean getWasUpdate() {
            return wasUpdate;
        }

        @Override
        public long getNewCreationTime() {
            return newCreationTime;
        }

        @Override
        public long getNewModificationTime() {
            return newModificationTime;
        }

        @Override
        public int getNewStorageSize() {
            return newStorageSize;
        }

        public int getShard() {
            return shard;
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof PutResult)) {
                return false;
            }
            final PutResult other = (PutResult) obj;
            return Objects.equals(newVersion, other.newVersion) &&
                (newExpirationTime == other.newExpirationTime) &&
                (newCreationTime == other.newCreationTime) &&
                (newModificationTime == other.newModificationTime) &&
                (newStorageSize == other.newStorageSize) &&
                (shard == other.shard) &&
                (wasUpdate == other.wasUpdate) &&
                Objects.equals(generatedValue, other.generatedValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(newVersion, newExpirationTime, newCreationTime,
                                newModificationTime, newStorageSize, shard,
                                wasUpdate, generatedValue);
        }

        @Override
        public PutResult deserializedForm(short serialVersion) {
            return new PutResult(this, serialVersion);
        }
    }

    /**
     * The result of a Delete operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    static class DeleteResult extends ValueVersionResult {

        private final boolean success;

        DeleteResult(OpCode opCode,
                     int readKB, int writeKB,
                     ResultValueVersion prevVal,
                     boolean success) {
            super(opCode, readKB, writeKB, prevVal);
            this.success = success;
        }

        /** Constructor to implement deserializedForm */
        private DeleteResult(DeleteResult other, short serialVersion) {
            super(other, serialVersion);
            success = other.success;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        DeleteResult(OpCode opCode,
                     int readKB, int writeKB,
                     DataInput in, short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);
            success = in.readBoolean();
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link ValueVersionResult}) {@code super}
         * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getSuccess
         *      success}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            out.writeBoolean(success);
        }

        @Override
        public boolean getSuccess() {
            return success;
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof DeleteResult)) {
                return false;
            }
            final DeleteResult other = (DeleteResult) obj;
            return (success == other.success);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), success);
        }

        @Override
        public DeleteResult deserializedForm(short serialVersion) {
            return new DeleteResult(this, serialVersion);
        }
    }

    /**
     * The result of a Delete operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    static class MultiDeleteResult extends Result {

        private final int nDeletions;
        private final byte[] resumeKey;

        MultiDeleteResult(OpCode opCode,
                          int readKB, int writeKB,
                          int nDeletions) {
            this(opCode, readKB, writeKB, nDeletions, null);
        }

        MultiDeleteResult(OpCode opCode,
                          int readKB, int writeKB,
                          int nDeletions, byte[] resumeKey) {
            super(opCode, readKB, writeKB);
            this.nDeletions = nDeletions;
            this.resumeKey = resumeKey;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        MultiDeleteResult(OpCode opCode,
                          int readKB, int writeKB,
                          DataInput in, short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);
            nDeletions = in.readInt();
            resumeKey = readByteArray(in);
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Result}) {@code super}
         * <li> ({@link DataOutput#writeInt int}) {@link #getNumRecords
         *      nDeletions}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            out.writeInt(nDeletions);
            writeByteArray(out, resumeKey);
        }

        @Override
        public int getNDeletions() {
            return nDeletions;
        }

        @Override
        public boolean getSuccess() {
            return nDeletions > 0;
        }

        @Override
        public int getNumRecords() {
            return nDeletions;
        }

        @Override
        public byte[] getPrimaryResumeKey() {
            return resumeKey;
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof MultiDeleteResult)) {
                return false;
            }
            final MultiDeleteResult other = (MultiDeleteResult) obj;
            return (nDeletions == other.nDeletions) &&
                Arrays.equals(resumeKey, other.resumeKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), nDeletions, resumeKey);
        }
    }

    /**
     * Base class for results with a Value and Version.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public static abstract class ValueVersionResult extends Result {

        private final ResultValue resultValue;
        protected Version version;
        private final long expirationTime;
        protected long creationTime;
        private final long modificationTime;
        private final int storageSize;

        ValueVersionResult(OpCode op,
                           int readKB, int writeKB,
                           ResultValueVersion valueVersion) {
            super(op, readKB, writeKB);

            if (valueVersion != null) {
                resultValue =
                    (valueVersion.getValueBytes() != null ?
                     new ResultValue(valueVersion.getValueBytes()) :
                     null);
                version = valueVersion.getVersion();
                expirationTime = valueVersion.getExpirationTime();
                creationTime = valueVersion.getCreationTime();
                modificationTime = valueVersion.getModificationTime();
                storageSize = valueVersion.getStorageSize();
            } else {
                resultValue = null;
                version = null;
                expirationTime = 0;
                creationTime = 0;
                modificationTime = 0;
                storageSize = -1;
            }
        }

        /**
         * Copy the specified instance, converting it to the format we expect
         * to see when it is deserialized on a client using the specified
         * serial version.
         */
        ValueVersionResult(ValueVersionResult other, short serialVersion) {
            super(other);
            resultValue = toDeserializedForm(other.resultValue, serialVersion);
            version = other.version;
            expirationTime = other.expirationTime;
            creationTime = other.creationTime;
            modificationTime = other.modificationTime;
            storageSize = other.storageSize;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        ValueVersionResult(OpCode op,
                           int readKB, int writeKB,
                           DataInput in, short serialVersion)
            throws IOException {

            super(op, readKB, writeKB, in, serialVersion);
            if (in.readByte() != 0) {
                resultValue = new ResultValue(in, serialVersion);
            } else {
                resultValue = null;
            }
            if (in.readByte() != 0) {
                version =  Version.createVersion(in, serialVersion);
            } else {
                version = null;
            }
            expirationTime = readTimestamp(in, serialVersion);
            modificationTime = readTimestamp(in, serialVersion);

            storageSize = in.readInt();
            if (serialVersion >= CREATION_TIME_VER) {
                creationTime = in.readLong();
            } else {
                creationTime = 0;
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Result}) {@code super}
         * <li> ({@link SerializationUtil#writeFastExternalOrNull ResultValue
         *      or null}) {@link #getPreviousValue resultValue}
         * <li> ({@link SerializationUtil#writeFastExternalOrNull Version or
         *      null}) {@link #getPreviousVersion version}
         * <li> <i>[Optional]</i> ({@link DataOutput#writeBoolean boolean})
         *      <i>whether expirationTime is present</i>
         * <li> <i>[Optional]</i> ({@link DataOutput#writeLong long}) {@link
         *      #getNewExpirationTime newExpirationTime}
         * <li> <i>[Optional]</i> ({@link DataOutput#writeBoolean boolean})
         *      <i>whether modificationTime is present</i>
         *      // for {@code serialVersion}
         * <li> <i>[Optional]</i> ({@link DataOutput#writeLong long}) {@link
         *      #getPreviousModificationTime modificationTime}
         *      // for {@code serialVersion}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            writeFastExternalOrNull(out, serialVersion, resultValue);
            writeFastExternalOrNull(out, serialVersion, version);
            writeTimestamp(out, expirationTime, serialVersion);
            writeTimestamp(out, modificationTime, serialVersion);
            out.writeInt(storageSize);
            if (serialVersion >= CREATION_TIME_VER) {
                out.writeLong(creationTime);
            }
        }

        @Override
        public Value getPreviousValue() {
            return (resultValue == null) ? null : resultValue.getValue();
        }

        public byte[] getPreviousValueBytes() {
            return (resultValue == null) ? null : resultValue.getBytes();
        }

        @Override
        public Version getPreviousVersion() {
            return version;
        }

        @Override
        public long getPreviousExpirationTime() {
            return expirationTime;
        }

        @Override
        public long getPreviousCreationTime() {
            return creationTime;
        }

        @Override
        public long getPreviousModificationTime() {
            return modificationTime;
        }

        @Override
        public int getPreviousStorageSize() {
            return storageSize;
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof ValueVersionResult)) {
                return false;
            }
            final ValueVersionResult other = (ValueVersionResult) obj;
            return Objects.equals(resultValue, other.resultValue) &&
                Objects.equals(version, other.version) &&
                (expirationTime == other.expirationTime) &&
                (modificationTime == other.modificationTime) &&
                (creationTime == other.creationTime) &&
                (storageSize == other.storageSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), resultValue, version,
                                expirationTime, modificationTime, creationTime,
                                storageSize);
        }
    }

    /**
     * @see #writeFastExternal FastExternalizable format
     */
    static class NOPResult extends Result {

        NOPResult(DataInput in, short serialVersion) {
            super(OpCode.NOP, 0, 0, in, serialVersion);
        }

        NOPResult() {
            super(OpCode.NOP,0, 0);
        }

        @Override
        public boolean getSuccess() {
            return true;
        }

        /* NOPs don't actually handle any records. */
        @Override
        public int getNumRecords() {
            return 0;
        }
    }


    /**
     * The result of an Execute operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    static class ExecuteResult extends Result {

        private final boolean success;
        private final List<Result> successResults;
        private final int failureIndex;
        private final Result failureResult;

        ExecuteResult(OpCode opCode,
                      int readKB, int writeKB,
                      List<Result> successResults) {
            super(opCode, readKB, writeKB);
            checkNull("successResults", successResults);
            for (final Result r : successResults) {
                checkNull("successResults element", r);
            }
            this.successResults = successResults;
            failureIndex = -1;
            failureResult = null;
            success = true;
        }

        ExecuteResult(OpCode opCode,
                      int readKB, int writeKB,
                      int failureIndex,
                      Result failureResult) {
            super(opCode, readKB, writeKB);
            checkNull("failureResult", failureResult);
            this.failureIndex = failureIndex;
            this.failureResult = failureResult;
            successResults = null;
            success = false;
        }

        /** Constructor to implement deserializedForm */
        private ExecuteResult(ExecuteResult other, short serialVersion) {
            super(other);
            success = other.success;
            successResults = other.success ?
                other.successResults.stream()
                .map(r -> toDeserializedForm(r, serialVersion))
                .collect(Collectors.toList()) :
                null;
            failureIndex = other.failureIndex;
            failureResult = !other.success ?
                toDeserializedForm(other.failureResult, serialVersion) :
                null;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        ExecuteResult(OpCode opCode,
                      int readKB, int writeKB,
                      DataInput in, short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);
            success = in.readBoolean();
            if (success) {
                final int listSize = readNonNullSequenceLength(in);
                successResults = new ArrayList<>(listSize);
                for (int i = 0; i < listSize; i += 1) {
                    final Result result =
                        Result.readFastExternal(in, serialVersion);
                    successResults.add(result);
                }
                failureIndex = -1;
                failureResult = null;
            } else {
                failureIndex = in.readInt();
                failureResult = Result.readFastExternal(in, serialVersion);
                successResults = null;
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Result}) {@code super}
         * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getSuccess
         *      success}
         * <li> <i>[Optional]</i> ({@link
         *      SerializationUtil#writeNonNullCollection non-null collection})
         *      {@link #getExecuteResult successResults} // if success is true
         * <li> <i>[Optional]</i> ({@link DataOutput#writeInt int})
         *      <i>failureIndex</i> // if success is false
         * <li> <i>[Optional]</i> ({@link Result}) <i>failureResult</i> // if
         *      success is false
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            out.writeBoolean(success);
            if (success) {
                writeNonNullCollection(out, serialVersion, successResults);
            } else {
                out.writeInt(failureIndex);
                failureResult.writeFastExternal(out, serialVersion);
            }
        }


        @Override
        public boolean getSuccess() {
            return success;
        }

        @Override
        public OperationExecutionException
            getExecuteException(List<Operation> ops) {

            if (success) {
                return null;
            }
            return new OperationExecutionException
                (ops.get(failureIndex), failureIndex, failureResult);
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public List<OperationResult> getExecuteResult() {
            if (!success) {
                return null;
            }
            /* Cast: a Result is an OperationResult. */
            return (List) Collections.unmodifiableList(successResults);
        }

        @Override
        public int getNumRecords() {
            if (!success) {
                return 0;
            }
            return successResults.size();
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof ExecuteResult)) {
                return false;
            }
            final ExecuteResult other = (ExecuteResult) obj;
            return (success == other.success) &&
                Objects.equals(successResults, other.successResults) &&
                (failureIndex == other.failureIndex) &&
                Objects.equals(failureResult, other.failureResult);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), success, successResults,
                                failureIndex, failureResult);
        }

        @Override
        public ExecuteResult deserializedForm(short serialVersion) {
            return new ExecuteResult(this, serialVersion);
        }
    }

    /**
     * @see #writeFastExternal FastExternalizable format
     */
    public static class PutBatchResult extends Result {

        private int numKVPairs;
        private List<Integer> keysPresent;

        PutBatchResult(int readKB, int writeKB,
                       int numKVPairs, List<Integer> keysPresent) {
            super(OpCode.PUT_BATCH, readKB, writeKB);
            checkNull("keysPresent", keysPresent);
            for (final Integer element : keysPresent) {
                checkNull("keysPresent element", element);
            }
            this.numKVPairs = numKVPairs;
            this.keysPresent = keysPresent;
        }


        PutBatchResult(OpCode op,
                       int readKB, int writeKB,
                       DataInput in, short serialVersion)
            throws IOException {

            super(op, readKB, writeKB, in, serialVersion);

            numKVPairs = in.readInt();

            final int count = readNonNullSequenceLength(in);
            if (count == 0) {
                keysPresent = Collections.emptyList();
                return;
            }

            keysPresent = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
               keysPresent.add(in.readInt());
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Result}) {@code super}
         * <li> ({@link DataOutput#writeInt int}) <i>numKVPairs</i>
         * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
         *      sequence length}) <i>keysPresent length</i>
         * <li> For each element:
         *    <ol type="a">
         *    <li> ({@link DataOutput#writeInt int}) <i>key</i>
         *    </ol>
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);

            out.writeInt(numKVPairs);
            writeNonNullSequenceLength(out, keysPresent.size());

            for (int position : keysPresent) {
                out.writeInt(position);
            }
        }

        @Override
        public boolean getSuccess() {
            return true;
        }

        public List<Integer> getKeysPresent() {
            return keysPresent;
        }

        @Override
        public int getNumRecords() {
            return numKVPairs - keysPresent.size();
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof PutBatchResult)) {
                return false;
            }
            final PutBatchResult other = (PutBatchResult) obj;
            return (numKVPairs == other.numKVPairs) &&
                keysPresent.equals(other.keysPresent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), numKVPairs, keysPresent);
        }
    }

    /**
     * The result of a MultiGetIterate or StoreIterate operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    static class IterateResult extends Result {

        private final List<ResultKeyValueVersion> elements;
        private final boolean moreElements;

        IterateResult(OpCode opCode,
                      int readKB, int writeKB,
                      List<ResultKeyValueVersion> elements,
                      boolean moreElements) {
            super(opCode, readKB, writeKB);
            checkNull("elements", elements);
            for (final ResultKeyValueVersion element : elements) {
                checkNull("elements element", element);
            }
            this.elements = elements;
            this.moreElements = moreElements;
        }

        IterateResult(IterateResult other, short serialVersion) {
            super(other);
            elements = other.elements.stream()
                .map(e -> toDeserializedForm(e, serialVersion))
                .collect(Collectors.toList());
            moreElements = other.moreElements;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        IterateResult(OpCode opCode,
                      int readKB, int writeKB,
                      DataInput in, short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);

            final int listSize = readNonNullSequenceLength(in);
            elements = new ArrayList<>(listSize);
            for (int i = 0; i < listSize; i += 1) {
                elements.add(new ResultKeyValueVersion(in, serialVersion));
            }

            moreElements = in.readBoolean();
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Result}) {@code super}
         * <li> ({@link SerializationUtil#writeNonNullCollection non-null
         *      collection}) {@link #getKeyValueVersionList elements}
         * <li> ({@link DataOutput#writeBoolean boolean}) {@link
         *      #hasMoreElements moreElements}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            writeNonNullCollection(out, serialVersion, elements);
            out.writeBoolean(moreElements);
        }

        @Override
        public boolean getSuccess() {
            return elements.size() > 0;
        }

        @Override
        public List<ResultKeyValueVersion> getKeyValueVersionList() {
            return elements;
        }

        @Override
        public boolean hasMoreElements() {
            return moreElements;
        }

        @Override
        public int getNumRecords() {
            return elements.size();
        }

        @Override
        public byte[] getPrimaryResumeKey() {

            if (!moreElements || elements == null || elements.isEmpty()) {
                return null;
            }

            return elements.get(elements.size() - 1).getKeyBytes();
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof IterateResult)) {
                return false;
            }
            final IterateResult other = (IterateResult) obj;
            return elements.equals(other.elements) &&
                (moreElements == other.moreElements);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), elements, moreElements);
        }

        @Override
        public IterateResult deserializedForm(short serialVersion) {
            return new IterateResult(this, serialVersion);
        }
    }

    /**
     * The result of a MultiGetKeysIterate or StoreKeysIterate operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    static class KeysIterateResult extends Result {

        private final List<ResultKey> elements;
        private final boolean moreElements;

        KeysIterateResult(OpCode opCode,
                          int readKB, int writeKB,
                          List<ResultKey> elements,
                          boolean moreElements) {
            super(opCode, readKB, writeKB);
            checkNull("elements", elements);
            for (final ResultKey element : elements) {
                checkNull("elements element", element);
            }
            this.elements = elements;
            this.moreElements = moreElements;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        KeysIterateResult(OpCode opCode,
                          int readKB, int writeKB,
                          DataInput in, short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);

            final int listSize = readNonNullSequenceLength(in);
            elements = new ArrayList<>(listSize);
            for (int i = 0; i < listSize; i += 1) {
                elements.add(new ResultKey(in, serialVersion));
            }

            moreElements = in.readBoolean();
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Result}) {@code super}
         * <li> ({@link SerializationUtil#writeNonNullCollection non-null
         *      collection}) {@link #getKeyList elements}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            writeNonNullCollection(out, serialVersion, elements);
            out.writeBoolean(moreElements);
        }

        @Override
        public boolean getSuccess() {
            return elements.size() > 0;
        }

        @Override
        public List<ResultKey> getKeyList() {
            return elements;
        }

        @Override
        public boolean hasMoreElements() {
            return moreElements;
        }

        @Override
        public int getNumRecords() {
            return elements.size();
        }

        @Override
        public byte[] getPrimaryResumeKey() {

            if (!moreElements || elements == null || elements.isEmpty()) {
                return null;
            }

            return elements.get(elements.size() - 1).getKeyBytes();
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof KeysIterateResult)) {
                return false;
            }
            final KeysIterateResult other = (KeysIterateResult) obj;
            return elements.equals(other.elements) &&
                (moreElements == other.moreElements);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), elements, moreElements);
        }
    }

    /**
     * The result of a table index key iterate operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    static class IndexKeysIterateResult extends Result {

        private final List<ResultIndexKeys> elements;
        private final boolean moreElements;

        IndexKeysIterateResult(OpCode opCode,
                               int readKB, int writeKB,
                               List<ResultIndexKeys> elements,
                               boolean moreElements) {
            super(opCode, readKB, writeKB);
            checkNull("elements", elements);
            for (final ResultIndexKeys element : elements) {
                checkNull("elements element", element);
            }
            this.elements = elements;
            this.moreElements = moreElements;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        IndexKeysIterateResult(OpCode opCode,
                               int readKB, int writeKB,
                               DataInput in, short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);

            final int listSize = readNonNullSequenceLength(in);
            elements = new ArrayList<>(listSize);
            for (int i = 0; i < listSize; i += 1) {
                elements.add(new ResultIndexKeys(in, serialVersion));
            }

            moreElements = in.readBoolean();
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Result}) {@code super}
         * <li> ({@link SerializationUtil#writeNonNullCollection non-null
         *      collection}) {@link #getIndexKeyList elements}
         * <li> ({@link DataOutput#writeBoolean boolean}) {@link
         *      #hasMoreElements moreElements}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            writeNonNullCollection(out, serialVersion, elements);
            out.writeBoolean(moreElements);
        }

        @Override
        public boolean getSuccess() {
            return elements.size() > 0;
        }

        @Override
        public List<ResultIndexKeys> getIndexKeyList() {
            return elements;
        }

        @Override
        public boolean hasMoreElements() {
            return moreElements;
        }

        @Override
        public int getNumRecords() {
            return elements.size();
        }

        @Override
        public byte[] getPrimaryResumeKey() {

            if (!moreElements || elements == null || elements.isEmpty()) {
                return null;
            }

            return elements.get(elements.size() - 1).getPrimaryKeyBytes();
        }

        @Override
        public byte[] getSecondaryResumeKey() {

            if (!moreElements || elements == null || elements.isEmpty()) {
                return null;
            }

            return elements.get(elements.size() - 1).getIndexKeyBytes();
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof IndexKeysIterateResult)) {
                return false;
            }
            final IndexKeysIterateResult other = (IndexKeysIterateResult) obj;
            return elements.equals(other.elements) &&
                (moreElements == other.moreElements);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), elements, moreElements);
        }
    }

    /**
     * The result of a table index row iterate operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    static class IndexRowsIterateResult extends Result {

        private final List<ResultIndexRows> elements;
        private final boolean moreElements;

        IndexRowsIterateResult(OpCode opCode,
                               int readKB, int writeKB,
                               List<ResultIndexRows> elements,
                               boolean moreElements) {
            super(opCode, readKB, writeKB);
            checkNull("elements", elements);
            for (final ResultIndexRows element : elements) {
                checkNull("elements element", element);
            }
            this.elements = elements;
            this.moreElements = moreElements;
        }

        /** Constructor to implement deserializedForm */
        private IndexRowsIterateResult(IndexRowsIterateResult other,
                                       short serialVersion) {
            super(other);
            elements = other.elements.stream()
                .map(e -> toDeserializedForm(e, serialVersion))
                .collect(Collectors.toList());
            moreElements = other.moreElements;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        IndexRowsIterateResult(OpCode opCode,
                               int readKB, int writeKB,
                               DataInput in, short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);

            final int listSize = readNonNullSequenceLength(in);
            elements = new ArrayList<>(listSize);
            for (int i = 0; i < listSize; i += 1) {
                elements.add(new ResultIndexRows(in, serialVersion));
            }

            moreElements = in.readBoolean();
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Result}) {@code super}
         * <li> ({@link SerializationUtil#writeNonNullCollection non-null
         *      collection}) {@link #getIndexRowList elements}
         * <li> ({@link DataOutput#writeBoolean boolean}) {@link
         *      #hasMoreElements moreElements}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            writeNonNullCollection(out, serialVersion, elements);
            out.writeBoolean(moreElements);
        }

        @Override
        public boolean getSuccess() {
            return elements.size() > 0;
        }

        @Override
        public List<ResultIndexRows> getIndexRowList() {
            return elements;
        }

        @Override
        public boolean hasMoreElements() {
            return moreElements;
        }

        @Override
        public int getNumRecords() {
            return elements.size();
        }

        @Override
        public byte[] getPrimaryResumeKey() {

            if (!moreElements || elements == null || elements.isEmpty()) {
                return null;
            }

            return elements.get(elements.size() - 1).getKeyBytes();
        }

        @Override
        public byte[] getSecondaryResumeKey() {

            if (!moreElements || elements == null || elements.isEmpty()) {
                return null;
            }

            return elements.get(elements.size() - 1).getIndexKeyBytes();
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof IndexRowsIterateResult)) {
                return false;
            }
            final IndexRowsIterateResult other = (IndexRowsIterateResult) obj;
            return elements.equals(other.elements) &&
                (moreElements == other.moreElements);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), elements, moreElements);
        }

        @Override
        public IndexRowsIterateResult deserializedForm(short serialVersion) {
            return new IndexRowsIterateResult(this, serialVersion);
        }
    }

    /**
     * The result of a multi-get-batch iteration operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    static class BulkGetIterateResult extends IterateResult {

        private final int resumeParentKeyIndex;

        BulkGetIterateResult(OpCode opCode,
                             int readKB, int writeKB,
                             List<ResultKeyValueVersion> elements,
                             boolean moreElements,
                             int resumeParentKeyIndex) {
            super(opCode, readKB, writeKB, elements, moreElements);
            this.resumeParentKeyIndex = resumeParentKeyIndex;
        }

        /** Constructor to implement deserializedForm */
        private BulkGetIterateResult(BulkGetIterateResult other,
                                     short serialVersion) {
            super(other, serialVersion);
            resumeParentKeyIndex = other.resumeParentKeyIndex;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        BulkGetIterateResult(OpCode opCode,
                             int readKB, int writeKB,
                             DataInput in, short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);
            resumeParentKeyIndex = in.readInt();
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link IterateResult}) {@code super}
         * <li> ({@link DataOutput#writeInt int}) {@link
         *      #getResumeParentKeyIndex resumeParentKeyIndex}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            out.writeInt(resumeParentKeyIndex);
        }

        /**
         * Returns the parent key index to start from if has more elements,
         * returns -1 if no more element.
         */
        @Override
        public int getResumeParentKeyIndex() {
            return resumeParentKeyIndex;
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof BulkGetIterateResult)) {
                return false;
            }
            final BulkGetIterateResult other = (BulkGetIterateResult) obj;
            return (resumeParentKeyIndex == other.resumeParentKeyIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), resumeParentKeyIndex);
        }

        @Override
        public BulkGetIterateResult deserializedForm(short serialVersion) {
            return new BulkGetIterateResult(this, serialVersion);
        }
    }

    /**
     * The result of a multi-get-batch-keys iteration operation.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    static class BulkGetKeysIterateResult extends KeysIterateResult {

        private final int resumeParentKeyIndex;

        BulkGetKeysIterateResult(OpCode opCode,
                                 int readKB, int writeKB,
                                 List<ResultKey> elements,
                                 boolean moreElements,
                                 int lastParentKeyIndex) {
            super(opCode, readKB, writeKB, elements, moreElements);
            this.resumeParentKeyIndex = lastParentKeyIndex;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        BulkGetKeysIterateResult(OpCode opCode,
                                 int readKB, int writeKB,
                                 DataInput in,
                                 short serialVersion)
            throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);
            resumeParentKeyIndex = in.readInt();
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link KeysIterateResult}) {@code super}
         * <li> ({@link DataOutput#writeInt int}) {@link
         *      #getResumeParentKeyIndex resumeParentKeyIndex}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            out.writeInt(resumeParentKeyIndex);
        }

        /**
         * Returns the parent key index to start from if has more elements,
         * returns -1 if no more element.
         */
        @Override
        public int getResumeParentKeyIndex() {
            return resumeParentKeyIndex;
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof BulkGetKeysIterateResult)) {
                return false;
            }
            final BulkGetKeysIterateResult other =
                (BulkGetKeysIterateResult) obj;
            return (resumeParentKeyIndex == other.resumeParentKeyIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), resumeParentKeyIndex);
        }
    }

    /**
     * The result of a Query operation.
     * This class is public to allow access to the resume key.
     *
     * @since 4.0
     * @see #writeFastExternal FastExternalizable format
     */
    public static class QueryResult extends Result {

        private final List<FieldValueImpl> results;

        private final FieldDefImpl resultDef;

        /* added in QUERY_VERSION_2 */
        private final boolean mayReturnNULL;

        private final boolean moreElements;

        private final ResumeInfo resumeInfo;

        private int[] pids;

        private int[] numResultsPerPid;

        private ResumeInfo[] resumeInfos;

        /*
         * Flag indicates the total read KB exceeds the specified maxReadKB in
         * TableQeuery operation.
         */
        private final boolean exceededSizeLimit;

        private final String batchName;

        private final String batchTrace;

        public QueryResult(OpCode opCode,
                           int readKB,
                           int writeKB,
                           List<FieldValueImpl> results,
                           FieldDefImpl resultDef,
                           boolean mayReturnNULL,
                           boolean moreElements,
                           ResumeInfo resumeInfo,
                           boolean exceededSizeLimit,
                           int[] pids,
                           int[] numResultsPerPid,
                           ResumeInfo[] resumeInfos,
                           String batchName,
                           String batchTrace) {

            super(opCode, readKB, writeKB);

            checkNull("results", results);
            for (final FieldValueImpl element : results) {
                checkNull("results element", element);
            }

            checkNull("resultDef", resultDef);

            this.results = results;
            this.resultDef = resultDef;
            this.mayReturnNULL = mayReturnNULL;
            this.moreElements = moreElements;
            this.resumeInfo = resumeInfo;
            this.exceededSizeLimit = exceededSizeLimit;
            this.pids = pids;
            this.numResultsPerPid = numResultsPerPid;
            this.resumeInfos = resumeInfos;
            this.batchName = batchName;
            this.batchTrace = batchTrace;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        QueryResult( OpCode opCode,
                     int readKB,
                     int writeKB,
                     DataInput in,
                     short serialVersion)  throws IOException {

            super(opCode, readKB, writeKB, in, serialVersion);

            try {
                resultDef = FieldDefSerialization.readFieldDef(in, serialVersion);

                mayReturnNULL = in.readBoolean();

                FieldDefImpl valDef = (resultDef.isWildcard() ?
                                       null :
                                       resultDef);

                final int listSize = readNonNullSequenceLength(in);
                results = new ArrayList<>(listSize);

                if (mayReturnNULL) {
                    for (int i = 0; i < listSize; i += 1) {
                        FieldValueImpl val = (FieldValueImpl)
                            FieldValueSerialization.
                            readFieldValue(valDef,
                                           in,
                                           serialVersion);
                        results.add(val);
                    }
                } else {
                    for (int i = 0; i < listSize; i += 1) {
                        FieldValueImpl val = (FieldValueImpl)
                            FieldValueSerialization.
                            readNonNullFieldValue(
                                valDef,
                                null, // valKind
                                in,
                                serialVersion);
                        results.add(val);
                    }
                }

                moreElements = in.readBoolean();

                resumeInfo = new ResumeInfo(in, serialVersion);
                exceededSizeLimit = in.readBoolean();

                pids = PlanIter.deserializeIntArray(in, serialVersion);
                if (pids != null) {
                    numResultsPerPid =
                        PlanIter.deserializeIntArray(in, serialVersion);
                    resumeInfos = new ResumeInfo[pids.length];
                    for (int i = 0; i < pids.length; ++i) {
                        if (in.readBoolean()) {
                            resumeInfos[i] = new ResumeInfo(in, serialVersion);
                        } else {
                            resumeInfos[i] = null;
                        }
                    }
                }

                if (serialVersion >= QUERY_VERSION_14) {
                    batchName = SerializationUtil.readString(in, serialVersion);
                    batchTrace = SerializationUtil.readString(in, serialVersion);
                } else {
                    batchName = null;
                    batchTrace = null;
                }
            } catch (Throwable e) {
                if (!EmbeddedMode.isEmbedded()) {
                    System.err.println("Failed to deserialize result");
                    e.printStackTrace();
                }
                throw e;
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Result}) {@code super}
         * <li> {@link FieldDefSerialization#writeFieldDef
         *      writeFieldDef(resultDef)}
         * <li> ({@link DataOutput#writeBoolean boolean}) {@code mayReturnNull}
         * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
         *      sequence length}) <i>results length</i>
         * <li> For each result, choose one of the following:
         *   <ol type="a">
         *   <li> {@link FieldValueSerialization#writeFieldValue
         *        writeFieldValue(result, } {@link FieldDefImpl#isWildcard
         *        resultDef.isWildcard())} // If {@code mayReturnNULL} is
         *        {@code true}
         *   <li> {@link FieldValueSerialization#writeNonNullFieldValue
         *        writeNonNullFieldValue(result, } {@link
         *        FieldDefImpl#isWildcard resultDef.isWildcard()}{@code ,
         *        true)} // If {@code mayReturnNULL} is {@code false}
         *   </ol>
         * <li> ({@link DataOutput#writeBoolean boolean}) {@link
         *      #hasMoreElements moreElements}
         * <li> If there are more elements:
         *   <ol type="a">
         *   <li> ({@link DataOutput#writeInt int}) {@code currentIndexRange}
         *   <li> ({@link SerializationUtil#writeByteArray byte array}) {@link
         *        #getPrimaryResumeKey primaryResumeKey}
         *   <li> ({@link SerializationUtil#writeByteArray byte array}) {@link
         *        #getSecondaryResumeKey secondaryResumeKey}
         *   </ol>
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            try {
                super.writeFastExternal(out, serialVersion);

                FieldDefSerialization.writeFieldDef(resultDef, out, serialVersion);

                out.writeBoolean(mayReturnNULL);

                writeNonNullSequenceLength(out, results.size());

                boolean isWildcard = resultDef.isWildcard();

                if (mayReturnNULL) {
                    for (final FieldValueImpl res : results) {
                        FieldValueSerialization.
                            writeFieldValue(res,
                                            isWildcard, //writeValDef
                                            out, serialVersion);
                    }
                } else {
                    for (final FieldValueImpl res : results) {
                        FieldValueSerialization.
                            writeNonNullFieldValue(res,
                                                   isWildcard, //writeValDef
                                                   true, // writeValKind
                                                   out, serialVersion);
                    }
                }

                out.writeBoolean(moreElements);

                resumeInfo.writeFastExternal(out, serialVersion);

                out.writeBoolean(exceededSizeLimit);

                PlanIter.serializeIntArray(pids, out, serialVersion);
                if (pids != null && pids.length > 0) {
                    PlanIter.serializeIntArray(numResultsPerPid, out,
                                               serialVersion);
                    assert(pids.length == resumeInfos.length);
                    for (int i = 0; i < resumeInfos.length; ++i) {
                        if (resumeInfos[i] != null) {
                            out.writeBoolean(true);
                            resumeInfos[i].
                                writeFastExternal(out, serialVersion);
                        } else {
                            out.writeBoolean(false);
                        }
                    }
                }

                if (serialVersion >= QUERY_VERSION_14) {
                    SerializationUtil.writeString(out, serialVersion, batchName);
                    SerializationUtil.writeString(out, serialVersion, batchTrace);
                }

            } catch (IOException e) {
                if (!EmbeddedMode.isEmbedded()) {
                    System.err.println("Failed to serialize result");
                    e.printStackTrace();
                }
                throw e;
            } catch (ClassCastException e) {
                if (!EmbeddedMode.isEmbedded()) {
                    System.err.println("Failed to serialize result");
                    e.printStackTrace();
                }
                throw e;
            } catch (Throwable e) {
                if (!EmbeddedMode.isEmbedded()) {
                    System.err.println("Failed to serialize result");
                    e.printStackTrace();
                }
                throw e;
            }
        }

        @Override
        public boolean getSuccess() {
            return results.size() > 0;
        }

        @Override
        public List<FieldValueImpl> getQueryResults() {
            return results;
        }

        @Override
        public boolean hasMoreElements() {
            return moreElements;
        }

        @Override
        public int getNumRecords() {
            return results.size();
        }

        public ResumeInfo getResumeInfo() {
            return resumeInfo;
        }

        public boolean getExceededSizeLimit() {
            return exceededSizeLimit;
        }

        public int[] getPids() {
            return pids;
        }

        public int[] getNumResultsPerPid() {
            return numResultsPerPid;
        }

        public ResumeInfo getResumeInfo(int i) {
            return resumeInfos[i];
        }

        public String getBatchName() {
            return batchName;
        }

        public String getBatchTrace() {
            return batchTrace;
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof QueryResult)) {
                return false;
            }
            final QueryResult other = (QueryResult) obj;
            return results.equals(other.results) &&
                Objects.equals(resultDef, other.resultDef) &&
                (mayReturnNULL == other.mayReturnNULL) &&
                (moreElements == other.moreElements) &&
                Arrays.equals(pids, other.pids) &&
                Arrays.equals(numResultsPerPid, other.numResultsPerPid) &&
                (exceededSizeLimit == other.exceededSizeLimit);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), results,
                                resultDef, mayReturnNULL, moreElements,
                                resumeInfo, pids, numResultsPerPid,
                                resumeInfos, exceededSizeLimit);
        }
    }

    public static class GetIdentityResult extends Result {
        private final SGAttrsAndValues attrsAndValues;

        GetIdentityResult(OpCode op,
                          int readKB,
                          int writeKB,
                          DataInput in,
                          short serialVersion)
            throws IOException {
            super(op, readKB, writeKB, in, serialVersion);
            if (in.readByte() != 0) {
                attrsAndValues = new SGAttrsAndValues(in, serialVersion);
            } else {
                attrsAndValues = null;
            }
        }

        GetIdentityResult(OpCode op,
                          int readKB,
                          int writeKB,
                          SGAttrsAndValues attrsAndValues) {
            super(op, readKB, writeKB);
            this.attrsAndValues = attrsAndValues;
        }

        /**
        * Writes this object to the output stream.  Format:
        * <ol>
        * <li> ({@link Result}) {@code super}
        * <li> ({@link SerializationUtil#writeFastExternalOrNull
        *       SGAttrsAndValues or null})
        * </ol>
        */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
            super.writeFastExternal(out, serialVersion);
            writeFastExternalOrNull(out, serialVersion, attrsAndValues);
        }

        @Override
        public boolean getSuccess() {
            return attrsAndValues != null;
        }

        public SGAttrsAndValues getSGAttrsAndValues() {
            return attrsAndValues;
        }

        @Override
        public boolean equals(Object obj) {
            if (!super.equals(obj) ||
                !(obj instanceof GetIdentityResult)) {
                return false;
            }
            final GetIdentityResult other = (GetIdentityResult) obj;
            return Objects.equals(attrsAndValues, other.attrsAndValues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), attrsAndValues);
        }
    }

    /**
     * Utility method to write an expiration time conditionally into an output
     * stream based on serial version.
     */
    static void writeTimestamp(DataOutput out,
                                    long expirationTime,
                                    @SuppressWarnings("unused")
                                            short serialVersion)
    throws IOException {

        if (expirationTime == 0) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(expirationTime);
        }
    }

    static class TableChargeResult extends Result {

        TableChargeResult(DataInput in, short serialVersion) {
            super(OpCode.TABLE_CHARGE, 0, 0, in, serialVersion);
        }

        TableChargeResult() {
            super(OpCode.NOP,0, 0);
        }

        @Override
        public boolean getSuccess() {
            return true;
        }

        @Override
        public int getNumRecords() {
            return 0;
        }
    }

    /**
     * Utility method to read an expiration time conditionally from an input
     * stream based on serial version. Returns 0 if it is not available in the
     * stream.
     */
    static long readTimestamp(DataInput in,
                                   @SuppressWarnings("unused")
                                           short serialVersion)
        throws IOException {

        if (in.readBoolean()) {
            return in.readLong();
        }
        return 0;
    }
}
