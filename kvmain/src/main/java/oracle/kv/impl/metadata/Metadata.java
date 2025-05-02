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

package oracle.kv.impl.metadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Interface implemented by all metadata objects.
 *
 * @param <I> the type of metadata information object return by this metadata
 */
public interface Metadata<I extends MetadataInfo>
        extends FastExternalizable {

    /** Metadata types. */
    enum MetadataType implements FastExternalizable {
        /* New types must be added to the end of this list */
        TOPOLOGY(0, Topology::new) {
            @Override
            public String getKey() { return "Topology"; }
        },
        TABLE(1, TableMetadata::new) {
            @Override
            public String getKey() { return "Table"; }
        },
        SECURITY(2, SecurityMetadata::new) {
            @Override
            public String getKey() { return "Security"; }
        };

        private static final MetadataType[] VALUES = values();
        private final ReadFastExternal<Metadata<?>> reader;

        MetadataType(final int ordinal,
                     final ReadFastExternal<Metadata<?>> reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        /**
         * Gets a unique string for this type. This string may be used as a
         * key into a metadata store.
         *
         * @return a unique string
         */
        public abstract String getKey();

        /** Deserialize a MetadataType. */
        public static MetadataType readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unknown MetadataType: " + ordinal);
            }
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }

        /** Deserialize the metadata associated with the type. */
        Metadata<?> readMetadata(DataInput in, short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }
    }

    /**
     * The sequence number of an newly created metadata, empty object.
     */
    int EMPTY_SEQUENCE_NUMBER = 0;

    /**
     * A value indicating that a sequence number is not known. Note that
     * {@literal UNKNOWN_SEQ_NUM < EMPTY_SEQUENCE_NUMBER < the first sequence
     * number}
     */
    int UNKNOWN_SEQ_NUM = -1;

    /**
     * Gets the type of this metadata object.
     *
     * @return the type of this metadata object
     */
    MetadataType getType();

    /**
     * Gets the highest sequence number of this metadata object.
     * Returns Metadata.EMPTY_SEQUENCE_NUMBER if the metadata has not been
     * initialized.
     *
     * @return the highest sequence number of this metadata object
     */
    int getSequenceNumber();

    /**
     * Gets an information object for this metadata. The returned object will
     * include the changes between this object and the metadata at the
     * specified sequence number. If the metadata object can not supply
     * information based on the sequence number an empty metadata information
     * object is returned.
     *
     * @param startSeqNum the inclusive start of the sequence of
     * changes to be included
     * @return a metadata info object
     */
    I getChangeInfo(int startSeqNum);

    /**
     * Prunes the change history of this metadata. If change history is kept
     * and the number of change objects exceeds maxChanges, the oldest objects
     * are removed until the history is at or below maxChanges or limitSeqNum
     * is reached.
     *
     * @param limitSeqNum this change seq num must be retained if present
     * @param maxChanges the maximum trailing changes to be retained
     *
     * @return the pruned metadata
     */
    Metadata<I> pruneChanges(int limitSeqNum, int maxChanges);

    /**
     * Creates a deep copy of the metadata.
     *
     * @return the new Metadata instance
     */
    @SuppressWarnings("unchecked")
    default Metadata<I> getCopy() {
        final byte[] mdBytes = SerializationUtil.getBytes(this);
        return SerializationUtil.getObject(mdBytes, this.getClass());
    }

    /**
     * Write a metadata instance, first writing the metadata type and then the
     * data for this instance.
     */
    default void writeMetadata(DataOutput out, short serialVersion)
        throws IOException
    {
        getType().writeFastExternal(out, serialVersion);
        writeFastExternal(out, serialVersion);
    }

    /**
     * Read a metadata instance.
     */
    static Metadata<?> readMetadata(DataInput in, short serialVersion)
        throws IOException
    {
        return MetadataType.readFastExternal(in, serialVersion)
            .readMetadata(in, serialVersion);
    }
}
