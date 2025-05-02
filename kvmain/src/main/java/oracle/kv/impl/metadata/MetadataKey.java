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

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.api.table.TableMetadata.MRTableListKey;
import oracle.kv.impl.api.table.TableMetadata.RegionMapperKey;
import oracle.kv.impl.api.table.TableMetadata.SysTableListKey;
import oracle.kv.impl.api.table.TableMetadata.TableListKey;
import oracle.kv.impl.api.table.TableMetadata.TableMetadataKey;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.SerialVersion;

/**
 * Marker interface for classes used as a key into metadata. Objects
 * implementing MetadataKey should also implement <code>Serializable</code> and
 * should include an entry in {@link StdMetadataKeyType}.
 */
public interface MetadataKey extends FastExternalizable {

    /** The type of a metadata key. */
    interface MetadataKeyType {

        /** Returns the integer value associated with the type. */
        int getIntValue();

        /** Reads the associated metadata key. */
        MetadataKey readMetadataKey(DataInput in, short serialVersion)
            throws IOException;

        /** Reads the integer value of a metadata key type. */
        static int readIntValue(DataInput in,
                                @SuppressWarnings("unused") short sv)
            throws IOException
        {
            return readPackedInt(in);
        }

        /** Writes the integer value of this metadata key type. */
        default void writeIntValue(DataOutput out,
                                   @SuppressWarnings("unused") short sv)
            throws IOException
        {
            writePackedInt(out, getIntValue());
        }
    }

    /** Finds a MetadataKeyType from the associated integer value. */
    interface MetadataKeyTypeFinder {

        /**
         * Returns the MetadataKeyType associated with the specified value, or
         * null if none is found.
         */
        MetadataKeyType getMetadataKeyType(int intValue);
    }

    /** The key types for standard metadata keys. */
    enum StdMetadataKeyType implements MetadataKeyType {
        REGION_MAPPER(0, (i, s) -> RegionMapperKey.INSTANCE),
        SYS_TABLE_LIST(1, (i, s) -> SysTableListKey.INSTANCE),
        TABLE_LIST(2, TableListKey::new),
        TABLE_KEY(3, TableMetadataKey::new),
        MR_TABLE_LIST(4, MRTableListKey::new);

        private static final MetadataKeyType[] VALUES = values();
        private final ReadFastExternal<MetadataKey> reader;

        private StdMetadataKeyType(final int ordinal,
                                   final ReadFastExternal<MetadataKey> reader)
        {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        /**
         * Returns the MetadataKeyType with the specified ordinal, or null if
         * not found.
         *
         * @param ordinal the ordinal
         * @return the MetadataKeyType or null
         */
        static MetadataKeyType valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                return null;
            }
        }

        @Override
        public int getIntValue() {
            return ordinal();
        }

        @Override
        public MetadataKey readMetadataKey(DataInput in, short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }
    }

    /**
     * Gets the type of metadata this key is for.
     */
    MetadataType getType();

    /**
     * Gets the required serial version for the key. The default implementation
     * returns SerialVersion.MINIMUM.
     */
    default short getRequiredSerialVersion() {
        return SerialVersion.MINIMUM;
    }

    /** Reads a MetadataKey. */
    static MetadataKey readMetadataKey(DataInput in, short serialVersion)
        throws IOException
    {
        final MetadataKeyType keyType =
            MetadataKeyTypeFinders.findKeyType(
                MetadataKeyType.readIntValue(in, serialVersion));
        return keyType.readMetadataKey(in, serialVersion);
    }

    /**
     * Writes this instance by writing the key type followed by the data for
     * this instance.
     */
    default void writeMetadataKey(DataOutput out, short serialVersion)
        throws IOException
    {
        getMetadataKeyType().writeIntValue(out, serialVersion);
        writeFastExternal(out, serialVersion);
    }

    /** Returns the type of this key. */
    MetadataKeyType getMetadataKeyType();
}
