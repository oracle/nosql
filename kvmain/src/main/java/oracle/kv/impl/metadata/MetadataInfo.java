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

import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.api.table.TableChangeList;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata.RegionMapperImpl;
import oracle.kv.impl.api.table.TableMetadata.TableList;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.metadata.SecurityMetadataInfo;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.WriteFastExternal;

/**
 * Interface implemented by all metadata information objects. Objects
 * implementing this interface are used to transmit metadata information in
 * response to a metadata request. The information may be in the form of changes
 * in metadata or a subset of metadata and is implementation dependent. The
 * responder may not have the requested information in which case the object
 * will be empty. Objects implementing MetadataInfo should also implement
 * <code>Serializable</code>.
 */
public interface MetadataInfo extends FastExternalizable {

    /**
     * The type of the MetadataInfo, used to support fast externalization of
     * different implementing types.
     */
    enum MetadataInfoType implements FastExternalizable {
        TABLE(0,
              (in, sv) -> new TableImpl(in, sv, null),
              (item, out, sv) -> {
                  final TableImpl table = (TableImpl) item;
                  if (table.getParent() != null) {
                      throw new IllegalStateException(
                          "Expected top level table: " + table);
                  }
                  table.writeFastExternal(out, sv);
              }),
        TOPOLOGY_INFO(1, TopologyInfo::new),
        SECURITY_METADATA_INFO(2, SecurityMetadataInfo::new),
        REGION_MAPPER(3, RegionMapperImpl::new),
        TABLE_CHANGE_LIST(4, TableChangeList::new),
        TABLE_LIST(5, TableList::new);

        private static final MetadataInfoType[] VALUES = values();
        private final ReadFastExternal<MetadataInfo> reader;
        private final WriteFastExternal<MetadataInfo> writer;

        MetadataInfoType(final int ordinal,
                         final ReadFastExternal<MetadataInfo> reader) {
            this(ordinal, reader, FastExternalizable::writeFastExternal);
        }
        MetadataInfoType(final int ordinal,
                         final ReadFastExternal<MetadataInfo> reader,
                         final WriteFastExternal<MetadataInfo> writer)
        {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
            this.writer = writer;
        }

        static MetadataInfoType readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException
        {
            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unknown MetadataInfoType: " + ordinal);
            }
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            out.writeByte(ordinal());
        }

        MetadataInfo readMetadataInfo(DataInput in, short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }

        void writeMetadataInfo(DataOutput out,
                               short serialVersion,
                               MetadataInfo info)
            throws IOException
        {
            writer.writeFastExternal(info, out, serialVersion);
        }
    }

    /**
     * Gets the type of metadata this information object contains.
     *
     * @return the type of metadata
     */
    MetadataType getType();

    /**
     * Returns the sequence number associated with the metadata that
     * is known to the source of this object.
     *
     * @return the source's metadata sequence number
     */
    int getSequenceNumber();

    /**
     * Returns true if this object does not include any metadata information
     * beyond the type and sequence number.
     *
     * @return true if this object does not include any metadata information
     */
    boolean isEmpty();

    /**
     * Returns the type of this metadata info.
     */
    MetadataInfoType getMetadataInfoType();

    /**
     * Reads an instance of a class that implements MetadataInfo.
     */
    static MetadataInfo readMetadataInfo(DataInput in, short serialVersion)
        throws IOException
    {
        return MetadataInfoType.readFastExternal(in, serialVersion)
            .readMetadataInfo(in, serialVersion);
    }

    /**
     * Writes this instance so that the caller can read arbitrary subclasses.
     * Callers can call writeFastExternal if they only want to support a
     * specific subclass.
     */
    default void writeMetadataInfo(DataOutput out, short serialVersion)
        throws IOException
    {
        final MetadataInfoType type = getMetadataInfoType();
        type.writeFastExternal(out, serialVersion);
        type.writeMetadataInfo(out, serialVersion, this);
    }
}
