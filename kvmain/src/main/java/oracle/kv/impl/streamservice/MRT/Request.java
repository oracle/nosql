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

package oracle.kv.impl.streamservice.MRT;

import static oracle.kv.impl.systables.StreamRequestDesc.COL_REQUEST_TYPE;
import static oracle.kv.impl.util.SerialVersion.CURRENT;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readPackedLong;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import oracle.kv.KVVersion;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.streamservice.ServiceMessage;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

/**
 * Multi-region service request messages.
 */
public abstract class Request extends ServiceMessage {

    /** original request format used in kvstore pre-22.1 */
    public static final short V1 = SerialVersion.V28;

    /** change drop table request to include table metadata */
    private static final short V2 = SerialVersion.V29;

    /**
     * 1. Switch to using Java serialization
     * 2. Change store version to include both store version and minimum
     *    agent version
     */
    public static final short V3 = SerialVersion.V30;

    /**
     * Multi-region service request types. New types must be added to the end of
     * this enum to maintain compatibility.
     */
    public enum Type {
        CREATE_TABLE,       /* Create a table */
        UPDATE_TABLE,       /* Update a table */
        DROP_TABLE,         /* Drop a table */
        CREATE_REGION,      /* Create a region */
        DROP_REGION,        /* Drop a region */
        STORE_VERSION,      /* Store version */
        CREATE_CHILD,       /* Create a child table */
        DROP_CHILD;         /* Drop a child table */
        private static final Type[] VALUES = values();

        private static Type getType(Row row) {
            final int ord = row.get(COL_REQUEST_TYPE).asInteger().get();
            return VALUES[ord];
        }
    }

    /**
     * Returns a request object from the specified row. If row is null then
     * null is returned.
     */
    static Request getFromRow(Row row) throws IOException {
        if (row == null) {
            return null;
        }
        switch (Type.getType(row)) {
            case CREATE_TABLE : return new CreateTable(row);
            case UPDATE_TABLE : return new UpdateTable(row);
            case DROP_TABLE : return new DropTable(row);
            case CREATE_REGION : return new CreateRegion(row);
            case DROP_REGION : return new DropRegion(row);
            case STORE_VERSION : return new StoreAndMinAgentVersions(row);
            case CREATE_CHILD : return new CreateChild(row);
            case DROP_CHILD : return new DropChild(row);
            default : throw new IllegalStateException("unreachable");
        }
    }

    private final Type type;

    /*
     * The serial version used to serialize or deserialize the message. It is
     * only set on de-serialization from a Row or after it has been serialized.
     * This is needed to aid converting to Java serialization.
     */
    protected short serialVersion = (short)0;

    /**
     * Returns the serial version to use given the max version allowed and
     * the required version. Throws IllegalStateException is the required
     * version is greater than the max.
     *
     * The maxVersion is the maximum serial version supported by both the
     * server and the agent.
     *
     * The requiredVersion is the minimum serial version needed to write
     * and read the message.
     */
    private static short useSerialVersion(short maxVersion,
                                          short requiredVersion) {
        if (requiredVersion > maxVersion) {
            throw new IllegalStateException("Required serial version " +
                                            requiredVersion +
                                            " is greater than max version " +
                                            " allowed " + maxVersion);
        }
        return V3;
    }

    protected Request(int requestId, Type type) {
        super(ServiceType.MRT, requestId);
        assert type != null;
        this.type = type;
    }

    protected Request(Row row) {
        super(row);
        if (!getServiceType().equals(ServiceType.MRT)) {
            throw new IllegalStateException("Row is not a MRT request");
        }
        type = Type.getType(row);
    }

    /**
     * Gets the type of this message.
     */
    public Type getType() {
        return type;
    }

    /**
     * Gets the serial version used to serialize or de-serialize the request.
     * Returns 0 if the request has not yet been serialized or de-serialized.
     */
    public short getSerialVersion() {
        return serialVersion;
    }

    @Override
    protected Row toRow(Table requestTable, short maxSerialVersion)
            throws IOException {
        final Row row = super.toRow(requestTable, maxSerialVersion);
        row.put(COL_REQUEST_TYPE, type.ordinal());
        return row;
    }

    /**
     * Returns the serial version read from the input stream.
     */
    short readSerialVersion(DataInput in) throws IOException {
        final short sv = in.readShort();
        if (sv > CURRENT) {
            throw new IOException("Unsupported serial version "
                                  + serialVersion);
        }
        return sv;
    }

    /**
     * Returns the table read from the input stream.
     */
    TableImpl readTable(DataInput in) throws IOException {
        return SerializationUtil.readJavaSerial(in, TableImpl.class);
    }

    /**
     * Writes the specified table to the output stream.
     */
    void writeTable(DataOutput out, TableImpl table)
            throws IOException {
        SerializationUtil.writeJavaSerial(out, table);
    }

    /**
     * Create table request. This message indicates that a multi-region table
     * has been created.
     * Response semantics:
     *      SUCCESS     - update successful, request terminated
     *      ERROR       - update failed, request terminated
     */
    public static class CreateTable extends Request {

        private final int seqNum;
        private final TableImpl table;

        public CreateTable(int requestId, TableImpl table, int seqNum) {
            super(requestId, Type.CREATE_TABLE);
            assert table.isTop();
            this.table = table;
            this.seqNum = seqNum;
        }

        private CreateTable(Row row) throws IOException {
            super(row);
            final byte[] payload = getPayloadFromRow(row);
            final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            final DataInput in = new DataInputStream(bais);
            serialVersion = readSerialVersion(in);
            seqNum = readPackedInt(in);
            table = readTable(in);
        }

        /**
         * Gets the metadata sequence number associated with the table create.
         */
        public int getMetadataSeqNum() {
            return seqNum;
        }

        /**
         * Gets the new table instance.
         */
        public TableImpl getTable() {
            return table;
        }

        @Override
        protected byte[] getPayload(short maxSerialVersion) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);
            serialVersion = useSerialVersion(maxSerialVersion,
                                             table.getRequiredSerialVersion());
            out.writeShort(serialVersion);
            writePackedInt(out, seqNum);
            writeTable(out, table);
            return baos.toByteArray();
        }

        @Override
        protected StringBuilder getToString(StringBuilder sb) {
            sb.append(", seqNum=").append(seqNum);
            sb.append(", tableId=").append(table.getId());
            return sb;
        }
    }

    /**
     * Create child table request. This message indicates that a
     * multi-region child table has been created.
     * Response semantics:
     *      SUCCESS     - update successful, request terminated
     *      ERROR       - update failed, request terminated
     */
    public static class CreateChild extends Request {
        private final int seqNum;
        private final TableImpl topTable;
        private final long childTableId;

        public CreateChild(int requestId,
                           TableImpl topTable,
                           int seqNum,
                           long childTableId) {
            super(requestId, Type.CREATE_CHILD);
            this.topTable = topTable;
            this.childTableId = childTableId;
            this.seqNum = seqNum;
        }

        private CreateChild(Row row) throws IOException {
            super(row);
            final byte[] payload = getPayloadFromRow(row);
            final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            final DataInput in = new DataInputStream(bais);
            serialVersion = readSerialVersion(in);
            seqNum = readPackedInt(in);
            topTable = readTable(in);
            childTableId = readPackedLong(in);
        }

        /**
         * Gets the metadata sequence number associated with the child table
         * create.
         */
        public int getMetadataSeqNum() {
            return seqNum;
        }

        /**
         * Gets the new top table instance.
         */
        public TableImpl getTopTable() {
            return topTable;
        }

        /**
         * Gets the child table id.
         */
        public long getTableId() {
            return childTableId;
        }

        @Override
        protected byte[] getPayload(short maxSerialVersion) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);
            serialVersion = useSerialVersion(maxSerialVersion,
                                           topTable.getRequiredSerialVersion());
            out.writeShort(serialVersion);
            writePackedInt(out, seqNum);
            writeTable(out, topTable);
            writePackedLong(out, childTableId);
            return baos.toByteArray();
        }

        @Override
        protected StringBuilder getToString(StringBuilder sb) {
            sb.append(", seqNum=").append(seqNum);
            sb.append(", childTableId=").append(childTableId);
            sb.append(", topTableId=").append(topTable.getId());
            return sb;
        }
    }

    /**
     * Update table request. This message indicates that a multi-region table
     * has been modified.
     * Response semantics:
     *      SUCCESS     - update successful, request terminated
     *      ERROR       - update failed, request terminated
     */
    public static class UpdateTable extends Request {

        private final int seqNum;
        private final TableImpl table;

        /**
         * Constructs a table update request.
         */
        public UpdateTable(int requestId, TableImpl table, int seqNum) {
            super(requestId, Type.UPDATE_TABLE);
            assert table.isTop();
            this.table = table;
            this.seqNum = seqNum;
        }

        private UpdateTable(Row row) throws IOException {
            super(row);
            final byte[] payload = getPayloadFromRow(row);
            final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            final DataInput in = new DataInputStream(bais);
            serialVersion = readSerialVersion(in);
            seqNum = readPackedInt(in);
            table = readTable(in);
        }

        /**
         * Gets the metadata sequence number associated with the table update.
         */
        public int getMetadataSeqNum() {
            return seqNum;
        }

        /**
         * Gets the updated table instance.
         */
        public TableImpl getTable() {
            return table;
        }

        @Override
        protected byte[] getPayload(short maxSerialVersion) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);
            serialVersion = useSerialVersion(maxSerialVersion,
                                            table.getRequiredSerialVersion());
            out.writeShort(serialVersion);
            writePackedInt(out, seqNum);
            writeTable(out, table);
            return baos.toByteArray();
        }

        @Override
        protected StringBuilder getToString(StringBuilder sb) {
            sb.append(", seqNum=").append(seqNum);
            sb.append(", tableId=").append(table.getId());
            return sb;
        }
    }

    /**
     * Drop table request. This message indicates that a multi-region table
     * has been dropped.
     * Response semantics:
     *      Any response terminates request
     */
    public static class DropTable extends Request {

        private final int seqNum;
        private final TableImpl table;
        private final long tableId;
        private final String tableName;

        /**
         * Constructs a drop table request.
         */
        public DropTable(int requestId, TableImpl table, int seqNum) {
            super(requestId, Type.DROP_TABLE);
            assert table != null;
            this.seqNum = seqNum;
            this.table = table;
            this.tableId = table.getId();
            this.tableName = table.getFullNamespaceName();
        }

        private DropTable(Row row) throws IOException {
            super(row);
            final byte[] payload = getPayloadFromRow(row);
            final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            final DataInput in = new DataInputStream(bais);
            serialVersion = readSerialVersion(in);
            seqNum = readPackedInt(in);

            /* after V2 the message included the table */
            if (serialVersion >= V2) {
                table = readTable(in);
                tableId = table.getId();
                tableName = table.getFullNamespaceName();
            } else {

                /*
                 * This could be V1 or later, but less than V2. Previous
                 * versions were not writing a constant serialVersion.
                 */
                tableId = readPackedLong(in);
                tableName = readNonNullString(in, serialVersion);
                table = null; /* no table md in V1 format */
            }
        }

        /**
         * Gets table metadata
         */
        public TableImpl getTable() {
            return table;
        }

        /**
         * Gets the metadata sequence number associated with the table drop.
         */
        public int getMetadataSeqNum() {
            return seqNum;
        }

        /**
         * Gets the table name of dropped table
         */
        public String getTableName() {
            return tableName;
        }

        /**
         * Gets the ID of the dropped table.
         */
        public long getTableId() {
            return tableId;
        }

        @Override
        protected byte[] getPayload(short maxSerialVersion) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);
            serialVersion = useSerialVersion(maxSerialVersion,
                                             table.getRequiredSerialVersion());
            /* write request/table format version */
            out.writeShort(serialVersion);
            /* write table md sequence number */
            writePackedInt(out, seqNum);
            /* after V2 the message included the table */
            if (serialVersion >= V2) {
                writeTable(out, table);
            } else {
                writePackedLong(out, table.getId());
                writeNonNullString(out, V1,
                                   table.getFullNamespaceName());
            }
            return baos.toByteArray();
        }

        @Override
        protected StringBuilder getToString(StringBuilder sb) {
            sb.append(", seqNum=").append(seqNum);
            sb.append(", tableName=").append(tableName);
            sb.append(", tableId=").append(tableId);
            return sb;
        }
    }

    /**
     * Drop child table request. This message indicates that a
     * multi-region child table has been dropped.
     * Response semantics:
     *      Any response terminates request
     */
    public static class DropChild extends Request {

        private final int seqNum;
        private final long tableId;
        private final String tableName;
        private final TableImpl topTable;

        /**
         * Constructs a drop table request.
         */
        public DropChild(int requestId, long tableId,
                         String tableName, int seqNum,
                         TableImpl topLevelTable) {
            super(requestId, Type.DROP_CHILD);
            assert tableName != null;
            this.seqNum = seqNum;
            this.tableId = tableId;
            this.tableName = tableName;
            this.topTable = topLevelTable;
        }

        private DropChild(Row row) throws IOException {
            super(row);
            final byte[] payload = getPayloadFromRow(row);
            final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            final DataInput in = new DataInputStream(bais);
            serialVersion = readSerialVersion(in);
            seqNum = readPackedInt(in);
            tableId = readPackedLong(in);
            tableName = readNonNullString(in, serialVersion);
            topTable = readTable(in);
        }

        /**
         * Gets the metadata sequence number associated with the dropped child
         * table.
         */
        public int getMetadataSeqNum() {
            return seqNum;
        }

        /**
         * Gets the table name of dropped child table.
         */
        public String getTableName() {
            return tableName;
        }

        /**
         * Gets the ID of the dropped child table.
         */
        public long getTableId() {
            return tableId;
        }

        /**
         * Gets the top level table.
         */
        public Table getTopLevelTable() {
            return topTable;
        }

        @Override
        protected byte[] getPayload(short maxSerialVersion) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);
            serialVersion = useSerialVersion(maxSerialVersion,
                                          topTable.getRequiredSerialVersion());
            out.writeShort(serialVersion);
            writePackedInt(out, seqNum);
            writePackedLong(out, tableId);
            writeNonNullString(out, serialVersion, tableName);
            writeTable(out, topTable);
            return baos.toByteArray();
        }

        @Override
        protected StringBuilder getToString(StringBuilder sb) {
            sb.append(", seqNum=").append(seqNum);
            sb.append(", tableId=").append(tableId);
            sb.append(", topTableId=").append(topTable.getId());
            return sb;
        }
    }

    /**
     * Create a region. This message indicates that a region has been created.
     *
     * Response semantics:
     *      Any response terminates request
     */
    public static class CreateRegion extends Request {

        private final int seqNum;
        private final Region region;

        /**
         * Constructs a drop table request.
         */
        public CreateRegion(int requestId, Region region, int seqNum) {
            super(requestId, Type.CREATE_REGION);
            this.seqNum = seqNum;
            this.region = region;
        }

        private CreateRegion(Row row) throws IOException {
            super(row);
            final byte[] payload = getPayloadFromRow(row);
            final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            final DataInput in = new DataInputStream(bais);
            serialVersion = readSerialVersion(in);
            seqNum = readPackedInt(in);
            region = SerializationUtil.readJavaSerial(in, Region.class);
        }

        /**
         * Gets the metadata sequence number associated with the table drop.
         */
        public int getMetadataSeqNum() {
            return seqNum;
        }

        /**
         * Gets the region.
         */
        public Region  getRegion() {
            return region;
        }

        @Override
        protected byte[] getPayload(short maxSerialVersion) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);
            serialVersion = useSerialVersion(maxSerialVersion,
                                             SerialVersion.MINIMUM);
            out.writeShort(serialVersion);
            writePackedInt(out, seqNum);
            SerializationUtil.writeJavaSerial(out, region);
            return baos.toByteArray();
        }

        @Override
        protected StringBuilder getToString(StringBuilder sb) {
            sb.append(", seqNum=").append(seqNum);
            sb.append(", region=").append(region.toString());
            return sb;
        }
    }

    /**
     * Drop a region. This message indicates that a region has been dropped.
     *
     * Response semantics:
     *      Any response terminates request
     */
    public static class DropRegion extends Request {

        private final int seqNum;
        private final int regionId;

        /**
         * Constructs a drop table request.
         */
        public DropRegion(int requestId, int regionId, int seqNum) {
            super(requestId, Type.DROP_REGION);
            Region.checkId(regionId, false /* isExternalRegion */);
            this.seqNum = seqNum;
            this.regionId = regionId;
        }

        private DropRegion(Row row) throws IOException {
            super(row);
            final byte[] payload = getPayloadFromRow(row);
            final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            final DataInput in = new DataInputStream(bais);
            serialVersion = readSerialVersion(in);
            seqNum = readPackedInt(in);
            regionId = readPackedInt(in);
        }

        /**
         * Gets the metadata sequence number associated with the table drop.
         */
        public int getMetadataSeqNum() {
            return seqNum;
        }

        /**
         * Gets the region.
         */
        public int getRegionId() {
            return regionId;
        }

        @Override
        protected byte[] getPayload(short maxSerialVersion) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);
            serialVersion = useSerialVersion(maxSerialVersion,
                                             SerialVersion.MINIMUM);
            out.writeShort(serialVersion);
            writePackedInt(out, seqNum);
            writePackedInt(out, regionId);
            return baos.toByteArray();
        }

        @Override
        protected StringBuilder getToString(StringBuilder sb) {
            sb.append(", seqNum=").append(seqNum);
            sb.append(", regionId=").append(regionId);
            return sb;
        }
    }

    /**
     * Store version and minimum agent version request. The minimum agent
     * version  may be lower than the global store version.
     *
     * The STORE_VERSION message contains both the store
     * version and the minimum agent version.
     */
    public static class StoreAndMinAgentVersions extends Request {

        /*  ID for version information */
        final static int REQUEST_ID = -1;

        private final KVVersion storeVersion;
        private final KVVersion minAgentVersion;

        StoreAndMinAgentVersions(KVVersion storeVersion,
                                 KVVersion minAgentVersion) {
            /* The store version is at a fixed ID */
            super(REQUEST_ID, Type.STORE_VERSION);
            this.storeVersion = storeVersion;
            this.minAgentVersion = minAgentVersion;
        }

        private StoreAndMinAgentVersions(Row row) throws IOException {
            super(row);
            final byte[] payload = getPayloadFromRow(row);
            final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            final DataInput in = new DataInputStream(bais);
            serialVersion = readSerialVersion(in);
            minAgentVersion = KVVersion.parseVersion(
                                        readNonNullString(in, serialVersion));
            storeVersion =
                KVVersion.parseVersion(readNonNullString(in, serialVersion));
        }

        /**
         * Gets the store version.
         */
        public KVVersion  getStoreVersion() {
            return storeVersion;
        }

        /**
         * Gets the minimum agent version.
         */
        KVVersion  getMinAgentVersion() {
            return minAgentVersion;
        }

        @Override
        protected byte[] getPayload(short maxSerialVersion) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);
            serialVersion = V3;
            out.writeShort(serialVersion);
            writeNonNullString(out, serialVersion,
                               minAgentVersion.getNumericVersionString());
            writeNonNullString(out, serialVersion,
                               storeVersion.getNumericVersionString());
            return baos.toByteArray();
        }

        @Override
        protected StringBuilder getToString(StringBuilder sb) {
            sb.append(", storeVersion=");
            sb.append(((storeVersion == null) ? "null" :
                                   storeVersion.getNumericVersionString()));
            sb.append(((minAgentVersion == null) ? "null" :
                                   minAgentVersion.getNumericVersionString()));
            return sb;
        }
    }
}
