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

package oracle.kv.impl.rep.migration;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.topo.RepNodeId;

import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;

/**
 * The partition migration transfer protocol. There are three components in the
 * protocol, the initial transfer request sent by the target to the source node,
 * the transfer response, and the database operations, both sent from the source
 * to the target.
 */
public class TransferProtocol {

    /**
     * The transfer protocol does the upgrade detection when the target
     * attempts to establish the migration stream. The target sends its
     * version, and if it does not match the source version exactly, the
     * source will reject the connection. The target will then retry a
     * some number of times. Obviously this means you can not do migration
     * between two shards at different versions.
     *
     * Version 2 as of release 4.0, with the introduction of TTL
     *
     * Version 3 as of release 18.3, with the introduction of partition
     * generation.
     *
     * Version 4 as of release 19.5, with the change in user record format to
     * support tombstones.
     *
     * Version 5 as of release 21.3, add support for migrating modification
     * times.
     *
     * Version 6 as of release 23.3, add target description.
     *
     * Version 7 as of release 25.3, add last record marker and support for
     * creation time in row metadata.
     *
     */
    static final int VERSION = 7;

    /* Constant used to indicate a transfer only request */
    static final RepNodeId TRANSFER_ONLY_TARGET = new RepNodeId(0, 0);

    /* -- Transfer request -- */

    /*
     * Transfer request size
     *      4 int protocol version
     *      4 int partition ID
     *      4 int target group ID
     *      4 int target node number
     *      8 long target creation time
     *      4 int target number of attempts
     *      4 int target status ordinal
     *      4 int 0 (unused - must be zero)
     */
    private static final int REQUEST_SIZE = 4 + 4 + 4 + 4 + 8 + 4 + 4 + 4;

    private static final Response[] RESPONSE_VALUES = Response.values();

    /**
     * Object encapsulating a transfer request.
     */
    public static class TransferRequest {

        final int partitionId;
        final RepNodeId targetRNId;
        final long creationTime;
        final PartitionMigrationState state;
        final int attempts;

        private TransferRequest(int partitionId,
                                RepNodeId targetRnId,
                                long creationTime,
                                int attempts,
                                PartitionMigrationState state) {
            this.partitionId = partitionId;
            this.targetRNId = targetRnId;
            this.creationTime = creationTime;
            this.state = state;
            this.attempts = attempts;
        }

        @Override
        public String toString() {
            return String.format(
                "Transfer target (%s, %s) [PARTITION-%s, %s, %s]",
                creationTime, attempts, partitionId, targetRNId, state);
        }

        /*
         * Writes a transfer only request.
         */
        public static void write(DataChannel channel, int partitionId)
                           throws IOException {
            write(channel, partitionId, TRANSFER_ONLY_TARGET,
                  0 /* creation time */,
                  0 /* migration status */,
                  0 /* numAttempts */);
        }

        /*
         * Writes a transfer request.
         */
        static void write(DataChannel channel,
                          MigrationTarget target)
            throws IOException {

            final ByteBuffer buffer = ByteBuffer.allocate(REQUEST_SIZE);
            buffer.putInt(VERSION);
            buffer.putInt(target.getPartitionId().getPartitionId());
            buffer.putInt(target.getRepNode().getRepNodeId().getGroupId());
            buffer.putInt(target.getRepNode().getRepNodeId().getNodeNum());
            buffer.putLong(target.getCreationTime());
            buffer.putInt(target.getAttempts());
            buffer.putInt(
                target.getState().getPartitionMigrationState().ordinal());
            buffer.putInt(0);
            buffer.flip();
            channel.write(buffer);
        }

        /*
         * Writes a transfer request.
         */
        private static void write(DataChannel channel,
                                  int partitionId,
                                  RepNodeId targetRNId,
                                  long creationTime,
                                  int migrationStatus,
                                  int numAttempts)
            throws IOException {

            final ByteBuffer buffer = ByteBuffer.allocate(REQUEST_SIZE);
            buffer.putInt(VERSION);
            buffer.putInt(partitionId);
            buffer.putInt(targetRNId.getGroupId());
            buffer.putInt(targetRNId.getNodeNum());
            buffer.putLong(creationTime);
            buffer.putInt(migrationStatus);
            buffer.putInt(numAttempts);
            buffer.putInt(0);
            buffer.flip();
            channel.write(buffer);
        }

        /*
         * Reads a transfer request.
         */
        public static TransferRequest read(DataChannel channel) throws
            IOException {
            ByteBuffer readBuffer =
                        ByteBuffer.allocate(TransferProtocol.REQUEST_SIZE);
            read(readBuffer, channel);

            final int version = readBuffer.getInt();

            if (version != TransferProtocol.VERSION) {
                final StringBuilder sb = new StringBuilder();
                sb.append("Protocol version mismatch, received ");
                sb.append(version);
                sb.append(" expected ");
                sb.append(TransferProtocol.VERSION);
                throw new IOException(sb.toString());
            }
            final int partitionId = readBuffer.getInt();
            final int targetGroupId = readBuffer.getInt();
            final int targetNodeNum = readBuffer.getInt();
            final long creationTime = readBuffer.getLong();
            final int attempts = readBuffer.getInt();
            final int stateOrdinal = readBuffer.getInt();

            /* Unused, mbz */
            readBuffer.getInt();

            final RepNodeId rnId = (targetGroupId == 0)
                ? TRANSFER_ONLY_TARGET
                : new RepNodeId(targetGroupId, targetNodeNum);
            final PartitionMigrationState state =
                RepNodeAdmin.PARTITION_MIGRATION_STATE_VALUES[stateOrdinal];

            return new TransferRequest(
                partitionId, rnId, creationTime, attempts, state);
        }

        private static void read(ByteBuffer bb, DataChannel channel)
            throws IOException {
            while (bb.remaining() > 0) {
                if (channel.read(bb) < 0) {
                    throw new IOException("Unexpected EOF");
                }
            }
            bb.flip();
        }

        /* -- Request response -- */

        /*
         * ACK Response
         *
         *  byte Response.OK
         */
        static void writeACKResponse(DataChannel channel) throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            buffer.put((byte)Response.OK.ordinal());
            buffer.flip();

            // TODO - Should this check be != buffer size?
            if (channel.write(buffer) == 0) {
                throw new IOException("Failed to write response. " +
                                      "Send buffer size: " +
                                      channel.socket().getSendBufferSize());
            }
        }

        /*
         * Busy Response
         *
         *  byte   Response.Busy
         *  int    numStreams - the number of migration streams the source
         *                      currently supports (may change, may be 0)
         *  int    reason message length
         *  byte[] reason message bytes
         */
        static void writeBusyResponse(DataChannel channel,
                                      int numStreams,
                                      String message) throws IOException {

            byte[] mb = message.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + mb.length);
            buffer.put((byte)Response.BUSY.ordinal());
            buffer.putInt(numStreams);
            buffer.putInt(mb.length);
            buffer.put(mb);
            buffer.flip();
            if (channel.write(buffer) == 0) {
                throw new IOException("Failed to write response. " +
                                      "Send buffer size: " +
                                      channel.socket().getSendBufferSize());
            }
        }

        /*
         * Error Response
         *
         *  byte   Response.Busy
         *  int    reason message length
         *  byte[] reason message bytes
         */
        static void writeErrorResponse(DataChannel channel,
                                       Response response,
                                       String message) throws IOException {

            byte[] mb = message.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + mb.length);
            buffer.put((byte)response.ordinal());
            buffer.putInt(mb.length);
            buffer.put(mb);
            buffer.flip();
            if (channel.write(buffer) == 0) {
                throw new IOException("Failed to write response. " +
                                      "Send buffer size: " +
                                      channel.socket().getSendBufferSize());
            }
        }

        public static Response readResponse(DataInputStream stream)
            throws IOException {

            int ordinal = stream.read();
            if ((ordinal < 0) || (ordinal >= RESPONSE_VALUES.length)) {
                throw new IOException("Error reading response= " + ordinal);
            }
            return RESPONSE_VALUES[ordinal];
        }

        public static int readNumStreams(DataInputStream stream)
            throws IOException {
            return stream.readInt();
        }

        public static String readReason(DataInputStream stream) {
            try {
                int size = stream.readInt();
                byte[] bytes = new byte[size];
                stream.readFully(bytes);
                return new String(bytes);
            } catch (IOException ioe) {
                return "";
            }
        }
    }

    /* -- DB OPs -- */

    /**
     * Operation messages. These are the messages sent from the source to the
     * target node during the partition data transfer.
     *
     * WARNING: To avoid breaking serialization compatibility, the order of the
     * values must not be changed and new values must be added at the end.
     */
    public enum OP {

        /**
         * A DB read operation. A COPY is generated by the key-ordered reads
         * of the source DB.
         */
        COPY(0),

        /**
         * A client put operation.
         */
        PUT(1),

        /**
         * A client delete operation.
         */
        DELETE(2),

        /**
         * Indicates that client transaction is about to be committed. No
         * further PUT or DELETE messages should be sent for the transaction.
         */
        PREPARE(3),

        /**
         * The client transaction has been successfully committed.
         */
        COMMIT(4),

        /**
         * The client transaction has been aborted.
         */
        ABORT(5),

        /**
         * End of Data. The partition migration data transfer is complete and
         * no further messages will be sent from the source.
         */
        EOD(6),

        /**
         * Last record marker. This informs that source shard has sent the last
         * record.
         */
        LAST_RECORD_MARKER(7);

        private static OP[] VALUES = values();

        OP(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        /*
         * Gets the OP corresponding to the specified ordinal.
         */
        public static OP get(int ordinal) {
            if ((ordinal >= 0) && (ordinal < VALUES.length)) {
                return VALUES[ordinal];
            }
            return null;
        }
    }
}
