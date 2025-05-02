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

package oracle.kv;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;

/**
 * Defines the durability characteristics associated with a standalone write
 * (put or update) operation, or in the case of {@link KVStore#execute
 * KVStore.execute} with a set of operations performed in a single transaction.
 * <p>
 * The overall durability is a function of the {@link SyncPolicy} and {@link
 * ReplicaAckPolicy} in effect for the Master, and the {@link SyncPolicy} in
 * effect for each Replica.
 * </p>
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class Durability implements FastExternalizable, Serializable {

    private static final long serialVersionUID = 1;

    /** Disable constructor null parameter checks, for testing. */
    static boolean disableConstructorNullChecks = false;

    /**
     * A convenience constant that defines a durability policy with COMMIT_SYNC
     * for Master commit synchronization.
     *
     * The policies default to COMMIT_NO_SYNC for commits of replicated
     * transactions that need acknowledgment and SIMPLE_MAJORITY for the
     * acknowledgment policy.
     */
    public static final Durability COMMIT_SYNC =
        new Durability(SyncPolicy.SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    /**
     * A convenience constant that defines a durability policy with
     * COMMIT_NO_SYNC for Master commit synchronization.
     *
     * The policies default to COMMIT_NO_SYNC for commits of replicated
     * transactions that need acknowledgment and SIMPLE_MAJORITY for the
     * acknowledgment policy.
     */
    public static final Durability COMMIT_NO_SYNC =
        new Durability(SyncPolicy.NO_SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    /**
     * A convenience constant that defines a durability policy with
     * COMMIT_WRITE_NO_SYNC for Master commit synchronization.
     *
     * The policies default to COMMIT_NO_SYNC for commits of replicated
     * transactions that need acknowledgment and SIMPLE_MAJORITY for the
     * acknowledgment policy.
     */
    public static final Durability COMMIT_WRITE_NO_SYNC =
        new Durability(SyncPolicy.WRITE_NO_SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    /**
     * Defines the synchronization policy to be used when committing a
     * transaction. High levels of synchronization offer a greater guarantee
     * that the transaction is persistent to disk, but trade that off for
     * lower performance.
     *
     * @hidden.see {@link #writeFastExternal FastExternalizable format}
     */
    public enum SyncPolicy implements FastExternalizable {

        /*
         * WARNING: To avoid breaking serialization compatibility, the order of
         * the values must not be changed and new values must be added at the
         * end.
         */

        /**
         *  Write and synchronously flush the log on transaction commit.
         *  Transactions exhibit all the ACID (atomicity, consistency,
         *  isolation, and durability) properties.
         */
        SYNC(0),

        /**
         * Do not write or synchronously flush the log on transaction commit.
         * Transactions exhibit the ACI (atomicity, consistency, and isolation)
         * properties, but not D (durability); that is, database integrity will
         * be maintained, but if the application or system fails, it is
         * possible some number of the most recently committed transactions may
         * be undone during recovery. The number of transactions at risk is
         * governed by how many log updates can fit into the log buffer, how
         * often the operating system flushes dirty buffers to disk, and how
         * often log checkpoints occur.
         */
        NO_SYNC(1),

        /**
         * Write but do not synchronously flush the log on transaction commit.
         * Transactions exhibit the ACI (atomicity, consistency, and isolation)
         * properties, but not D (durability); that is, database integrity will
         * be maintained, but if the operating system fails, it is possible
         * some number of the most recently committed transactions may be
         * undone during recovery. The number of transactions at risk is
         * governed by how often the operating system flushes dirty buffers to
         * disk, and how often log checkpoints occur.
         */
        WRITE_NO_SYNC(2);

        private static final SyncPolicy[] VALUES = values();
        public static final int VALUES_COUNT = VALUES.length;

        private SyncPolicy(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        static SyncPolicy readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readUnsignedByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "unknown SyncPolicy: " + ordinal);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #SYNC}=0, {@link
         *      #NO_SYNC}=1, {@link #WRITE_NO_SYNC}=2
         * </ol>
         *
         * @hidden For internal use only
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    /**
     * A replicated environment makes it possible to increase an application's
     * transaction commit guarantees by committing changes to its replicas on
     * the network. ReplicaAckPolicy defines the policy for how such network
     * commits are handled.
     *
     * @hidden.see {@link #writeFastExternal FastExternalizable format}
     */
    public enum ReplicaAckPolicy implements FastExternalizable {

        /**
         * All replicas must acknowledge that they have committed the
         * transaction. This policy should be selected only if your replication
         * group has a small number of replicas, and those replicas are on
         * extremely reliable networks and servers.
         */
        ALL(0),

        /**
         * No transaction commit acknowledgments are required and the master
         * will never wait for replica acknowledgments. In this case,
         * transaction durability is determined entirely by the type of commit
         * that is being performed on the master.
         */
        NONE(1),

        /**
         * A simple majority of replicas must acknowledge that they have
         * committed the transaction. This acknowledgment policy, in
         * conjunction with an election policy which requires at least a simple
         * majority, ensures that the changes made by the transaction remains
         * durable if a new election is held.
         */
        SIMPLE_MAJORITY(2);

        private static final ReplicaAckPolicy[] VALUES = values();
        static final int VALUES_COUNT = VALUES.length;

        private ReplicaAckPolicy(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        static ReplicaAckPolicy readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readUnsignedByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "unknown ReplicaAckPolicy: " + ordinal);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>value</i> // {@link #ALL}=0, {@link #NONE}=1,
         *      {@link #SIMPLE_MAJORITY}=2
         * </ol>
         *
         * @hidden For internal use only
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    /* The sync policy in effect on the Master node. */
    private final SyncPolicy masterSync;

    /* The sync policy in effect on a replica. */
    final private SyncPolicy replicaSync;

    /* The replica acknowledgment policy to be used. */
    final private ReplicaAckPolicy replicaAck;

    /**
     * A multi-dimensional array indexed by: Master sync policy, Replica sync
     * policy and the AckPolicy, to yield a durability with those
     * characteristics.
     */
    private final static Durability durabilityMap[][][] =
        new Durability[SyncPolicy.VALUES_COUNT][SyncPolicy.VALUES_COUNT]
            [ReplicaAckPolicy.VALUES_COUNT];

    static {
        /* Initialize durabilityMap */
        for (SyncPolicy masterSync : SyncPolicy.values()) {
            for (SyncPolicy replicaSync : SyncPolicy.values()) {
                for (ReplicaAckPolicy replicaAckPolicy :
                         ReplicaAckPolicy.values()) {
                    durabilityMap[masterSync.ordinal()]
                                 [replicaSync.ordinal()]
                                 [replicaAckPolicy.ordinal()] =
                    new Durability(masterSync, replicaSync, replicaAckPolicy);
                }
            }
        }
    }

    /**
     * Creates an instance of a Durability specification.
     *
     * @param masterSync the SyncPolicy to be used when committing the
     * transaction on the Master.
     * @param replicaSync the SyncPolicy to be used remotely, as part of a
     * transaction acknowledgment, at a Replica node.
     * @param replicaAck the acknowledgment policy used when obtaining
     * transaction acknowledgments from Replicas.
     */
    public Durability(SyncPolicy masterSync,
                      SyncPolicy replicaSync,
                      ReplicaAckPolicy replicaAck) {
        this.masterSync = masterSync;
        this.replicaSync = replicaSync;
        this.replicaAck = replicaAck;
        if (!disableConstructorNullChecks) {
            checkValidFields();
        }
    }

    private void checkValidFields() {
        checkNull("masterSync", masterSync);
        checkNull("replicaSync", replicaSync);
        checkNull("replicaAck", replicaAck);
    }

    /**
     * For internal *test* use only.
     *
     * @hidden
     */
    public Durability(DataInput in, short serialVersion)
        throws IOException {

        final Durability d = Durability.readFastExternal(in, serialVersion);

        this.masterSync = d.masterSync;
        this.replicaSync = d.replicaSync;
        this.replicaAck = d.replicaAck;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Deserializes durability from the input stream to yield an immutable
     * instance of durability that can be shared.
     */
    public static Durability readFastExternal(DataInput in,
                                              short serialVersion)
        throws IOException {

        final SyncPolicy masterSync =
            SyncPolicy.readFastExternal(in, serialVersion);
        final SyncPolicy replicaSync =
            SyncPolicy.readFastExternal(in, serialVersion);
        final ReplicaAckPolicy replicaAck =
            ReplicaAckPolicy.readFastExternal(in, serialVersion);

        return durabilityMap[masterSync.ordinal()][replicaSync.ordinal()]
            [replicaAck.ordinal()];
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SyncPolicy}) {@link #getMasterSync masterSync}
     * <li> ({@link SyncPolicy}) {@link #getReplicaSync replicaSync}
     * <li> ({@link ReplicaAckPolicy}) {@link #getReplicaAck replicaAck}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        masterSync.writeFastExternal(out, serialVersion);
        replicaSync.writeFastExternal(out, serialVersion);
        replicaAck.writeFastExternal(out, serialVersion);
    }

    /**
     * Returns this Durability as a serialized byte array, such that {@link
     * #fromByteArray} may be used to reconstitute the Durability.  Values
     * returned by calls to this method can be used with current and newer
     * releases, but are not guaranteed to be compatible with earlier releases.
     */
    public byte[] toByteArray() {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(200);
        try {
            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeShort(SerialVersion.CURRENT);
            writeFastExternal(oos, SerialVersion.CURRENT);

            oos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            /* Should never happen. */
            throw new FaultException(e, false /*isRemote*/);
        }
    }

    /**
     * Deserializes the given bytes that were returned earlier by {@link
     * #toByteArray} and returns the resulting Durability.  Values created with
     * either the current or earlier releases can be used with this method, but
     * values created by later releases are not guaranteed to be compatible.
     */
    public static Durability fromByteArray(byte[] keyBytes) {

        final ByteArrayInputStream bais = new ByteArrayInputStream(keyBytes);
        try {
            final ObjectInputStream ois = new ObjectInputStream(bais);

            final short serialVersion = ois.readShort();

            return readFastExternal(ois, serialVersion);

        } catch (IOException e) {
            /* Should never happen. */
            throw new FaultException(e, false /*isRemote*/);
        }
    }

    @Override
    public String toString() {
        return masterSync.toString() + "," +
               replicaSync.toString() + "," +
               replicaAck.toString();
    }

    /**
     * Returns the transaction synchronization policy to be used on the Master
     * when committing a transaction.
     */
    public SyncPolicy getMasterSync() {
        return masterSync;
    }

    /**
     * Returns the transaction synchronization policy to be used by the replica
     * as it replays a transaction that needs an acknowledgment.
     */
    public SyncPolicy getReplicaSync() {
        return replicaSync;
    }

    /**
     * Returns the replica acknowledgment policy used by the master when
     * committing changes to a replicated environment.
     */
    public ReplicaAckPolicy getReplicaAck() {
        return replicaAck;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + masterSync.hashCode();
        result = (prime * result) + replicaAck.hashCode();
        result = (prime * result) + replicaSync.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Durability)) {
            return false;
        }
        Durability other = (Durability) obj;
        if (!masterSync.equals(other.masterSync)) {
            return false;
        }
        if (!replicaAck.equals(other.replicaAck)) {
            return false;
        }
        if (!replicaSync.equals(other.replicaSync)) {
            return false;
        }
        return true;
    }

    /**
     * Enforce valid field values.
     */
    private void readObject(ObjectInputStream in)
        throws ClassNotFoundException, IOException {

        in.defaultReadObject();
        try {
            checkValidFields();
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid field: " + e.getMessage(), e);
        }
    }
}
