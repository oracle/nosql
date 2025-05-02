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
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Thrown when write operations cannot be initiated because a quorum of
 * Replicas as determined by the {@link ReplicaAckPolicy} was not
 * available.
 *
 * <p>The likelihood of this exception being thrown depends on the number of
 * nodes per replication group, the rate of node failures and how quickly a
 * failed node is restored to operation, and the specified {@code
 * ReplicaAckPolicy}.  The {@code ReplicaAckPolicy} for the default durability
 * policy (specified by {@link KVStoreConfig#getDurability}) is {@link
 * ReplicaAckPolicy#SIMPLE_MAJORITY}.  With {@code SIMPLE_MAJORITY},
 * this exception is thrown only when the majority of nodes in a replication
 * group are unavailable, and in a well-maintained KVStore system with at least
 * three nodes per replication group this exception should rarely be
 * thrown.</p>
 *
 * <p>If the client overrides the default and specifies {@link
 * ReplicaAckPolicy#ALL}, then this exception will be thrown when
 * any node in a replication group is unavailable; in other words, it is much
 * more likely to be thrown.  If the client specifies {@link
 * ReplicaAckPolicy#NONE}, then this exception will never be
 * thrown.</p>
 * 
 * <p>When this exception is thrown the KVStore service will perform
 * administrative notifications so that actions can be taken to correct the
 * problem.  Depending on the nature of the application, the client may wish
 * to</p>
 * <ul>
 * <li>retry the write operation immediately,</li>
 * <li>fall back to a read-only mode and resume write operations at a later
 * time, or</li>
 * <li>give up and report an error at a higher level.</li>
 * </ul>
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class DurabilityException extends FaultException {

    private static final long serialVersionUID = 1L;

    private final ReplicaAckPolicy ackPolicy;
    private final int requiredNodeCount;
    private final Set<String> availableReplicas;

    /**
     * For internal use only.
     * @hidden
     */
    public DurabilityException(Throwable cause,
                               ReplicaAckPolicy ackPolicy,
                               int requiredNodeCount,
                               Set<String> availableReplicas) {
        super(cause, true /*isRemote*/);
        this.ackPolicy = ackPolicy;
        this.requiredNodeCount = requiredNodeCount;
        this.availableReplicas = availableReplicas;
        checkValidFields();
    }

    /*
     * Various fields could be null in KV version 4.3 and before because there
     * were no null checks at that time, although nulls are unlikely, because
     * the constructor was hidden and existing callers provided non-null
     * values.
     *
     * TODO: Add a readObject method that checks for null when KV version 4.3
     * compatibility is no longer required.
     */
    private void checkValidFields() {
        checkNull("ackPolicy", ackPolicy);
        checkNull("availableReplicas", availableReplicas);
        for (final String s : availableReplicas) {
            checkNull("element of availableReplicas", s);
        }
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public DurabilityException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        ackPolicy = ReplicaAckPolicy.readFastExternal(in, serialVersion);
        requiredNodeCount = readPackedInt(in);
        final int availableReplicasCount = readNonNullSequenceLength(in);
        availableReplicas = new HashSet<String>(availableReplicasCount);
        for (int i = 0; i < availableReplicasCount; i++) {
            availableReplicas.add(readNonNullString(in, serialVersion));
        }
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FaultException}) {@code super}
     * <li> ({@link ReplicaAckPolicy}) {@link #getCommitPolicy ackPolicy}
     * <li> ({@link SerializationUtil#writePackedInt packed int}) {@link
     *      #getRequiredNodeCount requiredNodeCount}
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) <i>availableReplicas count</i>
     * <li> For each replica:
     *   <ol type="a">
     *   <li> ({@link SerializationUtil#writeNonNullString non-null String})
     *        <i>available replica</i>
     *   </ol>
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        ackPolicy.writeFastExternal(out, serialVersion);
        writePackedInt(out, requiredNodeCount);
        writeNonNullSequenceLength(out, availableReplicas.size());
        for (final String s : availableReplicas) {
            writeNonNullString(out, serialVersion, s);
        }
    }

    /**
     * Returns the Replica ack policy that was in effect for the operation.
     */
    public ReplicaAckPolicy getCommitPolicy() {
        return ackPolicy;
    }

    /**
     * Returns the number of nodes that were required to be active in order to
     * satisfy the Replica ack policy associated with the operation.
     */
    public int getRequiredNodeCount() {
        return requiredNodeCount;
    }

    /**
     * Returns the set of Replicas that were available at the time of the
     * operation.
     */
    public Set<String> getAvailableReplicas() {
        return availableReplicas;
    }

    /**
     * Return whether it known that the operation that produced this exception
     * did not have side effects. Applications that receive an exception of
     * this class when performing a modify operation and find that this method
     * returns true can safely assume that none of the changes requested by the
     * operation have been performed. If the return value is false, then the
     * operation may or may not have had side effects.
     *
     * @return true if it known that the operation that produced this exception
     * did not have side effects, otherwise false.
     */
    public boolean getNoSideEffects() {
        /*
         * For remote exception, its cause is null, but its fault class name is
         * set to be the original cause class name. So we use its fault class
         * name to check if it's caused by IRE.
         */
        return getFaultClassName().equals(
            "com.sleepycat.je.rep.InsufficientReplicasException");
    }
}
