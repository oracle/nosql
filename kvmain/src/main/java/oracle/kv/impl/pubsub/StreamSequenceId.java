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

import java.nio.ByteBuffer;

import oracle.kv.pubsub.StreamOperation;

/**
 * Object represents a unique sequence id associated with a stream operation,
 * e.g, a put or delete in NoSQL Stream.
 *
 * Please note the scope of sequence id is replication group (shard). That
 * is, only stream operations from the same shard are comparable with their
 * respective sequence ids to determine the order of events. Stream operations
 * from different shard cannot be compared on their sequence ids to determine
 * their arrival order. We keep this semantics for now until a new global VLSN
 * is introduced to order events and operations from different replication
 * groups.
 */
public class StreamSequenceId implements StreamOperation.SequenceId {

    /* internally the sequence id is represented as long */
    final private long sequence;

    StreamSequenceId(long sequence) {
        this.sequence = sequence;
    }

    /**
     * Returns a byte representation suitable for saving in some other
     * data source. The byte array representation can also be used as a for
     * comparisons.
     */
    @Override
    public byte[] getBytes() {
        return longToBytes(sequence);
    }

    /**
     * Compares this object with the specified object for order.  Returns a
     * negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     *
     * @param other   the object to be compared.
     *
     * @return  negative, 0, and positive values as this object is less than,
     * equal to, or greater than the specified object.
     *
     * @throws ClassCastException if compare an object which is not an
     * instance of StreamSequenceId
     * @throws NullPointerException if the specified object is null
     */
    @Override
    public int compareTo(StreamOperation.SequenceId other) {

        if (other == null) {
            throw new NullPointerException("Sequence id cannot be null");
        }

        if (!(other instanceof StreamSequenceId)) {
            throw new ClassCastException("Compare to an object that " +
                                         "is not an instance of " +
                                         "StreamSequenceId");
        }

        return Long.signum(sequence - ((StreamSequenceId) other).getSequence());
    }

    public long getSequence() {
        return sequence;
    }

    private static byte[] longToBytes(long x) {
        final ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(x);
        return buffer.array();
    }

    @Override
    public String toString() {
        return String.valueOf(sequence);
    }
}
