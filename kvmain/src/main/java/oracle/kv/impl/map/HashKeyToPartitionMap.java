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

package oracle.kv.impl.map;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import oracle.kv.Key;
import oracle.kv.impl.topo.PartitionId;

/**
 * A hash based implementation used to distribute keys across partitions.
 */
public class HashKeyToPartitionMap implements KeyToPartitionMap {

    private static final long serialVersionUID = 1L;

    final BigInteger nPartitions;

    transient DigestCache digestCache = new DigestCache();

    public HashKeyToPartitionMap(int nPartitions) {
        super();
        this.nPartitions = new BigInteger(Integer.toString(nPartitions));
    }

    @Override
    public int getNPartitions() {
        return nPartitions.intValue();
    }

    @Override
    public PartitionId getPartitionId(byte[] keyBytes) {
        if (digestCache == null) {
            digestCache = new DigestCache();
        }
        /* Clone one for use by this thread. */
        final MessageDigest md = digestCache.get();

        /* Digest Key major path. */
        md.update(keyBytes, 0, Key.getMajorPathLength(keyBytes));

        final BigInteger index = new BigInteger(md.digest()).mod(nPartitions);
        return new PartitionId(index.intValue() + 1);
    }

    /**
     * Implements a per-thread cache using a thread local, to mitigate the cost
     * of calling MessageDigest.getInstance("MD5").
     */
    static class DigestCache extends ThreadLocal<MessageDigest> {

        /** Create the message digest. */
        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("MD5 algorithm unavailable");
            }
        }

        /** Reset the message digest before returning. */
        @Override
        public MessageDigest get() {
            final MessageDigest md = super.get();
            md.reset();
            return md;
        }
    }
}
