/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.util.ldiff;

import java.util.Arrays;
import java.util.Formatter;

public class Block implements java.io.Serializable {

    private static final long serialVersionUID = 111858779935447845L;

    /* The block ID. */
    private final int blockId;

    /* The actual records that the block holds. */
    int numRecords;

    /*
     * For debugging support and to minimize the actual data that is
     * transferred over the network, I store the beginKey and endKey as the
     * index to each of the block.
     * 
     * TODO to optimize: replace the {beginKey, endKey} by something like LSN.
     */

    /* The database key that the current block starts with. */
    private byte[] beginKey;

    /* The database key that the current block ends with. */
    private byte[] beginData;

    /* The rolling checksum computed from the sequence of Adler32 checksums. */
    private long rollingChksum;

    /* An sha256 hash is also computed for each block. */
    private byte[] sha256Hash;

    public Block(int blockId) {
        this.blockId = blockId;
    }

    int getBlockId() {
        return blockId;
    }

    int getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

    byte[] getBeginKey() {
        return beginKey;
    }

    public void setBeginKey(byte[] beginKey) {
        this.beginKey = beginKey;
    }

    byte[] getBeginData() {
        return beginData;
    }

    public void setBeginData(byte[] beginData) {
        this.beginData = beginData;
    }

    long getRollingChksum() {
        return rollingChksum;
    }

    public void setRollingChksum(long rollingChksum) {
        this.rollingChksum = rollingChksum;
    }

    byte[] getSHA256Hash() {
        return sha256Hash;
    }

    public void setSHA256Hash(byte[] sha256Hash) {
        this.sha256Hash = sha256Hash;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Block)) {
            return false;
        }
        final Block other = (Block) o;
        return (this.blockId == other.blockId) &&
            (this.numRecords == other.numRecords) &&
            Arrays.equals(this.beginKey, other.beginKey) &&
            Arrays.equals(this.beginData, other.beginData) &&
            (this.rollingChksum == other.rollingChksum) &&
            Arrays.equals(this.sha256Hash, other.sha256Hash);
    }

    @Override
    public String toString() {
        final Formatter fmt = new Formatter();
        fmt.format("Block %d: rollingChksum=%x sha256Hash=%s numRecords=%d",
                   blockId, rollingChksum, Arrays.toString(sha256Hash),
                   numRecords);
        return fmt.toString();
    }
}
