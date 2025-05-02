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

package oracle.kv.impl.api.table;

import oracle.nosql.nson.Nson;

/**
 * NsonRow represents a Row along with row metadata but with the key and value
 * in NSON format. It is not really a "Row" in that it does not implement that
 * interface. It's primary use is for serialization/deserialization of rows
 * for streaming, replication and backup/restore
 */
public class NsonRow {

    private final long modificationTime;
    private final long expirationTime;
    private final int regionId;
    private final byte[] nsonKey;
    private final boolean isTombstone;
    private final TableImpl table; /* used for re-serialization to Avro */

    /*
     * Nson value can be null. As a stream event a null value may mean that
     * it's delete or tombstone
     */
    private final byte[] nsonValue;

    public NsonRow(long modificationTime,
                   long expirationTime,
                   int regionId,
                   byte[] nsonKey,
                   byte[] nsonValue,
                   boolean isTombstone,
                   TableImpl table) {
        if (nsonKey == null) {
            throw new IllegalArgumentException("Primary key can not be null");
        }
        this.modificationTime = modificationTime;
        this.expirationTime = expirationTime;
        this.regionId = regionId;
        this.nsonKey = nsonKey;
        this.nsonValue = nsonValue;
        this.isTombstone = isTombstone;
        this.table = table;
    }

    public TableImpl getTable() {
        return table;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public int getRegionId() {
        return regionId;
    }

    public byte[] getNsonKey() {
        return nsonKey;
    }

    public byte[] getNsonValue() {
        return nsonValue;
    }

    public boolean getIsTombstone() {
        return isTombstone;
    }

    public long getSize() {
        /*
         * Use uncompressed sizes for the metadata
         *
         * expiration time (long, 8 bytes)
         * modification time (long, 8 bytes)
         * regionId (int, 4 bytes)
         * plus key and value sizes
         */
        long size = 20 + nsonKey.length;
        if (nsonValue != null) {
            size += nsonValue.length;
        }
        return size;
    }

    public String nsonToJson() {
        StringBuilder json = new StringBuilder();
        json.append("{\"key\":").append(Nson.toJsonString(nsonKey));
         if (nsonValue != null) {
             json.append(",\"value\":").append(Nson.toJsonString(nsonValue));
         }
        json.append("}");
        return json.toString();
    }
}
