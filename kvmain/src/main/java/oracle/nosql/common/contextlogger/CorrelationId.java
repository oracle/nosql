/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.contextlogger;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The CorrelationId is intended for use in log messsages to associate the
 * messages with the original user request that triggered them.
 *
 * The value of a Correlation ID is a twelve-byte number.  The highest-order
 * five bytes represent the number of milliseconds since the epoch, which is
 * defined to be January 1, 2017.  The biggest value that can be stored in 5
 * bytes is about 1.1 trillion, which translates to nearly 35 years' worth of
 * milliseconds.  This value we refer to as the "since" part of an id.
 *
 * The next byte after "since" is the "entropy" byte, which is defined as a
 * sample of the lowest-order byte of the nanosecond clock.
 *
 * Together, the five "since" bytes and the one "entropy" byte are known as the
 * "fingerprint."  The fingerprint is statically initialized when the class is
 * loaded and retains the same value for the lifetime of the process.
 *
 * We expect the fingerprint to be unique among processes that might be
 * involved in writing log records in a NDCS instance.  It is first of all
 * unlikely that multiple processes will load this class during the same
 * millisecond; and with the addition of the entropy byte the likelihood of two
 * fingerprints matching is further reduced.
 *
 * Following the six-byte fingerprint is a sequence number that is derived from
 * incrementing a static AtomicLong.
 *
 * The string representation of the CorrelationId is in base-36.  It is
 * returned by the toString method.  It is possible to reconstitute a
 * CorrelationId from its string representation via the parse method.
 */
public class CorrelationId {
    /**
     * Create a new CorrelationId
     */
    public static CorrelationId getNext() {
        return new CorrelationId(counter.getAndIncrement());
    }

    /**
     * @return a base-36 string representation of the CorrelationId.
     */
    @Override
    public String toString() {
        return getBigInteger().toString(36);
    }

    /**
     * Reconstitute the original CorrelationId from its String representation.
     */
    public static CorrelationId parse(String id) {
        return new CorrelationId(new BigInteger(id, 36).toByteArray());
    }

    /*
     * Timestamps use this date as the epoch.
     */
    private static final String EPOCH = "2017-01-01 00:00:00 UTC";

    /*
     * The fingerprint is a historically unique identifier for a process.
     * Its least significant byte is a sample of nanotime; the next five bytes
     * represent the number of ms since January 1, 2017.
     */
    private static final byte[] fingerprint;

    /*
     * The counter is incremented every time a new CorrelationId is created.
     */
    private static final AtomicLong counter = new AtomicLong(0L);

    /*
     * The complete ID has is a twelve-byte value representing six bytes of
     * fingerprint and six bytes of sequence number.
     */
    private byte[] unique;

    static {
        Date e;
        try {
            e = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss zzz").parse(EPOCH);
        } catch (ParseException pe) {
            throw new ExceptionInInitializerError
                ("Cannot parse epoch string " + EPOCH);
        }
        final long now = System.currentTimeMillis();
        final long since = now - e.getTime();
        final long entropy = System.nanoTime();
        fingerprint = new byte[] {
            (byte)(since >>> 32), /* byte 4 of since */
            (byte)(since >>> 24), /* byte 3 of since */
            (byte)(since >>> 16), /* byte 2 of since */
            (byte)(since >>> 8),  /* byte 1 of since */
            (byte)since,          /* byte 0 of since */
            (byte)entropy         /* byte 0 of entropy  */
        };
    }

    /*
     * Construct a CorrelationId from the static fingerprint and the given
     * sequence number value.
     */
    private CorrelationId(long c) {
        unique  = new byte[] {
            fingerprint[0],
            fingerprint[1],
            fingerprint[2],
            fingerprint[3],
            fingerprint[4],
            fingerprint[5],
            (byte)(c >>> 40), /* byte 5 of c */
            (byte)(c >>> 32), /* byte 4 of c */
            (byte)(c >>> 24), /* byte 3 of c */
            (byte)(c >>> 16), /* byte 2 of c */
            (byte)(c >>> 8),  /* byte 1 of c */
            (byte)c           /* byte 0 of c */
        };
    }

    /*
     * Construct a CorrelationId from the given 12-byte array.
     */
    private CorrelationId(byte[] unique) {
        this.unique = unique;
    }

    /*
     * Get the unique value as a BigInteger.
     */
    private BigInteger getBigInteger() {
        return new BigInteger(unique);
    }

    /*
     * Extract the number of milliseconds since the start of 2017.
     */
    public long getSince() {
    	final byte[] since = new byte[] {
    			unique[0],
    			unique[1],
    			unique[2],
    			unique[3],
    			unique[4]
    	};
    	return new BigInteger(since).longValue();
    }

    /*
     * Extract the entropy byte.
     */
    public byte getEntropy() {
    	return unique[5];
    }

    /*
     * Get the sequence number part.
     */
    public long getSequence() {
    	final byte[] since = new byte[] {
    			unique[6],
    			unique[7],
    			unique[8],
    			unique[9],
    			unique[10],
    			unique[11]
    	};
    	return new BigInteger(since).longValue();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(unique);
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
        if (!(obj instanceof CorrelationId)) {
            return false;
        }
        CorrelationId other = (CorrelationId) obj;
        if (!Arrays.equals(unique, other.unique)) {
            return false;
        }
        return true;
    }
}
