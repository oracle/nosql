/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;

/**
 * Provides utilities for permuting the order of a collection of integers of a
 * fixed size.  The transform and untransform operations are inverses of each
 * other. <p>
 *
 * These methods can be used to convert an increasing sequence of ints into a
 * sequence of unique ints ordered pseudo-randomly, but making it possible to
 * convert the random values back to the original sequence numbers.
 */
class Permutation {

    /**
     * The initialization vector shared by all keys.  Unlike the normal
     * circumstances with encryption, we want a given key to always encrypt the
     * same way given the particular key bytes, so always use the same
     * initialization vector.
     */
    private static final IvParameterSpec IV =
        CipherUtils.createIvParameterSpec(new byte[] { 1, 2, 6 });

    /** The key for this permutation. */
    private final SecretKeySpec key;

    /**
     * Creates an instance of this class.
     *
     * @param key the key that uniquely defines the permutation
     */
    Permutation(long key) {
        this.key = CipherUtils.createKey(ByteUtils.longToBytes(key));
    }

    /** Transform a long. */
    long transformLong(long l) {
        return CipherUtils.encryptLong(l, key, IV);
    }

    /** Reverse the transformation of a long. */
    long untransformLong(long l) {
        return CipherUtils.decryptLong(l, key, IV);
    }

    /** Transform a 48-bit long. */
    long transformSixByteLong(long l) {
        return CipherUtils.encryptSixByteLong(l, key, IV);
    }

    /** Reverse the transformation of a 48-bit long. */
    long untransformSixByteLong(long l) {
        return CipherUtils.decryptSixByteLong(l, key, IV);
    }

    /** Transform a 40-bit long. */
    long transformFiveByteLong(long l) {
        return CipherUtils.encryptFiveByteLong(l, key, IV);
    }

    /** Reverse the transformation of a 40-bit long. */
    long untransformFiveByteLong(long l) {
        return CipherUtils.decryptFiveByteLong(l, key, IV);
    }

    /** Transform a short. */
    short transformShort(short s) {
        return CipherUtils.encryptShort(s, key, IV);
    }

    /** Reverse the transformation of a short. */
    short untransformShort(short s) {
        return CipherUtils.decryptShort(s, key, IV);
    }
}
