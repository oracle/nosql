/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static standalone.datacheck.ByteUtils.bytesToInt;
import static standalone.datacheck.ByteUtils.bytesToLong;
import static standalone.datacheck.ByteUtils.bytesToShort;
import static standalone.datacheck.ByteUtils.fiveBytesToLong;
import static standalone.datacheck.ByteUtils.intToBytes;
import static standalone.datacheck.ByteUtils.longToBytes;
import static standalone.datacheck.ByteUtils.longToFiveBytes;
import static standalone.datacheck.ByteUtils.longToSixBytes;
import static standalone.datacheck.ByteUtils.shortToBytes;
import static standalone.datacheck.ByteUtils.sixBytesToLong;

import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Utilities for using a secret key (symmetric) cipher to encrypt or decrypt
 * integers.
 */
class CipherUtils {

    /**
     * The cipher algorithm.  We're using the AES cipher.  The CFB8 mode
     * encodes one 8-bit byte at a time, so we can use it to encode arbitrary
     * streams of bytes, and the encryption of each byte feeds back to the
     * encryption of the next bytes, so we get good randomness.  The NOPADDING
     * padding mode means we get exactly the same number of bytes out as we put
     * in.
     */
    private static final String ALGORITHM = "AES/CFB8/NOPADDING";

    /** The key algorithm. */
    private static final String KEY_ALGORITHM = "AES";

    /** The number of key bytes. */
    private static final int KEY_BYTES = 16;

    /** The number of bytes in the initialization vector. */
    private static final int IV_NUM_BYTES = 16;

    /**
     * A thread-local copy of the cipher.  Methods should initialize the cipher
     * before use and not call other methods that use it.
     */
    private static final ThreadLocal<Cipher> perThreadCipher =
        new ThreadLocal<Cipher>() {
            @Override
            protected Cipher initialValue() {
                try {
                    return Cipher.getInstance(ALGORITHM);
                } catch (NoSuchAlgorithmException e) {
                    throw new ExceptionInInitializerError(e);
                } catch (NoSuchPaddingException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }
        };

    /**
     * Creates an initialization vector using the specified bytes.  Only
     * IV_NUM_BYTES of them are used.  The initialization vector is an extra
     * piece of state that effects the way the encryption is performed,
     * independent of the key.
     */
    static IvParameterSpec createIvParameterSpec(byte[] bytes) {
        bytes = Arrays.copyOf(bytes, IV_NUM_BYTES);
        return new IvParameterSpec(bytes);
    }

    /**
     * Creates a secret key from the specified bytes.  Only KEY_BYTES of them
     * are used.
     */
    static SecretKeySpec createKey(byte[] bytes) {
        return new SecretKeySpec(Arrays.copyOf(bytes, KEY_BYTES),
                                 KEY_ALGORITHM);
    }

    /** Encrypt a long */
    static long encryptLong(long l, SecretKeySpec key, IvParameterSpec iv) {
        return bytesToLong(cryptBytes(longToBytes(l), key, iv, ENCRYPT_MODE));
    }

    /** Decrypt a long */
    static long decryptLong(long l, SecretKeySpec key, IvParameterSpec iv) {
        return bytesToLong(cryptBytes(longToBytes(l), key, iv, DECRYPT_MODE));
    }

    /** Encrypt a 48-bit long */
    static long encryptSixByteLong(long l,
                                   SecretKeySpec key,
                                   IvParameterSpec iv) {
        return sixBytesToLong
            (cryptBytes(longToSixBytes(l), key, iv, ENCRYPT_MODE));
    }

    /** Decrypt a 48-bit long */
    static long decryptSixByteLong(long l,
                                   SecretKeySpec key,
                                   IvParameterSpec iv) {
        return sixBytesToLong
            (cryptBytes(longToSixBytes(l), key, iv, DECRYPT_MODE));
    }

    /** Encrypt a 40-bit long */
    static long encryptFiveByteLong(long l,
                                    SecretKeySpec key,
                                    IvParameterSpec iv) {
        return fiveBytesToLong
            (cryptBytes(longToFiveBytes(l), key, iv, ENCRYPT_MODE));
    }

    /** Decrypt a 40-bit long */
    static long decryptFiveByteLong(long l,
                                    SecretKeySpec key,
                                    IvParameterSpec iv) {
        return fiveBytesToLong
            (cryptBytes(longToFiveBytes(l), key, iv, DECRYPT_MODE));
    }

    /** Encrypt an int */
    static int encryptInt(int i, SecretKeySpec key, IvParameterSpec iv) {
        return bytesToInt(cryptBytes(intToBytes(i), key, iv, ENCRYPT_MODE));
    }

    /** Decrypt an int */
    static int decryptInt(int i, SecretKeySpec key, IvParameterSpec iv) {
        return bytesToInt(cryptBytes(intToBytes(i), key, iv, DECRYPT_MODE));
    }

    /** Encrypt a short */
    static short encryptShort(short s, SecretKeySpec key, IvParameterSpec iv) {
        return bytesToShort(cryptBytes(shortToBytes(s), key, iv, ENCRYPT_MODE));
    }

    /** Decrypt a short */
    static short decryptShort(short s, SecretKeySpec key, IvParameterSpec iv) {
        return bytesToShort(cryptBytes(shortToBytes(s), key, iv, DECRYPT_MODE));
    }

    /** Encrypt or decrypt an array of bytes. */
    static byte[] cryptBytes(byte[] bytes,
                             SecretKeySpec key,
                             IvParameterSpec iv,
                             int cipherMode) {
        Cipher cipher = perThreadCipher.get();
        try {
            cipher.init(cipherMode, key, iv);
            return cipher.doFinal(bytes);
        } catch (IllegalBlockSizeException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (BadPaddingException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
