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

package oracle.kv.impl.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.Value;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

/**
 * This is the locus of control for secure display of user data.
 *
 * KVStore records are represented within the implementation as
 * - {@link oracle.kv.Key} or {@link oracle.kv.Value}
 * - after passing through to the server side, keys and values are generally
 * represented as byte arrays rather than objects, in order to reduce the cost
 * of serialization and deserialization.
 *
 * Keys and values are meant to be easily viewed as Strings. Keys intentionally
 * provide both toString() and fromString() methods as ways of displaying and
 * creating keys. However, in order to restrict the insecure display of user
 * data, care must be taken to avoid the inadvertent use of Key.toString() and
 * Value.toString().
 *
 * Display Guidelines
 * -------------------
 * - by default, user data is not displayed in server side logs or
 * exceptions. The content of a key or value is replaced with the word
 * "hidden".  It is possible to enable the display of keys or values for
 * debugging purposes.
 *
 * - user data is visible when accessed by utilities whose purpose is to
 * display records, such as a CLI CRUD utility. Security is implemented at
 * a higher level, through privilege controls for that utility
 *
 * - user data may be visible within client side exception messages which
 * are in direct reaction to api operations. User data is most commonly
 * added to IllegalArgumentExceptions, in order to explain the problem.
 * Currently, by default, keys and values are visible. Possible options are
 * to change that default to hidden, to omit the mention of the record on
 * the theory that the target record is self evident in the context of the
 * exception, or to make the record available as a getter on the exception,
 * rather than having the record displayed as part of the message.
 *
 * Implementation Guidelines
 * -------------------------
 * Any display of keys, key ranges, or values in exception messages or to the
 * log should use the static display{Key,Value} methods as gatekeepers, rather
 * than Key.toString(), KeyRange.toString() and Value.toString().
 */
public class UserDataControl {

    private static final String hashAlgorithm = "SHA-256";

    private static DigestCache messageDigest = new DigestCache();

    private static ParameterListener PARAM_SETTER = new ParamSetter();

    /** If true, keys are displayed as the hash value */
    private static volatile boolean hideKey = true;

    /** If true, value are displayed as the hash value */
    private static volatile boolean hideValue = true;

    /**
     * Key hiding may be controlled by a kvstore param, public for
     * access from RepNodeService
     */
    public static void setKeyHiding(boolean shouldHide) {
        hideKey = shouldHide;
    }

    /**
     * Value hiding may be controlled by a kvstore param, public for
     * access from RepNodeService
     */
    public static void setValueHiding(boolean shouldHide) {
        hideValue = shouldHide;
    }

    public static boolean hideUserData() {
        return hideKey || hideValue;
    }

    /**
     * Convert a byte array to a fixed size hex string. A sample of return
     * string is HASH:d4cc965320f6.
     */
    public static String getHash(byte[] bytes) {
        if (messageDigest == null) {
            messageDigest = new DigestCache();
        }
        /* Get instance for use by this thread. */
        final MessageDigest md = messageDigest.get();

        byte[] digest = md.digest(bytes);

        long l = ((long) (digest[0] & 0xff)) << 40 |
        ((long) (digest[1] & 0xff)) << 32 |
        ((long) (digest[2] & 0xff)) << 24 |
        (digest[3] & 0xff) << 16 |
        (digest[4] & 0xff) << 8 |
        (digest[5] & 0xff);
        return "HASH:" + Long.toHexString(l);
    }

    /**
     * Depending on configuration, display:
     * - a string representing the key's hash value or
     * - the actual key, or
     * - "null" if keyBytes are null.
     */
    public static String displayKey(final byte[] keyBytes) {
        if (keyBytes == null) {
            return "null";
        }
        return hideKey ?
               getHash(keyBytes) :
               Key.fromByteArray(keyBytes).toString();
    }

    /**
     * Depending on configuration, display:
     * - a string representing the key's hash value or
     * - the actual key, or
     * - "null" if key is null.
     */
    public static String displayKey(final Key key) {
        if (key == null) {
            return "null";
        }
        return hideKey ?
               getHash(key.toByteArray()) :
               key.toString();
    }

    /**
     * Depending on configuration, display:
     * - a string representing the keyrange's hash value or
     * - the actual keyrange, or
     * - "null" if the keyrange is null.
     */
    public static String displayKeyRange(final KeyRange keyRange) {
        if (keyRange == null) {
            return "null";
        }
        return hideKey ?
               getHash(keyRange.toByteArray()) :
               keyRange.toString();
    }

    /**
     * Depending on configuration, display:
     * - a string representing the row's hash value or
     * - the actual row, or
     * - "null" if row is null.
     */
    public static String displayRow(final Row row) {
        if (row == null) {
            return "null";
        }
        return hideUserData() ?
               getHash(row.toString().getBytes()) :
               row.toString();
    }

    /**
     * Depending on configuration, display:
     * - a string representing the row's hash value or
     * - the actual row in JSON format, or
     * - "null" if row is null.
     */
    public static String displayRowJson(final Row row) {
        if (row == null) {
            return "null";
        }
        return hideUserData() ?
               getHash(row.toJsonString(true).getBytes()) :
               row.toJsonString(true);
    }

    /**
     * Depending on configuration, display:
     * - a string representing the primary key's hash value or
     * - the actual primary key in JSON format, or
     * - "null" if primary key is null.
     */
    public static String displayPrimaryKeyJson(final PrimaryKey key) {
        if (key == null) {
            return "null";
        }
        return hideUserData() ?
               getHash(key.toJsonString(true).getBytes()) :
               key.toJsonString(true);
    }

    /**
     * Provide a Value or a byte array representing the value.
     * Depending on configuration, display:
     * - a string representing the value's hash value or
     * - the actual value or
     * - "null" if both the value and the valueBytes are null
     */
    public static String displayValue(final Value value,
                                      final byte[] valueBytes ) {
        if (value == null) {
            if (valueBytes == null) {
                return "null";
            }

            /* valueBytes is not null, but value is null */
            return hideValue ?
                   getHash(valueBytes) :
                   Value.fromByteArray(valueBytes).toString();
        }

        return hideValue ?
               getHash(value.toByteArray()) :
               value.toString();
    }

    public static ParameterListener getParamListener() {
        return PARAM_SETTER;
    }

    private static class ParamSetter implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            boolean hideData = newMap.getOrDefault
                    (ParameterState.COMMON_HIDE_USERDATA).asBoolean();
            setKeyHiding(hideData);
            setValueHiding(hideData);
        }
    }

    /**
     * Implements a per-thread cache using a thread local, to mitigate the cost
     * of calling MessageDigest.getInstance("SHA-256").
     */
    private static class DigestCache extends ThreadLocal<MessageDigest> {

        /** Create the message digest. */
        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance(hashAlgorithm);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(hashAlgorithm
                                                + " algorithm unavailable");
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
