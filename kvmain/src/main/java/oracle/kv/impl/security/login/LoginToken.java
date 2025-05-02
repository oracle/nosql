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
package oracle.kv.impl.security.login;

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
 * LoginToken provides a wrapper around a login session identifier and
 * public information about the session. It is used to communicate login
 * authentication between client and server as well as between server
 * components.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public final class LoginToken implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1;

    /*
     * Symbols used to converting hex string
     */
    private static final String HEX = "0123456789ABCDEF";

    /*
     * The time at which the session will expire, provided in milliseconds
     * since the epoch.
     */
    private long expireAt;

    /*
     * The session identifier
     */
    private SessionId sessionId;

    /**
     * Constructor.
     * @param sessionId The session identifier
     * @param expireTime The time at which the token expires, in milliseconds.
     */
    public LoginToken(SessionId sessionId, long expireTime) {
        this.sessionId = sessionId;
        this.expireAt = expireTime;
    }

    /* for FastExternalizable */
    public LoginToken(DataInput in, short serialVersion)
        throws IOException {

        expireAt = in.readLong();
        sessionId = new SessionId(in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link DataOutput#writeLong long}) {@link #getExpireTime expireAt}
     * <li> ({@link SessionId}) {@link #getSessionId sessionId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        out.writeLong(expireAt);
        sessionId.writeFastExternal(out, serialVersion);
    }

    /**
     * Return the session Id value for the token.
     */
    public SessionId getSessionId() {
        return sessionId;
    }

    /**
     * Return the time at which the session expires.  Time is in the
     * units of System.currentTimeMillis().
     */
    public long getExpireTime() {
        return expireAt;
    }

    /**
     * Encode the token into a byte array for later resurrection by
     * {@link #fromByteArray}.
     */
    public byte[] toByteArray() {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeShort(SerialVersion.CURRENT);
            writeFastExternal(oos, SerialVersion.CURRENT);
            oos.close();

            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unabled to encode", ioe);
        }
    }

    /**
     * Convert a byte array created by {@link #toByteArray} back into a
     * LoginToken object.
     */
    public static LoginToken fromByteArray(byte[] bytes) {
        try {
            final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            final ObjectInputStream ois = new ObjectInputStream(bais);

            final short version = ois.readShort();
            assert version == SerialVersion.CURRENT;
            final LoginToken result = new LoginToken(ois, version);
            ois.close();
            return result;
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unable to decode", ioe);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other.getClass() != LoginToken.class) {
            return false;
        }

        final LoginToken otherToken = (LoginToken) other;
        if (expireAt == otherToken.expireAt &&
            (sessionId == otherToken.sessionId ||
             (sessionId != null && sessionId.equals(otherToken.sessionId)))) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = (int) expireAt;
        result += hashId();

        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("LoginToken: expires=");
        sb.append(expireAt);
        sb.append(", id=");
        sb.append(hashId());
        return sb.toString();
    }

    public int hashId() {
        return sessionId.hashId();
    }

    /*
     * Hex conversion methods are used to pass the login token via HTTP
     */

    /**
     * Convert a byte array to hex string.
     */
    public static String convertBytesToHex(byte[] byteArray) {

        final char[] hexValue = new char[byteArray.length * 2];

        final char[] hexSymbols = HEX.toCharArray();

        for (int i = 0; i < byteArray.length; i++) {
            final int current = byteArray[i] & 0xff;
            /* determine the Hex symbol for the last 4 bits */
            hexValue[i * 2 + 1] = hexSymbols[current & 0x0f];
            /* determine the Hex symbol for the first 4 bits */
            hexValue[i * 2] = hexSymbols[current >> 4];
        }
        return new String(hexValue);
    }

    /**
     * Convert a hex string to byte array.
     */
    public static byte[] convertHexToBytes(CharSequence hexString) {

        if (hexString.length() % 2 != 0) {
            throw new IllegalArgumentException("Invalid hex string length");
        }

        final byte[] result = new byte[hexString.length() / 2];

        final int n = hexString.length();

        for (int i = 0; i < n; i += 2) {
            /* high bits */
            final int hb = HEX.indexOf(hexString.charAt(i));
            /* low bits */
            final int lb = HEX.indexOf(hexString.charAt(i + 1));
            result[i / 2] = (byte) ((hb << 4) | lb);
        }
        return result;
    }
}
