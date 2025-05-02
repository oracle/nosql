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

import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * SessionId denotes the identity of a login session.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public final class SessionId implements Serializable, FastExternalizable {

    /*
     * Maximum allowable size of a session id
     */
    public static final int SESSION_ID_MAX_SIZE = 127;

    private static final long serialVersionUID = 1;

    /**
     * Bit mask for indication that the loginToken has an allocator encoded.
     */
    public static final int HAS_ALLOCATOR = 0x1;

    /*
     * The scope of the idValue
     */
    private IdScope idValueScope;

    /*
     * The scope-local session identifier value
     */
    private byte[] idValue;

    /* Allocating resource for non-persistent tokens */
    private ResourceId allocator;

    /**
     * The scope of the ID. The scope generally depends on the allocator, and
     * the extent to which the session manager can be successfully referenced
     * by other Components.
     *
     * WARNING: To avoid breaking serialization compatibility, the order of the
     * values must not be changed and new values must be added at the end.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public enum IdScope implements FastExternalizable {

        /*
         * PERSISTENT scope refers to sessions which are stored persistently in
         * the KVStore.
         */
        PERSISTENT(0),

        /*
         * LOCAL scope refers to sessions which are stored
         * transiently within a KVStore component and which cannot be correctly
         * interpreted by components in SNs other than the one that created the
         * session.
         */
        LOCAL(1),

        /*
         * STORE scope refers to sessions which are stored
         * transiently within a KVStore component and which CAN be interpreted
         * by components in SNs other than the one that created the session,
         * provided that topology information is available.
         */
        STORE(2);

        private static final IdScope[] VALUES = values();

        private IdScope(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        static IdScope readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            return valueOf(in.readByte());
        }

        static IdScope valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "invalid scope: " + ordinal);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #PERSISTENT}=0,
         *      {@link #LOCAL}=1, {@link #STORE}=2
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    /**
     * Creates a session id for a persistent session.
     * @param idValue The session identifier value
     */
    public SessionId(byte[] idValue) {
        if (idValue.length > SESSION_ID_MAX_SIZE) {
            throw new IllegalArgumentException(
                "sessionId length exceeds limit");
        }
        this.idValueScope = IdScope.PERSISTENT;
        this.idValue = Arrays.copyOf(idValue, idValue.length);
    }

    /**
     * Creates a session id for a non-persistent session.
     * @param idValue The session identifier calue
     * @param idValueScope Must be LOCAL or NON_LOCAL
     * @param allocator The component that allocated this id
     */
    public SessionId(byte[] idValue,
                     IdScope idValueScope,
                     ResourceId allocator) {

        if (idValueScope == IdScope.PERSISTENT) {
            throw new IllegalArgumentException("invalid scope");
        }
        if (idValue.length > SESSION_ID_MAX_SIZE) {
            throw new IllegalArgumentException(
                "sessionId length exceeds limit");
        }
        this.idValueScope = idValueScope;
        this.idValue = Arrays.copyOf(idValue, idValue.length);
        this.allocator = allocator;
    }

    /* for FastExternalizable */
    public SessionId(DataInput in, short serialVersion)
        throws IOException {

        final int flagByte = in.readByte();
        idValueScope = IdScope.readFastExternal(in, serialVersion);
        idValue = readNonNullByteArray(in);

        if ((flagByte & HAS_ALLOCATOR) != 0) {
            allocator = ResourceId.readFastExternal(in, serialVersion);
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@code byte}) <i>flagByte</i> // bit set with {@link
     *      #HAS_ALLOCATOR}=1
     * <li> ({@link IdScope}) {@link #getIdValueScope idValueScope}
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@link #getIdValue idValue}
     * <li> <i>[Optional]</i> ({@link ResourceId}) {@link #getAllocator
     *      allocator} // if present
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        int flagByte = 0;
        if (allocator != null) {
            flagByte |= HAS_ALLOCATOR;
        }
        out.writeByte(flagByte);
        idValueScope.writeFastExternal(out, serialVersion);
        writeNonNullByteArray(out, idValue);

        if (allocator != null) {
            allocator.writeFastExternal(out, serialVersion);
        }
    }

    /**
     * Return the session Id value for the token.
     */
    public byte[] getIdValue() {
        return idValue;
    }

    /**
     * Return the session scope for the token.
     */
    public IdScope getIdValueScope() {
        return idValueScope;
    }

    /**
     * Return the allocation scope. If the scope is PERSISTENT, this will
     * return null.
     */
    public ResourceId getAllocator() {
        return allocator;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other.getClass() != SessionId.class) {
            return false;
        }

        final SessionId otherToken = (SessionId) other;
        if (idValueScope == otherToken.idValueScope &&
            Arrays.equals(idValue, otherToken.idValue) &&
            ((allocator == null && otherToken.allocator == null) ||
             (allocator != null && allocator.equals(otherToken.allocator)))) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(idValue);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("SessionId: scope=");
        sb.append(idValueScope);
        sb.append(", hashId()=");
        sb.append(hashId());
        sb.append(", allocator=");
        sb.append(allocator);
        return sb.toString();
    }

    /**
     * Computes a securely hashed identifier for the session id. The hash
     * values for two distinct ids are not guaranteed to be unique.
     */
    public int hashId() {
        return hashId(idValue);
    }

    /**
     * Produce a compact identifier for the input idValue that is securely
     * generated.  This allows session id values to be represented in log
     * output without the risk of someone using the underying id.  The
     * generated output must not be relied upon as a truly unique
     * identifier for the idValue.
     */
    public static int hashId(byte[] idValue) {

        final String hashAlgo = "SHA-256";
        try {
            final MessageDigest md = MessageDigest.getInstance(hashAlgo);
            md.update(idValue, 0, idValue.length);
            final byte[] mdbytes = md.digest();

            /* Use the first 31 bits of the secure hash as the id value */
            return (mdbytes[0] & 0xff) |
                ((mdbytes[1] & 0xff) << 8) |
                ((mdbytes[2] & 0xff) << 16) |
                ((mdbytes[3] & 0x7f) << 24);
        } catch (NoSuchAlgorithmException nsae) {

            /*
             * This is unlikely to occur, since this is available in the
             * versions of the JDK that we support, but the output of this
             * is not critical, so just report it in a form that would allow
             * us to recognize that a problem is occurring.
             */
            return -1;
        }
    }
}
