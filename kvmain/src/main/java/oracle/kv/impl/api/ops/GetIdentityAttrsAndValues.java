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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.impl.util.SerializationUtil;

/**
 * A GetIdentityAttrsAndValues operation gets the attributes and new values of
 * an identity column if needed.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class GetIdentityAttrsAndValues extends SingleKeyOperation {

    final private long curVersion;
    final private int clientIdentityCacheSize;
    final private boolean needAttributes;
    final private boolean needNextSequence;
    final private String sgName;

    /**
     * Constructs a GetIdentityAttrsAndValues operation with a table id.
     */
    public GetIdentityAttrsAndValues(byte[] keyBytes,
                                     long curVersion,
                                     int clientIdentityCacheSize,
                                     boolean needAttributes,
                                     boolean needNextSequence,
                                     String sgName
                                     ) {
        super(OpCode.GET_IDENTITY, keyBytes);
        this.curVersion = curVersion;
        this.clientIdentityCacheSize = clientIdentityCacheSize;
        this.needAttributes = needAttributes;
        this.needNextSequence = needNextSequence;
        this.sgName = sgName;
    }

    GetIdentityAttrsAndValues(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.GET_IDENTITY, in, serialVersion);
        this.curVersion = in.readLong();
        this.clientIdentityCacheSize = in.readInt();
        this.needAttributes = in.readBoolean();
        this.needNextSequence = in.readBoolean();
        this.sgName = readString(in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SingleKeyOperation}) {@code super}
     * <li> ({@link DataOutput#writeLong long}) {@link #getCurVersion
     *      curVersion}
     * <li> ({@link DataOutput#writeInt int}) {@link #getClientCacheSize
     *      clientIdentityCache}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getNeedAttributes
     *      needAttributes}
     * <li> ({@link DataOutput#writeBoolean boolean})
     *      {@link #getNeedNextSequence needNextSequence}
     * <li> ({@link DataOutput#writeLong long}) {@link #getTableId
     *      sysTableId}
     * <li> ({@link SerializationUtil#writeString String}) {@link #getSgName
     *      sgName}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);

        out.writeLong(curVersion);
        out.writeInt(clientIdentityCacheSize);
        out.writeBoolean(needAttributes);
        out.writeBoolean(needNextSequence);
        writeString(out, serialVersion, sgName);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof GetIdentityAttrsAndValues)) {
            return false;
        }
        final GetIdentityAttrsAndValues other =
            (GetIdentityAttrsAndValues) obj;
        return (curVersion == other.curVersion) &&
            (clientIdentityCacheSize == other.clientIdentityCacheSize) &&
            (needAttributes == other.needAttributes) &&
            (needNextSequence == other.needNextSequence) &&
            Objects.equals(sgName, other.sgName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), curVersion,
                            clientIdentityCacheSize, needAttributes,
                            needNextSequence, sgName);
    }

    public long getCurVersion() {
        return curVersion;
    }

    public int getClientCacheSize() {
        return clientIdentityCacheSize;
    }

    public boolean getNeedAttributes() {
        return needAttributes;
    }

    public boolean getNeedNextSequence() {
        return needNextSequence;
    }

    public String getSgName() {
        return sgName;
    }
}
