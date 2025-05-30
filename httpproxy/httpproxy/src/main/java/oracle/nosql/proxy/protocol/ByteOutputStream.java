/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import oracle.nosql.nson.util.NettyByteOutputStream;

/**
 * An extension of the Netty ByteBufOutputStream that provides access to methods
 * to get and set offsets into the byte buffer that underlies the stream. This
 * class prevents knowledge of Netty from being required in the serialization
 * code.
 *
 * NOTE: this class implements DataOutput
 */
public class ByteOutputStream extends NettyByteOutputStream {

    public ByteOutputStream(ByteBuf buffer) {
        super(buffer);
    }

    public ByteOutputStream(ByteBuf buffer, boolean release) {
        super(buffer, release);
    }

    /**
     * Creates a ByteOutputStream, also allocating a ByteBuf. This
     * buffer must be released by calling releaseByteBuf.
     */
    public static ByteOutputStream createByteOutputStream() {
        return new ByteOutputStream(Unpooled.buffer(), true);
    }

    public void releaseByteBuf() {
        close();
    }

    /**
     * Returns the underlying ByteBuf
     */
    public ByteBuf getByteBuf() {
        return getBuffer();
    }

    /**
     * Skip numBytes, resetting the offset.
     */
    public void skipBytes(int numBytes) {
        skip(numBytes);
    }
}
