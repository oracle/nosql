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

/**
 * An extension of the Netty ByteBufInputStream that provides access to methods
 * to get and set offsets into the byte buffer that underlies the stream. This
 * class prevents knowledge of Netty from being required in the serialization
 * code.
 *
 * NOTE: this class implements DataInput
 */

import io.netty.buffer.ByteBuf;
import oracle.nosql.nson.util.NettyByteInputStream;

public class ByteInputStream extends NettyByteInputStream {

    public ByteInputStream(ByteBuf buffer) {
        super(buffer);
    }
}
