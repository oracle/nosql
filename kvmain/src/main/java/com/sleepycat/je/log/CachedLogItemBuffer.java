/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.util.Queue;

import com.sleepycat.je.utilint.Resource;

class CachedLogItemBuffer extends Resource<CachedLogItemBuffer> {

    private final Queue<CachedLogItemBuffer> pool;
    private final ByteBuffer buffer;

    CachedLogItemBuffer(final Queue<CachedLogItemBuffer> pool,
                        final int size) {
        this.pool = pool;
        buffer = ByteBuffer.allocate(size);
    }

    ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    protected void noteDeallocation() {
        pool.add(this);
    }

    @Override
    protected void addResourceInfo(StringBuilder sb) {
        sb.append(" bufSize=").append(buffer.remaining());
    }
}
