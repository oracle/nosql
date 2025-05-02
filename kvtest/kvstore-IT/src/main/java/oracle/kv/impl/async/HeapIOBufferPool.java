/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import java.nio.ByteBuffer;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A mocked pool that allocates buffers directly from heap instead of actually
 * pooling.
 */
public class HeapIOBufferPool extends IOBufferPool {

    public HeapIOBufferPool(int bufsize) {
        super("HeapIOBufferPool", bufsize);
    }

    @Override
    public @Nullable ByteBuffer allocPooled() {
        return allocDiscarded();
    }

    @Override
    public void deallocate(ByteBuffer buffer) {
        /* do nothing */
    }
}

