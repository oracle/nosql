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

package oracle.kv.impl.async;

import java.nio.ByteBuffer;

/**
 * A (slice of) byte buffer that can fork and be freed.
 *
 * A byte buffer slice can fork child slices. The child slices are backed by
 * the same bytes used by the parent slices, but with a separate byte buffer. A
 * parent slice is freed when itself is marked free and all the child slices
 * are freed.

 *         --------------
 *        |             |  -- child slice
 *        --------------
 *              /\
 *             | |
 * ----------------------------------------
 * |    |             |                   | -- parent slice
 * ----------------------------------------

*/
public interface IOBufSlice {

    /**
     * Returns the actual byte buffer of the slice.
     *
     * @return the byte buffer
     */
    ByteBuffer buf();

    /**
     * Forks a child buffer slice and advances the current position of this
     * slice.
     *
     * Forks a child slice beginning at the current position and of the
     * specified length, and then advances the current position of this slice
     * by that length.
     *
     * @param len the length of the forked child buffer slice
     * @param description the description of the context of forking
     * @return the child slice
     * @throws IllegalArgumentException if len is less than or equal to 0
     */
    IOBufSliceImpl forkAndAdvance(int len, String description);

    /**
     * Forks a child buffer slice before the current position.
     *
     * Forks a child slice ending at the current position with the specified
     * length. The current position of this slice is unchanged.
     *
     * @param len the length of the forked child buffer slice
     * @param description the description of the context of forking
     * @return the child slice
     * @throws IllegalArgumentException if len is less than or equal to 0 or
     * greater than the buffer's current position
     */
    IOBufSliceImpl forkBackwards(int len, String description);

    /**
     * Marks this buffer slice as free.
     *
     * The slice is freed if it is marked free and all its child slices are
     * freed. The procedure applies recursively to its parent.
     */
    void markFree();
}
