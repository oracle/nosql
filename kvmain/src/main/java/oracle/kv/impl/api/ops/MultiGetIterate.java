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

import java.io.DataInput;
import java.io.IOException;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyRange;

/**
 * A multi-get-iterate operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class MultiGetIterate extends MultiKeyIterate {

    /**
     * Construct a multi-get-iterate operation.
     */
    public MultiGetIterate(byte[] parentKey,
                           KeyRange subRange,
                           Depth depth,
                           Direction direction,
                           int batchSize,
                           byte[] resumeKey,
                           boolean excludeTombstones) {
        super(OpCode.MULTI_GET_ITERATE, parentKey, subRange, depth, direction,
              batchSize, resumeKey, excludeTombstones);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiGetIterate(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_GET_ITERATE, in, serialVersion);
    }
}
