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

package oracle.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An exception thrown when a prepared query is executed and the query must
 * be re-prepared by the application before it can be executed.
 */
public class PrepareQueryException extends FastExternalizableException {

    private static final long serialVersionUID = 1L;

    public PrepareQueryException(String msg) {
        super(msg);
    }

    public PrepareQueryException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }
}
