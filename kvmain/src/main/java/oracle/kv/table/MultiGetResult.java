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

package oracle.kv.table;

import java.util.List;

/**
 * For http client API
 * @hidden
 *
 * The class encapsulates the result of partitionbased table scan
 * and shardbased index scan operations.
 */
public class MultiGetResult<T> {
    /* The returned results */
    private final List<T> result;

    /* The continuation key where the next multiGet resume from */
    private final byte[] continuationKey;

    /* The readKB of this operation */
    private final int readKB;

    /* The writeKB of this operation */
    private final int writeKB;

    public MultiGetResult(List<T> result,
                          byte[] continuationKey,
                          int readKB,
                          int writeKB) {
        this.result = result;
        this.continuationKey = continuationKey;
        this.readKB = readKB;
        this.writeKB = writeKB;
    }

    public List<T> getResult() {
        return result;
    }

    public byte[] getContinuationKey() {
        return continuationKey;
    }

    public int getReadKB() {
        return readKB;
    }

    public int getWriteKB() {
        return writeKB;
    }
}
