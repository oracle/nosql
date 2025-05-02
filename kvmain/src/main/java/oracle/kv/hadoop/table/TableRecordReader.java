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

package oracle.kv.hadoop.table;

import java.io.IOException;

import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

/**
 * Concrete implementation of TableRecordReaderBase that reads table rows
 * from an InputSplit using a PrimaryKey.
 *
 * @since 3.1
 */
public class TableRecordReader extends TableRecordReaderBase<PrimaryKey, Row> {

    /**
     * Get the current key.
     * @return the current key or null if there is no current key
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    @Override
    public PrimaryKey getCurrentKey()
        throws IOException, InterruptedException {

        if (current == null) {
            return null;
        }
        return current.createPrimaryKey();
    }

    /**
     * Get the current value.
     * @return the object that was read
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    @Override
    public Row getCurrentValue()
        throws IOException, InterruptedException {

        if (current == null) {
            return null;
        }
        return current;
    }
}
