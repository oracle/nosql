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

package oracle.kv.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * @hidden
 */
public class KVRecordReader extends KVRecordReaderBase<Text, Text> {

    /**
     * Get the current key
     * @return the current key or null if there is no current key
     */
    @Override
    public Text getCurrentKey()
        throws IOException, InterruptedException {

        if (current == null) {
            return null;
        }

        /* Need a way to delimit the major and minor paths. */
        return new Text(current.getKey().toString());
    }

    /**
     * Get the current value.
     * @return the object that was read
     */
    @Override
    public Text getCurrentValue()
        throws IOException, InterruptedException {

        if (current == null) {
            return null;
        }

        return new Text(new String(current.getValue().getValue()));
    }
}
