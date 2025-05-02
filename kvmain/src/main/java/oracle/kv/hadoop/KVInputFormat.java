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

import oracle.kv.Key;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A Hadoop InputFormat class for reading data from an Oracle NoSQL Database.
 * Map/reduce keys and values are returned as Text objects.
 *
 * NoSQL Database Key arguments are passed in the canonical format returned by
 * {@link Key#toString Key.toString} format.
 *
 * <p>
 * Refer to the javadoc for {@link KVInputFormatBase} for information on the
 * parameters that may be passed to this class.
 * <p>
 * A simple example demonstrating the Oracle NoSQL DB Hadoop
 * oracle.kv.hadoop.KVInputFormat class can be found in the
 * KVHOME/example/hadoop directory. It demonstrates how to read records from
 * Oracle NoSQL Database in a Map/Reduce job.  The javadoc for that program
 * describes the simple Map/Reduce processing as well as how to invoke the
 * program in Hadoop.
 */
public class KVInputFormat extends KVInputFormatBase<Text, Text> {

    /**
     * @hidden
     */
    @Override
    public RecordReader<Text, Text>
        createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

        KVRecordReader ret = new KVRecordReader();
        ret.initialize(split, context);
        return ret;
    }
}
