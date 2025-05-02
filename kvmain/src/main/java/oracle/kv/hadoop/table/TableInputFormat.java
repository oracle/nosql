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

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A Hadoop InputFormat class for reading data from an Oracle NoSQL Database.
 * Map/reduce keys and values are returned as PrimaryKey and Row objects
 * respectively.
 * <p>
 * For information on the parameters that may be passed to this class,
 * refer to the javadoc for the parent class of this class;
 * <code>TableInputFormatBase</code>.
 * <p>
 * A simple example demonstrating the Oracle NoSQL DB Hadoop
 * oracle.kv.hadoop.table.TableInputFormat class can be found in the
 * KVHOME/example/table/hadoop directory. It demonstrates how, in a MapReduce
 * job, to read records from an Oracle NoSQL Database that were written using
 * Table API.  The javadoc for that program describes the simple Map/Reduce
 * processing as well as how to invoke the program in Hadoop.
 * </p>
 * @since 3.1
 */
public class TableInputFormat extends TableInputFormatBase<PrimaryKey, Row> {

    /**
     * No-arg constructor.
     */
    public TableInputFormat() {
        this.topologyLocator =
            new TableInputFormatBase.TopologyLocatorWrapper();
    }

    /**
     * Used for testing only.
     * @hidden
     */
    public TableInputFormat(
        final TableInputFormatBase.TopologyLocatorWrapper topologyLocator) {
        this.topologyLocator = topologyLocator;
    }

    /**
     * Returns the RecordReader for the given InputSplit.
     */
    @Override
    public RecordReader<PrimaryKey, Row>
        createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

        final TableRecordReader ret = new TableRecordReader();
        ret.initialize(split, context);
        return ret;
    }

    /**
     * Special setter method that used by tests that wish to mock the
     * wrapper for the TopologyLocator employed by the getSplits method
     * of this class to connect to a store to retrieve the splits. This
     * method is used by the tests to tell the TableInputFormat to use
     * the given TopologyLocator mocked by the test rather than the
     * non-mocked TopologyLocator created by the no-arg constructor of
     * this class.
     */
    public void setTopologyLocator(
        final TableInputFormatBase.TopologyLocatorWrapper newTopologyLocator) {

        this.topologyLocator = newTopologyLocator;
    }
}
