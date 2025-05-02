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
import java.io.PrintStream;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TableIterator;

/**
 * A StatementResult provides information about the execution and outcome of a
 * table statement. If obtained via {@link ExecutionFuture#updateStatus}, it can
 * represent information about either a completed or in progress operation. If
 * obtained via {@link ExecutionFuture#get} or {@link KVStore#executeSync}, it
 * represents the final status of a finished operation.
 *
 * @since 3.2
 */
public interface StatementResult extends Iterable<RecordValue> {

    /**
     * Shows the kind of StatementResult.
     * @see #getKind()
     * @hidden.see {@link #writeFastExternal FastExternalizable format}
     * @since 4.0
     */
    enum Kind implements FastExternalizable {
        /**
         * Results of data definition language statements: create or remove
         * table, a create or remove index or an alter index, or other
         * statements that don't return data records. In this case the
         * iterator().hasNext() will always return false.
         */
        DDL(0),
        /**
         * Query statements that return records, for example SELECT FROM ....
         */
        QUERY(1);

        private static final Kind[] VALUES = values();

        private Kind(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        /**
         * Reads an enum constant from the stream.
         *
         * @hidden For internal use only
         */
        public static Kind readFastExternal(DataInput in,
                                            @SuppressWarnings("unused")
                                            short serialVersion)
            throws IOException {

            final int ordinal = in.readUnsignedByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "unknown StatementResult.Kind: " + ordinal);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #DDL}=0, {@link
         *      #QUERY}=1
         * </ol>
         *
         * @hidden For internal use only
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    /**
     * Returns the administrative plan id for this operation if the statement
     * was a DDL statement: create or remove table, a create or remove index,
     * or an alter index. When using the Admin CLI (runadmin) utility,
     * administrative operations are identified by plan id. The plan id can be
     * used to correlate data definition and administrative statements issued
     * programmatically using the API against operations viewed via the
     * interactive Admin CLI or other monitoring tool.
     * <p>
     * The plan id will be 0 if this statement was not an administrative
     * operation, or did not require execution.
     */
    int getPlanId();

    /**
     * Returns information about the execution of the statement, in human
     * readable form. If the statement was a data definition command, the
     * information will show the start and end time of the operation and
     * details about server side processing. For data manipulation commands
     * it will return null.
     */
    String getInfo();

    /**
     * Returns the same information as {@link #getInfo}, in JSON format.
     * Several possible formats are returned, depending on the statement. The
     * format of a data definition command which requires server side
     * processing is as follows:
     * <pre>
     * {
     *   "version" : "2",
     *   "planInfo" : {
     *     "id" : 6,
     *     "name" : "CreateIndex:users:LastName",
     *     "isDone" : true,
     *     "state" : "SUCCEEDED",
     *     "start" : "2014-10-29 18:41:12 UTC",
     *     "interrupted" : null,
     *     "end" : "2014-10-29 18:41:12 UTC",
     *     "error" : null,
     *     "executionDetails" : {
     *       "taskCounts" : {
     *         "total" : 3,
     *         "successful" : 3,
     *         "failed" : 0,
     *         "interrupted" : 0,
     *         "incomplete" : 0,
     *         "notStarted" : 0
     *       },
     *       "finished" : [ {
     *         "taskNum" : 1,
     *         "name" : "StartAddIndex:users:LastName",
     *         "state" : "SUCCEEDED",
     *         "start" : "2014-10-29 18:41:12 UTC",
     *         "end" : "2014-10-29 18:41:12 UTC"
     *       }, {
     *         "taskNum" : 2,
     *         "name" : "WaitForAddIndex:users:LastName",
     *         "state" : "SUCCEEDED",
     *         "start" : "2014-10-29 18:41:12 UTC",
     *         "end" : "2014-10-29 18:41:12 UTC"
     *       }, {
     *         "taskNum" : 3,
     *         "name" : "CompleteAddIndex:users:LastName",
     *         "state" : "SUCCEEDED",
     *         "start" : "2014-10-29 18:41:12 UTC",
     *         "end" : "2014-10-29 18:41:12 UTC"
     *       } ],
     *       "running" : [ ],
     *       "pending" : [ ]
     *     }
     *   }
     * }
     * </pre>
     *
     * For data manipulation commands it will return null.
     */
    String getInfoAsJson();

    /**
     * If {@link #isSuccessful} is false, return a description of the
     * problem. Will be null if {@link #isSuccessful} is true.
     */
    String getErrorMessage();

    /**
     * Returns true if this statement has finished and was successful.
     */
    boolean isSuccessful();

    /**
     * Returns true if the statement completed. For DDL this is the
     * equivalent of {@link ExecutionFuture#isDone}. For QUERY statements this
     * indicates if the iterator is depleted.
     */
    boolean isDone();

    /**
     * Returns true if the statement had been cancelled. This is the equivalent
     * of {@link ExecutionFuture#isCancelled}
     *
     * @see ExecutionFuture#cancel
     */
    boolean isCancelled();

    /**
     * Returns the output of a DDL statement that generates results, such as
     * SHOW TABLES, SHOW AS JSON TABLES, DESCRIBE TABLE, or DESCRIBE AS JSON
     * TABLE. The output of a DESCRIBE AS JSON TABLES command is:
     * <pre>
     * {
     *     "type" : "table",
     *     "name" : "users",
     *     "comment" : null,
     *     "shardKey" : [ "id" ],
     *     "primaryKey" : [ "id" ],
     *     "fields" : [ {
     *       "name" : "id",
     *       "type" : "INTEGER",
     *       "nullable" : true,
     *       "default" : null
     *     }, {
     *       "name" : "firstName",
     *       "type" : "STRING",
     *       "nullable" : true,
     *       "default" : null
     *     }, {
     *       "name" : "lastName",
     *       "type" : "STRING",
     *       "nullable" : true,
     *       "default" : null
     *     }, {
     *       "name" : "age",
     *       "type" : "INTEGER",
     *       "nullable" : true,
     *       "default" : null
     *     } ],
     *     "indexes" : [ {
     *       "name" : "LastName",
     *       "comment" : null,
     *       "fields" : [ "lastName" ]
     *     } ]
     *   }
     * }
     * </pre>
     * The output of a SHOW AS JSON TABLES command is:
     * <pre>
     * {"tables" : ["users"]}
     * </pre>
     * <pre>Returns null in the case of a DML statement.</pre>
     * @since 3.3
     */
    String getResult();

    /**
     * Returns the Kind of StatementResult.
     *
     * @since 4.0
     */
    Kind getKind();


    /**
     * <p>Returns a TableIterator over the records in this result. If the
     * statement is DDL, an iterator with an empty result will be
     * returned.</p>
     *
     * <p>{@link StatementResult#close()} will close this iterator, any
     * subsequent calls to hasNext() will return false and any calls to next()
     * will throw a java.util.IllegalStateException.</p>
     *
     * <p>Note: Multiple calls to this method will return the same iterator
     * object.</p>
     *
     * @throws IllegalStateException if the result is closed.
     *
     * @since 4.0
     */
    @Override
    TableIterator<RecordValue> iterator() throws IllegalStateException;

    /**
     * Closes the result including the iterator and releases all the resources
     * related to this result. This method is idempotent.
     *
     * For query statements any subsequent calls to {@link #getResultDef()}
     * will trigger an IllegalStateException.
     *
     * Applications should discard all references to this object after it has
     * been closed.
     *
     * When a <code>StatementResult</code> is closed, any metadata
     * instances that were created by calling the  {@link #getResultDef()}
     * method remain accessible.
     *
     * @since 4.0
     */
    void close();

    /**
     * Returns the definition of the result of this statement if the
     * statement is a query, otherwise null.
     *
     * @throws IllegalStateException if the result is closed.
     * @throws FaultException if the operation cannot be completed for any
     * reason
     * @since 4.0
     */
    RecordDef getResultDef() throws IllegalStateException, FaultException;

    @SuppressWarnings("unused")
    default void printTrace(PrintStream out) {
    }
}
