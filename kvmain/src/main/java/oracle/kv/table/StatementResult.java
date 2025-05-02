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

/**
 * A StatementResult provides information about the execution and outcome of a
 * table statement. If obtained via {@link ExecutionFuture#updateStatus}, it can
 * represent information about either a completed or in progress operation. If
 * obtained via {@link ExecutionFuture#get} or {@link TableAPI#executeSync}, it
 * represents the final status of a finished operation.
 * <p>
 * <b>JSON format for results</b>:
 * Additional information about the statement execution is available via
 * {@link StatementResult#getInfo} or {@link StatementResult#getInfoAsJson}. The
 * former is formatted for human readability, whereas the latter provides
 * a JSON version of the same information. The JSON info may be one of four
 * types: show, describe, noop, plan
 * <p>
 * A plan result provides the output of a data definition statement which was
 * executed on the server. This output can be correlated to the information
 * visible from the Admin CLI and other monitoring tools.
 * <pre>
 * {
 *   "type" : "plan",
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
 * A show result provides the output of a SHOW statement:
 * <pre>
 * {
 *   "type" : "show",
 *   "result" : {"tableHierarchy" : ["users"]}
 * }
 * </pre>
 * A describe result provides the output of a DESCRIBE statement:
 * <pre>
 * {
 *   "type" : "describe",
 *   "result" : {
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
 * A noop result is returned when the statement did not result in any
 * processing.
 * <pre>
 * {
 *     "type" : "noop",
 *     "comment" : "Statement did not require execution"
 * }
 * </pre>
 *
 * @since 3.2
 * @deprecated since 3.3 in favor of {@link oracle.kv.StatementResult}
 */
@Deprecated
public interface StatementResult {

    /**
     * Returns the administrative plan id for this operation if the statement
     * was a create or remove table, a create or remove index, or an alter
     * index. When using the Admin CLI (runadmin) utility, administrative
     * operations are identified by plan id. The plan id can be used to
     * correlate data definition and administrative statements issued
     * programmatically using the API against operations viewed via the
     * interactive Admin CLI or other monitoring tool.
     * <p>
     * The plan id will be 0 if this statement was not an administrative
     * operation, or did not require execution.
     */
    public int getPlanId();

    /**
     * Returns information about the execution of the statement, in human
     * readable form. If the statement was a data definition command, the
     * information will show the start and end time of the operation and
     * details about server side processing. If the statement was a DESCRIBE
     * or SHOW command, the information will describe the table metadata.
     */
    public String getInfo();

    /**
     * Get detailed information about the execution of the statement in JSON
     * text. See the header comments for details about the JSON format.
     */
    public String getInfoAsJson();

    /**
     * If {@link #isSuccessful} is false, return a description of the
     * problem. Will be null if {@link #isSuccessful} is true.
     */
    public String getErrorMessage();

    /**
     * Return true if this statement has finished and was successful.
     */
    boolean isSuccessful();

    /**
     * Return true if the statement completed. This is the equivalent of
     * {@link ExecutionFuture#isDone}
     */
    boolean isDone();

    /**
     * Return true if the statement had been cancelled. This is the equivalent
     * of {@link ExecutionFuture#isCancelled}
     *
     * @see ExecutionFuture#cancel
     */
    boolean isCancelled();
}
