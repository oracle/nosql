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
package oracle.kv.impl.client.admin;


/**
 * ExecutionInfo describes the information about ddl command execution passed
 * between the Admin service and a client.
 */
public interface ExecutionInfo {

    /**
     * @return a version number describing the format of this planInfo.
     */
    public int getVersion();

    /**
     * @return the id of the target plan, if a plan was used to execute the
     * operation. May be 0 if no plan was required.
     */
    public int getPlanId();

    /**
     * @return true if the operation is no longer running. Encompasses both
     * success and failure cases.
     */
    public boolean isTerminated();

    /**
     * @return a JSON formatted detailed history of plan execution to date.
     * It's a plan history, returned in a structured format so that the client
     * can take actions based on particular plan status, or some aspect of
     * the plan execution. The information is returned as JSON rather than as
     * a Java class to reduce the demands on having Java on the client side.
     */
    public String getJSONInfo();

    /**
     * @return a text version of the plan execution status, formatted for
     * human readability.
     */
    public String getInfo();

    /**
     * @return true if the command succeeded.
     */
    public boolean isSuccess();

    /**
     * If the operation was not successful, there should be a non-null error
     * message.
     */
    public String getErrorMessage();

    /**
     * @return true if the command was cancelled.
     */
    public boolean isCancelled();

    /**
     * Return true if the operation is in such a state that it needs to be
     * explicitly cancelled.
     */
    public boolean needsTermination();

    /**
     * Return results for statements that are essentially read operations,
     * such as show tables and describe table, and read operation DML
     */
    public String getResult();
}
