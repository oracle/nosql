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

package oracle.kv.query;

import java.util.Map;

import oracle.kv.KVStore;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;

/**
 * <p> Represents a {@link Statement} that has been compiled and an execution
 * plan generated for it. A BoundStatement must be used when the Statement is
 * a DML statement that contains external variables. It provides methods to
 * bind the external variables to specific values.</p>
 *
 * <p>To create an instance of a BoundStatement, an instance of a
 * {@link PreparedStatement} must be created first, which is done via the
 * {@link KVStore#prepare(String statement)} method. Next, one or more
 * BoundStatement instances can be created from the same PreparedStatement via
 * the {@link PreparedStatement#createBoundStatement() method}. Creating
 * multiple BoundStatements from the same PreparedStatement allows for the
 * potentially concurrent execution of the PreparedStatement multiple times
 * with different bind values each time.</p>
 *
 * <p>Like a PreparedStatement, a BoundStatement can be executed multiple
 * times via one of the KVStore.execute methods (for example, {@link
 * KVStore#execute(String statement, ExecuteOptions options)} or
 * {@link KVStore#executeSync(Statement statement, ExecuteOptions options) }).
 * All the external variables must be bound before execution is initiated.</p>
 *
 * <p>Objects implementing the PreparedStatement interface are thread-safe
 * and their methods are re-entrant. On the other hand, BoundStatement
 * instances are not thread-safe.</p>
 *
 * <p>{@link FieldValueFactory} to create values for complex types.</p>
 *
 * @since 4.0
 */
public interface BoundStatement
        extends PreparedStatement {

    /**
     * Returns the map of bound variables.
     * @return a map of variables that have been set.
     */
    Map<String, FieldValue> getVariables();

    /**
     * Sets the value of a variable.
     * @param variableName the name of the query variable
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(String variableName, FieldValue value);

    /**
     * @hidden
     * Sets the value of a variable. The variable is identified by its position
     * within the query string: the variable that appears first in the query
     * text has position 1, the variable that appears second has position 2
     * and so on.
     * @param pos the position of the variable. 
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(int pos, FieldValue value);

    /**
     * Sets the value of a variable.
     * @param variableName the name of the query variable
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(String variableName, int value);

    /**
     * @hidden
     * Sets the value of a variable. The variable is identified by its position
     * within the query string: the variable that appears first in the query
     * text has position 1, the variable that appears second has position 2
     * and so on.
     * @param pos the position of the variable. 
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(int pos, int value);

    /**
     * Sets the value of a variable.
     * @param variableName the name of the query variable
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(String variableName, boolean value);

    /**
     * @hidden
     * Sets the value of a variable. The variable is identified by its position
     * within the query string: the variable that appears first in the query
     * text has position 1, the variable that appears second has position 2
     * and so on.
     * @param pos the position of the variable. 
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(int pos, boolean value);

    /**
     * Sets the value of a variable.
     * @param variableName the name of the query variable
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(String variableName, double value);

    /**
     * @hidden
     * Sets the value of a variable. The variable is identified by its position
     * within the query string: the variable that appears first in the query
     * text has position 1, the variable that appears second has position 2
     * and so on.
     * @param pos the position of the variable. 
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(int pos, double value);

    /**
     * Sets the value of a variable.
     * @param variableName the name of the query variable
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(String variableName, float value);

    /**
     * @hidden
     * Sets the value of a variable. The variable is identified by its position
     * within the query string: the variable that appears first in the query
     * text has position 1, the variable that appears second has position 2
     * and so on.
     * @param pos the position of the variable. 
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(int pos, float value);

    /**
     * Sets the value of a variable.
     * @param variableName the name of the query variable
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(String variableName, long value);

    /**
     * @hidden
     * Sets the value of a variable. The variable is identified by its position
     * within the query string: the variable that appears first in the query
     * text has position 1, the variable that appears second has position 2
     * and so on.
     * @param pos the position of the variable. 
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(int pos, long value);

    /**
     * Sets the value of a variable.
     * @param variableName the name of the query variable
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(String variableName, String value);

    /**
     * @hidden
     * Sets the value of a variable. The variable is identified by its position
     * within the query string: the variable that appears first in the query
     * text has position 1, the variable that appears second has position 2
     * and so on.
     * @param pos the position of the variable. 
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(int pos, String value);

    /**
     * Sets the value of a variable.
     * @param variableName the name of the query variable
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(String variableName, byte[] value);

    /**
     * @hidden
     * Sets the value of a variable. The variable is identified by its position
     * within the query string: the variable that appears first in the query
     * text has position 1, the variable that appears second has position 2
     * and so on.
     * @param pos the position of the variable. 
     * @param value the value to be set
     * @return the same object
     * @throws IllegalArgumentException if the variable does not appear in
     * the query or the type of value doesn't match the type of the variable.
     */
    BoundStatement setVariable(int pos, byte[] value);
}
