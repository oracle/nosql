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
import oracle.kv.table.FieldDef;
import oracle.kv.table.RecordDef;

/**
 * <p>Represents a {@link Statement} that has been compiled and an execution
 * plan generated for it. A PreparedStatement should be used when it is
 * expected that the same statement will be executed multiple times. This way,
 * the cost of compilation is paid only once.</p>
 *
 * <p>An instance of PreparedStatement can be created via the
 * {@link KVStore#prepare(String statement)} method. This instance can then be
 * executed multiple times via one of the KVStore.execute methods (for example,
 * {@link KVStore#execute(String statement, ExecuteOptions options)} or
 * {@link KVStore#executeSync(Statement statement, ExecuteOptions options)}).</p>
 *
 * <p>If the statement is a DML statement that contains external variables,
 * these variables must be bound to specific values before the statement can be
 * executed. To allow for potentially concurrent execution of the same
 * PreparedStatement multiple times with different bind values each time,
 * binding of external variables must be done via one or more instances of
 * the {@link BoundStatement} interface. Such instances are created via the
 * #createBoundStatement() method. It is then the BoundStatement instances
 * that must be executed (i.e., passed as input to the KVStore execute
 * methods) instead of the PreparedStatement instance</p>
 *
 * <p>Objects implementing the PreparedStatement interface are thread-safe
 * and their methods are re-entrant. On the other hand, {@link BoundStatement}
 * instances are not thread-safe.</p>
 *
 * @since 4.0
 */
public interface PreparedStatement extends Statement {

    /**
     * Returns the definition of the result of this Statement.
     */
    RecordDef getResultDef();

    /**
     * Returns the types of the variables.
     */
    Map<String, FieldDef> getVariableTypes();

    /**
     * Returns the type of the given variableName or {@code null} if it
     * doesn't exist.
     */
    FieldDef getVariableType(String variableName);

    /**
     * Creates a new BoundStatement object for this PreparedStatement.
     */
    BoundStatement createBoundStatement();
}
