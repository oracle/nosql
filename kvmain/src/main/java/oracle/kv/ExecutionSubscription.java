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

import oracle.kv.query.Statement;
import oracle.kv.table.RecordDef;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A subinterface of {@link Subscription} implemented by subscriptions supplied
 * when a {@link Subscriber} subscribes to a {@link Publisher} associated with
 * the asynchronous execution of a query.
 *
 * @see KVStore#executeAsync
 * @since 19.5
 */
public interface ExecutionSubscription extends IterationSubscription {

    /**
     * Returns the statement object for the associated statement execution.
     *
     * @return the statement
     */
    Statement getStatement();

    /**
     * Returns the definition of the result of this statement if the
     * statement is a query, otherwise null.
     * @since 22.2
     *
     * @return the definition of the result or {@code null}
     */
    RecordDef getResultDef();
}
