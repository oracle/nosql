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

package oracle.kv.impl.query.runtime;

/**
 * An interface to construct iterators that must exist at the server only (because
 * they make use of server-side classes). 
 *
 * For example, consider BaseTableIter. The compiler creates an instance T of
 * BaseTableIter, which is a class visible in both client and server code. During
 * the open() method on T, TableIterFactory.createTableIter() is called to create
 * an instance TC of ServerTableIter. A ref to TC is stored in T, and the next(),
 * reset(), and close() methods on T are propagated to TC.
 *
 * ServerIterFactoryImpl (in the query/runtime/server dir) implements
 * ServerIterFactory. An instance of ServerIterFactoryImpl is created during
 * TableQueryHandler.execute() and is stored in the RCB.
 */
public interface ServerIterFactory {

    public PlanIter createTableIter(
        RuntimeControlBlock rcb,
        BaseTableIter parent);

    public PlanIter createUpdateRowIter(UpdateRowIter iter);

    public PlanIter createDeleteRowIter(
        RuntimeControlBlock rcb,
        DeleteRowIter iter);

    public PlanIter createIndexSizeIter(
        FuncIndexStorageSizeIter parent);
}
