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

import oracle.kv.ParallelScanIterator;

/**
 * Interface to the specialized Iterator type returned by the iterator methods
 * in the oracle.kv.table package.
 * <p>
 * Iterators implementing this interface can only be used safely by one thread
 * at a time unless synchronized externally.
 *
 * @since 3.0
 */
public interface TableIterator<K> extends ParallelScanIterator<K> {}
