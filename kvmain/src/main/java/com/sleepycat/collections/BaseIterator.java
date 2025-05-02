/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.collections;

import java.util.ListIterator;

/**
 * Common interface for BlockIterator and StoredIterator.  This is an abstract
 * class rather than in interface to prevent exposing these methods in javadoc.
 */
abstract class BaseIterator<E> implements ListIterator<E> {

    abstract ListIterator<E> dup();

    abstract boolean isCurrentData(Object currentData);

    abstract boolean moveToIndex(int index);
}
