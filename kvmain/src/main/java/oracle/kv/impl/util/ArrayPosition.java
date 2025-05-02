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

package oracle.kv.impl.util;

/*
 * Used to iterate over an array, when the iteration crosses method boundaries.
 */
public class ArrayPosition {

    private final int length;
    private int pos;

    public ArrayPosition(int len) {
       length = len;
    }

    public boolean hasNext() {
        return pos < length;
    }

    public int next() {
        return pos++;
    }

    public void prev() {
        assert(pos > 0);
        --pos;
    }
} 
