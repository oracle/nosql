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

import oracle.kv.KeyRange;

/**
 * QueryKeyRange overrides the isPrefix() method of KeyRange to return false
 * always. This allows us to specify a range that contains only one key, i.e.
 * KeyRange.start == KeyRange.end and KeyRange.startInclusive == true and
 * KeyRange.endInclusive == true. KeyRange treats this case a prefix spec,
 * which causes wrong results to be returned by some queries (see QTF test
 * case multi_index/q/keyonly01.q) 
 */
public class QueryKeyRange extends KeyRange {

    public QueryKeyRange(
        String start,
        boolean startInclusive,
        String end,
        boolean endInclusive) {

        super(start, startInclusive, end, endInclusive);
    }

    @Override
    public boolean isPrefix() {
        return false;
    }
}
