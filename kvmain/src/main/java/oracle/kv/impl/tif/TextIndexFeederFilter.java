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

package oracle.kv.impl.tif;

import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.OutputWireRecord;
import com.sleepycat.je.rep.subscription.SubscriptionConfig.DefaultFeederFilter;

/**
 * Feeder filter used by text index. It extends the base filter with check of
 * non-partition db
 */
public class TextIndexFeederFilter extends DefaultFeederFilter {

    private static final long serialVersionUID = 1L;

    /* cached dbs of non-partition db */
    protected final Set<DatabaseId> nonPartDBId;

    /* cached dbs of partition db */
    protected final Set<DatabaseId> partDBId;

    TextIndexFeederFilter() {
        super();
        nonPartDBId = new HashSet<>();
        partDBId = new HashSet<>();
    }

    @Override
    public OutputWireRecord execute(final OutputWireRecord record,
                                    final RepImpl repImpl) {
        if (super.execute(record, repImpl) == null) {
            /* already filtered out by based filter */
            return null;
        }

        /* additional check if the entry is from non-partition db */
        if (!record.getLNEntryInfo(lnInfo)) {
            /* Keep record if not an LN. All LNs have a DB ID. */
            return record;
        }
        final DatabaseId dbId = new DatabaseId(lnInfo.databaseId);

        /* see this id before */
        if (nonPartDBId.contains(dbId)) {
            return null;
        }
        if (partDBId.contains(dbId)) {
            return record;
        }

        final DbTree dbTree = repImpl.getDbTree();
        final DatabaseImpl impl = dbTree.getDb(dbId);
        try {
            /* keep record if db impl is not available */
            if (impl == null) {
                return record;
            }

            /* block all entries from non-partition db */
            final String dbName = impl.getName();
            if (!PartitionId.isPartitionName(dbName)) {
                nonPartDBId.add(dbId);
                return null;
            }
            /* remember it */
            partDBId.add(dbId);
            return record;
        } finally {
            if (impl != null) {
                dbTree.releaseDb(impl);
            }
        }
    }
}
