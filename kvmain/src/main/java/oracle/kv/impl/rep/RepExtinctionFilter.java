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

package oracle.kv.impl.rep;

import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.EXTINCT;
import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.MAYBE_EXTINCT;
import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.NOT_EXTINCT;

import oracle.kv.Key;
import oracle.kv.impl.api.table.DroppedTableException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.rep.table.TableManager;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.ExtinctionFilter;

/**
 * Callback that is called by JE during cleaning for each LN (record) that is
 * not otherwise known to be obsolete. Returns EXTINCT if the record is extinct
 * because its table has been dropped or its partition has been migrated away.
 *
 * If metadata is not yet available, this method returns MAYBE_EXTINCT
 * because JE cannot purge LNs or perform integrity checks unless we are sure
 * the records is extinct.
 */
class RepExtinctionFilter implements ExtinctionFilter {

    private final TableManager tableManager;
    private final PartitionManager partitionManager;
    
    /* TODO: enable this if we use record extinction for index cleaning. */
    private static final boolean doPartitionLookup  = false;

    RepExtinctionFilter(final RepNode repNode) {
        this.tableManager = repNode.getTableManager();
        this.partitionManager = repNode.getPartitionManager();
    }

    @Override
    @SuppressWarnings("unused")
    public ExtinctionStatus getExtinctionStatus(final String dbName,
                                                final boolean dups,
                                                final byte[] key) {
    	/*
    	 * Changes to this function may have to be copied to
    	 * DbVerifyExtinctionFilter.getExtinctionStatus.
    	 */

        /*
         * Do not check DBs other than index DBs and partition DBs.
         * Index DBs are the only databases with duplicates configured.
         */
        if (!dups && !PartitionId.isPartitionName(dbName)) {
            /* Not an index DB and not a partition DB. */
            return NOT_EXTINCT;
        }

        /*
         * Now dups==true -> index record and dups==false -> primary record.
         *
         * Do partition lookup only for index records. It is wasteful to check
         * primary records because partition DBs are removed explicitly during
         * partition migration. We should remove the check for dups if we
         * use a single partitions/data DB in the future.
         */
        if (doPartitionLookup && dups && partitionManager.isInitialized()) {
            final PartitionId id = partitionManager.getPartitionId(key);
            if (!partitionManager.isPresent(id)) {
                /* Partition has been migrated away. */
                return EXTINCT;
            }
        }

        /*
         * Do table lookup only for primary records. It is wasteful to check
         * index records because index DBs are removed explicitly when an
         * index or table is dropped.
         */
        if (dups) {
            return NOT_EXTINCT;
        }

        /* Internal keyspace keys aren't in tables. */
        if (Key.keySpaceIsInternal(key)) {
            return NOT_EXTINCT;
        }

        try {
            tableManager.getTable(key);
            return NOT_EXTINCT;
        } catch (RNUnavailableException e) {
            /* RepNode metadata initialization is incomplete. */
            return MAYBE_EXTINCT;
        } catch (DroppedTableException e) {
            return EXTINCT;
        }
    }
}