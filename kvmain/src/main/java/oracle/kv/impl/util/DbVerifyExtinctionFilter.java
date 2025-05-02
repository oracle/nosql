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

import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.EXTINCT;
import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.MAYBE_EXTINCT;
import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.NOT_EXTINCT;

import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.impl.api.table.DroppedTableException;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.table.TableManager.IDBytesComparator;
import oracle.kv.impl.rep.table.TableMetadataPersistence;
import oracle.kv.impl.rep.table.TableMetadataPersistence.TableKeyGenerator;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.table.Table;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.ExtinctionFilter;
import com.sleepycat.je.LockMode;

/**
 * This class is used to tell if a table is extinct or not when accessing a
 * KV store while it is offline.
 * 
 * This class is in response to a bug were DbVerify, when run separate from
 * KV such as when doing debugging or when verifying a backup, could falsely
 * return a table as corrupted.  When a table is dropped the records are not
 * marked as deleted, but rather RepExtinctionFilter is called to see if the
 * record is in a dropped table.  However, if DbVerify is run independent of
 * KV it does not have access to the information RepExtinctionFilter needs to
 * determine if a table has been dropped.  To fix that, this class reads the
 * table metadata from the metadata database when initialized, and uses that
 * information to determine if a table is extinct or not.  The code to read
 * and parse the table metadata is mostly copied from TableManager and
 * MetadataManager, and the code to decide if a table is extinct or not is
 * taken from RepExtinctionFilter.  Changes to the code in those classes may
 * have to be copied to this class.
 * 
 * Note this class should only be used when the KV store is offline, as it
 * lacks any concurrency control and skips several checks that only apply to
 * active systems.
 * 
 * See RepExtinctionFilter, TableManager, and MetadataManager.
 *
 */
public class DbVerifyExtinctionFilter
	implements ExtinctionFilter, TableKeyGenerator {
	/*
     * Maximum of all table IDs seen so far, including child tables. A value
     * of 0 indicates that table metadata has not yet be updated.
     */
    private volatile long maxTableId = 0;

    private volatile Map<byte[], TableImpl> idBytesLookupMap = null;

    private final Logger logger;

    /**
     * The constructor initializes the idBytesLookupMap by opening the metadata
     * database and reading the table information from each entry.  This code
     * is based on how TableManager.updateTableMaps opens the metadata database
     * and sets up idBytesLookupMap.  Since this is only used on KV stores that
     * are offline this does not require the locks or synchronization that is
     * in the TableManager code.
     */
    public DbVerifyExtinctionFilter(Environment env) {
    	final String metadataPrefix = MetadataType.TABLE.getKey();
    	TableMetadata tableMd = null;
		Database metaDb = null;
		final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(true);
        logger = DbInternal.getNonNullEnvImpl(env).getLogger();

        try {
        	// Open the metadata database.
            metaDb =
            	env.openDatabase(null, metadataPrefix + "Metadata", dbConfig);
            // Read in the TableMetadata from the metadata database.
        	DatabaseEntry value = new DatabaseEntry();
        	DatabaseEntry key = new DatabaseEntry();
        	StringBinding.stringToEntry(metadataPrefix, key);
            metaDb.get(null, key, value, LockMode.DEFAULT);
            tableMd = (TableMetadata)SerializationUtil.
            	getObject(value.getData(), Metadata.class);

	        if (tableMd != null) {
	        	if (tableMd.isShallow()) {
	        		TableMetadataPersistence.readTables(
	        			tableMd, metaDb, null, this, logger);
	            }

	        	// Map the TableMetadata in to idBytesLookupMap.
	        	maxTableId = tableMd.getMaxTableId();
	            idBytesLookupMap = new TreeMap<>(new IDBytesComparator());
	            for (Table table : tableMd.getTables().values()) {
	                final TableImpl tableImpl = (TableImpl)table;
	                idBytesLookupMap.put(tableImpl.getIDBytes(), tableImpl);
	            }
	        }

        } finally {
            try {
            	if (metaDb != null) {
            		metaDb.close();
            	}
            } catch (DatabaseException de) {}
        }
    }
    
    /**
     * Gets the table instance for a given record key as a byte array.
     *
     * @param key the record key as a byte array.
     * @return the table instance or null if the key is not a table key.
     * @throws RNUnavailableException is the table metadata is not yet
     * initialized
     * @throws DroppedTableException if the key is not for an existing table,
     * and the key does not appear to be a non-table (KV API) key.
     *
     * See IDBytesComparator.
     */
    private TableImpl getTable(byte[] key) {
        final Map<byte[], TableImpl> map = idBytesLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                "Table metadata is not yet initialized");
        }
        TableImpl table = map.get(key);
        final int nextOff = Key.findNextComponent(key, 0);
        if (table == null) {
            /* Check for a dropped top-level table. */
            TableImpl.checkForDroppedTable(key, 0, nextOff, maxTableId);
            return null;
        }
        table = table.findTargetTable(key, nextOff + 1, maxTableId);
        if (table == null) {
            return null;
        }

        /* A "deleting" table be considered as dropped */
        if (table.isDeleting()) {
            throw new DroppedTableException();
        }
        return table;
    }

    @Override
    public ExtinctionStatus getExtinctionStatus(String dbName, boolean dups,
                    byte[] key) {

        /*
         * Do not check DBs other than index DBs and partition DBs.
         * Index DBs are the only databases with duplicates configured.
         */
        if (!dups && !PartitionId.isPartitionName(dbName)) {
            /* Not an index DB and not a partition DB. */
            return NOT_EXTINCT;
        }

        /*
         * No need to do partition lookup, that check only applies to active KV
         * stores.
         */

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
            getTable(key);
            return NOT_EXTINCT;
        } catch (RNUnavailableException e) {
            /* RepNode metadata initialization is incomplete. */
            return MAYBE_EXTINCT;
        } catch (DroppedTableException e) {
            return EXTINCT;
        }
    }

    /* -- From TableKeyGenerator -- */

    /**
     * The table ID is used for the key to store the table records. The IDs
     * are short and unique. The RN does not need to access these records
     * by table name (or ID for that matter).
     *
     * The table ID does not conflict with the metadata key which is the
     * string returned from MetadataType.getType().
     */

    /* Table IDs start at 1 */
    private final static long START_TABLE_KEY = 1L;

    @Override
    public void setStartKey(DatabaseEntry key) {
        LongBinding.longToEntry(START_TABLE_KEY, key);
    }

    @Override
    public void setKey(DatabaseEntry key, TableImpl table) {
        assert table.isTop();
        LongBinding.longToEntry(table.getId(), key);
    }

}
