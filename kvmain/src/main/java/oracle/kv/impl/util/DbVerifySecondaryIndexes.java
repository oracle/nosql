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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Get;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.map.HashKeyToPartitionMap;
import oracle.kv.impl.map.KeyToPartitionMap;
import oracle.kv.impl.rep.table.IndexKeyCreator;
import oracle.kv.impl.rep.table.TableMetadataPersistence;
import oracle.kv.Key.BinaryKeyIterator;
import oracle.kv.table.Index;
import oracle.kv.table.Table;

public class DbVerifySecondaryIndexes implements SecondaryAssociation {

    static final Map<String, TableEntry> secondaryLookupMap = new HashMap<>();
    static final Map<String, IndexImpl> indexLookupMap = new HashMap<>();
    static Map<String, Database> DatabaseLookupMap = new HashMap<>();
    Map<String, SecondaryConfig> keyCreatorLookup = new HashMap<>();
    static Environment env = null;
    private volatile KeyToPartitionMap mapper = null;

    @Override  
    public boolean isEmpty() {
        return false;
    }

    public DbVerifySecondaryIndexes(Environment curEnv,
                                    Map<String, Database> allPDbs,
                                    Map<String, SecondaryConfig>
                                    keyCreatorLookup) {
        try {
            env = curEnv;
            this.keyCreatorLookup = keyCreatorLookup;
            populateSecondaryLookupMap(keyCreatorLookup);
            String pattern = "p(\\d{1,5})$";
            Pattern r = Pattern.compile(pattern);
            int nPartitions = 0;
            for (String dbName : env.getDatabaseNames()) {
                if (r.matcher(dbName).find()) {
                    nPartitions++;
                }
            }
            if (nPartitions > 0) {
                mapper = new HashKeyToPartitionMap(nPartitions);
            }
            DatabaseLookupMap = allPDbs;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void scanTable(TableImpl table,
            Map<String, IndexImpl> indexes) {

        for (Table child : table.getChildTables().values()) {
            scanTable((TableImpl)child, indexes);
        }

        if (indexes == null) {
            return;
        }
        for (Index index :
                table.getIndexes(Index.IndexType.SECONDARY).values()) {

            indexes.put(createDbName(table.getInternalNamespace(),
                        index.getName(),
                        table.getFullName()),
                    (IndexImpl)index);
        }
    }

    public static void populateSecondaryLookupMap(
            Map<String, SecondaryConfig> keyCreatorLookup) throws Exception {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(true);
        dbConfig.setCacheMode(CacheMode.EVICT_LN);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        Database db = env.openDatabase(null, "TableMetadata", dbConfig);

        Cursor c = db.openCursor(null, null);
        boolean firstRec = true;
        while (c.get(key, data, Get.NEXT, null) != null) {
            if (firstRec) {
                firstRec = false; /*skip first record*/
            } else {
                TableImpl t = TableMetadataPersistence.getTable(data);
                final TableEntry ent = new TableEntry(t);
                if (ent.hasSecondaries()) {
                    secondaryLookupMap.put(t.getIdString(), ent);
                }
                scanTable(t, indexLookupMap);
                //System.out.println(n + " " + ent.hasSecondaries());
            }
        }
        for (Map.Entry<String, IndexImpl> entry : indexLookupMap.entrySet()) {
            IndexImpl index = indexLookupMap.get(entry.getKey());
            final IndexKeyCreator keyCreator = new IndexKeyCreator(
                    index, 10000);
            final SecondaryConfig secConfig = new SecondaryConfig();
            secConfig.setReadOnly(true).
                setDuplicateByteComparator(oracle.kv.Key.BytesComparator.class).
                setSortedDuplicates(true);
            if (keyCreator.isMultiKey() || index.isGeometryIndex()) {
                secConfig.setMultiKeyCreator(keyCreator);
            } else {
                secConfig.setKeyCreator(keyCreator);
            }
            keyCreatorLookup.put(entry.getKey(), secConfig);
        }

        c.close();
        db.close();
    }

    public static String createDbName(String namespace,
            String indexName,
            String tableName) {
        final StringBuilder sb = new StringBuilder();
        sb.append(indexName).append(".").append(tableName);
        if (namespace != null) {
            sb.append(":").append(namespace);
        }
        return sb.toString();
    }

    @Override
    public Database getPrimary(DatabaseEntry primaryKey) {
        String dbName = mapper.getPartitionId(primaryKey.getData()).
                               getPartitionName();
        return DatabaseLookupMap.get(dbName);
    }

    public SecondaryDatabase getSecondaryDb(String dbName) {
        if (DatabaseLookupMap.containsKey(dbName)) {
            return (SecondaryDatabase)DatabaseLookupMap.get(dbName);
        }
        IndexImpl index = indexLookupMap.get(dbName);
        final IndexKeyCreator keyCreator = new IndexKeyCreator(
                index, 10000);
        //        params.getRepNodeParams().getMaxIndexKeysPerRow());

        final SecondaryConfig dbConfig = new SecondaryConfig();
        dbConfig.setSecondaryAssociation(this).
            setReadOnly(true).
            setDuplicateByteComparator(oracle.kv.Key.BytesComparator.class).
            setSortedDuplicates(true);
        if (keyCreator.isMultiKey() || index.isGeometryIndex()) {
            dbConfig.setMultiKeyCreator(keyCreator);
        } else {
            dbConfig.setKeyCreator(keyCreator);
        }

        try {
            final SecondaryDatabase db =
                env.openSecondaryDatabase(null, dbName, null, dbConfig);
            return db;
        } catch (IllegalStateException e) {

            /*
             * The exception was most likely thrown because the environment
             * was closed.  If it was thrown for another reason, though,
             * then invalidate the environment so that the caller will
             * attempt to recover by reopening it.
             */
            if (env.isValid()) {
                EnvironmentFailureException.unexpectedException(
                        DbInternal.getEnvironmentImpl(env), e);
            }
            throw e;

        } finally {
        }
    }
    
    private static class TableEntry {
        private final int keySize;
        private final Set<String> secondaries = new HashSet<>();
        private final Map<String, TableEntry> children = new HashMap<>();

        TableEntry(TableImpl table) {
            /* For child tables subtract the key count from parent */
            keySize = (table.getParent() == null ?
                    table.getPrimaryKeySize() :
                    table.getPrimaryKeySize() -
                    ((TableImpl)table.getParent()).getPrimaryKeySize());

            /* For each index, save the secondary DB name */
            for (Index index :
                    table.getIndexes(Index.IndexType.SECONDARY).values()) {

                secondaries.add(createDbName(
                        ((TableImpl)index.getTable()).getInternalNamespace(),
                        index.getName(),
                        index.getTable().getFullName()));
            }

            /* Add only children which have indexes */
            for (Table child : table.getChildTables().values()) {
                final TableEntry entry = new TableEntry((TableImpl)child);

                if (entry.hasSecondaries()) {
                    children.put(((TableImpl)child).getIdString(), entry);
                }
            }
        }

        private boolean hasSecondaries() {
            return !secondaries.isEmpty() || !children.isEmpty();
        }

        private Collection<String> matchIndexes(BinaryKeyIterator keyIter) {
            /* Match up the primary keys with the input keys, in number only */
            for (int i = 0; i < keySize; i++) {
                /* If the key is short, then punt */
                if (keyIter.atEndOfKey()) {
                    return null;
                }
                keyIter.skip();
            }

            /* If both are done we have a match */
            if (keyIter.atEndOfKey()) {
                return secondaries;
            }

            /* There is another component, check for a child table */
            final String childId = keyIter.next();
            final TableEntry entry = children.get(childId);
            return (entry == null) ? null : entry.matchIndexes(keyIter);
        }
    }

    @Override
    public Collection<SecondaryDatabase>
        getSecondaries(DatabaseEntry primaryKey) {
            final BinaryKeyIterator keyIter =
                new BinaryKeyIterator(primaryKey.getData());

            /* The first element of the key will be the top level table */
            final String rootId = keyIter.next();

            final TableEntry entry = secondaryLookupMap.get(rootId);

            /* The entry could be null if the table doesn't have indexes */
            if (entry == null) {
                return Collections.emptySet();
            }

            /* We have a table with indexes, match the rest of the key. */
            final Collection<String> matchedIndexes =
                entry.matchIndexes(keyIter);

            /* This could be null if the key did not match any table 
             * with indexes 
             */
            if (matchedIndexes == null) {
                return Collections.emptySet();
            }
            final List<SecondaryDatabase> secondaries =
                new ArrayList<>(matchedIndexes.size());

            for (String dbName : matchedIndexes) {
                final SecondaryDatabase db = getSecondaryDb(dbName);
                if (db == null) {
                    /* Throwing RNUnavailableException should cause a retry */
                    System.out.println("Secondary db not yet opened " + dbName);
                }
                secondaries.add(db);
            }
            return secondaries;
    }
}
