/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.systables.IndexStatsLeaseDesc;
import oracle.kv.impl.systables.PartitionStatsLeaseDesc;
import oracle.kv.impl.systables.TableStatsIndexDesc;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

/**
 * Utility class for doing operations on stores related to load and compare. It
 * can be configured to generate keys and values that are ints (smaller, easier
 * to debug), or UUIDs (larger, more realistic).
 *
 * Be sure to call StoreUtils.close() when finished with the test, because
 * otherwise the kvstore client will stay alive!
 */
public class StoreUtils {

    /** This class can use random ints or UUIDs for keys and values. */
    public enum RecordType {INT, UUID}

    private final KVStoreConfig config;
    private final KVStore store;
    private final DataGenerator dataGenerator;

    public StoreUtils(String storeName, String host, int port,
                      RecordType type) {
        this(storeName, host, port, null, type, 0, null, null);
    }

    public StoreUtils(String storeName, String host, int port, RecordType type,
                      long seed) {
        this(storeName, host, port, null, type, seed, null, null);
    }

    /*
     * Used for secure store with security configuration file.
     */
    public StoreUtils(String storeName, String host, int port,
                      String securityFile, RecordType type, long seed) {
        this(storeName, host, port, securityFile, type, seed, null, null);
    }

    /*
     * Used for secure store with login credential and trust store path.
     */
    public StoreUtils(String storeName, String host, int port, RecordType type,
                      long seed, LoginCredentials cred, String trustStorePath) {
        this(storeName, host, port, null, type, seed, cred, trustStorePath);
    }

    /**
     * @param storeName
     * @param host
     * @param port
     * @param securityFile used to login store
     * @param cred login credential
     * @param trustStorePath trust store path
     * @param type what type of records to load
     * @param seed applies only to RecordType.INT records, for the random
     * number generator, so that unit tests are deterministic.
     */
    public StoreUtils(String storeName, String host, int port,
                      String securityFile, RecordType type, long seed,
                      LoginCredentials cred, String trustStorePath) {

        String hostPort = host + ":" + port;
        config = new KVStoreConfig(storeName, hostPort);

        if (cred ==null) {
            config.setSecurityProperties(KVStoreLogin
                .createSecurityProperties(securityFile));
            store = KVStoreFactory.getStore(config);
        } else {
            Properties props = new Properties();
            props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                      KVSecurityConstants.SSL_TRANSPORT_NAME);
            props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                      trustStorePath);
            config.setSecurityProperties(props);
            store = KVStoreFactory.getStore(config, cred, null);
        }
        switch(type) {
            case INT:
               dataGenerator = new IntGenerator(seed);
               break;
            case UUID:
               dataGenerator = new UUIDGenerator(seed);
               break;
            default:
                throw new RuntimeException("Didn't deal with new enum type");
        }
    }

    public KVStore getKVStore() {
        return store;
    }

    /**
     * Load numRecords of randomly generated key/value pairs into the store.
     */
    public List<Key> load(int numRecords) {
        return load(numRecords, null, 0);
    }

    /**
     * Load numRecords of randomly generated key/value pairs into the store.
     * Assert that each of the targetPartitions is populated with a minimum
     * number of keys/values.
     */
    public List<Key> load(int numRecords,
                          Collection<PartitionId> targetPartitions,
                          int minRecordsPerPartition) {
        List<Key> loadedKeys = new ArrayList<Key>();

        Map<PartitionId, Integer> perPartitionCount =
            new HashMap<PartitionId, Integer>();
        if (targetPartitions != null) {
            for (PartitionId p : targetPartitions) {
                perPartitionCount.put(p, 0);
            }
        }

        for (int i = 0; i < numRecords; i++) {
            String testVal = dataGenerator.genValue();
            Key key = Key.createKey(testVal);
            loadedKeys.add(key);
            Value value = Value.createValue(testVal.getBytes());
            store.put(key, value);

            if (targetPartitions != null) {
                PartitionId p = ((KVStoreImpl) store).getPartitionId(key);
                int count = perPartitionCount.get(p);
                count++;
                perPartitionCount.put(p, count);
            }
        }

        if (targetPartitions != null) {
            for (Map.Entry<PartitionId,Integer> entry:
                     perPartitionCount.entrySet()) {

                if (entry.getValue() < minRecordsPerPartition) {
                    throw new RuntimeException
                    ("Partition " + entry.getKey() + " has " +
                     entry.getValue() +
                     " records, which is less than the required minimum of " +
                     minRecordsPerPartition);
                }
            }
        }

        return loadedKeys;
    }

    /**
     * @return the number of records in the store.
     */
    public long numRecords() {
        long retVal = 0;

        /* Avoid timing dependent variations by using absolute consistency. */
        Iterator<Key> iterator =
            store.storeKeysIterator(Direction.UNORDERED, 0, null, null, null,
                                    Consistency.ABSOLUTE, 0, null);

        TableAPI tableAPI = store.getTableAPI();
        List<TableImpl> list = getStatsTableList(tableAPI);

        while (iterator.hasNext()) {
            Key key = iterator.next();
            /* Filter out the records in statistics tables */
            if(!isTablesRecord(key, list)) {
                ++retVal;
            }
        }
        return retVal;
    }

    /**
     * 1. Iterate "this" store, looking for each record in the other store.
     * 2. Reverse the process, iterating other store.
     * Used default (0) batch size.
     */
    public boolean compare(StoreUtils other) {
        if (!iterateAndCompare(store.storeIterator
                               (Direction.UNORDERED, 0), other.getKVStore())) {
            return false;
        }
        return iterateAndCompare(other.getKVStore().storeIterator
                                 (Direction.UNORDERED, 0), store);
    }

    /**
     * Check whether a key is belong to the tables in list or not.
     */
    private boolean isTablesRecord(Key key, List<TableImpl> list) {
        for (TableImpl table : list) {
            if (table.findTargetTable(key.toByteArray()) != null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Create a list contains stats tables
     */
    private List<TableImpl> getStatsTableList(TableAPI tableAPI) {
        List<TableImpl> list = new ArrayList<>();
        String[] tablesName = { IndexStatsLeaseDesc.TABLE_NAME,
                                PartitionStatsLeaseDesc.TABLE_NAME,
                                TableStatsPartitionDesc.TABLE_NAME,
                                TableStatsIndexDesc.TABLE_NAME };

        for (String tableName : tablesName) {
            Table table =  tableAPI.getTable(tableName);
            if (table != null) {
                list.add((TableImpl)table);
            }

        }
        return list;
    }

    private boolean iterateAndCompare(Iterator<KeyValueVersion> iterator,
                                      KVStore otherStore) {
        TableAPI tableAPI = otherStore.getTableAPI();
        List<TableImpl> list = getStatsTableList(tableAPI);

        while (iterator.hasNext()) {
            KeyValueVersion record = iterator.next();

            /* If the key is belong to stats tables, no need comparison */
            if(isTablesRecord(record.getKey(), list)) {
                continue;
            }

            ValueVersion vversion = otherStore.get(record.getKey());
            if (vversion == null ||
                !vversion.getValue().equals(record.getValue())) {
                return false;
            }
            String keyVal;
            String valueVal;

            try {
                keyVal = dataGenerator.parseKey(record);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Invalid key", e);
            }
            try {
                valueVal = dataGenerator.parseValue(record);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Invalid val", e);
            }
            if (!keyVal.equals(valueVal)) {
                throw new RuntimeException
                    ("Record mismatch, key: " + keyVal + " val: " + valueVal);
            }
        }
        return true;
    }

    /**
     * @throws KeyMismatchException if any key from the list is missing from
     * the store. More information about the problem key can be obtained via
     * the exception.
     */
    public void keysExist(List<Key> keys) {
        for (Key key : keys) {
            if (store.get(key) == null) {
                throw new KeyMismatchException(key,
                                               " is missing from the store");
            }
        }
    }

    /**
     * @throws KeyMismatchException if any key from the list exists in the
     * store. More information about the problem key can be obtained via the
     * exception.
     */
    public void keysDoNotExist(List<Key> keys) {
        for (Key key : keys) {
            if (store.get(key) != null) {
                throw new KeyMismatchException(key,
                                               " should not be in the store");
            }
        }
    }

    /**
     * A key does not match what is expects. Makes the key itself available to
     * the caller.
     */
    public class KeyMismatchException extends RuntimeException {

        private static final long serialVersionUID = 1L;
        private final Key key;
        KeyMismatchException(Key key, String problem) {
            super(key.getFullPath() + problem);
            this.key = key;
        }

        /**
         * @return the problem key.
         */
        public Key getKey() {
            return key;
        }
    }

    /**
     * a generator for test values.
     */
    interface DataGenerator {
        String genValue();
        String parseKey(KeyValueVersion record);
        String parseValue(KeyValueVersion record);
    }

    /**
     * Generates integer values for loads
     */
    private class IntGenerator implements DataGenerator {

        private final Random random;

        IntGenerator(long seed) {
            random = new Random(seed);
        }

        @Override
        public String genValue() {
            return Integer.toString(random.nextInt());
        }

        @Override
        public String parseKey(KeyValueVersion record) {
            String keyVal = record.getKey().getMajorPath().get(0);
            Integer.parseInt(keyVal);
            return keyVal;
        }

        @Override
        public String parseValue(KeyValueVersion record) {
            String val = new String(record.getValue().getValue());
            Integer.parseInt(val);
            return val;
        }
    }

    /**
     * Generates UUID values for loads
     */
    private class UUIDGenerator implements DataGenerator {

        private final Random random;

        UUIDGenerator(long seed) {
            random = new Random(seed);
        }

        @Override
        public String genValue() {
            return new UUID(random.nextLong(), random.nextLong()).toString();
        }

        @Override
        public String parseKey(KeyValueVersion record) {
            String keyVal = record.getKey().getMajorPath().get(0);
            return UUID.fromString(keyVal).toString();
        }

        @Override
        public String parseValue(KeyValueVersion record) {
            byte[] val = record.getValue().getValue();
            return UUID.fromString(new String(val)).toString();
        }
    }

    public void close() {
        store.close();
    }
}
