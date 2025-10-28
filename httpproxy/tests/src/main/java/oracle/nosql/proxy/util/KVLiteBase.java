package oracle.nosql.proxy.util;

import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.util.kvlite.KVLite;

/**
 * A base class for tests that start and stop KVLite instances.
 */
public abstract class KVLiteBase extends TestBase {
    static protected int multishardShards = 3;
    static protected int multishardPartitions = 10 * getNumPartitions();

    public static KVLite startKVLite(String hostName,
                                     String storeName,
                                     boolean useThreads,
                                     boolean verbose,
                                     boolean multishard,
                                     int memoryMB,
                                     boolean isSecure) {
        return startKVLite(hostName,
                           storeName,
                           useThreads,
                           verbose,
                           multishard,
                           memoryMB,
                           isSecure,
                           getKVPort(),     // default port
                           getPortRange(), // default port range
                           getTestDir()); // root
    }

    /*
     * Allow kv port, port range, and root to be specified in order to
     * allow multiple kvlite instances in the same test
     */
    public static KVLite startKVLite(String hostName,
                                     String storeName,
                                     boolean useThreads,
                                     boolean verbose,
                                     boolean multishard,
                                     int memoryMB,
                                     boolean isSecure,
                                     int port,
                                     String rangestr,
                                     String rootDir) {
        if (storeName == null) {
            storeName = getStoreName();
        }
        int capacity = 1;
        int numStorageNodes = 1;
        int repfactor = 1;
        int partitions = getNumPartitions();
        String portstr = Integer.toString(port);
        if (multishard) {
            if (memoryMB == 0 || memoryMB < 768) {
                memoryMB = 768; /* need extra space for multi-shard */
            }
            capacity = multishardShards;
            numStorageNodes = multishardShards;
            repfactor = 3;
            partitions = multishardPartitions;
            portstr = Integer.toString(port) + KVLite.DEFAULT_SPLIT_STR +
                      Integer.toString(port + 30) + KVLite.DEFAULT_SPLIT_STR +
                      Integer.toString(port + 60);
            rangestr = getPortRange() + KVLite.DEFAULT_SPLIT_STR +
                       Integer.toString(port + 35) + "," +
                       Integer.toString(port + 40) + KVLite.DEFAULT_SPLIT_STR +
                       Integer.toString(port + 65) + "," +
                       Integer.toString(port + 70);
        }


        KVLite kvlite = new KVLite(rootDir,
                                   storeName,
                                   portstr,
                                   true, /* run bootadmin */
                                   hostName,
                                   rangestr,
                                   null, /* service port range */
                                   partitions,
                                   null, /* mount point */
                                   useThreads,
                                   isSecure,
                                   null, /* no backup to restore */
                                   -1,
                                   numStorageNodes,
                                   repfactor,
                                   capacity);
        kvlite.setVerbose(verbose);
        kvlite.setTableOnly(true);

        if (memoryMB == 0 || memoryMB < 256) {
            /* use 256 if not multi-shard and not explicitly set by caller */
            memoryMB = 256;
        }
        kvlite.setMemoryMB(memoryMB);

        ParameterMap policies = new ParameterMap();
        policies.setParameter(ParameterState.COMMON_HIDE_USERDATA, "false");
        kvlite.setPolicyMap(policies);

        try  {
            kvlite.start(true);
        } catch (Throwable t) {
            /*
             * Display any setup problems, which may happen in particular
             * when debugging standalone, outside a nightly build run.
             */
            System.err.println("problems starting up KvLite");
            t.printStackTrace();
            throw t;
        }

        return kvlite;
    }

    public static KVLite startKVLite(String hostName,
            String storeName,
            boolean useThreads,
            boolean verbose) {
        return startKVLite(hostName, storeName, useThreads,
                           verbose, false, 0, false);
    }
}
