/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.PrintStream;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Operation;
import oracle.kv.RequestLimitConfig;
import oracle.kv.Value;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableOperation;

import standalone.datacheck.DataCheck.Phase;

import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.values.MapValue;

/**
 * Test that populates, exercises, and checks the contents of a store. <p>
 *
 * A test procedure should first use the populate phase to insert entries into
 * an empty store, next use the exercise phase to update the store, and finally
 * use the check phase to check the final contents of the store.  Multiple
 * threads and hosts can be used simultaneously or sequentially for each phase;
 * it is up to the caller to perform operations on the same non-overlapping
 * ranges of entries in each phase. <p>
 *
 * The main program exits with a non-zero status if the test fails, and
 * otherwise exits normally.  A test fails if any errors are thrown, which
 * causes a test thread to exit immediately, or if an exception is thrown or an
 * unexpected result is detected, both of which allow the test to proceed. <p>
 *
 * Test results are output to standard error.  The final entry includes the
 * following fields:
 * <ul>
 * <li>timeMillis - the elapsed time in milliseconds
 * <li>lagCount - the number of exercise operations that lagged (see below for
 *     explanation of lag)
 * <li>lagAverage - the average lag for operations that lagged
 * <li>lagMax - the maximum lag
 * <li>errors - the number of errors that caused threads to exit
 * <li>exceptions - the number of exceptions
 * <li>unexpectedResults - the number of unexpected results
 * </ul> <p>
 *
 * <b>Theory of operation</b><p>
 *
 * The test performs random operations with random keys in such a way that the
 * expected results are known and can be checked.  The behavior of each test
 * run is determined by a single random seed value, allowing each test run to
 * use a different pattern of operations and keys.  The initial seed is
 * included in the test results, and can be specified to repeat a particular
 * run.  Multiple executions that are part of the same run, both from different
 * hosts and different phases, should use the same seed, and will note an
 * unexpected result if different seed values are used. <p>
 *
 * To provide for scalability, test operations are divided into blocks which
 * can be distributed across hosts and threads.  Each block consists of 2^15
 * operations on a collection of 2^16 possible keys.  By performing operations
 * where half of the keys are present, the test exercises the behavior of
 * operations on both present and absent keys.  Exercise operations maintain
 * the ratio of present and absent keys. <p>
 *
 * To test concurrency, the exercise phase always runs pairs of threads, each
 * of which perform operations simultaneously on the same block of keys.  Each
 * thread performs operations on the same block of keys, but choosing the order
 * of the keys, the selection of half the keys, and the order of operations
 * differently. <p>
 *
 * <b>Keys and Values</b><p>
 *
 * Keys are a fixed size, with a maximum of 2^48 keys supported.  Keys have two
 * major components, to allow using store iterator operations, and one minor
 * component.  The major key is chosen using a pseudo-random algorithm as
 * described below.  The first component of the major key represents a 32-bit
 * integer, the second an 8-bit integer, and the minor key an 8-bit integer.
 * For a given block, each major key has on average only half of the possible
 * minor keys allocated. <p>
 *
 * Keys are formatted in hexadecimal as "/k12345678/12/-/12". <p>
 *
 * The index of an operation within the sequence of operations is related to
 * the key value as follows.  To allow each operation to refer to only half of
 * the possible key values, the index is multiplied by two to convert it to a
 * "keynum".  The bits of the keynum are then directly related to the key.  The
 * upper 40 bits specify the major key, with 32 bits for the first component
 * and 8 for the second.  The bottom 8 bits specify the minor key value.  The
 * value of the major key bytes is converted to a pseudo-random value by using
 * secret key encryption.  Because encryption provides a 1-to-1 transformation
 * of the bytes, it provides a reversible way to scramble a collection of
 * values represented by fixed-size integers.  The test uses different
 * encryption keys, each dependent on the original random seed, to produce the
 * major key, as well as to scramble bucket values and operations during the
 * exercise phase. <p>
 *
 * Values are a fixed size and represent the operation which stored the value,
 * either the populate phase or one of the two threads in the exercise phase,
 * plus the sequence number of the operation. <p>
 *
 * Values are represented in UTF8, formatted in hexadecimal as
 * "/OP-index=123456789abc", where OP is either "pp" for the populate phase,
 * "ea" for the first of a pair of exercise threads, or "eb" for the second of
 * the pair. <p>
 *
 * The test also stores additional entries with the key prefix "/m/" to
 * represent meta data about the test itself. <p>
 *
 * <b>Checking</b><p>
 *
 * The test supports three different ways of checking correct results.  During
 * the exercise phase, the previous values returned by update operations are
 * checked by default for correctness.  The values returned by read operations
 * are checked.  Finally, the check phase reads and checks all values saved in
 * the store. <p>
 *
 * To perform correctness checks, the test uses the reversibility of the key
 * generation scheme to check for a given key which operations might have been
 * responsible for setting its value: the populate phase, or one or both of the
 * two exercise threads.  The relationship between the key and the order of
 * operations means that the checker can determine which of the two exercise
 * operations is expected to happen first, and uses that information to compute
 * the expected values. <p>
 *
 * Because the store can be configured to support different levels of
 * durability and consistency, as well as the fact that the pairs of exercise
 * threads are only coordinated on block boundaries, the interaction between
 * the exercise threads is somewhat non-deterministic.  If the test finds a
 * value that can only be explained by a lag in consistency or coordination
 * between exercise threads, it keeps track of the number of operations of lag
 * between the pair of operations and includes statistics about lags in the
 * results.  Note that the caller should make sure that all replicas are
 * up-to-date before starting another test phase: the test assumes that
 * populate values are consistent for the exercise phase, and exercise updates
 * are consistent for the check phase. <p>
 *
 * <b>Arguments</b><p>
 *
 * The first set of arguments is required. <p>
 *
 * The -store, -host, and -port arguments specify how to contact the store. <p>
 *
 * The remaining arguments are optional. <p>
 *
 * The -admin-host specifies the admin host used to connect to admin service.
 * <p>
 *
 * The -admin-port specifies the admin port used to connect to admin service.
 * <p>
 *
 * The -phase argument specifies the test phase. <p>
 *
 * The -start, -count, and -threads arguments control performing test
 * operations on multiple threads and hosts.  Start and count values are
 * specified as numbers of blocks, each representing 2^15 or 32,768 operations.
 * The exercise phase performs operations using pairs of threads, so the actual
 * number of threads used is twice the specified number.  Each thread performs
 * operations on the same number of blocks, so the count must be a multiple of
 * the number of threads. <p>
 *
 * The -consistency and -durability arguments control the consistency of read
 * operations and the durability of write operations.  Note that the exercise
 * and check phases depend on seeing data saved to the store by earlier phases.
 * Callers may need to specify absolute or time-based consistency to satisfy
 * this requirement. <p>
 *
 * The -requestLimitConfig argument controls the number of concurrent requests
 * permitted to RNs. <p>
 *
 * The -requestTimeout argument specifies the maximum permitted timeout for
 * store operations, including the network read timeout. <p>
 *
 * The -blockTimeout argument specifies the maximum permitted timeout for
 * performing a set of operations on a block of entries. <p>
 *
 * The -lobChunkTimeout argument specifies the maximum timeout for chunk access
 * during operations on LOBs. <p>
 *
 * The -lobOpTimeout argument specifies the maximum permitted timeout for
 * performing a LOB operation. <p>
 *
 * The -lobPercent argument specifies the proportion of LOB operations during
 * the whole population and exercise phases. <p>
 *
 * The -reportingInterval argument specifies the time interval for reporting
 * test progress and the client-side status of the store. <p>
 *
 * The -partitions argument limits operations to keys that fall within the
 * specified partitions.  Note that the behavior of the -count argument does
 * not take partition filtering into account, so specifying partitions will
 * reduce the number of operations performed.  It is not required to specify
 * the same partitions for each phase.  If partitions are specified for a
 * phase, though, then the same partitions, or a more restricted set, must be
 * specified for subsequent phases. <p>
 *
 * The -readPercent argument specifies the proportion of operations during the
 * exercise phase that should be read operations.  These operations are
 * interspersed with the update operations to achieve the requested percentage.
 * If the value is 100, then no update operations will be performed.  Note
 * that, in the exercise phase, the -count argument only controls the number of
 * update operations, except if the value is 100, in which case it specifies
 * the number of read operations. <p>
 *
 * The -maxThroughput argument specifies the maximum number of client
 * requests performed per second.  A positive value will throttle the request
 * rate across all client threads to keep the throughput below the requested
 * value.  Specifying 0, which is the default, will disable throttling.  Note
 * that if the value for this argument is larger than the maximum throughput
 * that the client threads are able to achieve, you should consider adding more
 * threads or clients to reach the desired throughput. <p>
 *
 * The -maxExecutionTime argument specifies the maximum execution time in
 * seconds for the phase to complete.  If the maximum time is reached while
 * operations are underway, each client thread will stop operations when it
 * reaches the end of the current block.  The default value of 0 disables the
 * execution time limit. <p>
 *
 * The -exerciseNoCheck argument specifies that the exercise phase should not
 * check the correctness of previous values returned by update operations.
 * Previous values are checked by default.  This argument does not disable
 * checking the results of read operations. <p>
 *
 * The -useParallelScan argument specifies whether we should use parallel scan
 * during the exercise phase when running in Key/Value mode. If it is not
 * specified, only single thread scans will be performed in Key/Value mode.
 * Note that table mode always uses parallel scan for multi-partition and
 * multi-shard table iterations and queries, regardless of whether this flag is
 * specified. <p>
 *
 * The -scanMaxConcurrentRequests specifies how many threads will be started
 * on client side to accomplish the parallel scan tasks. <p>
 *
 * The -scanMaxResultsBatches specifies how many batches of results are allowed
 * on client side. One batch will handle scan results returned from one scan
 * operation. <p>
 *
 * The -seed argument specifies the seed for the random number generator used
 * by the test.  If not specified, the seed value is chosen at random by the
 * exercise phase and saved in the store.  Subsequent phases use this value by
 * default.  Callers should specify the value explicitly if the exercise phase
 * will be run on multiple hosts.  Note that the exercise phase performs two
 * threads of operation simultaneously for each block of entries.  Although the
 * sequence of operations performed by each thread is completely determined by
 * the random seed, the interaction of the two threads for a given block is not
 * controlled and will not be repeatable. <p>
 *
 * The -table and -kv arguments specify whether to perform operations using the
 * Table API or to perform Key/Value operations. <p>
 *
 * The -syncMode controls the API style used for testing, it accepts the
 * following values:
 * <ul>
 * <li> SYNC: Test the sync style API via RMI protocol.
 * <li> SYNC_OVER_ASYNC: Test the sync style API via async protocol, this is
 *      default.
 * <li> ASYNC: Test the async style API, it only uses async protocol.
 * <li> BOTH_OVER_ASYNC: Test the sync and async style API via async protocol.
 * <li> SYNC_OVER_BOTH: Test the sync style API via RMI and async protocol.
 * <li> SYNC_AND_ASYNC: Combination of 'SYNC' and 'ASYNC'.
 * </ul>
 * Currently, only the 'Table' mode has the async style API, so it requires
 * '-table' for the values of ASYNC, BOTH_OVER_ASYNC and SYNC_AND_ASYNC. Other
 * three values can work under both 'KV' and 'Table' mode. <p>
 *
 * The -useTTL argument specifies whether to test the time to live feature when
 * running in table mode. By default it is true. <p>
 *
 * The -verbose argument specifies printing verbose output if specified once,
 * and debugging output if specified more than once.  When specified once,
 * output includes an entry when each thread starts a block, and information
 * about lagging operations.  When specified twice, output includes an entry
 * for each operation performed. <p>
 *
 * The -exitOnUnexpectedException argument allows to exit the test if unexpected
 * exception occurs.  This can be used for debugging of the unexpected
 * exception by preventing excessive running time and log accumulation. <p>
 *
 * The -exitOnUnexpectedResult argument allows to exit the test if unexpected
 * result occurs.  This can be used for debugging of the unexpected
 * result by preventing excessive running time and log accumulation. <p>
 *
 * If -proxy CLOUD, then that test uses additional arguments needed for
 * authentication and rate limiting. See {@link DataCheckTableProxy} <p>
 *
 * Drop table test has uses some additional arguments.  For their description,
 * see {@link DataCheckDropTable.InitParams}.
 */
public final class DataCheckMain {

    private static final long DEFAULT_COUNT = 100;

    public static final String USAGE =
        "Usage: " + DataCheck.class.getName() + " [args...]\n" +
        "\n" +
        "Required arg:\n" +
        "  -store <store name>\n" +
        "Required args if NOT using -proxy CLOUD:\n" +
        "  -host <host name>\n" +
        "  -port <host port>\n" +
        "\n" +
        "Optional args:\n" +
        "  -admin-host <admin host name> (default: value of -host arg)\n" +
        "  -admin-port <admin host port> (default: value of -port arg)\n" +
        "  -phase [populate | exercise | check | clean]" +
        " (default: populate)\n" +
        "    Specifies the test phase\n" +
        "  -start <block> (default: 0)\n" +
        "    Zero-based starting index for test operations\n" +
        "  -count <blocks> (default: " + DEFAULT_COUNT + ")\n" +
        "    Number of blocks of 2^15 operations to perform\n" +
        "  -threads <number threads> (default: 1)\n" +
        "    Number of concurrent clients\n" +
        "  -consistency [ABSOLUTE | NONE_REQUIRED_NO_MASTER | " +
        "NONE_REQUIRED |\n" +
        "                lag=<ms>,timeout=<ms>] (default: NONE_REQUIRED)\n" +
        "    Consistency for read operations\n" +
        "  -durability [COMMIT_NO_SYNC | COMMIT_SYNC | COMMIT_WRITE |\n" +
        "               masterSync=<sync>,replicaSync=<sync>," +
        "replicaAck=<ack>]\n" +
        "              (default: COMMIT_NO_SYNC)\n" +
        "    Durability for write operations:\n" +
        "      <sync> should be NO_SYNC, SYNC, or WRITE_NO_SYNC\n" +
        "      <ack> should be ALL, NONE, or SIMPLE_MAJORITY\n" +
        "  -requestLimitConfig maxActiveRequests=<integer>, (default: 100)\n" +
        "                      requestThresholdPercent=<percent>, " +
        "(default: 90)\n" +
        "                      nodeLimitPercent=<percent> (default: 80)\n" +
        "    RequestLimit for kvstore\n" +
        "    <percent> should be an integer between 0 and 100\n" +
        "  -requestTimeout <ms> (default: 20000)\n" +
        "  -blockTimeout <ms> (default: 60000)\n" +
        "  -lobChunkTimeout <ms> (default: 10000)\n" +
        "    The timeout value for accessing a chunk in LOB operations\n" +
        "  -lobOpTimeout <ms> (default: 0)\n" +
        "    The timeout value for a whole LOB operation, if set to 0,\n" +
        "    defaults to lobChunkTimeout*8\n" +
        "  -lobPercent <percent> (default: 0.0)\n" +
        "    Percentage of LOB operations in the populate and exercise \n" +
        "    phases, a number between 0.0 and 100.0\n" +
        "  -reportingInterval <ms> (default: 10000)\n" +
        "    Time interval for reporting status\n" +
        "  -partitions <partition[,partition...]>\n" +
        "    Restrict operations to the keys in the specified partitions\n" +
        "  -readPercent <percent> (default: 50.0)\n" +
        "    Percentage of operations in the exercise phase that should be\n" +
        "    read operations, a number between 0.0 and 100.0\n" +
        "  -maxThroughput <ops/s> (default: 0)\n" +
        "    Client maximum request rate, default value 0 disables.\n" +
        "  -maxExecutionTime <seconds> (default: 0)\n" +
        "    Maximum execution time, default value 0 disables.\n" +
        "  -exerciseNoCheck\n" +
        "    Don't perform checks during exercise phase\n" +
        "  -useParallelScan\n" +
        "    Perform parallel scan operations if specified.\n" +
        "  -scanMaxConcurrentRequests <number> (default: 0)\n" +
        "    Max concurrency for parallel scan.\n" +
        "  -scanMaxResultsBatches <number> (default: 0)\n" +
        "    Max slots to store results returned from parallel scan.\n" +
        "  -seed <long>\n" +
        "    Seed for random number generator\n" +
        "  -table [-indexReadPercent <percent> (default: 10)] (default in" +
        " proxy mode)\n" +
        "    Use table mode. -indexReadPercent is used to specify\n" +
        "    the percentage of index read operations in all read\n" +
        "    operations, it should be an integer number between\n" +
        "    0 and 100.\n" +
        "  -kv (default, except in proxy mode)\n" +
        "    Use key/value mode.\n" +
        "  -proxy <ONPREM | CLOUD> \n" +
        "    Use the Java SDK driver and the proxy.\n" +
        "  -numpartitions <number of partitions>\n" +
        "    Mandatory only for Java SDK driver.\n" +
        "  -secureproxy [true|false] (default: false)\n" +
        "  -proxyhost <proxy host name>\n" +
        "  -proxyport <proxy port number>\n" +
        "  -proxyuser <proxy kv user name>\n" +
        "  -proxypass <proxy kv user password>\n" +
        "  -principalType <USER | INSTANCE | RESOURCE>\n" +
        "    Cloud authorization that needs to be used with\n" +
        "    -proxy CLOUD.\n" +
        "    USER requires -ociConfigPath and -ociConfigProfile\n" +
        "    INSTANCE requires -tenantOcid\n" +
        "    RESOURCE requires -tenantOcid\n" +
        "  -tenantOcid <tenancy ocid for Instance Principal>\n" +
        "  -ociConfigPath </path/to/.oci/config>\n" +
        "  -ociConfigProfile <profile name for credentials in .oci/config>\n" +
        "  -syncMode [SYNC | SYNC_OVER_ASYNC | ASYNC | BOTH_OVER_ASYNC |\n" +
        "             SYNC_OVER_BOTH | SYNC_AND_ASYNC]" +
        " (default: SYNC_OVER_ASYNC)\n" +
        "    Specify the API test mode.\n" +
        "  -useTTL [true|false] (default: true)\n" +
        "    Whether to use TTL records for the testing\n" +
        "  -verbose\n" +
        "    Verbose output, repeat for debug output\n" +
        "  -exitOnUnexpectedException\n" +
        "    Exit the test when unexpected exception occurs.\n" +
        "  -exitOnUnexpectedResult\n" +
        "    Exit the test when unexpected result occurs.\n" +
        "  -dcTableReadUnits <KB reads/s>\n" +
        "    read units for a DataCheck table running on the cloud\n" +
        "  -dcTableWriteUnits <KB writes/s>\n" +
        "    write units for a DataCheck table running on the cloud\n" +
        "  -dcTableStorageGB <GB>\n" +
        "    storage limits in GB for DataCheck table running on the cloud\n" +
        "  -writeMultipleMax <ops> (default: 50)\n" +
        "    Max bulk write size for running operations on the cloud\n" +
        "  -useDcTableId [true|false] (default: true)\n" +
        "    Using debugging feature on kv24.2 or above\n" +
        "  -dropCloudTableBefore [true|false] (default: true)\n" +
        "    Drop DataCheck and DataCheckMD tables if they exist before\n" +
        "    populate phase\n" +
        "  -dropCloudTableAfter [true|false] (default: true) \n" +
        "    Drop DataCheck and DataCheckMD tables for clean phase instead\n" +
        "    of deleting by blocks\n" +
        "\n" +
        DataCheckDropTable.InitParams.USAGE;

    enum SyncMode {
        SYNC, SYNC_OVER_ASYNC, ASYNC, BOTH_OVER_ASYNC,
        SYNC_OVER_BOTH, SYNC_AND_ASYNC;
    }

    enum ProxyType { ONPREM, CLOUD }

    /**
     * different types of auth to run on the cloud
     */
    public enum PrincipalType { USER, INSTANCE, RESOURCE }

    /**
     * Private constructor to satisfy CheckStyle and to prevent
     * instantiation of this utility class.
     */
    private DataCheckMain() {
        throw new AssertionError("Instantiated utility class " +
            DataCheckMain.class);
    }

    /**
     * Run the test, with arguments as documented by {@link #USAGE}, and exit
     * with a non-zero status value if the test fails.
     */
    public static void main(String[] args) {
        try {
            mainNoExit(System.err, args);
            return;
        } catch (UsageException e) {
            System.err.println("\n" + e.getMessage());
        } catch (DataCheck.TestFailedException e) /* CHECKSTYLE:OFF */ {
            /* Message already logged */
        } /* CHECKSTYLE:ON */ catch (Throwable t) {
            t.printStackTrace();
        }
        System.exit(1);
    }

    /*
     * The chart of the mode, the client API used, and the server used.
     * Mode             Client       Server
     * ---------------  -----------  ------------
     * SYNC             Sync         Sync
     * SYNC_OVER_ASYNC  Sync         Async
     * ASYNC            Async        Async
     * BOTH_OVER_ASYNC  Sync, Async  Async
     * SYNC_OVER_BOTH   Sync         Sync, Async
     * SYNC_AND_ASYNC   Sync, Async  Sync, Async
     *
     */
    private static boolean checkSyncMode(SyncMode mode, boolean useTable) {
        boolean asyncAPI = false;
        switch (mode) {
        case ASYNC:
        case BOTH_OVER_ASYNC:
        case SYNC_AND_ASYNC:
            if (!useTable) {
                usage("-syncMode ASYNC or -syncMode BOTH_OVER_ASYNC or " +
                      "-syncMode SYNC_AND_ASYNC requires -table");
            }
            asyncAPI = true;
            break;
        default:
            break;
        }

        if (mode != SyncMode.SYNC) {
            System.setProperty("oracle.kv.async.server", "true");
        } else {
            System.setProperty("oracle.kv.async.server", "false");
        }

        return asyncAPI;
    }

    /**
     * When we are testing two protocols or two API styles at the same time,
     * we are doing mixed testing. Mixed testing only happens at the exercise
     * phase.
     * The rule for mixed testing is that, the first DataCheck object will test
     * sync API and RMI protocol as much as possible. So when we are testing
     * both sync and async style API, the first DataCheck object tests the
     * sync API, while the second DataCheck object tests the async style API.
     * When we are testing both RMI protocol and async protocol, the first
     * DataCheck object tests the RMI protocol, while the second DataCheck
     * object tests the async protocol. The following functions of 'fixConfig'
     * and 'getSecondStorePair' sets up the store configurations according to
     * the rule.
     */

    /*
     * This is used to setup store configuration for the first DataCheck object.
     */
    private static void fixConfig(KVStoreConfig config, SyncMode mode) {
        switch (mode) {
        case SYNC:
        case SYNC_OVER_BOTH:
        case SYNC_AND_ASYNC:
            config.setUseAsync(false);
            break;
        case SYNC_OVER_ASYNC:
        case ASYNC:
        case BOTH_OVER_ASYNC:
            config.setUseAsync(true);
            break;
        default:
            break;
        }
    }

    static class StoreAndConfig {
        final KVStore store;
        final KVStoreConfig config;

        StoreAndConfig(KVStore store, KVStoreConfig config) {
            this.store = store;
            this.config = config;
        }
    }

    private static StoreAndConfig getSecondStoreAndConfig(
        KVStoreConfig config, SyncMode mode) {
        switch (mode) {
        case SYNC:
        case SYNC_OVER_ASYNC:
        case ASYNC:
            /* Not mixed testing, so only one store is enough. */
            return null;
        default:
            KVStoreConfig secondConfig = config.clone();
            secondConfig.setUseAsync(true);
            KVStore secondStore = KVStoreFactory.getStore(secondConfig);
            return new StoreAndConfig(secondStore, secondConfig);
        }
    }

    /**
     * Run the test, with arguments as documented by {@link #USAGE}, printing
     * output to the specified stream.  Returns normally if the test passed,
     * otherwise throws an exception.
     */
    public static void mainNoExit(PrintStream err, String... argv) {
        new Main(err, argv).main();
    }

    static class Main {
        final PrintStream err;
        final String[] argv;
        String storeName = null;
        String hostName = null;
        String portName = null;
        String proxyProtocol = "http";
        String proxyHostName = null;
        String proxyPortNum = null;
        String proxyUserName = null;
        String proxyUserPass = null;
        String adminHostName = null;
        String adminPortName = null;
        Phase phase = Phase.POPULATE;
        long startBlocks = 0;
        long countBlocks = DEFAULT_COUNT;
        int threads = 1;
        Consistency consistency = Consistency.NONE_REQUIRED;
        Durability durability = Durability.COMMIT_NO_SYNC;
        RequestLimitConfig requestLimit =
            new RequestLimitConfig(100, 90, 80);
        long requestTimeout = 20000;
        long blockTimeout = 60000;
        long lobChunkTimeout = 10000;
        long lobOpTimeout = 0; /* default value */
        long reportingInterval = 10000;
        String partitionsString = null;
        int[] partitions = null;
        double readPercent = 50.0;
        double lobPercent = 0.0;
        double maxThroughput = 0;
        int maxExecTime = 0;
        boolean exerciseNoCheck = false;
        boolean useParallelScan = false;
        int scanMaxConcurrentRequests = 0;
        int scanMaxResultsBatches = 0;
        long seed = -1;
        int verbose = 0;
        boolean useTable = false;
        boolean useKv = false;
        boolean useProxy = false;
        int numPartitions = 0;
        SyncMode syncMode = SyncMode.SYNC_OVER_ASYNC;
        boolean asyncAPI = false;
        boolean useTTL = true;
        int indexReadPercent = 10;
        DataCheckDropTable.InitParams dropTableParams = null;
        String ociConfigPath = null;
        String ociConfigProfile = null;
        ProxyType proxyType = ProxyType.ONPREM;
        PrincipalType principalType = null;
        String tenantOcid = null;
        TableLimits dcTableLimits = null;
        int dcTableReadUnits = 0;
        int dcTableWriteUnits = 0;
        int dcTableStorageGB = 0;
        int writeMultipleMax = 50;
        boolean useDcTableId = true;
        boolean dropCloudTableBefore = true;
        boolean dropCloudTableAfter = true;

        Main(PrintStream err, String... argv) {
            this.err = err;
            this.argv = argv;
        }

        void main() {
            final int nArgs = argv.length;
            if (nArgs == 0) {
                usage(null);
            }

            int argc = 0;
            while (argc < nArgs) {
                final String thisArg = argv[argc++];

                if (("-store").equals(thisArg)) {
                    storeName = getStringArg(thisArg, argv, argc++);
                } else if (("-host").equals(thisArg)) {
                    hostName = getStringArg(thisArg, argv, argc++);
                } else if (("-port").equals(thisArg)) {
                    portName = getStringArg(thisArg, argv, argc++);
                } else if (("-secureproxy").equals(thisArg)) {
                    String value = getStringArg(thisArg, argv, argc++);
                    if ("true".equalsIgnoreCase(value)) {
                        proxyProtocol = "https";
                    } else if ("false".equalsIgnoreCase(value)) {
                        proxyProtocol = "http";
                    } else {
                        usage("-secureproxy value not recognized: " + value);
                    }
                } else if (("-proxyhost").equals(thisArg)) {
                    proxyHostName = getStringArg(thisArg, argv, argc++);
                } else if (("-proxyport").equals(thisArg)) {
                    proxyPortNum = getStringArg(thisArg, argv, argc++);
                } else if (("-proxyuser").equals(thisArg)) {
                    proxyUserName = getStringArg(thisArg, argv, argc++);
                } else if (("-proxypass").equals(thisArg)) {
                    proxyUserPass = getStringArg(thisArg, argv, argc++);
                } else if (("-ociConfigPath").equals(thisArg)) {
                    ociConfigPath = getStringArg(thisArg, argv, argc++);
                } else if (("-ociConfigProfile").equals(thisArg)) {
                    ociConfigProfile = getStringArg(thisArg, argv, argc++);
                } else if (("-principalType").equals(thisArg)) {
                    principalType = PrincipalType.valueOf(
                            getStringArg(thisArg, argv, argc++)
                                    .toUpperCase());
                } else if (("-tenantOcid").equals(thisArg)) {
                    tenantOcid = getStringArg(thisArg, argv, argc++);
                } else if (("-numpartitions").equals(thisArg)) {
                    numPartitions = getIntArg(thisArg, argv, argc++);
                    if (numPartitions <= 0) {
                        usage("Invalid -numpartitions value: " + numPartitions +
                              ", should be greater than 0");
                    }
                } else if ("-admin-host".equals(thisArg)) {
                    adminHostName = getStringArg(thisArg, argv, argc++);
                } else if ("-admin-port".equals(thisArg)) {
                    adminPortName = getStringArg(thisArg, argv, argc++);
                } else if (("-phase").equals(thisArg)) {
                    final String val = getStringArg(thisArg, argv, argc++);
                    try {
                        phase = Phase.valueOf(val.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        usage("-phase value not recognized: " + val);
                    }
                } else if (("-start").equals(thisArg)) {
                    startBlocks = getLongArg(thisArg, argv, argc++);
                } else if (("-count").equals(thisArg)) {
                    countBlocks = getLongArg(thisArg, argv, argc++);
                } else if (("-threads").equals(thisArg)) {
                    threads = getIntArg(thisArg, argv, argc++);
                } else if (("-consistency").equals(thisArg)) {
                    final String val = getStringArg(thisArg, argv, argc++);
                    try {
                        consistency =
                            ConsistencyArgument.parseConsistency(val);
                    } catch (IllegalArgumentException e) {
                        usage("-consistency value not recognized: " + val);
                    }
                } else if (("-durability").equals(thisArg)) {
                    final String val = getStringArg(thisArg, argv, argc++);
                    try {
                        durability = DurabilityArgument.parseDurability(val);
                    } catch (IllegalArgumentException e) {
                        usage("-durability value not recognized: " + val);
                    }
                } else if (("-requestLimitConfig").equals(thisArg)) {
                    final String val = getStringArg(thisArg, argv, argc++);
                    try {
                        requestLimit = RequestLimitConfigArgument.
                            parseRequestLimitConfig(val);
                    } catch (IllegalArgumentException e) {
                        usage("-requestLimitConfig value not recognized: " +
                              val);
                    }
                } else if (("-requestTimeout").equals(thisArg)) {
                    requestTimeout = getLongArg(thisArg, argv, argc++);
                } else if (("-blockTimeout").equals(thisArg)) {
                    blockTimeout = getLongArg(thisArg, argv, argc++);
                } else if (("-lobChunkTimeout").equals(thisArg)) {
                    lobChunkTimeout = getLongArg(thisArg, argv, argc++);
                } else if (("-lobOpTimeout").equals(thisArg)) {
                    lobOpTimeout = getLongArg(thisArg, argv, argc++);
                } else if (thisArg.equals("-lobPercent")) {
                    lobPercent = getDoubleArg(thisArg, argv, argc++);
                } else if (("-reportingInterval").equals(thisArg)) {
                    reportingInterval = getIntArg(thisArg, argv, argc++);
                } else if (("-partitions").equals(thisArg)) {
                    partitionsString = getStringArg(thisArg, argv, argc++);
                    final String[] values = partitionsString.split(",");
                    partitions = new int[values.length];
                    try {
                        for (int i = 0; i < partitions.length; i++) {
                            partitions[i] = Integer.parseInt(values[i]);
                        }
                    } catch (IllegalArgumentException e) {
                        usage("-partitions value not recognized: " +
                              partitionsString);
                    }
                } else if (("-readPercent").equals(thisArg)) {
                    readPercent = getDoubleArg(thisArg, argv, argc++);
                } else if (("-maxThroughput").equals(thisArg)) {
                    maxThroughput = getDoubleArg(thisArg, argv, argc++);
                } else if (("-maxExecutionTime").equals(thisArg)) {
                    maxExecTime = getIntArg(thisArg, argv, argc++);
                } else if (("-exerciseNoCheck").equals(thisArg)) {
                    exerciseNoCheck = true;
                } else if (("-useParallelScan").equals(thisArg)) {
                    useParallelScan = true;
                } else if (("-scanMaxConcurrentRequests").equals(thisArg)) {
                    scanMaxConcurrentRequests = getIntArg(thisArg, argv,
                                                          argc++);
                } else if (("-scanMaxResultsBatches").equals(thisArg)) {
                    scanMaxResultsBatches = getIntArg(thisArg, argv, argc++);
                } else if (("-seed").equals(thisArg)) {
                    seed = getLongArg(thisArg, argv, argc++);
                    if (seed == -1) {
                        usage("-seed value cannot be -1");
                    }
                } else if (thisArg.equals("-table")) {
                    useKv = false;
                    useTable = true;
                    final String indexFlag = "-indexReadPercent";
                    if (argc < nArgs && argv[argc].equals(indexFlag)) {
                        argc++;
                        indexReadPercent = getIntArg(indexFlag, argv, argc++);
                    }
                } else if (thisArg.equals("-kv")) {
                    useKv = true;
                    useTable = false;
                } else if (thisArg.equals("-proxy")) {
                    useProxy = true;
                    proxyType = ProxyType.valueOf(
                        getStringArg(thisArg, argv, argc++).toUpperCase());
                } else if (thisArg.equals("-syncMode")) {
                    syncMode = SyncMode.valueOf(
                        getStringArg(thisArg, argv, argc++));
                } else if (thisArg.equals("-useTTL")) {
                    String value = getStringArg(thisArg, argv, argc++);
                    if ("true".equalsIgnoreCase(value)) {
                        useTTL = true;
                    } else if ("false".equalsIgnoreCase(value)) {
                        useTTL = false;
                    } else {
                        usage("-useTTL value not recognized: " + value);
                    }
                } else if (("-verbose").equals(thisArg)) {
                    verbose++;
                } else if (thisArg.equals("-exitOnUnexpectedException")) {
                    DataCheck.EXIT_ON_UNEXPECTED_EXCEPTION = true;
                } else if (thisArg.equals("-exitOnUnexpectedResult")) {
                    DataCheck.EXIT_ON_UNEXPECTED_RESULT = true;
                } else if (thisArg.startsWith("-dropTable")) {
                    useTable = true;
                    if (dropTableParams == null) {
                        dropTableParams = new DataCheckDropTable.InitParams();
                    }
                    if (thisArg.length() > "-dropTable".length()) {
                        dropTableParams.parseArg(thisArg, argv, argc++);
                    }
                } else if (("-dcTableReadUnits").equals(thisArg)) {
                    dcTableReadUnits = getIntArg(thisArg, argv, argc++);
                    if (dcTableReadUnits < 1) {
                        usage("-dcTableReadUnits must be greater than 1.");
                    }
                } else if (("-dcTableWriteUnits").equals(thisArg)) {
                    dcTableWriteUnits = getIntArg(thisArg, argv, argc++);
                    if (dcTableWriteUnits < 1) {
                        usage("-dcTableWriteUnits must be greater than 1.");
                    }
                } else if (("-dcTableStorageGB").equals(thisArg)) {
                    dcTableStorageGB = getIntArg(thisArg, argv, argc++);
                    if (dcTableStorageGB < 1) {
                        usage("-dcTableStorageGB must be greater than 1.");
                    }
                }
                else if (("-writeMultipleMax").equals(thisArg)) {
                    writeMultipleMax = getIntArg(thisArg, argv, argc++);
                    if (writeMultipleMax < 1) {
                        usage("-writeMultipleMax must be greater than 1 " +
                              "for running on the cloud.");
                    }
                } else if (("-useDcTableId").equals(thisArg)) {
                    String value = getStringArg(thisArg, argv, argc++);
                    if ("true".equalsIgnoreCase(value)) {
                        useDcTableId = true;
                    } else if ("false".equalsIgnoreCase(value)) {
                        useDcTableId = false;
                    } else {
                        usage("-useDcTableId value not recognized: " + value +
                              ". Must be true or false");
                    }
                } else if (("-dropCloudTableBefore").equals(thisArg)) {
                    String value = getStringArg(thisArg, argv, argc++);
                    if ("true".equalsIgnoreCase(value)) {
                        dropCloudTableBefore = true;
                    } else if ("false".equalsIgnoreCase(value)) {
                        dropCloudTableBefore = false;
                    } else {
                        usage("-dropCloudTableBefore value not recognized: " +
                              value + ". Must be true or false");
                    }
                } else if (("-dropCloudTableAfter").equals(thisArg)) {
                    String value = getStringArg(thisArg, argv, argc++);
                    if ("true".equalsIgnoreCase(value)) {
                        dropCloudTableAfter = true;
                    } else if ("false".equalsIgnoreCase(value)) {
                        dropCloudTableAfter = false;
                    } else {
                        usage("-dropCloudTableAfter value not recognized: " +
                              value + ". Must be true or false");
                    }
                } else {
                    usage("Unknown argument: " + thisArg);
                }
            }

            if (storeName == null) {
                usage("-store must be specified");
            }
            if (proxyType != ProxyType.CLOUD) {
                if (hostName == null) {
                    usage("-host must be specified");
                } else if (portName == null) {
                    usage("-port must be specified");
                }
            }

            if (useProxy) {
                if (useKv) {
                    usage("-kv is not supported with -proxy");
                }
                useTable = true;
                if (proxyHostName == null) {
                    usage("-proxyhost is required with -proxy");
                } else if (proxyPortNum == null &&
                           proxyType != ProxyType.CLOUD) {
                    usage("-proxyport is required with -proxy");
                } else if (numPartitions == 0) {
                    usage("-numpartitions is required with -proxy");
                }

                if (proxyProtocol.equals("https")) {
                    if (proxyType == ProxyType.ONPREM) {
                        if (proxyUserName == null) {
                            usage("-proxyuser is required with -proxy ONPREM" +
                                    " and -secureproxy");
                        } else if (proxyUserPass == null) {
                            usage("-proxypass is required with -proxy ONPREM" +
                                    " and -secureproxy");
                        }
                    }
                    /* use cloud proxy */
                    else {
                        /* check valid authorization */
                        if (principalType == PrincipalType.USER) {
                            if (ociConfigPath == null) {
                                usage("-ociConfigPath is required with " +
                                        "-proxy CLOUD, -secureproxy " +
                                        "and -principalType USER");
                            } else if (ociConfigProfile == null) {
                                usage("-ociConfigProfile is required with " +
                                        "-proxy CLOUD, -secureproxy " +
                                        "and -principalType USER");
                            }
                        } else if (principalType == PrincipalType.INSTANCE) {
                            if (tenantOcid == null) {
                                usage("-tenantOcid is required with " +
                                        "-proxy CLOUD, -secureproxy " +
                                        "and -principalType INSTANCE");
                            }
                        } else if (principalType == PrincipalType.RESOURCE) {
                            usage("-principalType RESOURCE is not supported " +
                                    "yet.");
                        } else {
                            usage("-principalType [ USER | INSTANCE | " +
                                    "RESOURCE ] is required with -proxy " +
                                    "CLOUD and -secureproxy");
                        }
                        /* check if all read/write/storage units are entered */
                        if ((dcTableReadUnits == 0) ||
                            (dcTableWriteUnits == 0) ||
                            (dcTableStorageGB == 0)) {
                            usage("-dcTableReadUnits, -dcTableWriteUnits, " +
                                  "and -dcTableStorageGB are required with " +
                                  "-proxy CLOUD");
                        }
                    }
                }
            }

            asyncAPI = checkSyncMode(syncMode, useTable);

            /* By default use the registry host as admin host */
            if (adminHostName == null) {
                adminHostName = hostName;
            }

            /* By default use the registry port as admin port */
            if (adminPortName == null) {
                adminPortName = portName;
            }

            if (dropTableParams != null &&
                syncMode != SyncMode.SYNC &&
                syncMode != SyncMode.SYNC_OVER_ASYNC) {
                usage("-dropTable must not be specified when syncMode " +
                    "is not SYNC or SYNC_OVER_ASYNC");
            }

            if (countBlocks < threads || countBlocks % threads != 0) {
                usage("-count value must be a multiple of threads");
            }

            err.println
                ("Running test:" +
                 "\n -store " + storeName +
                 (hostName == null ?
                  "" :
                  "\n -host " + hostName) +
                 (portName == null ?
                  "" :
                  "\n -port " + portName) +
                 (proxyProtocol == null ?
                  "" :
                  "\n -secureproxy " +
                 (proxyProtocol == "https" ? "true" : "false")) +
                 (proxyHostName == null ?
                  "" :
                  "\n -proxyhost " + proxyHostName) +
                 (proxyPortNum == null ?
                  "" :
                  "\n -proxyport " + proxyPortNum) +
                 (proxyUserName == null ?
                  "" :
                  "\n -proxyuser " + proxyUserName) +
                 (proxyUserPass == null ?
                  "" :
                  "\n -proxypass " + proxyUserPass) +
                 (numPartitions == 0 ?
                  "" :
                  "\n -numpartitions " + numPartitions) +
                 (adminHostName == null ?
                  "" :
                  "\n -admin-host " + adminHostName) +
                 (adminPortName == null ?
                  "" :
                  "\n -admin-port " + adminPortName) +
                 "\n -phase " + phase.toString().toLowerCase() +
                 "\n -start " + startBlocks +
                 "\n -count " + countBlocks +
                 "\n -threads " + threads +
                 "\n -consistency " +
                 ConsistencyArgument.toString(consistency) +
                 "\n -durability " + DurabilityArgument.toString(durability) +
                 "\n -requestLimitConfig " +
                 RequestLimitConfigArgument.toString(requestLimit) +
                 "\n -requestTimeout " + requestTimeout +
                 "\n -blockTimeout " + blockTimeout +
                 "\n -lobChunkTimeout " + lobChunkTimeout +
                 "\n -lobOpTimeout " + lobOpTimeout +
                 "\n -lobPercent " + lobPercent +
                 "\n -reportingInterval " + reportingInterval +
                 (partitions == null ?
                  "" :
                  "\n -partitions " + partitionsString) +
                 (phase != Phase.EXERCISE ?
                  "" :
                  "\n -readPercent " + readPercent) +
                 (maxThroughput == 0 ?
                  "" :
                  "\n -maxThroughput " + maxThroughput) +
                 (maxExecTime == 0 ?
                  "" :
                  "\n -maxExecutionTime " + maxExecTime) +
                 (((phase != Phase.EXERCISE) || !exerciseNoCheck) ?
                  "" :
                  " -exerciseNoCheck") +
                 (useParallelScan ? "\n -useParallelScan" : "") +
                 "\n -scanMaxConcurrentRequests " + scanMaxConcurrentRequests +
                 "\n -scanMaxResultsBatches " + scanMaxResultsBatches +
                 (seed == -1 ?
                  "" :
                  "\n -seed " + seed) +
                 (!useTable ?
                  "\n -kv" :
                  "\n -table -indexReadPercent " + indexReadPercent) +
                 (!useProxy ?
                  "" :
                  "\n -proxy " + proxyType) +
                  "\n -syncMode " + syncMode +
                  "\n -useTTL " + useTTL +
                 (proxyType != ProxyType.CLOUD ?
                  "" :
                  "\n -principalType " + principalType +
                  (tenantOcid != null ?
                    "\n -tenantOcid " + tenantOcid :
                    "\n -ociConfigPath " + ociConfigPath +
                    "\n -ociConfigProfile " + ociConfigProfile) +
                  "\n -dcTableReadUnits " + dcTableReadUnits +
                  "\n -dcTableWriteUnits " + dcTableWriteUnits +
                  "\n -dcTableStorageGB " + dcTableStorageGB +
                  "\n -writeMultipleMax " + writeMultipleMax +
                  "\n -useDcTableId " + useDcTableId +
                  "\n -dropCloudTableBefore " + dropCloudTableBefore +
                  "\n -dropCloudTableAfter " + dropCloudTableAfter));
            
            for (int i = 0; i < verbose; i++) {
                err.println(" -verbose");
            }
            err.println(
                (!DataCheck.EXIT_ON_UNEXPECTED_EXCEPTION ?
                 "" :
                 "\n -exitOnUnexpectedException") +
                (!DataCheck.EXIT_ON_UNEXPECTED_RESULT ?
                 "" :
                 "\n -exitOnUnexpectedResult") +
                (dropTableParams == null ?
                 "" :
                 dropTableParams));

            Thread.setDefaultUncaughtExceptionHandler(
                    new UncaughtExceptionHandler(err));

            KVStoreConfig config = null;
            KVStore store = null;

            if (!useProxy || proxyType == ProxyType.ONPREM) {
                config =
                    new KVStoreConfig(storeName, hostName + ":" + portName);
                config.setConsistency(consistency);
                config.setDurability(durability);
                config.setRequestTimeout(requestTimeout, MILLISECONDS);
                config.setLOBTimeout(lobChunkTimeout, MILLISECONDS);
                config.setSocketReadTimeout(requestTimeout, MILLISECONDS);
                config.setRequestLimit(requestLimit);
                fixConfig(config, syncMode);
                store = KVStoreFactory.getStore(config);
            }

            final DataCheck<?, ?, ?> dc;

            final StoreAndConfig secondPair =
                getSecondStoreAndConfig(config, syncMode);

            DataCheck<Key, Value, Operation> kVDc = null;
            DataCheck<Key, Value, Operation> secondKVDc = null;
            DataCheck<PrimaryKey, Row, TableOperation> tableDc = null;
            DataCheck<PrimaryKey, Row, TableOperation> secondTableDc = null;

            if (!useTable) {
                /* syncMode == SYNC, SYNC_OVER_ASYNC, SYNC_OVER_BOTH */
                kVDc = createDataCheckKV(store, config);
                dc = kVDc;

                if (secondPair != null) {
                    /* syncMode == SYNC_OVER_BOTH */
                    secondKVDc = createDataCheckKV(secondPair.store,
                                                   secondPair.config);
                }
            } else if (dropTableParams != null) {
                dc = new DataCheckDropTable(
                    store, config, reportingInterval, seed, verbose, err,
                    useParallelScan, scanMaxConcurrentRequests,
                    scanMaxResultsBatches, indexReadPercent, dropTableParams);
            } else if (useProxy) {
                if (proxyType == ProxyType.CLOUD) {
                    dcTableLimits = new TableLimits(dcTableReadUnits,
                                                    dcTableWriteUnits,
                                                    dcTableStorageGB);
                }
                dc = createDataCheckTableProxy(store, config);
            } else {
                if (syncMode == SyncMode.ASYNC) {
                    /* syncMode == ASYNC */
                    tableDc = createDataCheckTableAsync(store, config);
                } else {
                    /*
                     * syncMode == SYNC, SYNC_OVER_ASYNC, BOTH_OVER_ASYNC,
                     * SYNC_OVER_BOTH, SYNC_AND_ASYNC
                     */
                    tableDc = createDataCheckTable(store, config);
                }
                dc = tableDc;

                if (syncMode == SyncMode.BOTH_OVER_ASYNC ||
                    syncMode == SyncMode.SYNC_AND_ASYNC) {
                        /* syncMode == BOTH_OVER_ASYNC, SYNC_AND_ASYNC */
                    secondTableDc = createDataCheckTableAsync(
                        secondPair.store, secondPair.config);
                } else if (syncMode == SyncMode.SYNC_OVER_BOTH) {
                    /* syncMode = SYNC_OVER_BOTH */
                    secondTableDc = createDataCheckTable(
                        secondPair.store, secondPair.config);
                }
            }

            final long count = countBlocks * DataCheck.BLOCK_COUNT;
            final long start = startBlocks * DataCheck.BLOCK_COUNT;
            switch (phase) {
            case POPULATE:
                dc.populate(start, count, threads);
                break;
            case EXERCISE:
                if (kVDc != null) {
                    kVDc.exercise(start, count, threads, readPercent,
                                  exerciseNoCheck, secondKVDc);
                } else if (tableDc != null) {
                    tableDc.exercise(start, count, threads, readPercent,
                                     exerciseNoCheck, secondTableDc);
                } else {
                    dc.exercise(start, count, threads, readPercent,
                                exerciseNoCheck);
                }
                break;
            case CHECK:
                dc.check(start, count, threads);
                break;
            case CLEAN:
                dc.clean(start, count, threads);
                break;
            default:
                throw new AssertionError();
            }
        }

        DataCheck<Key, Value, Operation> createDataCheckKV(KVStore s,
                                                           KVStoreConfig c) {
            return new DataCheckKV(
                s, c, reportingInterval, partitions, seed, verbose, err,
                blockTimeout, lobOpTimeout, lobPercent, useParallelScan,
                scanMaxConcurrentRequests, scanMaxResultsBatches,
                maxThroughput, maxExecTime);
        }

        DataCheck<PrimaryKey, Row, TableOperation>
            createDataCheckTable(KVStore s, KVStoreConfig c) {
            return new DataCheckTableDirect(
                s, c, reportingInterval, partitions, seed, verbose, err,
                blockTimeout, useParallelScan, scanMaxConcurrentRequests,
                scanMaxResultsBatches, indexReadPercent,
                maxThroughput, maxExecTime,
                useTTL, adminHostName, adminPortName);
        }

        DataCheck<PrimaryKey, Row, TableOperation>
            createDataCheckTableAsync(KVStore s, KVStoreConfig c) {
            return new DataCheckTableAsync(
                s, c, reportingInterval, partitions, seed, verbose, err,
                blockTimeout, useParallelScan, scanMaxConcurrentRequests,
                scanMaxResultsBatches, indexReadPercent,
                maxThroughput, maxExecTime,
                useTTL, adminHostName, adminPortName);
        }

        DataCheck<MapValue, MapValue, Request>
            createDataCheckTableProxy(KVStore s,
                                      KVStoreConfig c) {
            return new DataCheckTableProxy(
                proxyType, proxyProtocol, proxyHostName, proxyPortNum,
                proxyUserName, proxyUserPass,
                principalType, tenantOcid,
                ociConfigPath, ociConfigProfile,
                s, c, reportingInterval, partitions, seed, verbose, err,
                requestTimeout, blockTimeout, useParallelScan,
                scanMaxConcurrentRequests, scanMaxResultsBatches,
                indexReadPercent, maxThroughput, maxExecTime,
                useTTL, threads, numPartitions,
                dcTableLimits, writeMultipleMax, useDcTableId,
                dropCloudTableBefore, dropCloudTableAfter,
                (phase == Phase.POPULATE));
        }
    }

    private static String getStringArg(String arg, String[] argv, int argc) {
        if (argc >= argv.length) {
            usage(arg + " requires an argument");
        }
        return argv[argc];
    }

    static int getIntArg(String arg, String[] argv, int argc) {
        final String val = getStringArg(arg, argv, argc);
        try {
            return Integer.parseInt(val);
        } catch (IllegalArgumentException e) {
            throw new UsageException(arg + " value not recognized: " + val);
        }
    }

    static long getLongArg(String arg, String[] argv, int argc) {
        final String val = getStringArg(arg, argv, argc);
        try {
            return Long.parseLong(val);
        } catch (IllegalArgumentException e) {
            throw new UsageException(arg + " value not recognized: " + val);
        }
    }

    static boolean getBooleanArg(String arg, String[] argv, int argc) {
        final String val = getStringArg(arg, argv, argc);
        try {
            return Boolean.parseBoolean(arg);
        } catch (IllegalArgumentException e) {
            throw new UsageException(arg + " value not recognized: " + val);
        }
    }

    private static double getDoubleArg(String arg, String[] argv, int argc) {
        String val = getStringArg(arg, argv, argc);
        try {
            return Double.parseDouble(val);
        } catch (IllegalArgumentException e) {
            throw new UsageException(arg + " value not recognized: " + val);
        }
    }

    static void usage(String message) {
        throw new UsageException(message);
    }

    private static class UsageException extends RuntimeException {
        private static final long serialVersionUID = 1;
        UsageException(String message) {
            super((message == null) ?
                  USAGE :
                  ("Error: " + message + "\n" + USAGE));
        }
    }

    private static class UncaughtExceptionHandler implements
        Thread.UncaughtExceptionHandler {
        private PrintStream err;

        UncaughtExceptionHandler(PrintStream err) {
            this.err = err;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            err.printf("Exiting test due to uncaught exception " +
                    "thrown by thread [%s]:\n",
                    t.getName());
            e.printStackTrace(err);
            System.exit(1);
        }
    }
}
