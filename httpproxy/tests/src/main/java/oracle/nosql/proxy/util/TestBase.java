package oracle.nosql.proxy.util;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import oracle.nosql.util.HttpRequest;
import oracle.nosql.util.HttpResponse;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.tmi.TenantLimits;

import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * The base class for all spartakv unit tests.
 */
public abstract class TestBase {

    /**
     * root directory of the store
     */
    private final static String ROOT_DIR = "kvroot";

    /**
     * store name
     */
    private final static String STORE_NAME = "kvstore";

    /**
     * host name
     */
    private final static String HOSTNAME = "localhost";

    /**
     * port number of the store
     */
    private final static int KV_PORT = 13250;

    /**
     * number of the partitions
     */
    private final static int NUM_PARTITIONS = 10;

    /**
     * HA port range
     */
    private final static String HA_PORT_RANGE = "13255,13260";

    /**
     * port range
     */
    private final static String PORT_RANGE = "13255,13260";

    /**
     * system property pointing to the test directory
     */
    private final static String TEST_DIR_PROP = "testsandbox";

    /**
     * system property pointing to the directory stores logs of failure tests
     */
    private final static String FAILURE_DIR = "failurecopydir";

    /**
     * default directory stores the logs of failure tests
     */
    private final static String DEFAULT_FAIL_DIR = "build/failures";

    private final static boolean TEST_TRACE = Boolean.getBoolean("test.trace");

    protected static String scHost;
    protected static Integer scPort;
    protected static String scUrlBase;
    protected static String scTierBase;
    protected static String scDSConfigBase;
    static {
        doStaticSetup();
    }

    /**
     * Set up URLs for talking to the SC to create and delete test tiers
     * and tenants. This allows tests to modify default TenantLimits.
     */
    protected static void doStaticSetup() {
        scHost = System.getProperty("sc.host");
        if (scHost != null) {
            scPort = Integer.parseInt(System.getProperty("sc.port", "13600"));
            scUrlBase = "http://" + scHost + ":" + scPort + "/V0/service/";
            scTierBase = scUrlBase + "tier/";
            scDSConfigBase = scUrlBase + "dsconfig/";
        }
    }

    /**
     * The rule we use to control every test case, this rule is primarily to
     * copy the testing environment, files, sub directories to another place
     * for future investigation, if any of test failed.
     */
    @Rule
    public TestRule watchman = new TestWatcher() {

        /* Copy Environments when the test failed. */
        @Override
        protected void failed(Throwable t, Description desc) {
            String dirName = makeFileName(desc);
            try {
                copyEnvironments(dirName);
            } catch (Exception e) {
                throw new RuntimeException(
                   "Can't copy env dir to " + " after failure", e);
            }
        }

        @Override
        protected void succeeded(Description desc){
        }

        @Override
        protected void starting(Description description) {
            if (TEST_TRACE) {
                System.out.println("Starting test: " +
                                   description.getMethodName());
            }
        }
    };

    /** Provides the name of the current test. */
    @Rule
    public final TestName testName = new TestName();

    /**
    * Copy the testing directory to other place.
    */
    protected static void copyEnvironments(String path) throws Exception{
        File failureDir = getFailureCopyDir();

        if (failureDir == null || failureDir.list() == null) {
            return;
        }

        /* If the testsandbox is not set, do not copy. */
        if (System.getProperty(TEST_DIR_PROP) == null ||
            System.getProperty(TEST_DIR_PROP).length() == 0) {
            return;
        }

        copyDir(getTestDirFile(), new File(failureDir, path));
    }

    /**
     * Allow to set up self defined directory store failure copy.
     */
    private static File getFailureCopyDir() {
        String dir = System.getProperty(FAILURE_DIR, DEFAULT_FAIL_DIR);
        File file = new File(dir);
        if (!file.isDirectory()) {
            file.mkdir();
        }

        return file;
    }

    /**
     * get the testing directory
     */
    private static File getTestDirFile() {
        String dir = System.getProperty(TEST_DIR_PROP);
        if (dir == null || dir.length() == 0) {
            throw new IllegalArgumentException
                ("System property must be set to test data directory: " +
                 TEST_DIR_PROP);
        }

        return new File(dir);
    }

    /**
     * Copy everything in test destination directory to another place for
     * future evaluation when test failed.
     */
    private static void copyDir(File fromDir, File toDir) throws IOException {

        if (fromDir == null || toDir == null) {
            throw new NullPointerException("File location error");
        }

        if (!fromDir.isDirectory()) {
            throw new IllegalStateException(fromDir +  " should be a directory");
        }

        if (!fromDir.exists()) {
            throw new IllegalStateException(fromDir +  " does not exist");
        }

        if (!toDir.exists() && !toDir.mkdirs()) {
            throw new IllegalStateException("Unable to create copy dest dir:" +
                                            toDir);
        }

        File [] fileList = fromDir.listFiles();
        if (fileList != null && fileList.length != 0) {
            for (File file : fileList) {
                if (file.isDirectory()) {
                    copyDir(file, new File(toDir, file.getName()));
                } else {
                    copyFile(file, new File(toDir, file.getName()));
                }
            }
        }
    }

    /**
     * Copy a file
     * @param sourceFile the file to copy from, which must exist
     * @param destFile the file to copy to.  The file is created if it does
     *        not yet exist.
     */
    private static void copyFile(File sourceFile, File destFile)
        throws IOException {

        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        try (final FileInputStream source = new FileInputStream(sourceFile);
             final FileOutputStream dest = new FileOutputStream(destFile)) {
             final FileChannel sourceChannel = source.getChannel();
             dest.getChannel().transferFrom(sourceChannel, 0,
                                            sourceChannel.size());
        }
    }

    /**
     * Get failure copy directory name.
     */
    private String makeFileName(Description desc) {
        String name = desc.getClassName() + "-" + desc.getMethodName();
        return name;
    }

    /**
     * get the string of testing directory
     */
    protected static String getTestDir() {
        String dir = System.getProperty(TEST_DIR_PROP);
        if (dir == null) {
            fail("System property \"testsandbox\" must be set");
        }
        return dir;
    }

    /**
     * remove all files and directories from the test instance directory.
     */
    public static void cleanupTestDir() {
        File testDir = new File(getTestDir());
        if (!testDir.exists()) {
            return;
        }
        clearDirectory(testDir);
    }

    /**
     * clears out the contents of the directory, recursively
     */
    public static void clearDirectory(File dir) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                clearDirectory(file);
            }
            boolean deleteDone = file.delete();
            assert deleteDone: "Couldn't delete " + file;
        }
    }

    /**
     * return the name of the root directory
     */
    public static String getRootDir() {
        return ROOT_DIR;
    }

    /**
     * return the store name
     */
    public static String getStoreName() {
        return STORE_NAME;
    }

    /**
     * return the host name
     */
    public static String getHostName() {
        return HOSTNAME;
    }

    /**
     * return the port number of the store
     */
    public static int getKVPort() {
        return KV_PORT;
    }

    /**
     * return the number of the partitions of the store
     */
    public static int getNumPartitions() {
        return NUM_PARTITIONS;
    }

    /**
     * return the HA port range of the store
     */
    public static String getHAPortRange() {
        return HA_PORT_RANGE;
    }

    /**
     * return the port range of the store
     */
    public static String getPortRange() {
        return PORT_RANGE;
    }

    /**
     * return the name of the system property that
     * points to the testing directory
     */
    public static String getTestDirProp() {
        return TEST_DIR_PROP;
    }

    /*
     * Add a tier
     */
    protected static void addTier(String tenantId, TenantLimits limits) {
        if (scTierBase == null) {
            return;
        }

        final String tierUrl = scTierBase + tenantId;

        HttpRequest httpRequest = new HttpRequest().disableRetry();
        HttpResponse response =
            httpRequest.doHttpPost(tierUrl, JsonUtils.print(limits));
        if (200 != response.getStatusCode()) {
            fail("addTier failed: " + response);
        }
    }

    /*
     * Delete a tier
     */
    protected static void deleteTier(String tenantId) {
        if (scTierBase == null) {
            return;
        }

        final String tierUrl = scTierBase + tenantId;

        HttpRequest httpRequest = new HttpRequest().disableRetry();
        HttpResponse response = httpRequest.doHttpDelete(tierUrl, null);
        /* allow 404 -- not found -- in this path */
        if (response.getStatusCode() != 200 &&
            response.getStatusCode() != 404) {
            fail("deleteTier failed: " + response);
        }
    }
}
