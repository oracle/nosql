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

package oracle.kv.xregion;

import static java.lang.Long.parseLong;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.KVStoreMain;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.xregion.service.JsonConfig;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.impl.xregion.service.XRegionService;
import oracle.nosql.common.contextlogger.LogFormatter;

import com.sleepycat.je.log.FileManager;

/**
 * Object represents the entry point of cross-region service agent.
 */
public class XRService {

    /**
     * max number of logging files
     */
    private static final int LOG_FILE_LIMIT_COUNTS =
        Integer.getInteger("oracle.kv.xregion.logfile.count", 20);
    /**
     * size limit of each logging file in bytes
     */
    private static final int LOG_FILE_LIMIT_BYTES =
        Integer.getInteger("oracle.kv.xregion.logfile.limit",
                           100 * 1024 * 1024);
    /**
     * failure injection for test to kill process
     */
    public static final String KILL_PROCESS_VAR_TEST =
        "oracle.kv.xregion.test.killProcessTest";
    private static final boolean KILL_PROCESS_TEST =
            Boolean.getBoolean(KILL_PROCESS_VAR_TEST);
    /**
     * failure injection for test to delete status file
     */
    public static final String REMOVE_STATUS_VAR_TEST =
        "oracle.kv.xregion.test.removeStatusTest";
    private static final boolean REMOVE_STATUS_TEST =
            Boolean.getBoolean(REMOVE_STATUS_VAR_TEST);

    /**
     * Test only, wait time in seconds in killing process
     */
    private static final int TEST_WAIT_KILL_SECS = 10;

    /* retry interval in ms if host region is not reachable */
    private final static int UNREACHABLE_HOST_REGION_RETRY_MS = 6 * 1000;

    /* Time out value of 30000 millisecond */
    private static final long TIMEOUT_MS = 30 * 1000;

    /* Interval value of 1000 millisecond */
    private static final long INTV_MS = 1000;

    /* Wait value for stopping agent forcefully */
    private static final long WAIT_TIME_MS = 10;

    private final static String DEFAULT_LOG_DIR = "log";
    private final static String LOG_FILE_SUFFIX = ".log";
    private final static String LOG_FILE_NAME_SPLITTER = ".";
    private final static String LOCK_FILE_SUFFIX = ".lck";
    private final static String PID_FILE_SUFFIX = ".pid";

    /* External commands, for "java -jar" usage. */
    public static final String START_COMMAND_NAME = "xrstart";
    public static final String START_COMMAND_DESC = "start cross-region " +
                                                    "(xregion) service";
    public static final String STATUS_COMMAND_NAME = "xrstatus";
    public static final String STATUS_COMMAND_DESC = "status of cross-region" +
                                                     "(xregion) service";
    public static final String STOP_COMMAND_NAME = "xrstop";
    public static final String STOP_COMMAND_DESC = "stop cross-region " +
                                                   "(xregion) service";

    private static final String STOP_FORCE_FLAG = "-force";
    private static final String CONFIG_FLAG = "-config";
    private static final String START_BG_FLAG = "-bg";
    public static final String START_COMMAND_ARGS =
        CommandParser.optional(CONFIG_FLAG + " <JSON config file>") + " " +
        CommandParser.optional(START_BG_FLAG);
    public static final String STATUS_COMMAND_ARGS =
        CommandParser.optional(CONFIG_FLAG + " <JSON config file>");
    public static final String STOP_COMMAND_ARGS =
        CommandParser.optional(CONFIG_FLAG + " <JSON config file>") + " " +
        CommandParser.optional(STOP_FORCE_FLAG);

    /* rate limiting log period in ms */
    private static final int RL_LOG_PERIOD_MS = 10 * 1000;

    /* list of arguments */
    private final String[] args;

    /* status file name base and extension  */
    private static String STATUS_FILE_NAME_BASE = "status";
    private static String STATUS_FILE_NAME_EXT = "txt";

    /* JSON configuration file for bootstrap */
    private String json;

    /*
     * false if stop the agent after pending requests are done and
     * checkpoint of each stream is done, true if immediately stop the agent,
     * the default is false
     */
    private boolean force = false;
    /*
     * true if the service is running in background, true otherwise
     */
    private boolean background = false;

    /* lock file manager */
    private final LockFileManager lockMan;

    /* json config */
    private final JsonConfig conf;

    /* command */
    private String command;

    /* logger */
    private Logger logger;

    /* rate limiting logger */
    private final RateLimitingLogger<String> rlLogger;

    /* status file name */
    private final String statusFileName;

    /**
     * Parsing status of agent:
     * Success: agent successfully started
     * Failure: agent failed to start
     * Duplicate: another agent already running
     * Crashed: agent started but eventually stopped working
     */
    public enum StartParseStatus {
        SUCCESS, FAILURE, DUPLICATE, CRASHED;
    }

    interface ExitCode {
        int getCode();
        String getMsg();
        default boolean isSuccessful() {
            return false;
        }
    }

    /**
     * Exit code for command starting agent in bg:
     * Success: agent successfully started
     * Failed: agent failed to start
     * Duplicate: another agent already running
     * Crashed:  agent start but eventually stopped working
     * Timeout: no status file found even though agent started
     */
    public enum StartExitCode implements ExitCode {
        SUCCESS(0, "Started successfully") {
            @Override
            public boolean isSuccessful() {
                return true;
            }
        },

        FAILED(1, "Failed to start"),
        TIMEOUT(2, "Failed with timeout"),
        CRASHED(3, "Crashed"),
        DUPLICATE(4, "Already running");

        final int code;
        final String msg;
        StartExitCode(int code, String msg) {
            this.code = code;
            this.msg = msg;
        }
        @Override
        public int getCode(){
            return code;
        }
        @Override
        public String getMsg(){
            return msg;
        }
    }

    /**
     * Exit code for checking agent status:
     * Success is agent successfully running
     * Failed is agent not running
     * Crashed is agent start but eventually stopped working
     */
    public enum StatusExitCode implements ExitCode {
        SUCCESS(0, "Agent running") {
            @Override
            public boolean isSuccessful() {
                return true;
            }
        },
        FAILED(1, "Agent not running"),
        CRASHED(2, "Agent crashed");
        final int code;
        final String msg;
        StatusExitCode(int code, String msg) {
            this.code = code;
            this.msg = msg;
        }
        @Override
        public int getCode(){
            return code;
        }
        @Override
        public String getMsg(){
            return msg;
        }
    }

    /**
     * Exit code for stopping agent:
     * Stop: stopped successfully
     * Nonstop: not stopped successfully
     */
    public enum StopExitCode implements ExitCode {
        STOP(0, "Stopped") {
            @Override
            public boolean isSuccessful() {
                return true;
            }
        },
        NONSTOP(1, "Failed to stop");
        final int code;
        final String msg;
        StopExitCode(int code, String msg) {
            this.code = code;
            this.msg = msg;
        }
        @Override
        public int getCode(){
            return code;
        }
        @Override
        public String getMsg(){
            return msg;
        }
    }

    private XRService(final String[] args) throws IOException {
        this.args = args;
        parseArgs();

        try {
            conf = JsonConfig.readJsonFile(json, logger);
            statusFileName = getStatusFileName(conf);
        } catch (Exception exp) {
            final String err = "cannot parse the configuration file " + json +
                               ", " + exp.getMessage();
            throw new IllegalArgumentException(err, exp);
        }

        try {
            logger = getServiceLogger(conf);
            rlLogger = new RateLimitingLogger<>(RL_LOG_PERIOD_MS, 8, logger);
        } catch (IOException ioe) {
            final String err = "cannot create logger for region=" +
                               conf.getRegion() + ", " + ioe.getMessage();
            throw new IllegalStateException(err, ioe);
        }
        lockMan = new LockFileManager(conf);

        /* dump the json config with parameters in log file */
        logger.info(lm("Run XRegion Service with command=" + command +
                       ", configuration=" + json +
                       ", status file=" + statusFileName +
                       ", argument list=" + Arrays.toString(args)));
    }

    public static void main(String[] args) {
        try {
            final XRService xrs = new XRService(args);
            final ExitCode result = xrs.run();
            if (result.isSuccessful()) {
                System.out.println(result.getMsg());
            } else{
                System.err.println(result.getMsg());
            }
            System.exit(result.getCode());
        } catch (Exception exp) {
            System.err.println("Error in executing command=" +
                               args[args.length - 1] +
                               " for cross-region service, " +
                               exp.getMessage());
            System.exit(StartExitCode.FAILED.getCode());
        }
    }

    /**
     * Builds the agent id
     *
     * @param conf json config
     * @return agent id
     */
    public static String buildAgentId(JsonConfig conf) {
        return conf.getRegion() + LOG_FILE_NAME_SPLITTER +
               conf.getAgentGroupSize() + LOG_FILE_NAME_SPLITTER +
               conf.getAgentId();
    }

    @Override
    public String toString() {
        return "command=" + command +
               ", json=" + json +
               (command.equals(STOP_COMMAND_ARGS) ? ", force=" + force : "");
    }

    /*---------------------*
     * Private Functions   *
     *---------------------*/
    private static void usage(final String message) {
        if (message != null) {
            System.err.println("\n" + message + "\n");
        }
        System.err.println("Usage: + XRService");
        System.err.println("\t[ xrstart | xrstop | xrstatus ] " +
                           "-config <JSON config file> [ -bg ] [ -force ]");
        System.exit(1);
    }

    private static void usageStop() {
        System.err.println("Usage: + XRService");
        System.err.println("\t[ xrstop ]" +
                           "-config <JSON config file> [ -force ]");
        System.exit(1);
    }

    private static void usageStatus() {
        System.err.println("Usage: + XRService");
        System.err.println("\t[ xrstatus ]" +
                           "-config <JSON config file>");
        System.exit(1);
    }

    private static String[] heapSizeFinder(String jsonPath) {
        JsonConfig configure;
        try {
            configure = JsonConfig.readJsonFile(jsonPath, null);
        } catch (Exception exp) {
            final String err = "Cannot parse the configuration file at path=" +
                               jsonPath + ", error=" + exp.getMessage();
            throw new IllegalArgumentException(err, exp);
        }
        final String xms = ("-Xms" + configure.getBgInitHeapSizeMB() + "m");
        final String xmx = ("-Xmx" + configure.getBgMaxHeapSizeMB() + "m");
        return new String[] {xms, xmx};
    }

    private static String getStatusFileName(JsonConfig config) {
        /* status file name: e.g., status.2.0.txt, status.2.1.txt, etc. */
        return STATUS_FILE_NAME_BASE + "." + config.getAgentGroupSize() +
               "." + config.getAgentId() + "." + STATUS_FILE_NAME_EXT;
    }

    /**
     * Method to check if status file is present with timeout
     * */
    private boolean statusFilePresent(String fileDir) throws
            InterruptedException {
        final File file = new File(fileDir, statusFileName);
        final long stopTime = System.currentTimeMillis() + TIMEOUT_MS;
        String fileString = file.toString();
        assert testDeleteStatus(fileString);
        if (!file.exists()) {
            while (true) {
                final long wait = stopTime - System.currentTimeMillis();
                if (wait <= 0) {
                    break;
                }
                /* for testing only */
                assert testDeleteStatus(fileString);
                if (file.exists()) {
                    return true;
                }
                final long interval = Math.min(wait, INTV_MS);
                Thread.sleep(interval);
            }
            return false;
        }
        return true;
    }

    /**
     * Method to check if pid is running
     * */
    private static boolean isProcessRunning(long processID)
        throws IOException, InterruptedException {
        String[] cmd = {"ps", "-p", String.valueOf(processID)};
        ProcessBuilder build = new ProcessBuilder().command(cmd);
        build.redirectErrorStream(true);
        build.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        Process p = build.start();
        return p.waitFor() == 0;
    }

    /**
     * Failure injection for testing
     * */
    private static boolean testKillProcess(long pid)
            throws InterruptedException{
        if (KILL_PROCESS_TEST) {
            final Optional<ProcessHandle> optHandle = ProcessHandle.of(pid);
            final ProcessHandle handle = optHandle.get();
            handle.destroyForcibly();
            try {
                handle.onExit().get(TEST_WAIT_KILL_SECS, SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    /**
     * Failure injection for testing
     * */
    private static boolean testDeleteStatus(String fileDir) {
        if (REMOVE_STATUS_TEST) {
            final File file = new File(fileDir);
            file.delete();
            if (file.exists()) {
                System.err.println("Fail to delete file=" + fileDir);
            }
        }
        return true;
    }

    /**
     * Process builder to start a new process
     * */
    public static ProcessBuilder buildProcess(String jsonPath){
        final List<String> cmd = new ArrayList<>();
        final String[] heapSize = heapSizeFinder(jsonPath);
        final String cp = System.getProperty("java.class.path");
        final String className = KVStoreMain.class.getName();

        Collections.addAll(cmd,"java", heapSize[0], heapSize[1], "-cp", cp,
                           className, "xrstart", "-config", jsonPath);
        return new ProcessBuilder().command(cmd);
    }

    /**
     * Start agent in background
     * */
    public Process startProcess() throws IOException{
        final ProcessBuilder build = buildProcess(json);
        build.redirectError(ProcessBuilder.Redirect.INHERIT);
        build.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        return build.start();
    }

    /**
     * Parse status file with given process id
     * */
    private StartParseStatus statusParseWithPid(String fileDir,
                                                long pid) {
        final File file = new File(fileDir, statusFileName);
        if (file.exists()) {
            try (BufferedReader br = new BufferedReader(
                new FileReader(fileDir + "/" + statusFileName))) {
                final String line = br.readLine();
                final long pid1 = parseLong(line);
                if (pid != pid1) {
                    return StartParseStatus.DUPLICATE;
                }
                assert testKillProcess(pid1);
                if (isProcessRunning(pid1)) {
                    return StartParseStatus.SUCCESS;
                }
                return StartParseStatus.CRASHED;
            } catch (IOException| InterruptedException e) {
                return StartParseStatus.FAILURE;
            }
        }
        return StartParseStatus.FAILURE;
    }

    /**
     * Parse status file
     * */
    private StartParseStatus statusParse(String fileDir) {
        try (BufferedReader br = new BufferedReader(
            new FileReader(fileDir + "/" + statusFileName))) {
            final String line = br.readLine();
            final long pid = parseLong(line);
            assert testKillProcess(pid);
            if (isProcessRunning(pid)) {
                return StartParseStatus.SUCCESS;
            }
            return StartParseStatus.CRASHED;
        } catch (IOException|InterruptedException e) {
            return StartParseStatus.FAILURE;
        }
    }

    /**
     * Runs one of the commands
     */
    private ExitCode run() throws IOException, InterruptedException {
        switch (command) {
            case START_COMMAND_NAME:
                if (background) {
                    return runStartBg(json);
                }
                return runStart();
            case STOP_COMMAND_NAME:
                if (background) {
                    /* stop command cannot run in background */
                    usageStop();
                    break;
                }
                return runStop();
            case STATUS_COMMAND_NAME:
                if (background) {
                    /* status command cannot run in background */
                    usageStatus();
                    break;
                }
                return runStatus(json);
            default:
                throw new IllegalStateException("Unsupported command " +
                                                command);
        }
        throw new IllegalStateException("Cannot run command=" + command);
    }

    private StartExitCode runStart() {
        final String dir = new File(json).getAbsoluteFile().getParent();
        final String tmpPath = dir + "/" + statusFileName + ".tmp";
        final String resPath = dir + "/" + statusFileName;
        try (PrintWriter pw = new PrintWriter(tmpPath)) {
            if (!lockMan.lock()) {
                System.err.println(
                    "Duplicate cross-region service is not allowed, id=" +
                    buildAgentId(conf));
                return StartExitCode.DUPLICATE;
            }
            /* get the lock */
            XRegionService service;
            int attempts = 0;
            while (true) {
                try {
                    service = new XRegionService(conf, logger);
                    break;
                } catch (ServiceMDMan.UnreachableHostRegionException exp) {
                    attempts++;
                    final String msg = "Please check if the local region=" +
                                       conf.getRegion() +
                                       " is online, will retry after " +
                                       UNREACHABLE_HOST_REGION_RETRY_MS +
                                       " ms, # attempts=" + attempts +
                                       ", error " +
                                       exp.getCause().getMessage();
                    rlLogger.log(conf.getRegion(), Level.WARNING, lm(msg));
                    synchronized (this) {
                        wait(UNREACHABLE_HOST_REGION_RETRY_MS);
                    }
                }
            }

            /* add shutdown hook */
            addShutdownHook(service);
            /* start service */
            service.start();
            final long pid = lockMan.readPid();
            final String ts =
                    FormatUtils.formatDateTime(System.currentTimeMillis());
            String result = pid + "\n" + "Cross-region agent (region=" +
                            conf.getRegion() +
                            ", store=" + conf.getStore() +
                            ", helpers=" +
                            Arrays.toString(conf.getHelpers()) +
                            ") starts up from config file=" +
                            json + " at " + ts;
            pw.write(result);
            pw.close();
            Files.move(Paths.get(tmpPath), Paths.get(resPath),
                       StandardCopyOption.ATOMIC_MOVE);
            System.out.println(result);
            /* wait for service to exit */
            service.join();
            return StartExitCode.SUCCESS;
        }catch (Exception exp) {
            System.err.println("Cannot start cross-region service agent: " + exp);
            return StartExitCode.FAILED;
        } finally {
            lockMan.release();
        }
    }

    /**
     * Run agent in bg and get return code
     * */
    private StartExitCode runStartBg(String jsonPath)
        throws IOException, InterruptedException {
        final String fileDir = new File(jsonPath).getParent();
        final Process p = startProcess();
        /*
         * Wait to give time for process to generate exit code if any
        */
        p.waitFor(INTV_MS, TimeUnit.MILLISECONDS);
        if (p.isAlive()) {
            final long pid = p.pid();
            boolean statusFile = statusFilePresent(fileDir);
            if (statusFile) {
                final StartParseStatus statusRes = statusParseWithPid(fileDir,
                                                                      pid);
                switch (statusRes) {
                    case SUCCESS:
                        return StartExitCode.SUCCESS;
                    case DUPLICATE:
                        return StartExitCode.DUPLICATE;
                    case CRASHED:
                        return StartExitCode.CRASHED;
                    default:
                        return StartExitCode.FAILED;
                }
            }
            return StartExitCode.TIMEOUT;
        }
            /* return the status from the process exit code */
            final int code = p.exitValue();
            return StartExitCode.values()[code];
    }

    /**
     * Run status command and get return value
     * */
    private StatusExitCode runStatus(String fileDir){
        final String dir = new File(fileDir).getParent();
        final StartParseStatus status = statusParse(dir);
        switch (status){
            case SUCCESS:
                return StatusExitCode.SUCCESS;
            case CRASHED:
                return StatusExitCode.CRASHED;
            default:
                return StatusExitCode.FAILED;
        }
    }

    private StopExitCode runStop() {
        String dir = new File(json).getParent();
        String resPath = dir + "/" + statusFileName;
        File status = new File(resPath);
        try {
            final long pid = lockMan.readPid();
            final String error = runKill(pid, force);
            final String ts =
                    FormatUtils.formatDateTime(System.currentTimeMillis());
            if (error == null) {
                String result = pid + "\n" +
                                "Cross-region service (pid=" + pid +
                                ", region=" + conf.getRegion() +
                                ", store=" + conf.getStore() +
                                ") shuts down (force=" + force + ")" +
                                " at time=" + ts;
                status.delete();
                lockMan.deletePid();
                System.out.println(result);
                return StopExitCode.STOP;
            }
            System.err.println("Cannot shut down cross-region service " +
                               "(pid=" + pid +
                               ", region=" + conf.getRegion() +
                               ", store=" + conf.getStore() +
                               ", force=" + force + ")" +
                               " at time=" + ts +
                               ", error=" + error);
            return StopExitCode.NONSTOP;

        } catch (Exception exp) {
            System.err.println(exp.getMessage());
        }
        return StopExitCode.NONSTOP;
    }

    /**
     * Parses the argument list
     */
    private void parseArgs() {

        int nArgs = args.length;
        /* get the command */
        command = args[nArgs - 1];
        if (!command.equals(START_COMMAND_NAME) &&
            !command.equals(STOP_COMMAND_NAME) &&
            !command.equals(STATUS_COMMAND_NAME)) {
            usage("invalid command: " + command);
        }

        int argc = 0;
        while (argc < nArgs - 1) {
            final String thisArg = args[argc++];
            if ("-config".equals(thisArg)) {
                if (argc < nArgs) {
                    json = args[argc++];
                } else if (args[0].equals("start")) {
                    usage("-config requires an argument to start");
                }
            } else if ("-bg".equals(thisArg)) {
                background = true;
            } else if ("-force".equals(thisArg)) {
                force = true;
            } else {
                usage("Unknown argument: " + thisArg);
            }
        }
    }

    /**
     * Gets client side logger for agent thread
     *
     * @return client side logger
     */
    private static Logger getServiceLogger(JsonConfig conf)
        throws IOException {
        final Logger logger = ClientLoggerUtils.getLogger(
            XRService.class, XRService.class.getSimpleName());
        final String dir = createLogDirIfNotExist(conf.getAgentRoot());
        final String fileName = buildLogFileName(conf);
        final File lf = new File(dir, fileName);
        /* TODO: Provide a way for users to customize the logging config */
        final FileHandler fh = new FileHandler(lf.getAbsolutePath(),
                                               LOG_FILE_LIMIT_BYTES,
                                               LOG_FILE_LIMIT_COUNTS, true);
        fh.setFormatter(new LogFormatter(null));
        logger.addHandler(fh);
        return logger;
    }

    /* create log directory if not exist */
    private static String createLogDirIfNotExist(String path)
        throws IOException {
        final Path dir = Paths.get(path, DEFAULT_LOG_DIR);
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException exp) {
                System.err.println("Cannot create log directory " +
                                   dir.getFileName() +
                                   ", error " + exp.getMessage());
                throw exp;
            }
        }
        return dir.toString();
    }

    /* build the log file name from agent id */
    private static String buildLogFileName(JsonConfig conf) {
        return buildAgentId(conf) + LOG_FILE_SUFFIX;
    }

    private static String buildLockFileName(JsonConfig conf) {
        return buildAgentId(conf) + LOCK_FILE_SUFFIX;
    }

    public static String buildPidFileName(JsonConfig conf) {
        return buildAgentId(conf) + PID_FILE_SUFFIX;
    }

    /**
     * Kill the process with the specified ID, waiting for the process to exit.
     *
     * @param pid the process ID
     * @param force whether to force kill
     * @return null if command exits normally, or error output otherwise
     */

    private static String runKill(long pid, boolean force) {
        final Optional<ProcessHandle> optHandle = ProcessHandle.of(pid);
        /* Process is already dead */
        if (optHandle.isEmpty()) {
            return "Process with pid=" + pid + " is already dead";
        }
        final ProcessHandle handle = optHandle.get();
        if (force) {
            handle.destroyForcibly();
        } else {
            handle.destroy();
        }
        /* Wait for the process to exit */
        try {
            handle.onExit().get(WAIT_TIME_MS, SECONDS);
            return null;
        } catch (Exception e) {
            return "Error in stop the service, error=" + e;
        }
    }

    private void addShutdownHook(XRegionService service) {
        logger.fine(() -> lm("Adding shutdown hook"));
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(service));
    }

    private class LockFileManager {

        private final String lockFileDir;
        private final String pidFileName;

        /* The channel and lock for the lock file. */
        private final RandomAccessFile lockFile;
        private FileLock exclLock = null;

        LockFileManager(JsonConfig conf) throws IOException {
            pidFileName = buildPidFileName(conf);
            lockFileDir = conf.getAgentRoot();
            /* lock file name and dir */
            final String lockFileName = buildLockFileName(conf);
            lockFile = new RandomAccessFile(
                new File(lockFileDir, lockFileName),
                FileManager.FileMode.READWRITE_MODE.getModeValue());
        }

        public boolean lock() throws IOException {
            final FileChannel channel = lockFile.getChannel();
            try {
                /* lock exclusive */
                exclLock = channel.tryLock(0, 1, false);
                final boolean succ = (exclLock != null);
                if (succ) {
                    /* delete previous pid file if existent */
                    deletePid();
                    /* persist process id for stop */
                    writePid();
                }
                return succ;
            } catch (OverlappingFileLockException e) {
                return false;
            }
        }

        public void release() {
            try {
                if (exclLock != null) {
                    exclLock.release();
                }
            } catch (Exception e) {
                /* ignore? */
            }
        }

        /* read pid from pid file */
        long readPid() throws IOException {
            final File file = new File(lockFileDir, pidFileName);
            if (!file.exists()) {
                throw new IOException("Cannot find PID file=" +
                                      file.getAbsolutePath() +
                                      ", check the file and if the " +
                                      "service has already shut down.");
            }
            if (file.length() == 0) {
                throw new IOException("Empty PID file: " +
                                      file.getAbsolutePath());
            }
            /* only read the first line */
            final String line = Files.lines(file.toPath()).iterator().next();
            return Long.parseLong(line);
        }

        /* delete pid file */
        void deletePid() {
            final File file = new File(lockFileDir, pidFileName);
            if (!file.exists()) {
                return;
            }
            if (!file.delete()) {
                logger.info(lm("Fail to delete pid file=" +
                               file.getAbsolutePath()));
            }
        }

        /*  write pid to pid file */
        private void writePid() throws IOException {
            final long pid = getPid();
            if (pid == 0) {
                final String err = "Cannot determine process id";
                throw new IOException(err);
            }
            final List<String> lines =
                Collections.singletonList(String.valueOf(pid));
            final Path file = Paths.get(lockFileDir, pidFileName);
            try {
                Files.write(file, lines);
            } catch (IOException ioe) {
                logger.warning(lm("Cannot write process id=" + pid +
                                  " to pid file=" + file.toAbsolutePath()));
                throw ioe;
            }
        }

        /* get pid from OS */
        private long getPid() {
            //TODO: simplify when upgrade to Java 9+
            // return ProcessHandle.current().pid();

            /* Java 8 */
            final String processName =
                ManagementFactory.getRuntimeMXBean().getName();
            if (processName != null && processName.length() > 0) {
                try {
                    return Long.parseLong(processName.split("@")[0]);
                }
                catch (Exception e) {
                    return 0;
                }
            }
            return 0;
        }
    }

    /* Provide a shutdown hook so that if the service is killed externally */
    private class ShutdownThread extends Thread {

        private final XRegionService service;

        ShutdownThread(XRegionService service) {
            this.service = service;
        }

        @Override
        public void run() {
            logger.info(lm("Shutdown thread running, stopping services"));
            try {
                service.shutdown();
                /* wait for shutdown complete */
                service.join();
            } catch (Exception exp) {
                /* ignored in shut down */
            } finally {
                logger.info(lm("Shutdown complete"));
            }
        }
    }

    private String lm(String msg) {
        return "[XRegionService] " + msg;
    }
}
