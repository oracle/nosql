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

package oracle.kv.impl.sna;

import static oracle.kv.impl.param.ParameterState.JVM_RN_EXCLUDE_ARGS;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GroupNodeParams;
import oracle.kv.impl.measurement.ProxiedServiceStatusChange;
import oracle.kv.impl.mgmt.MgmtAgent;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.test.TwoArgTestHook;
import oracle.kv.impl.test.TwoArgTestHookExecute;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * Implementation of ServiceManager that uses processes.
 */
public class ProcessServiceManager extends ServiceManager {
    /**
     * The name of a system parameter that specifies extra JVM arguments that
     * will be included in the command line for spawned processes. Multiple
     * arguments can be separated with semicolons.
     */
    public static final String JVM_EXTRA_ARGS_FLAG =
        "oracle.kv.jvm.extraargs";

    /**
     * Global static map contains JVM arguments not supported for a Java
     * major version. The Key is min java version when the JVM argument is
     * deprecated/excluded. The JVM arguments should be removed for any java
     * major version >= key. The value may contain multiple jvm args with
     * space. Values for a jvm args should not be provided. For example,
     * JVM_VERSION_ARGS_EXCLUDE_MAP.put(18, "-XX:key1 -XX:key2").
     */
    private static final Map<Integer, String> JVM_VERSION_ARGS_EXCLUDE_MAP =
        new HashMap<>();
    static {
        JVM_VERSION_ARGS_EXCLUDE_MAP.put(18, "-XX:G1RSetRegionEntries " +
                                             "-XX:G1RSetSparseRegionEntries");
    }

    private AgentRepository agentRepository;
    private ProcessMonitor monitor;
    private MgmtAgent mgmtAgent;
    private static String pathToJava;
    private final StorageNodeAgent sna;
    private boolean stopExplicitly = false;
    
    /**
     * Test hook that is called to permit reading or updating the exec
     * arguments on startup or restart.  The first argument is the list of
     * command line arguments, and the second is this instance.
     */
    private static TwoArgTestHook<List<String>, ProcessServiceManager>
        createExecArgsHook;

    public ProcessServiceManager(StorageNodeAgent sna,
                                 ManagedService service) {
        super(sna, service);
        this.sna = sna;
        this.mgmtAgent = sna.getMgmtAgent();
        this.agentRepository = null;
        monitor = null;
        registered(sna);
    }

    class ServiceProcessMonitor extends ProcessMonitor {
        private final ServiceManager mgr;

        public ServiceProcessMonitor(ServiceManager mgr,
                                     List<String> command,
                                     Map<String, String> env) {
            super(command, env, -1, service.getServiceName(), logger);
            this.mgr = mgr;
        }

        private void generateStatusChange(ServiceStatus status) {
            if (agentRepository != null) {
                ResourceId rid = getService().getResourceId();
                ProxiedServiceStatusChange sc =
                    new ProxiedServiceStatusChange(rid, status);
                agentRepository.add(sc);
                mgmtAgent.proxiedStatusChange(sc);
            }
        }

        @Override
        protected void onExit() {

            service.setStartupBuffer(startupBuffer);
            final ResourceId rId = service.getResourceId();
            if (rId instanceof RepNodeId) {
                if (sna.getMasterBalanceManager() != null) {
                    sna.getMasterBalanceManager().noteExit((RepNodeId)rId);
                }
            }

            if (stopExplicitly) {
                /*
                 * If stop explicitly, proxy a STOPPED state because that
                 * happens on admin-generated shutdown and may not have been
                 * picked up by the monitor.
                 */
                generateStatusChange(ServiceStatus.STOPPED);
            } else {
                /*
                 * exceed limited restart count or exit code indicates that the
                 * process don't need to be restarted.
                 */
                generateStatusChange(ServiceStatus.ERROR_NO_RESTART);
            }

            if (!skipMonitorEventLogging(rId)) {
                LoggerUtils.logServiceMonitorExitEvent(
                    logger, rId, stopExplicitly ? 0 : getExitCode(),
                    false /* restart */);
            }
        }

        @Override
        protected void onRestart() {

            final ResourceId rId = service.getResourceId();
            if (rId instanceof RepNodeId) {
               sna.getMasterBalanceManager().noteExit((RepNodeId)rId);
            }

            if (getExitCode() == 0) {
                generateStatusChange(ServiceStatus.EXPECTED_RESTARTING);
            } else {
                generateStatusChange(ServiceStatus.ERROR_RESTARTING);
            }
            service.resetHandles();
            service.resetParameters(false);
            if (service.resetOnRestart() || (createExecArgsHook != null)) {
                mgr.reset();
            }

            if (!skipMonitorEventLogging(rId)) {
                LoggerUtils.logServiceMonitorExitEvent(
                    logger, rId, getExitCode(), true /* restart */);
            }
        }

        @Override
        protected void afterStart() {
            mgr.notifyStarted();
            final ResourceId rId = service.getResourceId();
            if (!skipMonitorEventLogging(rId)) {
                LoggerUtils.logServiceMonitorStartEvent(logger, rId);
            }
        }

        /**
         * Returns whether to skip logging monitor events for the service with
         * the specified ID.
         */
        private boolean skipMonitorEventLogging(ResourceId rId) {

            /*
             * admin0 is used by the bootstrap admin, which seems to get
             * started and stopped many times without meaning anything, so
             * don't log monitor events for it
             */
            return (rId instanceof AdminId) &&
                (((AdminId) rId).getAdminInstanceId() == 0);
        }
    }

    @Override
    public void registered(StorageNodeAgent sna1) {
        if (agentRepository == null) {
            agentRepository = (sna1.getMonitorAgent() != null) ?
                sna1.getMonitorAgent().getAgentRepository() : null;
        }
    }

    @Override
    public void start()
        throws Exception {

        createSecurityPropertyFile();
        List<String> command = createExecArgs();
        Map<String, String> env = service.getEnvironment();
        if (logger != null) {
            logger.info("Executing process with arguments: " + command +
                        " with environment: " + env);
        }
        monitor = new ServiceProcessMonitor(this, command, env);
        monitor.startProcess();
    }

    /**
     * Create a security property file based on the properties string.
     *
     * The file serves to append the security properties. It is generated every
     * time we start the service based on the StorageNodeParams in the
     * configuration file. We do this to avoid consistency issues between
     * values in the memory and in the property file. Even though the content
     * is the same, we still create a file per service to avoid issues when
     * concurrently launching services.
     */
    private void createSecurityPropertyFile() throws Exception {
        final File securityPropertyFile = getSecurityPropertyFile();
        Properties properties =
            ConfigUtils.getSecurityProperties(sna.getBootstrapParams());
        try (FileOutputStream fos =
             new FileOutputStream(securityPropertyFile)) {
            properties.store(
                fos, "Automatically generated before service start");
        }
    }

    public File getSecurityPropertyFile() {
        final BootstrapParams bp = sna.getBootstrapParams();
        final String storeName = bp.getStoreName();
        final String serviceName = service.getServiceName();
        if (storeName == null) {
            return new File(sna.getBootstrapDir(),
                            serviceName +
                            FileNames.SECURITY_PROPERTY_FILE_SUFFIX);
        }
        return new File(FileNames.getSecurityPropertyDir(
                            ConfigUtils.getSNRootDir(bp)),
                        serviceName +
                        FileNames.SECURITY_PROPERTY_FILE_SUFFIX);
    }

    /**
     * Terminate managed process with prejudice.
     */
    @Override
    public void stop() {

        try {
            if (monitor != null) {
                monitor.stopProcess(false);
            }
        } catch (InterruptedException ie) {
        } finally {
            monitor.destroyProcess();
            try {
                monitor.waitProcess(0);
            } catch (InterruptedException ignored) {
                /* nothing to do if this was interrupted */
            }
        }
    }

    @Override
    public void waitFor(int millis) {

        try {
            if (monitor != null) {
                if (!monitor.waitProcess(millis)) {
                    logger.info("Service did not exiting cleanly in " + millis
                                + " milliseconds, it will be killed");
                    stop();
                }
            }
        } catch (InterruptedException ie) {
        }
    }

    @Override
    public void dontRestart() {

        /* User stop service explicitly. */
        stopExplicitly = true;
        if (monitor != null) {
            monitor.dontRestart();
        }
    }

    @Override
    public boolean isRunning() {
        if (monitor != null) {
            return monitor.isRunning();
        }
        return false;
    }

    /**
     * Reset ProcessMonitor command using the current service info.
     */
    @Override
    public void reset() {
        List<String> command = createExecArgs();
        monitor.reset(command, service.getServiceName());
    }

    /**
     * Force is OK with processes.
     */
    @Override
    public boolean forceOK(boolean force) {
        return force;
    }

    @Override
    public void resetLogger(Logger logger1) {
        this.logger = logger1;
        monitor.resetLogger(logger);
    }

    @Override
    public void reloadSNParams() {
        mgmtAgent = sna.getMgmtAgent();
    }

    /* Public for testing */
    public int getExitCode() {
        return monitor.getExitCode();
    }

    /**
     * Terminate managed process without any bookkeeping -- this simulates
     * a random exit.  The process should be restarted by the ProcessMonitor.
     */
    void destroy() {
        if (monitor != null) {
            monitor.destroyProcess();
            try {
                monitor.waitProcess(0);
            } catch (InterruptedException ignored) {
                /* nothing to do if this was interrupted */
            }
        }
    }

    /**
     * Tiny test class to test exec of java.
     */
    public static class TestJavaExec {
        public static final int EXITCODE = 75;
        public static void main(String args[]) {
            System.exit(EXITCODE);
        }
    }

    /**
     * Try exec'ing "java.home"/bin/java.  If it works, use it.  If not,
     * return "java" and rely on the PATH environment variable.
     */
    private String findJava() {
        String home = System.getProperty("java.home");
        String cp = System.getProperty("java.class.path");
        String path = home + File.separator + "bin" +
            File.separator + "java";
        List<String> command = new ArrayList<>();
        command.add(path);
        if (cp != null) {
            command.add("-cp");
            command.add(cp);
        }
        command.add(getClass().getName() + "$TestJavaExec");
        String[] commandArray = command.toArray(new String[command.size()]);
        try {
            Process process = Runtime.getRuntime().exec(commandArray);
            process.waitFor();
            if (process.exitValue() == TestJavaExec.EXITCODE) {
                return path;
            }
        } catch (Exception e) {
            logger.info("Unable to exec test process: " +
                        command + ", exception: " + e);
        }
        return "java";
    }

    /**
     * Determine the path to the JVM used to execute this process and use it
     * for execution of new JVMs.  Test it out with a tiny program.
     */
    private synchronized String getPathToJava() {
        if (pathToJava == null) {
            pathToJava = findJava();
            logger.info("Using java program: " + pathToJava +
                        " to execute managed processes");
        }
        return pathToJava;
    }

    /**
     * TODO: think about inferred arguments that could be added based on
     * heap size.  E.g.:
     * < 4G -d32
     * > 4G -d64 if available, -XX:+UseCompressedOops
     * > 32G -- they should just figure it out.
     */
    private void addJavaMiscArgs(List<String> command,
                                 String jvmArgs) {

        String miscParams = "";
        /* A service may add its own default arguments */
        if (service.getDefaultJavaArgs(jvmArgs) != null) {
            miscParams = service.getDefaultJavaArgs(jvmArgs);
        }
        /* OmitStackTraceInFastThrow is an optimization in hotspot that
         * uses preallocated exceptions in highly optimized code. This
         * saves the time to allocate the exception object. But the
         * preallocated exceptions have no message nor a stack trace. To
         * disable the use of preallocated exceptions, use this new flag:
         * -XX:-OmitStackTraceInFastThrow.
         */
        miscParams += " -XX:-OmitStackTraceInFastThrow";
        if (jvmArgs != null) {
            miscParams += " " + jvmArgs;
        }
        addParams(miscParams, command);
    }
    
    private void addParams(String miscParams, List<String>command) {
        if (miscParams.length() != 0) {
            String[] args = miscParams.trim().split("\\s+");
            for (String arg : args) {

                /**
                 * Replace leading/trailing quotes that may have ended up in
                 * the command.  These will cause problems.
                 */
                arg = arg.replaceAll("^\"|\"$", "");
                arg = arg.replaceAll("^\'|\'$", "");
                command.add(arg);
            }
        }
    }

    private void addJavaExtraArgs(List<String> command) {
        final String jvmExtraArgs =
            System.getProperty(JVM_EXTRA_ARGS_FLAG);
        if (jvmExtraArgs != null) {
            for (String arg : splitExtraArgs(jvmExtraArgs)) {
                command.add(arg);
            }
            command.add("-D" + JVM_EXTRA_ARGS_FLAG + "=" + jvmExtraArgs);
        }
    }

    /*
     * Add the args to read the security properties.
     */
    private void addSecurityPropertyFile(List<String> command) {
        command.add(String.format("-Djava.security.properties=%s",
                                  getSecurityPropertyFile()));
    }

    private static String[] splitExtraArgs(String extraArgs) {
        return extraArgs.split(";");
    }

    private void addLoggingArgs(List<String> command,
                                String loggingConfig) {

        if (loggingConfig == null || loggingConfig.length() == 0) {
            return;
        }
        String logConfigFile = service.createLoggingConfigFile(loggingConfig);
        if (logConfigFile != null) {
            command.add("-Djava.util.logging.config.file=" + logConfigFile);
        }
    }

    private boolean addAssertions(List<String> command) {
        command.add("-ea");
	return true;
    }

    private List<String> createExecArgs() {

        List<String> command = new ArrayList<String>();

        String customStartupPrefix = sna.getCustomProcessStartupPrefix();
        if ((customStartupPrefix != null) && !customStartupPrefix.isEmpty()){
            command.add(customStartupPrefix);
        }

        command.add(getPathToJava());
        JVMSystemUtils.addZingJVMArgs(command);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));

        /**
         * If Java arguments and/or logging configuration are present, add
         * them.
         */
        String overrideArgs = service.getJVMOverrideArgs();
        String jvmArgs = 
            removeOverriddenArgs(service.getJVMArgs(), overrideArgs);
        String loggingConfig = service.getLoggingConfig();
        addJavaMiscArgs(command, jvmArgs);
        TwoArgTestHookExecute.doHookIfSet(createExecArgsHook, command, this);
        addJavaExtraArgs(command);
        addSecurityPropertyFile(command);
        assert(addAssertions(command));
        if (loggingConfig != null) {
            /*
             * Logging configuration.  This will create the logging config file.
             */
            addLoggingArgs(command, loggingConfig);
        } else {
            String logConfig =
                System.getProperty("java.util.logging.config.file");
            if (logConfig != null) {
                command.add("-Djava.util.logging.config.file=" + logConfig);
            }
        }
        if (overrideArgs != null && overrideArgs.trim().length() > 0) {
            addParams(overrideArgs.trim(), command);
        }

        removeExcludedJVMArgs(command, service.getJVMExcludeArgs());
        return service.addExecArgs(command);
    }

    private void removeExcludedJVMArgs(List<String> command,
                                         String jvmArgExcludeParams) {

        final int majorJavaVersion = VersionUtil.getJavaMajorVersion();
        final Map<Integer, String> mergedJVMExcludeMap =
            mergeJVMExcludeMaps(parseJVMExcludeParams(jvmArgExcludeParams)
            );
        /* remove the command if unsupport for java version*/
        command.removeIf(s -> isJVMArgUnsupported(majorJavaVersion, s,
                                                  mergedJVMExcludeMap));
    }

    private Map<Integer, String> parseJVMExcludeParams(
        String jvmArgExcludeParams) {
        final String[] params = jvmArgExcludeParams.split("\\s");
        final Map<Integer, String> paramsExcludeMap = new HashMap<>();
        for(String arg : params) {
            if (arg.isEmpty()) {
                continue;
            }
            final int equalsPos = arg.indexOf('=');
            if (equalsPos < 0) {
                logger.info("Missing '=' in value of " +
                            JVM_RN_EXCLUDE_ARGS + " parameter");
                continue;
            }
            final String javaVersionString = arg.substring(0, equalsPos);
            final int javaVer;
            try {
                javaVer = Integer.parseInt(javaVersionString);
            } catch (NumberFormatException e) {
                logger.info("Invalid Java version value '" +
                            javaVersionString + "' in value of " +
                            JVM_RN_EXCLUDE_ARGS + " parameter");
                continue;
            }
            final String excludeArg = arg.substring(equalsPos + 1);
            paramsExcludeMap.merge(javaVer, excludeArg,
                                   (oldVal, newVal) -> oldVal + " " + newVal);
        }

        return paramsExcludeMap;
    }

    private Map<Integer, String> mergeJVMExcludeMaps(
        Map<Integer, String> paramsExcludeMap) {

        if (paramsExcludeMap.isEmpty()) {
            return JVM_VERSION_ARGS_EXCLUDE_MAP;
        }

        final Map<Integer, String> result = new HashMap<>(JVM_VERSION_ARGS_EXCLUDE_MAP);
        for (Map.Entry<Integer, String> entry : paramsExcludeMap.entrySet()) {
            result.merge(entry.getKey(), entry.getValue(),
                         (oldVal, newVal) -> oldVal + " " + newVal);
        }
        return result;
    }

    private boolean isJVMArgUnsupported(int majorJavaVersion,
                                        String jvmArg,
                                        Map<Integer, String> excludeMap) {
        for (Map.Entry<Integer, String> entry :
            excludeMap.entrySet()) {
            if (majorJavaVersion >= entry.getKey()) {
                /* Exclude any arguments that part of exclude group*/
                final String excludeJVMArgs = entry.getValue();
                /*
                 * Extract JVM argument key by truncating before the equals
                 * sign, if present: -XX:<key>=<value>
                 *
                 * Boolean arguments should be handled by providing
                 * both values in the params. For example, if a boolean flag
                 * (AllowUserSignalHandlers) is not supported for java
                 * version 21 onwards, we should add following param value
                 * "21=-XX:-AllowUserSignalHandlers
                 * -XX:+AllowUserSignalHandlers".
                 */
                final String jvmArgKey = jvmArg.split("=")[0];
                if (excludeJVMArgs.contains(jvmArgKey)) {
                    logger.info("JVM argument " + jvmArgKey +" is removed " +
                                "from the command as its not supported for " +
                                "java version " + majorJavaVersion);
                    return true;
                }
            }
        }

        return false;
    }

    /*
     * Set the test hook to read or modify the command line arguments.
     */
    public static void setCreateExecArgsHook(
        TwoArgTestHook<List<String>, ProcessServiceManager> hook) {
        createExecArgsHook = hook;
    }

    /*
     * Return a modified command line string. The returned string has
     * the -Xms, -Xmx, -XX:ParallelGCThreads, or -XX:ConcGCThreads 
     * parameters removed if the parameters exist in the override value.
     */
    private String removeOverriddenArgs(String jvmMiscArgs, String jvmMiscOver){
        boolean foundGCOver = false;
        boolean foundCGCTOver = false;
        boolean foundXmsOver = false;
        boolean foundXmxOver = false;
        if (jvmMiscOver == null || jvmMiscOver.length() == 0 || 
            jvmMiscArgs == null || jvmMiscArgs.length() == 0) {
            return jvmMiscArgs;
        }
        
        String tokens[] = jvmMiscOver.split("\\s");
        for (String arg : tokens) {
            if (arg.startsWith(GroupNodeParams.PARALLEL_GC_FLAG)) {
                foundGCOver = true;
            }
            if (arg.startsWith(GroupNodeParams.CONCURRENT_GC_FLAG)) {
                foundCGCTOver = true;
            }
            if (arg.startsWith(GroupNodeParams.XMS_FLAG)) {
                foundXmsOver = true;
            }
            if (arg.startsWith(GroupNodeParams.XMX_FLAG)) {
                foundXmxOver = true;
            }
        }
        if (!foundGCOver && !foundXmsOver && !foundXmxOver) {
            return jvmMiscArgs;
        }
        
        StringBuilder result = new StringBuilder();
        tokens = jvmMiscArgs.split("\\s");
        for (String arg : tokens) {
            if (arg.startsWith(GroupNodeParams.PARALLEL_GC_FLAG)) {
                if (foundGCOver) {
                    arg = null;
                }
            } else if (arg.startsWith(GroupNodeParams.CONCURRENT_GC_FLAG)) {
                if (foundCGCTOver) {
                    arg = null;
                }
            } else if (arg.startsWith(GroupNodeParams.XMS_FLAG )) {
                if (foundXmsOver) {
                    arg = null;
                }
            } else if (arg.startsWith(GroupNodeParams.XMX_FLAG)) {
                if (foundXmxOver) {
                        arg = null;
                }
            } 
            if (arg != null) {
               result.append(arg).append(" ");
            }
        }
        return result.toString().trim();
    }
}
