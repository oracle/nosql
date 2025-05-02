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

import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Map;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.utilint.JVMSystemUtils;

public class ManagedRepNode extends ManagedService {

    private SecurityParams sp;
    private RepNodeParams rnp;
    private RepNodeAdminAPI repNodeAdmin;
    private LoadParameters lp;
    private volatile RepNodeService rns;

    /**
     * Constructor used by the SNA client.
     */
    public ManagedRepNode(SecurityParams sp,
                          RepNodeParams rnp,
                          File kvRoot,
                          File kvSNDir,
                          String kvName) {
        super(kvRoot, sp.getConfigDir(), kvSNDir, kvName,
              REP_NODE_NAME, rnp.getRepNodeId().getFullName(), rnp.getMap());

        this.sp = sp;
        this.rnp = rnp;
        repNodeAdmin = null;
    }

    /**
     * Constructor used by the service instance upon startup.
     */
    public ManagedRepNode(String kvSecDir,
                          String kvSNDir,
                          String kvName,
                          String serviceClass,
                          String serviceName) {

        super(null, nullableFile(kvSecDir), new File(kvSNDir),
              kvName, serviceClass, serviceName, null);
        resetParameters(!usingThreads);
        startRNLogger(RepNodeService.class, rnp, rnp.getRepNodeId(), lp);
        repNodeAdmin = null;
    }

    @Override
    public void resetParameters(boolean inTarget) {
        sp = getSecurityParameters();
        lp = getParameters();
        params = lp.getMap(serviceName, ParameterState.REPNODE_TYPE);
        rnp = new RepNodeParams(params);
        rnp.validateCacheAndHeap(inTarget);
    }

    static final String DEFAULT_MISC_JAVA_ARGS =
        /*
         * Don't use large pages on the Mac, where they are not supported and,
         * in Java 1.7.0_13, doing so with these other flags causes a crash.
         * [#22165]
         */
        ((!"Mac OS X".equals(System.getProperty("os.name")) &&
            /*
             * Don't use large pages on test machines not configured to use
             * them, it just slows down test startup.
             */
            !TestStatus.isActive()) ?
            "-XX:+UseLargePages " :
            "") +

        /**
         * Pretouch pages on OS and jvms that support it. Pretouching a 32GB
         * vm adds ~10 sec to the startup time with 4K pages and just 500ms with
         * 2MB huge pages and reduces latency spikes during normal operation.
         */
        ("linux".equalsIgnoreCase(System.getProperty("os.name")) &&
            (System.getProperty("java.vendor") != null) &&
            (System.getProperty("java.vendor").startsWith("Oracle") &&
            /*
             * Don't pretouch pages when running tests to speed up frequent
             * short-lived RN initialization
             */
            !TestStatus.isActive()) ?
                "-XX:+AlwaysPreTouch " : "") +

        /*
         * Disable biased locking.
         * Its not beneficial for RNs and may result in JVM pause spikes
         * when there is resource starvation.
         *
         * [KVSTORE-1047]
         *
         * Biased locking is permanently disabled as of JDK 15, and the flag is
         * no longer supported as of JDK 19. See:
         * https://bugs.openjdk.java.net/browse/JDK-8231265
         *
         * [KVSTORE=1529]
         */
        (getJavaMajorVersion() < 15 ?
         "-XX:-UseBiasedLocking " :
         "") +

        /*
         * Disable JE's requirement that helper host names be resolvable.  We
         * want nodes to be able to start up even if other nodes in the
         * replication group have been removed and no longer have DNS names.
         * [#23120]
         */
        "-Dje.rep.skipHelperHostResolution=true ";

    @Override
    public String getDefaultJavaArgs(String overrideJvmArgs) {
        final ParameterMap rnpMap = rnp.getMap();

        String defaultJavaArgs = DEFAULT_MISC_JAVA_ARGS + " ";

        /*
         * Disable JIT compiler for problematic methods. See KVSTORE-374.
         */
        defaultJavaArgs +=
            "-XX:CompileCommand=exclude," +
                "oracle/kv/impl/rep/table/TableManager.getSecondaryDB " +
            "-XX:CompileCommand=exclude," +
                "oracle/kv/impl/rep/table/TableManager.updateIndexKeyCreator ";

        /*
         * When Zing is running, always use the Zing GC params. CMS and G1 are
         * not supported by Zing.
         *
         * For non-Zing JVMs, use the CMS GC params only if CMS is explicitly
         * requested; otherwise use the G1 GC params.
         */
        if (JVMSystemUtils.ZING_JVM) {
            defaultJavaArgs += rnpMap.getOrDefault(
                ParameterState.RN_ZING_GC_PARAMS).asString();
        } else if ((overrideJvmArgs != null) &&
            overrideJvmArgs.contains("-XX:+UseConcMarkSweepGC")) {
            defaultJavaArgs += rnpMap.getOrDefault(
                ParameterState.RN_CMS_GC_PARAMS).asString();
        } else {
            defaultJavaArgs += rnpMap.getOrDefault(
                ParameterState.RN_G1_GC_PARAMS).asString();
        }

        final String resourceName = rnp.getRepNodeId().toString();
        final Parameter numGCLogFiles =
            rnpMap.getOrDefault(ParameterState.RN_NUM_GC_LOG_FILES);
        final Parameter gcLogFileSize =
            rnpMap.getOrDefault(ParameterState.RN_GC_LOG_FILE_SIZE);

        return defaultJavaArgs +
            getGCLoggingArgs(numGCLogFiles, gcLogFileSize, resourceName, rnp);
    }

    /**
     * Called from the service manager.
     */
    @Override
    public ResourceId getResourceId() {
        if (rnp != null) {
            return rnp.getRepNodeId();
        }
        throw new IllegalStateException("No resource id");
    }

    @Override
    public void resetHandles() {
        repNodeAdmin = null;
    }

    /**
     * Reset the command line on restart so we notice parameter changes that
     * affect the command line even if they did not cause the service to
     * restart, in particular if JVM_RN_OVERRIDE_NO_RESTART is set.
     */
    @Override
    public boolean resetOnRestart() {
        return true;
    }

    @Override
    public String getJVMArgs() {
        if (params != null) {
            String args = "";
            if (params.exists(ParameterState.JVM_MISC)) {
                args += params.get(ParameterState.JVM_MISC).asString();
            }
            return args;
        }
        return null;
    }

    @Override
    public String getJVMOverrideArgs() {
        if (params != null) {
            return params.getOrDefault(
                ParameterState.JVM_RN_OVERRIDE).asString() +
                " " +
                params.getOrDefault(
                    ParameterState.JVM_RN_OVERRIDE_NO_RESTART).asString();
        }
        return "";
    }

    /**
     * Returns the environment variables and values associated with the
     * service process.
     */
    @Override
    public Map<String, String> getEnvironment() {
        if (rnp == null) {
            return null;
        }
        final String mallocArenaMax = rnp.getMallocArenaMax();
        return (mallocArenaMax != null) ?
            Collections.singletonMap("MALLOC_ARENA_MAX", mallocArenaMax) : null;
    }

    public RepNodeParams getRepNodeParams() {
        return rnp;
    }

    /**
     * This method is called in the context of the SNA (manager of the service)
     * and not the running service itself.
     */
    public RepNodeAdminAPI getRepNodeAdmin(StorageNodeAgent sna)
        throws RemoteException {

        if (repNodeAdmin == null) {
            try {
                repNodeAdmin = RegistryUtils.getRepNodeAdmin
                    (sna.getStoreName(), sna.getHostname(),
                     sna.getRegistryPort(), rnp.getRepNodeId(),
                     sna.getLoginManager(), sna.getLogger());
            } catch (NotBoundException ne) {
                final String msg = "Cannot get handle from Registry: " +
                    rnp.getRepNodeId().getFullName() + ": " + ne.getMessage();
                sna.getLogger().severe(msg);
                throw new RemoteException(msg, ne);
            }
        }
        return repNodeAdmin;
    }

    /**
     * Like getRepNodeAdmin but with a timeout.  There are two methods because
     * of the timeout overhead, and situations where the handle should be
     * immediately available or it's an error.
     *
     * This method is called in the context of the SNA (manager of the service)
     * and not the running service itself.
     */
    protected RepNodeAdminAPI waitForRepNodeAdmin(StorageNodeAgent sna,
                                                  int timeoutSec) {

        if (repNodeAdmin == null) {
            try {
                final ServiceStatus[] target = {ServiceStatus.RUNNING};
                repNodeAdmin = ServiceUtils.waitForRepNodeAdmin
                    (sna.getStoreName(), sna.getHostname(),
                     sna.getRegistryPort(), rnp.getRepNodeId(),
                     sna.getStorageNodeId(), sna.getLoginManager(),
                     timeoutSec, target, sna.getLogger());
            } catch (Exception e) {
                final String msg =
                    "Cannot get RepNodeAdmin handle from Registry: " +
                    rnp.getRepNodeId().getFullName() + ": " + e.getMessage();
                sna.getLogger().severe(msg);
                return null;
            }
        }
        return repNodeAdmin;
    }

    /**
     * This method must be run in the execution context of the service.
     */
    @Override
    public void start(boolean threads) {
        rns = new RepNodeService(threads);

        /**
         * The ProcessFaultHandler created in the constructor does not have
         * a Logger instance.  It will be reset in newProperties().
         */
        rns.getFaultHandler().setLogger(logger);
        rns.getFaultHandler().execute
            (new ProcessFaultHandler.Procedure<RuntimeException>() {
                @Override
                public void execute() {
                    rns.initialize(sp, rnp, lp);
                    rns.start();
                }
            });
    }

    @Override
    public boolean stopRequested() {
        return (rns != null) && rns.stopRequested();
    }

    /* for tests */
    public RepNodeService getRepNodeService() {
        return rns;
    }
}
