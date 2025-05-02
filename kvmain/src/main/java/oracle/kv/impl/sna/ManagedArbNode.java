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

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.arb.ArbNodeService;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

public class ManagedArbNode extends ManagedService {

    private SecurityParams sp;
    private ArbNodeParams anp;
    private ArbNodeAdminAPI arbNodeAdmin;
    private LoadParameters lp;
    private ArbNodeService ans;

    /**
     * Constructor used by the SNA client.
     */
    public ManagedArbNode(SecurityParams sp,
                          ArbNodeParams anp,
                          File kvRoot,
                          File kvSNDir,
                          String kvName) {
        super(kvRoot, sp.getConfigDir(), kvSNDir, kvName,
              ARB_NODE_NAME, anp.getArbNodeId().getFullName(), anp.getMap());

        this.sp = sp;
        this.anp = anp;
        arbNodeAdmin = null;
    }

    /**
     * Constructor used by the service instance upon startup.
     */
    public ManagedArbNode(String kvSecDir,
                          String kvSNDir,
                          String kvName,
                          String serviceClass,
                          String serviceName) {

        super(null, nullableFile(kvSecDir), new File(kvSNDir),
              kvName, serviceClass, serviceName, null);
        resetParameters(!usingThreads);
        startLogger(ArbNodeService.class, anp.getArbNodeId(), lp);
        arbNodeAdmin = null;
    }

    @Override
    public void resetParameters(boolean inTarget) {
        sp = getSecurityParameters();
        lp = getParameters();
        params = lp.getMap(serviceName, ParameterState.ARBNODE_TYPE);
        anp = new ArbNodeParams(params);
        anp.validateHeap(inTarget);
    }

    @Override
    public String getDefaultJavaArgs(String overrideJvmArgs) {
        String defaultJavaArgs =
            ManagedRepNode.DEFAULT_MISC_JAVA_ARGS + " " ;

        /*
         * Let the GC parameters default; don't use the RN's settings in
         * particular, since it has completely different (simpler and smaller)
         * heap requirements, and does not cache anything of significance.
         */
        final String resourceName = anp.getArbNodeId().toString();
        final Parameter numGCLogFiles =
            anp.getMap().getOrDefault(ParameterState.RN_NUM_GC_LOG_FILES);
        final Parameter gcLogFileSize =
            anp.getMap().getOrDefault(ParameterState.RN_GC_LOG_FILE_SIZE);

        return defaultJavaArgs +
            getGCLoggingArgs(numGCLogFiles, gcLogFileSize, resourceName, null);
    }

    /**
     * Called from the service manager.
     */
    @Override
    public ResourceId getResourceId() {
        if (anp != null) {
            return anp.getArbNodeId();
        }
        throw new IllegalStateException("No resource id");
    }

    @Override
    public void resetHandles() {
        arbNodeAdmin = null;
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
                       ParameterState.JVM_AN_OVERRIDE).asString();
        }
        return "";
    }

    public ArbNodeParams getArbNodeParams() {
        return anp;
    }

    /**
     * This method is called in the context of the SNA (manager of the service)
     * and not the running service itself.
     */
    public ArbNodeAdminAPI getArbNodeAdmin(StorageNodeAgent sna)
        throws RemoteException {

        if (arbNodeAdmin == null) {
            try {
                arbNodeAdmin = RegistryUtils.getArbNodeAdmin
                    (sna.getStoreName(), sna.getHostname(),
                     sna.getRegistryPort(), anp.getArbNodeId(),
                     sna.getLoginManager(), sna.getLogger());
            } catch (NotBoundException ne) {
                final String msg = "Cannot get handle from Registry: " +
                    anp.getArbNodeId().getFullName() + ": " + ne.getMessage();
                sna.getLogger().severe(msg);
                throw new RemoteException(msg, ne);
            }
        }
        return arbNodeAdmin;
    }

    /**
     * Like getArbNodeAdmin but with a timeout.  There are two methods because
     * of the timeout overhead, and situations where the handle should be
     * immediately available or it's an error.
     *
     * This method is called in the context of the SNA (manager of the service)
     * and not the running service itself.
     */
    protected ArbNodeAdminAPI waitForArbNodeAdmin(StorageNodeAgent sna,
                                                  int timeoutSec) {

        if (arbNodeAdmin == null) {
            try {
                final ServiceStatus[] target = {ServiceStatus.RUNNING};
                arbNodeAdmin = ServiceUtils.waitForArbNodeAdmin
                    (sna.getStoreName(), sna.getHostname(),
                     sna.getRegistryPort(), anp.getArbNodeId(),
                     sna.getStorageNodeId(), sna.getLoginManager(),
                     timeoutSec, target, sna.getLogger());
            } catch (Exception e) {
                final String msg =
                    "Cannot get ArbNodeAdmin handle from Registry: " +
                    anp.getArbNodeId().getFullName() + ": " + e.getMessage();
                sna.getLogger().severe(msg);
                return null;
            }
        }
        return arbNodeAdmin;
    }

    /**
     * This method must be run in the execution context of the service.
     */
    @Override
    public void start(boolean threads) {
        ans = new ArbNodeService(threads);

        /**
         * The ProcessFaultHandler created in the constructor does not have
         * a Logger instance.  It will be reset in newProperties().
         */
        ans.getFaultHandler().setLogger(logger);
        ans.getFaultHandler().execute
            (new ProcessFaultHandler.Procedure<RuntimeException>() {
                @Override
                public void execute() {
                    ans.initialize(sp, anp, lp);
                    ans.start();
                }
            });
    }

    @Override
    public boolean stopRequested() {
        return (ans != null) && ans.stopRequested();
    }
}
