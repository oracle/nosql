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
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;

import oracle.kv.impl.admin.AdminService;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A class that wraps a bootstrap Admin service which is not associated with a
 * particular kvstore or even fully operational.  This object may "morph" into
 * a full-blown Admin if this SN is chosen to host it.
 */
public class ManagedBootstrapAdmin extends ManagedAdmin {

    private final String bootstrapConfigFile;

     /**
      * Using a single arena results in a smaller, and perhaps more importantly
      * predictable, heap size, which is needed for correct management of
      * native (non-Java heap) memory. Although we do not use native memory
      * directly, it may be used by some Java facilities. With multiple arenas,
      * each arena can grow to some non-deterministic max size wasting memory,
      * and provoking page swapping activity.
      */

    private final String BOOTSTRAP_ADMIN_MALLOC_ARENA_MAX = "1";

    /**
     * Constructor used by the SNA client.
     */
    public ManagedBootstrapAdmin(StorageNodeAgent sna) {
        super(new File(sna.getBootstrapDir()), sna.getSecurityDir(),
              sna.makeBootstrapAdminName());
        bootstrapConfigFile = sna.getBootstrapFile();
    }

    /**
     * Constructor used by the service instance upon startup.
     */
    public ManagedBootstrapAdmin(String rootDir,
                                 String secDir,
                                 String configFile,
                                 String serviceName)
        throws Exception {
        super(new File(rootDir), nullableFile(secDir), serviceName);

        bootstrapConfigFile = configFile;

        /**
         * This Logger will only ever be a console logger because there is no
         * store in which to log as yet.
         */
        logger = LoggerUtils.getLogger(AdminService.class, serviceName);
    }

    /**
     * This method turns a bootstrap admin into a real admin.  It is run on the
     * SNA so that if the bootstrap admin dies and is restarted, it is started
     * using the appropriate AdminParams rather than as a bootstrap admin.
     *
     * The class members must be reset as well as the command line for use by
     * the ProcessMonitor.
     */
    public void resetAsManagedAdmin(AdminParams newAp,
                                    File newRoot,
                                    File newSecDir,
                                    File newSNDir,
                                    String newName,
                                    Logger logger1) {

        this.ap = newAp;
        this.kvRootDir = newRoot;
        this.kvSecDir = newSecDir;
        this.kvSNDir = newSNDir;
        this.kvName = newName;
        this.params = ap.getMap();
        this.serviceName = ap.getAdminId().getFullName();
        this.logger = logger1;
    }

    /**
     * This method must be run in the execution context of the service.
     */
    @Override
    public void start(boolean threads) {
        /**
         * In this path kvSNDir is the rootdir, kvConfigFile is the
         * bootstrap config file.
         */
        final File configPath = new File(kvSNDir, bootstrapConfigFile);
        logger.fine("Starting BootstrapAdmin using configuration file " +
                    configPath);
        final BootstrapParams bp = ConfigUtils.getBootstrapParams(configPath);
        sp = getSecurityParameters();
        sp.initRMISocketPolicies(logger);
        if (!threads) {
            BootstrapParams.initRegistryCSF(sp);
        }

        final AdminService as = new AdminService(bp, sp, threads);
        as.start();
    }

    /**
     * If restarting a bootstrap admin, reset the command line in the event it
     * has turned into a deployed admin.
     */
    @Override
    public boolean resetOnRestart() {
        return true;
    }

    @Override
    public void additionalExecArgs(List<String> command) {
        command.add(StorageNodeAgent.CONFIG_FLAG);
        command.add(bootstrapConfigFile);
    }

    @Override
    public int additionalArgs(String[] args, int index) {
        args[index++] = StorageNodeAgent.CONFIG_FLAG;
        args[index++] = bootstrapConfigFile;
        return index;
    }

    /**
     * Returns the environment variables and values associated with the
     * service process.
     */
    @Override
    public Map<String, String> getEnvironment() {
        return Collections.singletonMap("MALLOC_ARENA_MAX",
                                        BOOTSTRAP_ADMIN_MALLOC_ARENA_MAX);
    }

}
