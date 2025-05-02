/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import java.io.File;
import java.io.IOException;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.TestUtils;

/**
 * Creates a Parameters object needed by Admin side.
 */
public class AdminTestConfig {

    public final static String testhost = "localhost";
    public final static int startPort = 6000;
    public final static int haRange = 5;

    /* The directory used for the test. */
    private final File testDir = TestUtils.getTestDir();
    private final PortFinder portFinder;
    private final AdminServiceParams params;

    /**
     * This constructor is used for tests where the registry port is immaterial.
     */
    public AdminTestConfig(String kvstoreName)
        throws IOException {

        this(kvstoreName, new PortFinder(startPort, haRange));
    }

    /**
     * This constructor is used for tests where the registry port needs to be
     * specified externally.
     */
    public AdminTestConfig(String kvstoreName, PortFinder portFinder)
        throws IOException {

        if (portFinder == null) {
            portFinder = new PortFinder(startPort, haRange);
        }
        this.portFinder = portFinder;
        String rootDir = testDir.getCanonicalPath();
        GlobalParams global = new GlobalParams(kvstoreName);

        StorageNodeId storageNodeId = new StorageNodeId(1);
        StorageNodeParams snp =
            new StorageNodeParams(storageNodeId,
                                  testhost,
                                  portFinder.getRegistryPort(),
                                  "a test storage node");
        SecurityParams sp = SecurityParams.makeDefault();

        snp.setRootDirPath(rootDir);
        snp.setHAHostname(testhost);
        snp.setHAPortRange(portFinder.getHaRange());

        AdminParams adminParams = createAdminParams(storageNodeId, 
                                                    snp.getRootDirPath());
        params = new AdminServiceParams(sp, global, snp, adminParams);
    }

    public String getTestHost() {
        return testhost;
    }

    public File getTestDir() {
        return testDir;
    }

    public PortFinder getPortFinder() {
        return portFinder;
    }

    public AdminServiceParams getParams() {
        return params;
    }

    private AdminParams createAdminParams(StorageNodeId snid, 
                                          String adminStorageDir) {
        AdminParams ap =
            new AdminParams(new AdminId(1), snid, null,
                            adminStorageDir);

        /*
         * For unit tests, monitor much more frequently to cause more
         * activity.
         */
        ParameterMap map = ap.getMap();
        map.setParameter(ParameterState.MP_POLL_PERIOD, "1 SECONDS");
        map.setParameter(ParameterState.JE_HOST_PORT, testhost + ":" +
                         portFinder.getHaNextPort());
        map.setParameter(ParameterState.JE_HELPER_HOSTS, testhost + ":" +
                         portFinder.getHaFirstPort());
        return ap;
    }
}
