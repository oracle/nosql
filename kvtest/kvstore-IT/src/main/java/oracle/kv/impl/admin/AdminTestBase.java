/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.util.ConfigurableService.ServiceStatus.RUNNING;
import static oracle.kv.util.CreateStore.MB_PER_SN;

import oracle.kv.TestBase;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Simple base class for Admin test classes.
 */
public class AdminTestBase extends TestBase {

    private final static int startPort1 = 13230;
    private final static int startPort2 = 13250;
    private final static int startPort3 = 13270;
    protected final static int haRange = 5;

    protected AdminTestConfig atc;
    protected StorageNodeAgent sna1;
    protected StorageNodeAgent sna2;
    protected StorageNodeAgent sna3;
    protected PortFinder portFinder1;
    protected PortFinder portFinder2;
    protected PortFinder portFinder3;
    protected LoginManager loginMgr;
    protected final boolean secured;

    protected AdminTestBase() {
        this.secured = false;
    }

    protected AdminTestBase(boolean secured) {
        this.secured = secured;
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();

        portFinder1 = new PortFinder(startPort1, haRange);
        portFinder2 = new PortFinder(startPort2, haRange);
        portFinder3 = new PortFinder(startPort3, haRange);

    	atc = new AdminTestConfig(kvstoreName, portFinder1);

        sna1 = StorageNodeTestBase.createUnregisteredSNA
            (portFinder1, 1, "config0.xml", false, true, secured, MB_PER_SN);
        sna2 = StorageNodeTestBase.createUnregisteredSNA
            (portFinder2, 1, "config1.xml", false, false, secured, MB_PER_SN);
        sna3 = StorageNodeTestBase.createUnregisteredSNA
            (portFinder3, 1, "config2.xml", false, false, secured, MB_PER_SN);
        sna1.waitForAdmin(RUNNING, 10);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (sna1 != null) {
            sna1.shutdown(true, true);
        }

        if (sna2 != null) {
            sna2.shutdown(true, true);
        }

        if (sna3 != null) {
            sna3.shutdown(true, true);
        }

        LoggerUtils.closeAllHandlers();
    }
}
