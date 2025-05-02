/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

import oracle.kv.KVStoreConfig;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.security.SignatureFaultException;
import oracle.kv.impl.security.TopoSignatureHelper;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SignedTopologyTest extends SecureTestBase {

    private static final String ADMIN_NAME = "admin";
    private static final String ADMIN_PW = "NoSql00__7654321";

    private static final String USER_NAME = "user";
    private static final String USER_PW = "NoSql00__7654321";

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        users.add(new SecureUser(ADMIN_NAME, ADMIN_PW, true /* admin */));
        users.add(new SecureUser(USER_NAME, USER_PW, false /* admin */));
        numSNs = 3;
        repFactor = 3;
        secureOpts = new SecureOpts().setSecure(true);

        startup();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        shutdown();
    }

    @Override
    public void setUp()
        throws Exception {

        createStore.getStorageNodeAgent(0).resetRMISocketPolicies();
    }

    @Override
    public void tearDown()
        throws Exception {

        /* Run tearDowns here since we're not calling super */
        tearDowns.forEach(Runnable::run);
        tearDowns.clear();
    }

    @Test
    public void testTopologyHasSignature() throws Exception {
        final Topology topo = getTopology();
        final int topoSeq = topo.getSequenceNumber();
        final Set<RepNodeId> rnIds = topo.getRepNodeIds();

        for (RepNodeId rnid : rnIds) {
            final RepNodeAdminAPI rnai =
                createStore.getRepNodeAdmin(rnid,
                                            USER_NAME,
                                            USER_PW.toCharArray());

            byte[] topoSig = rnai.getTopology().getSignature();
            assertTrue(topoSig != null && topoSig.length > 0);

            final Topology rnTopo =
                (Topology) rnai.getMetadata(MetadataType.TOPOLOGY);
            topoSig = rnTopo.getSignature();
            assertTrue(topoSig != null && topoSig.length > 0);

            final TopologyInfo topoInfo =
                (TopologyInfo) rnai.getMetadata(MetadataType.TOPOLOGY,
                                                topoSeq - 1);
            topoSig = topoInfo.getTopoSignature();
            assertTrue(topoSig != null && topoSig.length > 0);
        }
    }

    @Test
    public void testInvalidTopologyUpdate() throws Exception {
        final Topology topo = getTopology();
        final int topoSeq = topo.getSequenceNumber();
        final RepNodeId rnIds[] =
            topo.getRepNodeIds().toArray(new RepNodeId[0]);

        final RepNodeAdminAPI rnai =
                createStore.getRepNodeAdmin(rnIds[0],
                                            USER_NAME,
                                            USER_PW.toCharArray());

        final Topology topoWithRnRemoved = rnai.getTopology();
        topoWithRnRemoved.remove(rnIds[0]);

        /*
         * Signature should be invalid now, neither full nor delta update is
         * allowed
         */
        TopologyInfo topoChangeInfo =
            topoWithRnRemoved.getChangeInfo(topoSeq + 1);

        fullTopoUpdateInvalid(topoWithRnRemoved, rnai);
        deltaTopoUpdateInvalid(topoChangeInfo, rnai);

        /* With no signature, neither full nor delta update is allowed */
        topoWithRnRemoved.stripSignature();
        topoChangeInfo = topoWithRnRemoved.getChangeInfo(topoSeq + 1);

        fullTopoUpdateInvalid(topoWithRnRemoved, rnai);
        deltaTopoUpdateInvalid(topoChangeInfo, rnai);
    }

    @Test
    public void testValidTopologyUpdate() throws Exception {
        final TopoSignatureHelper topoSignature = getTopoSignatureHelper();
        final Topology topo = getTopology();
        final RepNodeId rnIds[] =
            topo.getRepNodeIds().toArray(new RepNodeId[0]);

        final RepNodeAdminAPI rnai =
                createStore.getRepNodeAdmin(rnIds[0],
                                            USER_NAME,
                                            USER_PW.toCharArray());

        /* Bumps the topo seq by adding a new dc */
        final Topology newTopo = rnai.getTopology().getCopy();
        newTopo.add(Datacenter.newInstance("dc2", 3 /* rf */,
                                           DatacenterType.PRIMARY, false,
                                           false));
        byte[] topoSigBytes = topoSignature.sign(newTopo);
        newTopo.updateSignature(topoSigBytes);

        /* Test valid full topo update */
        rnai.updateMetadata(newTopo);
        for (int i = 0; i < rnIds.length; i++) {
            waitForUpdate(createStore.getRepNodeAdmin(i),
                          newTopo.getSequenceNumber());
        }

        /* Bumps topo seq by adding another new dc */
        final int origSeq = newTopo.getSequenceNumber();
        newTopo.add(Datacenter.newInstance("dc3", 3 /* rf */,
                                           DatacenterType.PRIMARY, false,
                                           false));
        topoSigBytes = topoSignature.sign(newTopo);
        newTopo.updateSignature(topoSigBytes);
        final TopologyInfo topoInfo = newTopo.getChangeInfo(origSeq + 1);

        /* Test valid delta topo update */
        rnai.updateMetadata(topoInfo);
        for (int i = 0; i < rnIds.length; i++) {
            waitForUpdate(createStore.getRepNodeAdmin(i),
                          newTopo.getSequenceNumber());
        }
    }

    @Test
    public void testTopoUpdateFromClient() throws Exception {
        final TopoSignatureHelper topoSignature = getTopoSignatureHelper();
        final String host = createStore.getHostname();
        final int port = createStore.getRegistryPort();

        final LoginManager loginMgr =
            KVStoreLogin.getRepNodeLoginMgr(host, port,
                                            new PasswordCredentials(
                                                USER_NAME,
                                                USER_PW.toCharArray()),
                                            createStore.getStoreName());
        final KVStoreConfig storeConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              host + ":" + port);
        final ClientId clientId = new ClientId(1000);

        final RequestDispatcherImpl dispatcher =
            RequestDispatcherImpl.createForClient(
                storeConfig,
                clientId,
                loginMgr,
                this,
                ClientLoggerUtils.getLogger(RequestDispatcherImpl.class,
                                            "test"));

        /* Client should have signed topology */
        byte[] clientTopoSig =
            dispatcher.getTopologyManager().getTopology().getSignature();
        assertTrue(clientTopoSig != null && clientTopoSig.length > 0);

        Topology newTopo =
            dispatcher.getTopologyManager().getTopology().getCopy();
        int origTopoSeq = newTopo.getSequenceNumber();

        /* Bumps topo seq on client side */
        newTopo.add(Datacenter.newInstance("dc2", 3 /* rf */,
                                           DatacenterType.PRIMARY, false,
                                           false));
        assertTrue(dispatcher.getTopologyManager().update(newTopo));

        /*
         * Wait a moment, new topo on client side should propagate to rns via
         * NOP, but should not succeed due to invalid signature
         */
        Thread.sleep(5000);

        final RepNodeId rnIds[] =
            newTopo.getRepNodeIds().toArray(new RepNodeId[0]);
        for (int i = 0; i < rnIds.length; i++) {
            assertEquals(origTopoSeq,
                         createStore.getRepNodeAdmin(i).getTopoSeqNum());
        }

        newTopo = newTopo.getCopy();
        /* Bumps topo seq on client side, and set valid signature */
        newTopo.add(Datacenter.newInstance("dc3", 3 /* rf */,
                                           DatacenterType.PRIMARY, false,
                                           false));
        clientTopoSig = topoSignature.sign(newTopo);
        newTopo.updateSignature(clientTopoSig);
        assertTrue(dispatcher.getTopologyManager().update(newTopo));

        /*
         * New topo on client side should propagate to rns via NOP and update
         * successfully
         */
        for (int i = 0; i < rnIds.length; i++) {
            waitForUpdate(createStore.getRepNodeAdmin(i),
                          newTopo.getSequenceNumber());
        }
    }

    /**
     * Test that a failure when attempting to sign a topology produces a stack
     * trace [KVSTORE-553]
     */
    @Test
    public void testLogFailedSignTopology() throws Exception {

        /* Collect warnings */
        final List<LogRecord> warnings = new ArrayList<>();
        final Handler warningsHandler = new StreamHandler() {
            @Override
            public synchronized void publish(LogRecord record) {
                if ((record != null) && (record.getLevel() == Level.WARNING)) {
                    warnings.add(record);
                }
            }
        };
        tearDowns.add(() -> logger.removeHandler(warningsHandler));
        logger.addHandler(warningsHandler);

        /* Inject failure during signing */
        tearDowns.add(() -> { TopoSignatureHelper.signTestHook = null; });
        TopoSignatureHelper.signTestHook = t -> {
            throw new SignatureFaultException("Injected failure", null);
        };

        /* Sign */
        final TopoSignatureManager tsm =
            new TopoSignatureManager(getTopoSignatureHelper(), logger);
        final Topology topo = getTopology();
        assertFalse(tsm.signTopology(topo));

        /* Make sure we get a warning with the exception in it */
        assertEquals(1, warnings.size());
        final LogRecord record = warnings.get(0);
        assertTrue(record.getMessage(),
                   record.getMessage().contains(
                       "Failed to generate signature"));
        assertTrue("Exception: " + record.getThrown(),
                   record.getThrown() instanceof SignatureFaultException);
    }

    /**
     * Test that a failure when attempting to verify a topology produces a
     * stack trace [KVSTORE-553]
     */
    @Test
    public void testLogFailedVerifyTopology() throws Exception {

        /* Collect warnings */
        final List<LogRecord> warnings = new ArrayList<>();
        final Handler warningsHandler = new StreamHandler() {
            @Override
            public synchronized void publish(LogRecord record) {
                if ((record != null) && (record.getLevel() == Level.WARNING)) {
                    warnings.add(record);
                }
            }
        };
        tearDowns.add(() -> logger.removeHandler(warningsHandler));
        logger.addHandler(warningsHandler);

        /* Inject failure during verify */
        tearDowns.add(() -> { TopoSignatureHelper.verifyTestHook = null; });
        TopoSignatureHelper.verifyTestHook = t -> {
            throw new SignatureFaultException("Injected failure", null);
        };

        /* Verify a signed topology */
        final TopoSignatureHelper tsHelper = getTopoSignatureHelper();
        final TopoSignatureManager tsm =
            new TopoSignatureManager(tsHelper, logger);
        final Topology topo = getTopology().getCopy();
        topo.updateSignature(tsHelper.sign(topo));
        assertFalse(tsm.verifyTopology(topo));

        /* Make sure we get a warning with the exception in it */
        assertEquals("Warnings: " + warnings, 1, warnings.size());
        final LogRecord record = warnings.get(0);
        assertTrue(record.getMessage(),
                   record.getMessage().contains(
                       "Problem verifying signature"));
        assertTrue("Exception: " + record.getThrown(),
                   record.getThrown() instanceof SignatureFaultException);
    }

    private static TopoSignatureHelper getTopoSignatureHelper() {
        final File secDir =
            new File(TestUtils.getTestDir(), FileNames.SECURITY_CONFIG_DIR);
        final SecurityParams sp = ConfigUtils.getSecurityParams(
            new File(secDir, FileNames.SECURITY_CONFIG_FILE));
        return TopoSignatureHelper.buildFromSecurityParams(sp);
    }

    private void waitForUpdate(final RepNodeAdminAPI rnai,
                               final int topoSeq) {
        boolean success = new PollCondition(500, 20000) {
            @Override
            protected boolean condition() {
                try {
                    return rnai.getTopoSeqNum() >= topoSeq;
                } catch (RemoteException e) {
                    return false;
                }
            }
        }.await();
        assertTrue(success);
    }

    private void fullTopoUpdateInvalid(Topology topo, RepNodeAdminAPI rnai)
        throws RemoteException {

        try {
            rnai.updateMetadata(topo);
            fail("Expected OperationFaultException");
        } catch (RepNodeAdminFaultException rnafe) {
            assertEquals(OperationFaultException.class.getName(),
                         rnafe.getFaultClassName());
        }
    }

    private void deltaTopoUpdateInvalid(TopologyInfo topoInfo,
                                        RepNodeAdminAPI rnai)
        throws RemoteException {

        final int origTopoSeq = rnai.getTopoSeqNum();
        int newTopoSeq = rnai.updateMetadata(topoInfo);

        /* Unchanged topo seq indicates update does not succeed */
        assertEquals(origTopoSeq, newTopoSeq);
    }
}
