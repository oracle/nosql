/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static oracle.kv.impl.async.StandardDialogTypeFamily.TRUSTED_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.TestProcessFaultHandler;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Test;

/**
 * Test the InternalLoginManager class.  InternalLogin is performed using
 * SSL client certificates, so no application-level authentication is performed.
 */
public class InternalLoginManagerTest extends LoginTestBase {

    private static final String TRUSTED_BS_LOGIN = "SNA:TRUSTED_LOGIN";
    private static final String TRUSTED_SN1_LOGIN = "SNA:TRUSTED_LOGIN";
    private static final String TRUSTED_SN2_LOGIN = "SNA:TRUSTED_LOGIN";
    private static final String HOSTNAME = "localhost";

    private int registryPort1 = 0;
    private Registry registry1;
    private ListenHandle registryHandle1;

    private int registryPort2 = 0;
    private Registry registry2;
    private ListenHandle registryHandle2;

    @Override
    public void setUp()
        throws Exception {

        registryPort1 =
            new FreePortLocator(HOSTNAME, 5050, 5100).next();

        registry1 = TestUtils.createRegistry(registryPort1);
        if (AsyncRegistryUtils.serverUseAsync) {
            registryHandle1 = TestUtils.createServiceRegistry(registryPort1);
        }

        registryPort2 =
            new FreePortLocator(HOSTNAME, 5150, 5200).next();

        registry2 = TestUtils.createRegistry(registryPort2);
        if (AsyncRegistryUtils.serverUseAsync) {
            registryHandle2 = TestUtils.createServiceRegistry(registryPort2);
        }
    }

    @Override
    public void tearDown()
        throws Exception {

        if (registry1 != null) {
            TestUtils.destroyRegistry(registry1);
        }
        if (registryHandle1 != null) {
            registryHandle1.shutdown(true);
        }
        if (registry2 != null) {
            TestUtils.destroyRegistry(registry2);
        }
        if (registryHandle2 != null) {
            registryHandle2.shutdown(true);
        }
        super.tearDown();
    }

    @Test
    public void testBSILM()
        throws RemoteException {

        assertTrue(registryPort1 != registryPort2);

        final TLFH tlfh = new TLFH(logger);

        final StorageNodeId rid1 = new StorageNodeId(1);
        final TrustedLoginHandler tlh1 = new TrustedLoginHandler(rid1, true);
        final TrustedLoginImpl tli1 = new TrustedLoginImpl(tlfh, tlh1, logger);
        tearDownListenHandle(
            RegistryUtils.rebind(HOSTNAME, registryPort1, TRUSTED_BS_LOGIN,
                                 rid1,
                                 export(tli1),
                                 DEFAULT_CSF,
                                 DEFAULT_SSF,
                                 TRUSTED_LOGIN_TYPE_FAMILY,
                                 () ->
                                 new TrustedLoginResponder(
                                     tli1, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final StorageNodeId rid2 = new StorageNodeId(2);
        final TrustedLoginHandler tlh2 = new TrustedLoginHandler(rid2, true);
        final TrustedLoginImpl tli2 = new TrustedLoginImpl(tlfh, tlh2, logger);
        tearDownListenHandle(
            RegistryUtils.rebind(HOSTNAME, registryPort2, TRUSTED_BS_LOGIN,
                                 rid2,
                                 export(tli2),
                                 DEFAULT_CSF,
                                 DEFAULT_SSF,
                                 TRUSTED_LOGIN_TYPE_FAMILY,
                                 () -> new TrustedLoginResponder(
                                     tli2, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final InternalLoginManager ilm =
            new InternalLoginManager(null, logger);
        final HostPort targ1 = new HostPort(HOSTNAME, registryPort1);
        final HostPort targ2 = new HostPort(HOSTNAME, registryPort2);
        LoginToken savelt1 = null;

        /* get login for sn1 */
        final LoginHandle lh1 = ilm.getHandle(targ1,
                                              ResourceType.STORAGE_NODE);
        assertNotNull(lh1);
        final LoginToken lt1 = lh1.getLoginToken();
        assertNotNull(lt1);

        /* get login for sn2 */
        final LoginHandle lh2 = ilm.getHandle(targ2,
                                              ResourceType.STORAGE_NODE);
        assertNotNull(lh2);
        final LoginToken lt2 = lh2.getLoginToken();
        assertNotNull(lt2);

        /* Session ids should not match */
        assertFalse(lt1.getSessionId().equals(lt2.getSessionId()));

        /* Session id allocators should not match */
        assertFalse(lt1.getSessionId().getAllocator().equals(
                        lt2.getSessionId().getAllocator()));

        /* repeat sn1 */
        final LoginHandle lh1a = ilm.getHandle(targ1,
                                               ResourceType.STORAGE_NODE);
        assertNotNull(lh1a);
        final LoginToken lt1a = lh1a.getLoginToken();
        assertNotNull(lt1a);
        assertTrue(lt1.getSessionId().equals(lt1a.getSessionId()));

        /* renew sn1 token */
        final LoginToken lt1b = lh1.renewToken(lt1);
        assertNotNull(lt1b);
        assertFalse(lt1.getSessionId().equals(lt1b.getSessionId()));

        /* remember this for later */
        savelt1 = lt1b;
        Subject subj = tlh1.validateLoginToken(savelt1, null);
        assertNotNull(subj);

        /* logout */
        ilm.logout();

        subj = tlh1.validateLoginToken(savelt1, null);
        assertNull(subj);
    }

    @Test
    public void testILM()
        throws RemoteException {

        assertTrue(registryPort1 != registryPort2);

        final TLFH tlfh = new TLFH(logger);

        final StorageNodeId rid1 = new StorageNodeId(1);
        final TrustedLoginHandler tlh1 = new TrustedLoginHandler(rid1, false);
        final TrustedLoginImpl tli1 = new TrustedLoginImpl(tlfh, tlh1, logger);
        tearDownListenHandle(
            RegistryUtils.rebind(HOSTNAME, registryPort1, TRUSTED_SN1_LOGIN,
                                 rid1,
                                 export(tli1),
                                 DEFAULT_CSF,
                                 DEFAULT_SSF,
                                 TRUSTED_LOGIN_TYPE_FAMILY,
                                 () -> new TrustedLoginResponder(
                                     tli1, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final StorageNodeId rid2 = new StorageNodeId(2);
        final TrustedLoginHandler tlh2 = new TrustedLoginHandler(rid2, false);
        final TrustedLoginImpl tli2 = new TrustedLoginImpl(tlfh, tlh2, logger);
        tearDownListenHandle(
            RegistryUtils.rebind(HOSTNAME, registryPort2, TRUSTED_SN2_LOGIN,
                                 rid2,
                                 export(tli2),
                                 DEFAULT_CSF,
                                 DEFAULT_SSF,
                                 TRUSTED_LOGIN_TYPE_FAMILY,
                                 () -> new TrustedLoginResponder(
                                     tli2, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final InternalLoginManager ilm =
            new InternalLoginManager(new TopoResolver(), logger);
        final HostPort targ1 = new HostPort(HOSTNAME, registryPort1);
        final HostPort targ2 = new HostPort(HOSTNAME, registryPort2);
        LoginToken savelt1 = null;

        /* get login for sn1 */
        final LoginHandle lh1 = ilm.getHandle(targ1,
                                              ResourceType.STORAGE_NODE);
        assertNotNull(lh1);
        final LoginToken lt1 = lh1.getLoginToken();
        assertNotNull(lt1);

        /* get login for sn2 */
        final LoginHandle lh2 = ilm.getHandle(targ2,
                                              ResourceType.STORAGE_NODE);
        assertNotNull(lh2);
        final LoginToken lt2 = lh2.getLoginToken();
        assertNotNull(lt2);

        /* Session ids should not match */
        assertFalse(lt1.getSessionId().equals(lt2.getSessionId()));

        /* Session id allocators should not match */
        assertFalse(lt1.getSessionId().getAllocator().equals(
                        lt2.getSessionId().getAllocator()));

        /* repeat sn1 */
        final LoginHandle lh1a = ilm.getHandle(targ1,
                                               ResourceType.STORAGE_NODE);
        assertNotNull(lh1a);
        final LoginToken lt1a = lh1a.getLoginToken();
        assertNotNull(lt1a);
        assertTrue(lt1.getSessionId().equals(lt1a.getSessionId()));

        /* renew sn1 token */
        final LoginToken lt1b = lh1.renewToken(lt1);
        assertNotNull(lt1b);
        assertFalse(lt1.getSessionId().equals(lt1b.getSessionId()));

        /* remember this for later */
        savelt1 = lt1b;
        Subject subj = tlh1.validateLoginToken(savelt1, null);
        assertNotNull(subj);

        /* repeat using resource ids */
        /* get login for sn1 */
        final LoginHandle lh3 = ilm.getHandle(rid1);
        assertNotNull(lh3);
        final LoginToken lt3 = lh3.getLoginToken();
        assertNotNull(lt3);

        /* get login for sn2 */
        final LoginHandle lh4 = ilm.getHandle(rid2);
        assertNotNull(lh4);
        final LoginToken lt4 = lh4.getLoginToken();
        assertNotNull(lt4);

        /* Session ids should not match */
        assertFalse(lt3.getSessionId().equals(lt4.getSessionId()));

        /* Session id allocators should not match */
        assertFalse(lt3.getSessionId().getAllocator().equals(
                        lt4.getSessionId().getAllocator()));

        /* savelt1, lt3 should be equal */
        assertEquals(savelt1.getSessionId(), lt3.getSessionId());

        /* renew sn2 token */
        final LoginToken lt4b = lh3.renewToken(lt4);
        assertNotNull(lt4b);
        assertFalse(lt4.getSessionId().equals(lt4b.getSessionId()));

        subj = tlh1.validateLoginToken(lt4b, null);
        assertNotNull(subj);

        /* logout */
        ilm.logout();

        subj = tlh1.validateLoginToken(savelt1, null);
        assertNull(subj);
    }

    static class TLFH extends TestProcessFaultHandler {
        TLFH(Logger logger) {
            super(logger, ProcessExitCode.RESTART);
        }

        @Override
        public void queueShutdownInternal(Throwable th, ProcessExitCode pec) {
            fail("queueShutdownInternal called");
        }
    }

    class TopoResolver implements TopologyResolver {
        private final SNInfo sn1;
        private final SNInfo sn2;

        TopoResolver() {
            sn1 = new SNInfo(HOSTNAME, registryPort1, new StorageNodeId(1));
            sn2 = new SNInfo(HOSTNAME, registryPort2, new StorageNodeId(2));
        }

        @Override
        public SNInfo getStorageNode(ResourceId target) {
            if (target instanceof StorageNodeId) {
                final StorageNodeId snid = (StorageNodeId) target;
                if (snid.getStorageNodeId() == 1) {
                    return sn1;
                } else if (snid.getStorageNodeId() == 2) {
                    return sn2;
                }
            }
            return null;
        }

        @Override
        public List<RepNodeId> listRepNodeIds(
            int maxRns) {
            return null;
        }
    }
}
