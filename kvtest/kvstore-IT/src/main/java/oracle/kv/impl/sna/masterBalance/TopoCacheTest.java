/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna.masterBalance;

import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static oracle.kv.impl.util.TestUtils.safeUnexport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.rep.admin.RepNodeAdminResponder;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager.SNInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.StateInfo;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.TopoUtils;
import oracle.kv.impl.util.RepNodeAdminNOP;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

import org.junit.Test;

/**
 * Test for the topology cache used to cache a Topology during master balancing.
 */
public class TopoCacheTest extends TestBase {

    final static LoginManager NULL_LOGIN_MGR = null;

    /* RF of 2 to get two rns/sn. */
    final static int RF = 2;

    final RepNodeId rn11Id = new RepNodeId(1, 1);
    final RepNodeId rn12Id = new RepNodeId(1, 2);
    final StorageNodeId sn1Id = new StorageNodeId(1);

    final StorageNodeId sn2Id = new StorageNodeId(2);
    Topology topo ;
    StorageNode sn1;

    TopoCache topoCache;
    SNInfo snInfo;
    Registry registry;
    ListenHandle registryHandle;

    @Override
    public void setUp() throws Exception {

        topo = TopoUtils.create("burl", 1, 3, RF, 100);
        sn1 = topo.get(sn1Id);

        snInfo = new MasterBalanceManager.SNInfo(topo.getKVStoreName(),
                                                 sn1Id,
                                                 sn1.getHostname(),
                                                 sn1.getRegistryPort());
        topoCache = new TopoCache(snInfo, logger, NULL_LOGIN_MGR) {

            @Override
            Set<RepNodeId> getActiveRNs() {
               HashSet<RepNodeId> activeRNs = new HashSet<RepNodeId>();
               activeRNs.add(rn11Id);
               return activeRNs;
            }
        };

        /* Set up registry on sn1, and bind the rn11 RepNodeAdmin interface. */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, NULL_LOGIN_MGR, logger);

        registry = TestUtils.createRegistry(sn1.getRegistryPort());
        if (AsyncRegistryUtils.serverUseAsync) {
            registryHandle =
                TestUtils.createServiceRegistry(sn1.getRegistryPort());
        }

        /* Bind mock object to return topology. */
        final RepNodeAdminNOP rnAdmin = new RepNodeAdminNOP() {
            @Override
            public Topology getTopology(AuthContext ac, short serialVersion) {
                return topo;
            }

            @Override
            public int getTopoSeqNum(AuthContext ac, short sv) {
                return topo.getSequenceNumber();
            }
        };
        tearDowns.add(() -> safeUnexport(rnAdmin));
        final ListenHandle listenHandle = regUtils.rebind(
            rn11Id, rnAdmin, DEFAULT_CSF, DEFAULT_SSF,
            () -> new RepNodeAdminResponder(rnAdmin, DEFAULT_THREAD_POOL,
                                            logger));
        if (listenHandle != null) {
            tearDowns.add(() -> listenHandle.shutdown(true));
        }
    }

    @Override
    public void tearDown() throws Exception {
        TopoCache.FAULT_HOOK = null;
        if (registry != null) {
            TestUtils.destroyRegistry(registry);
        }
        if (registryHandle != null) {
            registryHandle.shutdown(true);
        }
    }

    @Test
    public void testBasic() throws InterruptedException {

        assertTrue(!topoCache.isInitialized());
        topoCache.noteLatestTopo(new StateInfo(rn11Id, State.MASTER,
                                               topo.getSequenceNumber()));
        topoCache.ensureTopology();

        assertEquals(topoCache.getTopology().getSequenceNumber(),
                     topo.getSequenceNumber());

        /* update the topology. */
        topo.add(Datacenter.newInstance("london", 3,
                                        DatacenterType.PRIMARY, false, false));

        topoCache.noteLatestTopo(new StateInfo(rn11Id, State.MASTER,
                                               topo.getSequenceNumber()));

        /* Verify that cached topology is obsolete. */
        assertTrue(topo.getSequenceNumber() >
                   topoCache.getTopology().getSequenceNumber());

        /* verify that that the cache is updated. */
        topoCache.ensureTopology();
        assertEquals(topoCache.getTopology().getSequenceNumber(),
                     topo.getSequenceNumber());

        assertEquals("Primary RF", 5, topoCache.getPrimaryRF());

        assertEquals(2, topoCache.getRnCount());

        /* Reduce the validation period for the test. */
        TopoCache.setValidationIntervalMs(5000);

        /* update the topology. */
        topo.add(Datacenter.newInstance("tokyo", 3,
                                        DatacenterType.PRIMARY, false, false));

        Thread.sleep(2 * TopoCache.getValidationIntervalMs());

        /* This time there is no call to noteLatestTopo */
        topoCache.ensureTopology();
        assertTrue(topoCache.getTopology().getSequenceNumber() ==
                   topo.getSequenceNumber());

        /* Update cache and check primary RF */
        topoCache.noteLatestTopo(new StateInfo(rn11Id, State.MASTER,
                                               topo.getSequenceNumber()));
        topoCache.ensureTopology();
        assertEquals("Primary RF", 8, topoCache.getPrimaryRF());

        /* Make sure secondary zone doesn't change primary RF */
        topo.add(Datacenter.newInstance("nara", 3,
                                        DatacenterType.SECONDARY, false,
                                        false));
        topoCache.noteLatestTopo(new StateInfo(rn11Id, State.MASTER,
                                               topo.getSequenceNumber()));
        topoCache.ensureTopology();
        assertEquals("Primary RF", 8, topoCache.getPrimaryRF());

        TopoCache.FAULT_HOOK = new FaultTestHook(
            new SessionAccessException("sae"));
        topoCache.ensureTopology();

        TopoCache.FAULT_HOOK = new FaultTestHook(
            new RepNodeAdminFaultException(new SessionAccessException("sae")));
        topoCache.ensureTopology();

        topoCache.shutdown();
    }

    /*
     * A test hook that throws the specified exception
     */
    class FaultTestHook implements TestHook<Integer> {
        private final RuntimeException fault;

        FaultTestHook(RuntimeException fault) {
            this.fault = fault;
        }

        @Override
        public void doHook(Integer unused) {
            throw fault;
        }
    }
}
