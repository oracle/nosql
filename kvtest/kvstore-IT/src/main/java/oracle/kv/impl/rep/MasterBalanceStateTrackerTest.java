/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static oracle.kv.impl.async.StandardDialogTypeFamily.STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static oracle.kv.impl.util.TestUtils.safeUnexport;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.rmi.registry.Registry;
import oracle.kv.TestBase;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.rep.masterBalance.MasterBalanceStateTracker;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.sna.SNAFaultException;
import oracle.kv.impl.sna.StorageNodeAgentInterfaceResponder;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.StorageNodeAgentNOP;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.impl.node.NameIdPair;

import org.junit.Test;

/**
 * Tests for the RN side of master balancing.
 */
public class MasterBalanceStateTrackerTest extends TestBase {

    KVRepTestConfig config;
    RepNode rn;
    StorageNode sn;
    Topology topo;
    Registry registry;
    ListenHandle registryHandle;

    final StorageNodeId sn1Id = new StorageNodeId(1);

    @Override
    public void setUp() throws Exception {

        super.setUp();
        final int nDC = 1;
        final int nSN = 1;
        final int nPartitions = 10;
        final int repFactor = 1;
        config = new KVRepTestConfig(
            this, nDC, nSN, repFactor, nPartitions);
        rn = config.getRN(new RepNodeId(1,1));
        topo = config.getTopology();

        sn = topo.get(sn1Id);
        registry = TestUtils.createRegistry(sn.getRegistryPort());
        if (AsyncRegistryUtils.serverUseAsync) {
            registryHandle =
                TestUtils.createServiceRegistry(sn.getRegistryPort());
        }
    }

    @Override
    public void tearDown() throws Exception {

        if (registry != null) {
            TestUtils.destroyRegistry(registry);
        }
        if (registryHandle != null) {
            registryHandle.shutdown(true);
        }

        /* Clear the hook */
        ContextProxy.beforeInvokeNoAuthRetry = null;
        super.tearDown();
    }

    /* Tests that the SNA is informed of state changes. */
    @Test
    public void testBasic()
        throws IOException {

        testStateNotify(null);
    }

    @Test
    public void testNotifyFailure()
        throws Exception {

        testStateNotify(new TestUtils.CountDownFaultHook(3 /* fault count */,
            MasterBalanceStateTracker.class.getSimpleName(),
            new SessionAccessException("sae")));
    }

    @Test
    public void testFaultException()
        throws Exception {

        testStateNotify(new TestUtils.CountDownFaultHook(3 /* fault count */,
            MasterBalanceStateTracker.class.getSimpleName(),
            new SNAFaultException(new SessionAccessException("sae"))));
    }

    private void testStateNotify(TestHook<Integer> hook)
        throws IOException {

        final SNAMock snai = new SNAMock();

        /* Create the SNA registry entry. */
        tearDowns.add(() -> safeUnexport(snai));
        final ListenHandle listenHandle =
            RegistryUtils.rebind(
                sn.getHostname(),
                sn.getRegistryPort(),
                topo.getKVStoreName(),
                sn1Id,
                RegistryUtils.InterfaceType.MAIN,
                snai,
                DEFAULT_CSF,
                DEFAULT_SSF,
                STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY,
                () -> new StorageNodeAgentInterfaceResponder(
                    snai, DEFAULT_THREAD_POOL, logger),
                logger);
        if (listenHandle != null) {
            tearDowns.add(() -> listenHandle.shutdown(true));
        }
        final MasterBalanceStateTracker mbsm =
                new MasterBalanceStateTracker(rn, logger) {
            @Override
            protected int getTopoSeqNum() {
                return 100;
            }

            @Override
            protected boolean ensureTopology() {
                /* NOP */
                return true;
            }
        };
        if (hook != null) {
            /* Inject test hook */
            ContextProxy.beforeInvokeNoAuthRetry = hook;
        }
        mbsm.start();

        assertNull(snai.stateInfo);

        final NameIdPair rn1NameId =
                new NameIdPair(rn.getRepNodeId().getFullName(), 1);
        final StateChangeEvent sce =
                new StateChangeEvent(ReplicatedEnvironment.State.MASTER,
                                     rn1NameId);
        mbsm.noteStateChange(sce);

        /* Wait for the SNA to be informed of the master change. */
        assertTrue(new PollCondition(10, 100000) {

            @Override
            protected boolean condition() {
               return (snai.stateInfo != null) &&
                      snai.stateInfo.getState().isMaster();
            }

        }.await());

        mbsm.shutdown();
    }

    /**
     * Mock SNA to receive state changes.
     */
    static private final class SNAMock extends StorageNodeAgentNOP {
        private StateInfo stateInfo;

        @Override
        public void noteState(StateInfo si, AuthContext authContext,
                              short serialVersion) {
            this.stateInfo = si;
        }
    }
}
