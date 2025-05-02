/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static oracle.kv.impl.util.KVRepTestConfig.RG1_RN1_ID;
import static oracle.kv.impl.util.KVRepTestConfig.RG1_RN2_ID;
import static oracle.kv.impl.util.KVRepTestConfig.RG1_RN3_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.elections.Acceptor;
import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.PollCondition;

/**
 * KVS level network partition tests.
 */
public class RequestDispatcherNetworkPartitionTest extends
    RequestDispatcherTestBase {

    private final LoginManager LOGIN_MGR = null;

    @Override
    public void setUp() throws Exception {

        nSN = 1;
        super.setUp();
        dispatcher.getStateUpdateThread().start();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /**
     * Test to verify that a client on a segmented network (the client and HA
     * are on different segments), correctly switches over to the new master
     * on the majority side of the partition. This switchover happens when
     * the dispatcher in the client gets the new master in a response either
     * a read request or a NOP from the state update thread as in the case of
     * this test.
     *
     * The test sequence is:
     * 1) Create a 3 node rg, rg1-rn1 is the master.
     * 2) Isolate rg1-rn1 so that it's the master on the minority side of the
     * partition.
     * 3) Force rg1-rn2 to become the master on the majority side.
     * 4) Verify that the client can direct write requests to the new master
     * rg1-rn2.
     * 5) Heal the partition
     * 6) Verify that rg1-rn1 transitions to a replica and rg1-rn2 continues
     * to remain the master.
     */
    @Test
    public void testNetworkPartition()
        throws DatabaseException, InterruptedException {

        final Response resp = dispatcher.execute(createPutRequest(), LOGIN_MGR);
        assertEquals(RG1_RN1_ID, resp.getRespondingRN());

        final RepImpl rn1Impl = config.getRN(RG1_RN1_ID).getEnvImpl(60000);

        /*
         * Isolate the master so that it can't participate in elections; the
         * client can still talk to the node.
         */
        disableServices(rn1Impl);

        final RepImpl rn2Impl = config.getRN(RG1_RN2_ID).getEnvImpl(60000);
        final RepImpl rn3Impl = config.getRN(RG1_RN3_ID).getEnvImpl(60000);
        rn2Impl.getRepNode().forceMaster(true);

        /* Wait for a new master to emerge. */
        boolean newMaster = new PollCondition(100, 60000) {

            @Override
            protected boolean condition() {
                final StateChangeEvent r2sce = rn2Impl.getStateChangeEvent();
                final StateChangeEvent r3sce = rn3Impl.getStateChangeEvent();

                return r2sce.getState().isActive() &&
                       r3sce.getState().isActive() &&
                       (!r2sce.getMasterNodeName().
                           equals(rn1Impl.getName())) &&
                       (r2sce.getMasterNodeName().
                           equals(r3sce.getMasterNodeName()));
            }
        }.await();

        assertTrue(newMaster);
        assertTrue(rn1Impl.getState().isMaster());

        /*
         * Wait until the client switches over to the master on the majority
         * side. Until then it could encounter Durability or request timeout
         * exceptions.
         */
        boolean ok = new PollCondition
            (100, 4 * RepNodeState.RATE_INTERVAL_MS) {

            @Override
            protected boolean condition() {
                try {
                    Request req = createPutRequest();
                    final Response resp2 = dispatcher.execute(req, LOGIN_MGR);
                    //Return true when get the expected master.
                    if(!RG1_RN1_ID.equals(resp2.getRespondingRN())) {
                        return true;
                    }
                } catch (DurabilityException e) {
                    return false;
                } catch (RequestTimeoutException e) {
                    return false;
                } catch (Exception e) {
                    fail("Unexpected exception:" + e);
                }
                return false;
            }
        }.await();

        assertTrue(ok);

        /*
         * heal the partition. rn1 can now talk to the other nodes.
         */
        enableServices(rn1Impl);

        boolean isReplicaOrRollback = new PollCondition(100, 60000) {

            @Override
            protected boolean condition() {
                try {
                    final State state =
                        config.getRN(RG1_RN1_ID).getEnv(60000).getState();
                    return state.isReplica();
                } catch (RollbackException re) {
                    /*
                     * Encountered when trying to transition to a Replica. The
                     * old master could have been ahead due to some internal
                     * transaction, e.g. one to update the cbvlsn in response
                     * to a heartbeat.
                     *
                     * The environment has been invalidated and the transition
                     * to replica, while intended, will not actually happen,
                     * since the test does not close and reopen the
                     * environment.
                     */
                    return true;
                }
            }
        }.await();

        assertTrue(isReplicaOrRollback);

        final Response resp2 =
            dispatcher.execute(createPutRequest(), LOGIN_MGR);
        assertTrue(!RG1_RN1_ID.equals(resp2.getRespondingRN()));


        /* Verify that replica rns can process reads. */
        Response rresp =
            dispatcher.execute(createGetRequest(), RG1_RN1_ID, LOGIN_MGR);
        assertNotNull(rresp);

        rresp = dispatcher.execute(createGetRequest(), RG1_RN3_ID, LOGIN_MGR);
        assertNotNull(rresp);
        dispatcher.shutdown(null);
    }

    /**
     * Create a get request for the test key new byte[1]
     */
    private Request createGetRequest() {
        final byte[] keyBytes = new byte[1];
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final Get rop = new Get(keyBytes);
        return new Request(rop, partitionId, false,
                           null,
                           Consistency.NONE_REQUIRED,
                           5, seqNum, clientId, timeoutMs,
                           null);
    }

    /**
     * Create a put request for the test key new byte[1]
     */
    private Request createPutRequest() {
        final byte[] keyBytes = new byte[1];
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);

        final Put op = new Put(keyBytes, Value.createValue(new byte[0]),
                               ReturnValueVersion.Choice.NONE);
        return new Request(op, partitionId, true,
                           Durability.COMMIT_NO_SYNC,
                           null, 5, seqNum, clientId, timeoutMs,
                           null);
    }

    /**
     * Disable all HA-related services for repImpl. Note that this merely
     * prevents new connections at the service dispatcher, it does not
     * impact any existing connections, in particular ones already established
     * by the feeder. So it's imperfect, but good enough to simulate a
     * scenario with two masters in an RG.
     */
    public static void disableServices(RepImpl repImpl) {
        final ServiceDispatcher sd1 = repImpl.getRepNode().getServiceDispatcher();
        sd1.setSimulateIOException(Learner.SERVICE_NAME, true);
        sd1.setSimulateIOException(Acceptor.SERVICE_NAME, true);
        sd1.setSimulateIOException(FeederManager.FEEDER_SERVICE, true);
    }

    /**
     * The reverse of the above method. re-enable the services disabled above.
     */
    public static void enableServices(RepImpl repImpl) {
        final ServiceDispatcher sd1 = repImpl.getRepNode().getServiceDispatcher();
        sd1.setSimulateIOException(Learner.SERVICE_NAME, false);
        sd1.setSimulateIOException(Acceptor.SERVICE_NAME, false);
        sd1.setSimulateIOException(FeederManager.FEEDER_SERVICE, false);
    }
}
