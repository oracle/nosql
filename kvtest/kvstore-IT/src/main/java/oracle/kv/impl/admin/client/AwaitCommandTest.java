/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static org.easymock.EasyMock.createMock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import oracle.kv.impl.admin.client.AwaitCommand.ReplicaDelayInfo;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.util.shell.ShellArgumentException;

/**
 * Verifies the functionality and error paths of the AwaitCommand class.
 */
public class AwaitCommandTest extends RepNodeTestBase {

    @Test
    public void testAwaitConsistentInvalidTimeout() throws Exception {
        final AwaitCommand command = new AwaitCommand();

        final String invalidArg = "-10";
        final String[] args = {"", "-timeout", invalidArg};
        String exceptionMessage = null;
        try {
            command.invalidArgument(invalidArg);
        } catch (ShellArgumentException sae) {
            exceptionMessage = sae.getMessage();
        }

        final CommandShell testShell = createMock(CommandShell.class);
        try {
            command.execute(args, testShell);
            fail("ShellArgumentException expected");
        } catch (ShellArgumentException sae) {
            assertEquals(exceptionMessage, sae.getMessage());
        }
    }

    @Test
    public void testAwaitConsistentInvalidThreshold() throws Exception {
        final AwaitCommand command = new AwaitCommand();

        final String invalidArg = "-10";
        final String[] args = {"", "-replica-delay-threshold", invalidArg};
        String exceptionMessage = null;
        try {
            command.invalidArgument(invalidArg);
        } catch (ShellArgumentException sae) {
            exceptionMessage = sae.getMessage();
        }

        final CommandShell testShell = createMock(CommandShell.class);
        try {
            command.execute(args, testShell);
            fail("ShellArgumentException expected");
        } catch (ShellArgumentException sae) {
            assertEquals(exceptionMessage, sae.getMessage());
        }
    }

    /*
     * Creates a store and test basic waiting operations.
     */
    @Test
    public void testAwaitConsistent() throws Exception {
        /* In general, 1 second threshold is plenty for a unit test */
        final long THRESHOLD = 1000;
        final int TIMEOUT = 30;

        final KVRepTestConfig config = new KVRepTestConfig(this,
                                             2 /* nDC */, 2 /* nSN */,
                                             2 /* RF */, 10 /* nPartition */);
        config.startRepNodeServices();

        try {
            final Topology topo = config.getTopology();
            final Set<DatacenterId> zones = topo.getDatacenterMap().getAllIds();


            Map<String, ReplicaDelayInfo> map =
                    AwaitCommand.waitForZones(topo, zones, TIMEOUT, THRESHOLD);

            assertNull("The store did not become consistent", map);

            /* Check each zone individually */
            for (DatacenterId zId : zones) {
                map = AwaitCommand.waitForZones(topo,
                                                Collections.singleton(zId),
                                                TIMEOUT, THRESHOLD);
                assertNull(zId + " did not become consistent", map);
            }

            /* Stop one of the nodes and recheck */
            final RepNodeId rnId = new RepNodeId(1,1);
            final RepNode rn = config.getRN(rnId);
            rn.stop(true);

            /* Check all zones */
            map = AwaitCommand.waitForZones(topo, zones, TIMEOUT, THRESHOLD);

            assertNotNull("Await incorrectly reported store was consistent",
                          map);

            assertEquals(1, map.size());

            /*
             * rg1-rn1 should be unreachable and have an entry with null delay
             * info
             */
            boolean ok = false;
            for (Map.Entry<String, ReplicaDelayInfo> e : map.entrySet()) {
                if (e.getKey().equals(rnId.getFullName())) {
                    assertNull(e.getValue());
                    ok = true;
                    break;
                }
            }
            if (!ok) {
               fail("Await failed to report that " + rnId + " was unreachable");
            }

            /* Check zone 1, which has the stopped node */
            final DatacenterId z1 = new DatacenterId(1);
            map = AwaitCommand.waitForZones(topo,
                                            Collections.singleton(z1),
                                            TIMEOUT, THRESHOLD);
            assertNotNull("Await incorrectly reported store was consistent",
                          map);

            /* Check zone 2, which should be OK */
            final DatacenterId z2 = new DatacenterId(2);
            map = AwaitCommand.waitForZones(topo,
                                            Collections.singleton(z2),
                                            TIMEOUT, THRESHOLD);
            assertNull("Zone 2 did not become consistent", map);

        } finally {
            config.stopRepNodeServices();
        }

        // TODO: It would be somewhat harder to do, but worth checking with a
        // node that is behind rather than offline?  I think you could do this
        // by using a test hook to slow down or stop replication for a node,
        // and then populate the store.
    }
}
