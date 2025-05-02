/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.rgstate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.Version;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.PollCondition;

import com.sleepycat.je.utilint.VLSN;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(FilterableParameterized.class)
public class RepNodeStateTest extends UncaughtExceptionTestBase {

    private RepNodeState rns;

    /**
     * 3000ms for the rn state to be declared obsolete and the VLSN rate
     * revised.
     */
    private final int rateIntervalMs = 3000;

    private final boolean async;

    public RepNodeStateTest(boolean async) {
        this.async = async;
    }

    @Parameters(name="async={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Override
    public void setUp()
        throws Exception {

        RepNodeId rnId = new RepNodeId(1,1);
        rns = new RepNodeState(rnId, null /* trackerId */, async, logger,
                               rateIntervalMs);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * Test av resp time calculations
     */
    @Test
    public void testGetAvRespTimeMs() {
        int avRespTimeMs = rns.getAvReadRespTimeMs();
        assertEquals(0, avRespTimeMs);
        final int sampleSize = RepNodeState.SAMPLE_SIZE;

        for (int i=0; i < sampleSize; i++) {
            rns.accumRespTime(false, 10);
        }

        avRespTimeMs = rns.getAvReadRespTimeMs();
        assertEquals(10, avRespTimeMs);

        for (int i=0; i < RepNodeState.SAMPLE_SIZE; i++) {
            rns.accumRespTime(false, 30);
        }

        assertEquals(10, avRespTimeMs);
        assertEquals(30, rns.getAvReadRespTimeMs());

        for (int i=0; i < RepNodeState.SAMPLE_SIZE; i++) {
            rns.accumRespTime(false, 1000000);
        }
        assertEquals(Short.MAX_VALUE, rns.getAvReadRespTimeMs());
    }

    @Test
    public void testVLSNState() {
        /* Must be null at startup. */
        assertTrue(VLSN.isNull(rns.getVLSN()));

        /* Ensure that it's updated. */
        for (int i=0; i <= 10; i++) {
            long vlsn = i;
            rns.updateVLSN(vlsn);
            assertTrue(!rns.isObsoleteVLSNState());
            assertEquals(vlsn, rns.getVLSN());
        }

        /* Reverse updates must be ignored. */
        rns.updateVLSN(1);
        assertEquals(10, rns.getVLSN());

        /* Inactivity should cause the state to be rendered obsolete. */
        boolean success = new PollCondition(1000, 60000) {

            @Override
            protected boolean condition() {
                return rns.isObsoleteVLSNState();
            }

        }.await();
        assertTrue(success);

        /* Updates as usual, after becoming obsolete */
        rns.updateVLSN(100);
        assertTrue(!rns.isObsoleteVLSNState());
        assertEquals(100, rns.getVLSN());
    }

    @Test
    public void testConsistency()
        throws InterruptedException {

        final RepNodeId masterId = new RepNodeId(1,2);
        final RepNodeState rnsMaster = new RepNodeState(
            masterId, null /* trackerId */, async, logger, rateIntervalMs);
        final int masterLeadVlsns = 10000;

        /* Start the ball rolling, reset the starting interval. */
        rnsMaster.updateVLSN(masterLeadVlsns);
        rns.updateVLSN(0);

        /*
         * Update vlsns to ultimately establish a 10 vlsns/ms vlsn rate at the
         * end of the rate interval, not taking affect until time advances.
         */
        final long replicaVLSN = rateIntervalMs * 10;
        rns.updateVLSN(replicaVLSN);
        /* Master is 10000 vlsns or 1sec ahead of the replica. */
        final long masterVLSN = masterLeadVlsns + (rateIntervalMs * 10);
        rnsMaster.updateVLSN(masterVLSN);

        Consistency.Version verConsistency =
            createVerConsistency(replicaVLSN);

        long time = System.currentTimeMillis();
        assertTrue(rns.inConsistencyRange(time, verConsistency));

        verConsistency = createVerConsistency(replicaVLSN + 1);

        /*
         * The initial vlsn rate is still zero since we are still in the rate
         * interval so can't reach this VLSN.
         */
        assertTrue(!rns.inConsistencyRange(time, verConsistency));

        Thread.sleep(rateIntervalMs + 1);

        /*
         * Close the current interval establishing a ~10 vlsns/sec rate with
         * the following update calls.
         */
        rns.updateVLSN(replicaVLSN);
        rnsMaster.updateVLSN(masterVLSN);

        /*
         * Permit a lag of four seconds or 40K vlsns. The replica's 10K lag
         * only represents a 1 second lag, so it can satisfy the request.
         */
        Consistency.Time tConsistency =
            new Consistency.Time(4, SECONDS, 5, SECONDS);

        /* Wait 1 ms, which should represent at least 10 vlsns */
        Thread.sleep(1);
        time = System.currentTimeMillis();
        assertTrue(rns.inConsistencyRange(time, tConsistency, rnsMaster));

        /*
         * Time consistency within 2ms of the master which is impossible to
         * satisfy since there is a 10K difference and the vlsn rate is
         * just 10 vlsns/ms
         */
        tConsistency = new Consistency.Time(2, MILLISECONDS,
                                            5, SECONDS);
        assertTrue(!rns.inConsistencyRange(time, tConsistency, rnsMaster));

        assertTrue(rns.inConsistencyRange(time, verConsistency));
    }

    private Consistency.Version createVerConsistency(long vlsn) {

        Version version = new Version(new UUID(32, 32), vlsn);

        Consistency.Version consistency =
            new Consistency.Version(version, 5, TimeUnit.SECONDS);
        return consistency;
    }

    /**
     * Tests RepNodeState#printString.
     */
    @Test
    public void testPrintString() {
        final int sampleSize = RepNodeState.SAMPLE_SIZE;
        for (int i=0; i < sampleSize; i++) {
            rns.accumRespTime(false, 10);
        }
        assertEquals(
            "node: rg1-rn1 state: REPLICA "
            + "errors: 0 "
            + "av resp time 11,000,000 ns "
            + "total requests: 0",
            rns.printString());
    }

}
