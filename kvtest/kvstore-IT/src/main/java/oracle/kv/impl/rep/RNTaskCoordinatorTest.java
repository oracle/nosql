/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */


package oracle.kv.impl.rep;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.TestBase;
import oracle.kv.impl.api.AggregateThroughputTracker;
import oracle.kv.impl.api.AggregateThroughputTracker.RWKB;
import oracle.kv.impl.rep.RNTaskCoordinator.ThroughputPercent;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.StorageTypeDetector.StorageType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verify that the RN Task coordinator responds correctly to changing load, by
 * adjusting the percentage of application permits it reserves.
 */
public class RNTaskCoordinatorTest extends TestBase {

    final AtomicInteger rKBPerSec = new AtomicInteger(0);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void test() {
        final AggregateThroughputTracker rwkb =
            new AggregateThroughputTracker();
        final RNTaskCoordinator coord =
            new RNTaskCoordinator(logger,
                                  StorageType.HD,
                                  null) {
            @Override
            RWKB getRWKB() {
                rwkb.accumulate(rKBPerSec.get(), 0);
                return rwkb.getRWKB();
            }
        };

        /*
         * Create a load map consisting of a test load and expected permit
         * percent. To make the test load relatively machine-load insensitive,
         * the test KBPerSec is chosen as either the midpoint of the load
         * interval associated with the app percent, or at the top end is a
         * value 20% higher than the top end RKBPerSec.
         */
        final List<ThroughputPercent> load = new ArrayList<>();

        final ThroughputPercent[] tpmap = coord.getThroughputPercentMap();
        for (int i=0; i < tpmap.length; i++) {
            final ThroughputPercent low = tpmap[i];
            final int rkbps = (i == (tpmap.length -1)) ?
                    (low.KBPerSec + low.KBPerSec/5) : /* 20 % higher. */
                    (low.KBPerSec + tpmap[i+1].KBPerSec)/ 2; /* Mid point. */
            load.add(new ThroughputPercent(rkbps, low.percent));
        }

        /* Ramp up load. */
        runLoadSequence(coord, load);

        /* Ramp down. */
        Collections.reverse(load);
        runLoadSequence(coord, load);

        /* Up and down, Random load. */
        Collections.shuffle(load);
        runLoadSequence(coord, load);

        coord.close();
    }

    /* Varies the load based on the load sequence and verifies the response. */
    private void runLoadSequence(final RNTaskCoordinator coord,
                                 List<ThroughputPercent> load) {
        for (ThroughputPercent tp : load) {
            rKBPerSec.set(tp.KBPerSec);

            /* Wait for the coordinator to react */
            boolean appPercentReached = new PollCondition(10, 10000) {

                @Override
                protected boolean condition() {
                   return coord.getAppPermitPercent() == tp.percent;
                }

            }.await();

            assertTrue("goal:" + tp.percent + " actual:" +
                       coord.getAppPermitPercent() +
                       " max throughput:" + coord.getMaxKBPerSec(),
                       appPercentReached);
        }
    }
}
