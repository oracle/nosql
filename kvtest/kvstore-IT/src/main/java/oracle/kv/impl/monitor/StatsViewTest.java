/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.AdminTestConfig;
import oracle.kv.impl.measurement.EnvStats;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.monitor.views.CSVFileView;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;

import org.junit.Test;


/**
 *
 */
public class StatsViewTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    public StatsViewTest() {
    }

    /**
     * Test the creation of the .csv file performance data. For testing
     * validation, we're really going to look at the .csv file in a
     * spreadsheet.  This unit test will do some checking that the values are
     * as expected, and that nothing untoward happens during this execution.
     */
    @Test
    public void testCSVFile()
        throws IOException {

        AdminTestConfig testConfig = new AdminTestConfig(kvstoreName);
        AdminServiceParams params = testConfig.getParams();

        CSVFileView view = new CSVFileView(params);

        TestVal[] testVals = {
            new TestVal(0L, 0L, 100_000_000_000L, 110_000_000_000L,
                        3, 300, 5, 200, 299, 0),
            new TestVal(0L, 0L, 200_000_000_000L, 220_000_000_000L,
                        2, 200, 4, 100, 199, 0),
            new TestVal(0L, 0L, 300_000_000_000L, 330_000_000_000L,
                        1, 100, 3, 98, 99, 0),
            new TestVal(0L, 0L, 400_000_000_000L, 440_000_000_000L,
                        5, 500, 2, 400, 499, 0)
        };

        RepNodeId rnG1N1 = new RepNodeId(1, 1);
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        Environment env = new Environment(testConfig.getTestDir(), envConfig);
        EnvironmentStats envStats = env.getStats(null);

        /*
         * Manufacture a single StatsPacket with latency and environment stats
         * to mimic a single stats collection period.
         */
        StatsPacket stats = new StatsPacket(0, 0, "rg1-rn1", "rg1");
        stats.add(testVals[0].makeLatency(PerfStatType.PUT_IF_ABSENT_INT));
        stats.add(testVals[1].makeLatency(PerfStatType.PUT_IF_PRESENT_INT));
        stats.add(testVals[2].makeLatency(PerfStatType.GET_INT));
        stats.add(new EnvStats(0, 0, envStats));

        /*
         * Deliver it to the view, and check that the view has summarized it,
         * and that the .csv files exist.
         */
        view.applyNewInfo(rnG1N1, stats);
        Map<PerfStatType, LatencyInfo> summary = view.getSummary();
        LatencyResult userOps =
            summary.get(PerfStatType.USER_SINGLE_OP_INT).getLatency();
        assertEquals(userOps.toString(), 1, userOps.getMin());
        assertEquals(userOps.toString(), 300, userOps.getMax());
        assertEquals(userOps.toString(),
                     600_000_000_000L, userOps.getRequestCount());
        assertEquals(userOps.toString(),
                     660_000_000_000L, userOps.getOperationCount());

        File logDir = FileNames.getLoggingDir
            (new File(params.getStorageNodeParams().getRootDirPath()),
             params.getGlobalParams().getKVStoreName());

        assertTrue(new File(logDir, rnG1N1.toString() + FileNames.DETAIL_CSV).
                   exists());
        assertTrue(new File(logDir, rnG1N1.toString() + FileNames.SUMMARY_CSV).
                   exists());

        /* Create and deliver another stats packet */
        stats = new StatsPacket(0, 0, "rg1-rn1", "rg1");
        stats.add(testVals[2].makeLatency(PerfStatType.PUT_IF_ABSENT_INT));
        stats.add(testVals[3].makeLatency(PerfStatType.PUT_IF_PRESENT_INT));

        view.applyNewInfo(rnG1N1, stats);
        summary = view.getSummary();
        assertEquals(null, summary.get(PerfStatType.GET_INT));
        userOps = summary.get(PerfStatType.USER_SINGLE_OP_INT).getLatency();
        assertEquals(userOps.toString(), 1, userOps.getMin());
        assertEquals(userOps.toString(), 500, userOps.getMax());
        assertEquals(userOps.toString(),
                     700_000_000_000L, userOps.getRequestCount());
        assertEquals(userOps.toString(),
                     770_000_000_000L, userOps.getOperationCount());

        /* Create and deliver another stats packet */
        stats = new StatsPacket(0, 0, "rg1-rn1", "rg1");
        stats.add(testVals[0].makeLatency(PerfStatType.PUT_IF_ABSENT_INT));
        stats.add(testVals[1].makeLatency(PerfStatType.PUT_IF_VERSION_INT));

        view.applyNewInfo(rnG1N1, stats);
        summary = view.getSummary();
        assertEquals(null, summary.get(PerfStatType.GET_INT));
        userOps = summary.get(PerfStatType.USER_SINGLE_OP_INT).getLatency();
        assertEquals(userOps.toString(), 2, userOps.getMin());
        assertEquals(userOps.toString(), 300, userOps.getMax());
        assertEquals(userOps.toString(),
                     300_000_000_000L, userOps.getRequestCount());
        assertEquals(userOps.toString(),
                     330_000_000_000L, userOps.getOperationCount());

        view.close();
        env.close();
    }

    class TestVal {
        final long start;
        final long end;
        final long totalReq;
        final long totalOps;
        final int min;
        final int max;
        final int avg;
        final int percent95;
        final int percent99;
        final int reqOverflow;

        TestVal(long start, long end,
                long totalReq,
                long totalOps,
                int min, int max, int avg,
                int percent95, int percent99,
                int reqOverflow) {
            this.start = start;
            this.end = end;
            this.totalReq = totalReq;
            this.totalOps = totalOps;
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.percent95 = percent95;
            this.percent99 = percent99;
            this.reqOverflow = reqOverflow;
        }

        LatencyInfo makeLatency(PerfStatType statType) {
            return new LatencyInfo
               (statType, start, end,
                new LatencyResult(totalReq, totalOps,
                                  min, max, avg,
                                  percent95, percent99, reqOverflow));
        }
    }
}
