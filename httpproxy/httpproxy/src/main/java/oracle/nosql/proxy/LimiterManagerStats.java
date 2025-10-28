/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.nosql.proxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.nosql.common.kv.drl.LimiterManager;
import oracle.nosql.common.sklogger.CustomGauge;
import oracle.nosql.common.sklogger.CustomGauge.GaugeCalculator;
import oracle.nosql.common.sklogger.CustomGauge.GaugeResult;
import oracle.nosql.common.sklogger.MetricRegistry;

/**
 * Collect LimiterManager stats to monitoring.
 */
public class LimiterManagerStats {

    final LimiterManager limiterManager;

    public LimiterManagerStats(MetricRegistry registry,
                               LimiterManager limiterManager) {
        if (limiterManager == null) {
            throw new IllegalArgumentException("LimiterManagerStats: " +
                "limiterManager must be non-null");
        }
        this.limiterManager = limiterManager;
        /*
         * Register the limiterStatsGauge metric so that SKLogger will output
         * it at background automatically.
         */
        CustomGauge limiterStatsGauge =
            new CustomGauge("limiterManager",
                            new LimiterStatsCalculator());
        registry.register(limiterStatsGauge);
    }

    /*
     * Get the latest stats from limiterManager and then map them to a list of
     * metrics maps.
     */
    private class LimiterStatsCalculator implements GaugeCalculator {

        @Override
        public List<GaugeResult> getValuesList() {
            List<GaugeResult> resultList = new ArrayList<GaugeResult>();

            /* get and reset stats from limiterManager */
            LimiterManager.Stats stats = new LimiterManager.Stats();
            limiterManager.collectStats(stats);

            Map<String, Object> map = new HashMap<String, Object>();
            map.put("delayedResponses", stats.delayedResponses);
            map.put("delayedMillis", stats.delayedMillis);
            map.put("storeErrors", stats.storeErrors);
            map.put("activeTables", stats.activeTables);
            map.put("overLimitTables", stats.overLimitTables);
            resultList.add(new GaugeResult(map));

            return resultList;
        }
    }
}
