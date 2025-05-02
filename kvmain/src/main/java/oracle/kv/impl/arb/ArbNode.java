/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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
package oracle.kv.impl.arb;

import java.io.File;
import java.util.Properties;
import java.util.logging.Logger;

import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.arbiter.Arbiter;
import com.sleepycat.je.rep.arbiter.ArbiterConfig;
import com.sleepycat.je.rep.arbiter.ArbiterMutableConfig;
import com.sleepycat.je.rep.arbiter.ArbiterStats;
import com.sleepycat.je.rep.impl.RepParams;

import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.arb.ArbNodeService.Params;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.util.server.JENotifyHooks.RedirectHandler;

public class ArbNode implements ParameterListener {

    /**
     * The parameters used to configure the arb node.
     */
    private final Params params;

    private final ArbiterConfig arbiterConfig;
    private final File envDir;
    private Arbiter arbiter;
    private final ArbNodeId arbId;
    private final Logger logger;
    /*  Time when the ArbNode instance was created. */
    private final long serviceStartTime;

    public ArbNode(Params params) {

        logger = LoggerUtils.getLogger(this.getClass(), params);

        ParameterUtils pu =
            new ParameterUtils(params.getArbNodeParams().getMap());
        StorageNodeParams snParams = params.getStorageNodeParams();
        ArbNodeParams arbParams = params.getArbNodeParams();
        envDir =
            FileNames.getEnvDir(snParams.getRootDirPath(),
                                params.getGlobalParams().getKVStoreName(),
                                null,
                                snParams.getStorageNodeId(),
                                arbParams.getArbNodeId());

        FileNames.makeDir(envDir);
        arbId = arbParams.getArbNodeId();
        arbiterConfig = pu.getArbConfig(envDir.getAbsolutePath());
        arbiterConfig.setNodeName(arbId.getFullName());
        arbiterConfig.setGroupName(arbId.getGroupName());
        arbiterConfig.setLoggingHandler
            (new ArbEnvRedirectHandler(params));

        if (TestStatus.isActive()) {
            arbiterConfig.setConfigParam(RepParams.SO_REUSEADDR.getName(),
                                         "true");

            arbiterConfig.setConfigParam(RepParams.SO_BIND_WAIT_MS.getName(),
                                         "120000");
        }

        /* Configure the JE HA communication mechanism */
        if (params.getSecurityParams() != null) {
            final Properties haProps =
                params.getSecurityParams().getJEHAProperties();
            logger.info("DataChannelFactory: " +
                        haProps.getProperty(
                            ReplicationNetworkConfig.CHANNEL_TYPE));
            arbiterConfig.setRepNetConfig(
                ReplicationNetworkConfig.create(haProps));
        }
        serviceStartTime = System.currentTimeMillis();
        this.params = params;
    }

    public Arbiter startup() {
        arbiter = new Arbiter(arbiterConfig);
        return arbiter;
    }

    public void stop() {
        try {
            if (arbiter != null) {
                arbiter.shutdown();
            }
        } finally {
            arbiter = null;
        }
    }

    public GlobalParams getGlobalParams() {
        return params.getGlobalParams();
    }

    public ArbNodeParams getArbNodeParams() {
        return params.getArbNodeParams();
    }

    public StorageNodeParams getStorageNodeParams() {
        return params.getStorageNodeParams();
    }

    public LoadParameters getAllParams() {
        LoadParameters ret = new LoadParameters();
        ret.addMap(params.getGlobalParams().getMap());
        ret.addMap(params.getStorageNodeParams().getMap());
        ret.addMap(params.getArbNodeParams().getMap());
        return ret;
    }

    public long getServiceStartTime() {
        return serviceStartTime;
    }

    public ArbiterStats getStats(boolean clearStats) {
        /* Configuration used to collect stats. */
        StatsConfig config = new StatsConfig().setClear(clearStats);

        if (arbiter == null) {
            return null;
        }
        return arbiter.getStats(config);
    }

    public ArbiterConfig getArbiterConfig() {
        return arbiterConfig;
    }

    public ReplicationNetworkConfig getRepNetConfig() {
        return arbiterConfig == null ? null : arbiterConfig.getRepNetConfig();
    }

    private static class ArbEnvRedirectHandler extends RedirectHandler {
        ArbEnvRedirectHandler(Params serviceParams) {
            super(LoggerUtils.getLogger(ArbEnvRedirectHandler.class,
                                        serviceParams));
        }
    }

    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        if (arbiter == null) {
            return;
        }
        ArbiterMutableConfig amc = arbiter.getArbiterMutableConfig();
        if (newMap.exists(ParameterState.JE_HELPER_HOSTS)) {
            amc.setHelperHosts(
                newMap.get(ParameterState.JE_HELPER_HOSTS).asString());
        }
        ParameterUtils.setJEMutableParams(
            oldMap, newMap,
            (k, v) -> amc.setConfigParam(k, v),
            logger);
        arbiter.setArbiterMutableConfig(amc);
    }

}
