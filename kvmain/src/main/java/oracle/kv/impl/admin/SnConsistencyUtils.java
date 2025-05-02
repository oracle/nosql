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

package oracle.kv.impl.admin;

import static oracle.kv.impl.param.ParameterState.RN_NODE_TYPE;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.admin.VerifyConfiguration.CompareParamsResult;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.NodeType;

public class SnConsistencyUtils {
    /* TODO: currently not checking for Admin, perhaps do that as well? */
    /*
     * Check parameters for SN, RN or AN.
     */
    static CompareParamsResult checkParameters(Parameters dbParams,
                                               LoadParameters serviceParams,
                                               LoadParameters checkParams,
                                               ResourceId resId,
                                               StorageNodeId snId)  {
        CompareParamsResult dbCompare =
            VerifyConfiguration.compareParamsPCon(checkParams,
                                                  dbParams.getMap(resId));
        if (serviceParams == null) {
            return dbCompare;
        }

        CompareParamsResult serviceCompare =
            VerifyConfiguration.compareServiceParamsPCon(
                snId, resId, serviceParams, dbParams);
        return VerifyConfiguration.combineCompareParamsResults(
            dbCompare, serviceCompare);
    }

    /**
     * Checks the global and SN parameters.
     */
    static ParamCheckResults checkParameters(RegistryUtils regUtils,
                                             StorageNodeId snId,
                                             Parameters params)
       throws RemoteException, NotBoundException {
        final StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);

        /* Get SN configuration parameters */
        final LoadParameters configParams;
        configParams = sna.getParams();
        ParamCheckResults retVal = new ParamCheckResults();

        CompareParamsResult snCompare =
            VerifyConfiguration.compareParamsPCon(
                configParams,
                params.getGlobalParams().getMap());

        if (snCompare != CompareParamsResult.NO_DIFFS) {
            retVal.setGlobalDiff();
        }

        snCompare = checkParameters(params, null, configParams, snId, snId);

        if (snCompare != CompareParamsResult.NO_DIFFS) {
            retVal.addDiff(snId);
        }

        for (RepNodeParams rnDbParams : params.getRepNodeParams()) {

            final RepNodeId rnId = rnDbParams.getRepNodeId();

            /*
             * Compare sn config with admin db for given RN
             */
            if (!rnDbParams.getStorageNodeId().equals(snId)) {
                continue;
            }

            LoadParameters serviceParams = null;
            /* Check in-memory parameters from the RN */
            try {
                final RepNodeAdminAPI rna = regUtils.getRepNodeAdmin(rnId);
                serviceParams = rna.getParams();

            } catch (RemoteException | NotBoundException e) {
                retVal.setHadError(true);
            }
            CompareParamsResult combinedCompare =
                checkParameters(params, serviceParams, configParams,
                                rnId, snId);

            if (combinedCompare == CompareParamsResult.NO_DIFFS) {
                continue;
            }

            if (combinedCompare == CompareParamsResult.MISSING) {
                retVal.addMissing(rnId);
            } else {
                ParameterMap rnCfg =
                    configParams.getMap(rnId.getFullName(),
                                        ParameterState.REPNODE_TYPE);
                NodeType nt =
                    NodeType.valueOf(
                        rnCfg.getOrDefault(RN_NODE_TYPE).asString());
                /*
                 * Do not attempt to fix a RN with an out of sync
                 * node type. Leave this correction to plan repair.
                 */
                if (rnDbParams.getNodeType() == nt) {
                    retVal.addDiff(rnId);
                }
            }
        }

        for (ArbNodeParams anDbParams : params.getArbNodeParams()) {

            final ArbNodeId anId = anDbParams.getArbNodeId();

            /*
             * Compare sn config with admin db for given AN
             */
            if (!anDbParams.getStorageNodeId().equals(snId)) {
                continue;
            }

            LoadParameters serviceParams = null;
            /* Check in-memory parameters from the RN */
            try {
                final ArbNodeAdminAPI ana = regUtils.getArbNodeAdmin(anId);
                serviceParams = ana.getParams();

            } catch (RemoteException | NotBoundException e) {
                retVal.setHadError(true);
            }
            CompareParamsResult combinedCompare =
                checkParameters(params, serviceParams, configParams, anId,
                                snId);

            if (combinedCompare == CompareParamsResult.NO_DIFFS) {
                continue;
            }

            if (combinedCompare == CompareParamsResult.MISSING) {
                retVal.addMissing(anId);
            } else {
                retVal.addDiff(anId);
            }
        }
        return retVal;
    }

    /**
     * Used to save results of parameter consistency between the
     * Admin database and the corresponding SN configuration.
     *
     */
    public static class ParamCheckResults {
        private boolean globalParametersDiff;
        private final List<ResourceId> idsDiff;
        private final List<ResourceId> idsMissing;
        private boolean hadError = false;

        private ParamCheckResults() {
            idsDiff = new ArrayList<ResourceId>();
            idsMissing = new ArrayList<ResourceId>();
        }

        private void addDiff(ResourceId resId) {
            idsDiff.add(resId);
        }

        private void addMissing(ResourceId resId) {
            idsMissing.add(resId);
        }

        private void setGlobalDiff() {
            globalParametersDiff = true;
        }

        public boolean getGlobalDiff() {
            return globalParametersDiff;
        }

        public List<ResourceId> getDiffs() {
            return idsDiff;
        }

        public List<ResourceId> getMissing() {
            return idsMissing;
        }

        public boolean getHadError() {
            return hadError;
        }

        public void setHadError(boolean hadError) {
            this.hadError = hadError;
        }
    }
}
