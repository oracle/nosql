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

package oracle.kv.impl.admin.plan.task;

import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.RequestTypeUpdater.RequestType;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Write new enabled request type of specified RN to storage node and Admin.
 */
public class WriteNewRequestType extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final String requestType;
    private final StorageNodeId targetSNId;
    private final RepNodeId rnid;

    public WriteNewRequestType(AbstractPlan plan,
                               String requestType,
                               RepNodeId rnid,
                               StorageNodeId targetSNId) {
        super();
        this.plan = plan;
        this.requestType = requestType;
        this.rnid = rnid;
        this.targetSNId = targetSNId;
    }

    public static RequestType validateRequestType(String requestType) {
        if (requestType == null) {
            throw new IllegalCommandException("Request type cannot be null");
        }
        try {
            return RequestType.valueOf(requestType.toUpperCase());
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException(
                "Specified an invalid request type");
        }
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        final RepNodeParams rnParams = admin.getRepNodeParams(rnid);
        final RequestType newType = validateRequestType(requestType);

        /* Check if the request type of this RN have changed */
        final RequestType currentType = rnParams.getEnabledRequestType();
        rnParams.setEnabledRequestType(newType);
        final ParameterMap rnParamsMap = rnParams.getMap();
        plan.getLogger().log(
            Level.INFO,
            "{0} changing enabled request type for {1}: {2}",
            new Object[]{plan, rnid, requestType});

        final Topology topo = admin.getCurrentTopology();
        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils registryUtils =
            new RegistryUtils(topo, loginMgr, plan.getLogger());
        final StorageNodeAgentAPI sna =
            registryUtils.getStorageNodeAgent(targetSNId);

        try {
            /* Check the parameters prior to writing them to the DB. */
            sna.checkParameters(rnParamsMap, rnid);
        } catch (UnsupportedOperationException ignore) {
            /*
             * If UOE, the SN is not yet upgraded to a version that
             * supports this check, so just ignore
             */
        }

        /*
         * If enabled request type is not changed, don't update admin database
         * but still update the SN parameters, in case the SN parameters has
         * mismatched value.
         */
        if (currentType != newType) {
            admin.updateParams(rnParams);
        }
        sna.newRepNodeParameters(rnParamsMap);
        return State.SUCCEEDED;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(rnid);
    }
}
