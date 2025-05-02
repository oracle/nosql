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

package oracle.kv.impl.xregion.service;

import static oracle.kv.KVVersion.CURRENT_VERSION;
import static oracle.kv.impl.xregion.service.JsonConfig.MIN_XREGION_GROUP_STORE_VER;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.streamservice.MRT.Response;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.pubsub.NoSQLSubscriberId;

/**
 * Object that validates the configuration of multiple XRegionService agents
 */
public class MultiServiceConfigValidator {

    /** always uses the very first agent in the group as source of truth */
    public static final int LEAD_AGENT_INDEX = 0;
    /** wait in ms before retry during start up */
    private static final int STARTUP_WAIT_MS = 5 * 1000;
    /** wait in ms before retry during start up */
    private static final int RUN_WAIT_MS = 60 * 1000;

    /** max retry time in ms */
    private static final int MAX_RETRY_MS = 60 * 60 * 1000;
    /** rate limiting logger sample interval */
    private static final int RL_INTV_MS = 60 * 60 * 1000;
    /** rate limiting logger max objects */
    private static final int RL_MAX__OBJ = 1024;

    /** subscriber id */
    private final NoSQLSubscriberId sid;
    /** handle of parent request response manager */
    private final XRegionService parent;
    /** private logger */
    private final Logger logger;
    /** rate limiting logger */
    private final RateLimitingLogger<String> rl;
    /** True if validation is enabled */
    private final boolean enabled;

    public MultiServiceConfigValidator(XRegionService parent, Logger logger) {
        this.parent = parent;
        this.logger = logger;
        sid = parent.getSid();

        if (sid.getTotal() == 1) {
            logger.info(lm("Disable validation with singleton group" +
                           ", agentId=" + sid));
            enabled = false;
        } else {
            enabled = true;
        }
        rl = new RateLimitingLogger<>(RL_INTV_MS, RL_MAX__OBJ, logger);
    }

    /**
     * Returns true if the given agent id is a lead agent id
     *
     * @param index agent index in group
     * @return true if the given agent id is a lead agent id, false otherwise.
     */
    public static boolean isLead(int index) {
        return index == LEAD_AGENT_INDEX;
    }

    /**
     * Validates configuration of member agent with lead agent when
     * member agent starts up.
     *
     * @param ver version of the agent
     * @return true if configuration is valid, or false otherwise
     */
    public boolean startupValidate(NoSQLSubscriberId id, KVVersion ver) {
        if (!enabled) {
            return true;
        }

        if (isLeadAgent()) {
            parent.getReqRespMan().setAgentVersion(id, ver);
            logger.info(lm("Lead agent=" + id + " sets version=" +
                           ver.getNumericVersionString()));
            return true;
        }

        /* other agents, validate its config and post version */
        return validate(true, ver, STARTUP_WAIT_MS);
    }

    /**
     * Checks the agent is configured with group size more than the number of
     * shards in source region.
     *
     * @param source source region
     * @param srcKVs source kvstore
     * @param id agent id
     * @throws ServiceException if oversize agent group
     */
    void checkOverSizeGroup(RegionInfo source,
                            KVStore srcKVs,
                            NoSQLSubscriberId id) throws ServiceException {
        final int total = id.getTotal();
        final Topology topo = ((KVStoreImpl) srcKVs).getTopology();
        final int shards = topo.getNumRepGroups();
        if (total > shards) {
            final String err = "Agent id=" + id + " configured with more " +
                               "agents=" + total +
                               " than #shards=" + shards +
                               " in region=" + source.getName();
            logger.severe(lm(err));
            throw new ServiceException(id, err);
        }
    }

    /**
     * Validates configuration of member agent with that of lead agent
     *
     * @return true if the configuration is valid, or false otherwise
     */
    public boolean runtimeValidate() {
        if (!enabled) {
            return true;
        }
        if (isStoreVersionTooLow()) {
            return false;
        }
        return validate(false, CURRENT_VERSION, RUN_WAIT_MS);
    }

    /**
     * Checks if the local store version is high enough to support multiple
     * agents. If not, check the agent group size in JSON config and throw
     * exception if it is greater than 1.
     * @return true if store version is too low for XRegion groups, false
     * otherwise
     */
    boolean isStoreVersionTooLow()
        throws ServiceException {
        final KVVersion storeVer = parent.getReqRespMan().getStoreVersion();
        if (storeVer == null) {
            logger.fine(() -> lm("Store has not post its version, will check " +
                                "in next turn"));
            return false;
        }
        if (VersionUtil.compareMinorVersion(
            storeVer, MIN_XREGION_GROUP_STORE_VER) >= 0) {
            /* store version is high enough to support multiple agents */
            logger.info(lm("Store version=" + storeVer +
                           " support multi-agents (min ver=" +
                           MIN_XREGION_GROUP_STORE_VER + "), " +
                           ", agent version=" + CURRENT_VERSION));
            return false;
        }

        final String err =
            "Store version=" + storeVer.getVersionString() +
            "not high enough to support XRegion group (min version=" +
            MIN_XREGION_GROUP_STORE_VER.getVersionString() + ")" +
            ", agent will shut down";
        logger.warning(lm(err));
        return true;
    }

    private boolean checkLeadConflictRuntime() {
        if (!isLeadAgent()) {
            throw new IllegalStateException("Expect lead agent but id=" + sid);
        }

        final Response resp;
        try {
            resp = parent.getReqRespMan().getResponseAgentVer();
        } catch (IOException e) {
            logger.info(lm("Cannot read agent response, wait the next turn, " +
                           "error=" + e));
            return true;
        }
        if (resp == null) {
            throw new IllegalStateException("Expect lead agent has posted " +
                                            "agent version response");
        }
        if (resp.getLeadAgentId().equals(sid)) {
            /* no conflict, I am still the lead */
            return true;
        }
        logger.warning(lm("Lead conflict detected, another lead (id=" +
                          resp.getLeadAgentId() + ") has come up, " +
                          "this lead agent (id=" + sid + ") must shutdown, " +
                          "complete version resp=" + resp));
        return false;
    }

    private boolean validate(boolean startup,
                             KVVersion kvVersion,
                             long waitMs) {
        if (isLeadAgent()) {
            if (startup) {
                logger.fine(() -> "No check for lead agent at startup time");
                return true;
            }
            /* check lead conflict at runtime */
            return checkLeadConflictRuntime();
        }

        final long startMs = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startMs) < MAX_RETRY_MS) {
            try {
                final Response resp = parent.getReqRespMan()
                                            .getResponseAgentVer();
                if (resp != null) {
                    return doRuntimeCheck(resp, startup, kvVersion);
                }

                logger.info(lm("Lead agent yet to post its version, " +
                               "will retry after timeMs=" + waitMs +
                               ", timeoutMs=" + MAX_RETRY_MS));
                synchronized (this) {
                    wait(waitMs);
                }
            } catch (IOException exp) {
                logger.warning(lm("Cannot read agent version, will retry, " +
                                  "error=" + exp));
            } catch (InterruptedException ie) {
                logger.info(lm("Interrupted, might be in shutdown"));
                return false;
            }
        }

        logger.warning(lm("Cannot complete check in timeMs=" + MAX_RETRY_MS +
                          ", the lead agent might shut down and multi-region " +
                          "tables might not fully synchronized"));
        return false;
    }

    private boolean doRuntimeCheck(Response ver,
                                   boolean startup,
                                   KVVersion myVer) {
        final NoSQLSubscriberId leadId = ver.getLeadAgentId();
        final KVVersion leadVersion = ver.getVersion(leadId);
        if (isMisConfig(leadId)) {
            final String err = "Misconfiguration detected" +
                               ", local group size=" + sid.getTotal() +
                               ", lead (id=" + leadId + ")"  +
                               " group size=" + leadId.getTotal() +
                               ", agent version=" +
                               CURRENT_VERSION.getNumericVersionString() +
                               ", lead version=" +
                               leadVersion.getNumericVersionString() +
                               ", complete version resp=" + ver;
            logger.severe(lm(err));
            return false;
        }

        /* check version and log mixed versions, which is allowed */
        if (CURRENT_VERSION.compareTo(leadVersion) == 0) {
            rl.log(leadVersion.toString(),
                   Level.INFO,
                   lm("Pass runtime check of group and version" +
                      ", group size=" + leadId.getTotal() +
                      ", current version=" +
                      CURRENT_VERSION.getNumericVersionString()));
        } else {
            rl.log(leadVersion.toString(),
                   Level.INFO,
                   lm("Mixed version detected, agent version=" +
                      CURRENT_VERSION.getNumericVersionString() +
                      ", lead agent version=" +
                      leadVersion.getNumericVersionString()));
        }

        if (startup) {
            /* post my own version */
            parent.getReqRespMan().setAgentVersion(sid, myVer);
            Response resp = null;
            try {
                resp = parent.getReqRespMan().getResponseAgentVer();
            } catch (IOException e) {
                logger.info(lm("Unable to read response, err=" + e));
            }
            logger.info(lm("Agent=" + sid + " post version=" +
                           myVer.getNumericVersionString() +
                           ", complete version resp=" + resp));
        }

        return true;
    }

    /**
     * Returns true if misconfiguration is detected
     * @param leadAgentId id of the lead agent, source of truth
     * @return true if misconfiguration is detected, false otherwise
     */
    private boolean isMisConfig(NoSQLSubscriberId leadAgentId) {
        return leadAgentId.getTotal() != sid.getTotal();
    }

    /**
     * Returns true if it is a lead agent
     */
    private boolean isLeadAgent() {
        return sid.getIndex() == LEAD_AGENT_INDEX;
    }

    /**
     * Adds logger header
     *
     * @param msg logging msg
     * @return logging msg with header
     */
    private String lm(String msg) {
        return "[ConfigValidator_" + sid.toString() + "] " + msg;
    }
}
