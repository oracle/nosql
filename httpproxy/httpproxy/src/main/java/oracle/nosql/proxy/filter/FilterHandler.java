/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.filter;

import static oracle.nosql.proxy.protocol.Protocol.RESOURCE_EXISTS;
import static oracle.nosql.proxy.protocol.Protocol.RESOURCE_NOT_FOUND;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpResponse;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.sc.ListRuleResponse;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.util.filter.Rule;
import oracle.nosql.util.filter.Rule.OpType;

/*
 * The class manages rules in memory that contains:
 *
 *  o Map<RuleType, Set<RuleWrapper>> rules, the map of RuleType and its
 *    corresponding rules, the rule can be transient and/or persistent.
 *
 *    The RuleWrapper class contains Rule and its transient and persistent name,
 *    its transientName is not null if the rule is transient, its persistentName
 *    is not null if the rule is persistent.
 *
 *  o Methods to add, delete, get and list rules to access transient rule in
 *    proxy cache.
 *
 *  o Method matchRule() that check if there is a rule matching the given
 *    request information.
 *
 *  o PullRulesThread that periodically pull persistent rules and load to proxy
 *    cache, a method reloadPersistentRules() to reload persistent rules
 *    immediately.
 */
public class FilterHandler {

    /*
     * In order to filter requests as earlier stage as possible, proceed the
     * filtering check once get the required information during request
     * processing.
     *
     * The RuleType defines groups of rules that will be checked in different
     * timings of request handling.
     *
     *   1. ALL: block all, the big red button
     *      Check the rule at beginning of DataService.handleRequest()
     *
     *   2. OPERATION: op[,op..],
     *      Rule contains operations only, these rules are checked after read
     *      OpCode of the request in DataService.handleRequest().
     *
     *   3. PRINCIPAL: tenant/user + op[,op...],
     *      Rule contains principal information, these rules are checked once
     *      get principal information, it is after authentication in
     *      AccessChecker.checkAccess().
     *
     *   4. TABLE: block [tenant/user] + table + op[,op...]
     *      Rules contains table information and possible principal information.
     *      These rules are check once get target table ocid, it is after
     *      permission check in AccessChecker.checkAccess().
     */
    public enum RuleType {
        ALL,
        OPERATION,
        PRINCIPAL,
        TABLE
    }

    /*
     * The map holds the rules in memory, the rule can be transient and/or
     * persistent rule
     */
    private final Map<RuleType, Set<RuleWrapper>> rules;

    /* The thread to pull persistent rules from SC and update local cache */
    private final PullRulesThread pullThread;

    private final TenantManager tm;
    private final SkLogger logger;

    /* The action handler of ActionType.DROP_REQUEST */
    private static final DropRequest dropRequest = new DropRequest();

    /* The map of OpCode and OpType */
    private static Map<OpCode, OpType> opMap = new HashMap<>();
    static {
        /* ddl op */
        opMap.put(OpCode.TABLE_REQUEST, OpType.DDL);
        opMap.put(OpCode.CREATE_TABLE, OpType.DDL);
        opMap.put(OpCode.ALTER_TABLE, OpType.DDL);
        opMap.put(OpCode.DROP_TABLE, OpType.DDL);
        opMap.put(OpCode.CREATE_INDEX, OpType.DDL);
        opMap.put(OpCode.DROP_INDEX, OpType.DDL);
        opMap.put(OpCode.CHANGE_COMPARTMENT, OpType.DDL);
        opMap.put(OpCode.CANCEL_WORKREQUEST, OpType.DDL);
        opMap.put(OpCode.SYSTEM_REQUEST, OpType.DDL);
        opMap.put(OpCode.ADD_REPLICA, OpType.DDL);
        opMap.put(OpCode.DROP_REPLICA, OpType.DDL);
        opMap.put(OpCode.INTERNAL_DDL, OpType.DDL);

        /* write data */
        opMap.put(OpCode.DELETE, OpType.WRITE);
        opMap.put(OpCode.DELETE_IF_VERSION, OpType.WRITE);
        opMap.put(OpCode.PUT, OpType.WRITE);
        opMap.put(OpCode.PUT_IF_ABSENT, OpType.WRITE);
        opMap.put(OpCode.PUT_IF_PRESENT, OpType.WRITE);
        opMap.put(OpCode.PUT_IF_VERSION, OpType.WRITE);
        opMap.put(OpCode.MULTI_DELETE, OpType.WRITE);
        opMap.put(OpCode.WRITE_MULTIPLE, OpType.WRITE);

        /* read data */
        opMap.put(OpCode.GET, OpType.READ);
        opMap.put(OpCode.QUERY, OpType.ALL);
        opMap.put(OpCode.PREPARE, OpType.READ);
        opMap.put(OpCode.SUMMARIZE, OpType.READ);
        opMap.put(OpCode.SCAN, OpType.READ);
        opMap.put(OpCode.INDEX_SCAN, OpType.READ);

        /* read metadata */
        opMap.put(OpCode.GET_TABLE, OpType.READ);
        opMap.put(OpCode.GET_INDEX, OpType.READ);
        opMap.put(OpCode.GET_INDEXES, OpType.READ);
        opMap.put(OpCode.LIST_TABLES, OpType.READ);

        /* read ddl execution status */
        opMap.put(OpCode.LIST_WORKREQUESTS, OpType.READ);
        opMap.put(OpCode.GET_WORKREQUEST, OpType.READ);
        opMap.put(OpCode.GET_WORKREQUEST_LOGS, OpType.READ);
        opMap.put(OpCode.GET_WORKREQUEST_ERRORS, OpType.READ);
        opMap.put(OpCode.SYSTEM_STATUS_REQUEST, OpType.READ);
        opMap.put(OpCode.INTERNAL_STATUS, OpType.READ);

        /* table usage */
        opMap.put(OpCode.GET_TABLE_USAGE, OpType.READ);
        opMap.put(OpCode.GET_REPLICA_STATS, OpType.READ);
    }

    public FilterHandler(TenantManager tm,
                         int pullIntervalSec,
                         SkLogger logger) {
        rules = new TreeMap<>(new Comparator<RuleType>() {
            @Override
            public int compare(RuleType o1, RuleType o2) {
                return o1.compareTo(o2);
            }
        });

        this.tm = tm;
        this.logger = logger;

        if (pullIntervalSec > 0) {
            /*
             * Starts a demon thread that pull persistent rules from SC and load
             * to proxy cache
             */
            pullThread = new PullRulesThread(pullIntervalSec);
            pullThread.setDaemon(true);
            pullThread.start();
        } else {
            /* Don't start the thread if pullIntervalSec is <= 0 */
            pullThread = null;
        }
    }

    /*
     * Add a transient rule.
     *
     * Duplicate transient rule by name or by attribute are not allowed, throw
     * RESOURCE_EXISTS if exists.
     *
     * If persistent rule with same attributes existed, mark it as transient by
     * setting its transient name.
     */
    public synchronized RuleWrapper addRule(Rule rule) {
        final String name = rule.getName();
        RuleWrapper rw = findRuleByName(name);
        if (rw != null) {
            throw new RequestException(RESOURCE_EXISTS,
                "Transient rule with same name already exists: " +
                rw.getRule());
        }

        final RuleType type = getType(rule);
        Set<RuleWrapper> subRules = rules.get(type);
        RuleWrapper result = null;

        if (subRules == null) {
            subRules = new HashSet<>();
            rules.put(type, subRules);
        } else {
            for (RuleWrapper r: subRules) {
                if (r.isTransient() && r.attributesEqual(rule)) {
                    throw new RequestException(RESOURCE_EXISTS,
                        "Transient rule with same attributes already " +
                        "exists: " + r);
                }
            }
        }

        result = new RuleWrapper(rule, true /* isTransient */);
        subRules.add(result);

        logger.info("[Filter] Transient rule added: " + rule);
        return result;
    }

    /*
     * Delete a transient rule with the given name
     *
     * If the rule is also a persistent rule, unmark it by clearing its
     * transient name. Otherwise, remove it from cache.
     *
     * If rule with given name doesn't exist, throw RESOURCE_NOT_FOUND.
     */
    public synchronized void deleteRule(String name) {
        for (Set<RuleWrapper> subRules : rules.values()) {
            Iterator<RuleWrapper> iter = subRules.iterator();
            while(iter.hasNext()) {
                RuleWrapper rw = iter.next();
                if (rw.isTransient() &&
                    rw.getRuleName().equalsIgnoreCase(name)) {

                    iter.remove();
                    logger.info("[Filter] Transient rule deleted: " + name);
                    return;
                }
            }
        }

        throw new RequestException(RESOURCE_NOT_FOUND,
                                   "Transient rule not found: " + name);
    }

    /*
     * Gets a transient rule with given name
     *
     * If rule with given name doesn't exist, throw RESOURCE_NOT_FOUND.
     */
    public RuleWrapper getRule(String name) {
        RuleWrapper rw = findRuleByName(name);
        if (rw != null) {
            return rw;
        }
        throw new RequestException(RESOURCE_NOT_FOUND,
                                   "Filter rule not found: " + name);
    }

    /* Finds transient rule with the given name */
    private RuleWrapper findRuleByName(String name) {
        for (Set<RuleWrapper> subRules : rules.values()) {
            for (RuleWrapper rw : subRules) {
                if (rw.isTransient() &&
                    rw.getRuleName().equalsIgnoreCase(name)) {
                    return rw;
                }
            }
        }
        return null;
    }

    /*
     * Returns an iterator over the cached rules in order of rule type.
     *
     * If all is true, return an iterator over all the rules in cache.
     * Otherwise return an iterator over transient rules only.
     */
    public Iterator<RuleWrapper> rulesIterator(boolean all) {
        return new Iterator<RuleWrapper>() {

            private Iterator<RuleType> typeIter = rules.keySet().iterator();
            private Iterator<RuleWrapper> ruleIter = getNextRulesIterator();
            private RuleWrapper currentRule = null;

            @Override
            public boolean hasNext() {
                if (currentRule != null) {
                    return true;
                }

                while (ruleIter != null) {
                    while (ruleIter.hasNext()) {
                        RuleWrapper rw = ruleIter.next();
                        if (all || rw.isTransient()) {
                            currentRule = rw;
                            return true;
                        }
                    }
                    ruleIter = getNextRulesIterator();
                }
                return false;
            }

            private Iterator<RuleWrapper> getNextRulesIterator() {
                while (typeIter.hasNext()) {
                    Set<RuleWrapper> subRules = rules.get(typeIter.next());
                    if (!subRules.isEmpty()) {
                        return subRules.iterator();
                    }
                }
                return null;
            }

            @Override
            public RuleWrapper next() {
                if (hasNext()) {
                    RuleWrapper ret = currentRule;
                    currentRule = null;
                    return ret;
                }
                return null;
            }
        };
    }

    public boolean hasTransientRule() {
        return rulesIterator(false).hasNext();
    }

    /*
     * Returns the "big red button" rule which block all data requests, it is
     * called by DaveServiceHandler.checkFilterAll().
     */
    public Rule getFilterAllRule() {
        Set<RuleWrapper> subRules = rules.get(RuleType.ALL);
        if (subRules != null) {
            /* There can only be one filter all rule */
            Iterator<RuleWrapper> iter = subRules.iterator();
            if (iter.hasNext()) {
                RuleWrapper rw = iter.next();
                if (rw != null) {
                    return rw.getRule();
                }
            }
        }
        return null;
    }

    /* Pulls persistent rules from SC and load to proxy cache. */
    public PullRulesResult reloadPersistentRules(LogContext lc) {
        PullRulesResult ret = pullThread.execute(lc);
        logger.info("[Filter] Reload persistent rules: " + ret, lc);
        return ret;
    }

    /*
     * Updates the persistent rules in cache.
     *
     * Adds the given persistent rules and delete those obsolete persistent
     * rules which are not included in persistentRules.
     */
    private synchronized int updatePersistentRules(Rule[] prules) {

        final long fromTime = System.currentTimeMillis();
        final Map<RuleType, List<Rule>> newRules = new HashMap<>();

        /* Group the persistent rules to load by type */
        for (Rule rule : prules) {
            RuleType type = getType(rule);
            List<Rule> subRules = newRules.get(type);
            if (subRules == null) {
                subRules = new ArrayList<>();
                newRules.put(type, subRules);
            }
            subRules.add(rule);
        }

        /*
         * Load persistent rules.
         *
         * Add a new rule if no rule with same attributes exists, otherwise
         * set or update its persistent name.
         */
        for (Map.Entry<RuleType, List<Rule>> e : newRules.entrySet()) {
            RuleType type = e.getKey();
            Set<RuleWrapper> subRules = rules.get(type);

            /*
             * No existing rules with the given type, put all the rules with
             * this type.
             */
            if (subRules == null) {
                subRules = new HashSet<>();
                rules.put(type, subRules);
                for (Rule r : e.getValue()) {
                    subRules.add(new RuleWrapper(r, false /* isTransient */));
                }
            } else  {
                /*
                 * Check if the new rule is duplicated on attributes, update
                 * its persistent name if so. Otherwise, put the new persistent
                 * rule to the sub group.
                 */
                for (Rule r : e.getValue()) {
                    boolean found = false;
                    for (RuleWrapper rw : subRules) {
                        if (!rw.isTransient() && rw.attributesEqual(r)) {
                            rw.setRuleName(r.getName());
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        subRules.add(new RuleWrapper(r, false /* isTransient */));
                    }
                }
            }
        }

        /*
         * Deletes the old persistent rules which are not included in input
         * persistent Rules
         */
        int numDel = 0;
        for (Set<RuleWrapper> infos : rules.values()) {
            Iterator<RuleWrapper> iter = infos.iterator();
            while(iter.hasNext()) {
                RuleWrapper rw = iter.next();
                if (!rw.isTransient() && rw.getLastUpdateTime() < fromTime) {
                    iter.remove();
                    numDel++;
                }
            }
        }
        return numDel;
    }

    /*
     * Finds a rule matching the given information about the request.
     *
     * The method is called by DataServiceHandler.filterRequest() to filter
     * request with rules, it is called multiple times at different stages
     * when get below information for each request
     *   1. OpCode
     *      matchRule(opCode, null, null, null)
     *
     *   2. Principal information
     *      matchRule(opCode, tanentId, userId, null)
     *
     *   3. Target table ocid
     *      matchRule(opCode, tenantId, userId, tableId)
     *
     * So based on input parameters, we can determine the current stage and
     * check the corresponding type of rules only, this is to avoid check
     * the rules already checked in previous stages.
     */
    public Rule matchRule(OpCode op,
                          String tenantId,
                          String userId,
                          String tableId) {

        if (rules.isEmpty()) {
            return null;
        }

        RuleType type;
        if (tableId != null) {
            type = RuleType.TABLE;
        } else if (tenantId != null || userId != null) {
            type = RuleType.PRINCIPAL;
        } else {
            type = RuleType.OPERATION;
        }

        final Set<RuleWrapper> subRules = rules.get(type);
        if (subRules != null) {
            Iterator<RuleWrapper> iter = subRules.iterator();
            while (iter.hasNext()) {
                RuleWrapper rw = iter.next();
                if (rw != null) {
                    Rule rule = rw.getRule();
                    if (matchRule(rule, op, tenantId, userId, tableId)) {
                        return rule;
                    }
                }
            }
        }
        return null;
    }

    /* Check if the given information matched the specified rule */
    static boolean matchRule(Rule rule,
                             OpCode op,
                             String tenantId,
                             String userId,
                             String tableId) {
        if (rule.getOperations() != null) {
            if (!matchOpTypes(op, rule.getOpTypes())) {
                return false;
            }
        }

        if (rule.getTenant() != null) {
            if (!rule.getTenant().equalsIgnoreCase(tenantId)) {
               return false;
            }
        }

        if (rule.getUser() != null) {
            if (!rule.getUser().equalsIgnoreCase(userId)) {
                return false;
            }
        }

        if (rule.getTable() != null) {
            if (!rule.getTable().equalsIgnoreCase(tableId)) {
               return false;
            }
        }
        return true;
    }

    /* Checks if the op belongs to any OpType in the given set */
    private static boolean matchOpTypes(OpCode op, Set<OpType> types) {
        if (types.contains(OpType.ALL)) {
            return true;
        }

        OpType opType = getOpType(op);
        for (OpType type : types) {
            if (type == opType) {
                return true;
            }
        }
        return false;
    }

    static OpType getOpType(OpCode op) {
        OpType opType = opMap.get(op);
        if (opType != null) {
            return opType;
        }
        throw new IllegalStateException("Operation type not found: " + op);
    }

    /* Returns the action handler of the given rule */
    public Action getAction(Rule rule) {
        switch(rule.getAction()) {
        case DROP_REQUEST:
            return dropRequest;
        }
        throw new IllegalStateException("Unsupported action type: " +
                                        rule.getAction());
    }

    private RuleType getType(Rule rule) {
        if (rule.getTable() != null) {
            return RuleType.TABLE;
        }
        if (rule.getTenant() != null || rule.getUser() != null) {
            return RuleType.PRINCIPAL;
        }
        if (Rule.isAllOpType(rule.getOpTypes())) {
            return RuleType.ALL;
        }
        return RuleType.OPERATION;
    }

    /*
     * The RuleWrapper that represents a rule which can be transient or
     * persistent and possibly referenced by another rule:
     *   - rule: the rule object.
     *   - isTransient: the flag indicates the rule is transient if true, false
     *     for persistent.
     *   - lastUpdateTime: the last update time of this RuleWrapper. It is used
     *     for reload persistent rules to identify and delete those obsolete
     *     rules which are not included in the latest persistent rules from SC.
     */
    static class RuleWrapper {
        private final Rule rule;
        private boolean isTransient;
        private long lastUpdateTime;

        RuleWrapper(Rule rule, boolean isTransient) {
            this.rule = rule;
            this.isTransient = isTransient;
            setLastUpdateTime();
        }

        boolean isTransient() {
            return isTransient;
        }

        long getLastUpdateTime() {
            return lastUpdateTime;
        }

        Rule getRule() {
            return rule;
        }

        boolean attributesEqual(Rule r1) {
            return rule.attributesEqual(r1);
        }

        void setRuleName(String name) {
            assert(name != null);
            rule.setName(name);
            setLastUpdateTime();
        }

        String getRuleName() {
            return rule.getName();
        }

        private void setLastUpdateTime() {
            lastUpdateTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("isTransient=").append(isTransient);
            sb.append(", lastUpdateTime=").append(lastUpdateTime);
            sb.append(", rule=").append(rule.toJson());
            return sb.toString();
        }
    }

    /*
     * The helper interface that will be passed into AccessChecker.checkAccess()
     * to filter request.
     */
    public interface Filter {
        public void filterRequest(OpCode op,
                                  String principalTenantId,
                                  String principalId,
                                  String tableName,
                                  LogContext lc);
    }

    /*
     * Action handler interface
     */
    public interface Action {
        FullHttpResponse handleRequest(ByteBuf responseBuffer,
                                       String requestId,
                                       LogContext lc);
    }

    /*
     * The action handler that simply drops request.
     */
    private static class DropRequest implements Action {
        @Override
        public FullHttpResponse handleRequest(ByteBuf responseBuffer,
                                              String requestId,
                                              LogContext lc) {
            if (responseBuffer != null) {
                responseBuffer.release();
            }
            return null;
        }
    }

    /*
     * The thread periodically pull persistent rules from SC and load to
     * proxy cache.
     */
    private class PullRulesThread extends Thread {
        private static final int MAX_RETRIES = 3;
        private static final int DELAY_MS = 1000;

        private int intervalSec;

        PullRulesThread(int intervalSec) {
            this.intervalSec = intervalSec;
        }

        @Override
        public void run() {
            logger.info("[Filter] Start pulling persistent rules, " +
                        "interval is " + intervalSec + "sec");
            while (true) {
                PullRulesResult ret = execute(null);
                if (ret.getNumLoaded() > 0 || ret.getNumDeleted() > 0) {
                    logger.info("[Filter] Reload persistent rules: " + ret);
                } else {
                    logger.fine("[Filter] Reload persistent rules: " + ret);
                }
                try {
                    Thread.sleep(intervalSec * 1000);
                } catch (InterruptedException e) {
                    /* do nothing */
                }
            }
        }

        PullRulesResult execute(LogContext lc) {
            Rule[] prules = null;
            int retries = 0;

            final long startTime = System.currentTimeMillis();
            /* Retrieves persistent rules from SC, retry if failed */
            while (true) {
                try {
                    prules = getPersistentRules(null);
                    if (retries > 0) {
                        logger.info(
                            "[Filter] List persist rules successfully after " +
                            retries + " retry", lc);
                    }
                    break;
                } catch (RuntimeException ex) {
                    if (retries++ < MAX_RETRIES) {
                        logger.warning(
                            "[Filter] Failed to list persist rules, retry=" +
                            retries + ", error=" + ex.getMessage(), lc);
                        try {
                            Thread.sleep(DELAY_MS);
                        } catch (InterruptedException ignored) {
                            /* do nothing */
                        }
                        continue;
                    }
                    logger.warning("[Filter] Failed to list persist rules " +
                        "after retry " + retries + " times : " +
                        ex.getMessage(), lc);
                    throw ex;
                }
            }

            int numDel = 0;
            if (prules.length > 0 || !rules.isEmpty()) {
                /*
                 * Reload persistent rules to cache, remove existing persistent
                 * rules which are not included in the given array.
                 */
                numDel = updatePersistentRules(prules);
            }

            long timeMs = (System.currentTimeMillis() - startTime);
            return new PullRulesResult(prules.length, numDel, timeMs);
        }

        /* Get persistent rules from SC */
        private Rule[] getPersistentRules(LogContext lc) {
            ListRuleResponse resp = tm.listRules(lc);
            if (!resp.getSuccess()) {
                throw new RequestException(resp.getErrorCode(),
                                           resp.getErrorString());
            }
            return resp.getRules();
        }
    }

    /*
     * The result of a task that pulls persistent rules and update proxy cache.
     */
    static class PullRulesResult {
        private int numLoaded;
        private int numDeleted;
        private long timeMs;

        PullRulesResult(int numLoaded, int numDeleted, long timeMs) {
            this.numLoaded = numLoaded;
            this.numDeleted = numDeleted;
            this.timeMs = timeMs;
        }

        public int getNumLoaded() {
            return numLoaded;
        }

        public int getNumDeleted() {
            return numDeleted;
        }

        public long getTimeMs() {
            return timeMs;
        }

        @Override
        public String toString() {
            return numLoaded + " loaded, " + numDeleted + " deleted (" +
                   timeMs + "ms)";
        }
    }
}
