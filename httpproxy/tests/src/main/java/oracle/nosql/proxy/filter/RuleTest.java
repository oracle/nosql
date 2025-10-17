/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import oracle.nosql.proxy.filter.FilterHandler.RuleWrapper;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.util.filter.Rule;

/*
 * Test Rule.match() and FilterService add/delete/get/list operations
 */
public class RuleTest extends FilterTestBase {

    private final String testUserId =
        "ocid1.user.oc1..aaaaaaaaewwvvrm63ckh4e5an3q4use7tyieefx4qlpfxdkgezpujubfpp2a";
    private final String testTenantId =
        "ocid1.tenancy.oc1..aaaaaaaattuxbj75pnn3nksvzyidshdbrfmmeflv4kkemajroz2thvca4kba";
    private final String testTableId =
        "ocid1.nosqltable.oc1.phx.amaaaaaackmxu5iakh7m2e4uyfuyd66amaypeogtrcb5gu4iveqlcm567ppa";

    /* Test FilterHandler.matchRule() */
    @Test
    public void testMatchRule() {
        Rule.OpType type;
        boolean ret;
        boolean exp;
        Rule rule;

        rule = Rule.createRule("rule1", null, null, null, null,
                               new String[] {"all"});
        for (OpCode op : OpCode.values()) {
            ret = FilterHandler.matchRule(rule, op, testTenantId,
                                          testUserId, testTableId);
            assertTrue(ret);
        }

        rule = Rule.createRule("rule1", null, null, null, null,
                               new String[] {"ddl", "write"});
        for (OpCode op : OpCode.values()) {
            type = FilterHandler.getOpType(op);
            exp = (type == Rule.OpType.DDL) || (type == Rule.OpType.WRITE);
            ret = FilterHandler.matchRule(rule, op, testTenantId,
                                          testUserId, testTableId);
            assertEquals(exp, ret);
        }

        rule = Rule.createRule("rule1", null, testTenantId, null, null,
                               new String[]{"ddl"});
        assertTrue(FilterHandler.matchRule(rule, OpCode.CREATE_TABLE,
                                           testTenantId, testUserId,
                                           testTableId));
        assertFalse(FilterHandler.matchRule(rule, OpCode.CREATE_TABLE,
                                            "invalidTenant", testUserId,
                                            testTableId));

        rule = Rule.createRule("rule1", null, null, testUserId,
                               null, new String[]{"ddl"});
        assertTrue(FilterHandler.matchRule(rule, OpCode.CREATE_TABLE,
                                           testTenantId, testUserId,
                                           testTableId));
        assertFalse(FilterHandler.matchRule(rule, OpCode.CREATE_TABLE,
                                            testTenantId, "invalidUser",
                                            testTableId));

        rule = Rule.createRule("rule1", null, null, null, testTableId,
                               new String[]{"all"});
        assertTrue(FilterHandler.matchRule(rule, OpCode.CREATE_TABLE,
                                           testTenantId, testUserId,
                                           testTableId));
        assertFalse(FilterHandler.matchRule(rule, OpCode.CREATE_TABLE,
                                            testTenantId, testUserId,
                                            null));
        assertFalse(FilterHandler.matchRule(rule, OpCode.CREATE_TABLE,
                                            testTenantId, testUserId,
                                            "invalidTable"));

        rule = Rule.createRule("rule1", Rule.DROP_REQUEST,
                                testTenantId, testUserId, testTableId,
                                new String[] {"ddl"});
        assertTrue(FilterHandler.matchRule(rule, OpCode.ALTER_TABLE,
                                           testTenantId, testUserId,
                                           testTableId));
        assertFalse(FilterHandler.matchRule(rule, OpCode.QUERY,
                                            testTenantId, testUserId,
                                            testTableId));
        assertFalse(FilterHandler.matchRule(rule, OpCode.ALTER_TABLE,
                                            "invalidTenant", testUserId,
                                            testTableId));
        assertFalse(FilterHandler.matchRule(rule, OpCode.ALTER_TABLE,
                                            testTenantId, "invalidUser",
                                            testTableId));
        assertFalse(FilterHandler.matchRule(rule, OpCode.ALTER_TABLE,
                                            testTenantId, testUserId,
                                            "invalidTable"));
    }

    /* Test add rule */
    @Test
    public void testAddRule() {
        assumeTrue(!onprem);

        /* Add filter all rule */
        Rule rule = Rule.createRule("rule_1", null, null, null, null,
                                    new String[] {"all"});
        addRule(rule.toJson(), false);
        assertRulesEquals(rule, getRule(rule.getName(), false));
        /* Add the rule again, should be rejected */
        addRule(rule.toJson(), 409 /* CONFLICT */, false);

        /*
         * Adding a block all rule that includes all the operation types
         * (read, write, ddl) which is equivalent to "ALL" should be rejected
         * as well.
         */
        rule = Rule.createRule("rule_2", null, null, null, null,
                               new String[] {"read", "write", "ddl",
                                             "config_read", "config_update"});
        addRule(rule.toJson(), 409 /* CONFLICT */, false);

        /*
         * Update the rule to block the specified user operates on
         * the specified table.
         */
        rule = Rule.createRule("rule_3", Rule.DROP_REQUEST,
                               null, testUserId, testTableId,
                               new String[] {"all"});
        addRule(rule.toJson(), false);
        assertRulesEquals(rule, getRule(rule.getName(), false));

        /*
         * Add a duplicate rule which same user, table and equivalent
         * operations, it should be rejected.
         */
        rule = Rule.createRule("rule_4", Rule.DROP_REQUEST,
                               null, testUserId, testTableId,
                               new String[] {"read", "write", "ddl",
                                             "config_read", "config_update"});
        addRule(rule.toJson(), 409 /* CONFLICT */, false);

        /* Invalid rules */
        String[] badPayloads = new String[] {
            "",
            "abc",
            "{\"user\": \"" + testUserId + "\"}",
            "{\"name\": \"rule1\"}",
            "{\"name\": \"rule1\", \"user\":\" + testUserId + \"}",
        };
        for (String bad : badPayloads) {
            addRule(bad, 400, false);
        }
    }

    /* Test get and delete rule */
    @Test
    public void testGetDeleteRule() {
        final Rule[] rules = new Rule[] {
            Rule.createRule("block_all", null, null, null, null,
                            new String[] {"all"}),
            Rule.createRule("block_user_" + testUserId, Rule.DROP_REQUEST,
                            null, testUserId, null, new String[] {"ddl"}),
            Rule.createRule("block_user_table", Rule.DROP_REQUEST,
                            null, testUserId, testTableId,
                            new String[] {"ddl", "write"}),
        };

        for (Rule rule : rules) {
            addRule(rule.toJson(), false);
        }
        assertEquals(rules.length, listRules(false).size());

        /* Delete the rule with case-insensitive name */
        boolean testUpperCase = false;
        String name;
        for (Rule rule : rules) {
            name = rule.getName();
            if (testUpperCase) {
                name = name.toUpperCase();
            }
            testUpperCase = !testUpperCase;

            Rule ret = getRule(name, false);
            assertRulesEquals(rule, ret);

            /* Delete the rule */
            assertTrue(deleteRule(name, false));

            /* Get the rule, should get 404 NOT_FOUND */
            getRule(name, 404, false);

            /* Delete again, should get 404 NOT_FOUND */
            assertFalse(deleteRule(name, 404, false));
        }
        assertTrue(listRules(false).isEmpty());

        /* Invalid argument */
        deleteRule(null, 400, false);
        deleteRule("", 400, false);
    }

    /* Test list rules */
    @Test
    public void testListRules() {
        List<Rule> rules = new ArrayList<>();

        rules.add(Rule.createRule("block_all", null, null, null, null,
                                   new String[] {"all"}));
        rules.add(Rule.createRule("block_ddl_write", null, null, null, null,
                                   new String[] {"ddl", "write"}));
        rules.add(Rule.createRule("block_user", Rule.DROP_REQUEST,
                                  null, testUserId, null,
                                  new String[]{"read", "write"}));
        rules.add(Rule.createRule("block_user_table", Rule.DROP_REQUEST,
                                  null, testUserId, testTableId,
                                  new String[] {"all"}));
        for (Rule rule : rules) {
            addRule(rule.toJson(), false);
        }

        List<Rule> results = listRules(false);
        assertEquals(rules.size(), results.size());
        for (int i = 0; i < results.size(); i++) {
            assertRulesEquals(rules.get(i), results.get(i));
        }
    }

    /* Test persistent rules add/delete/get */
    @Test
    public void testPersistentRule() {
        assumeTrue(useMiniCloud);

        Rule[] rules = new Rule[] {
            Rule.createRule("rule_1", Rule.DROP_REQUEST,
                            testTenantId, null, null,
                            new String[] {"all"}),
            Rule.createRule("rule_2", Rule.DROP_REQUEST,
                            null, testUserId, null,
                            new String[] {"all"}),
            Rule.createRule("rule_3", Rule.DROP_REQUEST,
                            null, null, testTableId,
                            new String[] {"all"}),
            Rule.createRule("rule_4", Rule.DROP_REQUEST,
                            null, testUserId, testTableId,
                            new String[] {"read", "write", "ddl",
                                          "config_read", "config_update"})
        };

        for (Rule rule: rules) {
            addRule(rule.toJson(), true /* persistent */);
            assertRulesEquals(rule, getRule(rule.getName(), true));
        }
        assertEquals(rules.length, listRules(true).size());

        /*
         * Add a duplicate rule with same name, it should be rejected.
         */
        Rule rule = Rule.createRule("rule_2", Rule.DROP_REQUEST,
                                    null, testUserId, testTableId,
                                    new String[] {"write", "ddl"});
        addRule(rule.toJson(), 409 /* CONFLICT */, true /* persistent */);

        /*
         * Add a duplicate rule with same attributes, it should be rejected.
         */
        rule = Rule.createRule("rule_4_dup", Rule.DROP_REQUEST,
                               null, testUserId, testTableId,
                               new String[] {"all"});
        addRule(rule.toJson(), 409 /* CONFLICT */, true /* persistent */);

        /* Invalid rules */
        String[] badPayloads = new String[] {
            "{\"user\": \"" + testUserId + "\"}",
            "{\"name\": \"rule1\"}",
            "{\"name\": \"rule1\", \"user\":\" + testUserId + \"}",
            "{\"name\": \"rule1\", \"operations\":[\"ddl\",\"write\"]}",
        };
        for (String bad : badPayloads) {
            addRule(bad, 400, true);
        }

        /* Delete the rule */
        rule = rules[0];
        String name = rule.getName().toUpperCase();
        assertRulesEquals(rule, getRule(name, true));
        assertTrue(deleteRule(name, true));

        /* Get the rule, should get 404 NOT_FOUND */
        getRule(name, 404, true);

        /* Delete again, should get 404 NOT_FOUND */
        assertFalse(deleteRule(name, 404, true));
    }

    /* Test mixing transient and persistent rules in cache */
    @Test
    public void testMixedTransientPersistentRules() {
        assumeTrue(useMiniCloud);

        final String transientName = "t_block_usera";
        final String persistentName = "p_block_usera";

        List<RuleWrapper> rules;
        RuleWrapper rw;

        /* Add a transient rule */
        Rule trule = Rule.createRule(transientName,
                                     Rule.DROP_REQUEST,
                                     null,
                                     testUserId,
                                     testTableId,
                                     new String[] {"all"});
        addRule(trule.toJson(), false /* persist */);
        rules = listAllCacheRules();
        assertEquals(1, rules.size());
        rw = rules.get(0);
        assertTrue(transientName.equalsIgnoreCase(rw.getRuleName()));
        assertTrue(rw.isTransient());

        /* Add a persistent rule with same attributes as above transient rule */
        Rule prule = Rule.createRule(persistentName,
                                     Rule.DROP_REQUEST,
                                     null,
                                     testUserId,
                                     testTableId,
                                     new String[] {"all"});
        addRule(prule.toJson(), true /* persist */);
        reloadPersistentRules();
        rules = listAllCacheRules();
        assertEquals(2, rules.size());

        /* Delete the transient rule */
        deleteRule(transientName, false /* persist */);
        rules = listAllCacheRules();
        assertEquals(1, rules.size());
        rw = rules.get(0);
        assertTrue(persistentName.equalsIgnoreCase(rw.getRuleName()));
        assertFalse(rw.isTransient());

        /* Add the transient rule back */
        addRule(trule.toJson(), false /* persist */);
        rules = listAllCacheRules();
        assertEquals(2, rules.size());

        /* Delete the persistent rule */
        deleteRule(persistentName, true /* persist */);
        reloadPersistentRules();
        rules = listAllCacheRules();
        assertEquals(1, rules.size());
        rw = rules.get(0);
        assertTrue(transientName.equalsIgnoreCase(rw.getRuleName()));
        assertTrue(rw.isTransient());

        /* Delete transient rule */
        deleteRule(transientName, false /* persist */);
        rules = listAllCacheRules();
        assertEquals(0, rules.size());
    }
}
