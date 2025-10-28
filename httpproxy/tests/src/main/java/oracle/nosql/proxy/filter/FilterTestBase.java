/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static oracle.nosql.proxy.protocol.HttpConstants.FILTERS_PATH;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

import org.junit.BeforeClass;
import com.google.gson.reflect.TypeToken;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import oracle.nosql.proxy.ProxyTestBase;
import oracle.nosql.proxy.filter.FilterHandler.RuleWrapper;
import oracle.nosql.util.HttpRequest;
import oracle.nosql.util.HttpResponse;
import oracle.nosql.util.filter.Rule;
import oracle.nosql.util.filter.Rule.Action;

public class FilterTestBase extends ProxyTestBase {

    private final String proxyFilterUrl =
        getProxyEndpoint() + "/V0/" + FILTERS_PATH;
    private final HttpRequest httpRequest = new HttpRequest().disableRetry();
    private String scFilterUrl;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        /*
         * This test needs to call proxy filter rest API to setup the filter
         * rules, the filter rest API is not supported by onprem and not
         * accessible in cloud service, so skip the test in onprem or cloud test.
         */
        assumeTrue("Skip FilterTestBase in onprem or could test",
                   !Boolean.getBoolean(ONPREM_PROP) &&
                   !Boolean.getBoolean(USECLOUD_PROP));

        ProxyTestBase.staticSetUp();
    }

    @Override
    public void setUp() throws Exception {
        /*
        * Set Netty to use JDK logger factory.
        */
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        removeAllRules();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        removeAllRules();
        super.tearDown();
    }

    Rule addRule(String name, Action action, String[] operations) {
        return addRule(name, action, null /* tenant */, null /* user */,
                       null /* table */, operations, false /* persist */);
    }

    Rule addRule(String name,
                 Action action,
                 String tenantId,
                 String userId,
                 String tableId,
                 String[] operations,
                 boolean persist) {

        Rule rule = Rule.createRule(name, action, tenantId, userId,
                                    tableId, operations);
        addRule(rule.toJson(), persist);

        rule = getRule(name, persist);
        assertNotNull(rule);
        return rule;
    }

    void addRule(String payload, boolean persist) {
        addRule(payload, HttpResponseStatus.OK.code(), persist);
    }

    void addRule(String payload, int statusCode, boolean persist) {
        String url = getUrl(null, persist);
        HttpResponse resp = httpRequest.doHttpPost(url, payload);
        assertEquals(statusCode, resp.getStatusCode());
    }

    Rule getRule(String name, boolean persist) {
        return getRule(name, HttpResponseStatus.OK.code(), persist);
    }

    Rule getRule(String name, int statusCode, boolean persist) {
        String url = getUrl(name, persist);
        HttpResponse resp = httpRequest.doHttpGet(url);
        assertEquals(statusCode, resp.getStatusCode());
        if (statusCode == HttpResponseStatus.OK.code()) {
            return parseRuleFromResponse(resp);
        }
        return null;
    }

    boolean deleteRule(String name, boolean persist) {
        return deleteRule(name, HttpResponseStatus.OK.code(), persist);
    }

    boolean deleteRule(String name, int statusCode, boolean persist) {
        String url = getUrl(name, persist);
        HttpResponse resp = httpRequest.doHttpDelete(url, null);
        assertEquals(statusCode, resp.getStatusCode());
        if (statusCode == HttpResponseStatus.OK.code()) {
            return resp.getOutput().contains("deleted");
        }
        return false;
    }

    List<Rule> listRules(boolean persist) {
        String url = getUrl(null, persist);
        HttpResponse resp = httpRequest.doHttpGet(url);
        assertEquals(HttpResponseStatus.OK.code(), resp.getStatusCode());
        return parseRulesFromResponse(resp);
    }

    List<RuleWrapper> listAllCacheRules() {
        String url = getUrl(null, false) + "?all=true";
        HttpResponse resp = httpRequest.doHttpGet(url);
        assertEquals(HttpResponseStatus.OK.code(), resp.getStatusCode());
        return parseRuleWrappersFromResponse(resp);
    }

    void reloadPersistentRules() {
        String url = getUrl("reload", false);
        HttpResponse resp = httpRequest.doHttpPut(url, null);
        assertEquals(HttpResponseStatus.OK.code(), resp.getStatusCode());
    }

    private String getUrl(String append, boolean persist) {
        String url = persist ? getSCFilterUrl() : proxyFilterUrl;
        if (url == null) {
            fail("Filter url should not be null");
        }
        if (append != null) {
            url += "/" + append;
        }
        return url;
    }

    private String getSCFilterUrl() {
        if (cloudRunning) {
            if (scFilterUrl == null && scHost != null && scPort != null) {
                scFilterUrl = "http://" + scHost + ":" + scPort + "/V0/filters";
            }
            return scFilterUrl;
        }
        return null;
    }

    private void removeAllRules() {
        removeAllRules(false);
        if (cloudRunning) {
            removeAllRules(true);
        }
        reloadPersistentRules();
    }

    void removeAllRules(boolean persist) {
        List<Rule> rules = listRules(persist);
        for (Rule rule : rules) {
            assertTrue(deleteRule(rule.getName(), persist));
        }
        assertTrue(listRules(persist).isEmpty());
    }

    private List<Rule> parseRulesFromResponse(HttpResponse resp) {
        String output = resp.getOutput().trim();
        if (output.isEmpty()) {
            return null;
        }

        Type type = new TypeToken<List<Rule>>(){}.getType();
        return Rule.getGson().fromJson(output, type);
    }

    private Rule parseRuleFromResponse(HttpResponse resp) {
        String output = resp.getOutput().trim();
        if (output.isEmpty()) {
            return null;
        }

        return Rule.fromJson(output);
    }

    private List<RuleWrapper> parseRuleWrappersFromResponse(HttpResponse resp) {
        String output = resp.getOutput().trim();
        if (output.isEmpty()) {
            return null;
        }

        Type type = new TypeToken<List<RuleWrapper>>(){}.getType();
        return Rule.getGson().fromJson(output, type);
    }

    void assertRulesEquals(Rule r1, Rule r2) {
        assertTrue(r1.getName().equalsIgnoreCase(r2.getName()));
        assertTrue(r1.getAction().equals(r1.getAction()));
        assertTrue(Objects.equals(r1.getTenant(), r2.getTenant()));
        assertTrue(Objects.equals(r1.getUser(), r2.getUser()));
        assertTrue(Objects.equals(r1.getTable(), r2.getTable()));
        assertTrue(r1.getOpTypes().equals(r2.getOpTypes()));
    }
}
