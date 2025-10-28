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

package oracle.nosql.proxy.sc;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.filter.Rule;

/**
 * Response to a TenantManager listRules operation.
 */
public class ListRuleResponse extends CommonResponse {

    private final Rule[] rules;

    public ListRuleResponse(int httpResponse, Rule[] rules) {
        super(httpResponse);
        this.rules = rules;
    }

    public ListRuleResponse(ErrorResponse err) {
        super(err);
        rules = null;
    }

    /**
     * Returns a list of Rules
     */
    public Rule[] getRules() {
        return rules;
    }

    /**
     * {
     *   "rules" : [...]
     * }
     *
     */
    @Override
    public String successPayload() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"rules\": ");
            sb.append(JsonUtils.prettyPrint(rules)).append("}");
            return sb.toString();
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ListRulesResponse [rules=[");
        if (rules == null) {
            sb.append("null");
        } else {
            for (int i = 0; i < rules.length; i++) {
                sb.append(rules[i].toString());
                if (i < (rules.length - 1)) {
                    sb.append(",");
                }
            }
        }
        sb.append("]]");
        return sb.toString();
    }
}
