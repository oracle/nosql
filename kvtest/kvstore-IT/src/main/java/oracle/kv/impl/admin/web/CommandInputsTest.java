/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.web;

import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.shell.ShellArgumentException;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.ObjectNode;

import org.junit.Test;

public class CommandInputsTest {

    @Test
    public void testGetFilteredArguments() throws Exception {
        CommandInputs inputs = new CommandInputs();
        inputs.setCommand("command");
        inputs.setArguments(null);
        inputs.getFilteredArguments();

        ObjectNode [] ons = new ObjectNode[2];
        ons[0] = JsonUtils.createObjectNode();
        ons[0].put("security", true);
        ons[1] = JsonUtils.createObjectNode();
        ArrayNode an = JsonUtils.createArrayNode();
        ons[1].set("params", an);
        ObjectNode anOn = JsonUtils.createObjectNode();
        anOn.put("javaMisc", "abc");
        an.add(anOn);
        inputs.setArguments(ons);
        inputs.getFilteredArguments();

        ons = new ObjectNode[1];
        ons[0] = JsonUtils.createObjectNode();
        ons[0].put("abc", "abc");
        inputs.setArguments(ons);
        try {
            inputs.getFilteredArguments();
        } catch (ShellArgumentException e) {
            /* Expected */
        }

        ons = new ObjectNode[2];
        ons[0] = JsonUtils.createObjectNode();
        ons[0].put("security", false);
        ons[1] = JsonUtils.createObjectNode();
        an = JsonUtils.createArrayNode();
        ons[1].put("non-params", "test");
        inputs.setArguments(ons);
        inputs.getFilteredArguments();

        ons = new ObjectNode[2];
        ons[0] = JsonUtils.createObjectNode();
        ons[0].put("secObjectAbc", "abc");
        ons[1] = JsonUtils.createObjectNode();
        an = JsonUtils.createArrayNode();
        ons[1].put("params", "test");
        inputs.setArguments(ons);
        try {
            inputs.getFilteredArguments();
        } catch (ShellArgumentException e) {
            /* Expected */
        }
    }
}
