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

package oracle.kv.impl.admin.web;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.nosql.common.json.ObjectNode;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * POJO class to parse inputs from Http request's JSON payload.
 * Example of CommandInputs:
 * {
 *   "command" : "deploy-sn",
 *   "arguments" : [{"zn":"zn1"}, {"host":"localhost"}, {"port": 5000}]
 * }
 */
public class CommandInputs {

    private static final String DELIMITER = "-";

    /* command should be the main command name or sub command name */
    private String command;

    /*
     * Object node array for input arguments. Each of object node has only
     * one entry, the entry name is the argument name, the entry value is the
     * argument value. Argument value can be boolean, number or string, they
     * will eventually convert to string inputs for shell command.
     */
    private ObjectNode[] arguments;

    /*
     * "params" is a special argument field to handle.
     */
    private static final String PARAMS = "params";

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public ObjectNode[] getArguments() {
        return arguments;
    }

    public void setArguments(ObjectNode[] arguments) {
        this.arguments = arguments;
    }

    /*
     * Filter the argument names. Argument name need to start with lower case
     * letter. If there is multiple components of names, start from the second
     * component, the sub name's first letter need to be in upper case. For
     * boolean argument, it will only add argument to list when boolean value
     * is true.
     * For example,
     * "arguments":
     *   [
     *     {"argumentAppleMultiPart":"string value"},
     *     {"argumentBoyMultiPart":true},
     *     {"argumentCatNultiPart":5000}
     *   ]
     *  This will get converted to following string list:
     *  ["-argument-apple-multi-part", "string value",
     *   "-argument-boy-multi-part", "-argument-cat-multi-part", "5000"]
     *
     * Implementation NOTE:
     *   In order to keep the JSON wrapper code footprint small this code
     *   directly uses internal GSON classes for filtering the arguments.
     */
    public List<String> getFilteredArguments() throws ShellException {
        final List<String> result = new ArrayList<String>();
        if (arguments == null) {
            return null;
        }
        for (ObjectNode on : arguments) {
            JsonObject gsonObject = on.getElement().getAsJsonObject();
            int count = 0;
            for (Map.Entry<String, JsonElement> entry : gsonObject.entrySet()) {
                String name = entry.getKey();
                JsonElement value = entry.getValue();
                if (name == null) {
                    throw new ShellArgumentException(
                        "Fail to convert arguments field, key is null");
                }
                if (value == null) {
                    throw new ShellArgumentException(
                        "Fail to convert arguments field, value is null");
                }
                if (value.isJsonPrimitive() &&
                    value.getAsJsonPrimitive().isBoolean()) {
                    if (value.getAsJsonPrimitive().getAsBoolean()) {
                        result.add(processKey(name));
                    }
                } else {
                    result.add(processKey(name));
                    if (name.equals(PARAMS)) {
                        if (!value.isJsonArray()) {
                            throw new ShellArgumentException(
                                "Fail to convert argument " + PARAMS +
                                ", value is not array");
                        }
                        for (JsonElement param : value.getAsJsonArray()) {
                            int fieldCounter = 0;
                            if (!param.isJsonObject()) {
                                continue;
                            }
                            for (Map.Entry<String, JsonElement> pEntry:
                                     param.getAsJsonObject().entrySet()) {
                                final String paramKey = pEntry.getKey();
                                if (paramKey == null) {
                                    throw new ShellArgumentException(
                                        "Fail to convert params field, " +
                                        "key is null");
                                }
                                JsonElement paramValue = pEntry.getValue();
                                if (paramValue == null) {
                                    throw new ShellArgumentException(
                                        "Fail to convert params field, " +
                                        "value is null");
                                }
                                result.add(paramKey + "=" +
                                           paramValue.getAsString());
                                fieldCounter++;
                            }
                            if (fieldCounter > 1) {
                                throw new ShellArgumentException(
                                    "Incorrect params format");
                            }
                        }
                    } else {
                        result.add(value.getAsString());
                    }
                }
                count ++;
            }
            if (count > 1) {
                throw new ShellArgumentException(
                    "Incorrect arguments format");
            }
        }
        return result;
    }

    /*
     * Convert the argument from JSON style name to command line argument
     * name. Example,
     * argumentWithMultiPart -> -argument-with-multi-part
     */
    private String processKey(String key) {
        if (key.isEmpty() || Character.isUpperCase(key.charAt(0))) {
            throw new IllegalArgumentException(
                "Incorrect arguments format");
        }
        key = DELIMITER + key;
        final StringBuffer sb = new StringBuffer();
        final Matcher m =
            Pattern.compile("([A-Z])([a-z]*)").matcher(key);

        while (m.find()) {
            m.appendReplacement(
              sb,
              DELIMITER + m.group(1).toLowerCase() + m.group(2).toLowerCase());
        }
        return m.appendTail(sb).toString();
    }
}
