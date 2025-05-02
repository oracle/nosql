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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;

/**
 * This class provides utilities for interaction with Jackson JSON processing
 * libraries, as well as helpful JSON operations, for creating command result
 * JSON output.
 */
public class CommandJsonUtils {
    /*
     * These string constants are used for construction of command result
     * JSON fields.
     */
    public static final String FIELD_OPERATION = "operation";
    public static final String FIELD_RETURN_CODE = "return_code";
    public static final String FIELD_DESCRIPTION = "description";
    public static final String FIELD_RETURN_VALUE = "return_value";
    public static final String FIELD_CLEANUP_JOB = "cmd_cleanup_job";

    /**
     * Return a command result JSON output based on the operation and
     * CommandResult. A sample JSON output:
     * {
     *   "operation" : "configure",
     *   "return_code" : 5400,
     *   "return_value" : "",
     *   "description" : "Deploy failed: ConnectionIOException ......" ,
     *   "cmd_cleanup_job" : [ "clean-store.kvs -config store.config" ]
     * }
     * @param operation the name set in "operation" JSON field
     * @param result contains the information set in other JSON fields
     * @return return the created JSON output string
     */
    public static String getJsonResultString(String operation,
                                             CommandResult result) {

        final ObjectNode jsonTop = JsonUtils.createObjectNode();
        updateNodeWithResult(jsonTop, operation, result);
        return toJsonString(jsonTop);
    }

    /**
     * Add command result JSON fields to jsonTop node.
     * <p>
     * For example:
     * jsonTop is:
     * {
     *   "name" : <i>plan_name</i>,
     *   "id" : <i>plan_id</i>,
     *   "state" : <i>plan_state</i>
     * }
     * It will be updated as following:
     * {
     *   "name" : <i>plan_name</i>,
     *   "id" : <i>plan_id</i>,
     *   "state" : <i>plan_state</i>,
     *   "operation" : "plan deploy-admin",
     *   "return_code" : 5400,
     *   "return_value" : "",
     *   "description" : "Deploy failed: ConnectionIOException ......" ,
     *   "cmd_cleanup_job" : [ "clean-store.kvs -config store.config" ]
     * }
     * @param jsonTop the JSON node to be updated
     * @param operation the name set in "operation" JSON field
     * @param result the created JSON output string
     */
    public static void updateNodeWithResult(ObjectNode jsonTop,
                                            String operation,
                                            CommandResult result) {

        if (result == null) {
            return;
        }
        jsonTop.put(FIELD_OPERATION, operation);
        jsonTop.put(FIELD_RETURN_CODE, result.getErrorCode());
        jsonTop.put(FIELD_DESCRIPTION, result.getDescription());
        final String returnValueJsonStr = result.getReturnValue();
        if (returnValueJsonStr != null) {
            ObjectNode returnValueNode =
                JsonUtils.parseJsonObject(returnValueJsonStr);
            jsonTop.set(FIELD_RETURN_VALUE, returnValueNode);
        }
        if (result.getCleanupJobs() != null) {
            ArrayNode cleanupJobNodes = jsonTop.putArray(FIELD_CLEANUP_JOB);
            for(String job: result.getCleanupJobs()) {
                cleanupJobNodes.add(job);
            }
        }
    }

    /**
     * Return the JSON string to present the jsonTop node.
     */
    public static String toJsonString(ObjectNode jsonTop) {

        return JsonUtils.toJsonString(jsonTop, true); // pretty print
    }

    /*
     * Convert string to object node
     */
    public static ObjectNode readObjectValue(String input) {
        try {
            return JsonUtils.parseJsonObject(input);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "Problem parsing JSON object: '" + input + "': " +
                e.getMessage(),
                e);
        }
    }

    /*
     * Common method to handle failure of conversion between JSON object and
     * JSON string. Return 5500 code when there is failure.
     */
    public static <T> T handleConversionFailure(JsonConversionTask<T> task)
        throws ShellException {
        try {
            return task.execute();
        } catch (IOException e) {
            throw new ShellException(e.getMessage(),
                                     ErrorMessage.NOSQL_5500,
                                     new String[] {});
        }
    }

    /**
     * Formats the a set of strings. The generated list is in sorted order.
     * If asJson is true a JSON output format is used, otherwise it is a
     * CRLF separated string of names.
     * JSON:  {"<listName>" : ["n1", "n2", ..., "nN"]}
     */
    static String formatList(String listName, Set<String> names, boolean asJson) {

        SortedSet<String> sortedSet = new TreeSet<>(names);
        StringBuilder sb = new StringBuilder();
        boolean first = true;

        if (asJson) {
            sb.append("{");
            sb.append("\"").append(listName).append("\"");
            sb.append(" : [");
            for (String s : sortedSet) {
                if (!first) {
                    sb.append(",");
                }
                first = false;
                sb.append("\"");
                sb.append(s);
                sb.append("\"");
            }
            sb.append("]}");
        } else {
            sb.append(listName);
            for (String s : sortedSet) {
                /*
                 * Indent list members by 2 spaces.
                 */
                sb.append("\n  ");
                sb.append(s);
            }
        }
        return sb.toString();
    }

    /*
     * Define the JSON conversion work, it may be JSON object convert to
     * JSON string, or JSON string convert to JSON object
     */
    public static interface JsonConversionTask<R> {
        abstract R execute() throws IOException;
    }

    /*
     * Code to translate keys of the form par1_part2 to part1Part2 (camelCase)
     * is here to isolate direct use of Gson objects. Doing this tree
     * traversal using JsonNode and wrapper classes would be painful.
     * TODO: consider a better way to abstract this translation...
     */

    /**
     * Convert the JSON v1 admin CLI result string to ShellCommandResult.
     * 1. Remove the following fields from the result
     *   operation
     *   return_code
     *   description
     *   return_value
     *   cmd_cleanup_job
     * 2. use removed operation, return_code and description to set
     * corresponding values in ShellCommandResult
     * 3. re-insert return value (if not null) after converting field
     * names from part1_part2 to part1Part2.
     * 4. ensure all field names are camelCase
     */
    public static ShellCommandResult filterJsonV1Result(String input) {
        final ShellCommandResult scr = new ShellCommandResult();
        final ObjectNode v1Result = CommandJsonUtils.readObjectValue(input);
        final JsonNode operation =
            v1Result.remove(FIELD_OPERATION);
        final JsonNode returnCode =
            v1Result.remove(FIELD_RETURN_CODE);
        final JsonNode description =
            v1Result.remove(FIELD_DESCRIPTION);
        final JsonNode returnValue =
            v1Result.remove(FIELD_RETURN_VALUE);
        v1Result.remove(FIELD_CLEANUP_JOB);
        if (operation == null || returnCode == null || description == null) {
            throw new IllegalStateException("Fail to convert JSON result, " +
                "one of following fields is null: " +
                FIELD_OPERATION + ", " +
                FIELD_RETURN_CODE + ", " +
                FIELD_DESCRIPTION);
        }
        scr.setOperation(operation.asText());
        scr.setReturnCode(returnCode.asInt());
        scr.setDescription(description.asText());
        if (returnValue != null) {
            if (!(returnValue instanceof ObjectNode)) {
                throw new IllegalStateException(
                    "Fail to convert return value, " +
                    "return value is not instance of ObjectNode");
            }
            final JsonObject gsonReturnValue =
                returnValue.getElement().getAsJsonObject();
            JsonObject convertedNode = convertFields(gsonReturnValue);
            /* merge converted fields into result */
            v1Result.merge(JsonUtils.createObjectNode(convertedNode));
        }
        scr.setReturnValue(v1Result);
        return scr;
    }

    private static JsonObject convertFields(JsonObject v1Node) {

        /* this holds the converted object */
        final JsonObject result = new JsonObject();

        for (Map.Entry<String, JsonElement> entry : v1Node.entrySet()) {
            final String key = entry.getKey();
            /* this call does the key translation to camelCase if needed */
            final String resultKey = translateV1Key(key);
            final JsonElement elem = entry.getValue();
            if (elem.isJsonObject()) {
                result.add(resultKey, convertFields(elem.getAsJsonObject()));
            } else if (elem.isJsonArray()) {
                result.add(resultKey, convertFields(elem.getAsJsonArray()));
            } else {
                result.add(resultKey, elem);
            }
        }
        return result;
    }

    private static JsonArray convertFields(JsonArray v1Node) {
        final JsonArray result = new JsonArray();
        for (JsonElement elem : v1Node) {
            if (elem.isJsonObject()) {
                result.add(convertFields(elem.getAsJsonObject()));
            } else if (elem.isJsonArray()) {
                result.add(convertFields(elem.getAsJsonArray()));
            } else {
                result.add(elem);
            }
        }
        return result;
    }


    /*
     * Convert the field_with_underscore to camelCase.
     */
    private static String translateV1Key(String key) {
        final int index = key.indexOf("_");
        if (index == -1) {
            return key;
        }
        if (key.startsWith("_") || key.endsWith("_")) {
            throw new IllegalStateException(
                "Unexpected result. Fail to convert key: " + key);
        }
        String firstComp = key.substring(0, index);
        String upperCase =
            key.substring(index + 1, index + 2).toUpperCase();
        String lastComp = upperCase + key.substring(index + 2);
        return firstComp + translateV1Key(lastComp);
    }
}
