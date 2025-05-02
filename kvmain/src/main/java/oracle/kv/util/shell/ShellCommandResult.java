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

package oracle.kv.util.shell;

import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandResult;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.util.ErrorMessage;

/**
 * <p>
 * This is the POJO class for all the shell command execution result. In admin
 * CLI, the output of a command will be turn into this class before
 * display in the admin console. ShellCommandResult will then handle the
 * conversion of the command result to meaningful JSON string. The fields in
 * the JSON are mapping to fields in ShellCommandResult. ShellCommandResult
 * consists of following fields:
 * <p>
 * operation - The name of the executing operation, in most cases it will be
 * the same as command name. If there is internal level operation failure, the
 * operation field may be the name to indicate the internal operation.
 * <p>
 * returnCode - The number to indicate the command result return to caller.
 * For admin CLI result, 5000 return code indicate the successful execution of
 * operation, return code greater than 5000 indicate a operation failure.
 * <p>
 * description - This field store the string description of command result.
 * The description field store information that human readable. It is used
 * when some command results require manual operation on the JSON result. If
 * the command is failed, the message information will be stored in description
 * field. It is not recommended to use description field to do any automation,
 * programmer should instead use returnCode and returnValue fields for admin
 * CLI automation purpose.
 * <p>
 * returnValue - This field stores a JSON object. The JSON object field has
 * no fixed schema. Most of useful information retrieved from server should be
 * stored in returnValue field. The mapping class of returnValue field is
 * ObjectNode
 * <p>
 * The following will show an example that run an admin CLI command with JSON
 * flag, the output will map to fields in ShellCommandResult:
 *
 * <pre>
 * {@literal kv->} show users -json
 * {
 *  "operation" : "show user",
 *  "returnCode" : 5000,
 *  "description" : "Operation ends successfully",
 *  "returnValue" : {
 *    "users" : [ {
 *      "user" : "id=u1 name=root"
 *    } ]
 *  }
 *}
 *
 */
public class ShellCommandResult {

    public static final String SUCCESS_MESSAGE =
        "Operation ends successfully";

    public static final String UNSUPPORTED_MESSAGE =
        "JSON output does not suppport.";

    private String operation;

    private int returnCode;

    private String description;

    private ObjectNode returnValue;

    public ShellCommandResult() {}

    public ShellCommandResult(String operation,
                              int returnCode,
                              String description,
                              ObjectNode returnValue) {
        this.operation = operation;
        this.returnCode = returnCode;
        this.description = description;
        this.returnValue = returnValue;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public ObjectNode getReturnValue() {
        return returnValue;
    }

    public void setReturnValue(ObjectNode returnValue) {
        this.returnValue = returnValue;
    }

    /**
     * Convert this object to JSON string.
     */
    public String convertToJson() {
        return JsonUtils.writeAsJson(this, true);
    }

    /**
     * Return default instance of this class. By default, the return code is
     * 5000 indicate a successful operation.
     */
    public static ShellCommandResult getDefault(String operationName) {
        final ShellCommandResult scr = new ShellCommandResult();
        scr.setOperation(operationName);
        scr.setReturnCode(ErrorMessage.NOSQL_5000.getValue());
        scr.setDescription(SUCCESS_MESSAGE);
        return scr;
    }

    /*
     * Convert the previous CommandResult plus name to map the JSON output of
     * ShellCommandResult. This method is mostly used in exception handling
     * part of shell command execution. Previously, the JSON output for shell
     * exception handling has been implemented in along the CommandResult,
     * all the related exeception contains a CommandResult. This method is to
     * convert the information in CommandResult to match the new JSON output
     * format.
     */
    public static String toJsonReport(String command,
                                      CommandResult cmdResult) {
        final ShellCommandResult scr = new ShellCommandResult();
        scr.setOperation(command);
        scr.setDescription(cmdResult.getDescription());
        scr.setReturnCode(cmdResult.getErrorCode());
        final String returnValue = cmdResult.getReturnValue();
        if (returnValue != null) {
            scr.setReturnValue(
                CommandJsonUtils.readObjectValue(returnValue));
        }
        return scr.convertToJson();
    }
}
