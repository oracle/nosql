/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util.fault;

import java.util.Arrays;

/**
 * The class conveys error information used to build HTTP response.
 *
 * A problem detail response RFC7807 defined has following members:
 * <ol>
 * <li>"type" - A URI reference that identifies the problem type.</li>
 * <li>"title" - A short, human-readable summary of the problem type.</li>
 * <li>"status" - The HTTP status code</li>
 * <li>"details" - A human-readable explanation specific to this occurrence of
 * the problem.</li>
 * <li>"instance" - A URI reference that identifies the specific occurrence of
 * the problem</li>
 * </ol>
 *
 * Some of members are optional, the payload using contains "type", "title",
 * "status" and "details." For debugging purposes, the payload also includes
 * optional "stack-trace" as customized extension.
 * <p>
 *
 * The value of "type" is currently "about:blank", because no documentation URI
 * is available, cannot remove since it's required member. The "instance" is
 * also a documentation URI of problem but more specific, remove that for now.
 * "details", unlike "title" contains the specific of the exception, e.g.
 * if an argument was bad, the name of the argument. It could typically be the
 * exception message which contained the information. "stack-trace" is an
 * extended field, which contains the stack trace of the exception.<p>
 *
 * To make a JSON format response:<p>
 * Serialize a Java object into a Json string:<p>
 *   Foo foo;<p>
 *   String jsonPayload = JsonUtils.toJson(foo);<p>
 *
 * Deserialize a Json string into this object:<p>
 *   Foo foo = JsonUtils.fromJson(<jsonstring>, Foo.class);
 */
public class ErrorResponse {

    private static final String[] EMPTY_STACK_TRACE = new String[0];
    private String type;
    private String title;
    private int status;
    private String detail;
    private String[] stackTrace;

    /* Needed for serialization */
    public ErrorResponse() {
    }

    /**
     * Create an error response based on error code and exception.
     */
    public static ErrorResponse build(ErrorCode error,
                                      Exception exception,
                                      boolean includeDebugInfo) {
        if (error == null || exception == null) {
            throw new IllegalArgumentException(
                "ErrorCode or exception specified cannot be null");
        }
        String[] stackTrace = null;
        if (includeDebugInfo) {
            stackTrace = getStackTrace(exception);
        }

        return new ErrorResponse(
            error.getType(),
            error.getHttpReasonPhrase(),
            error.getHttpStatusCode(),
            exception.getMessage(),
            stackTrace);
    }

    /**
     * Builds a simple error response using an explicit message and no stack.
     */
    public static ErrorResponse build(ErrorCode error, String msg) {
        return new ErrorResponse(
            error.getType(),
            error.getHttpReasonPhrase(),
            error.getHttpStatusCode(),
            msg,
            null);
    }

    private ErrorResponse(String type,
                          String title,
                          int status,
                          String detail,
                          String[] stackTrace) {
        this.type = type;
        this.title = title;
        this.status = status;
        this.detail = detail;
        this.stackTrace = stackTrace;
    }

    /**
     * Returns a URI reference that identifies the problem type.
     */
    public String getType() {
        return type;
    }

    /**
     * Returns a short, human-readable summary of the problem type, typically
     * the HTTP reason phrase in the {@link ErrorCode}.
     */
    public String getTitle() {
        return title;
    }

    /**
     * Returns HTTP status code.
     */
    public int getStatus() {
        return status;
    }

    /**
     * Returns a human-readable explanation specific to this occurrence of
     * the problem, typically the message of exception.
     */
    public String getDetail() {
        return detail;
    }

    /**
     * Returns stack trace in String arrays of exception.
     */
    public String[] getStackTrace() {
        return stackTrace;
    }

    /**
     * ErrorResponse object contains stack trace of exception only for
     * debugging purpose. Calling this method would strip stack trace.
     */
    public ErrorResponse stripDebuggingInfo() {
        this.stackTrace = null;
        return this;
    }

    /**
     * Convert stack traces of exception to a String array.
     */
    public static String[] getStackTrace(Exception exception) {
        final StackTraceElement[] stackTraces = exception.getStackTrace();
        if (stackTraces == null || stackTraces.length == 0) {
            return EMPTY_STACK_TRACE;
        }
        return Arrays.stream(stackTraces).map(StackTraceElement::toString).
                        toArray(String[]::new);
    }

    @Override
    public String toString() {
        return String.format("ErrorResponse " +
            "[type=%s, title=%s, status=%s, detail=%s, stacktrace=%s]",
            type, title, status, detail, Arrays.toString(stackTrace));
    }
}
