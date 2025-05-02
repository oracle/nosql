/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.RequestLimitConfig;

/**
 * Utilities to parse and print RequestLimitConfig values for command line
 * tests. <p>
 *
 * Supported values:
 * <ul>
 * <li> {@code maxActiveRequests=<integer>,requestThresholdPercent=<percent>,
 *      nodeLimitPercent=<percent>} - for an instance of {@link
 *      oracle.kv.RequestLimitConfig} with the specified
 *      {@code maxActiveRequests}, {@code requestThresholdPercent} and
 *      {@code nodeLimitPercent} values measured in percentage integers.
 * </ul> <p>
 *
 * Parsing is case insensitive. <p>
 */
public class RequestLimitConfigArgument {

    /**
     * Regular expression pattern for parsing the maxActiveRequest,
     * requestThresholdPercent and nodeLimitPercent.
     */
    private static final Pattern REQUEST_LIMIT_PATTERN =
        Pattern.compile("maxActiveRequests=([0-9]+)," +
                        "requestThresholdPercent=([0-9]+)," +
                        "nodeLimitPercent=([0-9]+)",
                        Pattern.CASE_INSENSITIVE);

    /** This class should not be instantiated. */
    private RequestLimitConfigArgument() { }

    /**
     * Parse a RequestLimitConfig argument value
     * @param string the input RequestLimitConfig string
     * @return the RequestLimitConfig value
     * @throws IllegalArgumentException if the input is not recognized
     */
    public static RequestLimitConfig parseRequestLimitConfig(String string) {
        if (string == null) {
            throw new IllegalArgumentException("The argument must not be null");
        }
        final Matcher matcher = REQUEST_LIMIT_PATTERN.matcher(string);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                "Unrecognized RequestLimitConfig: " + string);
        }
        final int maxActiveRequests = Integer.parseInt(matcher.group(1));
        final int requestThresholdPercent = Integer.parseInt(matcher.group(2));
        final int nodeLimitPercent = Integer.parseInt(matcher.group(3));
        return new RequestLimitConfig(maxActiveRequests,
                                      requestThresholdPercent,
                                      nodeLimitPercent);
    }
    /**
     * Returns a string for the RequestLimitConfig value in a format understood
     * by {@link #parseRequestLimitConfig}.
     *
     * @param requestLimit the RequestLimitConfig value
     * @return the string
     * @throws IllegalArgumentException if the argument is null
     */
    public static String toString(RequestLimitConfig requestLimit) {
        if (requestLimit == null) {
            throw new IllegalArgumentException("The argument must not be null");
        }
        return "maxActiveRequests=" + requestLimit.getMaxActiveRequests() +
            ",requestThresholdPercent=" +
            requestLimit.getRequestThresholdPercent() + ",nodeLimitPercent=" +
            requestLimit.getNodeLimitPercent();
    }
}
