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

package oracle.kv.impl.security.oauth;

import java.nio.charset.Charset;
import java.util.Base64;

import oracle.nosql.common.json.JsonUtils;

import oracle.nosql.common.json.ObjectNode;

/**
 * Utility methods and constants for IDCS OAuth.
 */
public class IDCSOAuthUtils {
    /* Client claim name in IDCS access token */
    static String CLIENT_CLAIM_NAME = "client_name";

    /* User claim name in IDCS access token*/
    static String USER_CLAIM_NAME = "user_displayname";

    /* The algorithm supported by IDCS for signature verification */
    public static String IDCS_SUPPORTED_ALGORITHM = "RS256";

    /* The internal name of algorithm RS256v IDCS used */
    public static String RS256_INTERNAL_NAME = "SHA256withRSA";

    /**
     * If specified algorithm is supported by IDCS.
     */
    public static boolean idcsSupportedAlgorithm(String algorithm) {
        if (algorithm.equals(IDCS_SUPPORTED_ALGORITHM)) {
            return true;
        }
        return false;
    }

    public static String getIdcsSupportedAlgorithm() {
        return IDCS_SUPPORTED_ALGORITHM;
    }

    /**
     * Return internal algorithm name for verifying signature.
     */
    public static String getInternalAlgName(String algName) {
        if (algName.equals("RS256")) {
            return RS256_INTERNAL_NAME;
        }
        return null;
    }

    /**
     * Parse given access token and return client_name field. The input
     * argument accessToken could be null.
     * @throws IllegalArgumentException if access token having parsing problems
     */
    public static String getClientName(String accessToken)
        throws IllegalArgumentException {

        if (accessToken == null) {
            return null;
        }
        final ObjectNode json = getClaimSet(accessToken);
        return json.get(CLIENT_CLAIM_NAME).asText();
    }

    /**
     * Parse given access token and return user_displayname field. The input
     * argument accessToken could be null and client-only access token would
     * return null for user display name.
     * @throws IllegalArgumentException if access token having parsing problems
     */
    public static String getUserDisplayName(String accessToken) {

        if (accessToken == null) {
            return null;
        }
        final ObjectNode json = getClaimSet(accessToken);
        return json.get(USER_CLAIM_NAME).asText();
    }

    /**
     * Parse a JSON string to JSON Node.
     * @return parsed JSON node object, or null if parsing failed.
     */
    public static ObjectNode parseJsonString(String jsonString) {
        return JsonUtils.parseJsonObject(jsonString);
    }

    /**
     * Split the access token encoded string with delimiter ".". IDCS access
     * token is a JWT in JWS structure as "header.payload.signature"
     *
     * @return String[] of split string, or null if format is incorrect.
     * @throws IllegalArgumentException if given string is in invalid access
     * token format.
     */
    static String[] splitIDCSAccessToken(String s) {
        if (s == null) {
            throw new IllegalArgumentException(
                "Specified access token is null");
        }
        final int dot1 = s.indexOf(".");
        final int dot2 = s.indexOf(".", dot1 + 1);
        final int dot3 = s.indexOf(".", dot2 + 1);

        if (dot1 == -1 || dot2 == -1 || dot3 != -1) {
            throw new IllegalArgumentException(
                "Given string is not in the valid access token format");
        }

        final String[] parts = new String[3];
        parts[0] = s.substring(0, dot1);
        parts[1] = s.substring(dot1 + 1, dot2);
        parts[2] = s.substring(dot2 + 1);
        return parts;
    }

    /**
     * Parse and extract claim set (payload) from access token.
     * @return parsed claim set JSON Node, or null if parsing failed.
     * @throws IllegalArgumentException if there is any access token parsing
     * and decoding problems.
     */
    static ObjectNode getClaimSet(String accessToken) {
        final String[] splitString = splitIDCSAccessToken(accessToken);
        if (splitString == null || splitString[1] == null) {
            return null;
        }
        final String decodedClaimSet = decodeBase64URL(splitString[1]);
        return parseJsonString(decodedClaimSet);
    }

    /**
     * Use Base64 URL decoder to decode specified string.
     * @return decoded string
     * @throws IllegalArgument specified string is invalid or not in valid
     * Base64 scheme.
     */
    static String decodeBase64URL(String text) {
        if (text == null) {
            throw new IllegalArgumentException("Specified string is null");
        }
        final byte[] decoded = Base64.getUrlDecoder().decode(text);
        return new String(decoded, Charset.forName("UTF-8"));
    }
}
