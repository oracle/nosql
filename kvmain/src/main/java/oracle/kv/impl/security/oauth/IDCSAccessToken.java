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

import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

import oracle.nosql.common.json.JsonUtils;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;

/**
 * The IDCS OAuth2 Access Token are serialized as an Base64 encoded string of
 * JSON Web Token(JWT) with some customized fields, specifically it is using
 * standard format JSON Web Signature (JWS) structure described in RFC7519: <p>
 *
 * https://tools.ietf.org/html/rfc7519<p>
 *
 * This type of access token has three parts in encoded string delimited by
 * ".", the first part is the header that indicates which algorithm is used to
 * sign this token; the second part is the payload, also known as claim set,
 * which contains primary principals including scopes, audience, expiration
 * time and other claims. The last part contains signature of this token.
 * The IDCS OAuth authentication would only accept access token issued by IDCS
 * with this fixed format, this may extend with other format in the future.
 */
public class IDCSAccessToken {

    /* Algorithm field name in JWT */
    private static final String ALGORITHM_NAME = "alg";

    /* Audience claim name in JWT */
    private static final String AUDIENCE_CLAIM_NAME = "aud";

    /* Expiration claim name in JWT */
    private static final String EXPIRATION_CLAIM_NAME = "exp";

    /* Scope claim name in IDCS access token */
    private static final String SCOPE_CLAIM_NAME = "scope";

    /* Scope field delimiter in IDCS access token */
    private static final String SCOPE_CLAIM_DELIMITER = " ";

    /* Client id claim name in IDCS access token */
    static final String CLIENT_ID_CLAIM_NAME = "client_id";

    /* Algorithm used to sign this access token */
    private final String signingAlg;

    /* The content used to generate signature, header and payload */
    private final String signingContent;

    /* Signature of this access token */
    private final byte[] signature;

    /* Payload, all access token claims */
    private final ObjectNode claimSet;

    private IDCSAccessToken(String signingAlg,
                            String signningContent,
                            byte[] signature,
                            ObjectNode claimSet) {

        this.signingAlg = signingAlg;
        this.signingContent = signningContent;
        this.signature = signature;
        this.claimSet = claimSet;
    }

    /**
     * Parse given encoded access token string to IDCSAccessToken object.
     *
     * @return parsed access token instance
     * @throws IllegalArgumentException if any problems while parsing or
     * decoding the specified access token
     */
    public static IDCSAccessToken parse(String accessToken) {
        final String[] parts = IDCSOAuthUtils.splitIDCSAccessToken(accessToken);
        final ObjectNode header = IDCSOAuthUtils.parseJsonString(
            IDCSOAuthUtils.decodeBase64URL(parts[0]));
        final ObjectNode claimSet = IDCSOAuthUtils.parseJsonString(
            IDCSOAuthUtils.decodeBase64URL(parts[1]));
        if (header == null || claimSet == null) {
            throw new IllegalArgumentException(
                "Cannot find header and claim set from access token");
        }
        final String algorithm = JsonUtils.getAsText(header, ALGORITHM_NAME);

        return new IDCSAccessToken(
            algorithm,
            buildSigningContent(parts[0], parts[1]),
            decodeSignature(parts[2]),
            claimSet);
    }

    /**
     * Return algorithm used to sign this access token.
     */
    String getSigningAlg() {
        return signingAlg;
    }

    /**
     * Return signature of this access token.
     */
    byte[] getSignature() {
        return signature;
    }

    /**
     * Return signing content that includes header and claim set (payload)
     * without decoding.
     */
    String getSigningContent() {
        return signingContent;
    }

    /**
     * Return claim set, also know as JWT payload.
     */
    ObjectNode getClaimSet() {
        return claimSet;
    }

    /**
     * Return 'aud' field, audience defined in access token. May return null
     * if no audiences found in claim set.
     */
    List<String> getAudience() {
        final JsonNode audValue = claimSet.get(AUDIENCE_CLAIM_NAME);
        final List<String> audStrings = new ArrayList<>();
        if (audValue == null) {
            return audStrings;
        }

        if (audValue.isString()) {
            audStrings.add(audValue.asText());
        } else if (audValue.isArray()) {
            final Iterator<JsonNode> iter = audValue.asArray().iterator();
            while (iter.hasNext()) {
                audStrings.add(iter.next().asText());
            }
        }
        return audStrings;
    }

    /**
     * Return 'scope' field defined in access token. May return null if no
     * scope found in claim set.
     */
    String[] getScopes() {
        final String scopes = JsonUtils.getAsText(claimSet, SCOPE_CLAIM_NAME);
        if (scopes == null) {
            return null;
        }
        return scopes.trim().split(SCOPE_CLAIM_DELIMITER);
    }

    /**
     * Return expiration time 'exp' in milliseconds. May return 0 if no
     * expiration time found in claim set.
     */
     long getExpirationTime() {
        final String expirationTime =
            JsonUtils.getAsText(claimSet, EXPIRATION_CLAIM_NAME);
        if (expirationTime != null) {
            /* Expiration time defined in IDCS access token are seconds */
            return Long.parseLong(expirationTime) * 1000;
        }
        return 0;
    }

    /**
     * Return 'client_id' field defined in access token. May return null if
     * no client id found in claim set.
     */
    String getClientId() {
        final String clientId =
            JsonUtils.getAsText(claimSet, CLIENT_ID_CLAIM_NAME);
        if (clientId == null) {
            return null;
        }
        return clientId.trim();
    }

    /**
     * Return 'client_name' field defined in access token. May return null if
     * no client name found in claim set.
     */
    String getClientName() {
        final String clientName =
            JsonUtils.getAsText(claimSet, IDCSOAuthUtils.CLIENT_CLAIM_NAME);
        if (clientName == null) {
            return null;
        }
        return clientName.trim();
    }

    /**
     * Build signing content including header and payload.
     */
    private static String buildSigningContent(String header, String payload) {
        final StringBuilder sb = new StringBuilder();
        sb.append(header).append(".").append(payload);
        return sb.toString();
    }

    /**
     * Base64 URL decoder to decode the specified string.
     * @throws IllegalArgument if signature is not in valid Base64 scheme.
     */
    private static byte[] decodeSignature(String sig) {
        if (sig == null) {
            return null;
        }
        return Base64.getUrlDecoder().decode(sig);
    }
}
