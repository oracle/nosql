/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.junit.Assert.assertEquals;
import static oracle.nosql.common.json.JsonUtils.createObjectNode;

import oracle.kv.TestBase;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import org.junit.Test;

/** Test Admin.computeTlsCredentialsSummary */
public class AdminTlsCredentialsTest extends TestBase {

    private static final String time1 = "2021-01-01 11:11:11.111 UTC";
    private static final String time2 = "2022-02-02 22:22:22.222 UTC";
    private static final String time3 = "2023-03-03 33:33:33.333 UTC";
    private static final String time4 = "2024-04-04 44:44.44.444 UTC";
    private static final String time5 = "2025-05-05 55:55.55.555 UTC";
    private static final String time6 = "2026-06-06 66:66:66.666 UTC";

    @Test
    public void testComputeTlsCredentialsSummary() {

        final ObjectNode ks = createObjectNode()
            .put("file", "ks")
            .put("modTime", time1)
            .put("hash", "1111");
        final ObjectNode ts = createObjectNode()
            .put("file", "ts")
            .put("modTime", time2)
            .put("hash", "2222");
        final ObjectNode ks2 = createObjectNode()
            .put("file", "ks")
            .put("modTime", time3)
            .put("hash", "3333");
        final ObjectNode ts2 = createObjectNode()
            .put("file", "ts")
            .put("modTime", time4)
            .put("hash", "4444");

        /* One SN, no updates */
        ObjectNode snsMap = createObjectNode();
        snsMap.putObject("sn1")
            .put("installedCredentials",
                 JsonUtils.createObjectNode()
                 .put("keystore", ks)
                 .put("truststore", ts))
            .put("pendingUpdates",
                 JsonUtils.createObjectNode());
        checkSummary(JsonUtils.createObjectNode()
                     .put("installedCredentialsStatus", "consistent")
                     .put("pendingUpdatesStatus", "none")
                     .put("sns", snsMap),
                     snsMap);

        /* Updates match installed files */
        snsMap.getObject("sn1")
            .getObject("pendingUpdates")
            .set("keystore", ks)
            .set("truststore", ts);
        checkSummary(JsonUtils.createObjectNode()
                     .put("installedCredentialsStatus", "consistent")
                     .put("pendingUpdatesStatus", "consistent")
                     .put("sns", snsMap),
                     snsMap);

        /* Keystore update */
        snsMap.getObject("sn1")
            .getObject("pendingUpdates")
            .set("keystore", ks2);
        checkSummary(JsonUtils.createObjectNode()
                     .put("installedCredentialsStatus", "consistent")
                     .put("pendingUpdatesStatus", "consistent")
                     .put("sns", snsMap),
                     snsMap);

        /* Truststore update */
        snsMap.getObject("sn1")
            .getObject("pendingUpdates")
            .set("truststore", ts2);
        checkSummary(JsonUtils.createObjectNode()
                     .put("installedCredentialsStatus", "consistent")
                     .put("pendingUpdatesStatus", "consistent")
                     .put("sns", snsMap),
                     snsMap);

        /* Two updates */
        snsMap.getObject("sn1")
            .getObject("pendingUpdates")
            .set("keystore", ks2);
        checkSummary(JsonUtils.createObjectNode()
                     .put("installedCredentialsStatus", "consistent")
                     .put("pendingUpdatesStatus", "consistent")
                     .put("sns", snsMap),
                     snsMap);

        /* Two SNs, same keystore and truststore hashes */
        snsMap.getObject("sn1")
            .put("pendingUpdates",
                 JsonUtils.createObjectNode());
        snsMap.putObject("sn2")
            .put("installedCredentials",
                 JsonUtils.createObjectNode()
                 .put("keystore", ks)
                 .put("truststore", ts))
            .put("pendingUpdates",
                 JsonUtils.createObjectNode());
        checkSummary(JsonUtils.createObjectNode()
                     .put("installedCredentialsStatus", "consistent")
                     .put("pendingUpdatesStatus", "none")
                     .put("sns", snsMap),
                     snsMap);

        /* Different keystore modTime, still consistent */
        final ObjectNode ksNewMod = createObjectNode()
            .put("file", "ks")
            .put("modTime", time5)
            .put("hash", "1111");
        snsMap.getObject("sn2")
            .getObject("installedCredentials")
            .set("keystore", ksNewMod);
        checkSummary(JsonUtils.createObjectNode()
                     .put("installedCredentialsStatus", "consistent")
                     .put("pendingUpdatesStatus", "none")
                     .put("sns", snsMap),
                     snsMap);

        /* Different keystore hash, mixed */
        final ObjectNode ksNewHash = createObjectNode()
            .put("file", "ks")
            .put("modTime", time5)
            .put("hash", "5555");
        snsMap.putObject("sn2")
            .put("installedCredentials",
                 JsonUtils.createObjectNode()
                 .put("keystore", ksNewHash)
                 .put("truststore", ts))
            .put("pendingUpdates",
                 JsonUtils.createObjectNode());
        checkSummary(JsonUtils.createObjectNode()
                     .put("installedCredentialsStatus", "mixed")
                     .put("pendingUpdatesStatus", "none")
                     .put("sns", snsMap),
                     snsMap);

        /* Different truststore hash, mixed */
        final ObjectNode tsNewHash = createObjectNode()
            .put("file", "ts")
            .put("modTime", time6)
            .put("hash", "6666");
        snsMap.getObject("sn2")
            .getObject("installedCredentials")
            .set("keystore", ks);
        snsMap.getObject("sn2")
            .getObject("installedCredentials")
            .put("truststore", tsNewHash);
        checkSummary(JsonUtils.createObjectNode()
                     .put("installedCredentialsStatus", "mixed")
                     .put("pendingUpdatesStatus", "none")
                     .put("sns", snsMap),
                     snsMap);

        /* Mixed with exception */
        final ObjectNode exception = createObjectNode()
            .put("exception", "Couldn't contact SN");
        snsMap.put("sn3", exception);
        checkSummary(createObjectNode()
                     .put("installedCredentialsStatus", "mixed")
                     .put("pendingUpdatesStatus", "none")
                     .put("sns", snsMap),
                     snsMap);

        /* Consistent with exception */
        snsMap.getObject("sn2")
            .getObject("installedCredentials")
            .set("truststore", ts);
        checkSummary(createObjectNode()
                     .put("installedCredentialsStatus", "maybe-consistent")
                     .put("pendingUpdatesStatus", "none")
                     .put("sns", snsMap),
                     snsMap);

        /* Keystore update */
        snsMap.getObject("sn1")
            .getObject("pendingUpdates")
            .set("keystore", ks2);
        checkSummary(createObjectNode()
                     .put("installedCredentialsStatus", "maybe-consistent")
                     .put("pendingUpdatesStatus", "mixed")
                     .put("sns", snsMap),
                     snsMap);

        /* Truststore update */
        snsMap.getObject("sn1")
            .getObject("pendingUpdates")
            .set("keystore", ks)
            .set("truststore", ts2);
        checkSummary(createObjectNode()
                     .put("installedCredentialsStatus", "maybe-consistent")
                     .put("pendingUpdatesStatus", "mixed")
                     .put("sns", snsMap),
                     snsMap);

        /* Keystore and truststore updates on same SN */
        snsMap.getObject("sn1")
            .getObject("pendingUpdates")
            .set("keystore", ks2);
        checkSummary(createObjectNode()
                     .put("installedCredentialsStatus", "maybe-consistent")
                     .put("pendingUpdatesStatus", "mixed")
                     .put("sns", snsMap),
                     snsMap);

        /* Truststore update on different SN */
        snsMap.getObject("sn1")
            .getObject("pendingUpdates");
        snsMap.getObject("sn2")
            .getObject("pendingUpdates")
            .set("truststore", ts2);
        checkSummary(createObjectNode()
                     .put("installedCredentialsStatus", "maybe-consistent")
                     .put("pendingUpdatesStatus", "mixed")
                     .put("sns", snsMap),
                     snsMap);

        /* All updates, with exception */
        snsMap.getObject("sn1")
            .getObject("pendingUpdates")
            .set("keystore", ks2)
            .set("truststore", ts2);
        snsMap.getObject("sn2")
            .getObject("pendingUpdates")
            .set("keystore", ks2)
            .set("truststore", ts2);
        checkSummary(createObjectNode()
                     .put("installedCredentialsStatus", "maybe-consistent")
                     .put("pendingUpdatesStatus", "maybe-consistent")
                     .put("sns", snsMap),
                     snsMap);

        /* All updates, no exception */
        snsMap.getObject("sn3")
            .remove("exception");
        snsMap.putObject("sn3")
            .put("installedCredentials",
                 JsonUtils.createObjectNode()
                 .put("keystore", ks)
                 .put("truststore", ts))
            .put("pendingUpdates",
                 JsonUtils.createObjectNode()
                 .put("keystore", ks2)
                 .put("truststore", ts2));
        checkSummary(createObjectNode()
                     .put("installedCredentialsStatus", "consistent")
                     .put("pendingUpdatesStatus", "consistent")
                     .put("sns", snsMap),
                     snsMap);

        /* Mixed with full updates */
        snsMap.getObject("sn1")
            .getObject("installedCredentials")
            .set("keystore", ks2);
        checkSummary(createObjectNode()
                     .put("installedCredentialsStatus", "mixed")
                     .put("pendingUpdatesStatus", "consistent")
                     .put("sns", snsMap),
                     snsMap);
    }

    private static void checkSummary(ObjectNode expected, ObjectNode snsMap) {
        final ObjectNode summary = Admin.computeTlsCredentialsSummary(snsMap);
        assertEquals(summary.toPrettyString(),
                     expected.toPrettyString(),
                     summary.toPrettyString());
    }
}
