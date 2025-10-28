/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 */
package oracle.nosql.proxy.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.model.Configuration;
import com.oracle.bmc.nosql.model.HostedConfiguration;
import com.oracle.bmc.nosql.model.KmsKey;
import com.oracle.bmc.nosql.model.MultiTenancyConfiguration;
import com.oracle.bmc.nosql.model.UpdateHostedConfigurationDetails;
import com.oracle.bmc.nosql.model.WorkRequest.Status;
import com.oracle.bmc.nosql.requests.GetConfigurationRequest;
import com.oracle.bmc.nosql.requests.UnassignKmsKeyRequest;
import com.oracle.bmc.nosql.requests.UpdateConfigurationRequest;
import com.oracle.bmc.nosql.responses.GetConfigurationResponse;
import com.oracle.bmc.nosql.responses.UnassignKmsKeyResponse;
import com.oracle.bmc.nosql.responses.UpdateConfigurationResponse;

/**
 * Test configuration APIs:
 *  - get-configuration
 *  - update-configuration and
 *  - unassign-kms-key
 */
public class ConfigurationTest extends RestAPITestBase {

    private static final String testKeyId =
        "ocid1.key.oc1.ca-montreal-1.gbtt5qeuaaa2c.ab4xkljr2eqgqifrmpxqddjdtic2lfj4owmln4dyfu4hhifw677hanrk5pna";
    private static final String testUpdateKeyId =
        "ocid1.key.oc1.ca-montreal-1.gbtt5qeuaaa2c.ab4xkljrnljt7v4bebml7mqhonun4gwf2wcs5hjiejx6vni65f2vyht7wl6a";
    private static final String testVaultId =
        "ocid1.vault.oc1.ca-montreal-1.gbtt5qeuaaa2c.ab4xkljrikzzvhy2uvlafsg3qy5hwmngs74sqbexnwsgxhj7qii5llb7f7vq";

    private static final String testTenantId = TENANT_NOSQL_DEV;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        cloudRunning = Boolean.getBoolean(USEMC_PROP);
        Assume.assumeTrue(
            "Skipping ConfigurationTest if not run against minicloud",
             cloudRunning);
        RestAPITestBase.staticSetUp();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        removeCmekAndDedicatedTenancy(testTenantId);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        removeCmekAndDedicatedTenancy(testTenantId);
        super.tearDown();
    }

    private void removeCmekAndDedicatedTenancy(String tenantId) {
        Configuration config = getConfiguration(tenantId);
        if (config instanceof HostedConfiguration) {
            if (((HostedConfiguration) config).getKmsKey().getId() != null) {
                unassignKmsKey();
            }
            setDedicatedTenantId(null);
        }
    }

    @Test
    public void basicTest() {
        /*
         * multi-tenancies pod
         *
         * Get configuration should return MultiTenancyConfiguration
         */
        Configuration config = getConfiguration(testTenantId);
        assertTrue(config instanceof MultiTenancyConfiguration);

        /*
         * Assign the pod to testTenantId
         */
        setDedicatedTenantId(testTenantId);
        assertKmsKey(null /* keyId */);

        /*
         * Assign key
         */
        updateConfiguration(testKeyId, null);
        assertKmsKey(testKeyId);

        /*
         * Rotate key
         */
        updateConfiguration(testUpdateKeyId, testVaultId);
        assertKmsKey(testUpdateKeyId);

        /*
         * Remove key
         */
        unassignKmsKey();
        assertKmsKey(null /* keyId */);
    }

    @Test
    public void testDryRun() {

        String workRequestId;

        /*
         * Assign the pod to testTenantId.
         */
        setDedicatedTenantId(testTenantId);
        assertKmsKey(null /* keyId */);

        /*
         * Dry run: set key
         */
        updateConfiguration(testTenantId, testKeyId, null /* vaultId */,
                            true /* dryRun */, true /* wait */);
        assertKmsKey(null /* keyId */);

        /*
         * Dry run: assign key
         *
         * IllegalArgument: Invalid key
         */
        workRequestId = updateConfiguration(testTenantId, "invalidKey",
                                            null, /* vaultId */
                                            true  /* dryRun */,
                                            false /* wait */);
        waitForStatus(workRequestId, "IllegalArgument", Status.Failed);

        /*
         * Dry run: assign key
         *
         * IllegalArgument: Invalid key
         */
        workRequestId = updateConfiguration(testTenantId, testTenantId,
                                            null, /* vaultId */
                                            true  /* dryRun */,
                                            false /* wait */);
        waitForStatus(workRequestId, "IllegalArgument", Status.Failed);

        /*
         * Dry run: assign key
         *
         * IllegalArgument: The Kms Key doesn't belong to the Vault 'invalidVaultId'
         */
        workRequestId = updateConfiguration(testTenantId, testKeyId,
                                            "invalidVaultId",
                                            true  /* dryRun */,
                                            false /* wait */);
        waitForStatus(workRequestId, "IllegalArgument", Status.Failed);

        /*
         * Dry run: remove key
         *
         * IllegalArgument: No kms key is assigned to the service
         */
        workRequestId = unassignKmsKey(testTenantId, true /* dryRun */,
                                       false /* wait */);
        waitForStatus(workRequestId, "IllegalArgument", Status.Failed);

        /*
         * Set the kms key
         */
        updateConfiguration(testKeyId, null);
        assertKmsKey(testKeyId /* keyId */);

        /*
         * Dry run: remove key
         */
        unassignKmsKey(testTenantId, true /* dryRun */, true /* wait */);
        assertKmsKey(testKeyId /* keyId */);


        /* Invalid parameters */
        try {
            updateConfiguration(null, testKeyId, null, false);
            fail("updateConfiguration should fail with NPE");
        } catch (NullPointerException ex) {
        }

        try {
            updateConfiguration(testTenantId, "", null, false);
            fail("updateConfiguration should fail with 400-InvalidParameter");
        } catch (BmcException ex) {
            assertEquals(400, ex.getStatusCode());
        }

        try {
            updateConfiguration(testTenantId, null, null, false);
            fail("updateConfiguration should fail with 400-InvalidParameter");
        } catch (BmcException ex) {
            assertEquals(400, ex.getStatusCode());
        }
    }

    /*
     * Get configuration
     */
    private Configuration getConfiguration(String tenantId) {
        GetConfigurationRequest req = GetConfigurationRequest.builder()
                .compartmentId(tenantId)
                .build();
        GetConfigurationResponse res = client.getConfiguration(req);
        Configuration config = res.getConfiguration();
        /* Sleep 250ms to avoid throttling. */
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
        }
        return config;
    }

    /*
     * Update kms key
     */
    private String updateConfiguration(String keyId, String vaultId) {
        return updateConfiguration(testTenantId, keyId, vaultId, false, true);
    }

    private String updateConfiguration(String tenantId,
                                       String keyId,
                                       String vaultId,
                                       boolean dryRun,
                                       boolean wait) {

        UpdateConfigurationResponse res =
            updateConfiguration(tenantId, keyId, vaultId, dryRun);
        assertEquals(202, res.get__httpStatusCode__());

        String workRequestId = res.getOpcWorkRequestId();
        assertNotNull(workRequestId);

        if (!wait) {
            return workRequestId;
        }
        waitForStatus(workRequestId, Status.Succeeded);
        return workRequestId;
    }

    private UpdateConfigurationResponse updateConfiguration(String tenantId,
                                                            String keyId,
                                                            String vaultId,
                                                            boolean dryRun) {

        KmsKey.Builder key = KmsKey.builder().id(keyId);
        if (vaultId != null) {
            key.kmsVaultId(vaultId);
        }

        UpdateHostedConfigurationDetails details =
            UpdateHostedConfigurationDetails.builder()
                .kmsKey(key.build())
                .build();

        UpdateConfigurationRequest req =
             UpdateConfigurationRequest.builder()
                 .compartmentId(tenantId)
                 .updateConfigurationDetails(details)
                 .isOpcDryRun(dryRun)
                 .build();

        return client.updateConfiguration(req);
    }

    /*
     * Remove kms key
     */
    private String unassignKmsKey() {
        return unassignKmsKey(testTenantId, false /* dryRun */, true /* wait */);
    }

    private String unassignKmsKey(String tenantId,
                                  boolean dryRun,
                                  boolean wait) {
        UnassignKmsKeyRequest req = UnassignKmsKeyRequest
                .builder()
                .compartmentId(tenantId)
                .isOpcDryRun(dryRun)
                .build();
        UnassignKmsKeyResponse res = client.unassignKmsKey(req);
        assertEquals(202, res.get__httpStatusCode__());

        String workRequestId = res.getOpcWorkRequestId();
        assertNotNull(workRequestId);

        if (!wait) {
            return workRequestId;
        }
        waitForStatus(workRequestId, Status.Succeeded);
        return workRequestId;
    }

    /* Validate the key Id in the configuration */
    private void assertKmsKey(String keyId) {
        Configuration config = getConfiguration(testTenantId);
        assertTrue(config instanceof HostedConfiguration);

        HostedConfiguration hconfig = (HostedConfiguration)config;
        KmsKey key = hconfig.getKmsKey();
        assertNotNull(key);
        assertEquals(keyId, key.getId());
        if (keyId != null) {
            assertEquals(testVaultId, key.getKmsVaultId());
            assertNotNull(key.getTimeCreated());
            assertNotNull(key.getTimeUpdated());
        }
        assertEquals(KmsKey.KmsKeyState.Active, key.getKmsKeyState());
    }
}
