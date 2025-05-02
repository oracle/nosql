/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.kerberos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Set;

import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.security.util.SNKrbInstance;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StorageNodeUtils.KerberosOpts;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.Test;

/**
 * Test starting store instance with Kerberos authentication enabled.
 */
public class KerberosStoreTest extends SecureTestBase {

    private static final String ADMIN_NAME = "admin";
    private static final String ADMIN_PW = "NoSql00__7654321";

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        users.add(new SecureUser(ADMIN_NAME, ADMIN_PW, true /* admin */));
        numSNs = 3;
        repFactor = 3;
    }

    @Override
    public void tearDown()
        throws Exception {

        shutdown();
        super.tearDown();
    }

    @Test
    public void testBasic() throws Exception {

        /* Start a store with Kerberos enabled */
        userExternalAuth = "KERBEROS";
        krbOpts = new KerberosOpts[numSNs];
        for (int i = 0; i < numSNs; i++) {
            krbOpts[i] = new KerberosOpts().setInstanceName("instance" + i);
        }
        startup();

        /* Check if global parameter authMethods of started store is Kerberos */
        assertEquals("KERBEROS", getAuthMethods());

        /* Check if service principal instance name are available */
        final Topology topo = getTopology();
        final Set<RepNodeId> rnIds = topo.getRepNodeIds();

        for (RepNodeId rnid : rnIds) {
            final RepNodeAdminAPI rnai = createStore.getRepNodeAdmin(
                rnid, ADMIN_NAME, ADMIN_PW.toCharArray());
            final KerberosPrincipals princs = rnai.getKerberosPrincipals();
            final SNKrbInstance[] krbInstances = princs.getSNInstanceNames();
            assertEquals(krbInstances.length, numSNs);

            for (int i = 0; i < numSNs; i++) {
                assertEquals(krbInstances[i].getInstanceName(),
                    "instance" + (krbInstances[i].getStorageNodeId() - 1));
            }
        }

        changeAuthMethods("NONE");

        /* Check if global parameter authMethods has been changed */
        assertEquals("NONE", getAuthMethods());
    }

    @Test
    public void testKerberosWithoutInstanceName() throws Exception {

        /* Start a store with Kerberos enabled */
        userExternalAuth = "KERBEROS";
        krbOpts = new KerberosOpts[numSNs];
        for (int i = 0; i < numSNs; i++) {
            krbOpts[i] = new KerberosOpts().setInstanceName("");
        }
        startup();

        /* Check if global parameter authMethods of started store is Kerberos */
        assertEquals("KERBEROS", getAuthMethods());

        /* Check if service principal instance name are available */
        final Topology topo = getTopology();
        final Set<RepNodeId> rnIds = topo.getRepNodeIds();

        for (RepNodeId rnid : rnIds) {
            final RepNodeAdminAPI rnai = createStore.getRepNodeAdmin(
                rnid, ADMIN_NAME, ADMIN_PW.toCharArray());
            final KerberosPrincipals princs = rnai.getKerberosPrincipals();
            final SNKrbInstance[] krbInstances = princs.getSNInstanceNames();
            assertNull(krbInstances);
        }
    }

    @Test
    public void testChangeAuthMethods() throws Exception {

        userExternalAuth = "NONE";
        useThreads = true;
        krbOpts = new KerberosOpts[numSNs];
        for (int i = 0; i < numSNs; i++) {
            krbOpts[i] = new KerberosOpts().setInstanceName("instance" + i);
        }

        /* Start a store with authentication method is NONE */
        startup();

        /* Check if global parameter authMethods of started store is NONE */
        assertEquals("NONE", getAuthMethods());
        changeAuthMethods("NONE");
        assertEquals("NONE", getAuthMethods());

        /* Check if service principal instance name are available */
        final Topology topo = getTopology();
        final Set<RepNodeId> rnIds = topo.getRepNodeIds();

        for (RepNodeId rnid : rnIds) {
            final RepNodeAdminAPI rnai = createStore.getRepNodeAdmin(
                rnid, ADMIN_NAME, ADMIN_PW.toCharArray());
            final KerberosPrincipals princs = rnai.getKerberosPrincipals();
            final SNKrbInstance[] krbInstances = princs.getSNInstanceNames();
            assertNull(krbInstances);
        }

        /* Check if global parameter authMethods change succeed */
        changeAuthMethods("KERBEROS");
        assertEquals("KERBEROS", getAuthMethods());

        for (RepNodeId rnid : rnIds) {
            final RepNodeAdminAPI rnai = createStore.getRepNodeAdmin(
                rnid, ADMIN_NAME, ADMIN_PW.toCharArray());
            final KerberosPrincipals princs = rnai.getKerberosPrincipals();
            final SNKrbInstance[] krbInstances = princs.getSNInstanceNames();
            assertEquals(krbInstances.length, numSNs);

            for (int i = 0; i < numSNs; i++) {
                assertEquals(krbInstances[i].getInstanceName(),
                    "instance" + (krbInstances[i].getStorageNodeId() - 1));
            }
        }

        /* Change authentication methods multiple times */
        changeAuthMethods("NONE");
        assertEquals("NONE", getAuthMethods());
        changeAuthMethods("KERBEROS");
        assertEquals("KERBEROS", getAuthMethods());

        for (RepNodeId rnid : rnIds) {
            final RepNodeAdminAPI rnai = createStore.getRepNodeAdmin(
                rnid, ADMIN_NAME, ADMIN_PW.toCharArray());
            final KerberosPrincipals princs = rnai.getKerberosPrincipals();
            final SNKrbInstance[] krbInstances = princs.getSNInstanceNames();
            assertEquals(krbInstances.length, numSNs);

            for (int i = 0; i < numSNs; i++) {
                assertEquals(krbInstances[i].getInstanceName(),
                    "instance" + (krbInstances[i].getStorageNodeId() - 1));
            }
        }
    }
}
