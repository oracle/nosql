/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import javax.security.auth.Subject;

import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.TrustedLoginAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests the login operations of StorageNodeAgentImpl.
 */
@RunWith(FilterableParameterized.class)
public class StorageNodeLogin extends StorageNodeTestBase {

    public StorageNodeLogin(boolean useThreads) {
        super(useThreads);
    }

    /**
     * Notes: It is required to call the super methods if override
     * setUp and tearDown methods. 
     */
    @Override
    public void setUp() 
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() 
        throws Exception {

        super.tearDown();
    }

    /**
     * Test internal login in unregistered SNA.
     */
    @Test
    public void testUnregisteredTrustedLogin()
        throws Exception {

        createUnregisteredSNA(new SecureOpts().setSecure(true));

        /* Wait for the bootstrap admin. */
        waitForAdmin(testhost, sna.getRegistryPort(), sna.getLoginManager());

        final TrustedLoginAPI tla =
            RegistryUtils.getStorageNodeAgentLogin(sna.getHostname(),
                                                   sna.getRegistryPort(),
                                                   logger);

        LoginResult lr = tla.loginInternal();
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());

        LoginToken lt = lr.getLoginToken();
        Subject sub = tla.validateLoginToken(lt);
        assertNotNull(sub);

        tla.logout(lt);

        sub = tla.validateLoginToken(lt);
        assertNull(sub);
    }

    /**
     * Test trusted login in registered SNA.
     */
    @Test
    public void testRegisteredTrustedLogin()
        throws Exception {

        StorageNodeId snid = new StorageNodeId(1);
        createRegisteredStore(snid, true,
                              new SecureOpts().setSecure(true));

        /* Wait for the bootstrap admin. */
        waitForAdmin(testhost, sna.getRegistryPort(), sna.getLoginManager());

        final TrustedLoginAPI tla =
            RegistryUtils.getStorageNodeAgentLogin(sna.getHostname(),
                                                   sna.getRegistryPort(),
                                                   logger);

        LoginResult lr = tla.loginInternal();
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());

        LoginToken lt = lr.getLoginToken();
        Subject sub = tla.validateLoginToken(lt);
        assertNotNull(sub);

        tla.logout(lt);

        sub = tla.validateLoginToken(lt);
        assertNull(sub);

    }
}
