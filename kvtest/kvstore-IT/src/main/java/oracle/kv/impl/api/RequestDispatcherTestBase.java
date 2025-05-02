/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStoreConfig;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.Key;
import oracle.kv.RequestLimitConfig;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.Protocols;

/**
 * Base class for request dispatcher tests.
 */
public class RequestDispatcherTestBase extends RepNodeTestBase {

    private static final LoginManager NULL_LOGIN_MGR = null;

    /**
     * KVConfig parameters
     */
    protected int nDC = 1;
    protected int nSN = 2;
    protected int repFactor = 3;
    protected int nPartitions = 10;
    protected int nSecondaryZones = 0;
    protected int nShards = 0;
    protected String[] readZones = null;

    protected KVRepTestConfig config;
    protected int seqNum;
    protected RequestDispatcherImpl dispatcher;
    protected final Durability allDurability =
        new Durability(SyncPolicy.NO_SYNC,
                       SyncPolicy.NO_SYNC,
                       Durability.ReplicaAckPolicy.ALL);
    protected final Consistency consistencyNone = Consistency.NONE_REQUIRED;

    protected ClientId clientId;
    protected final RepGroupId rg1Id = new RepGroupId(1);
    protected final RepNodeId rg1n1Id = new RepNodeId(1,1);
    protected final RepNodeId rg1n2Id = new RepNodeId(1,2);

    public RequestDispatcherTestBase() {
        super();
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();

        /* Create two groups with three RNs each*/
        config = new KVRepTestConfig(this, nDC, nSN, repFactor,
                                     nPartitions, nSecondaryZones, nShards);
        config.startupRHs();

        clientId = new ClientId(1000);

        /* Simulate a client remote dispatcher. */
        dispatcher = createRequestDispatcher();

        seqNum = config.getTopology().getSequenceNumber();
    }

    @Override
    public void tearDown()
        throws Exception {

        config.stopRHs();
        super.tearDown();
    }

    protected byte[] getKeyBytes(Key key) {
        return key.getMajorPath().get(0).getBytes();
    }

    /**
     * Derived classes that test with security should override this to provide
     * their LoginManager.
     */
    protected LoginManager getLoginManager() {
        return NULL_LOGIN_MGR;
    }

    /**
     * Create a client remote dispatcher.
     */
    protected RequestDispatcherImpl createRequestDispatcher() {
        return RequestDispatcherImpl.createForClient(
            Protocols.getDefault(),
            kvstoreName,
            clientId,
            config.getTopology(),
            getLoginManager(),
            RequestLimitConfig.getDefault(),
            this,
            ClientLoggerUtils.getLogger(RequestDispatcherImpl.class, "test"),
            readZones);
    }

    /**
     * Create a client remote dispatcher with specified async configuration.
     */
    protected RequestDispatcherImpl createRequestDispatcher(boolean async) {
        final KVStoreConfig kvsconfig = config.getKVSConfig();
        kvsconfig.setUseAsync(async);
        return RequestDispatcherImpl.createForClient(
            Protocols.get(kvsconfig),
            kvstoreName,
            clientId,
            config.getTopology(),
            getLoginManager(),
            RequestLimitConfig.getDefault(),
            this,
            ClientLoggerUtils.getLogger(RequestDispatcherImpl.class, "test"),
            readZones);
    }
}
