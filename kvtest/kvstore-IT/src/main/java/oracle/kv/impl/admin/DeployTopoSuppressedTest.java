/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;

import java.io.File;

import oracle.kv.KVStoreConfig;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.util.KVSTestBase;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLPublisherConfig;
import oracle.kv.pubsub.NoSQLSubscriber;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;

import org.junit.Test;
import org.reactivestreams.Subscription;

/**
 * Test suppress deploying a topology when there are running subscription
 * streams. After SR[#26662], SR[#26724], SR#[26725], SR[#26726], the Streams
 * API can support elastic operations and concurrent streaming and partition
 * migration is supported.
 */
public class DeployTopoSuppressedTest extends KVSTestBase {

    String rootPath = "SuppressedTestSubscriptionDir";
    NoSQLPublisher publisher = null;

    public DeployTopoSuppressedTest() {
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        File file = new File(rootPath);
        if (file.exists()) {
            File[] files = file.listFiles();
            for (File f : files) {
                f.delete();
            }
        } else {
            file.mkdir();
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (publisher != null) {
            publisher.close(true);
        }

        File file = new File(rootPath);
        if (file.exists()) {
            File[] files = file.listFiles();
            for (File f : files) {
                f.delete();
            }
            file.delete();
        }
        super.tearDown();
    }

    /**
     * Redistribute a store when there is a running subscription stream,
     * expect success.
     */
    @Test
    public void testSuppressRedistributeTopo()
        throws Exception {
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 6 /* maxSNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);

        AdminUtils.makeSNPool(cs, "threeSNs", snSet.getIds(0, 1, 2));
        cs.createTopology("firstCandidate", "threeSNs", 10, false);
        int planNum = cs.createDeployTopologyPlan("DeployTopo",
                                                  "firstCandidate", null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        setSubscriber();

        snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3, 4, 5);
        AdminUtils.makeSNPool(cs, "sixSNs", snSet.getIds(0, 1, 2, 3, 4, 5));

        cs.copyCurrentTopology("redistribute");
        cs.redistributeTopology("redistribute", "sixSNs");

        planNum = cs.createDeployTopologyPlan("RedeployedTopo",
                                              "redistribute", null);
        /*
         * Because of running subscribe stream, creating deploy topo plan
         * fails and throws an exception
         */
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
    }

    /**
     * Contract a store when there is a running subscription stream, expect
     * success
     */
    @Test
    public void testSuppressContractTopo() throws Exception {
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 6 /* maxSNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3, 4, 5);

        AdminUtils.makeSNPool(cs, "sixSNs",
                              snSet.getIds(0, 1, 2, 3, 4, 5));
        cs.createTopology("firstCandidate", "sixSNs", 10, false);
        int planNum = cs.createDeployTopologyPlan("DeployTopo",
                                                  "firstCandidate", null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        setSubscriber();

        AdminUtils.makeSNPool(cs, "threeSNs", snSet.getIds(0, 1, 2));
        cs.copyCurrentTopology("contract");
        cs.contractTopology("contract", "threeSNs");


        planNum = cs.createDeployTopologyPlan("RedeployedTopo",
                                              "contract", null);
        /*
         * Because of running subscribe stream, creating deploy topo plan
         * fails and throws an exception
         */
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
    }

    /**
     * Install a subscription on KVStore.
     */
    private void setSubscriber() {
        SNSet snSet = sysAdminInfo.getSNSet();
        String host = snSet.getHostname(0);
        int port = snSet.getRegistryPort(0);
        String hostPort = host + ":" + port;

        final NoSQLPublisherConfig publisherConfig =
                new NoSQLPublisherConfig.Builder(
                    new KVStoreConfig(kvstoreName, hostPort), rootPath)
                    .setMaxConcurrentSubs(1)
                    .setShardTimeoutMs(6000)
                    .build();

        publisher = NoSQLPublisher.get(publisherConfig);

        final NoSQLSubscriptionConfig subscriptionConfig =
        /*
         * starting from the last checkpoint or from the beginning if
         * there was no checkpoint.
         */
        new NoSQLSubscriptionConfig.Builder("StreamExampleCkptTable")
            .setSubscribedTables("SYS$TableStatsPartition").build();

        final TestNoSQLSubscriber subscriber =
                new TestNoSQLSubscriber(subscriptionConfig);

        publisher.subscribe(subscriber);
    }

    class TestNoSQLSubscriber implements NoSQLSubscriber {
        final NoSQLSubscriptionConfig config;

        public TestNoSQLSubscriber(NoSQLSubscriptionConfig config) {
            this.config = config;
        }

        @Override
        public void onSubscribe(Subscription s) {
        }

        @Override
        public void onNext(StreamOperation t) {
        }

        @Override
        public void onError(Throwable t) {
            System.out.println(t);
        }

        @Override
        public void onComplete() {

        }

        @Override
        public NoSQLSubscriptionConfig getSubscriptionConfig() {
            return config;
        }

        @Override
        public void onWarn(Throwable t) {
        }

        @Override
        public void onCheckpointComplete(StreamPosition streamPosition,
                Throwable failureCause) {

        }
    }
}
