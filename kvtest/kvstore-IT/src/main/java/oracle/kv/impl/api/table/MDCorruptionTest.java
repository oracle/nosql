/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.AdminTestConfig;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.DeployTableMetadataPlan;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.task.UpdateMetadata;
import oracle.kv.impl.api.table.TableChange.ChangeType;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.TestUtils;

import com.sleepycat.je.Transaction;

import org.junit.Test;

/**
 * This tests the recovery code for a specific type of metadata corruption on
 * the RN. The recovery is for when the RN can no longer update it's table
 * metadata via a change list.
 */
public class MDCorruptionTest extends TestBase {

    static AdminTestConfig atc;
    static private StorageNodeAgent[] snas;
    static private PortFinder[] portFinders;
    static private Admin admin;
    static private final int startPort = 13250;
    static private final int haRange = 8;
    static private final int numSNs = 3;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.clearTestDirectory();
        initStorageNodes();
    	atc = new AdminTestConfig(kvstoreName, portFinders[0]);
        runStaticSetup();

    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        for (StorageNodeAgent sna : snas) {
            if (sna != null) {
                sna.shutdown(true, true);
            }
        }
    }

    /*
     * Tests metadata corruption preventing change list elements from being
     * processed. Note that this test injects a bad change list element vs.
     * corrupting the RNs metadata. The end result of the update call is the
     * same.
     *
     * Because of the store size (one shard) if the "corruption" is not
     * cleared the update will retry forever resulting in a test timeout
     * and failure.
     */
    @Test
    public void testUpdateFail() throws Exception {
        final Planner planner = admin.getPlanner();

        /*
         * Run a table metadata plan which contains a change list element
         * that will fail when applied. The Admin should detect this
         * and send the full metadata to the RN in an attempt to clear
         * the problem.
         */
        DeployTableMetadataPlan plan =
                new DeployTableMetadataPlan("BAD PLAN", planner, false);
        plan.addTask(new UpdateMDTask(plan, false /*isGood*/));
        planner.register(plan);
        admin.savePlan(plan, "unit test");

        runPlan(plan.getId());

        /* Run a good plan to double check that things are working properly */
        plan = new DeployTableMetadataPlan("GOOD PLAN", planner, false);
        plan.addTask(new UpdateMDTask(plan, true /*isGood*/));
        planner.register(plan);
        admin.savePlan(plan, "unit test");

        runPlan(plan.getId());
    }

    static private void runPlan(int planId) {

        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);
    }

    /* Store creation methods cribed from elsewhere */

    static private void runStaticSetup()
        throws Exception {

        AdminServiceParams adminServiceParams = atc.getParams();

        admin = new Admin(adminServiceParams);

        /* Deploy a Datacenter */
        deployDatacenter();

        /* Deploy SNs and Admin */
        deployStorageNodesAndAdmin();

        /* Deploy the store */
        deployStore();
    }

    static private void deployDatacenter() {
        int id = admin.getPlanner().
            createDeployDatacenterPlan("deploy data center", "Miami", 3,
                                       DatacenterType.PRIMARY, false, false);
        runPlan(id);
    }

    static private void deployStorageNodesAndAdmin() {

        DatacenterId dcid = new DatacenterId(1);
        boolean deployedAdmin = false;
        for (StorageNodeAgent sna : snas) {
            StorageNodeParams snp = new StorageNodeParams
                (sna.getHostname(), sna.getRegistryPort(), null);
            int id = admin.getPlanner().
                createDeploySNPlan("Deploy SN", dcid, snp);
            runPlan(id);

            if (!deployedAdmin) {
                deployAdmin();
                deployedAdmin = true;
            }
        }
    }

    static private void deployAdmin() {
        int id = admin.getPlanner().createDeployAdminPlan
            ("Deploy Admin", snas[0].getStorageNodeId());
        runPlan(id);
    }

    static private void deployStore() {
        admin.createTopoCandidate("DDL", Parameters.DEFAULT_POOL_NAME,
                                  10, false,
                                  SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
        int id = admin.getPlanner().createDeployTopoPlan("Deploy Store", "DDL",
                                                         null);
        runPlan(id);
    }

    static private void initStorageNodes()
        throws Exception {

        snas = new StorageNodeAgent[numSNs];
        portFinders = new PortFinder[numSNs];
        int port = startPort;

        for (int i = 0; i < snas.length; i++) {
            portFinders[i] = new PortFinder(port, haRange);
            snas[i] = StorageNodeUtils.createUnregisteredSNA
                (TestUtils.getTestDir().toString(),
                 portFinders[i], 1, ("config" + i + ".xml"),
                 false, /* useThreads */
                 true,  /* create admin */
                 null, 0, 0, 1024, null, null);
            port += 10;
        }
    }

    /*
     * Task which applies the bad change, placing it in the metadata's
     * change list.
     */
    private static class UpdateMDTask extends UpdateMetadata<TableMetadata> {
        private static final long serialVersionUID = 1L;

        private final boolean isGood;

        public UpdateMDTask(MetadataPlan<TableMetadata> plan, boolean isGood) {
            super(plan);
            this.isGood = isGood;
        }

        @Override
        protected TableMetadata updateMetadata(TableMetadata md,
                                               Transaction txn) {
            md.apply(isGood ? new GoodChange(md.getSequenceNumber()+1) :
                              new BadChange(md.getSequenceNumber()+1));
            return md;
        }

        @Override
        protected TableMetadata createMetadata() {
            return new TableMetadata(true);
        }
    }

    enum ExtraChangeTypes implements ChangeType {
        GOOD(100, GoodChange::new),
        BAD(101, BadChange::new);

        final int intValue;
        final ReadFastExternal<TableChange> reader;
        ExtraChangeTypes(int intValue, ReadFastExternal<TableChange> reader) {
            this.intValue = intValue;
            this.reader = reader;
        }
        @Override
        public int getIntValue() {
            return intValue;
        }
        @Override
        public TableChange readTableChange(DataInput in, short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }
    }

    static {
        TableChange.addChangeTypeFinder(tableId -> {
                switch (tableId) {
                case 100: return ExtraChangeTypes.GOOD;
                case 101: return ExtraChangeTypes.BAD;
                default: return null;
                }
            });
    }

    private static class GoodChange extends TableChange {
        private static final long serialVersionUID = 1L;

        GoodChange(int seqNum) {
            super(seqNum);
        }
        GoodChange(DataInput in, short sv) throws IOException {
            super(in, sv);
        }
        @Override
        protected TableImpl apply(TableMetadata md) {
            return null;
        }
        @Override
        ChangeType getChangeType() {
            return ExtraChangeTypes.GOOD;
        }
    }

    /*
     * Change list entry which will cause the metadata update to fail on
     * the RN. This failure simulates a metadata corruption that results in
     * the a change entry failure, rendering the RN unable to process any
     * further changes.
     */
    private static class BadChange extends TableChange {
        private static final long serialVersionUID = 1L;

        /*
         * A bit of trickery going on here. We are inserting this change
         * via the apply() on table metadata. But we don't want it to fail
         * that time. So we start with inTest=true. After the change is
         * applied once, firstTime=false and apply() will fail everywhere else.
         */
        private boolean firstTime;

        BadChange(int seqNum) {
            super(seqNum);
            firstTime = true;
        }

        BadChange(DataInput in, short sv) throws IOException {
            super(in, sv);
            firstTime = in.readBoolean();
        }

        @Override
        public void writeFastExternal(DataOutput out, short sv)
            throws IOException
        {
            out.writeBoolean(firstTime);
        }

        @Override
        protected TableImpl apply(TableMetadata md) {
            if (!firstTime) {
                throw new IllegalCommandException("Injected exception");
            }
            firstTime = false;
            return null;
        }
        @Override
        ChangeType getChangeType() {
            return ExtraChangeTypes.BAD;
        }
    }
}
