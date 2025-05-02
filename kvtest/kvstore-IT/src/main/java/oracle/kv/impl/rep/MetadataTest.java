/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 */
@RunWith(FilterableParameterized.class)
public class MetadataTest extends RepNodeTestBase {

    private static final TableImpl userTable =
        TableBuilder.createTableBuilder("User")
        .addInteger("id")
        .addString("firstName")
        .addString("lastName")
        .addInteger("age")
        .primaryKey("id")
        .shardKey("id")
        .buildTable();

    private final boolean useFastExternal;
    private KVRepTestConfig config;

    public MetadataTest(boolean useFastExternal) {
        this.useFastExternal = useFastExternal;
    }

    @Parameters(name="useFastExternal={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Override
    public void setUp() throws Exception {

        super.setUp();

        /*
         * Create a 2x2 so that the count of table rows is accurate.
         */
        config = new KVRepTestConfig(this,
                                     1, /* nDC */
                                     2, /* nSN */
                                     2, /* repFactor */
                                     10 /* nPartitions */);

        /*
         * Individual tests need to start rep node services after setting
         * any test specific configuration parameters.
         */
    }

    @Override
    public void tearDown() throws Exception {

        config.stopRepNodeServices();
        config = null;
        super.tearDown();
    }

    /**
     * General propagation test
     */
    @Test
    public void testPropagationAlltoAll() {
        config.startRepNodeServices();

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final RepNode rg1rn2 = config.getRN(new RepNodeId(1, 2));
        final RepNode rg2rn1 = config.getRN(new RepNodeId(2, 1));
        final RepNode rg2rn2 = config.getRN(new RepNodeId(2, 2));

        final TableMetadata md = new TableMetadata(false);

        addTable(md, userTable);

        /* update one node */
        updateMetadata(rg1rn1, md);

        assertEquals(md.getSequenceNumber(),
                     (int)rg1rn1.getMetadataSeqNum(MetadataType.TABLE));

        md.addTable(userTable.getInternalNamespace(),
                    userTable.getName() + "A",
                    userTable.getParentName(),
                    userTable.getPrimaryKey(),
                    null, /* primaryKeySizes */
                    userTable.getShardKey(),
                    userTable.getFieldMap(),
                    null, /* TTL */
                    null, /* limits */
                    false, 0,
                    null,
                    null);

        updateMetadata(rg2rn1, md);

        /* Wait for it to show up on node 2 */
        waitForUpdate(rg2rn2, md);

        /* Wait for it to show up on group 1 */
        waitForUpdate(rg1rn1, md);

        /* And finally, node 2 in group 1 */
        waitForUpdate(rg1rn2, md);

        /* Should still be correct */
        assertEquals(md.getSequenceNumber(),
                     (int)rg2rn2.getMetadataSeqNum(MetadataType.TABLE));
    }

    @Test
    public void testPropagationAlltoMasters() {
        config.startRepNodeServices();

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final RepNode rg1rn2 = config.getRN(new RepNodeId(1, 2));
        final RepNode rg2rn1 = config.getRN(new RepNodeId(2, 1));
        final RepNode rg2rn2 = config.getRN(new RepNodeId(2, 2));

        /* Send NOP to other nodes to update rgstate. */
        for (int i = 1; i <= 2; ++i) {
            for (int j = 1; j <=2; ++j) {
                RepNode rn1 = config.getRN(new RepNodeId(i, j));
                RepNodeId rn2Id = new RepNodeId(3 - i, 1);
                rn1.sendNOP(rn2Id);
            }
        }

        final TableMetadata md = new TableMetadata(false);

        addTable(md, userTable);

        /* update one node */
        updateMetadata(rg1rn1, md);

        assertEquals(md.getSequenceNumber(),
                     (int)rg1rn1.getMetadataSeqNum(MetadataType.TABLE));

        md.addTable(userTable.getInternalNamespace(),
                    userTable.getName() + "A",
                    userTable.getParentName(),
                    userTable.getPrimaryKey(),
                    null, /* primaryKeySizes */
                    userTable.getShardKey(),
                    userTable.getFieldMap(),
                    null, /* TTL */
                    null, /* limits */
                    false, 0,
                    null,
                    null);

        updateMetadata(rg2rn1, md);

        /* Wait for it to show up on node 2 */
        waitForUpdate(rg2rn2, md);

        /* Wait for it to show up on group 1 */
        waitForUpdate(rg1rn1, md);

        /* And finally, node 2 in group 1 */
        waitForUpdate(rg1rn2, md);

        /* Should still be correct */
        assertEquals(md.getSequenceNumber(),
                     (int)rg2rn2.getMetadataSeqNum(MetadataType.TABLE));
    }


    /**
     * Test metadata update push. By stopping rg1s update threads and updating
     *  rg2 it will force rg2 to push to rg1.
     */
    @Test
    public void testPropagationPush() {
        config.startRepNodeServices();

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final RepNode rg1rn2 = config.getRN(new RepNodeId(1, 2));
        final RepNode rg2rn1 = config.getRN(new RepNodeId(2, 1));
        final RepNode rg2rn2 = config.getRN(new RepNodeId(2, 2));

        config.getRepNodeService(rg1rn1.getRepNodeId())
                                            .stopMetadataUpdateThread(true);
        config.getRepNodeService(rg1rn2.getRepNodeId())
                                            .stopMetadataUpdateThread(true);

        final TableMetadata md = new TableMetadata(false);
        addTable(md, userTable);

        /* update one node in rg2 */
        updateMetadata(rg2rn1, md);

        /* Wait for it to show up on node 2 */
        waitForUpdate(rg2rn2, md);

        /* Wait for it to show up on group 1 */
        waitForUpdate(rg1rn1, md);

        /* And finally, node 2 in group 1 */
        waitForUpdate(rg1rn2, md);
    }

    /**
     * Test metadata update pull. By stopping rg2s update threads and updating
     * rg2 it will force rg1 to pull from rg2.
     *
     * Because the update does not poll, a node will not know to pull unless
     * prompted by some other action. In this test we write the MD to a node
     * which will cause it to find that there is newer MD out there.
     */
    @Test
    public void testPropagationPull() {
        config.startRepNodeServices();

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final RepNode rg1rn2 = config.getRN(new RepNodeId(1, 2));
        final RepNode rg2rn1 = config.getRN(new RepNodeId(2, 1));
        final RepNode rg2rn2 = config.getRN(new RepNodeId(2, 2));

        config.getRepNodeService(rg2rn1.getRepNodeId())
                                            .stopMetadataUpdateThread(true);
        config.getRepNodeService(rg2rn2.getRepNodeId())
                                            .stopMetadataUpdateThread(true);

        final TableMetadata md = new TableMetadata(false);
        addTable(md, userTable);

        final TableMetadata md1 = md.getCopy(); // MD seq # 1
        md.addTable(userTable.getInternalNamespace(),
                    userTable.getName() + "2",
                    userTable.getParentName(),
                    userTable.getPrimaryKey(),
                    null,
                    userTable.getShardKey(),
                    userTable.getFieldMap(),
                    null,
                    null, /* limits */
                    false, 0,
                    null,
                    null /* owner */);

        /* update one node in rg2 with md seq # 2 */
        updateMetadata(rg2rn1, md);

        /*
         * update rg1 with md seq #1. This should cause it to find md # 2 from
         * rg2
         */
        updateMetadata(rg1rn1, md1);

        /* Wait for seq # 2 to show up on group 1 */
        waitForUpdate(rg1rn1, md);

        /* And finally, node 2 in group 1 */
        waitForUpdate(rg1rn2, md);
    }

    /**
     * Waits for the specified node to have its metadata updated to the given
     * version or greater.
     *
     * @param rn
     * @param md
     */
    private void waitForUpdate(final RepNode rn, final Metadata<?> md) {

        boolean success = new PollCondition(100, 15000) {
            @Override
            protected boolean condition() {
                return
                   rn.getMetadataSeqNum(md.getType()) >= md.getSequenceNumber();
            }
        }.await();
        assert (success);
    }

    private void updateMetadata(RepNode rn, Metadata<?> md) {
        rn.updateMetadata(clone(md));
    }

    /* Clone the  MD to simulate remote call to the RN */
    private Metadata<?> clone(Metadata<?> md) {
        if (useFastExternal) {
            try {
                return TestUtils.fastSerialize(md, Metadata::readMetadata,
                                               Metadata::writeMetadata);
            } catch (IOException e) {
                throw new RuntimeException("Problem with serialization: " + e,
                                           e);
            }
        }
        return SerializationUtil.getObject(SerializationUtil.getBytes(md),
                                           md.getClass());
    }

    private TableImpl addTable(TableMetadata md, TableImpl table) {
        return md.addTable(table.getInternalNamespace(),
                           table.getName(),
                           table.getParentName(),
                           table.getPrimaryKey(),
                           null,
                           table.getShardKey(),
                           table.getFieldMap(),
                           null,
                           null, /* limits */
                           false, 0,
                           null, null);
    }
}
