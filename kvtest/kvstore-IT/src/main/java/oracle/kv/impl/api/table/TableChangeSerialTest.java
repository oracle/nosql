/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static java.util.Collections.singletonList;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.SerialVersion.BEFORE_IMAGE_VERSION;
import static oracle.kv.impl.util.SerialVersion.SCHEMALESS_TABLE_VERSION;
import static oracle.kv.util.TestUtils.checkAll;

import oracle.kv.impl.api.ops.BasicClientTestBase;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.FieldDef;

import org.junit.Test;

/**
 * Test serial version compatibility for TableChange subclasses and associated
 * classes.
 */
public class TableChangeSerialTest extends BasicClientTestBase {

    private static final TableImpl TABLE =
        TableBuilder.createTableBuilder("myName")
        .addInteger("id")
        .addString("val")
        .primaryKey("id")
        .buildTable();

    private static final TableImpl REMOTE_TABLE;
    static {
        final TableBuilder builder =
            TableBuilder.createTableBuilder(
                "namespace", "remoteTable", "My description",
                null, /* parent */
                new TableMetadata(false /* keepChanges */).getRegionMapper());
        builder.addInteger("id")
            .addString("val")
            .primaryKey("id");
        builder.addRegion(1);
        REMOTE_TABLE = builder.buildTable();
    }

    /* Tests for TableChange subclasses */

    @Test
    public void testAddIndex() {
        checkAll(
            serialVersionChecker(
                new AddIndex("myNamespace", "myIndexName", "myTableName",
                             singletonList("bool"),
                             singletonList(FieldDef.Type.BOOLEAN),
                             false, /* indexNulls */
                             true, /* isUnique */
                             "My description",
                             42 /* seqNum */),
                0x29e1d411ce24f410L),
            serialVersionChecker(
                new AddIndex(null, /* namespace */
                             null, /* indexName */
                             null, /* tableName */
                             null, /* fields */
                             null, /* types */
                             true, /* indexNulls */
                             false, /* isUnique */
                             null, /* description */
                             43 /* seqNum */),
                0x6aff33f45e99b367L));
    }

    @Test
    public void testAddNamespaceChange() {
        checkAll(
            serialVersionChecker(
                new AddNamespaceChange("myNamespace",
                                       new ResourceOwner("myId", "myName"),
                                       43 /* seqNum */),
                0x460292f75d0306d7L),
            serialVersionChecker(
                new AddNamespaceChange(null, /* namespace */
                                       null, /* owner */
                                       44 /* seqNum */),
                0x4bdf5d8ba6abc3f4L));
    }

    @Test
    public void testAddRegion() {
        checkAll(
            serialVersionChecker(
                new AddRegion(new Region("myRegion", 44 /* id */),
                              45 /* seqNum */),
                0x6e6f3c3a991a9028L));
    }

    @Test
    public void testAddTable() {
        checkAll(
            serialVersionChecker(
                new AddTable(TABLE, 46 /* seqNum */),
                SerialVersion.MINIMUM, 0xd54f666087adaf6L,
                SCHEMALESS_TABLE_VERSION, 0xb30ed8b6e8485dbL,
                BEFORE_IMAGE_VERSION, 0x36bf1a86ace389eL),
            serialVersionChecker(
                new AddTable(REMOTE_TABLE, 47 /* seqNum */),
                SerialVersion.MINIMUM, 0x301999cf5482ab91L,
                SCHEMALESS_TABLE_VERSION, 0xa9b4408458a8f229L,
                BEFORE_IMAGE_VERSION, 0xd666ab860686aedL));
    }

    @Test
    public void testDropIndex() {
        checkAll(
            serialVersionChecker(
                new DropIndex("myNamespace", "myIndexName", "myTableName",
                              47 /* seqNum */),
                0xbc2563dcd15e6658L),
            serialVersionChecker(
                new DropIndex(null, /* namespace */
                              null, /* indexName */
                              null, /* tableName */
                              48 /* seqNum */),
                0x13e60da785e1ba30L));
    }

    @Test
    public void testDropTable() {
        checkAll(
            serialVersionChecker(
                new DropTable("myNamespace", "myTableName",
                              false, /* markForDelete */
                              49 /* seqNum */),
                0x8089852d8c57082eL),
            serialVersionChecker(
                new DropTable(null, /* namespace */
                              null, /* tableName */
                              true, /* markForDelete */
                              50 /* seqNum */),
                0xb64fdae02184f1eaL));
    }

    @Test
    public void testEvolveTable() {
        checkAll(
            serialVersionChecker(
                new EvolveTable(TABLE, 100),
                SerialVersion.MINIMUM, 0xa8799cc74eaa214cL,
                BEFORE_IMAGE_VERSION, 0xaa5cb77ed2f951d1L),
            serialVersionChecker(
                new EvolveTable(REMOTE_TABLE, 101),
                SerialVersion.MINIMUM, 0xc6024a6a32055764L,
                BEFORE_IMAGE_VERSION, 0x737cedf8035376f4L));
    }

    @Test
    public void testRemoveNamespaceChange() {
        checkAll(
            serialVersionChecker(
                new RemoveNamespaceChange("myNamespace", 52 /* seqNum */),
                0x32f49c3b6752ebceL),
            serialVersionChecker(
                new RemoveNamespaceChange(null /* namespace */,
                                          53 /* seqNum */),
                0x1a5e27158e16d1a3L));
    }

    @Test
    public void testRemoveRegion() {
        checkAll(
            serialVersionChecker(
                new RemoveRegion("myRegionName", 54 /* seqNum */),
                0x9f9d35885b722fbeL),
            serialVersionChecker(
                new RemoveRegion(null /* regionName */, 55 /* seqNum */),
                0x2094d508e47926f5L));
    }

    @Test
    public void testTableLimit() {
        checkAll(
            serialVersionChecker(
                new TableLimit(TABLE, 56 /* seqNum */),
                0x4d7e4adf74ad7c32L));
    }

    @Test
    public void testUpdateIndexStatus() {
        final IndexImpl index = new IndexImpl("indexName", TABLE,
                                              singletonList("val"),
                                              singletonList(null),
                                              "description");
        checkAll(
            serialVersionChecker(
                new UpdateIndexStatus(index, 56 /* seqNum */),
                0xfdc2e8381ff17f3bL));
    }

    /* Tests for associated classes */

    @Test
    public void testRegion() {
        checkAll(
            serialVersionChecker(
                new Region("regionName", 51 /* id */),
                0x16bf25c6e19dc225L));
    }
}
