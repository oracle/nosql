/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;

import oracle.kv.impl.streamservice.MRTTestService;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests multi-region table DDL operations.
 */
public class RegionTest extends TableTestBase {

    private MRTTestService service = null;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /*
         * Exclude tombstones because some unit tests count the number of
         * records in the store and tombstones will cause the result not
         * match the expected values.
         */
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        TableTestBase.staticSetUp(1 /*nSNs*/, 1 /*rf*/, 1 /*capacity*/);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        /* Start the multi-region table test service */
        service = new MRTTestService(tableImpl, logger);
        service.startPolling();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (service != null) {
            service.stopPolling();
            service = null;
        }
    }

    /*
     * The store is only created once for this test. Any additional methods
     * must not set the local region name, otherwise this test method may
     * fail depending on the order of execution. This is due to it checking
     * the upgrade case where the local region was not set but MR tables
     * exist.
     */
    @Test
    public void testRegionDDL() {
        /* 127 chars */
        final String LONG_NAME = "A234567890123456789012345678901234567890" +
                                 "1234567890123456789012345678901234567890" +
                                 "1234567890123456789012345678901234567890" +
                                 "12345678";

        service.waitForReady();

        /* Show regions before any regions are defined */
        /* TODO - Need to verify result. */
        executeDdl("show regions");

        /* Attempt to create a table with a region before it is defined */
        executeDdl("create table MRTable(id integer, primary key(id))" +
                   " in regions nonexistRegion", null, false /* shouldSucceed*/,
                   true/*noMRTableMode*/);
        executeDdl("create region region1");
        executeDdl("create region region2");

        /* Attempt to create an existing region */
        executeDdl("create region region1", null, false /*shouldSucceed*/,
                   true);
        /* Check case insensitivity */
        executeDdl("create region Region1", null, false /*shouldSucceed*/,
                   true);
        /* 128 is OK */
        executeDdl("create region " + LONG_NAME);
        /* 129 is not */
        executeDdl("create region " + LONG_NAME + "9",
                   null, false /*shouldSucceed*/, true);

        /* Drop a region that doesn't exist */
        executeDdl("drop region notthere", null, false /*shouldSucceed*/, true);
        /* Attempt to create a table before local region is set */
        executeDdl("create region region3");
        executeDdl("create table MRTable(id integer, primary key(id))" +
                   " in regions region3", null, false /* shouldSucceed*/);

        executeDdl("drop region region3");

        /* Attempt to set local region name to an exiting remote region */
        executeDdl("set local region region1", null, false /* shouldSucceed*/);

        /* Set the local region name */
        executeDdl("set local region myFirstLocalRegion");

        /* Change the local region name */
        executeDdl("set local region myLocalRegion");

        /* Attempt to create a region with the same name as the local region */
        executeDdl("create region myLocalRegion", null,
                   false /*shouldSucceed*/);

        executeDdl("create region region1", null, false /*shouldSucceed*/,
                   true);

        /* Create a table with one region, include local region name */
        executeDdl("create table MRTable2(id integer, primary key(id))" +
                   " in regions myLocalRegion, region1");
        /* Attempt to set the local region name with an existing MRT */
        executeDdl("set local region shouldFail", null,
                   false /*shouldSucceed*/);

        /* Even setting local region to same name should fail */
        executeDdl("set local region myLocalRegion", null,
                   false /*shouldSucceed*/);
        /* Attempt to drop a region in use. Should fail. */
        executeDdl("drop region region1", null, false /*shouldSucceed*/, true);
        executeDdl("drop table MRTable2");
        /* Should be able to drop the region */
        executeDdl("drop region region1");

        /* Attempt to create a table in the dropped region. */
        executeDdl("create table MRTable3(id integer, primary key(id))" +
                   " in regions region1, region2", null,
                   false /* shouldSucceed*/, true);
        /* Recreate the region. TODO- need to check that ID is reused? */
        executeDdl("create region region1");
        /* Create the table in the recreated region. */
        executeDdl("create table MRTable3(id integer, primary key(id))" +
                   " in regions region1, region2", true, true);
        executeDdl("drop table MRTable3");
        executeDdl("drop region region1");
        executeDdl("drop region region2");

        /* Additional create MR table cases */

        executeDdl("create region region4", null, true/*shouldSucceed*/,
                   true);
        executeDdl("create table MRTable4(id integer, primary key(id))" +
                   " in regions", null, false /*shouldSucceed*/, true);

        /* Create MR table with only local region */
        executeDdl("create table MRLocal(id integer, primary key(id))" +
                   " in regions myLocalRegion");
        assertEquals(0, getTable("MRLocal").getRemoteRegions().size());
        executeDdl("drop table MRLocal");

        /*
         * Attempt to create a table with an identity column.
         */
        executeDdl("create table MRIdentity" +
                   "(id long generated always as identity " +
                   " (start with 1 increment by 1 maxvalue 100 cycle" +
                   " cache 3), name string, primary key(id))" +
                   " in regions region4", null, false /*shouldSucceed*/, true);
        /* Check ordering of region and ttl table options */
        executeDdl("create table MRTableOption1(id integer, primary key(id))" +
                   " in regions region4 using ttl 1 hours");
        executeDdl("create table MRTableOption2(id integer, primary key(id))" +
                   " using ttl 1 hours in regions region4");
        /* Can only specify option once */
        executeDdl("create table MRTableOption3(id integer, primary key(id))" +
                   " in regions region1 in regions region4",
                   null, false /*shouldSucceed*/, true);
        executeDdl("drop table MRTableOption1");
        executeDdl("drop table MRTableOption2");

        /*
         * Attempt to create with multiple regions. Should fail since
         * one region is not defined.
         */
        executeDdl("create table MRTable5(id integer, primary key(id))" +
                   " in regions region4, region5", null,
                   false /*shouldSucceed*/, true);

        /* Create the needed region + 1 */
        executeDdl("create region region5");
        /* Retry to create with multiple regions */
        executeDdl("create table MRTable5(id integer, primary key(id))" +
                   " in regions region4, region5", true, true);
        executeDdl("drop table MRTable5");
        /*
         * Attempt to create a child table in a region. Should fail, a child
         * table cannot be MR.
         */
        executeDdl("create table MRTable5.child(foo integer, " +
                   "primary key(foo))" +
                   " in regions region4", null, false /*shouldSucceed*/, true);

        /* Ran into a serialization bug with this one */
        executeDdl("create table DtTable (id LONG, timestampField TIMESTAMP" +
                   "(9)" +
                   " NOT NULL DEFAULT '1970-01-01T00:00:00.000000000'," +
                   " PRIMARY KEY(SHARD(id))) IN REGIONS region4");
        executeDdl("drop table DtTable");

        /* Wait until the service has dropped everything */
        service.waitForTables(0);
        service.dumpRegions();

        service.failRequest();

        /* Despite the failure from the request the DDL will succeed */
        executeDdl("create table MRTable6(id integer, primary key(id))" +
                   " in regions region4, region5");
        service.dumpTables();
        /* Though the request failed the table is present, remove it */
        executeDdl("drop table MRTable6");
        executeDdl("drop region region4");
        executeDdl("drop region region5");

        /* Cleanup */
        service.waitForGC();
        service.waitForTables(0);

        /* TODO - Need to verify result. */
        executeDdl("show regions");

        /* Alter/drop MR table cases */

        executeDdl("create table MRLocal(id integer, primary key(id))" +
                   " in regions myLocalRegion");
        assertEquals(0, getTable("MRLocal").getRemoteRegions().size());

        executeDdl("alter table MRLocal (add age integer)");
        executeDdl("create region region7");
        executeDdl("alter table MRLocal add regions region7");
        assertEquals(1, getTable("MRLocal").getRemoteRegions().size());

        /* Attempt to add the local region */
        executeDdl("alter table MRLocal add regions myLocalRegion",
                   null, false /*shouldSucceed*/);

        /* Attempt to drop the local region */
        executeDdl("alter table MRLocal drop regions myLocalRegion",
                   null, false /*shouldSucceed*/);
        executeDdl("alter table MRLocal drop regions region7");
        executeDdl("drop region region7");
        assertEquals(0, getTable("MRLocal").getRemoteRegions().size());
        executeDdl("drop table MRLocal");
        service.waitForTables(0);
    }
}
