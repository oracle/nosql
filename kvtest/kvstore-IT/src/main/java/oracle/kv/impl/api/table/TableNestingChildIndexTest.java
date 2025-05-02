/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */


package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;

import org.junit.Test;

/**
 * Unit test of nested child index
 *
 * To reproduce the test failure in oracle.kv.impl.api.table.TableNestingTest,
 * please revert the change
 * changeset:   7707:c5cee1e7c629
 * user:        Junyi Xie <junyi.xie@oracle.com>
 * date:        Wed Oct 14 12:07:20 2020 -0400
 * summary:     [] Move ancestorChildIndexTest out of TestNestingTest
 *
 * or simply copy test method {@link #ancestorChildIndexTest()} to
 * {@link TableNestingTest} and run test:
 * ant -e test -Dtestcase=oracle.kv.impl.api.table.TableNestingTest
 */
public class TableNestingChildIndexTest extends TableNestingTestBase {

    /**
     * Test ancestor/child returns on index iteration.
     */
    @Test
    public void ancestorChildIndexTest()
        throws Exception {

        /*
         * Putting this up here makes it easier to selectively comment out
         * test sections.
         */
        IndexKey ikey = null;
        Index index = null;
        TableIterator<Row> iter = null;
        int numRecords = 0;

        /*
         * Create and populate tables.  See comment above for hierarchy.
         */
        createTables();

        /*
         * Use the City index on the Address table and an ancestor
         * table of the User table.
         */
        MultiRowOptions mro =
            addressTable.createMultiRowOptions(Arrays.asList("User"), null);
        index = addressTable.getIndex("City");
        assertNotNull(index);
        ikey = index.createIndexKey();
        iter = tableImpl.tableIterator(ikey, mro, forwardOptions);
        numRecords = 0;
        boolean sawUserRecord = false;
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (row.getTable().equals(userTable)) {
                sawUserRecord = true;
            }
        }
        assertTrue(sawUserRecord);
        iter.close();

        /*
         * There are only 2 cities in the index.  There are numRows address
         * records for each user.  The city for all but one of the address
         * records for a given user is the same.  The redundant User table
         * records for the addresses with the common city are filtered but
         * the one address record that's different results in another match
         * to the same user records that cannot be filtered.  This means
         * an entry for every index entry (numUsers*numRows) + (2*numUsers)
         * for the redundant user record match.
         */
        assertEquals("Unexpected count", numUsers*numRows + 2 * numUsers,
                     numRecords);

        TableIterator<KeyPair> keyIter =
            tableImpl.tableKeysIterator(ikey, mro, forwardOptions);
        numRecords = 0;
        sawUserRecord = false;
        while (keyIter.hasNext()) {
            ++numRecords;
            KeyPair pair = keyIter.next();
            if (pair.getPrimaryKey().getTable().equals(userTable)) {
                sawUserRecord = true;
            }
        }
        assertTrue(sawUserRecord);
        assertEquals("Unexpected count", numUsers*numRows + 2*numUsers,
                     numRecords);
        keyIter.close();

        /*
         * Do the same operation but with a specific index key value vs
         * iteration over the entire index.
         */
        ikey.put("city", "Whoville");
        iter = tableImpl.tableIterator(ikey, mro, forwardOptions);
        numRecords = 0;
        sawUserRecord = false;
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (row.getTable().equals(userTable)) {
                sawUserRecord = true;
            }
        }
        assertTrue(sawUserRecord);
        iter.close();

        /*
         * one match for each address range (per-user) with a corresponding
         * user row.
         */
        assertEquals("Unexpected count", numUsers*2, numRecords);

        /*
         * Attempt to return child tables with an index iteration.  As of
         * R3 this is not supported.  It will probably be added.
         */
        mro.setIncludedChildTables(Arrays.asList((Table)emailTable));
        try {
            iter = tableImpl.tableIterator(ikey, mro, forwardOptions);
            fail("Child table returns are not yet supported for index scans");
        } catch (UnsupportedOperationException uoe) {
        }
    }
}
