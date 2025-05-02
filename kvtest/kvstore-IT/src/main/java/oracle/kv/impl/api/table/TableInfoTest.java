/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import oracle.kv.TestBase;
import oracle.kv.impl.measurement.TableInfo;
import oracle.kv.impl.rep.table.TableManager;
import org.junit.Test;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

/*
 * This test is meant to improve TableInfo's code coverage
 */
public class TableInfoTest extends TestBase {

    /*
     * Create 2 TableInfo objects to compare its values
     * Try to create a tableInfo object with duration == 0
     */
    @Test
    public void testTableInfo() throws Exception {
        /* Setup for 2 TableInfo objects */
        int numTables = 2;
        final TableManager tableManager = createMock(TableManager.class);
        expect(tableManager.getTableInfo()).andStubReturn(getTableInfo(numTables));
        replay(tableManager);
        Set<TableInfo> tableInfoSet = tableManager.getTableInfo();
        Iterator<TableInfo> tableInfoIterator = tableInfoSet.iterator();
        assertTrue("tableInfos.size() is not " + numTables + " as expected",
                numTables == tableInfoSet.size());

        TableInfo tableInfoA = tableInfoIterator.next();
        TableInfo tableInfoB = tableInfoIterator.next();

        /* Assert */
        assertNotEquals(tableInfoA, tableInfoB);
        assertNotNull(tableInfoA.toJson());
        assertTrue(tableInfoA.getStartTimeMillis() > 0);
        assertFalse(tableInfoB.toString().isEmpty());
        assertEquals(tableInfoA.getTableName(),
                tableInfoB.getTableName());
        assertNotEquals(tableInfoA.getDurationMillis(),
                tableInfoB.getDurationMillis());

        /* Tests TableInfo with duration == 0 */
        try {
            tableInfoA = new TableInfo("casewitherror",
                    1,
                    System.currentTimeMillis(), 0, 1000, 1000,
                    0, 0,
                    0, 0,
                    0);
            fail("AssertionError expected but got a TableInfo object");

        } catch (AssertionError e) {
            assertTrue(true);
        }
    }

    /**
     * Gets a dummy Set of TableInfo with no exceptions reported
     * limits are the same for every TableInfo object
     * duration is > 0 and different for each TableInfo object
     * @return Set<TableInfo>
     */
    private Set<TableInfo> getTableInfo(int numTables) {
        /* KB = 1024; MB = KB * KB; GB = MB * KB; */
        long GB = 1024 * 1024 * 1024;
        int readKB = 2000;
        int writeKB = 1000;
        final long startTime = System.currentTimeMillis();
        Set<TableInfo> tableInfoSet = new HashSet<TableInfo>();

        /* Intentionally skipping duration == 0 to avoid an assert error */
        for (int i = 1; i <= numTables; i++) {

            long duration = 1000 * i;
            long size = i * GB;
            tableInfoSet.add(new TableInfo("test", 1000,
                    startTime, duration, readKB, writeKB,
                    size, 0,
                    0, 0,
                    0));
        }
        return tableInfoSet;
    }
}
