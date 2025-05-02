/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.hadoop.table;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;

import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.topo.RepGroupId;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests that provide test coverage for the <code>TableInputSplit</code>
 * class from the <code>oracle.kv.hadoop.table</code> package.
 */
public class TableInputSplitCoverageTest extends HadoopTableCoverageTestBase {

    @BeforeClass
    public static void staticSetUp() throws Exception {

        THIS_CLASS = TableInputSplitCoverageTest.class;
        THIS_CLASS_NAME = THIS_CLASS.getSimpleName();
        HadoopTableCoverageTestBase.staticSetUp();
    }

    @Test
    public void testReadFields() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit1 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        final long nSets = 3;
        final int nPartitions = 3;
        final List<Set<Integer>> partitionSets1 =
                                     new ArrayList<Set<Integer>>();
        for (long i = 0; i < nSets; i++) {
            final Set<Integer> partitionSet = new HashSet<>();
            for (int j = 0; j < nPartitions; j++) {
                partitionSet.add(j);
            }
            partitionSets1.add(partitionSet);
        }
        tableInputSplit1 = tableInputSplit1.setPartitionSets(partitionSets1);
        tableInputSplit1 = tableInputSplit1.setPrimaryKeyProperty("PPPPP");
        tableInputSplit1 = tableInputSplit1.setFieldRangeProperty("RRRRR");
        tableInputSplit1 = tableInputSplit1.setQueryInfo(
                               TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS,
                               "AAA whereClause");

        final String[] locations = tableInputSplit1.getLocations();
        final int len = locations.length;

        final String[] helperHosts = tableInputSplit1.getKVHelperHosts();
        final int nHelperHosts = helperHosts.length;

        /* Exercise TableInputSplit.getKVStoreName() */
        assertTrue("null returned by TableInputSplit.getKVStoreName",
                   tableInputSplit1.getKVStoreName() != null);

        /* Exercise TableInputSplit.getTableName() */
        assertTrue("null returned by TableInputSplit.getKVStoreName",
                   tableInputSplit1.getTableName() != null);

        /* Exercise TableInputSplit.getPrimaryKeyProperty() */
        assertTrue("null returned by TableInputSplit.getPrimaryKeyProperty",
                   tableInputSplit1.getPrimaryKeyProperty() != null);

        /* Exercise TableInputSplit.getFieldRangeProperty() */
        assertTrue("null returned by TableInputSplit.getFieldRangeProperty",
                   tableInputSplit1.getFieldRangeProperty() != null);

        final byte defaultByte = 1;
        final int defaultInt = 1;
        final long defaultLong = 1L;

        final byte[] defaultBytesArray = { 0 };
        final int defaultBytesArrayOffset = 0;
        final int defaultBytesArrayLen = 1;

        final DataInput mockedDataInput = createMock(DataInput.class);

        // 1. nLocations - DataInputStream.readInt then in.readByte/readFully
        expect(mockedDataInput.readInt()).andReturn(len).once();
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);
        expectLastCall(); // readFully is void

        // 2. locations - Text.readString, then in.readByte and readFully
        for (int i = 0; i < len; i++) {
            expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
            mockedDataInput.readFully(defaultBytesArray,
                                      defaultBytesArrayOffset,
                                      defaultBytesArrayLen);
            expectLastCall(); // readFully is void
        }

        // 3. nHelperHosts - DataInputStream.readInt then in.readByte/readFully
        expect(mockedDataInput.readInt()).andReturn(nHelperHosts).once();
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);
        expectLastCall(); // readFully is void

        // 4. kvHelperHosts - Text.readString, then in.readByte and readFully
        for (int i = 0; i < nHelperHosts; i++) {
            expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
            mockedDataInput.readFully(defaultBytesArray,
                                      defaultBytesArrayOffset,
                                      defaultBytesArrayLen);
            expectLastCall(); // readFully is void
        }


        // 5. kvStore - Text.readString, then in.readByte and readFully
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);
        expectLastCall(); // readFully is void

        // 6. tableName - Text.readString only
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();

        // 7. primaryKeyProperty - Text.readString, then in.readFully
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);
        expectLastCall(); // readFully is void

        // 8. MultiRowOptions(fieldRangeProperty) - Text.readString only

        // 9. TableIteratorOptions(dirStr) - Text.readString, then in.readFully
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);
        expectLastCall(); // readFully is void

        // 10. Consistency - DataInputStream.readInt then in.readFully
        expect(mockedDataInput.readInt()).andReturn(defaultInt).once();
        mockedDataInput.readFully(defaultBytesArray);
        expectLastCall(); // readFully is void
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);
        expectLastCall(); // readFully is void

        // 11. timeout - DataInputString.readLong, then in.readByte
        expect(mockedDataInput.readLong()).andReturn(defaultLong).once();
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();

        // 12. timeoutUnit - Text.readString only

        // 13. maxRequests - DataInputString.readInt only
        expect(mockedDataInput.readInt()).andReturn(defaultInt).once();

        // 14. batchSize - DataInputString.readInt only
        expect(mockedDataInput.readInt()).andReturn(defaultInt).once();

        // 15. maxBatches - DataInputString.readInt only
        expect(mockedDataInput.readInt()).andReturn(defaultInt).once();

        // 16. nPartitionSets - DataInputString.readInt only
        expect(mockedDataInput.readInt()).andReturn(defaultInt).once();

        // 17a. partitionSets - DataInputString.readInt only
        for (int i = 0; i < defaultInt; i++) {
            expect(mockedDataInput.readInt()).andReturn(defaultInt).once();

            // 17b. partitions - DataInputString.readInt only
            for (int j = 0; j < defaultInt; j++) {
                expect(mockedDataInput.readInt()).andReturn(defaultInt).once();
            }
        }

        // 18. queryBy - DataInputString.readInt only
        expect(mockedDataInput.readInt()).andReturn(defaultInt).once();

        // 19. nShards - DataInputString.readInt
        expect(mockedDataInput.readInt()).andReturn(defaultInt).once();

        // 20. shardSet - DataInputString.readInt only
        for (int i = 0; i < defaultInt; i++) {
            expect(mockedDataInput.readInt()).andReturn(defaultInt).once();
        }

        // 21. onqlWhereClause - Text.readString, then in.readByte & readFully
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);
        expectLastCall(); // readFully is void

        // 22. loginFlnm - Text.readString, then in.readByte & readFully
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);
        expectLastCall(); // readFully is void

        // 23. userName - Text.readString, then in.readByte & readFully
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);

        // 24. userPasswordStr - Text.readString, then in.readByte & readFully
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);

        // 25. trustFlnm - Text.readString, then in.readByte & readFully
        expect(mockedDataInput.readByte()).andReturn(defaultByte).once();
        mockedDataInput.readFully(defaultBytesArray,
                                  defaultBytesArrayOffset,
                                  defaultBytesArrayLen);
        expectLastCall(); // readFully is void

        replay(mockedDataInput);

        tableInputSplit1.readFields(mockedDataInput);
    }

    /* --- Tests that exercise miscellaneous accessor methods --- */

    @Test
    public void testGetMaxBatches() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final int curMaxBatches = defaultTableInputSplit.getMaxBatches();
        final int expectedMaxBatches = maxBatches;
        assertTrue("unexpected value from TableInputSplit.getMaxBatches(): " +
                   "expected " + expectedMaxBatches + "batches, but " +
                   "returned " + curMaxBatches + " batches",
                   expectedMaxBatches == curMaxBatches);
    }

    @Test
    public void testGetLocations() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final String[] curLocsArray = defaultTableInputSplit.getLocations();
        final List<String> curLocs = Arrays.asList(curLocsArray);

        final List<String> expectedLocs = new ArrayList<>();
        for (String helperHost : defaultHelperHosts) {
            /* Strip off the ':port' suffix */
            final String[] hostPort = helperHost.trim().split(":");
            expectedLocs.add(hostPort[0]);
        }
        assertTrue("unexpected value from TableInputSplit.getLocations(): " +
                   "expected " + expectedLocs + ", but returned " + curLocs,
                   expectedLocs.equals(curLocs));
    }

    @Test
    public void testGetLength() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);

        final long expectedLength = 3;
        final int nPartitions = 3;
        final List<Set<Integer>> partitionSets1 =
                                     new ArrayList<Set<Integer>>();
        for (long i = 0; i < expectedLength; i++) {
            final Set<Integer> partitionSet = new HashSet<>();
            for (int j = 0; j < nPartitions; j++) {
                partitionSet.add(j);
            }
            partitionSets1.add(partitionSet);
        }
        tableInputSplit = tableInputSplit.setPartitionSets(partitionSets1);
        final long curLength = tableInputSplit.getLength();

        assertTrue("unexpected value from TableInputSplit.getLength(): " +
                   "expected " + expectedLength + " partition sets, but " +
                   "returned " + curLength + " partition sets",
                   expectedLength == curLength);
    }

    /* --- Test that exercise all branches of the hashCode() method --- */

    @Test
    public void testHashCode() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setFieldRangeProperty("XXXX");
        tableInputSplit = tableInputSplit.setQueryInfo(
                               TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS,
                               "AAA whereClause");

        final int curHashCode = tableInputSplit.hashCode();
        assertTrue("expected non-zero value from " +
                   "TableInputSplit.hashCode() but zero was returned",
                   curHashCode != 0);
    }


    /* --- Tests that exercise all branches of the equals() method --- */

    @Test
    public void testEqualsTrue() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        assertTrue("expected two TableInputSplit instances to be " +
                   "equal, but equals() returned false " +
                   "[split 1: " + defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsNotTableInputSplit() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final Object tableInputSplit = new Object();
        assertTrue("expected two TableInputSplit instances to NOT be " +
                   "equal, but equals() returned true " +
                   "[split 1: " + defaultTableInputSplit + " equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsSameTableInputSplit() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final TableInputSplit tableInputSplit = defaultTableInputSplit;
        assertTrue("expected equals() method to return true when applied " +
                   "to the same TableInputSplit instance, but false was " +
                   "returned [split 1: " + defaultTableInputSplit + " not " +
                   "equal to split 2: " + tableInputSplit + "]",
                   defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsNullStoreName2nd() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit = new TableInputSplit();
        tableInputSplit = tableInputSplit.setKVStoreName(null);

        assertTrue("expected equals() method to return false when applied " +
                   "to a TableInputSplit instance with null store name, " +
                   "but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsNullStoreName1st() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit = new TableInputSplit();
        tableInputSplit = tableInputSplit.setKVStoreName(null);

        assertTrue("expected equals() method to return false when applied " +
                   "to a TableInputSplit instance with null store name, " +
                   "but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !tableInputSplit.equals(defaultTableInputSplit));
    }

    @Test
    public void testEqualsDifferentStoreNames() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit = new TableInputSplit();
        tableInputSplit = tableInputSplit.setKVStoreName("XXXXX");

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with different " +
                   "store names, but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !tableInputSplit.equals(defaultTableInputSplit));
    }

    @Test
    public void testEqualsNullLocations2nd() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setLocations(null);

        assertTrue("expected equals() method to return false when applied " +
                   "to a TableInputSplit instance with null locations, " +
                   "but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsNullLocations1st() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setLocations(null);

        assertTrue("expected equals() method to return false when applied " +
                   "to a TableInputSplit instance with null locations, " +
                   "but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !tableInputSplit.equals(defaultTableInputSplit));
    }

    @Test
    public void testEqualsLocationsSizeNotEqual() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final int defaultLen = defaultHelperHosts.length + 1;
        final List<String> defaultLocs = new ArrayList<>();
        for (String helperHost : defaultHelperHosts) {
            /* Strip off the ':port' suffix */
            final String[] hostPort = helperHost.trim().split(":");
            defaultLocs.add(hostPort[0]);
        }
        defaultLocs.add("XXXX");
        final String[] newLocs = defaultLocs.toArray(new String[defaultLen]);

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setLocations(newLocs);

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with locations of " +
                   "different sizes, but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsLocationsNotContainsAll() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final List<String> defaultLocs = new ArrayList<>();
        for (String helperHost : defaultHelperHosts) {
            /* Strip off the ':port' suffix */
            final String[] hostPort = helperHost.trim().split(":");
            defaultLocs.add(hostPort[0]);
        }
        defaultLocs.set(defaultHelperHosts.length - 1, "XXXX");
        final String[] newLocs =
            defaultLocs.toArray(new String[defaultHelperHosts.length]);

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setLocations(newLocs);

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with locations having the " +
                   "same number of elements where one locations set does " +
                   "not contain all elements from the other locations set, " +
                   "but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsNullKVHelperHosts2nd() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setKVHelperHosts(null);

        assertTrue("expected equals() method to return false when applied " +
                   "to a TableInputSplit instance with null kvHelperHosts, " +
                   "but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsNullKVHelperHosts1st() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setKVHelperHosts(null);

        assertTrue("expected equals() method to return false when applied " +
                   "to a TableInputSplit instance with null kvHelperHosts, " +
                   "but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !tableInputSplit.equals(defaultTableInputSplit));
    }

    @Test
    public void testEqualsKVHelperHostsSizeNotEqual() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final int defaultLen = defaultHelperHosts.length + 1;
        final List<String> defaultHosts =
            new ArrayList<>(Arrays.asList(defaultHelperHosts));
        defaultHosts.add("XXXX:7000");
        final String[] newHosts = defaultHosts.toArray(new String[defaultLen]);

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setKVHelperHosts(newHosts);

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with kvHelperHosts of " +
                   "different sizes, but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsKVHelperHostsNotContainsAll() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        final List<String> defaultHosts =
            new ArrayList<>(Arrays.asList(defaultHelperHosts));
        defaultHosts.set(defaultHelperHosts.length - 1, "XXXX:7000");
        final String[] newHosts =
            defaultHosts.toArray(new String[defaultHelperHosts.length]);

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setKVHelperHosts(newHosts);

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with kvHelperHosts having " +
                   "the same number of elements where one kvHelperHosts " +
                   "set does not contain all elements from the other " +
                   "kvHelperHosts set, but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsNullTableName() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setTableName(null);

        assertTrue("expected equals() method to return false when applied " +
                   "to a TableInputSplit instance with null table name, " +
                   "but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsDifferentTableNames() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setTableName("XXXXX");

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with different " +
                   "table names, but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !tableInputSplit.equals(defaultTableInputSplit));
    }

    @Test
    public void testEqualsNullPrimaryKeyProperty() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setPrimaryKeyProperty(null);

        assertTrue("expected equals() method to return false when applied " +
                   "to a TableInputSplit instance with null primary key " +
                   "properties, but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsDifferentPrimaryKeyProperty() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit = tableInputSplit.setPrimaryKeyProperty("XXXXX");

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with different " +
                   "primary key properties, but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !tableInputSplit.equals(defaultTableInputSplit));
    }

    @Test
    public void testEqualsNullFieldRangeProperty() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit1 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        TableInputSplit tableInputSplit2 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit1 = tableInputSplit1.setFieldRangeProperty("XXXX");
        tableInputSplit2 = tableInputSplit2.setFieldRangeProperty(null);
        assertTrue("expected equals() method to return false when applied " +
                   "to a TableInputSplit instance with null field range " +
                   "properties, but true was returned [split 1: " +
                   tableInputSplit1 + " not equal to " +
                   "split 2: " + tableInputSplit2 + "]",
                   !tableInputSplit1.equals(tableInputSplit2));
    }

    @Test
    public void testEqualsDifferentFieldRangeProperty() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit1 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        TableInputSplit tableInputSplit2 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit1 = tableInputSplit1.setFieldRangeProperty("XXXX");
        tableInputSplit2 = tableInputSplit2.setFieldRangeProperty("YYYY");

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with different " +
                   "field range properties, but true was returned [split 1: " +
                   tableInputSplit1 + " not equal to " +
                   "split 2: " + tableInputSplit2 + "]",
                   !tableInputSplit1.equals(tableInputSplit2));
    }

    @Test
    public void testEqualsDifferentQueryBy() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit =
            tableInputSplit.setQueryInfo(
                defaultTableInputSplit.getQueryBy() + 1, "XXXX whereClause");

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with different " +
                   "queryBy values, but true was returned [split 1: " +
                   defaultTableInputSplit + " not equal to " +
                   "split 2: " + tableInputSplit + "]",
                   !defaultTableInputSplit.equals(tableInputSplit));
    }

    @Test
    public void testEqualsShardSetsNotNull() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit1 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        TableInputSplit tableInputSplit2 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);

        final Set<RepGroupId> shardSet1 = new HashSet<RepGroupId>();
        for (int i = 0; i < 3; i++) {
            shardSet1.add(new RepGroupId(i));
        }
        final Set<RepGroupId> shardSet2 = new HashSet<RepGroupId>();
        for (int i = 0; i < 3; i++) {
            final int id = (i == shardSet1.size() - 1 ? i + 1 : i);
            shardSet2.add(new RepGroupId(id));
        }
        tableInputSplit1 = tableInputSplit1.setShardSet(shardSet1);
        tableInputSplit2 = tableInputSplit2.setShardSet(shardSet2);

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with different " +
                   "shard sets, but true was returned [split 1: " +
                   tableInputSplit1 + " not equal to " +
                   "split 2: " + tableInputSplit2 + "]",
                   !tableInputSplit1.equals(tableInputSplit2));
    }

    @Test
    public void testEqualsShardSetsOneNull() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit1 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        TableInputSplit tableInputSplit2 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);

        final Set<RepGroupId> shardSet1 = null;
        final Set<RepGroupId> shardSet2 = new HashSet<RepGroupId>();
        for (int i = 0; i < 3; i++) {
            shardSet2.add(new RepGroupId(i));
        }
        tableInputSplit1 = tableInputSplit1.setShardSet(shardSet1);
        tableInputSplit2 = tableInputSplit2.setShardSet(shardSet2);

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with one null shard set " +
                   "and one non-null shard set, but true was returned " +
                   "[split 1: " + tableInputSplit1 + " not equal to " +
                   "split 2: " + tableInputSplit2 + "]",
                   !tableInputSplit1.equals(tableInputSplit2));
    }

    @Test
    public void testEqualsPartitionSetsNotNull() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit1 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        TableInputSplit tableInputSplit2 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);

        final int nSets = 3;
        final int nPartitions = 3;
        final List<Set<Integer>> partitionSets1 = null;
        final List<Set<Integer>> partitionSets2 =
                                     new ArrayList<Set<Integer>>();
        for (int i = 0; i < nSets; i++) {
            final Set<Integer> partitionSet = new HashSet<>();
            for (int j = 0; j < nPartitions; j++) {
                final int id = (j == nPartitions - 1 ? j + 1 : j);
                partitionSet.add(id);
            }
            partitionSets2.add(partitionSet);
        }
        tableInputSplit1 = tableInputSplit1.setPartitionSets(partitionSets1);
        tableInputSplit2 = tableInputSplit2.setPartitionSets(partitionSets2);

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with different " +
                   "partiton sets, but true was returned [split 1: " +
                   tableInputSplit1 + " not equal to " +
                   "split 2: " + tableInputSplit2 + "]",
                   !tableInputSplit1.equals(tableInputSplit2));
    }

    @Test
    public void testEqualsPartitionSetsOneNull() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit1 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        TableInputSplit tableInputSplit2 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);

        final int nSets = 3;
        final int nPartitions = 3;
        final List<Set<Integer>> partitionSets1 =
                                     new ArrayList<Set<Integer>>();
        for (int i = 0; i < nSets; i++) {
            final Set<Integer> partitionSet = new HashSet<>();
            for (int j = 0; j < nPartitions; j++) {
                partitionSet.add(j);
            }
            partitionSets1.add(partitionSet);
        }
        final List<Set<Integer>> partitionSets2 =
                                     new ArrayList<Set<Integer>>();
        for (int i = 0; i < nSets; i++) {
            final Set<Integer> partitionSet = new HashSet<>();
            for (int j = 0; j < nPartitions; j++) {
                final int id = (j == nPartitions - 1 ? j + 1 : j);
                partitionSet.add(id);
            }
            partitionSets2.add(partitionSet);
        }
        tableInputSplit1 = tableInputSplit1.setPartitionSets(partitionSets1);
        tableInputSplit2 = tableInputSplit2.setPartitionSets(partitionSets2);

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with different " +
                   "partiton sets, but true was returned [split 1: " +
                   tableInputSplit1 + " not equal to " +
                   "split 2: " + tableInputSplit2 + "]",
                   !tableInputSplit1.equals(tableInputSplit2));
    }

    @Test
    public void testEqualsDifferentWhereClause() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit1 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        TableInputSplit tableInputSplit2 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit1 =
            tableInputSplit1.setQueryInfo(
                TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS,
                "XXXX whereClause");
        tableInputSplit2 =
            tableInputSplit2.setQueryInfo(
                TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS,
                "YYY whereClause");

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with different " +
                   "where clauses, but true was returned [split 1: " +
                   tableInputSplit1 + " not equal to " +
                   "split 2: " + tableInputSplit2 + "]",
                   !tableInputSplit1.equals(tableInputSplit2));
    }

    @Test
    public void testEqualsOneWhereClauseNull() throws Exception {

        final String methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        TableInputSplit tableInputSplit1 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        TableInputSplit tableInputSplit2 =
            HadoopTableIntegrationTest.createTableInputSplit(
                              defaultTableJobConf);
        tableInputSplit1 =
            tableInputSplit1.setQueryInfo(
                TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS, null);
        tableInputSplit2 =
            tableInputSplit2.setQueryInfo(
                TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS,
                "YYY whereClause");

        assertTrue("expected equals() method to return false when applied " +
                   "to TableInputSplit instances with one null where clause " +
                   "and one non-null where clause, but true was returned " +
                   "[split 1: " + tableInputSplit1 + " not equal to " +
                   "split 2: " + tableInputSplit2 + "]",
                   !tableInputSplit1.equals(tableInputSplit2));
    }
}
