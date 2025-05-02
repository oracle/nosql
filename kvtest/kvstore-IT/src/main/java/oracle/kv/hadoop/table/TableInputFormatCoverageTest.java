/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.hadoop.table;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KVSecurityConstants;
import oracle.kv.ParamConstant;
import oracle.kv.PasswordCredentials;
import oracle.kv.hadoop.table.TableInputFormatBase.TopologyLocatorWrapper;
import oracle.kv.impl.security.filestore.FileStore;
import oracle.kv.impl.security.filestore.FileStoreManager;
import oracle.kv.impl.security.wallet.WalletManager;
import oracle.kv.impl.security.wallet.WalletStore;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepGroupMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ExternalDataSourceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests that provide test coverage for the <code>TableInputFormat</code>
 * class from the <code>oracle.kv.hadoop.table</code> package.
 */
public class TableInputFormatCoverageTest extends HadoopTableCoverageTestBase {

    private boolean deleteUserSecurityDir = false;
    private String methodName;

    @BeforeClass
    public static void staticSetUp() throws Exception {

        THIS_CLASS = TableInputFormatCoverageTest.class;
        THIS_CLASS_NAME = THIS_CLASS.getSimpleName();
        HadoopTableCoverageTestBase.staticSetUp();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        /*
         * If the user security directory does not exist before the test
         * runs, then check in the tearDown method to see if that directory
         * was created by the test and, of so, then delete that directory.
         */
        if (!(new File(TableInputFormatBase.USER_SECURITY_DIR).exists())) {
            deleteUserSecurityDir = true;
        }

        methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        final File userSecurityDirFd =
                       new File(TableInputFormatBase.USER_SECURITY_DIR);
        if (deleteUserSecurityDir && userSecurityDirFd.exists()) {
            final String[] names = userSecurityDirFd.list();
            for (String name : names) {
                final File nameFd = new File(userSecurityDirFd, name);
                nameFd.delete();
            }
            userSecurityDirFd.delete();
        }
    }


    /* --- Tests that exercise miscellaneous accessor methods --- */


    @SuppressWarnings("deprecation")
    @Test
    public void testSetterMethods() throws Exception {

        final Configuration conf =
            HadoopTableIntegrationTest.getBasicHadoopConfiguration(
                HadoopTableCoverageTestBase.defaultTableName);
        /*
         * Set expected items that need to be used by both the mocks
         * created here, as well as by the test(s) of the corresponding
         * setter method(s) that follow below.
         */
        final int expectedRepFactor = 3;
        final int expectedNPartions = 10;

        /* For test 1. */
        final String expectedStoreName = methodName + "-store-name";
        conf.set(ParamConstant.KVSTORE_NAME.getName(), expectedStoreName);

        /* For test 18. */
        final String[] defHelperHostsArray =
            HadoopTableCoverageTestBase.defaultHelperHosts;
        final List<String> expectedHelperHostsList = new ArrayList<>();
        for (int i = 0; i < defHelperHostsArray.length; i++) {
            final String[] hostPort = defHelperHostsArray[i].trim().split(":");
            int newPort = 5000 + i;
            try {
                newPort = Integer.parseInt(hostPort[1]);
            } catch (NumberFormatException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
            newPort = newPort + 1000;
            expectedHelperHostsList.add(
                hostPort[0] + "_NoSqlNode_" + i + ":" + newPort);
        }
        final int nExpectedHelperHosts = expectedHelperHostsList.size();
        final String[] expectedHelperHosts =
            expectedHelperHostsList.toArray(new String[nExpectedHelperHosts]);

        final StringBuilder expectedHelperHostsBuf =
            new StringBuilder(expectedHelperHosts[0]);
        for (int i = 1; i < expectedHelperHosts.length; i++) {
            expectedHelperHostsBuf.append("," + expectedHelperHosts[i]);
        }
        final String expectedHelperHostsStr =
            expectedHelperHostsBuf.toString();
        conf.set(ParamConstant.KVSTORE_NODES.getName(),
                 expectedHelperHostsStr);

        final TopologyLocatorWrapper mockedTopologyLocator =
            createMock(TopologyLocatorWrapper.class);
        final Topology mockedTopology = createMock(Topology.class);
        final RepGroupMap mockedRepGroupMap = createMock(RepGroupMap.class);
        final DatacenterMap mockedDatacenterMap =
            createMock(DatacenterMap.class);
        final PartitionMap mockedPartitionMap =
            createMock(PartitionMap.class);
        final Datacenter mockedDatacenter = createMock(Datacenter.class);

        final List<Datacenter> dcList = new ArrayList<>();
        dcList.add(mockedDatacenter);

        expect(mockedTopologyLocator.get(
            expectedHelperHosts, 0, null, expectedStoreName, null))
                .andReturn(mockedTopology).anyTimes();

        expect(mockedTopology.getRepGroupMap())
            .andReturn(mockedRepGroupMap).anyTimes();

        expect(mockedTopology.getDatacenterMap())
            .andReturn(mockedDatacenterMap).anyTimes();

        expect(mockedTopology.getPartitionMap())
            .andReturn(mockedPartitionMap).anyTimes();

        expect(mockedTopology.getRepGroupId(anyObject(PartitionId.class)))
            .andReturn(new RepGroupId(1)).anyTimes();

        expect(mockedTopology.getRepGroupIds())
            .andReturn(defaultShardSet).anyTimes();

        expect(mockedRepGroupMap.size())
            .andReturn(defaultShardSet.size()).anyTimes();

        expect(mockedDatacenterMap.getAll())
            .andReturn(dcList).anyTimes();

        expect(mockedDatacenter.getDatacenterType())
            .andReturn(DatacenterType.PRIMARY).anyTimes();

        expect(mockedDatacenter.getRepFactor())
            .andReturn(expectedRepFactor).anyTimes();

        expect(mockedPartitionMap.getNPartitions())
            .andReturn(expectedNPartions).anyTimes();

        replay(mockedTopologyLocator);
        replay(mockedTopology);
        replay(mockedRepGroupMap);
        replay(mockedDatacenterMap);
        replay(mockedDatacenter);
        replay(mockedPartitionMap);

        final TableInputFormat tableInputFormat =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        TableInputFormat.clearLocalKVSecurity(); /* reset */

        /* 1. Set the store name and verify via the splits. */
        TableInputFormat.setKVStoreName(expectedStoreName);
        List<InputSplit> splits =
            tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String name = ((TableInputSplit) split).getKVStoreName();
            assertTrue("TableInputFormat.setKVStoreName failed to set " +
                       "store name to expected value [expected=" +
                       expectedStoreName + ", split.getKVStoreName returned " +
                       name + "]", expectedStoreName.equals(name));
        }

        /* 2. Set ABSOLUTE Consistency and verify via the splits. */
        Consistency expectedConsistency = Consistency.ABSOLUTE;
        String expectedConsistencyStr = expectedConsistency.toString();
        String expectedConsistencyProp = "ABSOLUTE";
        TableInputFormat.setConsistency(expectedConsistency);
        conf.set(ParamConstant.CONSISTENCY.getName(), expectedConsistencyProp);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String cStr =
                (((TableInputSplit) split).getConsistency()).toString();
            assertTrue("TableInputFormat.setConsistency failed to set " +
                       "consistency to expected value [expected=" +
                       expectedConsistencyStr + ", split.getConsistency " +
                       "returned " + cStr + "]",
                       expectedConsistencyStr.equals(cStr));
        }

        /* 3. Set NONE_REQUIRED Consistency and verify via the splits. */
        expectedConsistency = Consistency.NONE_REQUIRED;
        expectedConsistencyStr = expectedConsistency.toString();
        expectedConsistencyProp = "NONE_REQUIRED";
        TableInputFormat.setConsistency(expectedConsistency);
        conf.set(ParamConstant.CONSISTENCY.getName(), expectedConsistencyProp);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String cStr =
                (((TableInputSplit) split).getConsistency()).toString();
            assertTrue("TableInputFormat.setConsistency failed to set " +
                       "consistency to expected value [expected=" +
                       expectedConsistencyStr + ", split.getConsistency " +
                       "returned " + cStr + "]",
                       expectedConsistencyStr.equals(cStr));
        }

        /* 4. Set Consistency to nulland verify via the splits. */
        expectedConsistency = null;
        expectedConsistencyStr = "null";
        TableInputFormat.setConsistency(expectedConsistency);
        conf.unset(ParamConstant.CONSISTENCY.getName());
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final Consistency consistencyVal =
                (((TableInputSplit) split).getConsistency());
            assertNull("TableInputFormat.setConsistency failed to set " +
                       "consistency to the expected null value, but " +
                       "split.getConsistency returned " + consistencyVal + "]",
                       consistencyVal);
        }

        /* 5. Set NONE_REQUIRED_NO_MASTER Consistency and verify exception. */
        expectedConsistency = Consistency.NONE_REQUIRED_NO_MASTER;
        expectedConsistencyStr = expectedConsistency.toString();
        try {
            TableInputFormat.setConsistency(expectedConsistency);
            fail("expected IllegalArgumentException because " +
                 expectedConsistencyStr + " was input to " +
                 "TableInputFormat.setConsistency, but the expected " +
                 "exception was not thrown");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* 6. Set max requests and verify via the splits. */
        final int expectedMaxRequests =
            HadoopTableCoverageTestBase.maxRequests + 1;
        final String expectedMaxRequestsStr =
            String.valueOf(expectedMaxRequests);
        TableInputFormat.setMaxRequests(expectedMaxRequests);
        conf.set(ParamConstant.MAX_REQUESTS.getName(), expectedMaxRequestsStr);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final int maxRqsts =
                ((TableInputSplit) split).getMaxRequests();
            assertTrue("TableInputFormat.setMaxRequests failed to set " +
                       "max requests to expected value [expected=" +
                       expectedMaxRequests + ", split.getMaxRequests " +
                       "returned " + maxRqsts + "]",
                       expectedMaxRequests == maxRqsts);
        }

        /* 7. Set batch size and verify via the splits. */
        final int expectedBatchSize =
            HadoopTableCoverageTestBase.batchSize + 1;
        final String expectedBatchSizeStr = String.valueOf(expectedBatchSize);
        TableInputFormat.setBatchSize(expectedBatchSize);
        conf.set(ParamConstant.BATCH_SIZE.getName(), expectedBatchSizeStr);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final int batchSz =
                ((TableInputSplit) split).getBatchSize();
            assertTrue("TableInputFormat.setBatchSize failed to set " +
                       "batch size to expected value [expected=" +
                       expectedBatchSize + ", split.getBatchSize " +
                       "returned " + batchSz + "]",
                       expectedBatchSize == batchSz);
        }

        /* 8. Set max batches and verify via the splits. */
        final int expectedMaxBatches =
            HadoopTableCoverageTestBase.maxBatches + 1;
        final String expectedMaxBatchesStr =
            String.valueOf(expectedMaxBatches);
        TableInputFormat.setMaxBatches(expectedMaxBatches);
        conf.set(ParamConstant.MAX_BATCHES.getName(), expectedMaxBatchesStr);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final int mxBatches =
                ((TableInputSplit) split).getMaxBatches();
            assertTrue("TableInputFormat.setMaxBatches failed to set " +
                       "max batches to expected value [expected=" +
                       expectedMaxBatches + ", split.getMaxBatches " +
                       "returned " + mxBatches + "]",
                       expectedMaxBatches == mxBatches);
        }

        /* 9. Set timeout unit and verify via the splits. */
        final String expectedTimeoutUnitStr = "SECONDS";
        final TimeUnit expectedTimeoutUnit =
            TimeUnit.valueOf(expectedTimeoutUnitStr);
        TableInputFormat.setTimeoutUnit(expectedTimeoutUnit);
        conf.set(ParamConstant.TIMEOUT_UNIT.getName(), expectedTimeoutUnitStr);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final TimeUnit timeoutUnit =
                ((TableInputSplit) split).getTimeoutUnit();
            assertTrue("TableInputFormat.setTimeoutUnit failed to set " +
                       "the timeout unit to expected value [expected=" +
                       expectedTimeoutUnit + ", split.getTimeoutUnit " +
                       "returned " + timeoutUnit + "]",
                       expectedTimeoutUnit.equals(timeoutUnit));
        }

        /* 10. Set timeout and verify via the splits. */
        final long newTimeout = 5000L;
        final String newTimeoutStr = String.valueOf(newTimeout);
        final long expectedTimeout = 5000L / 1000L; /* milliseconds */
        TableInputFormat.setTimeout(newTimeout);
        conf.set(ParamConstant.TIMEOUT.getName(), newTimeoutStr);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final long timeoutVal =
                ((TableInputSplit) split).getTimeout();
            assertTrue("TableInputFormat.setTimeout failed to set " +
                       "timeout to expected value [expected=" +
                       expectedTimeout + ", split.getTimeout " +
                       "returned " + timeoutVal + "]",
                       expectedTimeout == timeoutVal);
        }

        /* 11. Set null timeout unit and verify IllegalArgumentException. */
        final TimeUnit newTimeoutUnit = null;
        try {
            TableInputFormat.setTimeoutUnit(newTimeoutUnit);
            fail("expected IllegalArgumentException because null was input " +
                 "to TableInputFormat.setTimeoutUnit when the current " +
                 "timeout value is non-zero, but the expected exception " +
                 "was not thrown");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* 12. Set FORWARD Direction and verify via the splits. */
        Direction expectedDirection = Direction.FORWARD;
        String expectedDirectionStr = expectedDirection.toString();
        TableInputFormat.setDirection(expectedDirection);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String directionStr =
                (((TableInputSplit) split).getDirection()).toString();
            assertTrue("TableInputFormat.setDirection failed to set " +
                       "direction to expected value [expected=" +
                       expectedDirectionStr + ", split.getDirection " +
                       "returned " + directionStr + "]",
                       expectedDirectionStr.equals(directionStr));
        }

        /* 13. Set REVERSE Direction and verify via the splits. */
        expectedDirection = Direction.REVERSE;
        expectedDirectionStr = expectedDirection.toString();
        TableInputFormat.setDirection(expectedDirection);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String directionStr =
                (((TableInputSplit) split).getDirection()).toString();
            assertTrue("TableInputFormat.setDirection failed to set " +
                       "direction to expected value [expected=" +
                       expectedDirectionStr + ", split.getDirection " +
                       "returned " + directionStr + "]",
                       expectedDirectionStr.equals(directionStr));
        }

        /* 14. Set UNORDERED Direction and verify via the splits. */
        expectedDirection = Direction.UNORDERED;
        expectedDirectionStr = expectedDirection.toString();
        TableInputFormat.setDirection(expectedDirection);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String directionStr =
                (((TableInputSplit) split).getDirection()).toString();
            assertTrue("TableInputFormat.setDirection failed to set " +
                       "direction to expected value [expected=" +
                       expectedDirectionStr + ", split.getDirection " +
                       "returned " + directionStr + "]",
                       expectedDirectionStr.equals(directionStr));
        }

        /* 15. Set the table name and verify via the splits. */
        final String expectedTableName = methodName + "-table-name";
        TableInputFormat.setTableName(expectedTableName);
        conf.set(ParamConstant.TABLE_NAME.getName(), expectedTableName);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String name = ((TableInputSplit) split).getTableName();
            assertTrue("TableInputFormat.setTableName failed to set " +
                       "table name to expected value [expected=" +
                       expectedTableName + ", split.getTableName returned " +
                       name + "]", expectedTableName.equals(name));
        }

        /* 16. Set the primary key property and verify via the splits. */
        final String expectedPrimaryKeyProp =
            "{\"type\":\"truck\", \"make\":\"Chrysler\"}";
        TableInputFormat.setPrimaryKeyProperty(expectedPrimaryKeyProp);
        conf.set(ParamConstant.PRIMARY_KEY.getName(), expectedPrimaryKeyProp);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String prop =
                ((TableInputSplit) split).getPrimaryKeyProperty();
            assertTrue("TableInputFormat.setPrimaryKeyProperty failed " +
                       "to set primary key property to expected value " +
                       "[expected=" + expectedPrimaryKeyProp + ", " +
                       "split.getPrimaryKeyProperty returned " +
                       prop + "]", expectedPrimaryKeyProp.equals(prop));
        }

        /* 17. Set the field range property and verify via the splits. */
        final String expectedFieldRangeProp =
           "{\"name\":\"make\",\"start\":\"Chrysler\",\"startInclusive\":" +
           "false,\"end\":\"Ford\",\"endInclusive\":true}";
        TableInputFormat.setFieldRangeProperty(expectedFieldRangeProp);
        conf.set(ParamConstant.FIELD_RANGE.getName(), expectedFieldRangeProp);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String prop =
                ((TableInputSplit) split).getFieldRangeProperty();
            assertTrue("TableInputFormat.setFieldRangeProperty failed " +
                       "to set field range property to expected value " +
                       "[expected=" + expectedFieldRangeProp + ", " +
                       "split.getFieldRangeProperty returned " +
                       prop + "]", expectedFieldRangeProp.equals(prop));
        }

        /* 18. Set helper hosts and verify via the splits. */
        TableInputFormat.setKVHadoopHosts(expectedHelperHosts);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String[] helperHostsFromSplit =
                ((TableInputSplit) split).getKVHelperHosts();
            final List<String> helperHostsFromSplitList =
                Arrays.asList(helperHostsFromSplit);
            assertTrue("TableInputFormat.setKVHadoopHosts failed to set " +
                       "the helper hosts to expected value [expected=" +
                       expectedHelperHostsList + ", getKVHelperHosts " +
                       "returned " + helperHostsFromSplitList + "]",
                       expectedHelperHostsList.equals(
                                                   helperHostsFromSplitList));
        }

        /* 19. Set hadoop hosts and verify via the splits. */
        final List<String> expectedHadoopHostsList = new ArrayList<>();
        for (int i = 0; i < defHelperHostsArray.length; i++) {
            final String[] hostPort = defHelperHostsArray[i].trim().split(":");
            int newPort = 5000 + i;
            try {
                newPort = Integer.parseInt(hostPort[1]);
            } catch (NumberFormatException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
            newPort = newPort + 1000;
            expectedHadoopHostsList.add(
                hostPort[0] + "_DataNode_" + i + ":" + newPort);
        }
        final int nExpectedHadoopHosts = expectedHadoopHostsList.size();
        final String[] expectedHadoopHosts =
            expectedHadoopHostsList.toArray(new String[nExpectedHadoopHosts]);

        final StringBuilder expectedHadoopHostsBuf =
            new StringBuilder(expectedHadoopHosts[0]);
        for (int i = 1; i < expectedHadoopHosts.length; i++) {
            expectedHadoopHostsBuf.append("," + expectedHadoopHosts[i]);
        }
        final String expectedHadoopHostsStr =
            expectedHadoopHostsBuf.toString();
        conf.set(ParamConstant.KVHADOOP_NODES.getName(),
                 expectedHadoopHostsStr);
        TableInputFormat.setKVHadoopHosts(expectedHadoopHosts);
        conf.set(ParamConstant.KVHADOOP_NODES.getName(),
                 expectedHadoopHostsStr);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final String[] hadoopHostsFromSplit =
                ((TableInputSplit) split).getLocations();
            final List<String> hadoopHostsFromSplitList =
                Arrays.asList(hadoopHostsFromSplit);
            assertTrue("TableInputFormat.setKVHadoopHosts failed to set " +
                       "the hadoop hosts to expected value [expected=" +
                       expectedHadoopHostsList + ", getLocations " +
                       "returned " + hadoopHostsFromSplitList + "]",
                       expectedHadoopHostsList.equals(
                                                   hadoopHostsFromSplitList));
        }

        /* 20. Set the query info and verify via the splits. */
        final int expectedQueryBy = TableInputSplit.QUERY_BY_ONQL_SHARDS;
        final String expectedWhereClause = "make = 'Chrysler'";
        final int newPartitionId = 17;
        tableInputFormat.setQueryInfo(
            expectedQueryBy, expectedWhereClause, newPartitionId);
        splits = tableInputFormat.getSplits(new TableTaskAttemptContext(conf));
        for (InputSplit split : splits) {
            final int queryBy =
                ((TableInputSplit) split).getQueryBy();
            assertTrue("TableInputFormat.setQueryInfo failed " +
                       "to set the queryBy value to the expected value " +
                       "[expected=" + expectedQueryBy + ", " +
                       "split.getQueryBy returned " + queryBy + "]",
                       expectedQueryBy == queryBy);
            final String whereClause =
                ((TableInputSplit) split).getWhereClause();
            assertTrue("TableInputFormat.setQueryInfo failed " +
                       "to set the where clause to the expected value " +
                       "[expected=" + expectedWhereClause + ", " +
                       "split.getQueryBy returned " + whereClause + "]",
                       expectedWhereClause.equals(whereClause));
        }

    }

    @Test
    public void testSetKVSecurity() throws Exception {

        final String loginFile = HadoopTableCoverageTestBase.loginFileBaseDir;
        final String username = "test-user";
        final char[] pswd = {'t', 'e', 's', 't', '-', 'p', 's', 'w', 'd'};
        final PasswordCredentials userPasswordCredentials =
            new PasswordCredentials(username, pswd);
        final String trustFile = HadoopTableCoverageTestBase.trustFileBaseDir;

        /* Test fails if/when setKVSecurity throws an exception. */

        /* 1. Everything non-null */
        TableInputFormat.setKVSecurity(
            loginFile, userPasswordCredentials, trustFile);

        /* 2. setLocalKVSecurity branch: null loginFile */
        TableInputFormat.setKVSecurity(
            null, userPasswordCredentials, trustFile);

        /* 3. setLocalKVSecurity branch: null userPasswordCredentials */
        TableInputFormat.setKVSecurity(loginFile, null, trustFile);

        /* 4. setLocalKVSecurity branch: null trustFile */
        TableInputFormat.setKVSecurity(
            loginFile, userPasswordCredentials, null);

        /* 5. setLocalKVSecurity branch: loginFile not absolute */
        TableInputFormat.setKVSecurity(
            "notAbsoluteLoginFilePath", userPasswordCredentials, trustFile);

        /* 6. setLocalKVSecurity branch: trustFile not absolute */
        TableInputFormat.setKVSecurity(
            loginFile, userPasswordCredentials, "notAbsoluteTrustFilePath");
    }

    @Test
    public void testGetSplitsWhenTopologyLocatorCannotContactRNs()
                    throws Exception {

        final List<InputSplit> expectedSplitList = Collections.emptyList();

        final Configuration conf =
            HadoopTableIntegrationTest.getBasicHadoopConfiguration(
                HadoopTableCoverageTestBase.defaultTableName);
        final TableTaskAttemptContext ctx = new TableTaskAttemptContext(conf);
        final TableInputFormat tableInputFormat =
            HadoopTableIntegrationTest.getConfiguredTableInputFormat(conf);

        /*
         * The static localLoginFile field of TableInputFormat may have
         * been set to a non-null value by a previous unit test. Reset
         * that field (and the corresponding credentials and trust fields
         * to null.
         */
        TableInputFormat.clearLocalKVSecurity();

        assert expectedSplitList.equals(tableInputFormat.getSplits(ctx));
    }

    @Test
    public void testGetSplits() throws Exception {

        final Configuration conf =
            HadoopTableIntegrationTest.getBasicHadoopConfiguration(
                HadoopTableCoverageTestBase.defaultTableName);

        String[] helperHostsArray = new String[] { };
        final String helperHostsStr =
            conf.get(ParamConstant.KVSTORE_NODES.getName());
        if (helperHostsStr != null) {
            helperHostsArray = helperHostsStr.trim().split(",");
        }
        final String storeName =
            conf.get(ParamConstant.KVSTORE_NAME.getName());
        /* From HadoopTableIntegrationTest.getBasicHadoopConfiguration */
        final int expectedRepFactor = 3;
        final int expectedNPartions = 10;

        final TopologyLocatorWrapper mockedTopologyLocator =
            createMock(TopologyLocatorWrapper.class);
        final Topology mockedTopology = createMock(Topology.class);
        final RepGroupMap mockedRepGroupMap = createMock(RepGroupMap.class);
        final DatacenterMap mockedDatacenterMap =
            createMock(DatacenterMap.class);
        final PartitionMap mockedPartitionMap =
            createMock(PartitionMap.class);
        final Datacenter mockedDatacenter = createMock(Datacenter.class);

        final List<Datacenter> dcList = new ArrayList<>();
        dcList.add(mockedDatacenter);

        expect(mockedTopologyLocator.get(
            helperHostsArray, 0, null, storeName, null))
                .andReturn(mockedTopology).anyTimes();

        expect(mockedTopology.getRepGroupMap())
            .andReturn(mockedRepGroupMap).anyTimes();

        expect(mockedTopology.getDatacenterMap())
            .andReturn(mockedDatacenterMap).anyTimes();

        expect(mockedTopology.getPartitionMap())
            .andReturn(mockedPartitionMap).anyTimes();

        expect(mockedTopology.getRepGroupId(anyObject(PartitionId.class)))
            .andReturn(new RepGroupId(1)).anyTimes();

        expect(mockedTopology.getRepGroupIds())
            .andReturn(defaultShardSet).anyTimes();

        expect(mockedRepGroupMap.size())
            .andReturn(defaultShardSet.size()).anyTimes();

        expect(mockedDatacenterMap.getAll())
            .andReturn(dcList).anyTimes();

        expect(mockedDatacenter.getDatacenterType())
            .andReturn(DatacenterType.PRIMARY).anyTimes();

        expect(mockedDatacenter.getRepFactor())
            .andReturn(expectedRepFactor).anyTimes();

        expect(mockedPartitionMap.getNPartitions())
            .andReturn(expectedNPartions).anyTimes();

        replay(mockedTopologyLocator);
        replay(mockedTopology);
        replay(mockedRepGroupMap);
        replay(mockedDatacenterMap);
        replay(mockedDatacenter);
        replay(mockedPartitionMap);

        /*
         * Note: in the different test cases below, whenenver a new
         * TableInputFormat is created, the static localLoginFile field
         * of TableInputFormatBase class may have been set to a non-null
         * value by a previous unit test. To avoid unexpected exceptions
         * related to previous values set in that field, that field (as
         * well as the corresponding static credentials and trust fields)
         * should always be reset to null before calling that getSplits()
         * method on the new TableInputFormat.
         */

        /* 1. Default Configuration, TaskAttemptContext, TableInputFormat. */

        final TableInputFormat tableInputFormat1 =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx1 = new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        tableInputFormat1.getSplits(ctx1);

        /*
         * For the following cases, the Configuration created above is
         * changed, and a new TaskAttemptContext is created from that
         * changed Configuration; which is used to create a new
         * TableInputFormat on which to call the getSplits() method.
         * Each change made to the Configuration is intended to exercise
         * a specific branch of the initialParameters method that is
         * called by the getSplits() method of the new TableInputFormat.
         */

        /* 2. Non-null hadoop hosts branch. */

        final List<String> hadoopHostsList = new ArrayList<>();
        for (int i = 0; i < helperHostsArray.length; i++) {
            final String[] hostPort = helperHostsArray[i].trim().split(":");
            hadoopHostsList.add(hostPort[0] + "_DataNode_" + i);
        }
        final StringBuilder hadoopHostsBuf =
                                new StringBuilder(hadoopHostsList.get(0));
        for (int i = 1; i < hadoopHostsList.size(); i++) {
            hadoopHostsBuf.append("," + hadoopHostsList.get(i));
        }
        final String hadoopHostsStr = hadoopHostsBuf.toString();

        conf.set(ParamConstant.KVHADOOP_NODES.getName(), hadoopHostsStr);

        final TableInputFormat tableInputFormat2 =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx2 =
            new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        tableInputFormat2.getSplits(ctx2);

        /* 3. Non-null PrimaryKey property branch. */

        conf.set(
            ParamConstant.PRIMARY_KEY.getName(), defaultPrimaryKeyProperty);

        final TableInputFormat tableInputFormat3 =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx3 = new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        tableInputFormat3.getSplits(ctx3);

        /* 4. Non-null FieldRange property branch. */

        conf.set(
            ParamConstant.FIELD_RANGE.getName(), defaultFieldRangeProperty);

        final TableInputFormat tableInputFormat4 =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx4 = new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        tableInputFormat4.getSplits(ctx4);

        /* 5a. Non-null Consistency property branch. */

        conf.set(ParamConstant.CONSISTENCY.getName(), "NONE_REQUIRED");

        final TableInputFormat tableInputFormat5a =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx5a =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        tableInputFormat5a.getSplits(ctx5a);

        /* 5b. Invalid, non-null Consistency property. */

        conf.set(ParamConstant.CONSISTENCY.getName(),
                 Consistency.NONE_REQUIRED.toString());

        final TableInputFormat tableInputFormat5b =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx5b =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        try {
            tableInputFormat5b.getSplits(ctx5b);
            fail("expected ExternalDataSourceException because an invalid " +
                 "value [" + Consistency.NONE_REQUIRED.toString() + "] was " +
                 "specified for the " + ParamConstant.CONSISTENCY.getName() +
                 " property, but the expected exception was not thrown");
        } catch (ExternalDataSourceException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Reset Consistency property to a valid value for next cases. */
        conf.set(ParamConstant.CONSISTENCY.getName(), "NONE_REQUIRED");

        /* 6a. Non-null maxRequestsStr property branch. */
        final String notNumberStr = "not-a-number";

        conf.set(
            ParamConstant.MAX_REQUESTS.getName(), String.valueOf(maxRequests));

        final TableInputFormat tableInputFormat6a =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx6a =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        tableInputFormat6a.getSplits(ctx6a);

        /* 6b. Invalid, non-null maxRequestsStr property branch. */

        final TableInputFormat tableInputFormat6b =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        /*
         * Need to set the invalid value in conf -after- creating the
         * TableInputFormat above; because getConfiguredTableInputFormat()
         * will fail-fast and throw NumberFormatException before the
         * TaskAttemptContext can be created with the conf containing
         * the invalid MAX_REQUESTS below.
         */
        conf.set(ParamConstant.MAX_REQUESTS.getName(), notNumberStr);
        final TableTaskAttemptContext ctx6b =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        try {
            tableInputFormat6b.getSplits(ctx6b);
            fail("expected IllegalArgumentException because an invalid " +
                 "String value [" + notNumberStr + "] was specified for the " +
                 ParamConstant.MAX_REQUESTS.getName() + " property, but the " +
                 "expected exception was not thrown");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Reset maxRequests property to a valid value for next cases. */
        conf.set(ParamConstant.MAX_REQUESTS.getName(),
                 String.valueOf(maxRequests));

        /* 7a. Non-null batchSizeStr property branch. */

        conf.set(
            ParamConstant.BATCH_SIZE.getName(), String.valueOf(batchSize));

        final TableInputFormat tableInputFormat7a =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx7a =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        tableInputFormat7a.getSplits(ctx7a);

        /* 7b. Invalid, non-null batchSizeStr property branch. */

        final TableInputFormat tableInputFormat7b =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        /*
         * Need to set the invalid value in conf -after- creating the
         * TableInputFormat above; because getConfiguredTableInputFormat()
         * will fail-fast and throw NumberFormatException before the
         * TaskAttemptContext can be created with the conf containing
         * the invalid BATCH_SIZE below.
         */
        conf.set(ParamConstant.BATCH_SIZE.getName(), notNumberStr);
        final TableTaskAttemptContext ctx7b =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        try {
            tableInputFormat7b.getSplits(ctx7b);
            fail("expected IllegalArgumentException because an invalid " +
                 "String value [" + notNumberStr + "] was specified for the " +
                 ParamConstant.BATCH_SIZE.getName() + " property, but the " +
                 "expected exception was not thrown");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Reset batchSize property to a valid value for next cases. */
        conf.set(ParamConstant.BATCH_SIZE.getName(),
                 String.valueOf(batchSize));

        /* 8a. Non-null maxBatchesStr property branch. */

        conf.set(
            ParamConstant.MAX_BATCHES.getName(), String.valueOf(maxBatches));

        final TableInputFormat tableInputFormat8a =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx8a =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        tableInputFormat8a.getSplits(ctx8a);

        /* 8b. Invalid, non-null maxBatchesStr property branch. */

        final TableInputFormat tableInputFormat8b =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        /*
         * Need to set the invalid value in conf -after- creating the
         * TableInputFormat above; because getConfiguredTableInputFormat()
         * will fail-fast and throw NumberFormatException before the
         * TaskAttemptContext can be created with the conf containing
         * the invalid MAX_BATCHES below.
         */
        conf.set(ParamConstant.MAX_BATCHES.getName(), notNumberStr);
        final TableTaskAttemptContext ctx8b =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        try {
            tableInputFormat8b.getSplits(ctx8b);
            fail("expected IllegalArgumentException because an invalid " +
                 "String value [" + notNumberStr + "] was specified for the " +
                 ParamConstant.MAX_BATCHES.getName() + " property, but the " +
                 "expected exception was not thrown");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Reset maxBatches property to a valid value for next cases. */
        conf.set(ParamConstant.MAX_BATCHES.getName(),
                 String.valueOf(maxBatches));

        final String pswdStr = "test-pswd";

        /* 9. Non-null username and pswd property branch. */

        conf.set(KVSecurityConstants.AUTH_USERNAME_PROPERTY, userName);
        conf.set(ParamConstant.AUTH_USER_PWD_PROPERTY.getName(), pswdStr);

        final TableInputFormat tableInputFormat9 =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx9 =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */
        tableInputFormat9.getSplits(ctx9);

        /* Unset AUTH properties for next cases. */
        conf.unset(KVSecurityConstants.AUTH_USERNAME_PROPERTY);
        conf.unset(ParamConstant.AUTH_USER_PWD_PROPERTY.getName());


        /* For test cases 10, 11 & 12 below */
        final String walletDir = "testWalletDir";
        final File walletDirFd = new File(walletLoc, walletDir);
        final String walletFlnm = "cwallet.sso";
        final File walletFd = new File(walletDirFd, walletFlnm);
        boolean deleteWalletDir = false;
        boolean deleteWallet = false;

        final String pswdDir = "testPswdDir";
        final String pswdFlnm = "testPswd.txt";
        final File pswdDirFd = new File(walletLoc, pswdDir);
        boolean deletePswdDir = false;
        boolean deletePswd = false;

        /* 10. Non-null wallet location property branch. */

        if (!walletDirFd.exists()) {
            walletDirFd.mkdir();
            deleteWalletDir = true;
        }
        deleteWallet = walletFd.createNewFile();

        conf.set(KVSecurityConstants.AUTH_WALLET_PROPERTY,
                 walletDirFd.toString());

        final TableInputFormat tableInputFormat10 =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx10 =
                                          new TableTaskAttemptContext(conf);
        TableInputFormat.clearLocalKVSecurity(); /* reset */

        try {
            tableInputFormat10.getSplits(ctx10);

            fail("expected IOException because an empty wallet " +
                 "[" + walletFd + "] was specified for the " +
                 KVSecurityConstants.AUTH_WALLET_PROPERTY + " property, " +
                 "but the expected exception was not thrown");
        } catch (IOException e) /* CHECKSTYLE:OFF */ {
        } finally {

            /* Clean up files and directories that were created. */
            if (deleteWallet) {
                walletFd.delete();
            }
            if (deleteWalletDir) {
                final String[] names = walletDirFd.list();
                for (String name : names) {
                    final File nameFd = new File(walletDirFd, name);
                    nameFd.delete();
                }
                walletDirFd.delete();
            }
        } /* CHECKSTYLE:ON */

        /* Unset AUTH properties for next cases. */
        conf.unset(KVSecurityConstants.AUTH_WALLET_PROPERTY);

        /* For test cases 11 & 12 below */
        final char[] secret = pswdStr.toCharArray();
        File storeLocation = null;

        /* 11. Non-null username & pswd, plus non-empty FileStore branch. */

        storeLocation = new File(pswdDirFd, pswdFlnm);

        conf.set(KVSecurityConstants.AUTH_USERNAME_PROPERTY, userName);
        try {
            if (!pswdDirFd.exists()) {
                pswdDirFd.mkdir();
                deletePswdDir = true;
            }

            /* Create a non-empty FileStore. */
            final FileStoreManager fileStoreMgr = new FileStoreManager();
            final FileStore fileStore =
                (FileStore) fileStoreMgr.getStoreHandle(storeLocation);
            assertTrue("failed to create non-empty FileStore",
                       fileStore.create(null));
            fileStore.setSecret(userName, secret);
            fileStore.save();
            deletePswd = fileStore.exists();

            conf.set(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                     storeLocation.toString());

            final TableInputFormat tableInputFormat11 =
                HadoopTableIntegrationTest
                   .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
            final TableTaskAttemptContext ctx11 =
                new TableTaskAttemptContext(conf);
            TableInputFormat.clearLocalKVSecurity(); /* reset */

            tableInputFormat11.getSplits(ctx11);

        } finally {

            /* Clean up files and directories that were created. */
            if (deletePswd) {
                storeLocation.delete();
            }
            if (deletePswdDir) {
                final String[] names = pswdDirFd.list();
                for (String name : names) {
                    final File nameFd = new File(pswdDirFd, name);
                    nameFd.delete();
                }
                pswdDirFd.delete();
            }

        } /* CHECKSTYLE:ON */

        /* Unset AUTH properties for next cases. */
        conf.unset(KVSecurityConstants.AUTH_USERNAME_PROPERTY);
        conf.unset(KVSecurityConstants.AUTH_PWDFILE_PROPERTY);


        /* 12. Non-null username & wallet, plus non-empty Wallet branch. */

        storeLocation = walletDirFd;
        deleteWalletDir = false;
        deleteWallet = false;

        conf.set(KVSecurityConstants.AUTH_USERNAME_PROPERTY, userName);
        try {
            if (!walletDirFd.exists()) {
                walletDirFd.mkdir();
                deleteWalletDir = true;
            }
            deleteWallet = walletFd.createNewFile();

            /* Create a non-empty WalletStore. */
            final WalletManager walletMgr = new WalletManager();
            final WalletStore walletStore =
                (WalletStore) walletMgr.getStoreHandle(storeLocation);
            assertTrue("failed to create non-empty WalletStore",
                       walletStore.create(null));
            walletStore.setSecret(userName, secret);
            walletStore.save();
            deleteWallet = walletStore.exists();

            conf.set(KVSecurityConstants.AUTH_WALLET_PROPERTY,
                     storeLocation.toString());

            final TableInputFormat tableInputFormat12 =
                HadoopTableIntegrationTest
                   .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
            final TableTaskAttemptContext ctx12 =
                new TableTaskAttemptContext(conf);
            TableInputFormat.clearLocalKVSecurity(); /* reset */

            tableInputFormat12.getSplits(ctx12);

        } finally {

            /* Clean up files and directories that were created. */
            if (deleteWallet) {
                storeLocation.delete();
            }
            if (deleteWalletDir) {
                final String[] names = walletDirFd.list();
                for (String name : names) {
                    final File nameFd = new File(walletDirFd, name);
                    nameFd.delete();
                }
                walletDirFd.delete();
            }

        }

        /* Unset AUTH properties for next cases. */
        conf.unset(KVSecurityConstants.AUTH_USERNAME_PROPERTY);
        conf.unset(KVSecurityConstants.AUTH_WALLET_PROPERTY);

        /* 13. Additional partitionId branches in getSplitInfo. */

        conf.set(
            ParamConstant.PRIMARY_KEY.getName(), defaultPrimaryKeyProperty);

        final TableInputFormat tableInputFormat13 =
            HadoopTableIntegrationTest
                .getConfiguredTableInputFormat(conf, mockedTopologyLocator);
        final TableTaskAttemptContext ctx13 =
            new TableTaskAttemptContext(conf);

        TableInputFormat.clearLocalKVSecurity(); /* reset */

        tableInputFormat13.setQueryInfo(
            TableInputSplit.QUERY_BY_PRIMARY_SINGLE_PARTITION,
            HadoopTableCoverageTestBase.defaultWhereClause, 1);
        tableInputFormat13.getSplits(ctx13);

        tableInputFormat13.setQueryInfo(
            TableInputSplit.QUERY_BY_ONQL_SINGLE_PARTITION,
            HadoopTableCoverageTestBase.defaultWhereClause, null);
        tableInputFormat13.getSplits(ctx13);

        tableInputFormat13.setQueryInfo(
            TableInputSplit.QUERY_BY_ONQL_SHARDS,
            HadoopTableCoverageTestBase.defaultWhereClause, 1);
        tableInputFormat13.getSplits(ctx13);

        /* 14. Exercise branches that handle un-initialized fields. */

        final Configuration emptyConf = new Configuration();
        final TableTaskAttemptContext ctx14 =
            new TableTaskAttemptContext(emptyConf);
        final TableInputFormat tableInputFormat14 = new TableInputFormat();

        TableInputFormat.setKVStoreName(null);
        TableInputFormat.setKVHelperHosts(null);
        TableInputFormat.setTableName(null);
        TableInputFormat.clearLocalKVSecurity();

        /* 14a. For branch that handles null store name. */
        try {
            tableInputFormat14.getSplits(ctx14);
            fail("expected IllegalArgumentException because of a null " +
                 "store name, but the expected exception was not thrown");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
            TableInputFormat.setKVStoreName(methodName + "-store-name");
        }

        /* 14b. For branch that handles null helper hosts. */
        try {
            tableInputFormat14.getSplits(ctx14);
            fail("expected IllegalArgumentException because of a null " +
                 "helper hosts array, but the expected exception was " +
                 "not thrown");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
            final String[] localhostHelperHosts =
                new String[] { "localhost:5000" };
            TableInputFormat.setKVHelperHosts(localhostHelperHosts);
        }

        /* 14c. For null table name. */
        try {
            tableInputFormat14.getSplits(ctx14);
            fail("expected IllegalArgumentException because of a null " +
                 "table name, but the expected exception was not thrown");
        } catch (IllegalArgumentException e) /* CHECKSTYLE:OFF */ {
        }
    }
}
