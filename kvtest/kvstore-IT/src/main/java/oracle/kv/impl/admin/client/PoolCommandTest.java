/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.SerialVersion;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;

import org.junit.Test;

public class PoolCommandTest extends TestBase {
    private final String command = "pool";
    private final CommandShell shell = createMock(CommandShell.class);
    private final CommandService cs = createMock(CommandService.class);

    private static final AuthContext NULL_AUTH = null;

    /* Convenience method shared by all the test cases that employ the
     * mocked objects.
     */
    private void doVerification() {

        verify(cs);
        verify(shell);
    }

    @Test
    public void testCreatePoolSubExecutePoolCreatedJsonMode()
        throws Exception {

        final PoolCommand.CreatePool subObj =
            new PoolCommand.CreatePool();
        final String nameFlag = "-name";
        final String poolName = "TEST_POOL";
        final boolean json = true;

        final String[] args = {command, nameFlag, poolName};

        ObjectNode returnValue = JsonUtils.createObjectNode();
        returnValue.put("pool_name", poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT).
            anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(new ArrayList<String>()).anyTimes();
        cs.addStorageNodePool(poolName, NULL_AUTH, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null)).
            anyTimes();
        expect(shell.getJson()).andReturn(json).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            JsonNode resultNode = JsonUtils.parseJsonNode(result);
            assertEquals(command + " create",
                resultNode.get(CommandJsonUtils.FIELD_OPERATION).
                    asText());
            assertEquals(5000,
                resultNode.get(CommandJsonUtils.FIELD_RETURN_CODE).
                    asInt());
            assertEquals("Operation ends successfully",
                resultNode.get(CommandJsonUtils.FIELD_DESCRIPTION).
                    asText());
            assertNull(resultNode.get(CommandJsonUtils.FIELD_CLEANUP_JOB));
            JsonNode valueNode = resultNode.get(
                CommandJsonUtils.FIELD_RETURN_VALUE);
            assertEquals(poolName, valueNode.get("pool_name").asText());
        } finally {
            doVerification();
        }
        final ShellCommandResult scr = subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getOperation(), "pool create");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        final ObjectNode retValue = scr.getReturnValue();
        assertEquals(retValue.get("poolName").asText(), "TEST_POOL");
    }

    @Test
    public void testRemovePoolSubExecutePoolRemovedJsonMode()
        throws Exception {

        final PoolCommand.RemovePool subObj =
            new PoolCommand.RemovePool();
        final String nameFlag = "-name";
        final String poolName = "TEST_POOL";
        final boolean json = true;

        final String[] args = {command, nameFlag, poolName};

        ObjectNode returnValue = JsonUtils.createObjectNode();
        returnValue.put("pool_name", poolName);

        /* Objects to be used with the mock objects */
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);
        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT).
            anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(poolList).anyTimes();
        cs.removeStorageNodePool(poolName, NULL_AUTH, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null)).
            anyTimes();
        expect(shell.getJson()).andReturn(json).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            JsonNode resultNode = JsonUtils.parseJsonNode(result);
            assertEquals(command + " remove",
                resultNode.get(CommandJsonUtils.FIELD_OPERATION).
                    asText());
            assertEquals(5000,
                resultNode.get(CommandJsonUtils.FIELD_RETURN_CODE).
                    asInt());
            assertEquals("Operation ends successfully",
                resultNode.get(CommandJsonUtils.FIELD_DESCRIPTION).
                    asText());
            assertNull(resultNode.get(CommandJsonUtils.FIELD_CLEANUP_JOB));
            JsonNode valueNode = resultNode.get(
                CommandJsonUtils.FIELD_RETURN_VALUE);
            assertEquals(poolName, valueNode.get("pool_name").asText());
        } finally {
            doVerification();
        }
        final ShellCommandResult scr = subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getOperation(), "pool remove");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        final ObjectNode retValue = scr.getReturnValue();
        assertEquals(retValue.get("poolName").asText(), "TEST_POOL");
    }

    @Test
    public void testClonePoolSubExecutePoolClonedJsonMode()
        throws Exception {

        final PoolCommand.ClonePool subObj = new PoolCommand.ClonePool();
        final String nameFlag = "-name";
        final String poolName = "CLONED_POOL";
        final String sourceFlag = "-from";
        final String sourceName = "TEST_POOL";
        final boolean json = true;

        final String[] args = {command, nameFlag, poolName, sourceFlag,
                               sourceName};

        ObjectNode returnValue = JsonUtils.createObjectNode();
        returnValue.put("pool_name", sourceName);

        /* Objects to be used with the mock objects */
        final List<String> poolList = new ArrayList<String>();
        poolList.add(sourceName);
        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT).
            anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(poolList).anyTimes();
        cs.cloneStorageNodePool(
            poolName, sourceName, NULL_AUTH, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getJson()).andReturn(json).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            JsonNode resultNode = JsonUtils.parseJsonNode(result);
            assertEquals(command + " clone",
                resultNode.get(CommandJsonUtils.FIELD_OPERATION).
                    asText());
            assertEquals(5000,
                resultNode.get(CommandJsonUtils.FIELD_RETURN_CODE).
                    asInt());
            assertEquals("Operation ends successfully",
                resultNode.get(CommandJsonUtils.FIELD_DESCRIPTION).
                    asText());
            assertNull(resultNode.get(CommandJsonUtils.FIELD_CLEANUP_JOB));
            JsonNode valueNode = resultNode.get(
                CommandJsonUtils.FIELD_RETURN_VALUE);
            assertEquals(poolName, valueNode.get("pool_name").asText());
        } finally {
            doVerification();
        }
        final ShellCommandResult scr = subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getOperation(), "pool clone");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        final ObjectNode retValue = scr.getReturnValue();
        assertEquals(retValue.get("poolName").asText(), "CLONED_POOL");
    }

    @Test
    public void testCloneNoExistingPoolSubExecutePoolClonedJsonMode()
        throws Exception {

        final PoolCommand.ClonePool subObj = new PoolCommand.ClonePool();
        final String nameFlag = "-name";
        final String poolName = "CLONED_POOL";
        final String sourceFlag = "-from";
        final String sourceName = "TEST_POOL";
        final boolean json = true;

        final String[] args = {command, nameFlag, poolName, sourceFlag,
                               sourceName};

        ObjectNode returnValue = JsonUtils.createObjectNode();
        returnValue.put("pool_name", sourceName);

        /* Objects to be used with the mock objects */
        final List<String> poolList = new ArrayList<String>();
        poolList.add("ANOTHER_POOL");
        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andReturn(json);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            JsonNode resultNode = JsonUtils.parseJsonNode(result);
            assertEquals(command + " clone",
                resultNode.get(CommandJsonUtils.FIELD_OPERATION).
                    asText());
            assertEquals(5200,
                resultNode.get(CommandJsonUtils.FIELD_RETURN_CODE).
                    asInt());
            assertEquals("Source pool does not exist: TEST_POOL",
                resultNode.get(CommandJsonUtils.FIELD_DESCRIPTION).
                    asText());
            assertTrue(resultNode.get(CommandJsonUtils.FIELD_CLEANUP_JOB).
                       isEmpty());
            assertNull(resultNode.get(CommandJsonUtils.FIELD_RETURN_VALUE));
        } finally {
            doVerification();
        }
    }

    @Test
    public void testJoinPoolSubExecutePoolJoinedJsonMode()
        throws Exception {

        final PoolCommand.JoinPool subObj =
            new PoolCommand.JoinPool();
        final String nameFlag = "-name";
        final String poolName = "TEST_POOL";
        final String snFlag = "-sn";
        final String snName = "sn1";
        final boolean json = true;

        final String[] args = {command, nameFlag, poolName, snFlag, snName};

        ObjectNode returnValue = JsonUtils.createObjectNode();
        returnValue.put("pool_name", poolName);

        /* Objects to be used with the mock objects */
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);
        final Topology testTopo = new Topology("test_topo");
        final Datacenter dc1 = Datacenter.newInstance("zn1", 1,
                                                      DatacenterType.PRIMARY,
                                                      false, false);
        testTopo.add(new StorageNode(dc1, "localhost", 1000));
        final StorageNodeId storageNodeId = new StorageNodeId(1);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(poolList).anyTimes();
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(testTopo).anyTimes();
        cs.addStorageNodeToPool(poolName, storageNodeId,
                                NULL_AUTH, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getJson()).andReturn(json).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            JsonNode resultNode = JsonUtils.parseJsonNode(result);
            assertEquals(command + " join",
                resultNode.get(CommandJsonUtils.FIELD_OPERATION).
                    asText());
            assertEquals(5000,
                resultNode.get(CommandJsonUtils.FIELD_RETURN_CODE).
                    asInt());
            assertEquals("Operation ends successfully",
                resultNode.get(CommandJsonUtils.FIELD_DESCRIPTION).
                    asText());
            assertNull(resultNode.get(CommandJsonUtils.FIELD_CLEANUP_JOB));
            JsonNode valueNode = resultNode.get(
                CommandJsonUtils.FIELD_RETURN_VALUE);
            assertEquals(poolName, valueNode.get("pool_name").asText());
        } finally {
            doVerification();
        }
        final ShellCommandResult scr = subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getOperation(), "pool join");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        final ObjectNode retValue = scr.getReturnValue();
        assertEquals(retValue.get("poolName").asText(), "TEST_POOL");
    }

    @Test
    public void testLeavePoolSubExecutePoolLeftJsonMode()
        throws Exception {

        final PoolCommand.LeavePool subObj = new PoolCommand.LeavePool();
        final String nameFlag = "-name";
        final String poolName = "TEST_POOL";
        final String snFlag = "-sn";
        final String snName = "sn1";
        final boolean json = true;

        final String[] args = {command, nameFlag, poolName, snFlag, snName};

        ObjectNode returnValue = JsonUtils.createObjectNode();
        returnValue.put("pool_name", poolName);

        /* Objects to be used with the mock objects */
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);
        final Topology testTopo = new Topology("test_topo");
        final Datacenter dc1 = Datacenter.newInstance("zn1", 1,
                                                      DatacenterType.PRIMARY,
                                                      false, false);
        testTopo.add(new StorageNode(dc1, "localhost", 1000));
        final StorageNodeId storageNodeId = new StorageNodeId(1);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(poolList).anyTimes();
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(testTopo).anyTimes();
        cs.removeStorageNodeFromPool(poolName, storageNodeId,
                                     NULL_AUTH, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getJson()).
            andReturn(json).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            JsonNode resultNode = JsonUtils.parseJsonNode(result);
            assertEquals(command + " leave",
                resultNode.get(CommandJsonUtils.FIELD_OPERATION).
                    asText());
            assertEquals(5000,
                resultNode.get(CommandJsonUtils.FIELD_RETURN_CODE).
                    asInt());
            assertEquals("Operation ends successfully",
                resultNode.get(CommandJsonUtils.FIELD_DESCRIPTION).
                    asText());
            assertNull(resultNode.get(CommandJsonUtils.FIELD_CLEANUP_JOB));
            JsonNode valueNode = resultNode.get(
                CommandJsonUtils.FIELD_RETURN_VALUE);
            assertEquals(poolName, valueNode.get("pool_name").asText());
        } finally {
            doVerification();
        }
        final ShellCommandResult scr = subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getOperation(), "pool leave");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        final ObjectNode retValue = scr.getReturnValue();
        assertEquals(retValue.get("poolName").asText(), "TEST_POOL");
    }

    /*
     * Test cases to check the ShellException is thrown when a blank string is
     * passed as pool name.
     */

    @Test
    public void testBlankPoolName()
        throws Exception {

        /* Expect ShellException when empty or non-empty blank name is passed */
        testEmptyOrNonEmptyBlankPoolName("");
        testEmptyOrNonEmptyBlankPoolName("  ");
    }

    private void testEmptyOrNonEmptyBlankPoolName(String poolName)
        throws Exception {

        final PoolCommand.CreatePool subObj = new PoolCommand.CreatePool();
        final String nameFlag = "-name";
        final boolean json = true;
        final String[] args = {command, nameFlag, poolName};
        String info = "the value is empty or contain only blanks";

        ObjectNode returnValue = JsonUtils.createObjectNode();
        returnValue.put("pool_name", poolName);
        final CommandService csLocal = createMock(CommandService.class);
        /* Setup expectations for mocks */
        expect(csLocal.getSerialVersion()).andReturn(SerialVersion.CURRENT)
            .anyTimes();
        expect(csLocal.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(new ArrayList<>()).anyTimes();
        csLocal.addStorageNodePool(poolName, NULL_AUTH, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(csLocal);

        CommandShell shellLocal = createMock(CommandShell.class);
        expect(shellLocal.getAdmin()).andReturn(CommandServiceAPI.wrap(csLocal,
            null)).anyTimes();
        expect(shellLocal.getJson()).andReturn(json).anyTimes();

        expect(shellLocal.getCurrentCommand()).andReturn(subObj).anyTimes();

        shellLocal.badArgUsage(nameFlag, info, subObj);
        expectLastCall().andThrow(new ShellException()).anyTimes();
        replay(shellLocal);

        /* Executing command and checking result */
        assertThrows(ShellException.class, () -> subObj.execute(args, shellLocal));
    }
}
