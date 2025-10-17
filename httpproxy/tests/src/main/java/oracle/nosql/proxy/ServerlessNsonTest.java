/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest.OperationRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.ops.serde.nson.NsonSerializerFactory;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;
import oracle.nosql.driver.util.NettyByteInputStream;
import oracle.nosql.driver.util.NettyByteOutputStream;
import oracle.nosql.driver.values.MapValue;

/**
 * Extends ServerlessTestBase to exercise NSON (V4) protocol
 */
public class ServerlessNsonTest extends ServerlessTestBase {
    private static short v4ProtocolVersion = 4;
    private static SerializerFactory v4Factory = new NsonSerializerFactory();
    private static RequestSerializer serializer = new NsonRequestSerializer();
    private static ResponseDeserializer deserializer =
        new NsonResponseDeserializer();

    /*
     * Test create table, get, put, delete
     */
    @Test
    public void testCrud() throws Exception {
        final String tableName = "bar";
        TableResult tres = tableOp(
            "create table bar(id integer, name string, primary key(id))",
            tableLimits, null);

        /* put */
        MapValue value = new MapValue().put("id", 10).put("name", "jane");
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult pres = (PutResult) doRequest(putRequest);
        assertNotNull(pres.getVersion());
        value.put("id", 5).put("name", "joe");
        pres = (PutResult) doRequest(putRequest);
        Version version = pres.getVersion();
        assertNotNull(version);

        /* put ifAbsent, with return row info */
        putRequest.setOption(PutRequest.Option.IfAbsent).setReturnRow(true);
        pres = (PutResult) doRequest(putRequest);
        assertEquals(version, pres.getExistingVersion());
        assertEquals(value, pres.getExistingValue());

        /* get */
        MapValue key = new MapValue().put("id", 5);
        GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName(tableName);

        GetResult gres = (GetResult) doRequest(getRequest);
        assertEquals(value, gres.getValue());

        /* delete */
        DeleteRequest delRequest = new DeleteRequest()
            .setKey(key)
            .setTableName(tableName);
        DeleteResult dres = (DeleteResult) doRequest(delRequest);
        assertTrue(dres.getSuccess());
        dres = (DeleteResult) doRequest(delRequest);
        assertFalse(dres.getSuccess());

        /* drop the table */
        tres = tableOp(("drop table " + tableName), null, null);
        assertTrue(tres.getTableState() == TableResult.State.DROPPED);

        try {
            tres = getTable(tableName, null);
            fail("GetTable should have failed");
        } catch (TableNotFoundException tnfe) {
            // success
        }
    }



    /*
     * Test WriteMultiple
     */
    @Test
    public void testWriteMultiple() {
        /* Create a table */
        final String createTableDDL =
            "CREATE TABLE IF NOT EXISTS writeMultipleTable(" +
            "sid INTEGER, id INTEGER, name STRING, longString STRING, " +
            "PRIMARY KEY(SHARD(sid), id)) " +
            "USING TTL 1 DAYS";

        final String tableName = "writeMultipleTable";
        final int sid = 10;
        final int recordKB = 2;
        WriteMultipleRequest umRequest = new WriteMultipleRequest();
        List<Boolean> shouldSucceed = new ArrayList<Boolean>();
        List<Boolean> rowPresent = new ArrayList<Boolean>();

        tableOp(createTableDDL, tableLimits, null);

        /* Put 10 rows */
        for (int i = 0; i < 10; i++) {
            MapValue value = genRow(sid, i, recordKB);
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tableName);
            umRequest.add(putRequest, false);
            rowPresent.add(false);
            shouldSucceed.add(true);
        }

        WriteMultipleResult umResult =
            (WriteMultipleResult) doRequest(umRequest);
        verifyResult(umResult, umRequest, shouldSucceed, rowPresent, recordKB);
        Version versionId2 = umResult.getResults().get(2).getVersion();
        Version versionId7 = umResult.getResults().get(7).getVersion();

        umRequest.clear();
        shouldSucceed.clear();
        rowPresent.clear();

        /* PutIfAbsent, ReturnRow = true */
        MapValue value = genRow(sid, 0, recordKB, true);
        PutRequest put = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);
        rowPresent.add(true);
        shouldSucceed.add(false);

        /* PutIfPresent, ReturnRow = true */
        value = genRow(sid, 1, recordKB, true);
        put = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* PutIfVersion, ReturnRow = true */
        value = genRow(sid, 2, recordKB, true);
        put = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(versionId2)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);
        rowPresent.add(false);
        shouldSucceed.add(true);

        /* PutIfAbsent, ReturnRow = false */
        value = genRow(sid, 10, recordKB, true);
        put = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(false);
        umRequest.add(put, false);
        rowPresent.add(false);
        shouldSucceed.add(true);

        /* Put, ReturnRow = true */
        value = genRow(sid, 3, recordKB, true);
        put = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* Put, ReturnRow = false */
        value = genRow(sid, 4, recordKB, true);
        put = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(false);
        umRequest.add(put, false);
        rowPresent.add(false);
        shouldSucceed.add(true);

        /* Delete, ReturnRow = true */
        value = genKey(sid, 5);
        DeleteRequest delete = new DeleteRequest()
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(delete, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* Delete, ReturnRow = false */
        value = genKey(sid, 6);
        delete = new DeleteRequest()
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(false);
        umRequest.add(delete, false);
        rowPresent.add(false);
        shouldSucceed.add(true);

        /* DeleteIfVersion, ReturnRow = true */
        value = genKey(sid, 7);
        delete = new DeleteRequest()
            .setMatchVersion(versionId7)
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(delete, false);
        rowPresent.add(false);
        shouldSucceed.add(true);

        /* DeleteIfVersion, ReturnRow = true */
        value = genKey(sid, 8);
        delete = new DeleteRequest()
            .setMatchVersion(versionId7)
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(delete, false);
        rowPresent.add(true);
        shouldSucceed.add(false);

        /* Delete, ReturnRow = true */
        value = genKey(sid, 100);
        delete = new DeleteRequest()
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(delete, false);
        rowPresent.add(false);
        shouldSucceed.add(false);

        umResult = (WriteMultipleResult) doRequest(umRequest);
        verifyResult(umResult, umRequest, shouldSucceed, rowPresent, recordKB);
    }

    private MapValue genRow(int sid, int id, int recordKB) {
        return genRow(sid, id, recordKB, false);
    }

    private MapValue genRow(int sid, int id, int recordKB, boolean upd) {
        return new MapValue().put("sid", sid).put("id", id)
            .put("name", (upd ? "name_upd_" : "name_") + sid + "_" + id)
            .put("longString", genString((recordKB - 1) * 1024));
    }

    private MapValue genKey(int sid, int id) {
        return new MapValue().put("sid", sid).put("id", id);
    }

    private void verifyResult(WriteMultipleResult umResult,
                              WriteMultipleRequest umRequest,
                              List<Boolean> shouldSucceedList,
                              List<Boolean> rowPresentList,
                              int recordKB) {

        assertTrue("The operation expected to succeed",
                   umResult.getSuccess());

        List<OperationRequest> ops = umRequest.getOperations();
        assertTrue("Wrong number of results: expect " +
                   umRequest.getNumOperations() + ", actual " + umResult.size(),
                   umResult.size() == umRequest.getNumOperations());

        int ind = 0;

        for (OperationResult result : umResult.getResults()) {
            boolean shouldSucceed = shouldSucceedList.get(ind);
            assertTrue("Operation expected to succeed, opIdx=" + ind,
                       result.getSuccess() == shouldSucceed);

            OperationRequest op = ops.get(ind);
            WriteRequest request = op.getRequest();
            if (request instanceof PutRequest && shouldSucceed) {
                assertTrue("Expected to get new version ",
                           result.getVersion() != null);
            } else {
                assertTrue("Expected to not get new version",
                           result.getVersion() == null);
            }

            if (rowPresentList != null) {
                boolean hasReturnRow = rowPresentList.get(ind);

                assertTrue("The existing value is expected to be " +
                           (hasReturnRow ? "not null: " : "null: "),
                           (hasReturnRow ? result.getExistingValue() != null :
                            result.getExistingValue() == null));
                assertTrue("The existing version is expected to be " +
                           (hasReturnRow ? "not null: " : "null: "),
                           (hasReturnRow ? result.getExistingVersion() != null :
                            result.getExistingVersion() == null));
            }
            ind++;
        }
    }

    /**
     * Serialize a Request returning a new ByteBuf
     */
    static class NsonRequestSerializer implements RequestSerializer {
        @Override
        public ByteBuf serialize(Request req) throws IOException {

            ByteBuf content = Unpooled.buffer();
            ByteOutputStream bos = new NettyByteOutputStream(content);
            short proto = v4ProtocolVersion;
            Serializer ser = req.createSerializer(v4Factory);
            if (ser == null) {
                ser = req.createSerializer(binaryFactory);
                proto = (short) 3;
            }
            bos.writeShort(proto);
            try {
                ser.serialize(req, proto, bos);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return content;
        }
    }

    /**
     * Deserialize response
     */
    static class NsonResponseDeserializer implements ResponseDeserializer {

        @Override
        public Result deserialize(Request req,
                                  ServerlessContext ctx) throws IOException {

            assertEquals(HttpResponseStatus.OK, ctx.status);
            ByteBuf content = ctx.content;
            try (ByteInputStream bis = new NettyByteInputStream(content)) {
                short proto = v4ProtocolVersion;
                Serializer ser = req.createDeserializer(v4Factory);
                if (ser == null) {
                    ser = req.createDeserializer(binaryFactory);
                    proto = (short) 3;
                }
                Result res = ser.deserialize(req, bis, proto);
                return res;
            } finally {
                /* release the response buffer */
                content.release();
            }
        }
    }

    private TableResult tableOp(String statement,
                                TableLimits limits,
                                String tableName) {
        TableRequest tableRequest = new TableRequest()
            .setStatement(statement)
            .setTableLimits(tableLimits)
            .setTableName(tableName);
        TableResult res = (TableResult) doRequest(tableRequest);
        return waitForCompletion(res.getTableName(), res.getOperationId());
    }

    private TableResult waitForCompletion(String tableName, String opId) {

        TableResult res = getTable(tableName, opId);
        TableResult.State state = res.getTableState();
        while (!isTerminal(state)) {
            try {
                Thread.sleep(500);
            } catch (Exception e) {} // ignore
            res = getTable(tableName, opId);
            state = res.getTableState();
        }
        return res;
    }

    private TableResult getTable(String tableName, String opId) {

        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName).
            setOperationId(opId);
        return (TableResult) doRequest(getTable);
    }

    /**
     * Perform the request using V4 protocol and the NSON serializers
     */
    private Result doRequest(Request request) {
        return doRequest(request, serializer, deserializer, v4ProtocolVersion);
    }
}
