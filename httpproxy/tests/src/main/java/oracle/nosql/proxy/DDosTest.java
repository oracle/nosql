/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_ARRAY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_BOOLEAN;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_INTEGER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_MAP;
import static oracle.nosql.proxy.protocol.HttpConstants.ACCEPT;
import static oracle.nosql.proxy.protocol.HttpConstants.AUTHORIZATION;
import static oracle.nosql.proxy.protocol.HttpConstants.CONNECTION;
import static oracle.nosql.proxy.protocol.HttpConstants.CONTENT_LENGTH;
import static oracle.nosql.proxy.protocol.HttpConstants.CONTENT_TYPE;
import static oracle.nosql.proxy.protocol.HttpConstants.NOSQL_DATA_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.NOSQL_VERSION;
import static oracle.nosql.proxy.protocol.HttpConstants.REQUEST_COMPARTMENT_ID;
import static oracle.nosql.proxy.protocol.HttpConstants.REQUEST_ID_HEADER;
import static oracle.nosql.proxy.protocol.Protocol.BAD_PROTOCOL_MESSAGE;
import static oracle.nosql.proxy.protocol.Protocol.ILLEGAL_ARGUMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.httpclient.ResponseHandler;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.serde.BinarySerializerFactory;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;
import oracle.nosql.driver.util.NettyByteInputStream;
import oracle.nosql.driver.util.NettyByteOutputStream;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.security.SecureTestUtil;

/**
 * Tests on handling bad protocol on proxy side
 */
public class DDosTest extends ProxyTestBase {

    private final static String tableName = "users";

    private final BinarySerializerFactory factory =
        new BinarySerializerFactory();

    private final MapValue key = createTestKey(1);
    private final MapValue record = createTestValue();

    private final GetRequest getRequest = new GetRequest()
        .setTableName(tableName)
        .setConsistency(Consistency.ABSOLUTE)
        .setKey(key);

    private final PutRequest putRequest = new PutRequest()
        .setTableName(tableName)
        .setValue(record)
        .setTTL(TimeToLive.ofDays(1));

    private final DeleteRequest deleteRequest = new DeleteRequest()
        .setTableName(tableName)
        .setKey(key);

    private final String statement = "select * from users";
    private final PrepareRequest prepareRequest = new PrepareRequest()
        .setStatement(statement);

    private final String boundStatement = "declare $id integer; " +
        "select * from users where id = $id";
    private final PrepareRequest prepareBoundStmtRequest = new PrepareRequest()
        .setStatement(boundStatement);

    private final GetIndexesRequest getIndexesRequest = new GetIndexesRequest()
        .setTableName(tableName)
        .setIndexName("idx1");

    private final TableUsageRequest tableUsageRequest = new TableUsageRequest()
        .setTableName(tableName)
        .setStartTime(System.currentTimeMillis())
        .setEndTime(System.currentTimeMillis() + 3600_000)
        .setLimit(10);

    /* Create a table */
    private final static String createTableDDL =
        "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
            "id INTEGER, " +
            "name STRING, " +
            "count LONG, " +
            "avg DOUBLE, " +
            "sum NUMBER, " +
            "exp BOOLEAN, " +
            "key BINARY, " +
            "map MAP(INTEGER), " +
            "array ARRAY(STRING), " +
            "record RECORD(rid INTEGER, rs STRING), " +
            "PRIMARY KEY(id))";

    private final static String createIndexDDL =
        "CREATE INDEX IF NOT EXISTS idx1 ON " + tableName + "(name)";

    private ByteBuf buf;
    private HttpClient httpClient;
    private NoSQLHandleConfig httpConfig;
    private String kvRequestURI;
    private int timeoutMs;
    private int requestId = 0;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        assumeTrue("Skip DDosTest in onprem or minicloud or cloud test",
                   !Boolean.getBoolean(ONPREM_PROP) &&
                   !Boolean.getBoolean(USEMC_PROP) &&
                   !Boolean.getBoolean(USECLOUD_PROP));

        /* this test requires error limiting */
        System.setProperty(PROXY_ERROR_LIMITING_PROP, "true");

        staticSetUp(tenantLimits);
    }

    @Override
    public void setUp() throws Exception {
        if (onprem || cloudRunning) {
            return;
        }
        super.setUp();

        buf = Unpooled.buffer();

        URL url = new URL("http", getProxyHost(), getProxyPort(), "/");
        httpConfig = new NoSQLHandleConfig(url);

        httpConfig.configureDefaultRetryHandler(0, 0);
        timeoutMs = 1000;
        httpConfig.setRequestTimeout(timeoutMs);

        kvRequestURI = httpConfig.getServiceURL().toString() +
            NOSQL_VERSION + "/" + NOSQL_DATA_PATH;

        httpClient = createHttpClient(getProxyHost(),
                                      getProxyPort(),
                                      httpConfig.getNumThreads(),
                                      "DDosTest",
                                      null /* Logger */);
        assertNotNull(httpClient);
        createTable();

        if (isSecure()) {
            /* warm up security caches */
            handle.put(putRequest);
            handle.get(getRequest);
            handle.delete(deleteRequest);
            handle.getTable(new GetTableRequest().setTableName(tableName));
            handle.getTableUsage(tableUsageRequest);
            handle.getIndexes(getIndexesRequest);
            handle.query(createQueryWithBoundStmtRequest());
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (onprem || cloudRunning) {
            return;
        }

        if (buf != null) {
            buf.release(buf.refCnt());
        }

        if (httpClient != null) {
            httpClient.shutdown();
        }
        super.tearDown();
    }

    @Before
    public void setVersion() throws Exception {
        /*
         * This test suite is somewhat V2/V3-centric. So
         * set the serial version to 3 if higher.
         */
        forceV3((NoSQLHandleImpl)handle);
    }

    /*
     * Test bad protocol data on below values:
     *  1. SerialVersion
     *  2. OpCode
     *  3. RequestTimeout
     *  4. TableName
     *  5. ReturnRowFlag
     *  6. MapValue
     *  7. IfUpdateTTL
     *  8. TTLValue
     */
    @Test
    public void testPutDDoS() {

        assumeTrue(onprem == false);
        assumeTrue(cloudRunning == false);

        final int[] lengths = {
            2   /* SerialVersion: short */,
            1   /* OpCode: byte */,
            3   /* RequestTimeout: packed int */,
            6   /* TableName: String */,
            1   /* ReturnRowFlag: boolean */,
            1   /* Durability: one byte */,
            1   /* ExactMatch: boolean */,
            1   /* IdentityCacheSize: packed int */,
            248 /* Record: MapValue */,
            1   /* IfUpdateTTL: boolean */,
            2   /* TTL: value(packed long) + unit(byte)*/
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, putRequest);

        try {
            String test;
            int offset = 0;
            int pos = 0;

            test = "PUT OK test";
            executeDDoSRequests(test, buf, 0);

            /*
             * SerialVersion
             */

            /* SerialVersion: 0 */
            test = "PUT Bad serialVersion: 0";
            buf.setShort(offset, 0);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * OpCode
             */

            /* Invalid OpCode */
            offset += lengths[pos++];
            test = "PUT Bad OpCode";
            int invalidOpCode = OpCode.values().length;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, invalidOpCode);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * RequestTimeout
             */

            /* requestTimeout: -5000 */
            test = "PUT Bad requestTimeout: -5000";
            offset += lengths[pos++];
            int invalidTimeout = -5000;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, invalidTimeout);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * TableName
             */

            /* Invalid TableName: empty string */
            String invalidTableName = "";
            test = "PUT empty TableName";
            offset += lengths[pos++];
            refillBuffer(buf, bufBytes);
            setString(out, offset, invalidTableName);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * ReturnRowFlag
             */
            offset += lengths[pos++];

            /*
             * Durability
             * Only in V3 and above
             */
            short serialVersion = ((NoSQLHandleImpl)handle).getSerialVersion();
            if (serialVersion > 2) {
                offset += lengths[pos++];
            } else {
                pos++;
            }

            /*
             * ExactMatch
             */
            offset += lengths[pos++];

            /*
             * IdentityCacheSize
             */
            offset += lengths[pos++];

            /*
             * MapValue
             */
            offset += lengths[pos++];
            testMapValue(buf, out, bufBytes, offset, lengths[pos]);

            /*
             * IfUpdateTTLFlag
             */
            offset += lengths[pos++];

            /*
             * TTL
             */
            long invalidTTL = -2;
            offset += lengths[pos++];
            test = "PUT TTL: " + invalidTTL;
            refillBuffer(buf, bufBytes);
            setPackedLong(out, offset, invalidTTL);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            test = "PUT TTL: invalid ttl unit";
            refillBuffer(buf, bufBytes);
            buf.setByte(offset + 1, -1);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        } catch (IOException ioe) {
            fail("Write failed: " + ioe.getMessage());
        } finally {
            out.close();
        }
    }

    private void testMapValue(ByteBuf buffer,
                              ByteOutputStream out,
                              byte[] bufBytes,
                              int baseOffset,
                              int length) throws IOException {
        final int headerLen = 9; /* 1(type) + 4(length) + 4 (size)/*/
        final String[] fields = new String[] {
            "avg",
            "array",
            "record",
            "name",
            "count",
            "sum",
            "id",
            "exp",
            "map",
            "key"
        };
        final int[] lengths = new int[] {
            13, /* avg: DOUBLE, 4(name) + 1(type) + 8(double) */
            36, /* array: ARRAY, 6(name) + 1(type) + 29(value) */
            34, /* record: RECORD, 7(name) + 1(type) + 26(value) */
            19, /* name: STRING, 5(name) + 1(type) + 13(value) */
            16, /* count: LONG, 6(name) + 1(type) + 9(value) */
            44, /* sum: NUMBER, 4(name) + 1(type) + 39(value) */
            5,  /* id: INTEGER, 3(name) + 1(type) + 1(value) */
            6,  /* exp: BOOLEAN, 4(name) + 1(type) + 1(value) */
            30, /* map: MAP, 4(name) + 1(type) + 25(value) */
            36  /* key: BINARY, 4(name) + 1(type) + 31(value) */
        };

        final Map<String, Integer> offsets = new HashMap<String, Integer>();
        int offset = baseOffset + headerLen;
        for (int i = 0; i < fields.length; i++) {
            offsets.put(fields[i], offset);
            offset += lengths[i];
        }

        offset = baseOffset;
        String test;
        ByteInputStream in;
        int value;
        String svalue;

        /* Corrupted type of top MapValue */
        value = -1;
        test = "MapValue: corrupted type of top MapValue, " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Wrong length value */
        offset += 1;
        refillBuffer(buffer, bufBytes);
        in = new NettyByteInputStream(buffer);
        value = bufBytes.length + 1;
        setInt(out, offset, value);
        test = "MapValue: wrong length value, " + value ;
        executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Wrong size value */
        offset += 4;
        refillBuffer(buffer, bufBytes);
        value = -1;
        setInt(out, offset, value);
        test = "MapValue: wrong size value, " + value ;
        executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        /*
         * Field: avg
         */
        String fname = "avg";
        offset = offsets.get(fname);
        svalue = null;
        refillBuffer(buffer, bufBytes);
        setString(out, offset, svalue);
        test = "MapValue: field name is null" ;
        executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Corrupted value type */
        value = 100;
        offset += fname.length() + 1;
        test = "MapValue: corrupted type of field \"avg\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid value type for DOUBLE */
        value = TYPE_BOOLEAN;
        test = "MapValue: invalid value type for field \"avg\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        fname = "array";
        offset = offsets.get(fname);

        /* Invalid value type for array value */
        offset += fname.length() + 1;
        value = TYPE_MAP;
        test = "MapValue: invalid value type for field \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeDDoSRequests(test, buf, ILLEGAL_ARGUMENT);

        value = TYPE_INTEGER;
        test = "MapValue: invalid value type for field \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid length value of array value */
        length = readInt(in, offset);
        offset++;
        value = -1;
        test = "MapValue: invalid length of  \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);
    }


    /*
     * Test bad protocol data on below values:
     *  1. Consistency
     *  2. PrimaryKey type
     */
    @Test
    public void testGetDDoS() {

        assumeTrue(onprem == false);
        assumeTrue(cloudRunning == false);

        final int[] lengths = {
            2  /* SerialVersion: short*/,
            1  /* OpCode: byte*/,
            3  /* RequestTimeout: packed int */,
            6  /* TableName: string */,
            1  /* Consistency: boolean */,
            14 /* Key: 1(TYPE_MAP) + 4(length) + 4(size) + 3("id") +
                       1(TYPE_INT) + 1(1-value) */
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, getRequest);

        try {
            String test;
            int pos;
            int offset = 0;

            test = "GET OK test";
            executeDDoSRequests(test, buf, 0);

            /*
             * Consistency
             */

            /* Move to offset of consistency */
            for (pos = 0; pos < 4; pos++) {
                offset += lengths[pos];
            }

            /* Invalid consistency type */
            int value = -1;
            test = "GET Invalid consistency type: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            value = 3;
            test = "GET Invalid consistency type: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * PrimaryKey
             */
            offset += lengths[pos++];

            value = -1;
            test = "GET Invalid value type of PrimaryKey: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            value = TYPE_ARRAY;
            test = "GET Invalid value type of PrimaryKey: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        } finally {
            out.close();
        }
    }

    /*
     * Test bad protocol on below values:
     *  1. Statement
     */
    @Test
    public void testPrepareDDoS() {

        assumeTrue(onprem == false);
        assumeTrue(cloudRunning == false);

        final int[] lengths = new int[] {
            2  /* SerialVersion: short */,
            1  /* OpCode: byte */,
            3  /* RequestTimeout: packed int */,
            20 /* Statement: string */
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, prepareRequest);

        try {
            String test;
            int pos;
            int offset = 0;

            test = "PREPARE OK test";
            executeDDoSRequests(test, buf, 0);

            /*
             * Statement
             */
            for (pos = 0; pos < 3; pos++) {
                offset += lengths[pos];
            }

            String svalue = null;
            test = "PREPARE Invalid statement: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            svalue = "";
            test = "PREPARE Invalid statement: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

        } catch (IOException ioe) {
            fail("Failed to write to buffer: " + ioe);
        } finally {
            out.close();
        }
    }

    /*
     * Test bad protocol on below values:
     *  1. PreparedStatement
     *  2. Variables Number
     *  3. Variable Name
     *  4. Variable Value
     */
    @Test
    public void testQueryDDoS() {

        assumeTrue(onprem == false);
        assumeTrue(cloudRunning == false);

        final QueryRequest queryReq = createQueryWithBoundStmtRequest();

        final int prepStmtLen =
            4 /* int, length of PreparedStatement */+
            queryReq.getPreparedStatement().getStatement().length;

        final int[] lengths = {
            2  /* SerialVersion: short*/,
            1  /* OpCode: byte */,
            3  /* RequestTimeout: packed int */,
            1  /* Consistency: byte */,
            1  /* NumberLimit: packed int */,
            3  /* MaxReadKB: packed int */,
            1  /* ContinuationKey: byte array */,
            1  /* IsPreparedStatement: boolean */,
            2  /* QueryVersion: short */,
            1  /* traceLevel: packed int */,
            1  /* MaxWriteKB: packed int */,
            1  /* MathContext: byte */,
            1  /* ToplogySeqNum: packed int */,
            1  /* ShardId: packed int */,
            1  /* isSimpleQuery: boolean */,
            prepStmtLen /* PreparedStatement: byte array */,
            1  /* VariablesNumber: packed int */,
            4  /* VariableName: string */,
            2  /* VariableValue: INT_TYPE + packed int */
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, queryReq);

        try {
            String test;
            int pos;
            int offset = 0;

            test = "QUERY OK test";
            executeDDoSRequests(test, buf, 0);

            /*
             * PreparedStatement
             */
            for (pos = 0; pos < 15; pos++) {
                offset += lengths[pos];
            }

            int value = -1;
            test = "QUERY Invalid prepared Statement";
            refillBuffer(buf, bufBytes);
            setInt(out, offset, value);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            value = 0;
            test = "QUERY Invalid prepared Statement";
            refillBuffer(buf, bufBytes);
            setInt(out, offset, value);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * Variables number
             */
            value = -1;
            offset += lengths[pos++];
            test = "QUERY Invalid variable number: " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            value = 2;
            test = "QUERY Invalid variable number: " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * Variable name
             */
            offset += lengths[pos++];
            String svalue = null;
            test = "QUERY Invalid variable name: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            svalue = "";
            test = "QUERY Invalid variable name: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * Variable value
             */
            offset += lengths[pos++];
            value = -1;
            test = "QUERY Invalid variable value type: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeDDoSRequests(test, buf, BAD_PROTOCOL_MESSAGE);

            value = TYPE_ARRAY;
            test = "QUERY Invalid variable value type: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeDDoSRequests(test, buf, ILLEGAL_ARGUMENT);

        } catch (IOException ioe) {
            fail("Failed to write to buffer: " + ioe);
        } finally {
            out.close();
        }
    }

    private QueryRequest createQueryWithBoundStmtRequest() {
        final PrepareResult prepRet = handle.prepare(prepareBoundStmtRequest);
        prepRet.getPreparedStatement()
               .setVariable("$id", new IntegerValue(1));

        final QueryRequest queryReq = new QueryRequest()
            .setPreparedStatement(prepRet)
            .setMaxReadKB(1024)
            .setLimit(100);
        return queryReq;
    }

    private byte[] serializeRequest(ByteOutputStream out, Request request) {

        request.setDefaults(httpConfig);

        Serializer ser = request.createSerializer(factory);
        try {
            short serialVersion = ((NoSQLHandleImpl)handle).getSerialVersion();
            out.writeShort(serialVersion);
            if (request instanceof QueryRequest ||
                request instanceof PrepareRequest) {
                ser.serialize(request, serialVersion,
                              QueryDriver.QUERY_V3, out);
            } else {
                ser.serialize(request, serialVersion, out);
            }
        } catch (IOException e) {
            fail("Failed to serialize put request");
        }

        final byte[] bytes = new byte[buf.writerIndex()];
        System.arraycopy(buf.array(), 0, bytes, 0, bytes.length);

        return bytes;
    }

    private void executeRequest(String test,
                                ByteBuf buffer,
                                int expErrCode,
                                int minLatencyMs,
                                int maxLatencyMs,
                                int requestNum) {

        ResponseHandler responseHandler = null;
        ByteInputStream bis = null;

        try {
            Channel channel = httpClient.getChannel(timeoutMs);
            responseHandler = new ResponseHandler(httpClient, null, channel);

            final FullHttpRequest request =
                new DefaultFullHttpRequest(HTTP_1_1, POST, kvRequestURI,
                                           buffer,
                                           false /* Don't validate hdrs */);
            HttpHeaders headers = request.headers();
            headers.add(HttpHeaderNames.HOST, getProxyHost())
                .add(REQUEST_ID_HEADER, nextRequestId())
                .set(CONTENT_TYPE, "application/octet-stream")
                .set(CONNECTION, "keep-alive")
                .set(ACCEPT, "application/octet-stream")
                .setInt(CONTENT_LENGTH, buffer.readableBytes());

            if (!onprem) {
                headers.set(AUTHORIZATION, SecureTestUtil.getAuthHeader(
                    getTenantId(), isSecure()));
            }
            if (isSecure()) {
                headers.add(REQUEST_COMPARTMENT_ID, getTenantId());
            }

            long startMs = System.currentTimeMillis();
            httpClient.runRequest(request, responseHandler, channel);

            if (responseHandler.await(timeoutMs)) {
                throw new TimeoutException();
            }

            long endMs = System.currentTimeMillis();
            int latencyMs = (int)(endMs - startMs);

            if (latencyMs < minLatencyMs || latencyMs > maxLatencyMs) {
                fail("Request " + requestNum + " took " + latencyMs +
                     "ms, expected between " + minLatencyMs + "ms and " +
                     maxLatencyMs + "ms");
            }

            if (verbose) {
                System.out.println("Request " + requestNum + " took " +
                                   latencyMs + "ms");
            }

            /* Validates the response from proxy */
            assertEquals(HttpResponseStatus.OK, responseHandler.getStatus());
            bis = new NettyByteInputStream(responseHandler.getContent());
            int errCode = bis.readByte();
            if (expErrCode >= 0) {
                if (expErrCode == errCode) {
                    return;
                }
                /* support V4 server error codes */
                if (errCode == 6) { /* nson MAP */
                    errCode = getV4ErrorCode(responseHandler.getContent());
                }
                assertEquals(test + " failed", expErrCode, errCode);
            }
        } catch (Throwable t) {
            if (t instanceof TimeoutException) {
                /* did we expect a timeout? */
                /* if timeoutMs is within min/max latency, yes */
                if (maxLatencyMs > timeoutMs ) {
                    /* all good, expected */
                    if (verbose) {
                        System.out.println("Request " + requestNum +
                            " timed out (expected)");
                    }
                } else {
                    fail(test + " Request " + requestNum +
                         " timed out after " + timeoutMs + " ms");
                }
            } else {
                fail(test + " failed: " + t);
            }
        } finally {
            if (bis != null) {
                bis.close();
            }
            if (responseHandler != null) {
                responseHandler.close();
            }
        }
    }

    private void executeDDoSRequests(String test,
                                     ByteBuf buffer,
                                     int expErrCode) {
        if (expErrCode != 0) {
            /* sleep to cool down error limiters */
            try {
                if (verbose) {
                    System.out.println(test + " Sleeping for 4 seconds...");
                }
                Thread.sleep(4000);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }

        /* first 5 should return expected error code */
        /* latency should be in single-digit ms, after first */
        for (int x=0; x<5; x++) {
            executeRequest(test, buffer.retainedDuplicate(), expErrCode, 0,
                          (x==0) ? 500 : 100, x);
        }

        /* next 5 should be slowed to >200ms latency */
        for (int x=0; x<5; x++) {
            executeRequest(test, buffer.retainedDuplicate(), expErrCode,
            (expErrCode==0) ? 0 : 200,
            (expErrCode==0) ? 100 : 500, x);
        }

        if (expErrCode == 0) {
            return;
        }

        /* at this point we expect requests to mostly timeout */

        /* fire off parallel threads to effect >10 errs/sec */
        Thread threads[] = new Thread[5];
        for(int x=0; x<5; x++) {
            threads[x] = new Thread(() ->
                {
                    for (int y=0; y<3; y++) {
                        executeRequest(test, buffer.retainedDuplicate(),
                                       expErrCode, 200, timeoutMs + 100, y);
                    }
                });
            threads[x].start();
        }
        /* wait for threads to finish */
        for(int x=0; x<5; x++) {
            try {
                threads[x].join();
            } catch (Exception ignored) {}
        }
    }

    private String nextRequestId() {
        return String.valueOf(requestId++);
    }

    private void refillBuffer(ByteBuf buffer, byte[] bytes) {
        buffer.setBytes(0, bytes);
        buffer.readerIndex(0);
        buffer.writerIndex(bytes.length);
    }

    private void setPackedInt(ByteOutputStream out, int offset, int value)
        throws IOException {

        int savedOffset = out.getOffset();
        out.setWriteIndex(offset);
        SerializationUtil.writePackedInt(out, value);
        out.setWriteIndex(savedOffset);
    }

    private void setInt(ByteOutputStream out, int offset, int value)
        throws IOException {

        int savedOffset = out.getOffset();
        out.setWriteIndex(offset);
        out.writeInt(value);
        out.setWriteIndex(savedOffset);
    }

    private void setPackedLong(ByteOutputStream out, int offset, long value)
        throws IOException {

        int savedOffset = out.getOffset();
        out.setWriteIndex(offset);
        SerializationUtil.writePackedLong(out, value);
        out.setWriteIndex(savedOffset);
    }

    private void setString(ByteOutputStream out, int offset, String value)
        throws IOException {

        int savedOffset = out.getOffset();
        out.setWriteIndex(offset);
        SerializationUtil.writeString(out, value);
        out.setWriteIndex(savedOffset);
    }

    private int readInt(ByteInputStream in, int offset)
        throws IOException {

        int savedOffset = in.getOffset();
        in.setOffset(offset);
        int value = in.readInt();
        in.setOffset(savedOffset);
        return value;
    }

    private void createTable() {
        tableOperation(handle, createTableDDL,
                       new TableLimits(20000, 20000, 50),
                       TableResult.State.ACTIVE, 10000);
        tableOperation(handle, createIndexDDL, null,
                       TableResult.State.ACTIVE, 10000);
    }

    private MapValue createTestValue() {
        MapValue row = new MapValue();
        row.put("id", 1);
        row.put("name", "string value");
        row.put("count", Long.MAX_VALUE);
        row.put("avg", Double.MAX_VALUE);
        row.put("sum", new BigDecimal("12345678901234567890123456789012345678"));
        row.put("exp", true);
        row.put("key", genBytes(30, null));

        MapValue map = new MapValue();
        map.put("k1", 100);
        map.put("k2", 200);
        map.put("k3", 300);
        row.put("map", map);

        ArrayValue array = new ArrayValue();
        array.add("elem1");
        array.add("elem2");
        array.add("elem3");
        row.put("array", array);

        MapValue rec = new MapValue();
        rec.put("rid", 1024);
        rec.put("rs", "nosql");
        row.put("record", rec);

        return row;
    }

    private MapValue createTestKey(int id) {
        return new MapValue().put("id", id);
    }

    private byte[] genBytes(int length, Random rand) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (rand == null)? (byte)(i % 256) :
                                       (byte)rand.nextInt(256);
        }
        return bytes;
    }

}
