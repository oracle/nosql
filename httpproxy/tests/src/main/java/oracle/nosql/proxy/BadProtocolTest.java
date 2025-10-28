/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.proxy.protocol.BinaryProtocol.BAD_PROTOCOL_MESSAGE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ILLEGAL_ARGUMENT;
import static oracle.nosql.proxy.protocol.BinaryProtocol.REQUEST_SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_ARRAY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_BINARY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_BOOLEAN;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_INTEGER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_MAP;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_STRING;
import static oracle.nosql.proxy.protocol.BinaryProtocol.UNSUPPORTED_PROTOCOL;
import static oracle.nosql.proxy.protocol.HttpConstants.ACCEPT;
import static oracle.nosql.proxy.protocol.HttpConstants.AUTHORIZATION;
import static oracle.nosql.proxy.protocol.HttpConstants.CONNECTION;
import static oracle.nosql.proxy.protocol.HttpConstants.CONTENT_LENGTH;
import static oracle.nosql.proxy.protocol.HttpConstants.CONTENT_TYPE;
import static oracle.nosql.proxy.protocol.HttpConstants.NOSQL_DATA_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.NOSQL_VERSION;
import static oracle.nosql.proxy.protocol.HttpConstants.REQUEST_ID_HEADER;
import static oracle.nosql.proxy.protocol.HttpConstants.REQUEST_COMPARTMENT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import oracle.kv.impl.topo.RepNodeId;
import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.httpclient.ResponseHandler;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Tests on handling bad protocol on proxy side
 */
public class BadProtocolTest extends ProxyTestBase {

    private static final short PROXY_SERIAL_VERSION =
        oracle.nosql.proxy.protocol.BinaryProtocol.SERIAL_VERSION;

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

    private final PutRequest putIfVersionRequest = new PutRequest()
        .setOption(Option.IfVersion)
        .setTableName(tableName)
        .setValue(record)
        .setMatchVersion(genVersion());

    private final DeleteRequest deleteRequest = new DeleteRequest()
        .setTableName(tableName)
        .setKey(key);

    private final DeleteRequest deleteIfVersionRequest = new DeleteRequest()
        .setTableName(tableName)
        .setMatchVersion(genVersion())
        .setKey(key);

    private final MultiDeleteRequest multiDeleteRequest =
        new MultiDeleteRequest()
            .setTableName(tableName)
            .setKey(key)
            .setMaxWriteKB(1024)
            .setContinuationKey(genBytes(20, null));

    private final WriteMultipleRequest writeMultipleRequest =
        new WriteMultipleRequest()
            .add(putRequest, false);

    private final String statement = "select * from users";
    private final PrepareRequest prepareRequest = new PrepareRequest()
        .setStatement(statement);

    private final String boundStatement = "declare $id integer; " +
        "select * from users where id = $id";
    private final PrepareRequest prepareBoundStmtRequest = new PrepareRequest()
        .setStatement(boundStatement);

    private final TableRequest tableRequest = new TableRequest()
        .setStatement(createTableDDL)
        .setTableLimits(new TableLimits(50, 50, 50));

    private final TableRequest tableSetLimitsRequest = new TableRequest()
        .setTableName(tableName)
        .setTableLimits(new TableLimits(50, 50, 50));

    private final GetIndexesRequest getIndexesRequest = new GetIndexesRequest()
        .setTableName(tableName)
        .setIndexName("idx1");

    private final GetTableRequest getTableRequest = new GetTableRequest()
        .setTableName(tableName)
        .setOperationId("1");

    private final ListTablesRequest listTablesRequest = new ListTablesRequest()
        .setStartIndex(0)
        .setLimit(0);

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

        /*
         * This test composes the request and send to the proxy but does not
         * use the driver, the request is not signed, therefore cannot be run
         * in cloud test.
         */
        assumeTrue("Skip BadProtocolTest in could test",
                   !Boolean.getBoolean(USECLOUD_PROP));

        ProxyTestBase.staticSetUp();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        buf = Unpooled.buffer();

        URL url = new URL("http", getProxyHost(), getProxyPort(), "/");
        httpConfig = new NoSQLHandleConfig(url);

        kvRequestURI = httpConfig.getServiceURL().toString() +
            NOSQL_VERSION + "/" + NOSQL_DATA_PATH;
        timeoutMs = httpConfig.getDefaultRequestTimeout();

        httpClient = createHttpClient(getProxyHost(),
                                      getProxyPort(),
                                      httpConfig.getNumThreads(),
                                      "BadProtocolTest",
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
         * This test suite is completely V2/V3-centric. So
         * set the serial version to 3 if higher.
         */
        forceV3((NoSQLHandleImpl)handle);
    }

    /**
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
    public void testPutRequest() {

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

            test = "OK test";
            executeRequest(test, buf, 0);

            /*
             * SerialVersion
             */

            /* SerialVersion: 0 */
            test = "Bad serialVersion: 0";
            buf.setShort(offset, 0);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            /* SerialVersion: PROXY_SERIAL_VERSION + 1 */
            test = "Bad serialVersion: PROXY_SERIAL_VERSION + 1";
            refillBuffer(buf, bufBytes);
            buf.setShort(offset, PROXY_SERIAL_VERSION + 1);
            executeRequest(test, buf, UNSUPPORTED_PROTOCOL);

            /*
             * OpCode
             */

            /* Invalid OpCode */
            offset += lengths[pos++];
            test = "Bad OpCode";
            int invalidOpCode = OpCode.values().length;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, invalidOpCode);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * RequestTimeout
             */

            /* requestTimeout: -5000 */
            test = "Bad requestTimeout: -5000";
            offset += lengths[pos++];
            int invalidTimeout = -5000;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, invalidTimeout);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * TableName
             */

            /* Invalid TableName: null or empty string */
            String invalidTableName = null;
            test = "TableName: " + invalidTableName;
            offset += lengths[pos++];
            refillBuffer(buf, bufBytes);
            setString(out, offset, invalidTableName);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            invalidTableName = "";
            test = "TableName: " + invalidTableName;
            refillBuffer(buf, bufBytes);
            setString(out, offset, invalidTableName);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

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
            test = "TTL: " + invalidTTL;
            refillBuffer(buf, bufBytes);
            setPackedLong(out, offset, invalidTTL);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            test = "TTL: invalid ttl unit";
            refillBuffer(buf, bufBytes);
            buf.setByte(offset + 1, -1);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

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
        int pos = 0;
        int value;
        String svalue;

        /* Corrupted type of top MapValue */
        value = -1;
        test = "MapValue: corrupted type of top MapValue, " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = TYPE_INTEGER;
        test = "MapValue: corrupted type of top MapValue, " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Wrong length value */
        offset += 1;
        refillBuffer(buffer, bufBytes);
        in = new NettyByteInputStream(buffer);
        value = bufBytes.length + 1;
        setInt(out, offset, value);
        test = "MapValue: wrong length value, " + value ;
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        refillBuffer(buffer, bufBytes);
        value = -1;
        setInt(out, offset, value);
        test = "MapValue: wrong length value, " + value ;
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Wrong size value */
        offset += 4;
        refillBuffer(buffer, bufBytes);
        value = -1;
        setInt(out, offset, value);
        test = "MapValue: wrong size value, " + value ;
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /*
         * Field: avg
         */
        String fname = "avg";
        offset = offsets.get(fname);
        svalue = null;
        refillBuffer(buffer, bufBytes);
        setString(out, offset, svalue);
        test = "MapValue: field name is null" ;
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        svalue = "";
        refillBuffer(buffer, bufBytes);
        setString(out, offset, svalue);
        test = "MapValue: field name is empty string" ;
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Corrupted value type */
        value = 100;
        offset += fname.length() + 1;
        test = "MapValue: corrupted type of field \"avg\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid value type for DOUBLE */
        value = TYPE_BOOLEAN;
        test = "MapValue: invalid value type for field \"avg\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /*
         * Field: array
         */
        final int[] arrayElemLens = new int[] {
            4 /* length */,
            4 /* size */,
            1 /* 1st value's type */,
            6 /* 1st value */,
            1 /* 2nd value's type */,
            6 /* 2nd value */,
            1 /* 3rd value's type */,
            6 /* 3rd value */,
        };

        pos = 0;
        fname = "array";
        offset = offsets.get(fname);

        /* Invalid value type for array value */
        offset += fname.length() + 1;
        value = TYPE_MAP;
        test = "MapValue: invalid value type for field \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, ILLEGAL_ARGUMENT);

        value = TYPE_INTEGER;
        test = "MapValue: invalid value type for field \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid length value of array value */
        length = readInt(in, offset);
        offset++;
        value = -1;
        test = "MapValue: invalid length of  \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = length + 1;
        test = "MapValue: invalid length of  \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = length - 1;
        test = "MapValue: invalid length of  \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid size value of array value */
        offset += arrayElemLens[pos++];
        int size = readInt(in, offset);
        value = -1;
        test = "MapValue: invalid size of  \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = size + 2;
        test = "MapValue: invalid size of  \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid element type */
        offset += arrayElemLens[pos++];
        value = -1;
        test = "MapValue: invalid element type of  \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = TYPE_BINARY;
        test = "MapValue: invalid element type of  \"array\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, ILLEGAL_ARGUMENT);

        /* Invalid element value */
        offset += arrayElemLens[pos++];
        test = "MapValue: invalid element value of  \"array\"";
        refillBuffer(buffer, bufBytes);
        setPackedInt(out, offset, -1);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        refillBuffer(buffer, bufBytes);
        setPackedInt(out, offset, 100);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        final int[] mapElemLens = new int[] {
            4 /* length */,
            4 /* size */,
            3 /* k1 */,
            1 /* type */,
            1 /* k1's value */,
            3 /* k2 */,
            1 /* type */,
            2 /* k2's value */,
            3 /* k3 */,
            1 /* type */,
            2 /* k3's value */,
        };

        pos = 0;
        fname = "map";
        offset = offsets.get(fname);

        /* Invalid value type for map value */
        offset += fname.length() + 1;
        value = TYPE_ARRAY;
        test = "MapValue: invalid value type for field \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, ILLEGAL_ARGUMENT);

        value = TYPE_INTEGER;
        test = "MapValue: invalid value type for field \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid length value of map value */
        length = readInt(in, offset);
        offset++;
        value = -1;
        test = "MapValue: invalid length of  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = length + 1;
        test = "MapValue: invalid length of  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = length - 1;
        test = "MapValue: invalid length of  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid size value of map value */
        offset += mapElemLens[pos++];
        size = readInt(in, offset);
        value = -1;
        test = "MapValue: invalid size of  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = size + 2;
        test = "MapValue: invalid size of  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid key */
        offset += mapElemLens[pos++];
        svalue = null;
        test = "MapValue: invalid key  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        setString(out, offset, svalue);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid element type */
        offset += mapElemLens[pos++];
        value = -1;
        test = "MapValue: invalid element type of  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = TYPE_BINARY;
        test = "MapValue: invalid element type of  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, ILLEGAL_ARGUMENT);

        /* Invalid element value */
        offset += mapElemLens[pos++];
        test = "MapValue: invalid element value of  \"map\"";
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, -1);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        test = "MapValue: invalid element value of  \"map\"";
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, 0);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = TYPE_STRING;
        test = "MapValue: invalid element value of  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset - 1, value);
        setString(out, offset, null);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = TYPE_STRING;
        test = "MapValue: invalid element value of  \"map\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset - 1, value);
        setString(out, offset, "");
        executeRequest(test, buf, ILLEGAL_ARGUMENT);

        /* Record value */
        final int[] recordElemLens = new int[] {
            4 /*length*/,
            4 /*size*/,
            3 /*record.ri's name*/,
            1 /*record.ri's type*/,
            2 /*record.ri's value*/,
            3 /*record.rs's name*/,
            1 /*record.rs's type*/,
            13 /*record.rs's value*/,
        };

        pos = 0;
        fname = "record";
        offset = offsets.get(fname);

        /* Invalid value type for RECORD */
        offset += fname.length() + 1;
        value = TYPE_INTEGER;
        test = "MapValue: invalid value type for field \"record\", " + value ;
        refillBuffer(buffer, bufBytes);
        buffer.setByte(offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid length value of RECORD */
        length = readInt(in, offset);
        offset++;
        value = -1;
        test = "MapValue: invalid length of  \"record\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = length + 1;
        test = "MapValue: invalid length of  \"record\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = length - 1;
        test = "MapValue: invalid length of  \"record\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid size value of record */
        offset += recordElemLens[pos++];
        size = readInt(in, offset);
        value = -1;
        test = "MapValue: invalid size of  \"record\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        value = size + 2;
        test = "MapValue: invalid size of  \"record\", " + value ;
        refillBuffer(buffer, bufBytes);
        setInt(out, offset, value);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        /* Invalid field name */
        offset += recordElemLens[pos++];
        svalue = null;
        test = "MapValue: invalid field name of  \"record\", " + value ;
        refillBuffer(buffer, bufBytes);
        setString(out, offset, svalue);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        svalue = "";
        test = "MapValue: invalid field name of  \"record\", " + value ;
        refillBuffer(buffer, bufBytes);
        setString(out, offset, svalue);
        executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);
    }

    /**
     * Test bad protocol data on below values:
     *  1. Version
     */
    @Test
    public void testPutIfVersionRequst() {

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
            1   /* TTL: packed long (-1) */,
            51  /* Version: byte array */
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, putIfVersionRequest);

        try {
            String test;
            int offset = 0;

            test = "OK test";
            executeRequest(test, buf, 0);

            /*
             * Version
             */
            for (int i = 0; i < lengths.length - 1; i++) {
                offset += lengths[i];
            }
            byte[] versionBytes = null;
            test = "Version: null";
            refillBuffer(buf, bufBytes);
            setByteArray(out, offset, versionBytes);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            versionBytes = new byte[0];
            test = "Version: empty byte array";
            refillBuffer(buf, bufBytes);

            setByteArray(out, offset, versionBytes);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            versionBytes = genBytes(10, null);
            test = "Version: invalid binary format";
            refillBuffer(buf, bufBytes);
            setByteArray(out, offset, versionBytes);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        } catch (IOException ioe) {
            fail("Write failed: " + ioe.getMessage());
        } finally {
            out.close();
        }
    }

    /**
     * Test bad protocol data on below values:
     *  1. Consistency
     *  2. PrimaryKey type
     */
    @Test
    public void testGetRequest() {

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

            test = "OK test";
            executeRequest(test, buf, 0);

            /*
             * Consistency
             */

            /* Move to offset of consistency */
            for (pos = 0; pos < 4; pos++) {
                offset += lengths[pos];
            }

            /* Invalid consistency type */
            int value = -1;
            test = "Invalid consistency type: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            value = 3;
            test = "Invalid consistency type: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * PrimaryKey
             */
            offset += lengths[pos++];

            value = -1;
            test = "Invalid value type of PrimaryKey: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            value = TYPE_ARRAY;
            test = "Invalid value type of PrimaryKey: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        } finally {
            out.close();
        }
    }

    /**
     * Test bad protocol on below values:
     *  1. MaxWriteKB
     *  2. ContinuationKey
     */
    @Test
    public void testMultiDeleteRequest() {
        final int[] lengths = {
            2  /* SerialVersion: short */,
            1  /* OpCode: byte */,
            3  /* RequestTimeout: packed int */,
            6  /* TableName: string */,
            1  /* Durability: one byte */,
            14 /* FieldValue: MapValue */,
            1  /* HasFieldRange: boolean */,
            3  /* MaxWriteKB: packed int */,
            21 /* ContinuationKey: byte array */
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, multiDeleteRequest);

        try {
            String test;
            int pos;
            int offset = 0;

            test = "OK test";
            executeRequest(test, buf, 0);

            /* Move to offset of MaxWriteKB */
            for (pos = 0; pos < 7; pos++) {
                offset += lengths[pos];
            }

            /*
             * MaxWriteKB
             */
            int value = -1;
            test = "Invalid maxWriteKB: " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            if (!onprem) {
                value = rlimits.getRequestWriteKBLimit() + 1;
                test = "Invalid maxWriteKB: " + value;
                refillBuffer(buf, bufBytes);
                setPackedInt(out, offset, value);
                executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);
            }

            /*
             * Continuation Key
             */
            offset += lengths[pos];
            value = -2;
            test = "Invalid continuation key: " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            value = 100;
            test = "Invalid continuation key: " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        } catch (IOException ioe) {
            fail("Failed to write to buffer: " + ioe);
        } finally {
            out.close();
        }
    }

    /**
     * Test bad protocol on below values:
     *  1. Statement
     */
    @Test
    public void testPrepareStatement() {
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

            test = "OK test";
            executeRequest(test, buf, 0);

            /*
             * Statement
             */
            for (pos = 0; pos < 3; pos++) {
                offset += lengths[pos];
            }

            String svalue = null;
            test = "Invalid statement: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            svalue = "";
            test = "Invalid statement: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            int value = statement.length() + 1;
            test = "Invalid statement value, its length is " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, statement.length() + 1);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            value = -2;
            test = "Invalid statement value, its length is " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        } catch (IOException ioe) {
            fail("Failed to write to buffer: " + ioe);
        } finally {
            out.close();
        }
    }

    /**
     * Test bad protocol on below values:
     *  1. PreparedStatement
     *  2. Variables Number
     *  3. Variable Name
     *  4. Variable Value
     */
    @Test
    public void testQueryRequest() {
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

            test = "OK test";
            executeRequest(test, buf, 0);

            /*
             * PreparedStatement
             */
            for (pos = 0; pos < 15; pos++) {
                offset += lengths[pos];
            }

            int value = -1;
            test = "Invalid prepared Statement";
            refillBuffer(buf, bufBytes);
            setInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            value = 0;
            test = "Invalid prepared Statement";
            refillBuffer(buf, bufBytes);
            setInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * Variables number
             */
            value = -1;
            offset += lengths[pos++];
            test = "Invalid variable number: " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            value = 2;
            test = "Invalid variable number: " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * Variable name
             */
            offset += lengths[pos++];
            String svalue = null;
            test = "Invalid variable name: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            svalue = "";
            test = "Invalid variable name: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * Variable value
             */
            offset += lengths[pos++];
            value = -1;
            test = "Invalid variable value type: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            value = TYPE_ARRAY;
            test = "Invalid variable value type: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeRequest(test, buf, ILLEGAL_ARGUMENT);

        } catch (IOException ioe) {
            fail("Failed to write to buffer: " + ioe);
        } finally {
            out.close();
        }
    }

    /**
     * Test bad protocol on below values:
     *  1. Operation number
     *  2. OpCode of sub request
     */
    @Test
    public void testWriteMultipleRequest() {
        final int[] lengths = {
            2   /* SerialVersion: short */,
            1   /* OpCode: byte */,
            3   /* RequestTimeout: packed int */,
            6   /* TableName: string */,
            1   /* OperationNum: packed int */,
            1   /* Durability: one byte */,
            1   /* isAbortIfUnsuccessful: boolean */,
            253 /* Request */
        };

        final WriteMultipleRequest umReq = writeMultipleRequest;
        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, umReq);

        try {
            String test;
            int pos;
            int offset = 0;

            test = "OK test";
            executeRequest(test, buf, 0);

            /*
             * Operation number
             */
            for (pos = 0; pos < 4; pos++) {
                offset += lengths[pos];
            }

            int value = -1;
            test = "Invalid operation number: " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            value = 3;
            test = "Invalid operation number: " + value;
            refillBuffer(buf, bufBytes);
            setPackedInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            /* durability */
            value = 4; /* bad: only one of three values set */
            offset += lengths[pos++];
            test = "Invalid durability: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);
            /*
             * OpCode of sub request
             */
            offset += lengths[pos++]; /* isAbortIfUnsuccessful */
            offset += lengths[pos++];

            value = -1;
            test = "Invalid operation code: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            value = OpCode.GET.ordinal();
            test = "Invalid operation code: " + value;
            refillBuffer(buf, bufBytes);
            buf.setByte(offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);
        } catch (IOException ioe) {
            fail("Failed to write to buffer: " + ioe);
        } finally {
            out.close();
        }
    }

    /**
     * Test bad protocol on below values:
     *  1. ReadKB
     *  2. WriteKB
     *  3. StorageGB
     */
    @Test
    public void testTableRequest() {
        final int[] lengths = {
            2   /* SerialVersion: short */,
            1   /* OpCode: byte */,
            3   /* RequestTimeout: packed int */,
            215 /* Statement: string */,
            1   /* HasLimit: boolean */,
            4   /* ReadKB: int */,
            4   /* WriteKB: int */,
            4   /* StorageGB: int */,
            1   /* LimitsMode: byte */,
            1   /* HasTableName: boolean */
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, tableRequest);

        try {
            String test;
            int pos;
            int offset = 0;

            test = "OK test";
            executeRequest(test, buf, 0);

            /*
             * ReadKB
             */
            for (pos = 0; pos < 5; pos++) {
                offset += lengths[pos];
            }

            int value = 0;
            test = "Invalid readKB: " + value;
            refillBuffer(buf, bufBytes);
            setInt(out, offset, value);
            executeRequest(test, buf, ILLEGAL_ARGUMENT);

            /*
             * WriteKB
             */
            offset += lengths[pos++];
            value = 0;
            test = "Invalid writeKB: " + value;
            refillBuffer(buf, bufBytes);
            setInt(out, offset, value);
            executeRequest(test, buf, ILLEGAL_ARGUMENT);

            /*
             * StorageMaxGB
             */
            offset += lengths[pos++];
            value = 0;
            test = "Invalid StorageMaxGB: " + value;
            refillBuffer(buf, bufBytes);
            setInt(out, offset, value);
            executeRequest(test, buf, ILLEGAL_ARGUMENT);

        } catch (Exception ioe) {
            fail("Failed to write to buffer: " + ioe);
        } finally {
            out.close();
        }
    }

    @Test
    public void testGetIndexesRequest() {
        final int[] lengths = {
            2   /* SerialVersion: short */,
            1   /* OpCode: byte */,
            3   /* RequestTimeout: packed int */,
            6   /* TableName: string */,
            1   /* HasIndex: boolean */,
            5   /* IndexName: string */
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, getIndexesRequest);

        try {
            String test;
            int pos;
            int offset = 0;

            test = "OK test";
            executeRequest(test, buf, 0);

            /*
             * Index name
             */
            for (pos = 0; pos < 5; pos++) {
                offset += lengths[pos];
            }

            String svalue = null;
            test = "Invalid Index name: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            svalue = "";
            test = "Invalid Index name: " + svalue;
            refillBuffer(buf, bufBytes);
            setString(out, offset, svalue);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        } catch (Exception ioe) {
            fail("Failed to write to buffer: " + ioe);
        } finally {
            out.close();
        }
    }

    @Test
    public void testListTablesRequest() {
        final int[] lengths = {
            2   /* SerialVersion: short */,
            1   /* OpCode: byte */,
            3   /* RequestTimeout: packed int */,
            4   /* StartIndex: int */,
            4   /* Limit: int*/
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final byte[] bufBytes = serializeRequest(out, listTablesRequest);

        try {
            String test;
            int pos;
            int offset = 0;

            test = "OK test";
            executeRequest(test, buf, 0);

            /*
             * Start index
             */
            for (pos = 0; pos < 3; pos++) {
                offset += lengths[pos];
            }

            int value = -1;
            test = "Invalid start index: " + value;
            refillBuffer(buf, bufBytes);
            setInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

            /*
             * Limit
             */
            offset += lengths[pos++];
            test = "Invalid limit: " + value;
            refillBuffer(buf, bufBytes);
            setInt(out, offset, value);
            executeRequest(test, buf, BAD_PROTOCOL_MESSAGE);

        } catch (Exception ioe) {
            fail("Failed to write to buffer: " + ioe);
        } finally {
            out.close();
        }
    }

    @Test
    public void testBrokenRequest() {
        final Request[] requests = new Request[] {
            getRequest,
            putRequest,
            putIfVersionRequest,
            deleteRequest,
            deleteIfVersionRequest,
            multiDeleteRequest,
            writeMultipleRequest,
            prepareRequest,
            createQueryWithBoundStmtRequest(),
            tableRequest,
            tableSetLimitsRequest,
            getIndexesRequest,
            getTableRequest,
            listTablesRequest,
            tableUsageRequest
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        for (Request request : requests) {
            buf.clear();
            serializeRequest(out, request);
            testBrokenMessage(request.getClass().getName(), buf, 1,
                              BAD_PROTOCOL_MESSAGE);
        }
        out.close();
    }

    /*
     * TODO: Enable this test after enhance the validation check especially
     * for serialized PreparedStatment.
     */
    @Ignore
    public void testRandomCorruptedRequest() {
        final Request[] requests = new Request[] {
            getRequest,
            putRequest,
            putIfVersionRequest,
            deleteRequest,
            deleteIfVersionRequest,
            multiDeleteRequest,
            writeMultipleRequest,
            prepareRequest,
            createQueryWithBoundStmtRequest(),
            tableRequest,
            tableSetLimitsRequest,
            getIndexesRequest,
            getTableRequest,
            listTablesRequest,
            tableUsageRequest
        };

        final ByteOutputStream out = new NettyByteOutputStream(buf);
        final Random rand = new Random(System.currentTimeMillis());
        final int round = 10;

        for (Request request : requests) {
            buf.clear();
            byte[] bufBytes = serializeRequest(out, request);
            for (int i = 0; i < round; i++) {
                final int offset = rand.nextInt(buf.writerIndex() - 2);
                byte[] corruptedBytes = corruptBuffer(buf, rand, offset);
                executeRequest(request.getClass().getName(),
                               buf,
                               -1/* don't check error code */,
                               new ExecuteFailHandler() {
                                   @Override
                                   public void fail(String test, Throwable t) {
                                       printBytes(test + " offset=" + offset,
                                                  corruptedBytes);
                                   }
                               }
                );
                refillBuffer(buf, bufBytes);
            }
        }
        out.close();
    }

    /*
     * Test the check on request size limit on proxy.
     */
    @Test
    public void testRequestSizeLimit() {
        assumeTrue(onprem == false);

        int limit = rlimits.getRequestSizeLimit();
        final ByteOutputStream out = new NettyByteOutputStream(buf);

        try {
            String test = "Put request with size > " + limit;
            PutRequest putReq = new PutRequest()
                .setTableName(tableName)
                .setValue(new MapValue()
                          .put("id", 0)
                          .put("key", genBytes(limit, null)));

            serializeRequest(out, putReq);
            executeRequest(test, buf, REQUEST_SIZE_LIMIT_EXCEEDED);
        } finally {
            out.close();
        }
    }

    private byte[] corruptBuffer(ByteBuf buffer, Random rand, int offset) {
        int len = rand.nextInt(buffer.writerIndex() - offset);
        byte[] bytes = genBytes(len, rand);
        buffer.setBytes(offset, bytes);
        return bytes;
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

    private void testBrokenMessage(String name,
                                   ByteBuf buffer,
                                   int offset,
                                   int errCode) {
        for (int i = 0; i < buffer.writerIndex() - 1; i++) {
            buffer.readerIndex(0);
            buffer.writerIndex(offset + i);
            executeRequest("testBrokenMessage - " + name + ": " +
                           buffer.writerIndex(), buffer, errCode);
        }
    }

    private void executeRequest(String test, ByteBuf buffer, int expErrCode) {
        executeRequest(test, buffer, expErrCode, null);
    }

    private void executeRequest(String test,
                                ByteBuf buffer,
                                int expErrCode,
                                ExecuteFailHandler failHandler) {

        ResponseHandler responseHandler = null;
        ByteInputStream bis = null;

        /* Increase reference count of buffer by 1*/
        buffer.retain();

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

            httpClient.runRequest(request, responseHandler, channel);

            assertFalse("Request timed out after " + timeoutMs + " ms",
                        responseHandler.await(timeoutMs));
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
            if (failHandler != null) {
                failHandler.fail(test, t);
            }
            fail(test + " failed: " + t);
        } finally {
            if (bis != null) {
                bis.close();
            }
            if (responseHandler != null) {
                responseHandler.close();
            }
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

    private void setByteArray(ByteOutputStream out, int offset, byte[] bytes)
        throws IOException {

        int savedOffset = out.getOffset();
        out.setWriteIndex(offset);
        SerializationUtil.writeByteArray(out, bytes);
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

    private Version genVersion() {
        final UUID uuid = UUID.randomUUID();
        final long vlsn = 123456789;
        final long lsn = 0x1234567812345678L;
        final RepNodeId repNodeId = new RepNodeId(1234, 1234);
        final oracle.kv.Version kvVersion =
            new oracle.kv.Version(uuid, vlsn, repNodeId, lsn);
        return Version.createVersion(kvVersion.toByteArray());
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

    private static void printBytes(String title, byte[] bytes) {
        final char[] hexArray = "0123456789ABCDEF".toCharArray();
        StringBuilder sb = new StringBuilder(title);
        sb.append("[");
        sb.append(bytes.length);
        sb.append("]");
        for (int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            if (j % 5 == 0) {
                sb.append("\n\t");
            }
            sb.append("(byte)0x");
            sb.append(hexArray[v >>> 4]);
            sb.append(hexArray[v & 0x0F]);
            sb.append(", ");
        }
        System.out.println(sb.toString());
    }

    /**
     * For debug purpose.
     */
    @SuppressWarnings("unused")
    private byte[] corruptBuffer(ByteBuf buffer) {
        int offset = 18;
        byte[] bytes = new byte[] {
(byte)0xA1, (byte)0x76, (byte)0x46, (byte)0x11, (byte)0x0C,
(byte)0xD8, (byte)0x25, (byte)0x66,
        };
        buffer.setBytes(offset, bytes);
        return bytes;
    }

    /*
     * Interface invoked by executeRequest() when fails.
     */
    @FunctionalInterface
    private interface ExecuteFailHandler {
        void fail(String test, Throwable ex);
    }
}
