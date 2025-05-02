/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.query;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;

import oracle.kv.StaticClientTestBase;
import oracle.kv.impl.api.table.BinaryValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for FieldValue fromJson methods.
 */
public class FieldValueTest
    extends StaticClientTestBase {

    @BeforeClass
    public static void mySetUp()
        throws Exception {
        //staticSetUp("exceptionTestStore", false);
    }

    @AfterClass
    public static void myTearDown()
        throws Exception {
        //staticTearDown();
    }

    @Test
    public void testInteger()
        throws IOException {
        FieldDef type = FieldDefImpl.Constants.integerDef;
        int exp = 5;
        String str = "5";

        FieldValue v;

        v = FieldValueFactory.createInteger(exp);
        Assert.assertEquals(exp, v.asInteger().get());

        v = FieldValueFactory.createValueFromJson(type, str);
        Assert.assertEquals(exp, v.asInteger().get());

        v = FieldValueFactory.createValueFromJson(type,
            new ByteArrayInputStream(str.getBytes("UTF-8")));
        Assert.assertEquals(exp, v.asInteger().get());

        v = FieldValueFactory.createValueFromJson(type,
            new StringReader(str));
        Assert.assertEquals(exp, v.asInteger().get());
    }

    @Test
    public void testLong()
        throws IOException {
        FieldDef type = FieldDefImpl.Constants.longDef;
        long exp = 999999999999999l;
        String str = "999999999999999";

        FieldValue v;

        v = FieldValueFactory.createLong(exp);
        Assert.assertEquals(exp, v.asLong().get());

        v = FieldValueFactory.createValueFromJson(type, str);
        Assert.assertEquals(exp, v.asLong().get());

        v = FieldValueFactory.createValueFromJson(type,
            new ByteArrayInputStream(str.getBytes("UTF-8")));
        Assert.assertEquals(exp, v.asLong().get());

        v = FieldValueFactory.createValueFromJson( type,
            new StringReader(str));
        Assert.assertEquals(exp, v.asLong().get());
    }

    @Test
    public void testFloat()
        throws IOException {
        FieldDef type = FieldDefImpl.Constants.floatDef;
        float exp = 3.1415f;
        String str = "3.1415";

        FieldValue v;

        v = FieldValueFactory.createFloat(exp);
        Assert.assertEquals(exp, v.asFloat().get(), 0.001);

        v = FieldValueFactory.createValueFromJson(type, str);
        Assert.assertEquals(exp, v.asFloat().get(), 0.001);

        v = FieldValueFactory.createValueFromJson(type,
            new ByteArrayInputStream(str.getBytes("UTF-8")));
        Assert.assertEquals(exp, v.asFloat().get(), 0.001);

        v = FieldValueFactory.createValueFromJson( type,
            new StringReader(str));
        Assert.assertEquals(exp, v.asFloat().get(), 0.001);

    }

    @Test
    public void testDouble()
        throws IOException {
        FieldDef type = FieldDefImpl.Constants.doubleDef;
        double exp = 0.1234567891011121314d;
        String str = "0.1234567891011121314";

        FieldValue v;

        v = FieldValueFactory.createDouble(exp);
        Assert.assertEquals(exp, v.asDouble().get(), 0.001);

        v = FieldValueFactory.createValueFromJson(type, str);
        Assert.assertEquals(exp, v.asDouble().get(), 0.001);

        v = FieldValueFactory.createValueFromJson(type,
            new ByteArrayInputStream(str.getBytes("UTF-8")));
        Assert.assertEquals(exp, v.asDouble().get(), 0.001);

        v = FieldValueFactory.createValueFromJson( type,
            new StringReader(str));
        Assert.assertEquals(exp, v.asDouble().get(), 0.001);
    }

    @Test
    public void testBoolean()
        throws IOException {
        FieldDef type = FieldDefImpl.Constants.booleanDef;
        boolean exp = true;
        String str = "true";

        FieldValue v;

        v = FieldValueFactory.createBoolean(exp);
        Assert.assertEquals(exp, v.asBoolean().get());

        v = FieldValueFactory.createValueFromJson(type, str);
        Assert.assertEquals(exp, v.asBoolean().get());

        v = FieldValueFactory.createValueFromJson(type,
            new ByteArrayInputStream(str.getBytes("UTF-8")));
        Assert.assertEquals(exp, v.asBoolean().get());

        v = FieldValueFactory.createValueFromJson( type,
            new StringReader(str));
        Assert.assertEquals(exp, v.asBoolean().get());


        exp = false;
        str = "false";

        v = FieldValueFactory.createBoolean(exp);
        Assert.assertEquals(exp, v.asBoolean().get());

        v = FieldValueFactory.createValueFromJson(type, str);
        Assert.assertEquals(exp, v.asBoolean().get());

        v = FieldValueFactory.createValueFromJson( type,
            new ByteArrayInputStream(str.getBytes("UTF-8")));
        Assert.assertEquals(exp, v.asBoolean().get());

        v = FieldValueFactory.createValueFromJson( type,
            new StringReader(str));
        Assert.assertEquals(exp, v.asBoolean().get());
    }

    @Test
    public void testString()
        throws IOException {
        FieldDef type = FieldDefImpl.Constants.stringDef;
        String str = "\"ole\"";
        String exp = "ole";

        FieldValue v;

        v = FieldValueFactory.createString(exp);
        Assert.assertEquals(exp, v.asString().get());

        v = FieldValueFactory.createValueFromJson(type, str);
        Assert.assertEquals(exp, v.asString().get());

        v = FieldValueFactory.createValueFromJson(type,
            new ByteArrayInputStream(str.getBytes("UTF-8")));
        Assert.assertEquals(exp, v.asString().get());

        v = FieldValueFactory.createValueFromJson( type,
            new StringReader(str));
        Assert.assertEquals(exp, v.asString().get());
    }

    @Test
    public void testBinary()
        throws IOException {
        FieldDef type = FieldDefImpl.Constants.binaryDef;
        String str = "\"b2xl\"";
        byte[] exp = "ole".getBytes();


        StringBuilder sb = new StringBuilder();
        BinaryValueImpl bvi = FieldDefImpl.Constants.binaryDef.createBinary(
            "ole".getBytes());
        bvi.toStringBuilder(sb);

        FieldValue v;

        v = FieldValueFactory.createBinary(exp);
        Assert.assertArrayEquals(exp, v.asBinary().get());

        v = FieldValueFactory.createValueFromJson(type, str);
        Assert.assertArrayEquals(exp, v.asBinary().get());

        v = FieldValueFactory.createValueFromJson(type,
            new ByteArrayInputStream(str.getBytes("UTF-8")));
        Assert.assertArrayEquals(exp, v.asBinary().get());

        v = FieldValueFactory.createValueFromJson( type,
            new StringReader(str));
        Assert.assertArrayEquals(exp, v.asBinary().get());
    }
}
