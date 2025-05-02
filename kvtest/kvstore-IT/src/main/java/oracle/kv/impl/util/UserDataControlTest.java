/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.Depth;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.ReturnValueVersion;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.MultiGet;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import org.junit.Test;

/**
 */
public class UserDataControlTest extends TestBase{

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
    }

    @Test
    public void testVisiblity() throws Exception {
        validateVisibility("UserStuff", "User", "my toys");
        validateVisibility("", "", "");
    }

    private void validateVisibility(String keyContents,
                                    String rangePrefix,
                                    String valueContent) {
        String expectedKey = "/" + keyContents;
        List<String> majorPath = new ArrayList<String>();
        List<String> minorPath = new ArrayList<String>();

        majorPath.add(keyContents);
        Key key = Key.createKey(majorPath, minorPath);
        String hashKeyString = UserDataControl.getHash(key.toByteArray());
        assertEquals(hashKeyString,
                     UserDataControl.displayKey(key));
        assertEquals(hashKeyString,
                     UserDataControl.displayKey(key.toByteArray()));

        KeyRange range = new KeyRange(rangePrefix);
        String hashRangeString = UserDataControl.getHash(range.toByteArray());
        assertEquals(hashRangeString,
                     UserDataControl.displayKeyRange(range));

        Value value = Value.createValue(valueContent.getBytes());
        String hashValueString = UserDataControl.getHash(value.toByteArray());
        assertEquals("null", UserDataControl.displayValue(null, null));
        assertEquals(hashValueString,
                     UserDataControl.displayValue(value, null));
        assertEquals(hashValueString,
                     UserDataControl.displayValue(value, value.toByteArray()));
        assertEquals(hashValueString,
                     UserDataControl.displayValue(null, value.toByteArray()));

        TableImpl table = TableBuilder.createTableBuilder("testRow")
                                       .addInteger("id")
                                       .addString("lastName")
                                       .primaryKey("id")
                                       .buildTable();
        Row row = table.createRow();
        row.put("id", 1);
        row.put("lastName", "yang");
        String hashRow = UserDataControl.getHash(row.toString().getBytes());
        assertEquals(hashRow, UserDataControl.displayRow(row));
        String hashRowJson =
                UserDataControl.getHash(row.toJsonString(true).getBytes());
        assertEquals(hashRowJson, UserDataControl.displayRowJson(row));

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 2);
        String hashPK =
                UserDataControl.getHash(pk.toJsonString(true).getBytes());
        assertEquals(hashPK, UserDataControl.displayRowJson(pk));

        /*
         * Enable viewing by changing param. Since this is a static, be
         * sure to reset it
         */
        ParameterMap newParams = new ParameterMap();
        newParams.setParameter(ParameterState.COMMON_HIDE_USERDATA, "false");
        ParameterListener paramListener = UserDataControl.getParamListener();
        paramListener.newParameters(null, newParams);

        /* We should be able to view keys, ranges, and values */
        assertEquals(expectedKey, UserDataControl.displayKey(key));
        assertEquals(expectedKey,
                     UserDataControl.displayKey(key.toByteArray()));
        assertEquals("I/" + rangePrefix + "/" + rangePrefix + "/I",
                     UserDataControl.displayKeyRange(range));
        assertEquals(value.toString(),
                     UserDataControl.displayValue(value, null));
        assertEquals(value.toString(),
                     UserDataControl.displayValue(value, value.toByteArray()));
        assertEquals(value.toString(),
                     UserDataControl.displayValue(null, value.toByteArray()));

        /* We should be able to view row and primary key */
        assertEquals(row.toString(), UserDataControl.displayRow(row));
        assertEquals(row.toJsonString(true),
                     UserDataControl.displayRowJson(row));
        assertEquals(pk.toJsonString(true),
                     UserDataControl.displayRowJson(pk));

        /* Reset, because the visibility setting is static */
        newParams.setParameter(ParameterState.COMMON_HIDE_USERDATA,
                               ParameterState.COMMON_HIDE_USERDATA_DEFAULT);
        paramListener.newParameters(null, newParams);
    }
    @Test
    public void testOperations() {
        String keyVal = "Pets";
        Key key = Key.createKey(keyVal);
        String dataVal = "Toonces";
        Value value = Value.createValue(dataVal.getBytes());

        InternalOperation op = new Put(key.toByteArray(), value,
                                       ReturnValueVersion.Choice.NONE);
        assertFalse(op.toString().contains(keyVal));
        assertFalse(op.toString().contains(dataVal));

        op = new MultiGet(key.toByteArray(),
                                            new KeyRange(keyVal),
                                            Depth.PARENT_AND_CHILDREN, false);
        assertFalse(op.toString().contains(keyVal));
        assertFalse(op.toString().contains(dataVal));
    }
}
