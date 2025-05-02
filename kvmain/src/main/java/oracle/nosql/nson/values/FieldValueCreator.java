/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Stack;

/**
 * An instance of FieldValueEventHandler that accepts events and constructs
 * a {@link FieldValue} instance. This is typically used to create instances
 * from NSON events.
 *
 * In order to handle creation of nested complex types such as maps and
 * arrays stacks are maintained.

 * The current FieldValue instance is available using the
 * getCurrentValue() method.
 * @hide-in-sdk
 */
public class FieldValueCreator implements FieldValueEventHandler {

    private Stack<MapValue> mapStack;
    private Stack<ArrayValue> arrayStack;

    /*
     * A stack of map keys is needed to handle the situation where maps
     * are nested.
     */
    private Stack<String> keyStack;
    private MapValue currentMap;
    private ArrayValue currentArray;
    private String currentKey;
    private FieldValue currentValue;

    private void pushMap(MapValue map) {
        if (currentMap != null) {
            if (mapStack == null) {
                mapStack = new Stack<MapValue>();
            }
            mapStack.push(currentMap);
        }
        currentMap = map;
        currentValue = map;
    }

    private void pushArray(ArrayValue array) {
        if (currentArray != null) {
            if (arrayStack == null) {
                arrayStack = new Stack<ArrayValue>();
            }
            arrayStack.push(currentArray);
        }
        currentArray = array;
        currentValue = array;
    }

    private void pushKey(String key) {
        if (currentKey != null) {
            if (keyStack == null) {
                keyStack = new Stack<String>();
            }
            keyStack.push(currentKey);
        }
        currentKey = key;
    }

    /**
     * Returns the current FieldValue if available
     *
     * @return the current value
     */
    public FieldValue getCurrentValue() {
        return currentValue;
    }

    @Override
    public void startMap(int size) throws IOException {
        /* maintain insertion order */
        pushMap(new MapValue(true, size));
    }

    @Override
    public void startArray(int size) throws IOException {
        pushArray(new ArrayValue(size));
    }

    @Override
    public void endMap(int size) throws IOException {
        /*
         * The in-process map becomes the currentValue
         */
        currentValue = currentMap;
        if (mapStack != null && !mapStack.empty()) {
            currentMap = mapStack.pop();
        } else {
            currentMap = null;
        }
    }

    @Override
    public void endArray(int size) throws IOException {
        /*
         * The in-process array becomes the currentValue
         */
        currentValue = currentArray;
        if (arrayStack != null && !arrayStack.empty()) {
            currentArray = arrayStack.pop();
        } else {
            currentArray = null;
        }
    }

    @Override
    public boolean startMapField(String key) throws IOException {
        pushKey(key);
        return false; /* don't skip */
    }

    @Override
    public void endMapField(String key) throws IOException {
        /*
         * currentMap could be null if a subclass has suppressed
         * creation of a wrapper map for the entire FieldValue.
         * The "finder" code might do this
         */
        if (currentKey != null && currentMap != null) {
            currentMap.put(currentKey, currentValue);
        }
        if (keyStack != null && !keyStack.empty()) {
            currentKey = keyStack.pop();
        } else {
            currentKey = null;
        }
        /* currentValue undefined right now... */
    }

    @Override
    public void endArrayField(int index) throws IOException {
        if (currentArray != null) {
            currentArray.add(currentValue);
        }
    }

    @Override
    public void booleanValue(boolean value) throws IOException {
        currentValue = BooleanValue.getInstance(value);
    }

    @Override
    public void binaryValue(byte[] byteArray) throws IOException {
        currentValue = new BinaryValue(byteArray);
    }

    @Override
    public void binaryValue(byte[] byteArray, int offset, int length)
        throws IOException {
        /* TODO: BinaryValue() with offset/length */
        currentValue = new BinaryValue(byteArray);
    }

    @Override
    public void stringValue(String value) throws IOException {
        currentValue = new StringValue(value);
    }

    @Override
    public void integerValue(int value) throws IOException {
        currentValue = new IntegerValue(value);
    }

    @Override
    public void longValue(long value) throws IOException {
        currentValue = new LongValue(value);
    }

    @Override
    public void doubleValue(double value) throws IOException {
        currentValue = new DoubleValue(value);
    }

    @Override
    public void numberValue(BigDecimal value) throws IOException {
        currentValue = new NumberValue(value);
    }

    @Override
    public void timestampValue(TimestampValue timestamp) {
        currentValue = timestamp;
    }

    @Override
    public void jsonNullValue() throws IOException {
        currentValue = JsonNullValue.getInstance();
    }

    @Override
    public void nullValue() throws IOException {
        currentValue = NullValue.getInstance();
    }

    @Override
    public void emptyValue() throws IOException {
        currentValue = EmptyValue.getInstance();
    }
}
