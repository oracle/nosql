/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import java.io.IOException;

import oracle.nosql.nson.Nson;
import oracle.nosql.nson.util.ByteInputStream;

/**
 * A class that allows a caller to walk an NSON MAP using a "pull"
 * pattern. The ByteInputStream used for construction must be positioned
 * on an NSON MAP. Each call to {@link MapWalker#next} moves the state
 * forward. The value of each map entry <b>must</b> be consumed by
 * the caller either by reading it or skipping it by calling
 * {@link MapWalker#skip}. A typical usage pattern is:
 * <pre>
 *   MapWalker walker = new MapWalker(stream);
 *   while (walker.hasNext()) {
 *       walker.next();
 *       String name = walker.getName(); // returns name of current element
 *       // read or skip value
 *       if (name.equals("interesting_int")) {
 *           Nson.readNsonInt(stream);
 *       else {
 *           walker.skip();
 *       }
 *   }
 * </pre>
 * @hide-in-sdk
 */
public class MapWalker {
    /* this exists to prevent an infinite loop for bad serialization */
    private final static int MAX_NUM_ELEMENTS = 1_000_000_000;
    private final ByteInputStream bis;
    private final int numElements;
    private String currentName;
    private int currentIndex = 0;

    /**
     * Constructs a MapWalker from a stream
     * @param bis a ByteInputStream
     * @throws IllegalArgumentException if the stream is not positioned
     * on a MAP
     * @throws IOException if there's a problem with the stream
     */
    public MapWalker(ByteInputStream bis) throws IOException {
        this.bis = bis;
        int t = bis.readByte();
        /* must be map */
        if (t != Nson.TYPE_MAP) {
            throw new IllegalArgumentException(
                "MapWalker: Stream must point to a MAP, it points to " +
                Nson.typeString(t));
        }
        bis.readInt(); // total length of map in bytes
        numElements = bis.readInt();
        if (numElements < 0 || numElements > MAX_NUM_ELEMENTS) {
            throw new IllegalArgumentException(
                "Invalid number of map elements: " + numElements);
        }
    }

    /**
     * Returns the name of the current element in the map
     * @return the element name
     */
    public String getCurrentName() {
        return currentName;
    }

    /**
     * Returns the current index into the map
     * @return the index
     */
    public int getCurrentIndex() {
        return currentIndex;
    }

    /**
     * Returns the current offset of the underlying ByteInputStream
     *
     * @return the offset
     */
    public int getStreamOffset() {
        return bis.getOffset();
    }

    /**
     * Resets current index and stream state, allowing a MapWalker to
     * be reset to an earlier state. The current name will not be set
     * correctly until a {@link MapWalker#next} operation is performed.
     * <p>
     * This operation can be used in conjunction with
     * {@link MapWalker#getCurrentIndex} and {@link MapWalker#getStreamOffset}.
     * The stream should be positioned at the <b>beginning</b> of a map
     * entry, e.g. at the name, ready for the {@link MapWalker#next} call to be
     * made. The caller acquires the current index and offset and can then
     * manipulate the MapWalker as needed, after which is can call this method
     * with the saved offset and index to reset the state. The positioning of
     * the stream isn't enforced, but the caller must know the state so that
     * if it continues to use the MapWalker after reset it does not fail.
     *
     * @param index index into the map
     * @param streamOffset offset to use into the input stream
     */
    public void reset(int index, int streamOffset) {
        currentIndex = index;
        bis.setOffset(streamOffset);
    }

    /**
     * Returns the number of elements in the map
     * @return the number of elements
     */
    public int getNumElements() {
        return numElements;
    }

    /**
     * Returns the stream being walked
     * @return the stream
     */
    public ByteInputStream getStream() {
        return bis;
    }

    /**
     * Returns true if the map has more elements
     * @return true if the map has more elements, false otherwise
     */
    public boolean hasNext() {
        return (currentIndex < numElements);
    }

    /**
     * Moves the state forward to the next element in the map.
     * @throws IllegalArgumentException if called when the map has
     * no more elements
     * @throws IOException if there's a problem with the stream
     */
    public void next() throws IOException {
        if (currentIndex >= numElements) {
            throw new IllegalArgumentException(
                "Cannot call next with no elements remaining");
        }
        currentName = Nson.readString(bis);
        currentIndex++;
    }

    /**
     * Skips the value of the element at which the stream is positioned.
     * If called when the map has no further elements or without calling
     * {@link #next} the result is unpredictable and the stream state
     * will be undefined.
     * @throws IOException if there's a problem with the stream
     */
    public void skip() throws IOException {
        Nson.generateEventsFromNson(null, bis, true);
    }

    /**
     * Searches the map looking for a matching field, optionally
     * case-sensitive or case-insensitive. If
     * the field is found the method returns true and the stream is positioned
     * at the field's value. If the field does not exist the method returns
     * false and the stream is at the end of the map. In both cases it is
     * likely that the caller will want to reset the stream in the walker using
     * the {@link MapWalker#reset} method, using information saved before
     * this call because this call does not preserve the current location in
     * the stream.
     * <p>
     * When this call is made the stream must be positioned at the start of a
     * field, on the field name, or it will fail with an unpredictable
     * exception.
     * <p>
     * The field name is a single component and only the current map will be
     * searched. No navigation is performed.
     *
     * @param fieldName the field to look for
     * @param caseSensitive set to true for a case-sensitive search
     * @return true if the field is found, false otherwise
     */
    public boolean find(String fieldName,
                        boolean caseSensitive) throws IOException {
        while (hasNext()) {
            next();
            boolean found = (caseSensitive ?
                             fieldName.equals(currentName) :
                             fieldName.equalsIgnoreCase(currentName));
            if (found) {
                return true;
            } else {
                skip();
            }
        }
        return false;
    }
}
