/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.JsonEOFException;

import oracle.nosql.nson.JsonParseException;

/**
 * JsonReader reads a file or string that has JSON objects in it that may
 * represent rows and makes them available using the Iterable interface. The
 * source of the JSON, either a File or String, is expected to contain one or
 * more JSON objects, separated by white, or no space. While white space, or no
 * space between objects is explicitly supported, the JSON parser is flexible
 * and forgiving and will silently ignore and skip unexpected characters
 * between JSON objects.
 * <p>
 * As an iteration proceeds it may return any number of valid MapValue
 * instances before it encounters a problem in the input stream such as a bad
 * object or unexpected end of file. It is up to the application to handle
 * error conditions. There is no mechanism for restarting in the middle of
 * a file or string. If a {@link JsonParseException} is thrown it will
 * contain the location of the exception in the parse stream to help isolate
 * problems.
 * <p>
 * An instance of this class can only be used to create a single iterator.
 * This is to ensure that the Iterator can be closed explicitly by the
 * caller. If an attempt is made to create an additional iterator
 * IllegalArgumentException is thrown.
 * <p>
 * Instances of this class must be closed when done to free parser resources.
 * It implements AutoCloseable to assist but in the event it is not called
 * inside a try-with-resources block an explicit close() should be used.
 */
public class JsonReader implements Iterable<MapValue>, AutoCloseable {
    private final MapValueJsonIterator iterator;
    private boolean iteratorUsed;

    /**
     * Creates an iterator over JSON objects in the specified file.
     *
     * @param file the file to be used
     * @param options if desired, this can be null
     */
    public JsonReader(File file, JsonOptions options) {
        iterator = new MapValueJsonIterator(file, options);
    }

    /**
     * Creates an iterator over JSON objects in a string.
     *
     * @param jsonString the String to use
     * @param options if desired, this can be null
     */
    public JsonReader(String jsonString, JsonOptions options) {
        iterator = new MapValueJsonIterator(jsonString, options);
    }

    /**
     * Creates an iterator over JSON objects provided by the
     * InputStream. The caller is responsible for closing the InputStream
     * if necessary.
     *
     * @param input the InputStream to use
     * @param options if desired, this can be null
     */
    public JsonReader(InputStream input, JsonOptions options) {
        iterator = new MapValueJsonIterator(input, options);
    }

    /**
     * Creates an iterator over JSON objects provided by the
     * Reader. The caller is responsible for closing the Reader
     * if necessary.
     *
     * @param input the Reader to use
     * @param options if desired, this can be null
     */
    public JsonReader(Reader input, JsonOptions options) {
        iterator = new MapValueJsonIterator(input, options);
    }

    /**
     * Returns an Iterator&lt;MapValue&gt; over the source provided.
     * This method can only be called once for a given instance of JsonReader.
     * An additional call will result in IllegalArgumentException being thrown.
     *
     * @throws IllegalArgumentException if called twice
     */
    @Override
    public Iterator<MapValue> iterator() {
        if (iteratorUsed) {
            throw new IllegalArgumentException(
                "JsonReader can only create one iterator instance. If " +
                "another is needed, create a new instance of JsonReader");
        }
        iteratorUsed = true;
        return iterator;
    }

    @Override
    public void close() {
        iterator.close();
    }

    /**
     * Returns the byte offset into the JSON being processed if available or
     * -1 if not
     * @return the offset or -1 if not available in the underlying data
     * being parsed
     */
    public long getLocationOffset() {
        return iterator.getLocationOffset();
    }

    static class MapValueJsonIterator implements Iterator<MapValue>,
                                                 AutoCloseable {
        final JsonParser parser;
        private final JsonOptions options;
        boolean closed = false;

        public MapValueJsonIterator(File file, JsonOptions options) {
            this.options = options;
            parser = JsonUtils.createParserWithOptions(file, options);
        }

        public MapValueJsonIterator(String json, JsonOptions options) {
            this.options = options;
            parser = JsonUtils.createParserWithOptions(json, options);
        }

        public MapValueJsonIterator(InputStream input, JsonOptions options) {
            this.options = options;
            parser = JsonUtils.createParserWithOptions(input, options);
        }

        public MapValueJsonIterator(Reader input, JsonOptions options) {
            this.options = options;
            parser = JsonUtils.createParserWithOptions(input, options);
        }

        public long getLocationOffset() {
            return parser.getCurrentLocation().getByteOffset();
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (parser.currentToken() == JsonToken.START_OBJECT) {
                return true;
            }
            while(true) {
                try {
                    JsonToken token = parser.nextToken();

                    if (token == null) {
                        return false;
                    }
                    if (token == JsonToken.START_OBJECT) {
                        return true;
                    }
                } catch (JsonEOFException eof) {
                    return false;
                } catch (IOException ignored) {
                    /* Ignore parsing error, find next START_OBJECT */
                }
            }
        }

        @Override
        public MapValue next() {
            if (hasNext()) {
                return makeValue();
            }
            close();
            return null;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                try {
                    parser.close();
                } catch (IOException ioe) {}
            }
        }

        private MapValue makeValue() {
            FieldValue value = null;
            value = JsonUtils.createValueFromJson(parser,
                                                  false,
                                                  options);
            if (!(value instanceof MapValue)) {
                throw new IllegalArgumentException(
                    "Invalid JSON encountered in input. Input stream must " +
                    "contain only complete objects");
            }
            return (MapValue) value;
        }
    }
}
