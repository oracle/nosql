/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;

import oracle.nosql.nson.Nson;
import oracle.nosql.nson.util.ByteInputStream;

/**
 * This class is used to find paths within an NSON document. Only simple
 * path expressions are supported and are treated as an array of strings,
 * one string for each path component. Future expansion to support
 * arrays or more complex paths may be added.
 * <p>
 * A single instance of the class is create for each search, with some
 * static convenience methods that wrap some searches.
 * <p>
 * A single search can include multiple paths to optimize the case where
 * more than one path needs to be found.
 * <p>
 * Convenience constructors exist for simple cases where:
 * <ul>
 *   <li>only one path is used</li>
 *   <li> none of the path components contain the "." character, which can be
 *  used as a separator </li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>
 *
 *   // seek -- returns true if the specified path is found in the stream.
 *   //   On return the inputStream is positioned at the path if found.
 *   boolean found = PathFinder.seek("a.b.c", inputStream);
 *
 *   // find a single path, no handler set, stop when found
 *   PathFinder pf = new PathFinder(null, new String[]{"a", "b", "c"});
 *   pf.find(inputStream,
 *           (PathFinder pf, String[] path) -&gt; {
 *              System.out.println("Path found");
 *              pf.stop();
 *              return false;
 *           }
 *
 *   // find multiple paths, use a JsonSerializer handler, in the callback
 *   // skip the found paths, removing them from the JSON that is generated
 *   JsonSerializer js = new JsonPrettySerializer(null);
 *   List&lt;String[]&gt; paths = new ArrayList&lt;String[]&gt;();
 *   paths.add(new String[] {"a", "b", "path.dot"});
 *   paths.add(new String[] {"path2"});
 *   PathFinder pf = new PathFinder(js, paths);
 *   pf.find(inputStream, (PathFinder finder, String[] path)-&gt; {
 *       return true; // true means skip the field
 *   }
 *   // at this point js.toString() will have the JSON representation of
 *   // the NSON from the inputStream but without the 2 specified paths
 * </pre>
 * <p>
 * Think about (some of these apply to NSON in general and not this class):
 * <ul>
 *  <li>multi-path seek with ability to "continue"</li>
 *  <li>ability to find array fields (a.b[6], a[5].foo, etc) or maybe just
 *  convenience methods to move within arrays such as seek(5) to get to 5th
 *  element</li>
 *  <li>predicates/equality mechanism (think about abstractions that can be the
 *  underpinnings of query operations -- compare, arithmetic, other ops...)</li>
 *  <li>general query-ish support in this or related classes</li>
 *  <li>"index" style support that allows direct navigation to a field by
 *  setting an offset. That can be done in caller with bis but worth
 *  mentioning here</li>
 * </ul>
 * @hide-in-sdk
 */
public class PathFinder implements FieldValueEventHandler {

    /* array of path arrays */
    private String[][] paths;

    /*
     * if any paths are set case-insensitive, they are here. These can only
     * apply to single-component paths.
     *
     * If additional function is required in the future it gets more complex
     */
    private HashSet<String> caseInsensitivePaths;

    /* array of current indexes into each path */
    private int[] currentIndex;

    /* current path depth in traversal */
    private int depth;

    /*
     * true if currently skipping a map field. used to suppres and
     * endMapField call
     */
    private boolean skipping;

    /*
     * optional callback, called when a path is found
     */
    private PathFinderCallback cb;

    /* used to stop event generation */
    private boolean stop;

    /* true if a path found by seek, only used by seek */
    private boolean found;

    /*
     * optional handler. If set it will be given all not-skipped
     * events. A handler in conjunction with a smart callback can do
     * things like rename a field, transform a field, skip a field, etc
     */
    private final FieldValueEventHandler handler;

    /* the input stream */
    private ByteInputStream bis;

    /*
     * A callback used by the seek interface that
     *  o sets "found" boolean state
     *  o tells the event generation to stop
     * Seek is a one-path, one-time operation
     */
    private static PathFinderCallback seekCallback =
        (PathFinder finder, String[] path)-> {
        finder.setFound(true);
        finder.setStop();
        return true; /* note: skip is ignored if stop is set */
    };

    /**
     * Looks for a single path expression in the NSON, starting at the current
     * offset in the stream. Returns true if the path is found and the stream
     * is set at the location found. This is a static convenience method that
     * encapsulates construction of the class instance and a
     * {@link #seek(ByteInputStream)} call.
     * <p>
     * Seek only works with a single path and event generation stops when
     * the path is found. There is no facility to "continue" although a
     * caller can start a new seek from anywhere in the NSON stream.
     * @param pathExpr the path. The path comprises dot (".") separated
     * components. If a single path component needs to include a dot,
     * use {@link #seek(ByteInputStream, String...)}
     * @param bis the input stream. The operation starts at the current offset
     * of the stream
     * @return true if the path is found, false if not
     * @throws IOException if there is a problem processing the stream
     */
    public static boolean seek(String pathExpr, ByteInputStream bis)
        throws IOException {

        PathFinder pf = new PathFinder(null, pathExpr);
        return pf.seek(bis);
    }

    /**
     * Looks for a single path expression in the NSON, starting at the current
     * offset in the stream. Returns true if the path is found and the stream
     * is set at the location found. This is a static convenience method that
     * encapsulates construction of the class instance and a
     * {@link #seek(ByteInputStream)} call.
     * <p>
     * Seek only works with a single path and event generation stops when
     * the path is found. There is no facility to "continue" although a
     * caller can start a new seek from anywhere in the NSON stream.
     * @param bis the input stream. The operation starts at the current offset
     * of the stream
     * @param components the individual components of the path.
     * @return true if the path is found, false if not
     * @throws IOException if there is a problem processing the stream
     */
    public static boolean seek(ByteInputStream bis, String... components)
        throws IOException {

        PathFinder pf = new PathFinder(null, components);
        return pf.seek(bis);
    }

    /**
     * A convenience constructor for a single path search using a "." separated
     * path, where the components do not contain the "." character.
     * @param handler an optional {@link FieldValueEventHandler}. If set
     * this handler will receive all of the events that are generated by
     * the {@link find} call except for paths that are skipped
     * @param path the dot-separated path
     */
    public PathFinder(FieldValueEventHandler handler, String path) {
        this(handler, path.split("\\."));
    }

    /**
     * A constructor for a single-path search, where the path is expressed
     * as an array of Strings using a variable number of strings
     * @param handler an optional {@link FieldValueEventHandler}. If set
     * this handler will receive all of the events that are generated by
     * the {@link find} call except for paths that are skipped
     * @param components the individual string components of the path
     * @throws IllegalArgumentException if the path components are invalid
     */
    public PathFinder(FieldValueEventHandler handler, String... components) {
        if (components.length == 0) {
            throw new IllegalArgumentException(
                "PathFinder path requires at least one component");
        }
        this.handler = (handler != null ? handler : nullHandler);
        this.paths = new String[1][];
        this.currentIndex = new int[1];
        this.paths[0] = components;
    }

    /**
     * Constructs a PathFinder instance to search for multiple paths in a single
     * pass (in the same {@link #find} call)
     * Example
     * <pre>
     *
     *   List&lt;String[]&gt; paths = new ArrayList&lt;String[]&gt;();
     *   paths.add(new String[] {"a", "b", "c"});
     *   paths.add(new String[] {"name"});
     *   PathFinder pf = new PathFinder(handler, paths);
     *   pf.find(...)
     * </pre>
     * @param handler an optional {@link FieldValueEventHandler}. If set
     * this handler will receive all of the events that are generated by
     * the {@link find} call except for paths that are skipped
     * @param paths a not-empty list of paths to use for the search
     * @throws IllegalArgumentException if the paths are malformed or missing
     */
    public PathFinder(FieldValueEventHandler handler,
                      List<String[]> paths) {
        if (paths == null || paths.size() == 0) {
            throw new IllegalArgumentException(
                "PathFinder requires at least one path");
        }
        this.handler = (handler != null ? handler : nullHandler);
        int numPaths = paths.size();
        this.paths = new String[numPaths][];
        this.currentIndex = new int[numPaths];
        int i = 0;
        for (String s[] : paths) {
            if (s.length == 0) {
                throw new IllegalArgumentException(
                    "PathFinder paths require at least one component");
            }
            this.paths[i] = s;
            i++;
        }
    }

    /**
     * Looks for a single path expression in the NSON, starting at the current
     * offset in the stream. Returns true if the path is found and the stream
     * is set at the location found. The path itself is set during
     * construction of the class instance.
     * <p>
     * Seek only works with a single path and event generation stops when
     * the path is found. There is no facility to "continue" although a
     * caller can start a new seek from anywhere in the NSON stream.
     * @param bis the input stream. The operation starts at the current offset
     * of the stream
     * @return true if the path is found, false if not
     * @throws IOException if there is a problem reading the stream
     * @throws IllegalArgumentException if the class has not been constructed
     * correctly
     */
    public boolean seek(ByteInputStream bis) throws IOException {
        /* this call only operates if there's a single path */
        if (currentIndex.length > 1) {
            throw new IllegalArgumentException(
                "PathFinder.seek can only look for a single path");
        }
        this.bis = bis;
        this.cb = seekCallback;
        Nson.generateEventsFromNson(this, bis, false);
        return found;
    }

    /**
     * Searches the NSON in the input stream looking for the path or paths
     * specified during construction. When a path is found and a callback
     * has been used the callback is called. The callback can do a number of
     * things including
     * <ul>
     *  <li>stop the search and event generation by calling PathFinder.stop()
     *    </li>
     *  <li>process or otherwise transform the NSON itself</li>
     *  <li>tell the search process to skip the found field or not. It
     *  should return true in order to skip the field</li>
     * </ul>
     * @param bis the input stream
     * @param cb an optional {@link PathFinderCallback}
     * @throws IOException if there is a problem processing the stream
     */
    public void find(ByteInputStream bis,
                     PathFinderCallback cb) throws IOException {
        this.bis = bis;
        this.cb = cb;
        Nson.generateEventsFromNson(this, bis, false);
    }

    /**
     * Sets the specified path as case-insensitive. The path must be
     * a single component and must exist in paths.
     * NOTE: it would not be difficult to extend this mechanism to
     * multi-component paths as long as they do not embed "."
     * @param fieldName the field name to use
     */
    public void setCaseInsensitive(String fieldName) {
        for (String[] path : paths) {
            if (path[0].equalsIgnoreCase(fieldName) &&
                path.length == 1) {
                if (caseInsensitivePaths == null) {
                    caseInsensitivePaths = new HashSet<String>();
                }
                /* use lower-case */
                caseInsensitivePaths.add(fieldName.toLowerCase());
                return;
            }
        }
        /* failure to find the path is an error */
        throw new IllegalArgumentException("Cannot find field in PathFinder " +
                                           "paths: " + fieldName);
    }

    /**
     * Stops generation of events. This method might be called by a
     * {@link PathFinderCallback} method to end processing of the stream.
     */
    public void setStop() {
        stop = true;
    }

    /**
     * Sets the found state in the PathFinder instance. This is mostly
     * internal and is used by {@link seek} to let the caller know if the
     * path has been found.
     * @param value the value
     */
    public void setFound(boolean value) {
        found = value;
    }

    /**
     * Returns the input stream associated with the current search. This
     * method may be useful in a callback function.
     * @return the stream
     */
    public ByteInputStream getStream() {
        return bis;
    }

    /*
     * FieldValueEventHandler implementation. These are marked @hidden
     * to avoid appearing in javadoc
     */

    @Override
    public void startMap(int size) throws IOException {
        ++depth;
        handler.startMap(size);
    }

    @Override
    public void startArray(int size) throws IOException {
        handler.startArray(size);
    }

    @Override
    public void endMap(int size) throws IOException {
        --depth;
        handler.endMap(size);
    }

    @Override
    public void endArray(int size) throws IOException {
        handler.endArray(size);
    }

    @Override
    public boolean startMapField(String key) throws IOException {
        /*
         * Loop through all paths looking for partial or complete matches
         * based on:
         *  o key must match the path component at the current depth of the
         *  map walk
         *  o if there's a match and the walk is on the final component of
         *  the path and depth is appropriate, the path has been found
         */
        for (int i = 0; i < currentIndex.length; i++) {
            /* thisIndex is the index into a path's array that last matched */
            int thisIndex = currentIndex[i];
            /* thisPath is the path being checked on this loop iteration */
            String[] thisPath = paths[i];
            if (thisIndex + 1 == depth) {
                String curField = thisPath[thisIndex];
                boolean equals = isCaseInsensitive(curField) ?
                    key.equalsIgnoreCase(curField) :
                    key.equals(curField);
                if (equals) {
                    thisIndex++;

                    /*
                     * are we at the target? check:
                     *  o have all path components been matched (thisIndex)?
                     *  o is the target depth correct?
                     */
                    if (thisIndex == thisPath.length &&
                        thisIndex == depth) {
                        /*
                         * if a callback has been set, call it, saving and
                         * restoring the current byte offset in the stream
                         */
                        if (cb != null) {
                            int savedOffset = bis.getOffset();
                            skipping = cb.found(this, paths[i]);
                            bis.setOffset(savedOffset);
                            if (skipping) {
                                return true;
                            }
                        }
                    } else {
                        /*
                         * The path is a partial match. Advance the index of the
                         * last matched key for this path
                         */
                        currentIndex[i]++;
                    }
                }
            }
        }
        handler.startMapField(key);

        return false; /* don't skip */
    }

    private boolean isCaseInsensitive(String fieldName) {
        if (caseInsensitivePaths != null &&
            caseInsensitivePaths.contains(fieldName.toLowerCase())) {
            return true;
        }
        return false;
    }

    @Override
    public void endMapField(String key) throws IOException {
        /*
         * If this field was skipped, don't generate an end event, but
         * reset the skipping state
         */
        if (skipping) {
            skipping = false;
            return;
        }
        handler.endMapField(key);
    }

    @Override
    public void endArrayField(int index) throws IOException {
        handler.endArrayField(index);
    }

    @Override
    public void booleanValue(boolean value) throws IOException {
        handler.booleanValue(value);
    }

    @Override
    public void binaryValue(byte[] byteArray) throws IOException {
        handler.binaryValue(byteArray);
    }

    @Override
    public void binaryValue(byte[] byteArray, int offset, int length)
        throws IOException {
        handler.binaryValue(byteArray, offset, length);
    }

    @Override
    public void stringValue(String value) throws IOException {
        handler.stringValue(value);
    }

    @Override
    public void integerValue(int value) throws IOException {
        handler.integerValue(value);
    }

    @Override
    public void longValue(long value) throws IOException {
        handler.longValue(value);
    }

    @Override
    public void doubleValue(double value) throws IOException {
        handler.doubleValue(value);
    }

    @Override
    public void numberValue(BigDecimal value) throws IOException {
        handler.numberValue(value);
    }

    @Override
    public void timestampValue(TimestampValue timestamp) throws IOException {
        handler.timestampValue(timestamp);
    }

    @Override
    public void jsonNullValue() throws IOException {
        handler.jsonNullValue();
    }

    @Override
    public void nullValue() throws IOException {
        handler.nullValue();
    }

    @Override
    public void emptyValue() throws IOException {
        handler.emptyValue();
    }

    @Override
    public boolean stop() {
        return stop;
    }

    /**
     * @hidden
     * A function that is called when a path has been found. It is passed
     * the PathFinder instance and the path that was found.
     */
    @FunctionalInterface
    public interface PathFinderCallback {
        /**
         * A function that is called when a path has been found. It is passed
         * the PathFinder instance and the path that was found. At the time
         * of the callback the offset of the {@link ByteInputStream} associated
         * with the search is set at the found path. After return the offset is
         * reset to that same value, allowing this callback to read the
         * input stream in a non-destructive manner.
         *
         * @param finder the PathFinder instance
         * @param path the path that was found
         * @return true if the found element is to be skipped, false if it
         * is to be further processed
         * @throws IOException if there is a problem processing the stream
         */
        public boolean found(PathFinder finder, String[] path)
            throws IOException;
    }

}
