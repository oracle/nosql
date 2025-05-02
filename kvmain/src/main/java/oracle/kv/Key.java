/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv;

import static oracle.kv.impl.util.SerializationUtil.LOCAL_BUFFER_SIZE;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.sleepycat.je.BinaryEqualityComparator;
import com.sleepycat.je.ByteComparator;
import com.sleepycat.util.FastOutputStream;
import com.sleepycat.util.UtfOps;

/**
 * The Key in a Key/Value store.
 * <p>
 * A Key represents a path to a value in a hierarchical namespace. It consists
 * of a sequence of string path component names, and each component name is
 * used to navigate the next level down in the hierarchical namespace.  The
 * complete sequence of string components is called the Full Key Path.
 * <p>
 * The sequence of string components in a Full Key Path is divided into two
 * groups or sub-sequences: The Major Key Path is the initial or beginning
 * sequence and the Minor Key Path is the remaining or ending sequence.  The
 * Full Path is the concatenation of the Major and Minor Paths, in that order.
 * The Major Path must have at least one component, while the Minor path may be
 * empty (have zero components).
 * <p>
 * Each path component must be a non-null String.  Empty (zero length) Strings
 * are allowed, except that the first component of the major path must be a
 * non-empty String.
 * <p>
 * Given a Key, finding the location of a Key/Value pair is a two step process:
 * <ol>
 * <li>The Major Path is used to locate the node on which the Key/Value pair
 * can be found.</li>
 * <li>The Full Path is then used to locate the Key/Value pair within that
 * node.</li>
 * </ol>
 * <p>
 * Therefore all Key/Value pairs with the same Major Path are clustered on the
 * same node.
 * </p>
 * Keys which share a common Major Path are physically clustered by the KVStore
 * and can be accessed efficiently via special multiple-operation APIs, e.g.
 * {@link KVStore#multiGet multiGet}. The APIs are efficient in two ways:
 * <ol>
 * <li>
 * They permit the application to perform multiple-operations in single network
 * round trip.</li>
 * <li>
 * The individual operations (within a multiple-operation) are efficient since
 * the common-prefix keys and their associated values are themselves physically
 * clustered.</li>
 * </ol>
 * <p>
 * Multiple-operation APIs also support ACID transaction semantics. All the
 * operations within a multiple-operations are executed within the scope of a
 * single transaction.
 */
public class Key implements Comparable<Key>  {

    /*
     * Serialization Format
     * ====================
     * The serialization format used is similar to the BDB StringBinding tuple
     * format, except that delimiters after strings are handled differently in
     * order to reduce the byte size to a minimum.
     *
     * + Each string component is serialized using "Modified UTF-8" format (see
     *   java.io.DataInput javadoc) which means that no byte is zero or 0xFF.
     *
     * + Within the major and minor paths, a zero delimiter appears between
     *   each string component.
     *
     * + Between the major and minor paths, a 0xFF delimiter is used.  This is
     *   in contrast to BDB StringBinding, which uses 0xFF for a null string.
     *
     * + No delimiter appears at the beginning or end of the complete byte
     *   array.  This is in contrast to BDB StringBinding, which always appends
     *   a zero to every string including the last string in a multi-string
     *   tuple.
     *
     * + The total number of delimiter bytes per key is the total number of
     *   component strings minus one.  This is the minimum possible.
     *
     * Because of the 0xFF between paths, to obtain proper sort order a custom
     * key comparator is used.  Further details are below.
     *
     * In the description below the symbols 0 and $ are used to mean a zero and
     * 0xFF byte value, respectively.  Letters represent ordinary characters.
     *
     * Omitting the last null terminator
     * ------------------ --------------
     * Why this works and doesn't require a custom comparator: With an ending
     * terminator, X0 LT XY0.  Without the terminator, X LT XY.
     *
     * Why it works for empty strings at the end: With an ending terminator,
     * X0 LT X00.  Without the terminator, X LT X0.
     *
     * Example data in properly sorted order:
     * A
     * A0  (empty string is last)
     * A0A
     * AB
     * AB0A
     * B
     *
     * Using the 0xFF delimiter between major and minor paths
     * ------------------------------------------------------
     * The custom comparator is like the built-in unsigned byte comparator
     * except that 0xFF is treated as less than 0.  This sorts A$B0C before
     * A0B$C.
     *
     * Example data in properly sorted order:
     * A
     * A$   (empty string is last)
     * A$0  (two empty strings are last)
     * A$A
     * A0   (empty string is last)
     * AB
     *
     * Why do we need the special rule for 0xFF, and why do we need to sort the
     * major path separately from the minor path in compareTo?  If we didn't,
     * all keys with the same major path would not be contiguous.  Below we
     * treat 0 and $ as equal to show the problem.
     * A
     * A$B
     * A0B$C
     * A$B0D
     * A0C$
     * A$D
     *
     * Proper sorting by major path, sorting $ before 0, first gives:
     * A
     * A$B
     * A$B0D
     * A$D
     * A0B$C
     * A0C$
     */

    /* Used for empty minor key path. */
    private static final List<String> EMPTY_LIST = Collections.emptyList();

    /** Binary delimiter between Strings within a major or minor path. */
    private static final int BINARY_COMP_DELIM = 0;
    /** Binary delimiter between major and minor paths. */
    private static final int BINARY_PATH_DELIM = 0xff;
    /** Pseudo binary delimiter value indicating 'none'. */
    private static final int BINARY_NULL_DELIM = 1;

    /** String delimiter between Strings within a major or minor path. */
    static final String STRING_COMP_DELIM = "/";
    /** String delimiter between major and minor paths. */
    private static final String STRING_PATH_DELIM = "/-/";
    /** Pseudo string delimiter value indicating 'none'. */
    private static final String STRING_NULL_DELIM = null;

    /** Starting char of any string path or component delimiter. */
    private static final int STRING_DELIM_START = '/';
    /** Character delimiter between major and minor paths. */
    private static final String STRING_PATH_DELIM_CHAR = "-";
    /** Character delimiter between major and minor paths. */
    private static final String STRING_PATH_DELIM_ENCODED = "%2D";

    private static final String ZERO_ENCODED = "%00";
    private static final String SLASH_ENCODED = "%2F";

    private static final int MAX_KEY_LENGTH = Short.MAX_VALUE;

    /**
     * A thread-local byte array used as the initial array in
     * FastOutputStreams, to avoid allocation.
     */
    private static final ThreadLocal<byte[]> byteArrayBuffer =
        ThreadLocal.withInitial(() -> new byte[LOCAL_BUFFER_SIZE]);

    /** A thread-local char buffer used by writeUTF. */
    private static final ThreadLocal<char[]> utfCharBuffer =
        ThreadLocal.withInitial(() -> new char[LOCAL_BUFFER_SIZE]);

    /**
     * Creates a Key from a Major Path list and a Minor Path list.
     * <p>
     * <em>WARNING</em>: List instances passed as parameters are owned by the
     * Key object after calling this method. To avoid unpredictable results,
     * they must not be modified.
     *
     * @param majorPath may not be null or empty, and may not contain nulls.
     *
     * @param minorPath may be empty, but may not be null or contain nulls.
     */
    public static Key createKey(List<String> majorPath,
                                List<String> minorPath) {
        return new Key(Collections.unmodifiableList(majorPath),
                       Collections.unmodifiableList(minorPath));
    }

    /**
     * Creates a Key from a single component Major Path and a Minor Path list.
     * <p>
     * <em>WARNING</em>: List instances passed as parameters are owned by the
     * Key object after calling this method. To avoid unpredictable results,
     * they must not be modified.
     *
     * @param majorComponent may not be null.
     *
     * @param minorPath may be empty, but may not be null or contain nulls.
     */
    public static Key createKey(String majorComponent,
                                List<String> minorPath) {
        return new Key(Collections.singletonList(majorComponent),
                       Collections.unmodifiableList(minorPath));
    }

    /**
     * Creates a Key from a Major Path list and a single component Minor Path.
     * <p>
     * <em>WARNING</em>: List instances passed as parameters are owned by the
     * Key object after calling this method. To avoid unpredictable results,
     * they must not be modified.
     *
     * @param majorPath may not be null or empty, and may not contain nulls.
     *
     * @param minorComponent may not be null.
     */
    public static Key createKey(List<String> majorPath,
                                String minorComponent) {
        return new Key(Collections.unmodifiableList(majorPath),
                       Collections.singletonList(minorComponent));
    }

    /**
     * Creates a Key from a single component Major Path and a single component
     * Minor Path.
     *
     * @param majorComponent may not be null.
     *
     * @param minorComponent may not be null.
     */
    public static Key createKey(String majorComponent,
                                String minorComponent) {
        return new Key(Collections.singletonList(majorComponent),
                       Collections.singletonList(minorComponent));
    }

    /**
     * Creates a Key from a Major Path list; the Minor Path will be empty.
     * <p>
     * <em>WARNING</em>: List instances passed as parameters are owned by the
     * Key object after calling this method. To avoid unpredictable results,
     * they must not be modified.
     *
     * @param majorPath may not be null or empty, and may not contain nulls.
     */
    public static Key createKey(List<String> majorPath) {
        return new Key(Collections.unmodifiableList(majorPath), EMPTY_LIST);
    }

    /**
     * Creates a Key from a single component Major Path; the Minor Path will be
     * empty.
     *
     * @param majorComponent may not be null.
     */
    public static Key createKey(String majorComponent) {
        return new Key(Collections.singletonList(majorComponent), EMPTY_LIST);
    }

    private final List<String> majorPath;
    private final List<String> minorPath;

    /**
     * Creates a Key from immutable component lists.
     */
    private Key(List<String> majorPath, List<String> minorPath) {
        if (majorPath == null || minorPath == null) {
            throw new IllegalArgumentException
                ("Major and minor path must not be null.");
        }
        if (majorPath.size() == 0) {
            throw new IllegalArgumentException
                ("Major path must contain at least one component.");
        }
        for (String s : majorPath) {
            if (s == null) {
                throw new IllegalArgumentException
                    ("Major path component must not be null.");
            }
        }
        for (String s : minorPath) {
            if (s == null) {
                throw new IllegalArgumentException
                    ("Minor path component must not be null.");
            }
        }
        this.majorPath = majorPath;
        this.minorPath = minorPath;
    }

    /**
     * Returns the Full Path of the Key as new mutable list.
     *
     * <p>WARNING: The full path returned does not contain information about
     * the division between the Major and Minor Paths.  The user must take care
     * to preserve this division when necessary.</p>
     */
    public List<String> getFullPath() {
        final List<String> fullPath =
            new ArrayList<String>(majorPath.size() + minorPath.size());
        fullPath.addAll(majorPath);
        fullPath.addAll(minorPath);
        return fullPath;
    }

    /**
     * Returns the Major Path of the Key as an immutable list.
     */
    public List<String> getMajorPath() {
        return majorPath;
    }

    /**
     * Returns the Minor Path of the Key as an immutable list.
     */
    public List<String> getMinorPath() {
        return minorPath;
    }

    /**
     * Returns true if this key is a prefix of the key supplied as the
     * argument. That is, every component of this key is equal to the
     * corresponding component (by position and whether it is in the Major or
     * Minor Path) of the other key.
     *
     * Specifically:
     * <ul>
     * <li>
     * If the Minor Path of this Key is empty, this method returns true if the
     * Major Path of this Key is a prefix of the Major Path of the other Key.
     * </li>
     * <li>
     * If the Minor Path of this Key is non-empty, this method returns true if:
     *   <ul>
     *   <li>the Major Path of this Key equals the Major Path of the other Key,
     *   and</li>
     *   <li>the the Minor Path of this Key is a prefix of the Minor Path of
     *   the other Key.</li>
     *   </ul>
     * </li>
     * </ul>
     *
     * @param otherKey the key being tested
     */
    public boolean isPrefix(Key otherKey) {
        if (minorPath.size() == 0) {
            final int majorSize =
                Math.min(majorPath.size(), otherKey.majorPath.size());
            return majorPath.equals(otherKey.majorPath.subList(0, majorSize));
        }
        if (!majorPath.equals(otherKey.majorPath)) {
            return false;
        }
        final int minorSize =
            Math.min(minorPath.size(), otherKey.minorPath.size());
        return minorPath.equals(otherKey.minorPath.subList(0, minorSize));
    }

    /**
     * Compares this Key with the specified Key for order.
     *
     * <p>The Major and Minor Paths are compared separately.  The Major paths
     * are compared first.  If the Major Paths are unequal, then the result of
     * their comparison is returned and the Minor Paths are not compared.  If
     * the Major Paths are equal, then the Minor Paths are compared and the
     * result of their comparison is returned.</p>
     *
     * <p>For each of the Major and Minor Paths, its components are compared as
     * follows.  First, for the number of components the paths have in common
     * (the common prefix), the path components at each position are compared
     * in sequence.  If the components at one position are unequal, the result
     * is determined by comparing the strings as if {@link String#compareTo
     * String.compareTo} were called, and the result is returned.  If all
     * common components are equal, the path with less components is considered
     * less than the other path with more components.  If the paths have the
     * same number of components and all components are equal, the paths are
     * considered equal and zero is returned.</p>
     */
    @Override
    public int compareTo(Key otherKey) {
        final int minMajorSize =
            Math.min(majorPath.size(), otherKey.majorPath.size());
        for (int i = 0; i < minMajorSize; i += 1) {
            final int cmp =
                majorPath.get(i).compareTo(otherKey.majorPath.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        final int sizeCmp = majorPath.size() - otherKey.majorPath.size();
        if (sizeCmp != 0) {
            return sizeCmp;
        }
        final int minMinorSize =
            Math.min(minorPath.size(), otherKey.minorPath.size());
        for (int i = 0; i < minMinorSize; i += 1) {
            final int cmp =
                minorPath.get(i).compareTo(otherKey.minorPath.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return minorPath.size() - otherKey.minorPath.size();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Key)) {
            return false;
        }
        final Key otherKey = (Key) other;
        return majorPath.equals(otherKey.majorPath) &&
               minorPath.equals(otherKey.minorPath);
    }

    @Override
    public int hashCode() {
        return majorPath.hashCode() + minorPath.hashCode();
    }

    /**
     * Encodes the Key object and returns the resulting key path string, which
     * may be used as a URI path or simply as a string identifier.  The path
     * string may be decoded, to recreate the Key object, using {@link
     * #fromString}.
     *
     * <p>The key path string format is designed to work with URIs and URLs,
     * and is intended to be used as a general purpose string identifier.  The
     * key path components are separated by slash ({@code /}) delimiters.  A
     * special slash-hyphen-slash delimiter ({@code /-/}) is used to separate
     * the major and minor paths.  Characters that are not allowed in a URI
     * path are encoded using URI syntax ({@code %XX} where {@code XX} are
     * hexadecimal digits).  The string always begins with a leading slash to
     * prevent it from begin treated as a URI relative path.  Some examples are
     * below.</p>
     *
     * <ol>
     * <li><code>/SingleComponentMajorPath</code></li>
     * <li><code>/MajorPathPart1/MajorPathPart2/-/MinorPathPart1/MinorPathPart2</code></li>
     * <li><code>/HasEncodedSlash:%2F,Zero:%00,AndSpace:%20</code></li>
     * </ol>
     *
     * <p>Example 1 demonstrates the simplest possible path.  Note that a
     * leading slash is always necessary.</p>
     *
     * <p>Example 2 demonstrates the use of the {@code /-/} separator between
     * the major and minor paths.  If a key happens to have a path component
     * that is nothing but a hyphen, to distinguish it from that delimiter it
     * is encoded as {@code %2D}.  For example:
     * {@code /major/%2d/path/-/minor/%2d/path}.
     *
     * <p>Example 3 demonstrates encoding of characters that are not allowed in
     * a path component.  For URI compatibility, characters that are encoded
     * are the ASCII space and other Unicode separators (defined by {@link
     * Character#isSpaceChar}), the ASCII and Unicode control characters
     * (defined by {@link Character#isISOControl}), and the following 15 ASCII
     * characters: (<code>" # % / &lt; &gt; ? [ \ ] ^ ` { | }</code>).  The
     * hyphen ({@code -}) is also encoded when it is the only character in the
     * path component, as described above.</p>
     *
     * <p>When using the Key path string in a URI, there are two special
     * considerations.</p>
     * <ul>
     * <li>Although any Unicode character may be used in a Key path component
     * and characters will be encoded as necessary by this method for URI
     * compatibility, in practice it may be problematic to include control
     * characters because web user agents, proxies, etc, may not be tolerant of
     * all characters.  Although it will be encoded, embedding a slash in a
     * path component may also be problematic. It is the responsibility of the
     * application to use characters that are compatible with other software
     * that processes the URI.</li>
     * <li>When using the {@link java.net.URI} class, be aware that this class
     * has no constructor where the encoded (raw) path can be specified as a
     * separate parameter.  The {@code path} parameter of the {@code URI}
     * constructor must not contain encoded characters, and therefore slashes
     * may not be embedded in a path component.  To create a URI from a Key
     * object, the recommended approach is to create the URI with a predefined
     * path token and then replace the token with the raw path derived from the
     * Key, as shown below.
     * <pre>
     *   Key key = ...;
     *   String rawPath = key.toString();
     *   URI uri = new URI(..., "/PATH_TOKEN", ...);
     *   uri = new URI(uri.toString().replace("/PATH_TOKEN", rawPath));</pre>
     * The {@link java.net.URL} class, on the other hand, does not suffer from
     * this limitation.  The {@code file} parameter of the {@code URL}
     * constructor is the raw path, so the Key path string may be passed
     * directly as the {@code file} parameter.</li>
     * </ul>
     *
     * @return the key path string.
     */
    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder(200);
        for (final String comp : majorPath) {
            sb.append(STRING_COMP_DELIM);
            appendPathComponent(sb, comp);
        }
        if (!minorPath.isEmpty()) {
            String delim = STRING_PATH_DELIM;
            for (final String comp : minorPath) {
                sb.append(delim);
                appendPathComponent(sb, comp);
                delim = STRING_COMP_DELIM;
            }
        }
        return sb.toString();
    }

    /**
     * Append a single path component, not including the slash delimiter, to
     * the StringBuilder.
     */
    private static void appendPathComponent(StringBuilder sb, String comp) {

        /*
         * If the component contains nothing but the path delimiter character,
         * encode it to distinguish it from the path delimiter string.
         */
        if (comp.equals(STRING_PATH_DELIM_CHAR)) {
            sb.append(STRING_PATH_DELIM_ENCODED);
            return;
        }

        /*
         * Use URI constructor and URI.getRawPath to get the encoded form of
         * the path, according to the URI RFC.  FUTURE: potentially improve
         * speed by implementing encode directly.
         */
        final URI uri;
        try {
            uri = new URI("kv", null, "host", -1, "/" + comp, null, null);
        } catch (URISyntaxException shouldNeverHappen) {
            throw new FaultException(shouldNeverHappen, false /*isRemote*/);
        }
        final String rawPath = uri.getRawPath().substring(1);

        /*
         * Then encode zero and slash characters, since this is not done by the
         * URI class.
         */
        boolean startedEncoding = false;
        for (int i = 0; i < rawPath.length(); i += 1) {
            final char c = rawPath.charAt(i);
            if (c != 0 && c != '/') {
                if (startedEncoding) {
                    sb.append(c);
                }
                continue;
            }
            if (!startedEncoding) {
                startedEncoding = true;
                sb.append(rawPath.substring(0, i));
            }
            sb.append((c == '/') ? SLASH_ENCODED : ZERO_ENCODED);
        }
        if (!startedEncoding) {
            sb.append(rawPath);
        }
    }

    /**
     * Decodes a key path string and returns the resulting Key object.  The
     * path string should normally be created using {@link #toString}.  See
     * {@link #toString} for more information about the path string format.
     *
     * @param pathString a non-null path string, which should normally be
     * created using {@link #toString}.
     *
     * @return the resulting Key object.
     *
     * @throws IllegalArgumentException if pathString does not begin with a
     * slash or contains invalid characters, for example, an invalid encoding
     * sequence.
     */
    public static Key fromString(final String pathString) {
        return fromIterator(new StringKeyIterator(pathString));
    }

    /*
     * FUTURE: toASCIIString could be implemented but encoding must be done
     * directly rather than leveraging the URI.toASCIIString method.
     * URI.toASCIIString normalizes the Unicode string, which means that
     * character data (which may be desired by the application) is modified and
     * illegal character sequences are not allowed.
     *
     * public String toASCIIString() {...}
     */

    /**
     * KeyIterator implementation for a key serialized in String path format.
     */
    private static class StringKeyIterator implements KeyIterator {

        private final String pathString;
        private int off;
        private String delim = STRING_NULL_DELIM;
        private boolean endOfKey = false;

        StringKeyIterator(final String s) {
            if (s.length() == 0 || s.charAt(0) != '/') {
                throw new IllegalArgumentException
                    ("Path string does not begin with slash: " + s);
            }
            pathString = s;
            off = 1;
        }

        @Override
        public String next() {
            if (endOfKey) {
                throw new IllegalStateException();
            }
            final int origOff = off;

            /* Find next component. */
            boolean foundDelim = false;
            int compLength = 0;
            for (int i = origOff; i < pathString.length(); i += 1) {
                final int c = pathString.charAt(i);
                if (c == STRING_DELIM_START) {
                    foundDelim = true;
                    break;
                }
                compLength += 1;
            }
            off += compLength;

            /* Determine whether delimiter is simple / or /-/. */
            if (foundDelim) {
                String newDelim = STRING_PATH_DELIM;
                for (int j = 1;
                     newDelim == STRING_PATH_DELIM &&
                     j < STRING_PATH_DELIM.length();
                     j += 1) {
                    if (off + j >= pathString.length() ||
                        pathString.charAt(off + j) !=
                        STRING_PATH_DELIM.charAt(j)) {
                        newDelim = STRING_COMP_DELIM;
                    }
                }
                delim = newDelim;
                off += newDelim.length();
            } else {
                delim = STRING_NULL_DELIM;
                endOfKey = true;
            }

            /* For an empty component there is nothing else to do. */
            if (compLength == 0) {
                return "";
            }

            /*
             * Use URI.getPath to decode all encoded characters, including zero
             * and slash.  FUTURE: potentially improve speed by implementing
             * decode directly.
             */
            String comp = pathString.substring(origOff, origOff + compLength);
            final URI uri;
            try {
                uri = new URI("kv://host/" + comp);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException
                    ("Path component syntax is invalid", e);
            }
            comp = uri.getPath();
            assert comp.charAt(0) == '/';
            return comp.substring(1);
        }

        @Override
        public boolean atEndOfKey() {
            return endOfKey;
        }

        @Override
        public boolean atEndOfMajorPath() {
            return delim == STRING_PATH_DELIM;
        }
    }

    /**
     * Returns this Key as a serialized byte array, such that {@link
     * #fromByteArray} may be used to reconstitute the Key.
     * <p>
     * The number of bytes in the returned array is the number of UTF-8 bytes
     * needed to represent the component strings, plus the number of
     * components, minus one.
     * <p>
     * The intended use case for the {@link #toByteArray} and {@link
     * #fromByteArray} methods is to serialize keys for storage outside of
     * NoSQL DB or for sending across a network.
     * <p>
     * Values returned by calls to this method can be used with current and
     * newer releases, but are not guaranteed to be compatible with earlier
     * releases.
     */
    public byte[] toByteArray() {

        /*
         * The Key bytes are serialized such that {@link BytesComparator} will
         * collate keys properly, as if {@link #compareTo} were called.
         */
        final FastOutputStream out =
            new FastOutputStream(byteArrayBuffer.get());

        /* Write major path with BINARY_COMP_DELIM between strings. */
        final int majorLast = majorPath.size() - 1;
        assert majorLast >= 0;
        for (int i = 0; i <= majorLast; i += 1) {
            writeUTF(out, majorPath.get(i));
            if (i != majorLast) {
                out.writeFast(BINARY_COMP_DELIM);
            }
        }

        /*
         * If minor path is not empty, write BINARY_PATH_DELIM and minor path.
         */
        final int minorLast = minorPath.size() - 1;
        if (minorLast >= 0) {
            out.writeFast(BINARY_PATH_DELIM);
            for (int i = 0; i <= minorLast; i += 1) {
                writeUTF(out, minorPath.get(i));
                if (i != minorLast) {
                    out.writeFast(BINARY_COMP_DELIM);
                }
            }
        }

        byte[] keyBytes = out.toByteArray();
        /* Check the length of key */
        if (keyBytes.length > MAX_KEY_LENGTH) {
            throw new FaultException("Serialized key length:" +
                    keyBytes.length +
                    " exceeds maximum key permissible length:" +
                    MAX_KEY_LENGTH, false);
        }
        return keyBytes;
    }

    /**
     * Writes the UTF-8 representation of the string.
     */
    private static void writeUTF(final FastOutputStream out, final String s) {
        final int charLength = s.length();
        if (charLength == 0) {
            return;
        }
        char[] chars = (charLength <= LOCAL_BUFFER_SIZE) ?
            utfCharBuffer.get() :
            new char[charLength];
        s.getChars(0, charLength, chars, 0);
        final int utfLength = UtfOps.getByteLength(chars, 0, charLength);
        out.makeSpace(utfLength);
        UtfOps.charsToBytes(chars, 0, out.getBufferBytes(),
                            out.getBufferLength(), charLength);
        out.addSize(utfLength);
    }

    /**
     * Deserializes the given bytes that were returned earlier by {@link
     * #toByteArray} and returns the resulting Key.
     * <p>
     * The intended use case for the {@link #toByteArray} and {@link
     * #fromByteArray} methods is to serialize keys for storage outside of
     * NoSQL DB or for sending across a network.
     * <p>
     * Values created with either the current or earlier releases can be used
     * with this method, but values created by later releases are not
     * guaranteed to be compatible.
     */
    public static Key fromByteArray(final byte[] keyBytes) {
        return fromIterator(new BinaryKeyIterator(keyBytes));
    }

    /**
     * For internal use only.
     * @hidden
     * KeyIterator implementation for a key serialized in binary format.
     */
    public static class BinaryKeyIterator implements KeyIterator {
        protected final byte[] buf;
        private int off = 0;
        private int delim = BINARY_NULL_DELIM;
        private boolean endOfKey = false;

        public BinaryKeyIterator(final byte[] bytes) {
            buf = bytes;
        }

        @Override
        public String next() {
            return next(true);
        }

        public void reset() {
            off = 0;
            endOfKey = false;
        }

        /**
         * Iterate over the next component. This is equivalent to calling next()
         * and ignoring the return value. It is an optimization when the
         * value is not needed as it does not de-serialized the component.
         */
        public void skip() {
            next(false);
        }

        private String next(boolean returnValue) {
            if (endOfKey) {
                throw new IllegalStateException();
            }
            int origOff = off;
            boolean foundDelim = false;
            int utfLength = 0;
            for (int i = origOff; i < buf.length; i += 1) {
                final int b = (buf[i] & 0xff);
                if (b == BINARY_PATH_DELIM || b == BINARY_COMP_DELIM) {
                    delim = b;
                    foundDelim = true;
                    break;
                }
                utfLength += 1;
            }
            off += utfLength;
            if (foundDelim) {
                off += 1;
            } else {
                delim = BINARY_NULL_DELIM;
                endOfKey = true;
            }
            if (!returnValue) {
                return null;
            }
            if (utfLength == 0) {
                return "";
            }
            return UtfOps.bytesToString(buf, origOff, utfLength);
        }

        @Override
        public boolean atEndOfKey() {
            return endOfKey;
        }

        @Override
        public boolean atEndOfMajorPath() {
            return delim == BINARY_PATH_DELIM;
        }
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Given a key as a byte array and the offset of the prior component,
     * returns an offset that is one past the end of the next component.
     *
     * This static method is intended to be used when no object creation is
     * desired. If object creation is acceptable, an iterator class may be
     * more appropriate.
     *
     * @param key is the byte array containing the key.
     *
     * @param prevOff is the offset of the prior component. To get the first
     * component of a key, pass zero. To get subsequent components, pass the
     * value returned by the prior call to this method plus one (to skip the
     * one byte delimiter).
     *
     * @return the offset following the end of the next component, or -1 if
     * there are no more components. When -1 is not returned, the length of
     * the next component is (retVal - prevOff).
     */
    public static int findNextComponent(
        final byte[] key,
        final int prevOff) {

        final int endOff = key.length;
        if (prevOff >= endOff) {
            return -1;
        }

        for (int i = prevOff; i < endOff; i += 1) {
            if (isDelimiter(key[i])) {
                return i;
            }
        }

        return endOff;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public static boolean isDelimiter(byte val) {
        final int b = (val & 0xff);
        return b == BINARY_PATH_DELIM || b == BINARY_COMP_DELIM;
    }

    /**
     * Creates a key using an iterator over an abstract serialized format,
     * which may be string or binary.
     *
     * Optimizes construction of the key for six cases:
     * 1. Single part major path, no minor path.
     * 2. Single part major path, single part minor path.
     * 3. Single part major path, multiple part minor path.
     * 4. Multiple part major path, no minor path.
     * 5. Multiple part major path, single part minor path.
     * 6. Multiple part major path, multiple part minor path.
     */
    private static Key fromIterator(final KeyIterator iter) {

        /* Read initial part of major path. */
        final String s1 = iter.next();

        if (iter.atEndOfKey()) {
            /* Case 1: There is no minor path. */
            return createKey(s1);
        }

        if (iter.atEndOfMajorPath()) {

            /* Read initial part of minor path. */
            final String s2 = iter.next();

            if (iter.atEndOfKey()) {
                /* Case 2: Minor path has only one part. */
                return createKey(s1, s2);
            }

            /* Read the rest of minor path. */
            final List<String> minorList = new ArrayList<String>(2);
            minorList.add(s2);
            do {
                minorList.add(iter.next());
            } while (!iter.atEndOfKey());

            /* Case 3. */
            return createKey(s1, minorList);
        }

        /* Read the rest of major path. */
        final List<String> majorList = new ArrayList<String>(2);
        majorList.add(s1);
        do {
            majorList.add(iter.next());
        } while (!iter.atEndOfKey() && !iter.atEndOfMajorPath());

        if (iter.atEndOfKey()) {
            /* Case 4: There is no minor path. */
            return createKey(majorList);
        }

        /* Read initial part of minor path. */
        final String s2 = iter.next();

        if (iter.atEndOfKey()) {
            /* Case 5: Minor path has only one part. */
            return createKey(majorList, s2);
        }

        /* Read the rest of minor path. */
        final List<String> minorList = new ArrayList<String>(2);
        minorList.add(s2);
        do {
            minorList.add(iter.next());
        } while (!iter.atEndOfKey());

        /* Case 6. */
        return createKey(majorList, minorList);
    }

    /**
     * Specialized iterator for parsing a serialized key and returning each
     * component plus information about the component last returned.
     */
    private static interface KeyIterator {

        /**
         * Returns the next component.  Is guaranteed to return without
         * exception on the first call, but from then on will throw
         * IllegalStateException if called when atEndOfKey would return true.
         * Never returns null.
         */
        String next();

        /**
         * Returns true if the component last returned was the last component
         * in the key.
         */
        boolean atEndOfKey();

        /**
         * Returns true if the component last returned was the last component
         * in the major path and there are more components, which belong to the
         * minor path.  Normally atEndOfKey should be called before calling
         * this method.
         */
        boolean atEndOfMajorPath();
    }

    /**
     * For internal use only.
     * @hidden
     * FUTURE: Make visible in API?
     *
     * Compares two serialized keys for order.  Returns the same result as
     * {@link #compareTo} would for the deserialized Keys.  Used as JE Btree
     * key comparator.
     *
     * The only difference between this method and the default JE unsigned byte
     * comparison is that this method considers the BINARY_PATH_DELIM (0xff) to
     * be less than all other values.
     */
    public static class BytesComparator
        implements ByteComparator, BinaryEqualityComparator,
                   Comparator<byte[]> {

        @Override
        public int compare(final byte[] key1, final byte[] key2) {
            return compare(key1, 0, key1.length, key2, 0, key2.length);
        }

        @Override
        public int compare(final byte[] key1,
                           final int key1Off,
                           final int key1Len,
                           final byte[] key2,
                           final int key2Off,
                           final int key2Len) {

            /* First compare the bytes in both arrays. */
            final int minLen = Math.min(key1Len, key2Len);

            for (int i = 0; i < minLen; i++) {
                final byte b1 = key1[key1Off + i];
                final byte b2 = key2[key2Off + i];

                if (b1 == b2) {
                    continue;
                }

                /* Treat as unsigned bytes. */
                final int i1 = (b1 & 0xff);
                final int i2 = (b2 & 0xff);

                /* Treat BINARY_PATH_DELIM as less than all other values. */
                if (i1 == BINARY_PATH_DELIM) {
                    return -1;
                }
                if (i2 == BINARY_PATH_DELIM) {
                    return 1;
                }

                /* Result is unsigned comparison of unequal bytes. */
                return i1 - i2;
            }

            /*
             * minLen bytes are equal. The smaller array is considered less
             * than the larger array, or if they are the same length they are
             * equal.
             */
            return (key1Len - key2Len);
        }
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns the number of leading bytes in the given byte array that are the
     * significant bytes of the major path.  Used for hashing to derive the
     * partition ID.
     */
    public static int getMajorPathLength(final byte[] keyBytes) {
        final int len = keyBytes.length;
        for (int i = 0; i < len; i += 1) {
            if ((keyBytes[i] & 0xff) == BINARY_PATH_DELIM) {
                /* Major path precedes i. */
                return i;
            }
        }
        /* Key consists entirely of the major path. */
        return len;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns the number of path components in a byte array key.
     */
    public static int countComponents(final byte[] keyBytes) {
        final int len = keyBytes.length;
        int count = 1;
        for (int i = 0; i < len; i += 1) {
            final int b = keyBytes[i] & 0xff;
            if (b == BINARY_PATH_DELIM || b == BINARY_COMP_DELIM) {
                count += 1;
            }
        }
        return count;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns the number of bytes in a key derived from the given key but
     * containing the number of components specified, which is assumed to be
     * greater than zero and less than the number of components in the given
     * key.
     */
    public static int getPrefixKeySize(final byte[] keyBytes,
                                       final int nComponents) {
        assert (nComponents > 0);
        final int len = keyBytes.length;
        int count = 1;
        int i;
        for (i = 0; i < len; i += 1) {
            final int b = keyBytes[i] & 0xff;
            if (b == BINARY_PATH_DELIM || b == BINARY_COMP_DELIM) {
                if (count == nComponents) {
                    break;
                }
                count += 1;
            }
        }
        assert count == nComponents;
        return i;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns a key derived from the given key but containing the number of
     * components specified, which is assumed to be greater than zero and less
     * than the number of components in the given key.
     */
    public static byte[] getPrefixKey(final byte[] keyBytes,
                                      final int nComponents) {
        int i = getPrefixKeySize(keyBytes, nComponents);
        final byte[] prefix = new byte[i];
        System.arraycopy(keyBytes, 0, prefix, 0, prefix.length);
        return prefix;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns the length of the path component starting at the given offset,
     * or keyBytes.length if the key has no more components.
     */
    public static int getComponentLength(final byte[] keyBytes, final int off) {
        final int len = keyBytes.length;
        for (int i = off; i < len; i += 1) {
            final int b = keyBytes[i] & 0xff;
            if (b == BINARY_PATH_DELIM || b == BINARY_COMP_DELIM) {
                return i - off;
            }
        }
        return len - off;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Adds a path component to a byte array key using an appropriate
     * delimiter.
     *
     * @param majorPathComplete is true if the parentKey is known to contain a
     * complete major path, and must be accurate to use the correct delimiter.
     */
    public static byte[] addComponent(final byte[] parentKey,
                                      final boolean majorPathComplete,
                                      final String component) {

        final FastOutputStream out =
            new FastOutputStream(byteArrayBuffer.get());
        if (parentKey != null) {
            out.writeFast(parentKey);
            out.writeFast(getNextDelim(parentKey, majorPathComplete));
        }
        writeUTF(out, component);
        return out.toByteArray();
    }

    /**
     * For internal use only.
     *
     * @hidden
     *
     * Returns true if the key belongs the internal key space.
     *
     * @param serKeyBytes the internal database serialized form of the key
     * bytes. Note that it's specifically *not* the return value of
     * toByteArray.
     *
     * @see #keySpaceIsInternal()
     */
    public static boolean keySpaceIsInternal(final byte[] serKeyBytes) {
        return (serKeyBytes.length == 0) /* Just '/' */||
               ((serKeyBytes.length > 0) &&
                (serKeyBytes[0] == BINARY_COMP_DELIM));
    }

    /**
     * For internal use only.
     *
     * @hidden
     *
     * Returns true if the key belongs the internal key space
     *
     * @see #keySpaceIsInternal(byte[])
     */
    public boolean keySpaceIsInternal() {
        return getMajorPath().get(0).isEmpty();
    }

    /**
     * If majorPathComplete is true and the parentKey does not contain a major
     * path delimiter, we assume the parentKey is the major path alone and we
     * add a path delimiter.  Otherwise we use a component delimiter.
     */
    private static int getNextDelim(final byte[] parentKey,
                                    final boolean majorPathComplete) {
        assert (parentKey != null);
        if (majorPathComplete) {
            if (getMajorPathLength(parentKey) < parentKey.length) {
                return BINARY_COMP_DELIM;
            }
            return BINARY_PATH_DELIM;
        }
        return BINARY_COMP_DELIM;
    }
}
