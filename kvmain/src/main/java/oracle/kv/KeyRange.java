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

import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Defines a range of Key components for use in multiple-key operations and
 * iterations.
 *
 * <p>The KeyRange defines a range of String values for the key components
 * immediately following the last component of a parent key that is used in a
 * multiple-key operation.  In terms of a tree structure, the range defines the
 * parent key's immediate children that are selected by the multiple-key
 * operation.</p>
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class KeyRange implements FastExternalizable {

    /** Range specifies a lower bound. */
    static final int FLAG_START = 0x1;

    /** Lower bound is inclusive. */
    static final int FLAG_START_INCLUSIVE = 0x2;

    /** Range specifies an upper bound. */
    static final int FLAG_END = 0x4;

    /** Upper bound is inclusive. */
    static final int FLAG_END_INCLUSIVE = 0x8;

    private final String start;
    private final boolean startInclusive;
    private final String end;
    private final boolean endInclusive;

    /**
     * Creates a start/end KeyRange.
     *
     * @param start is the String component that defines lower bound of the key
     * range.  If null, no lower bound is enforced.
     *
     * @param startInclusive is true if start is included in the range, i.e.,
     * start is less than or equal to the first String component in the key
     * range.
     *
     * @param end is the String component that defines upper bound of the key
     * range.  If null, no upper bound is enforced.
     *
     * @param endInclusive is true if end is included in the range, i.e., end
     * is greater than or equal to the last String component in the key range.
     *
     * @throws IllegalArgumentException if either start and end are null or
     * start is not less than end.
     */
    public KeyRange(String start,
                    boolean startInclusive,
                    String end,
                    boolean endInclusive) {
        if (start == null && end == null) {
            throw new IllegalArgumentException
                ("start or end must be non-null");
        }

        if (start != null &&
            end != null &&
            ((start.compareTo(end) > 0) ||
             ((start.compareTo(end) == 0) &&
              !(startInclusive && endInclusive)))) {
            throw new IllegalArgumentException
                ("start key must be less than the end key. (" + start + "," +
                end + ")");
        }

        this.start = start;
        this.startInclusive = startInclusive;
        this.end = end;
        this.endInclusive = endInclusive;
    }

    /**
     * Creates a prefix KeyRange.
     *
     * <p>For a KeyRange created using this constructor, {@link #isPrefix} will
     * return true.</p>
     *
     * <p>Additionally, both {@link #getStart} and {@link #getEnd} will return
     * the prefix, and both {@link #getStartInclusive} and {@link
     * #getEndInclusive} will return true. However, the {@code KeyRange} will
     * be treated as a prefix, not an inclusive range.</p>
     *
     * @param prefix is the value that is a prefix of all matching key
     * components.  The KeyRange matches a key component if the component
     * starts with (as in {@code String.startsWith}) the prefix value.
     */
    public KeyRange(String prefix) {
        if (prefix == null) {
            throw new IllegalArgumentException("prefix must be non-null");
        }
        start = prefix;
        startInclusive = true;
        end = prefix;
        endInclusive = true;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * FastExternalizable constructor.
     */
    public KeyRange(DataInput in, short serialVersion)
        throws IOException {

        final int flags = in.readUnsignedByte();

        startInclusive = ((flags & FLAG_START_INCLUSIVE) != 0);
        endInclusive = ((flags & FLAG_END_INCLUSIVE) != 0);

        if ((flags & FLAG_START) == 0) {
            start = null;
        } else {
            start = readNonNullString(in, serialVersion);
        }

        if ((flags & FLAG_END) == 0) {
            end = null;
        } else {
            end = readNonNullString(in, serialVersion);
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@code byte}) <i>flags</i> // bit set with {@link
     *      #FLAG_START}=1, {@link #FLAG_START_INCLUSIVE}=2, {@link
     *      #FLAG_END}=4, and {@link #FLAG_END_INCLUSIVE}=8
     * <li> <i>[Optional]</i> ({@link
     *      SerializationUtil#writeNonNullString non-null String}) {@link
     *      #getStart start} // if <i>flags</i> has {@link #FLAG_START}=1 set
     * <li> <i>[Optional]</i> ({@link
     *      SerializationUtil#writeNonNullString non-null String}) {@link
     *      #getEnd end} // if <i>flags</i> has {@link #FLAG_END}=4 set
     * </ol>
     *
     * @hidden For internal use only
     */
    /*
     * Suppress warnings about references from public (but hidden) javadoc
     * comments to non-public items.
     */
    @SuppressWarnings("javadoc")
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        int flags = 0;
        if (start != null) {
            flags |= FLAG_START;
        }
        if (startInclusive) {
            flags |= FLAG_START_INCLUSIVE;
        }
        if (end != null) {
            flags |= FLAG_END;
        }
        if (endInclusive) {
            flags |= FLAG_END_INCLUSIVE;
        }

        out.writeByte(flags);
        if (start != null) {
            writeNonNullString(out, serialVersion, start);
        }
        if (end != null) {
            writeNonNullString(out, serialVersion, end);
        }
    }

    /**
     * Returns this KeyRange as a serialized byte array, such that {@link
     * #fromByteArray} may be used to reconstitute the KeyRange.  Values
     * returned by calls to this method can be used with current and newer
     * releases, but are not guaranteed to be compatible with earlier releases.
     */
    public byte[] toByteArray() {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(200);
        try {
            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeShort(SerialVersion.CURRENT);
            writeFastExternal(oos, SerialVersion.CURRENT);

            oos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            /* Should never happen. */
            throw new FaultException(e, false /*isRemote*/);
        }
    }

    /**
     * Deserializes the given bytes that were returned earlier by {@link
     * #toByteArray} and returns the resulting KeyRange.  Values created with
     * either the current or earlier releases can be used with this method, but
     * values created by later releases are not guaranteed to be compatible.
     */
    public static KeyRange fromByteArray(byte[] keyBytes) {

        final ByteArrayInputStream bais = new ByteArrayInputStream(keyBytes);
        try {
            final ObjectInputStream ois = new ObjectInputStream(bais);

            final short serialVersion = ois.readShort();

            return new KeyRange(ois, serialVersion);
        } catch (IOException e) {
            /* Should never happen. */
            throw new FaultException(e, false /*isRemote*/);
        }
    }

    /**
     * Returns the String component that defines lower bound of the key range,
     * or null if no lower bound is enforced.  For a prefix range, this method
     * returns the prefix.
     */
    public String getStart() {
        return start;
    }

    /**
     * Returns whether start is included in the range, i.e., start is less than
     * or equal to the first String component in the key range.  For a prefix
     * range, this method returns true.
     */
    public boolean getStartInclusive() {
        return startInclusive;
    }

    /**
     * Returns the String component that defines upper bound of the key range,
     * or null if no upper bound is enforced.  For a prefix range, this method
     * returns the prefix.
     */
    public String getEnd() {
        return end;
    }

    /**
     * Returns whether end is included in the range, i.e., end is greater than
     * or equal to the last String component in the key range.  For a prefix
     * range, this method returns true.
     */
    public boolean getEndInclusive() {
        return endInclusive;
    }

    /**
     * Returns whether this is a prefix range, i.e., whether start and end are
     * equal and both inclusive.
     */
    public boolean isPrefix() {
        return start != null &&
               startInclusive &&
               start.equals(end) &&
               endInclusive;
    }

    /**
     * Returns true if a Key is within the bounds of this range with respect to
     * a parent Key.
     *
     * @param parentKey the parent of the key range, or null to check the first
     * key component.
     *
     * @param checkKey the full key being checked.
     *
     * @return true if it's in range, false otherwise.
     */
    public boolean inRange(final Key parentKey, final Key checkKey) {

        if (checkKey == null) {
            throw new IllegalArgumentException("checkKey must be non-null");
        }

        if ((parentKey != null) && (!parentKey.isPrefix(checkKey))) {
            return false;
        }

        final List<String> checkPath;
        final int parentPathSize;

        if (parentKey == null) {
            checkPath = checkKey.getMajorPath();
            parentPathSize = 0;
        } else if (parentKey.getMajorPath().size() ==
                   checkKey.getMajorPath().size()) {
            checkPath = checkKey.getMinorPath();
            parentPathSize = parentKey.getMinorPath().size();
        } else {
            checkPath = checkKey.getMajorPath();
            parentPathSize = parentKey.getMajorPath().size();
        }

        if (checkPath.size() <= parentPathSize) {
            return false;
        }

        final String checkComp = checkPath.get(parentPathSize);

        if (isPrefix()) {
            return checkComp.startsWith(start);
        }

        return checkStart(checkComp) && checkEnd(checkComp);
    }

    private boolean checkStart(final String checkComp) {
        if (start == null) {
            return true;
        }
        final int cmp = checkComp.compareTo(start);
        return startInclusive ? (cmp >= 0) : (cmp > 0);
    }

    private boolean checkEnd(final String checkComp) {
        if (end == null) {
            return true;
        }
        final int cmp = checkComp.compareTo(end);
        return endInclusive ? (cmp <= 0) : (cmp < 0);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof KeyRange)) {
            return false;
        }
        final KeyRange o = (KeyRange) other;
        if ((start == null) != (o.start == null)) {
            return false;
        }
        if (start != null && !start.equals(o.start)) {
            return false;
        }
        if ((end == null) != (o.end == null)) {
            return false;
        }
        if (end != null && !end.equals(o.end)) {
            return false;
        }

        /*
         * Only check startInclusive (endInclusive, resp.) if start (end) is
         * set.
         */
        if ((start != null) && (startInclusive != o.startInclusive)) {
            return false;
        }
        if ((end != null) && (endInclusive != o.endInclusive)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return ((start != null) ? start.hashCode() : 0) +
               ((end != null) ? end.hashCode() : 0);
    }

    /**
     * Encodes the KeyRange object and returns the resulting String which can
     * later be decoded using {@link #fromString}. The format is:<p>
     *
     * <code>&lt;startType&gt;/&lt;start&gt;/&lt;end&gt;/&lt;endType&gt;</code>
     *
     * <p>where the <code>startType/endType</code> is either <code>"I"</code>
     * or <code>"E"</code> for Inclusive or Exclusive and <code>start</code>
     * and <code>end</code> are the start/end key strings.  The leading
     * <code>startType/start</code> specification is omitted from the String
     * representation if the range does not have a lower bound. Similarly, the
     * trailing <code>end/endType</code> specification is omitted if the range
     * does not have an upper bound. Since a <code>KeyRange</code> requires at
     * least one bound, at least one of the specifications is always present in
     * the String representation. Here are some examples:</p>
     *
     * <ol>
     * <li><code>I/alpha/beta/E</code> - from <code>alpha</code> inclusive
     * to <code>beta</code> exclusive</li>
     * <li><code>E//0123/I</code> - from "" exclusive to <code>0123</code>
     * inclusive</li>
     * <li><code>I/chi/</code> - from <code>chi</code> inclusive to infinity</li>
     * <li><code>E//</code>- from "" exclusive to infinity</li>
     * <li><code>/chi/E</code> - from negative infinity to <code>chi</code>
     * exclusive</li>
     * <li><code>//I</code> - from negative infinity to "" inclusive</li>
     * </ol>
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(200);

        if (start != null) {
            final Key startKey = Key.createKey(start);
            sb.append(startInclusive ? "I" : "E");
            sb.append(Key.STRING_COMP_DELIM);
            /* Remove leading / that Key.toString() gives us. */
            sb.append(startKey.toString().substring(1));
            sb.append(Key.STRING_COMP_DELIM);
        }

        if (end != null) {
            final Key endKey = Key.createKey(end);
            if (start == null) {
                /* Only output / at the start of end if there was no start. */
                sb.append(Key.STRING_COMP_DELIM);
            }

            /* Remove leading / that Key.toString() gives us. */
            sb.append(endKey.toString().substring(1));
            sb.append(Key.STRING_COMP_DELIM);
            sb.append(endInclusive ? "I" : "E");
        }

        return sb.toString();
    }

    /**
     * Converts an encoded KeyRange back to a KeyRange. The argument String
     * should normally be created using {@link #toString}. See {@link #toString}
     * for more information about the KeyRange String format.
     *
     * @param keyRangeStringArg a non null KeyRange encoding string, which
     * should normally be created using {@link #toString}.
     *
     * @return the resulting KeyRange object.
     *
     * @throws IllegalArgumentException if keyRangeStringArg does not contain
     * a valid KeyRange encoding.
     *
     * @since 2.0
     */
    public static KeyRange fromString(final String keyRangeStringArg) {
        String keyRangeString = keyRangeStringArg;

        /*
         * Plain "/" is not possible since you can't construct a KeyRange
         * with null start and end.
         */
        if (keyRangeString == null ||
            keyRangeString.length() == 0) {
            throw new IllegalArgumentException
                ("Null or zero-length argument to KeyRange.fromString()");
        }

        String retStart = null;
        boolean retStartInclusive = false;
        String retEnd = null;
        boolean retEndInclusive = false;

        /*
         * First check if there's a start key by seeing if the string starts
         * with either "I" or "E". If there is, then it is "I/start/..." or
         * "E/start/..."
         */
        final String firstChar = keyRangeString.substring(0, 1);
        if ("I".equals(firstChar) || "E".equals(firstChar)) {
            retStartInclusive = "I".equals(firstChar);
            /* Strip off "I/" or "E/" */
            if (!"/".equals(keyRangeString.substring(1, 2))) {
                throw new IllegalArgumentException
                    ("Couldn't find starting " + Key.STRING_COMP_DELIM +
                     " for start key in KeyRange. (" + keyRangeStringArg + ")");
            }
            keyRangeString = keyRangeString.substring(2);
            /* Find / that terminates the start key. */
            final int endOfStartKeyIdx =
                keyRangeString.indexOf(Key.STRING_COMP_DELIM);
            if (endOfStartKeyIdx < 0) {
                throw new IllegalArgumentException
                    ("Couldn't find closing " + Key.STRING_COMP_DELIM +
                     " for start key in KeyRange. (" + keyRangeStringArg + ")");
            }
            /* Grab the start key string. */
            retStart = keyRangeString.substring(0, endOfStartKeyIdx);

            /* Strip string to the ending "/" */
            keyRangeString = keyRangeString.substring(endOfStartKeyIdx);
            /* Now we're left with either "/end/I", "/end/E", or "/" */
        }

        /*
         * Check if the remaining string ends with "/I" or "/E".
         */
        int lastCharIdx = keyRangeString.length() - 1;
        final String lastChar = keyRangeString.substring(lastCharIdx);
        if ("I".equals(lastChar) || "E".equals(lastChar)) {
            /* There's an end Key present, so /end/I or /end/E. */
            retEndInclusive = "I".equals(lastChar);
            if (!Key.STRING_COMP_DELIM.equals(keyRangeString.substring(0, 1))) {
                throw new IllegalArgumentException
                    ("Couldn't find starting " + Key.STRING_COMP_DELIM +
                     " for end key in KeyRange.");
            }
            /* Move back from I|E to closing /. */
            lastCharIdx -= 1;
            if (lastCharIdx < 1 ||
                !Key.STRING_COMP_DELIM.equals
                (keyRangeString.substring(lastCharIdx, lastCharIdx + 1))) {
                throw new IllegalArgumentException
                    ("Couldn't find ending " + Key.STRING_COMP_DELIM +
                     " for end key in KeyRange. (" + keyRangeStringArg + ")");
            }

            retEnd = keyRangeString.substring(1, lastCharIdx);
            if (retEnd.contains("/")) {
                throw new IllegalArgumentException
                    ("Extra " + Key.STRING_COMP_DELIM +
                     " in KeyRange. (" + keyRangeStringArg + ")");
            }
        } else if (keyRangeString.length() != 1) {
            throw new IllegalArgumentException
                ("Extra characters on end of KeyRange. (" +
                 keyRangeStringArg + ")");
        }

        if (retStart == null &&
            retEnd == null) {
            throw new IllegalArgumentException
                ("Couldn't find start or end in KeyRange. (" +
                 keyRangeStringArg + ")");
        }

        return new KeyRange(retStart, retStartInclusive,
                            retEnd, retEndInclusive);
    }
}
