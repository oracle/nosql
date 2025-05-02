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

package oracle.kv.impl.api.table;

/**
 * A simple class to hold formatting information, such as indent level.
 * At this time it only encapsulates indentation and for JSON a key
 * separator, comma, and newline.
 *
 * indent:
 * The current number of space chars to be printed as indentation on display.
 * separator:
 *  either ":" or " : " depending on whether pretty print is used
 * newline:
 *  if pretty print, '\n', otherwise no-op.
 * comma:
 *  if pretty print, ", ", otherwise ","
 */
public class DisplayFormatter {

    private final int indentIncrement;

    private final String separator;

    private final String comma;

    private final boolean newline;

    private int indent;

    /* Indicates if append "Z" to Timestamp string */
    private final boolean timestampWithZone;

    private static final int DEFAULT_INDENT = 2;

    public DisplayFormatter(int increment,
                            boolean pretty,
                            boolean timestampWithZone) {
        indentIncrement = pretty ? increment : 0;
        separator = pretty ? " : " : ":";
        newline = pretty;
        comma = pretty ? ", " : ",";
        this.timestampWithZone = timestampWithZone;
    }

    public DisplayFormatter() {
        this(DEFAULT_INDENT, false, true);
    }

    public DisplayFormatter(boolean pretty) {
        this(DEFAULT_INDENT, pretty, true);
    }

    public int getIndent() {
        return indent;
    }

    public int getIndentIncrement() {
        return indentIncrement;
    }

    public void setIndent(int v) {
        indent = v;
    }

    public void incIndent() {
        indent += indentIncrement;
    }

    public void decIndent() {
        indent -= indentIncrement;
    }

    public void indent(StringBuilder sb) {
        for (int i = 0; i < indent; ++i) {
            sb.append(' ');
        }
    }

    public void separator(StringBuilder sb) {
        sb.append(separator);
    }

    public void comma(StringBuilder sb) {
        sb.append(comma);
    }

    public void newline(StringBuilder sb) {
        if (newline) {
            sb.append('\n');
        }
    }

    /**
     * Convenience methods for JSON
     */
    public void startObject() {
        incIndent();
    }

    public void endObject(StringBuilder sb, int numFields) {
        decIndent();
        /*
         * If no fields were displayed the object should just be
         * "{}" on the same line.
         */
        if (numFields > 0) {
            newline(sb);
            indent(sb);
        }
    }

    /**
     * Used to prepare for a new key/value pair in an object.
     */
    public void newPair(StringBuilder sb, boolean leadingComma) {
        if (leadingComma) {
            sb.append(',');
        }
        newline(sb);
        indent(sb);
    }

    public boolean getTimestampWithZone() {
        return timestampWithZone;
    }
}
