/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonLocation;

/**
 * An exception indicating a problem parsing JSON. This exception encapsulates
 * both illegal JSON as well as {@link IOException} errors during parsing. If
 * available the location in the JSON document is provided.
 */
public class JsonParseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private JsonLocation location;

    /**
     * @hidden
     * @param msg the exception message
     */
    public JsonParseException(String msg) {
        super(msg);
    }

    /**
     * @hidden
     * @param msg the exception message
     * @param location the exception location in the input
     */
    public JsonParseException(String msg, JsonLocation location) {
        super(msg);
        this.location = location;
    }

    /**
     * Returns the column number of the error within a line if available,
     * otherwise a negative number is returned.
     *
     * @return the column, or -1
     */
    public int getColumn() {
        if (location != null && location != JsonLocation.NA) {
            return location.getColumnNr();
        }
        return -1;
    }

    /**
     * Returns the line number of the error within a line if available, otherwise a
     * negative number is returned.
     *
     * @return the line, or -1
     */
    public int getLine() {
        if (location != null && location != JsonLocation.NA) {
            return location.getLineNr();
        }
        return -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getMessage()).append(" at line ");
        sb.append(lineToString()).append(", column ");
        sb.append(columnToString());
        return sb.toString();
    }

    /**
     * @hidden
     * @return the location of the exception in the input
     */
    public JsonLocation getLocation() {
        return location;
    }

    private String lineToString() {
        return (getLine() >= 0 ? Integer.toString(getLine()) : "n/a");
    }

    private String columnToString() {
        return (getColumn() >= 0 ? Integer.toString(getColumn()) : "n/a");
    }
}
