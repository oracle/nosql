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

package oracle.kv.impl.query;

import java.util.Objects;

import oracle.kv.impl.fault.WrappedClientException;

/**
 * A class to hold query exceptions indicating syntactic or semantic problems
 * with a query. It can be thrown at both the client and the server. It is
 * internal use only and it will be caught, and rethrown as
 * IllegalArgumentException to the application. It includes location information.
 * When converted to an IAE, the location info is put into the message created
 * for the IAE.
 *
 * It does not need to be Serializable as it is never passed across the
 * client/server RMI interface. This is because, when the exception is thrown
 * at the server, it will be caught and converted to an IAE, which will the
 * be wrapped inside a WrappedClientException and sent to the client as such.
 *
 * @since 4.0
 */
public class QueryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Location of an expression in the query. It contains both start and
     * end, line and column info.
     */
    public static class Location {

        private final int startLine;
        /* defined as char position in line */
        private final int startColumn;
        private final int endLine;
        /* defined as char position in line */
        private final int endColumn;

        public Location(
            int startLine,
            int startColumn,
            int endLine,
            int endColumn) {
            this.startLine = startLine;
            this.startColumn = startColumn;
            this.endLine = endLine;
            this.endColumn = endColumn;

            assert(startLine >= 0);
            assert(startColumn >= 0);
            assert(endLine >= 0);
            assert(endColumn >= 0);
        }

        @Override
        public String toString() {
            return startLine + ":" + startColumn + "-" + endLine + ":" +
                endColumn;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Location)) {
                return false;
            }
            final Location other = (Location) obj;
            return (startLine == other.startLine) &&
                (startColumn == other.startColumn) &&
                (endLine == other.endLine) &&
                (endColumn == other.endColumn);
        }

        @Override
        public int hashCode() {
            return Objects.hash(startLine, startColumn, endLine, endColumn);
        }

        /**
         * Returns the start line.
         */
        public int getStartLine() {
            return startLine;
        }

        /**
         * Returns the start column as its char position in line.
         */
        public int getStartColumn() {
            return startColumn;
        }

        /**
         * Returns the end line.
         */
        public int getEndLine() {
            return endLine;
        }

        /**
         * Returns the end column as its char position in line.
         */
        public int getEndColumn() {
            return endColumn;
        }
    }

    protected final Location location;

    public QueryException(
        String message,
        Throwable cause,
        QueryException.Location location) {
        super(message, cause);
        this.location = location;
    }

    public QueryException(String message, QueryException.Location location) {
        super(message);
        this.location = location;
    }

    public QueryException(Throwable cause, QueryException.Location location) {
        super(cause);
        this.location = location;
    }

    public QueryException(Throwable cause) {
        super(cause);
        location = null;
    }

    public QueryException(String message) {
        super(message);
        location = null;
    }

    /**
     * Returns the location associated with this exception. May be null if not
     * available.
     */
    public QueryException.Location getLocation() {
        return location;
    }

    /**
     * Get this exception as a simple IAE, wrapped so that it can pass to
     * the client transparently. This is called on the server side.
     */
    public RuntimeException getWrappedIllegalArgument() {
        return new WrappedClientException(
            new IllegalArgumentException(toString()));
    }

    /**
     * Get this exception as a simple IAE, not wrapped. This is used on the
     * client side.
     */
    public IllegalArgumentException getIllegalArgument() {
        return new IllegalArgumentException(toString());
    }

    @Override
    public String toString() {
        return "Error:" + (location == null ? "" : " at (" + location.startLine +
            ", " + location.startColumn + ")" ) + " " + getMessage();
    }
}
