/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.Consistency;

/**
 * Utilities to parse and print consistency values for command line tests. <p>
 *
 * Supported values:
 * <ul>
 * <li> {@code ABSOLUTE} - for {@link Consistency#ABSOLUTE}
 * <li> {@code NONE_REQUIRED} - for {@link Consistency#NONE_REQUIRED}
 * <li> {@code NONE_REQUIRED_NO_MASTER} - for
 *      {@link Consistency#NONE_REQUIRED_NO_MASTER}
 * <li> {@code lag=<ms>,timeout=<ms>} - for an instance of {@link
 *      oracle.kv.Consistency.Time} with the specified {@code permissibleLag}
 *      and {@code timeout} values measured in milliseconds.
 * </ul> <p>
 *
 * Parsing is case insensitive. <p>
 *
 * Note that this class does not provide support for {@link
 * oracle.kv.Consistency.Version}.
 */
@SuppressWarnings("javadoc")
public final class ConsistencyArgument {

    /** Regular expression pattern for specifying Consistency.Time. */
    private static final Pattern CONSISTENCY_TIME_PATTERN =
        Pattern.compile("lag=([0-9]+),timeout=([0-9]+)",
                        Pattern.CASE_INSENSITIVE);

    /** This class should not be instantiated. */
    private ConsistencyArgument() { }

    /**
     * Parses a consistency value.
     *
     * @param string the input string
     * @return the consistency value
     * @throws IllegalArgumentException if the input string is not recognized
     */
    @SuppressWarnings("deprecation")
    public static Consistency parseConsistency(String string) {
        if (string == null) {
            throw new IllegalArgumentException("The argument must not be null");
        } else if (("ABSOLUTE").equalsIgnoreCase(string)) {
            return Consistency.ABSOLUTE;
        } else if (("NONE_REQUIRED").equalsIgnoreCase(string)) {
            return Consistency.NONE_REQUIRED;
        } else if (("NONE_REQUIRED_NO_MASTER").equalsIgnoreCase(string)) {
            return Consistency.NONE_REQUIRED_NO_MASTER;
        }
        final Matcher matcher = CONSISTENCY_TIME_PATTERN.matcher(string);
        if (!matcher.matches()) {
            throw new IllegalArgumentException
                ("Unrecognized consistency: " + string);
        }
        final long lag = Long.parseLong(matcher.group(1));
        final long timeout = Long.parseLong(matcher.group(2));
        return new Consistency.Time(lag, MILLISECONDS, timeout, MILLISECONDS);
    }

    /**
     * Returns a string for the consistency value in a format understood by
     * {@link #parseConsistency}.
     *
     * @param consistency the consistency value
     * @return the string
     * @throws IllegalArgumentException if the argument is not {@link
     *         Consistency#ABSOLUTE}, {@link Consistency#NONE_REQUIRED},
     *         {@link Consistency#NONE_REQUIRED_NO_MASTER}, or an object
     *         with class {@link oracle.kv.Consistency.Time}
     */
    @SuppressWarnings("deprecation")
    public static String toString(Consistency consistency) {
        if (consistency == Consistency.ABSOLUTE) {
            return "ABSOLUTE";
        } else if (consistency == Consistency.NONE_REQUIRED) {
            return "NONE_REQUIRED";
        } else if (consistency == Consistency.NONE_REQUIRED_NO_MASTER) {
            return "NONE_REQUIRED_NO_MASTER";
        } else if (consistency == null) {
            throw new IllegalArgumentException("The argument must not be null");
        } else if (consistency.getClass() == Consistency.Time.class) {
            final Consistency.Time time = (Consistency.Time) consistency;
            return "lag=" + time.getPermissibleLag(MILLISECONDS) +
                ",timeout=" + time.getTimeout(MILLISECONDS);
        } else {
            throw new IllegalArgumentException
                ("Unrecognized consistency: " + consistency);
        }
    }
}
