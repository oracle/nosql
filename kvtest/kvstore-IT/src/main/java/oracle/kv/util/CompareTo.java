/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

/**
 * A JUnit matcher that compares a comparable value to the specified value.
 *
 * @param <T> the type of the comparable value
 */
public class CompareTo<T extends Comparable<T>> extends BaseMatcher<T> {

    private final T value;
    private final boolean greater;
    private final boolean equal;

    /**
     * Returns a matcher that checks for a value greater than the specified
     * value.
     *
     * @param value the value to check against
     * @return the matcher
     */
    public static <T extends Comparable<T>> Matcher<T> greaterThan(T value) {
        return new CompareTo<T>(value, true, false);
    }

    /**
     * Returns a matcher that checks for a value greater than or equal to the
     * specified value.
     *
     * @param value the value to check against
     * @return the matcher
     */
    public static <T extends Comparable<T>> Matcher<T> greaterThanEqual(
        T value) {

        return new CompareTo<T>(value, true, true);
    }

    /**
     * Returns a matcher that checks for a value less than the specified value.
     *
     * @param value the value to check against
     * @return the matcher
     */
    public static <T extends Comparable<T>> Matcher<T> lessThan(T value) {
        return new CompareTo<T>(value, false, false);
    }

    /**
     * Returns a matcher that checks for a value less than or equal to the
     * specified value.
     *
     * @param value the value to check against
     * @return the matcher
     */
    public static <T extends Comparable<T>> Matcher<T> lessThanEqual(T value) {
        return new CompareTo<T>(value, false, true);
    }

    /**
     * Creates a matcher that compares to the specified value.
     *
     * @param value the value to compare with
     * @param greater check for greater than the value
     * @param equal allow check to be equal
     */
    public CompareTo(T value, boolean greater, boolean equal) {
        if (value == null) {
            throw new NullPointerException("Value must not be null");
        }
        this.value = value;
        this.greater = greater;
        this.equal = equal;
    }

    @Override
    public boolean matches(Object item) {
        if (!(item instanceof Comparable)) {
            return false;
        }

        /* Catch the cast exception later to account for this unchecked cast */
        @SuppressWarnings("unchecked")
        T typedItem = (T) item;
        final int signum;
        try {
            signum = typedItem.compareTo(value);
        } catch (ClassCastException e) {
            return false;
        }
        return (signum == 0) ? equal :
            greater ? (signum > 0) :
            (signum < 0);
    }

    @Override
    public void describeTo(Description desc) {
        desc.appendText(" value ")
            .appendText(greater ? "greater" : "less")
            .appendText(" than ")
            .appendText(equal ? "or equal to " : "")
            .appendValue(value);
    }
}
