/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import java.lang.annotation.Annotation;

import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.Parameterized;

/**
 * A subclass of the Parameterized JUnit test runner that identifies
 * parameterized test methods using just the method name, not including the
 * parameter indices, so that parameterized methods can be filtered using the
 * standard testcase.methods system property.
 */
public class FilterableParameterized extends Parameterized {

    /** Required constructor. */
    public FilterableParameterized(Class<?> klass) throws Throwable {
        super(klass);
    }

    /** Override filtering to use plain method names. */
    @Override
    public void filter(Filter filter) throws NoTestsRemainException {
        super.filter(new FilterWrapper(filter));
    }

    /**
     * A delegating Filter that uses plain method names when determining if a
     * test should be run.
     */
    private static class FilterWrapper extends Filter {
        private final Filter filter;

        private FilterWrapper(Filter filter) {
            this.filter = filter;
        }

        @Override
        public boolean shouldRun(Description description) {
            return filter.shouldRun(wrap(description));
        }

        @Override
        public String describe() {
            return filter.describe();
        }
    }

    /** Convert to plain method names. */
    private static Description wrap(Description description) {
        final String name = description.getDisplayName();
        final String plainName = plainName(name);
        final Description clonedDescription =
            Description.createSuiteDescription(
                plainName,
                description.getAnnotations().toArray(new Annotation[0]));
        for (final Description child : description.getChildren()){
            clonedDescription.addChild(wrap(child));
        }
        return clonedDescription;
    }

    /** Convert a possibly parameterized method name to a plain name. */
    private static String plainName(String name) {
        /*
         * Parameters themselves are named [0], [1], etc. -- just return
         * those unchanged.
         */
        if (name.startsWith("[")){
            return name;
        }

        /* Convert methodName[index](className) to methodName(className) */
        int open = name.indexOf('[');
        int close = name.indexOf(']');
        return name.substring(0, open).concat(name.substring(close + 1));
    }
}
