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

package oracle.kv.impl.async.perf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a reference to an element of a json document.
 *
 * <p>
 * This class is a simplified alternative to json-pointer [RFC6901]. I am not
 * using json-pointer for the following reasons: (1) I do not need the full
 * functionality of json pointer. For example, we usually do not need to worry
 * about special characters such as "/" and "~" in the keys, therefore escaping
 * is not necessary. As another example, we do not need the evaluation part of
 * a json-pointer on a json document. In a word, it does not seem to be
 * worthwhile for me to complete a json pointer implementation; (2) We are
 * avoiding using third-party libraries for json related issues; (3) The cloud
 * team seems to prefer a change of the string syntax for the pointer.
 * Specifically, they want the separator to be "_" instead of "/".  For the
 * above reasons, I am not implementing a json pointer, but instead a
 * simplified represention of the reference.
 */
public class JsonReference {

    /**
     * Represents a key to reference a direct child element of a json document.
     *
     * <p>
     * There are three types of {@code JsonKeys}. A {@link RootKey} points to
     * the root json document itself which could be a primitive json, an array
     * or an object. An {@link ArrayKey} is an integer that points to a direct
     * child element of a json array. An {@link ObjectKey} is a string that
     * points to a direct child element of a json object. For example, given an
     * json array ["a", "b"], the {@code JsonKey#ROOT_KEY} can be used to point
     * to reference, and a {@code ArrayKey} of value 0 can be used to point to
     * "a".
     */
    public interface JsonKey<T> {

        /** A singleton root key. */
        RootKey ROOT_KEY = new RootKey();

        /**
         * Returns the value of the key.
         */
        T get();
    }

    /**
     * A root key points to the root document itself.
     */
    public static class RootKey implements JsonKey<Void> {

        @Override
        public Void get() {
            return null;
        }

        @Override
        public String toString() {
            return "";
        }
    }

    /**
     * An array key points to a direct child element of a json array. For
     * example, for a json array ["a", "b"], the array key of value 0 points to
     * the element "a".
     */
    public static class ArrayKey implements JsonKey<Integer> {

        private final int val;

        public ArrayKey(int val) {
            this.val = val;
        }

        @Override
        public Integer get() {
            return val;
        }

        @Override
        public String toString() {
            return Integer.toString(val);
        }
    }

    /**
     * A object key points to a direct child element of a json object. For
     * example, for a json object {"a" : 0, "b" : 1}, the object key of value
     * "a" points to the element 0.
     */
    public static class ObjectKey implements JsonKey<String> {

        private final String val;

        public ObjectKey(String val) {
            this.val = val;
        }

        @Override
        public String get() {
            return val;
        }

        @Override
        public String toString() {
            return val;
        }
    }

    /** A singleton root reference. */
    public static final JsonReference ROOT = new JsonReference();

    /**
     * The list of json keys which, combined together, represents the path to
     * navigate to the reference through multiple levels of Json instances.
     */
    private final List<JsonKey<?>> keys;

    /**
     * Constructs a reference from a list.
     */
    public JsonReference(List<JsonKey<?>> keys) {
        this.keys = Collections.unmodifiableList(new ArrayList<>(keys));
    }

    /**
     * Constructs a root reference.
     */
    private JsonReference() {
        this.keys = Collections.unmodifiableList(
            Arrays.asList(JsonKey.ROOT_KEY));
    }

    /**
     * Returns the keys as a list.
     */
    public List<JsonKey<?>> getKeys() {
        return keys;
    }

    /**
     * Returns a string representation of the reference.
     */
    public String getString(String separator) {
        return keys.stream()
            .map((r) -> r.toString())
            .collect(Collectors.joining(separator));
    }

    @Override
    public String toString() {
        return getString("/");
    }

    /**
     * Returns the last key.
     */
    public Object getLastKey() {
        return keys.get(keys.size() - 1).get();
    }

    /**
     * Returns the last key string of the reference.
     */
    public String getLastKeyString() {
        return keys.get(keys.size() - 1).toString();
    }
}
