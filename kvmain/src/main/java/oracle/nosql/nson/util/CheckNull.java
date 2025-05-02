/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.util;

import java.util.Objects;

/**
 * @hidden
 * Wrapper for calls to Objects.requireNonNull
 */
public class CheckNull {

    public static void requireNonNull(Object value, String message) {
        Objects.requireNonNull(value, message);
    }

    /*
     * throws IAE instead of NPE
     */
    public static void requireNonNullIAE(Object value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
    }
}
