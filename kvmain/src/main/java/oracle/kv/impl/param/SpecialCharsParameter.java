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

package oracle.kv.impl.param;

/**
 * Parameter to hold the value of allowed special characters.
 *
 * Only the characters in the default value of allowed special characters are
 * permitted in this parameter. @see ParameterState
 */
public class SpecialCharsParameter extends Parameter {

    private static final long serialVersionUID = 1L;

    private static final String PERMITED_SPECIAL =
        ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL_DEFAULT;

    private char[] value;

    public SpecialCharsParameter(String name, String value) {
        super(name);
        parseSpecialChars(value);
    }

    @Override
    public String asString() {
        return new String(value);
    }

    private void parseSpecialChars(String val) {
        final char[] values = val.toCharArray();
        /*
         * Check whether it is legal value
         */
        for (char c : values) {
            if (PERMITED_SPECIAL.indexOf(c) == -1) {
                throw new IllegalArgumentException(
                   "Unsupported value for allowed special characters: '" +
                   c + "'");
            }
        }
        value = val.toCharArray();
    }

    @Override
    public ParameterState.Type getType() {
        return ParameterState.Type.SPECIALCHARS;
    }
}
