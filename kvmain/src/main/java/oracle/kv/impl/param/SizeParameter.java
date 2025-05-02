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
 * A size parameter. The size parameter is a String parameter which will
 * parse the formated value into a long. The string may have a unit to allow
 * specifying large numbers in a convenient fashion. The String format is:
 * "long [unit]". Accepted unit strings are: KB, MB, GB, TB. All units are
 * case-insensitive.  The separator for the two tokens can be: " ", "-", "_".
 *
 * Valid examples:
 *   "1"
 *   "10 MB"
 *   "1_gb"
 *   "1-TB"
 * Invalid:
 *   "1mb", etc.
 */
public class SizeParameter extends StringParameter {

    private static final long serialVersionUID = 1L;

    /**
     * Allow various formats (N is long, SU is SizeUnit):
     *  "N SU", "N_SU", "N-SU"
     */
    private static final String[] allowedRegex = {"\\s+", "-", "_"};

    private final long size;

    /**
     * Constructs a SizeParameter with the specified name and value. If value
     * is null, the parameter's value is set to "0".
     *
     * @param name the name
     * @param value the value or null
     */
    public SizeParameter(String name, String value) {
        super(name, (value == null) ? "0" : value);
        this.size = parseSize(value);
    }

    @Override
    public long asLong() {
        return size;
    }

    private static long parseSize(String value) {
        if ((value == null) || ("".equals(value))) {
            return 0L;
        }
        for (String regex : allowedRegex) {
            final String[] tokens = value.split(regex);

            if (tokens.length == 1) {
                try {
                    return Long.parseLong(tokens[0]);
                } catch (NumberFormatException nfe) {
                    /* fall through */
                }
            } else if (tokens.length == 2) {
                try {
                    long amount = Long.parseLong(tokens[0]);

                    SizeUnit unit = SizeUnit.valueOf(tokens[1].toUpperCase
                                            (java.util.Locale.ENGLISH));
                    amount = unit.convert(amount);
                    return amount;
                } catch (IllegalArgumentException e) {
                    /* fall through */
                }
            }
        }
        throw new IllegalArgumentException("Invalid size format: " + value);
    }

    /**
     * Returns the size of the specified parameter. The parameter type must be
     * SIZE, STRING, or NONE (NullParameter). If it is STRING the value will be
     * parsed according to the rules for the SIZE parameter type. A type of
     * NONE will return 0. If the parameter type is not SIZE, STRING, or NONE,
     * or the value does not parse as a size, an IllegalArgumentException is
     * thrown.
     *
     * @param p parameter
     * @return a size
     */
    public static long getSize(Parameter p) {
        switch (p.getType()) {
        case SIZE :
            return p.asLong();
        case STRING :
            return parseSize(p.asString());
        case NONE :
            return 0L;
        default:
        }
        /* Don't mention NONE since that is for internal use only */
        throw new IllegalArgumentException("Parameter must be of type SIZE " +
                                           "or STRING, was " + p.getType());
    }

    @Override
    public ParameterState.Type getType() {
        return ParameterState.Type.SIZE;
    }

    /**
     * Compare based on absolute value, not the unit.
     */
    @Override
    public boolean equals(Parameter other) {
        if (!(other instanceof SizeParameter)) {
            return false;
        }
        if (!getName().equals(other.getName())) {
            return false;
        }
        return asLong() == other.asLong();
    }

    /**
     * Allow (case-insensitive) kb, mb, gb, tb as units.
     */
    private enum SizeUnit {
        KB() {
            @Override
            protected long convert(long size) { return size * 1024; }
        },
        MB() {
            @Override
            protected long convert(long size) { return size * 1024 * 1024; }
        },
        GB {
            @Override
            protected long convert(long size) {
                return size * 1024 * 1024 * 1024;
            }
        },
        TB {
            @Override
            protected long convert(long size) {
                return size * 1024 * 1024 * 1024 * 1024;
            }
        };

        /**
         * Converts the specified value into a unit-less value.
         */
        protected abstract long convert(long size);
    }
}