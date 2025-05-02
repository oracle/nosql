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

import java.util.concurrent.TimeUnit;

/**
 * String format is "long Unit"
 * Accepted units are valid TimeUnit strings, as well as NS, US, MS, S, MIN, H.
 * All units are case-insensitive.  The separator for the two tokens can be:
 * " ", "-", "_".  More could possibly be added.
 *
 * Valid examples:
 *   "1 SECONDS"
 *   "10 minutes"
 *   "1_ns"
 *   "500 ms"
 * Invalid:
 *   "1s", "10 sec", etc.
 *
 */
public class DurationParameter extends Parameter {

    private static final long serialVersionUID = 1L;
    /**
     * Allow various formats (N is Long, TU is TimeUnit):
     *  "N TU", "N_TU", "N-TU"
     */
    private static final String[] allowedRegex = {"\\s+", "-", "_"};

    private TimeUnit unit;
    private long amount;

    public DurationParameter(String name, String value) {
        super(name);
        parseDuration(value);
    }

    public DurationParameter(String name, TimeUnit unit, long amount) {
        super(name);
        this.unit = unit;
        this.amount = amount;
    }

    /**
     * Compare based on absolute value, not the unit.
     */
    @Override
    public boolean equals(Parameter other) {
        if (!(other instanceof DurationParameter)) {
            return false;
        }
        if (!getName().equals(other.getName())) {
            return false;
        }
        return toMillis() == ((DurationParameter)other).toMillis();
    }

    public long getAmount() {
        return amount;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public long toMillis() {
        return unit.toMillis(amount);
    }

    /* For unit tests - parameters should not be mutable */
    public void setMillis(long millis) {
        unit = TimeUnit.MILLISECONDS;
        amount = millis;
    }

    private void parseDuration(String value) {
        for (String regex : allowedRegex) {
            String[] tokens = value.split(regex);
            if (tokens.length == 2) {
                amount = Long.parseLong(tokens[0]);
                try {
                    unit = TimeUnit.valueOf(tokens[1].toUpperCase
                                            (java.util.Locale.ENGLISH));
                    return;
                } catch (IllegalArgumentException e) {
                    /**
                     * Try IEEE unit values (see below).
                     */
                    try {
                        unit = IEEEUnit.unit(tokens[1]);
                        return;
                    } catch (IllegalArgumentException e1) {
                        /* fall through */
                    }
                }
            }
        }
        throw new IllegalArgumentException
            ("Invalid duration format: " + value);
    }

    String asString(Character separator) {
        return Long.toString(amount) + separator + unit.toString();
    }

    @Override
    public String asString() {
        return asString(' ');
    }

    @Override
    public ParameterState.Type getType() {
        return ParameterState.Type.DURATION;
    }

    /**
     * Allow (case-insensitive) ns, us, ms, s, min, h as units.
     */
    private enum IEEEUnit {
        NS() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.NANOSECONDS;
            }
        },

        US() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.MICROSECONDS;
            }
        },

        MS() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.MILLISECONDS;
            }
        },

        S() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.SECONDS;
            }
        },

        MIN() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.MINUTES;
            }
        },

        H() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.HOURS;
            }
        };

        abstract TimeUnit getUnit();

        static TimeUnit unit(String value) {
            return valueOf(value.toUpperCase(java.util.Locale.ENGLISH)).
                getUnit();
        }
    }
}
