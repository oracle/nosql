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

import java.io.Serializable;

/**
 * This is a base class for parameters in the store.  Parameters are typed and
 * are limited to the types enumerated in ParameterState.Type.  Each of those
 * types has an implementation class derived from Parameter.
 *
 * Parameter objects for a given service are kept in a ParameterMap, which
 * is a simple collection that maps names to Parameter.  ParameterMap is the
 * object that is passed across RMI calls to configure services.  It is also
 * serialized to the Storage Node Agent's configuration file to define the
 * configuration for a given service.
 *
 * All known Parameter instances have meta-data that is stored in a
 * ParameterState object.  ParameterState also has a static map of
 * ParameterState objects which serve as a type catalog for known parameters.
 * ParameterState also includes the default values for individual parameters
 * and has a validation mechanism for validating ranges and enumerations
 * should that be desired.
 *
 * ParameterUtils is a small set of common utility methods.
 *
 * ParameterTracker and ParameterListener implement a way that services can be
 * notified of parameter changes at runtime (vs restart).
 *
 * When adding a new parameter to the system the work is done primarily in
 * ParamterState (see comments there).
 */
public abstract class Parameter implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String name;

    public Parameter(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Returns a String representation of the parameter's value.
     * All parameters must be representable as String.
     */
    public abstract String asString();

    /**
     * All parameters have a type.
     */
    public abstract ParameterState.Type getType();

    /**
     * Returns a String description of the parameter. The description includes
     * the parameter name, type, and value. Use asString() for getting a
     * representation of just the value.
     */
    @Override
    public String toString() {
        return "Parameter[" + name + ", " + getType() + ", " + asString() + "]";
    }

    /**
     * This will return true if both are the NullParameter
     */
    public boolean equals(Parameter other) {
        if (!getName().equals(other.getName())) {
            return false;
        }
        String thisString = asString();
        String otherString = other.asString();
        if (thisString != null) {
            return thisString.equals(otherString);
        }
        if (otherString == null) {
            return true;
        }
        return false;
    }

    public boolean restartRequired() {
        return ParameterState.restartRequired(name);
    }

    /**
     * Default type conversion methods.
     */
    public boolean asBoolean() {
        unsupportedType("boolean");
        return false;
    }

    public int asInt() {
        unsupportedType("int");
        return 0;
    }

    public long asLong() {
        unsupportedType("long");
        return 0;
    }

    public Enum<?> asEnum() {
        unsupportedType("enum");
        return null;
    }

    public static Parameter createKnownType(String name,
                                            String value,
                                            String type) {

        ParameterState.Type ptype =
            Enum.valueOf(ParameterState.Type.class, type);
        switch (ptype) {
        case INT:
            return new IntParameter(name, value);
        case LONG:
            return new LongParameter(name, value);
        case STRING:
            return new StringParameter(name, value);
        case BOOLEAN:
            return new BooleanParameter(name, value);
        case CACHEMODE:
            return new CacheModeParameter(name, value);
        case DURATION:
            return new DurationParameter(name, value);
        case AUTHMETHODS:
            return new AuthMethodsParameter(name, value);
        case SPECIALCHARS:
            return new SpecialCharsParameter(name, value);
        case SIZE:
            return new SizeParameter(name, value);
        case TIME_TO_LIVE:
            return new TimeToLiveParameter(name, value);
        case NONE:
            break;
        }
        return null;
    }

    private void unsupportedType(String type) {
        throw new IllegalStateException
            ("Parameter (" + name + ") cannot be represented as " + type);
    }

    /**
     * This method is used during initialization of the HashMap, pstate.  It is
     * called indirectly so that it can either exist or not.  In the webapp
     * version of this file this will not exist.
     */
    public static Parameter createParameter(String name,
                                            String value,
                                            ParameterState.Type type) {
        switch (type) {
        case INT:
            int ivalue = Integer.parseInt(value);
            return new IntParameter(name, ivalue);
        case LONG:
            long lvalue = Long.parseLong(value);
            return new LongParameter(name, lvalue);
        case BOOLEAN:
            return new BooleanParameter(name, value);
        case STRING:
            return new StringParameter(name, value);
        case CACHEMODE:
            return new CacheModeParameter(name, value);
        case DURATION:
            return new DurationParameter(name, value);
        case AUTHMETHODS:
            return new AuthMethodsParameter(name, value);
        case SPECIALCHARS:
            return new SpecialCharsParameter(name, value);
        case SIZE:
            return new SizeParameter(name, value);
        case TIME_TO_LIVE:
            return new TimeToLiveParameter(name, value);
        case NONE:
            break;
        }
        /* this should never happen */
        throw new IllegalArgumentException("Invalid type: " + type);
    }

    /**
     * Construct a Parameter based on its type and String value.  If
     * appropriate the value is validated.  Handle null value gracefully --
     * do nothing and return null.
     * TODO: add more validation (e.g. String).  CacheMode will be validated
     * on construction.
     *
     * @return the new Parameter, or null if the value was null.
     *
     * @throws IllegalStateException if an unknown name is passed and
     * ignoreUnknown is false.
     */
    public static Parameter createParameter
        (String name, String value, boolean ignoreUnknown) {
        ParameterState state = ParameterState.lookup(name);
        if (state != null) {
            if (value == null) {
                return null;
            }
            switch (state.getType()) {
            case INT:
                int ivalue = Integer.parseInt(value);
                state.validate(name, ivalue, true);
                return new IntParameter(name, ivalue);
            case LONG:
                long lvalue = Long.parseLong(value);
                state.validate(name, lvalue, true);
                return new LongParameter(name, lvalue);
            case BOOLEAN:
                return new BooleanParameter(name, value);
            case STRING:
                return new StringParameter(name, value);
            case CACHEMODE:
                return new CacheModeParameter(name, value);
            case DURATION:
                return new DurationParameter(name, value);
            case AUTHMETHODS:
                return new AuthMethodsParameter(name, value);
            case SPECIALCHARS:
                return new SpecialCharsParameter(name, value);
            case SIZE:
                return new SizeParameter(name, value);
            case TIME_TO_LIVE:
                return new TimeToLiveParameter(name, value);
            case NONE:
                break;
            }
        }
        if (ignoreUnknown) {
            return null;
        }
        throw new IllegalStateException("Invalid parameter: " + name);
    }

    public static Parameter createParameter
        (String name, String value) {
        return createParameter(name, value, false);
    }
}
