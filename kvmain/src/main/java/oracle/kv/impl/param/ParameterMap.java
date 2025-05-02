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

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;



/**
 * A named, sorted set of parameters.
 * Parameters returned by the iterator are sorted by name.
 *
 *  Version number for catalog and map to match.
 *  Map vs table.
 *  Deal with enums...
 *
 * Questions:
 *   o better validation -- abstract method?  if so, how/when to set.
 *   o standalone package, no external imports
 *   o what is persisted?
 *
 *   o use a test case to validate the enum strings
 */
public class ParameterMap implements Iterable<Parameter>, Serializable {
    private static final long serialVersionUID = 1L;
    private final static String eol = System.getProperty("line.separator");
    private static final String INDENT = "  ";
    private static final String PROPINDENT = "    ";
    private static final Parameter NULL_PARAMETER = new NullParameter();

    private String name;
    private String type;
    private boolean validate;
    private final int version;

    /* The persisted field is not used at runtime. Next time use an Interface */
    private HashMap<String, Parameter> parameters = null;

    /**
     * Parameters are kept in a sorted map while in memory. The parameters are
     * persisted via the parameters field for compatibility. This field can be
     * null and should only be accessed via getSortedParameters().
     */
    private transient SortedMap<String, Parameter> sortedParameters;

    /* Create a typeless map */
    public ParameterMap() {
        this(null, null, true, ParameterState.PARAMETER_VERSION);
    }

    public ParameterMap(String name, String type) {
        this(name, type, true, ParameterState.PARAMETER_VERSION);
    }

    public ParameterMap(String name,
                        String type,
                        boolean validate,
                        int version) {
        this.name = name;
        this.type = type;
        this.validate = validate;
        this.version = version;
    }

    @Override
    public Iterator<Parameter> iterator() {
        return getSortedParameters().values().iterator();
    }

    public boolean isEmpty() {
        return getSortedParameters().isEmpty();
    }

    public int size() {
        return getSortedParameters().size();
    }

    public int getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setValidate(boolean value) {
        this.validate = value;
    }

    public ParameterMap copy() {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);
        newParams.getSortedParameters().putAll(getSortedParameters());
        return newParams;
    }

    public ParameterMap filter(EnumSet<ParameterState.Info> set) {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);

        for (Parameter p : getSortedParameters().values()) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if (pstate != null && pstate.containsAll(set)) {
                newParams.put(p);
            }
        }
        return newParams;
    }

    /**
     * Create a new map, filtering parameters based on set:
     *  if (positive) and name is in the set, include it
     *  if (!positive) and name is not in the set, include it
     * Skip parameters entirely if they cannot be found in
     * ParameterState.
     */
    public ParameterMap filter(Set<String> set, boolean positive) {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);

        for (Parameter p : getSortedParameters().values()) {
            /* skip unknown parameters */
            if (ParameterState.lookup(p.getName()) != null) {
                if (set.contains(p.getName())) {
                    if (positive) {
                        newParams.put(p);
                    }
                } else if (!positive) {
                    newParams.put(p);
                }
            }
        }
        return newParams;
    }

    /**
     * Filter out read-only parameters.  Ignore unrecognized parameters.
     */
    public ParameterMap readOnlyFilter() {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);

        for (Parameter p : getSortedParameters().values()) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if (pstate != null && !pstate.getReadOnly()) {
                newParams.put(p);
            }
        }
        return newParams;
    }

    /**
     * Filter based on Scope.  Ignore unrecognized parameters.
     */
    public ParameterMap filter(ParameterState.Scope scope) {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);

        for (Parameter p : getSortedParameters().values()) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if (pstate != null && pstate.getScope() == scope) {
                newParams.put(p);
            }
        }
        return newParams;
    }

    /**
     * Filter based on Info
     * @param includeParamWithInfo if true, include params that have "info"
     * attribute. If false, include params that don't have "info" attribute.
     */
    public ParameterMap filter(ParameterState.Info info,
                               boolean includeParamWithInfo) {
        ParameterMap newParams =
                new ParameterMap(name, type, validate, version);

        for (Parameter p : getSortedParameters().values()) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if (pstate != null) {
                if ((includeParamWithInfo && pstate.appliesTo(info)) ||
                    (!includeParamWithInfo && !pstate.appliesTo(info))) {
                     newParams.put(p);
                }
            }
        }
        return newParams;
    }

    public Parameter put(Parameter value) {
        return getSortedParameters().put(value.getName(), value);
    }

    public Parameter get(String pname) {
        final Parameter p = getSortedParameters().get(pname);
        return (p != null) ? p : NULL_PARAMETER;
    }

    /**
     * Returns the parameter value if it exists, otherwise returns its default
     * value as specified in ParameterState.
     */
    public Parameter getOrDefault(String pname) {

        if (exists(pname)) {
            return get(pname);
        }
        return DefaultParameter.getDefaultParameter(pname);
    }

    public Parameter remove(String pname) {
        return getSortedParameters().remove(pname);
    }

    /**
     * These methods allow a caller to not worry about checking null
     */
    public int getOrZeroInt(String pname) {
        final Parameter p = getSortedParameters().get(pname);
        return (p != null) ? p.asInt() : 0;
    }

    public long getOrZeroLong(String pname) {
        final Parameter p = getSortedParameters().get(pname);
        return (p != null) ? p.asLong() : 0L;
    }

    public boolean exists(String pname) {
        final Parameter p = getSortedParameters().get(pname);
        return (p != null);
    }

    public void clear() {
        getSortedParameters().clear();
    }

    public boolean equals(ParameterMap other) {
        if (size() != other.size()) {
            return false;
        }
        for (Parameter p : getSortedParameters().values()) {
            if (!p.equals(other.get(p.getName()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Merge other's values into this map without checking for entries that
     * have been removed in the specified map relative to this map.
     *
     * @param other the parameters to merge
     * @param notReadOnly do not merge read-only parameters
     * @return the number of values merged.
     */
    public int merge(ParameterMap other, boolean notReadOnly) {
        return merge(other, notReadOnly, false /* checkRemoved */);
    }

    /*
     * TODO: Remove this next overloading and have the checkRemoved=true
     * behavior enabled always. ([#27527])
     */

    /**
     * Merge other's values into this map, checking for entries that have been
     * removed in the specified map relative to this map if requested.
     *
     * @param other the parameters to merge
     * @param notReadOnly do not merge read-only parameters
     * @param checkRemoved whether to check for removed entries
     * @return the number of values merged.
     */
    public int merge(ParameterMap other,
                     boolean notReadOnly,
                     boolean checkRemoved) {
        int numMerged = 0;
        for (Parameter p : other.getSortedParameters().values()) {
            if (!get(p.getName()).equals(p)) {
                if (notReadOnly) {
                    ParameterState pstate =
                        ParameterState.lookup(p.getName());
                    if (pstate.getReadOnly()) {
                        continue;
                    }
                }
                put(p);
                ++numMerged;
            }
        }
        if (checkRemoved) {
            final Iterator<Parameter> iter =
                getSortedParameters().values().iterator();
            while (iter.hasNext()) {
                final Parameter p = iter.next();
                if (other.exists(p.getName())) {
                    continue;
                }
                if (notReadOnly) {
                    final ParameterState pstate =
                        ParameterState.lookup(p.getName());
                    if (pstate.getReadOnly()) {
                        continue;
                    }
                }
                iter.remove();
                ++numMerged;
            }
        }
        return numMerged;
    }

    public boolean hasRestartRequiredDiff(ParameterMap other) {

        for (Parameter p : getSortedParameters().values()) {
            if (!p.equals(other.get(p.getName()))) {
                if (p.getName().equals(ParameterState.JE_MISC)) {
                    if (ParameterUtils.paramsRequireJEReboot(
                            other.get(ParameterState.JE_MISC).asString(), 
                            p.asString())) {
                        return true;
                    }
                }  else if (p.restartRequired()) {
                    return true;
                }
            }
        }
        return false;
    }

    /** One or more parameters in this map is a restart required param. */
    public boolean hasRestartRequired() {
        for (Parameter p : getSortedParameters().values()) {
           if (p.restartRequired()) {
               return true;
            }
        }
        return false;
    }

    /**
     * Return a map that has the values from "other" that are different
     * from "this"
     */
    public ParameterMap diff(ParameterMap other, boolean notReadOnly) {
        ParameterMap map = new ParameterMap();
        for (Parameter p : other.getSortedParameters().values()) {
            if (!get(p.getName()).equals(p)) {
                if (notReadOnly) {
                    ParameterState pstate =
                        ParameterState.lookup(p.getName());
                    if (pstate.getReadOnly()) {
                        continue;
                    }
                }
                map.put(p);
            }
        }
        return map;
    }

    /**
     * Return a map of parameters that are present in this map but not in the
     * supplied map. These are the parameters that will be removed if {@link
     * #merge} is called with checkRemoved=true.
     */
    public ParameterMap missingIn(ParameterMap other, boolean notReadOnly) {
        final ParameterMap map = new ParameterMap();
        for (Parameter p : sortedParameters.values()) {
            if (other.exists(p.getName())) {
                continue;
            }
            if (notReadOnly) {
                final ParameterState pstate =
                    ParameterState.lookup(p.getName());
                if (pstate.getReadOnly()) {
                    continue;
                }
            }
            map.put(p);
        }
        return map;
    }

    /**
     * Writes formated parameters to the specified writer in sorted order.
     * The parameters are written in sorted order by the parameter name.
     */
    public void write(PrintWriter writer) {
        writer.printf("%s<component name=\"%s\" type=\"%s\" validate=\"%s\">\n",
                      INDENT, name, type, Boolean.toString(validate));
        for (Parameter p : getSortedParameters().values()) {
            writer.printf
                ("%s<property name=\"%s\" value=\"%s\" type=\"%s\"/>\n",
                 PROPINDENT, p.getName(), p.asString(),
                 p.getType().toString());
        }
        writer.printf("%s</component>\n", INDENT);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        return toString(sb).toString();
    }

    StringBuilder toString(StringBuilder sb) {
        if (name != null) {
            sb.append("name=").append(name);
        }
        if (type != null) {
            sb.append(" type=").append(type);
        }
        sb.append(" ").append(getSortedParameters().toString());
        return sb;
    }

    /**
     * Returns a String representation of the parameters in sorted order.
     * Each parameter is separated by System.getProperty("line.separator").
     * The parameters appear in sorted order by the parameter name.
     *
     * @return a String representation of the parameters in sorted order
     */
    public String showContents() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Parameter p : getSortedParameters().values()) {
            if (first) {
                first = false;
            } else {
                sb.append(eol);
            }
            sb.append(p.getName()).append("=").append(p.asString());
        }
        return sb.toString();
    }

    /**
     * Set or create parameter in map.  If the value is null this means remove
     * the parameter.  If the map is set to not validate treat all parameters
     * as STRING.
     */
    public boolean setParameter(String name,
                                String value) {
        return setParameter(name, value, null, false);
    }

    /**
     * Adds or removes the specified parameter from the map. If value is null
     * the action is to remove the named parameter. Otherwise the parameter
     * is added. If type is null, and the map is not set to validate the added
     * parameter will be a STRING type. If ignoreUnknown is true and the map
     * is set to validate, unknown parameters are ignored. This allows the
     * system to silently ignore unrecognized (probably removed) parameters.
     *
     * @param name the parameter name
     * @param value the value or null
     * @param type the parameter type or null
     * @param ignoreUnknown if true ignore unknown parameters.
     * @return true if the operation was successful
     */
    public boolean setParameter(String name,
                                String value,
                                String type,
                                boolean ignoreUnknown) {
        if (value == null) {
            return ((remove(name)) != null);
        }
        final Parameter p;
        if (validate) {
            p = Parameter.createParameter(name, value, ignoreUnknown);
        } else if (type == null) {
            p = Parameter.createParameter(name, value,
                                          ParameterState.Type.STRING);
        } else {
            p = Parameter.createKnownType(name, value, type);
        }
        if (p != null) {
            put(p);
            return true;
        }
        return false;
    }

    /**
     * Create a ParameterMap of default Parameter objects for all POLICY
     * parameters.
     */
    public static ParameterMap createDefaultPolicyMap() {
        ParameterMap map = new ParameterMap();
        for (ParameterState ps : ParameterState.pstate.values()) {
            if (ps.getPolicyState()) {
                map.put(DefaultParameter.getDefaultParameter(ps));
            }
        }
        return map;
    }

    /**
     * Create a ParameterMap of default Parameter objects for all SECURITY
     * POLICY parameters.
     */
    public static ParameterMap createDefaultSecurityPolicyMap() {
        ParameterMap map = new ParameterMap();
        EnumSet<ParameterState.Info> set = EnumSet.of(
            ParameterState.Info.POLICY, ParameterState.Info.SECURITY);
        for (ParameterState ps : ParameterState.pstate.values()) {
            if (ps.containsAll(set)) {
                map.put(DefaultParameter.getDefaultParameter(ps));
            }
        }
        return map;
    }

    /**
     * Add default Parameter objects for parameters that are associated with
     * the service and are POLICY parameters.  These are the only parameters
     * that are suitable for defaulting.  Do not overwrite unless overwrite is
     * true
     */
    public void addServicePolicyDefaults(ParameterState.Info service) {
        EnumSet<ParameterState.Info> set =
            EnumSet.of(ParameterState.Info.POLICY,service);
        for (ParameterState ps : ParameterState.getMap().values()) {
            if (ps.containsAll(set)) {
                final String defaultName =
                    DefaultParameter.getDefaultParameter(ps).getName();
                /*
                 * Do not overwrite existing keys
                 */
                if (!exists(defaultName)) {
                    put(DefaultParameter.getDefaultParameter(ps));
                }
            }
        }
    }

    /**
     * Gets the parameters sorted by parameter name.
     * When this method returns sortedParameters will contain the sorted
     * parameter set and parameters will be null.
     */
    private SortedMap<String, Parameter> getSortedParameters() {

        if (sortedParameters == null) {
            sortedParameters = (parameters == null) ?
                    new TreeMap<String, Parameter>() :
                    new TreeMap<String, Parameter>(parameters);
            parameters = null;
        }
        return sortedParameters;
    }

    private void writeObject(ObjectOutputStream out)
            throws IOException {

        if (parameters == null) {
            parameters = new HashMap<String, Parameter>(getSortedParameters());
        }
        out.defaultWriteObject();

        /* Only keep one copy */
        if (sortedParameters != null) {
            parameters = null;
        }
    }

    /**
     * NullParameter exists to avoid extra checks for null objects.  It
     * should only ever happen for StringParameter.  It'll throw otherwise.
     */
    private static class NullParameter extends Parameter {
    	private static final long serialVersionUID = 1L;

        public NullParameter() {
            super("noname");
        }

        @Override
        public String asString() {
            return null;
        }

        @Override
        public ParameterState.Type getType() {
            return ParameterState.Type.NONE;
        }
    }


    /**
     * Duplicates the dns cache ttl values.
     *
     * Copies the values of COMMON_DNS_CACHE_TTL and
     * COMMON_DNS_CACHE_NEGATIVE_TTL from {@code srcMap} into the parameters of
     * COMMON_DNS_CACHE_TTL_VALUE and COMMON_DNS_CACHE_NEGATIVE_TTL_VALUE of
     * this map.
     */
    public void duplicateCacheTTLValues(ParameterMap srcMap) {
        duplicateCacheTTLValue(
            srcMap,
            ParameterState.COMMON_DNS_CACHE_TTL,
            ParameterState.COMMON_DNS_CACHE_TTL_VALUE);
        duplicateCacheTTLValue(
            srcMap,
            ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL,
            ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL_VALUE);
    }

    /**
     * Duplicates a value of {@code srcName} from {@code srcMap} to {@code
     * dstName} of this map.
     *
     * Note that if the default value equals to the source value, we still set
     * the parameter, since it is a explicit set to the value and default
     * values may be inconsistent due to bugs or code upgrade.
     */
    private void duplicateCacheTTLValue(ParameterMap srcMap,
                                        String srcName,
                                        String dstName) {
        final Parameter srcParam = srcMap.get(srcName);
        if (srcParam == NULL_PARAMETER) {
            return;
        }
        setParameter(dstName, srcParam.asString());
    }
}
