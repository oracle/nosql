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

package oracle.kv.impl.mgmt;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.util.CommandParser;

public final class MgmtUtil {

    /* Command options, for "java -jar" usage. */
    public static final String MGMT_FLAG = "-mgmt";
    public static final String MGMT_NOOP_ARG = "none";
    public static final String MGMT_JMX_ARG = "jmx";

    public static final String MGMT_NOOP_IMPL_CLASS =
        "oracle.kv.impl.mgmt.NoOpAgent";
    public static final String MGMT_JMX_IMPL_CLASS =
        "oracle.kv.impl.mgmt.jmx.JmxAgent";

    private static final Map<String, String> mgmtArg2ClassNameMap;
    static {
        mgmtArg2ClassNameMap = new HashMap<String, String>();
        mgmtArg2ClassNameMap.put(MGMT_NOOP_ARG, MGMT_NOOP_IMPL_CLASS);
        mgmtArg2ClassNameMap.put(MGMT_JMX_ARG, MGMT_JMX_IMPL_CLASS);
    }

    /* This class should not be instantiated. */
    private MgmtUtil() {
    }

    public static String getMgmtUsage() {

        final StringBuilder usage = new StringBuilder("[");
        usage.append(MGMT_FLAG);
        usage.append(" {");
        String delimiter = "";
        for (String k : mgmtArg2ClassNameMap.keySet()) {
            usage.append(delimiter);
            usage.append(k);
            delimiter = "|";
        }
        usage.append("}]");
        return usage.toString();
    }

    /**
     * Return the implementation class name for a given -mgmt flag argument.
     * Caller must check for a null return value.
     */
    public static String getImplClassName(String arg) {
        return mgmtArg2ClassNameMap.get(arg);
    }

    /**
     * Indicate whether the given implementation class name is one that we
     * approve of.
     */
    public static boolean verifyImplClassName(String arg) {
        return mgmtArg2ClassNameMap.values().contains(arg);
    }

    public static class ConfigParserHelper {
        private final CommandParser parser;

        private String mgmtImpl = null;

        public ConfigParserHelper(CommandParser parser) {
            this.parser = parser;
        }

        public boolean checkArg(String arg) {
            if (arg.equals(MgmtUtil.MGMT_FLAG)) {
                final String next = parser.nextArg(arg);
                mgmtImpl = MgmtUtil.getImplClassName(next);
                if (mgmtImpl == null) {
                    parser.usage
                        ("There is no mgmt implementation named " + next);
                }
                return true;
            }
            return false;
        }

        public void apply(BootstrapParams bp) {

            if (mgmtImpl != null) {
                bp.setMgmtClass(mgmtImpl);
            }
        }

        public String getSelectedImplClass() {
            return mgmtImpl;
        }
    }
}
