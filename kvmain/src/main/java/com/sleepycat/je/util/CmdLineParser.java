/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CmdLineParser {
    private final Map<String, String> argumentMap = new HashMap<>();
    private final Set<String> flags = new HashSet<>();
    private final List<String> reqOpts = new ArrayList<>();
    private final List<String> posArgs = new ArrayList<>();
    private final List<String> descriptions = new ArrayList<>();

    public void addFlag(String flag, String desc) {
        flags.add(flag);
        descriptions.add(flag + "\t" + desc);
    }

    public void addOption(String option, boolean isRequired, String desc) {
        argumentMap.put(option, null);
        descriptions.add(
                         option + (isRequired ? " (required)" : "") + "\t"
                             + desc);
        if (isRequired) {
            reqOpts.add(option);
        }
    }

    public boolean parse(String[] args) {
        try {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (flags.contains(arg)) {
                    argumentMap.put(arg, "true");
                } else if (argumentMap.containsKey(arg)) {
                    if (i + 1 < args.length) {
                        argumentMap.put(arg, args[++i]);
                    } else {
                        throw new IllegalArgumentException(
                            "Missing value for option: " + arg);
                    }
                } else {
                    posArgs.add(arg);
                }
            }
            validateReqOpts();
            return true;
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            printUsage();
            return false;
        }
    }

    private void validateReqOpts() {
        for (String opt : reqOpts) {
            if (argumentMap.get(opt) == null) {
                throw new IllegalArgumentException(
                    "Missing value for option: " + opt);
            }
        }
    }

    public void printUsage() {
        System.out.println("Usage: java" + getClass().getName()
            + "[options] [posArgs]");
        System.out.println("Options:");
        for (String desc : descriptions) {
            System.out.println(" " + desc);
        }
    }

    public String getOptionValue(String option) {
        return argumentMap.get(option);
    }

    public boolean isFlagSet(String flag) {
        return "true".equals(argumentMap.get(flag));
    }

    public List<String> getPosArgs() {
        return posArgs;
    }
}
