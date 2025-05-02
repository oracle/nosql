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
package oracle.kv.util.shell;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class encapsulates the method to read key-value pairs in a section of
 * configuration file, it is used for a shell to read the start-up parameters
 * from a file.
 *
 * All the arguments of a shell can be configured in the rc file in "key=value"
 * format, below lists arguments for sql shell:
 *
 * {@literal
 * [kvsqlcli]
 * helper-hosts=<host:port[,host:port]*>
 * store=<storeName>
 * timeout=<timeout ms>
 * consistency=<NONE_REQUIRED(default) | ABSOLUTE | NONE_REQUIRED_NO_MASTER>
 * durability=<COMMIT_SYNC(default) | COMMIT_NO_SYNC | COMMIT_WRITE_NO_SYNC>
 * username=<user>
 * security=<security-file-path>
 *
 * [kvcli]
 * host=<hostname>
 * port=<port>
 * store=<storeName>
 * username=<user>
 * security=<security-file-path>
 * admin-username=<adminUser>
 * admin-security=<admin-security-file-path>
 * timeout=<timeout ms>
 * consistency=<NONE_REQUIRED(default) | ABSOLUTE | NONE_REQUIRED_NO_MASTER>
 * durability=<COMMIT_SYNC(default) | COMMIT_NO_SYNC | COMMIT_WRITE_NO_SYNC>
 * }
 */
public class ShellRCFile {

    private final static String RCFILE = ".kvclirc";

    /*
     * Returns a array of string that contains key/value pairs in the specified
     * section, the return value of a key/value pair is [-key, value].
     *
     * e.g. section "kvsqlcli" in .kvclirc contains below parameters.
     * [kvsqlcli]
     * helper-hosts=localhost:5000
     * store=kvstore
     * timeout=10000
     *
     * Then return string array is like below:
     *  [-helper-hosts, localhost:5000, -store, kvstore, -timeout, 10000]
     */
    public static String[] readSection(final String section) {
        final String rcFile = System.getProperty("user.home") + "/" + RCFILE;
        final List<String> args = new ArrayList<String>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(rcFile));
            final String sectionName = "[" + section + "]";
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains(sectionName)) {
                    break;
                }
            }
            while ((line = br.readLine()) != null) {
                if (line.startsWith("[")) {
                    break;
                }
                String params[] = line.split("=");
                if (params.length == 2) {
                    args.add("-" + params[0]);
                    args.add(params[1]);
                }
            }
        } catch (FileNotFoundException ignored) {
            return null;
        } catch (IOException ignored) {
            return null;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ignored) {
                }
            }
        }
        return args.toArray(new String[args.size()]);
    }
}
