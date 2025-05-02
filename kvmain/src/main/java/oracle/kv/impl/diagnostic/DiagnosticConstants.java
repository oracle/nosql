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

package oracle.kv.impl.diagnostic;

/**
 * This class contains the constants used in diagnostics utility.
 *
 */
public class DiagnosticConstants {

    static final String DEFAULT_WORK_DIR = System.getProperty("user.dir");
    static final String CONFIG_FILE_NAME = "sn-target-list";

    static final String HOST_FLAG = "-host";
    static final String PORT_FLAG = "-port";
    static final String SSH_USER_FLAG = "-sshusername";
    static final String USER_FLAG = "-username";
    static final String SECURITY_FLAG = "-security";
    static final String CONFIG_DIRECTORY_FLAG = "-configdir";
    static final String STORE_FLAG = "-store";
    static final String SN_FLAG = "-sn";
    static final String ROOT_DIR_FLAG = "-rootdir";
    static final String SAVE_DIRECTORY_FLAG = "-savedir";
    static final String NO_COMPRESS_FLAG = "-nocompress";

    static final String NEW_LINE_TAB = "\n\t";
    static final String EMPTY_STRING = "";
    static final String NOT_FOUND_ROOT_MESSAGE =
            "Cannot find root directory: ";
}
