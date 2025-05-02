/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util.fault;

/**
 * The enumeration of process exit codes.
 * <p>
 * Process exit codes must be in the range [0-255] and must not be one of the
 * following: 1-2, 64-113 (C/C++ standard), 126 - 165, and 255 since they are
 * reserved and have special meaning.
 */
public enum ProcessExitCode {
    RESTART() {

        @Override
        public short getValue() {
            return 200;
        }

        @Override
        public boolean needsRestart() {
            return true;
        }
    },

    NO_RESTART{

        @Override
        public short getValue() {
            return 201;
        }

        @Override
        public boolean needsRestart() {
            return false;
        }
    };

    /**
     * Returns the numeric value associated with the process exit code.
     */
    public abstract short getValue();

    public abstract boolean needsRestart();

    /**
     * Returns true if the process exit code indicates that the process needs
     * to be restarted, or if the exit code is not one of the known exit codes
     * from the above enumeration.
     */
    static public boolean needsRestart(int exitCode) {
        for (ProcessExitCode v : ProcessExitCode.values()) {
            if (exitCode == v.getValue()) {
                return v.needsRestart();
            }
        }

        /*
         * Some unknown exitCode. Opt for availability, restart the process.
         */
        return true;
    }
}
