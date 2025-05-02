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

package oracle.kv.impl.fault;

/**
 * The enumeration of process exit codes used to communicate between a process
 * and its handler, e.g. the SNA, some shell script, etc.
 * <p>
 * Process exit codes must be in the range [0-255] and must not be one of the
 * following: 1-2, 64-113 (C/C++ standard), 126-128, 129-192 (signals 1-64),
 * and 255 since they are reserved and have special meaning.
 */
public enum ProcessExitCode {
    RESTART(200, true /* restart */),
    NO_RESTART(201, false /* restart */),
    /**
     * It's a variant of RESTART indicating that the process needs to be
     * started due to an OOME. It's a distinct OOME so that the SNA can log
     * this fact because the managed service can't.
     */
    RESTART_OOME(202, true /* restart */),
    /** An injected failure that requests a restart */
    INJECTED_FAULT_RESTART(203, true /* restart */, true /* fault */),
    /** An injected failure that requests no restart */
    INJECTED_FAULT_NO_RESTART(204, false /* restart */, true /* fault */);

    private static final ProcessExitCode[] VALUES = values();
    private final int value;
    private final boolean needsRestart;
    private final boolean injectedFault;

    private ProcessExitCode(int value, boolean needsRestart) {
        this(value, needsRestart, false /* injectedFault */);
    }

    private ProcessExitCode(int value,
                            boolean needsRestart,
                            boolean injectedFault) {
        this.value = value;
        this.needsRestart = needsRestart;
        this.injectedFault = injectedFault;
    }

    /**
     * Returns the numeric value associated with the process exit code.
     */
    public int getValue() {
        return value;
    }

    public boolean needsRestart() {
        return needsRestart;
    }

    /**
     * Returns whether the exit code means that the process exited because of
     * an injected fault.
     */
    public boolean injectedFault() {
        return injectedFault;
    }

    /** Returns the constant for the specified exit code, or null. */
    public static ProcessExitCode getProcessExitCode(int exitCode) {
        for (ProcessExitCode v : VALUES) {
            if (exitCode == v.getValue()) {
                return v;
            }
        }
        return null;
    }

    /**
     * Returns true if the process exit code indicates that the process needs
     * to be restarted, or if the exit code is not one of the know exit codes
     * from the above enumeration.
     */
    public static boolean needsRestart(int exitCode) {
        final ProcessExitCode v = getProcessExitCode(exitCode);
        if (v != null) {
            return v.needsRestart();
        }

        /*
         * Some unknown exitCode. Opt for availability, restart the process.
         * If its a recurring error, the SNA will eventually shut down the
         * managed service.
         */
        return true;
    }

    /**
     * Returns whether the process exit code means the process exited because
     * of an injected fault.
     */
    public static boolean injectedFault(int exitCode) {
        final ProcessExitCode v = getProcessExitCode(exitCode);
        return (v != null) ? v.injectedFault() : false;
    }

    /** Returns a description of the exit code. */
    public static String description(int exitCode) {
        if (exitCode == 0) {
            return "0/normal";
        }
        if ((129 <= exitCode) && (exitCode < (129 + 64))) {
            return exitCode + "/kill-" + (exitCode - 128);
        }
        final ProcessExitCode v = getProcessExitCode(exitCode);
        if (v != null) {
            return exitCode + "/" + v;
        }
        return exitCode + "/unknown";
    }

    /**
     * Returns the process exit code to use when the specified exception is
     * thrown and the process is not being restarted, returning
     * INJECTED_FAULT_NO_RESTART if the exception implements InjectedFault and
     * otherwise NO_RESTART.
     */
    public static ProcessExitCode getNoRestart(Throwable e) {
        return (e instanceof InjectedFault) ?
            INJECTED_FAULT_NO_RESTART :
            NO_RESTART;
    }

    /**
     * Returns the process exit code to use when the specified exception is
     * thrown and the process is being restarted, returning
     * INJECTED_FAULT_RESTART if the exception implements InjectedFault and
     * otherwise RESTART.
     */
    public static ProcessExitCode getRestart(Throwable e) {
        return (e instanceof InjectedFault) ?
            INJECTED_FAULT_RESTART :
            RESTART;
    }
}
