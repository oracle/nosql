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

package oracle.kv.impl.admin;

import static oracle.kv.impl.util.ObjectUtil.safeEquals;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeArrayLength;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.util.ErrorMessage;

/**
 * CommandResult describes the information about a command execution result,
 * which contains a return value, a description of the result, an error code,
 * and cleanup jobs used to remove issues causing execution failure.
 * <p>
 * Note that we need to avoid directly persisting the enum type of ErrorMessage
 * in a CommandResult instance. That is because when being deserialized in
 * old version code, any newly added ErrorMessage value will cause
 * compatibility issue since it cannot be recognized. Persisting the int type
 * error code of the corresponding ErrorMessage can help solve this problem.
 */
public interface CommandResult {

    public static final String[] NO_CLEANUP_JOBS = {};
    public static final String[] STORE_CLEANUP = {"store_clean.kvs"};
    public static final String[] TOPO_REPAIR = {"plan repair-topology"};
    public static final String[] PLAN_CANCEL = {"plan_cancel_retry.kvs"};
    public static final String[] TOPO_PLAN_REPAIR =
        {"plan repair-topology", "plan_cancel_retry.kvs"};

    /**
     * Returned value of command execution, may be null if the command fails
     */
    public String getReturnValue();

    /**
     * Returns a detailed description of this execution
     */
    public String getDescription();

    /**
     * Returns the standard NoSQL error code
     */
    public int getErrorCode();

    /**
     * Returns the suggested cleanup jobs.  The cleanup jobs are in the form
     * of an array of executable commands. If no cleanup job is available, or
     * the command ends successfully, null will be returned.
     */
    public String[] getCleanupJobs();


    /**
     * Provides information about a command failure.  Note that only this
     * subclass of CommandResult implements FastExternalizable because that
     * capability is only needed when an instance is included in an exception,
     * and only the CommandFails subclass is used that way.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public static class CommandFails
            implements CommandResult, FastExternalizable, Serializable {

        private static final long serialVersionUID = 1L;

        private final String description;
        private final int errorCode;
        private final String[] cleanupJobs;
        private final String returnValue;

        public CommandFails(String description,
                            ErrorMessage errorMsg,
                            String[] cleanupJobs,
                            String returnValue) {
            this.description = description;
            this.errorCode = errorMsg.getValue();
            this.cleanupJobs = cleanupJobs;
            this.returnValue = returnValue;
        }

        public CommandFails(String description,
                            ErrorMessage errorMsg,
                            String[] cleanupJobs) {
            this(description, errorMsg, cleanupJobs, null);
        }

        public CommandFails(CommandFaultException cfe) {
            this(cfe.getDescription(), cfe.getErrorMessage(),
                 cfe.getCleanupJobs(), null);
        }

        /**
         * Creates an instance from the input stream.
         */
        public CommandFails(DataInput in, short serialVersion)
            throws IOException {

            description = readString(in, serialVersion);
            errorCode = in.readInt();
            final int cleanupJobsCount = readSequenceLength(in);
            if (cleanupJobsCount == -1) {
                cleanupJobs = null;
            } else {
                cleanupJobs = new String[cleanupJobsCount];
                for (int i = 0; i < cleanupJobsCount; i++) {
                    cleanupJobs[i] = readString(in, serialVersion);
                }
            }
            returnValue = readString(in, serialVersion);
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link SerializationUtil#writeString String}) {@link
         *      #getDescription description}
         * <li> ({@link DataOutput#writeInt int}) {@link #getErrorCode
         *      errorCode}
         * <li> ({@link SerializationUtil#writeArrayLength sequence length})
         *      <i>cleanup jobs count</i>
         * <li> For each cleanup job:
         *   <ol type="a">
         *   <li> ({@link SerializationUtil#writeString String}) <i>cleanup job
         *        name</i>
         *   </ol>
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out,
                                      short serialVersion)
            throws IOException {
            writeString(out, serialVersion, description);
            out.writeInt(errorCode);
            writeArrayLength(out, cleanupJobs);
            if (cleanupJobs != null) {
                for (final String e : cleanupJobs) {
                    writeString(out, serialVersion, e);
                }
            }
            writeString(out, serialVersion, returnValue);
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public int getErrorCode() {
            return errorCode;
        }

        @Override
        public String[] getCleanupJobs() {
            return cleanupJobs;
        }

        @Override
        public String getReturnValue() {
            return returnValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CommandFails)) {
                return false;
            }
            final CommandFails other = (CommandFails) o;
            return safeEquals(description, other.getDescription()) &&
                errorCode == other.getErrorCode() &&
                Arrays.equals(cleanupJobs, other.getCleanupJobs());
        }

        @Override
        public int hashCode() {
            int value = 37;
            value += (description == null) ? 17 : description.hashCode();
            value = (value * 19) + errorCode;
            return value;
        }
    }

    public static class CommandSucceeds
        implements CommandResult, Serializable {

        private static final long serialVersionUID = 1L;

        private final static String SUCCESS_MSG =
            "Operation ends successfully";
        private final String returnValue;

        public CommandSucceeds(String returnValue) {
            this.returnValue = returnValue;
        }

        @Override
        public String getReturnValue() {
            return returnValue;
        }

        @Override
        public String getDescription() {
            return SUCCESS_MSG;
        }

        @Override
        public int getErrorCode() {
            return ErrorMessage.NOSQL_5000.getValue();
        }

        @Override
        public String[] getCleanupJobs() {
            return null;
        }
    }

    public static class CommandWarns
        implements CommandResult, Serializable {

        private static final long serialVersionUID = 1L;

        private final String description;
        private final String returnValue;

        public CommandWarns(String description, String returnValue) {
            this.description = description;
            this.returnValue = returnValue;
        }

        @Override
        public String getReturnValue() {
            return returnValue;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public int getErrorCode() {
            return ErrorMessage.NOSQL_5000.getValue();
        }

        @Override
        public String[] getCleanupJobs() {
            return null;
        }
    }
}
