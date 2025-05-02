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

import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.util.ErrorMessage;

/**
 * Subclass of InternalFaultException used to indicate that the fault
 * originated in the Admin service when satisfying an internal request. Also,
 * it helps to generate a result for admin CLI command execution in the case
 * of any exception happens.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class AdminFaultException extends InternalFaultException {

    private static final long serialVersionUID = 1L;

    private static final ErrorMessage DEFAULT_ERR_MSG =
        ErrorMessage.NOSQL_5100;

    private /* final */ CommandFails cmdResult;

    public AdminFaultException(Throwable cause) {
        /* uses default command result with error code of 5100 */
        // TODO: should default cmd result be null?
        this(cause, new CommandFails(cause.getMessage(),
                                     DEFAULT_ERR_MSG,
                                     CommandResult.NO_CLEANUP_JOBS));
    }

    public AdminFaultException(Throwable cause,
                               String description,
                               ErrorMessage errorMsg,
                               String[] cleanupJobs) {

        this(cause, new CommandFails(description, errorMsg, cleanupJobs));
    }

    /**
     * Constructs an AdminFaultException with the specified cause as well as
     * the command result describing the cause.
     */
    private AdminFaultException(Throwable cause, CommandFails cmdResult) {
        super(cause);
        this.cmdResult = cmdResult;
    }

    /**
     * Creates an instance from the input stream.
     */
    public AdminFaultException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        cmdResult =
            in.readBoolean() ? new CommandFails(in, serialVersion) : null;
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link InternalFaultException}) {@code super}
     * <li> ({@linkplain SerializationUtil#writeFastExternalOrNull
     *      <code>CommandFails</code> or <code>null</code>}) {@link
     *      #getCommandResult cmdResult}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeFastExternalOrNull(out, serialVersion, cmdResult);
    }

    /**
     * Returns the command result describing the cause
     */
    public CommandResult getCommandResult() {
        return cmdResult;
    }

    private void readObject(java.io.ObjectInputStream in)
        throws ClassNotFoundException, IOException {
        in.defaultReadObject();

        if (cmdResult == null) {
            cmdResult = new CommandFails(getMessage(),
                                         DEFAULT_ERR_MSG,
                                         CommandResult.NO_CLEANUP_JOBS);
        }
    }

    /**
     * Wraps a CommandFaultException to an AdminFaultException. If the
     * CommandFaultException has a wrapped cause in it, the cause will be
     * extracted and wrapped to the AdminFaultException so as that old version
     * callers are able to recognize the cause correctly. If the
     * CommandFaultException is an IllegalCommandException or has no cause, it
     * will be wrapped directly.
     * <p>
     * A command result will be generated and set to the new
     * AdminFaultException based on the information included in the
     * CommandFaultException.
     *
     * @param cfe the CommandFaultException to wrap
     * @return an AdminFaultException
     */
    public static AdminFaultException
        wrapCommandFault(CommandFaultException cfe) {

        final CommandFails cmdResult = new CommandFails(cfe);
        if (cfe instanceof IllegalCommandException ||
            cfe.getCause() == null) {
            return new AdminFaultException(cfe, cmdResult);
        }
        return new AdminFaultException(cfe.getCause(), cmdResult);
    }
}
