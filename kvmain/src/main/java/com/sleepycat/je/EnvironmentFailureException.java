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

package com.sleepycat.je;

import java.util.function.Supplier;

import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.utilint.FormatUtil;

/**
 * Indicates that a failure has occurred that could impact the {@code
 * Environment} as a whole.  For failures that impact only the current
 * operation and/or transaction, see {@link OperationFailureException}).  For
 * an overview of all exceptions thrown by JE, see {@link DatabaseException}.
 *
 * <p>Depending on the nature of the failure, this exception may indicate that
 * {@link Environment#close} must be called. The application should catch
 * {@code EnvironmentFailureException} and then call {@link
 * Environment#isValid}.  If {@code false} is returned, all {@code Environment}
 * handles (instances) must be closed and re-opened in order to run recovery
 * and continue operating.  If {@code true} is returned, the {@code
 * Environment} can continue operating without being closed and re-opened.
 * Also note that {@link Environment#isValid} may be called at any time, not
 * just during exception handling.</p>
 *
 * <p>The use of the {@link Environment#isValid} method allows JE to determine
 * dynamically whether the failure requires recovery or not, and allows for
 * this determination to change in future releases.  Over time, internal
 * improvements to error handling may allow more error conditions to be handled
 * without invalidating the {@code Environment}.</p>
 *
 * <p>(Although this exception class extends {@link RunRecoveryException}, it
 * does not always indicate that recovery is necessary, as described above.
 * {@code RunRecoveryException} has been deprecated and {@code
 * EnvironmentFailureException} should be used instead.)</p>
 *
 * <p>If an {@code EnvironmentFailureException} consistently occurs soon after
 * opening the Environment, this may indicate a persistent problem.  It may
 * indicate a system problem or a persistent storage problem.  In this case,
 * human intervention is normally required and restoring from a backup may be
 * necessary.</p>
 *
 * <p>Note that subclasses of {@code EnvironmentFailureException} indicate how
 * to handle the exception in more specific ways.</p>
 * <ul>
 * <li>If {@code Thread.interrupt} is called for a thread performing JE
 * operations, a {@link ThreadInterruptedException} is thrown.  Since
 * interrupting a thread is intentional, it does not indicate a persistent
 * problem and human intervention is not normally required.
 * </li>
 * <li>If an {@code IOException} occurs while writing to the JE log, a
 * {@link LogWriteException} is thrown.  Although an {@code IOException} can
 * occur for different reasons, it is a hint that the disk may be full and
 * applications may wish to attempt recovery after making more disk space
 * available.
 * </li>
 * <li>For replicated environments, see the subclasses of {@code
 * EnvironmentFailureException} in the {@link com.sleepycat.je.rep} package for
 * more information.  Such exceptions may require special handling.
 * </li>
 * </ul>
 *
 * <p>If {@link Environment#close} is not called after an {@code
 * EnvironmentFailureException} invalidates the {@code Environment}, all
 * subsequent method calls for the {@code Environment} will throw the same
 * exception.  This provides more than one opportunity to catch and handle the
 * specific exception subclass that caused the failure.</p>
 */
@SuppressWarnings("deprecation")
public class EnvironmentFailureException extends RunRecoveryException {

    /*
     * Classes that extend EnvironmentFailureException should be aware that
     * their constructors should not be seen as atomic. If the failure reason
     * mandates it, the environment may be invalidated. At invalidation time,
     * the exception is saved within the environment as the precipitating
     * failure, and may be seen and used by other threads, and the sub class
     * instance may be seen before construction is complete. The subclass
     * should take care if it has any fields that are initialized in the
     * constructor, after the call to super().
     *
     * Any overloadings of getMessage() should also assume that they may be
     * called asynchronously before the subclass if fully initialized.
     */

    private static final long serialVersionUID = 1;

    private volatile String javaErrorMsg;
    private EnvironmentFailureReason reason;

    /**
     * Only for use by makeJavaErrorException.
     */
    private EnvironmentFailureException(String msg) {
        super(msg);
        this.reason = EnvironmentFailureReason.JAVA_ERROR;
    }

    /**
     * Only for use by unexpectedState and unexpectedException.
     */
    private EnvironmentFailureException(EnvironmentFailureReason reason,
                                        String message,
                                        Throwable cause) {
        this(null /*envImpl*/, reason, message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public EnvironmentFailureException(EnvironmentImpl envImpl,
                                       EnvironmentFailureReason reason) {
        this(envImpl, reason, null /*message*/, null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public EnvironmentFailureException(EnvironmentImpl envImpl,
                                       EnvironmentFailureReason reason,
                                       Throwable cause) {
        this(envImpl, reason, null /*message*/, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public EnvironmentFailureException(EnvironmentImpl envImpl,
                                       EnvironmentFailureReason reason,
                                       String message) {
        this(envImpl, reason, message, null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public EnvironmentFailureException(EnvironmentImpl envImpl,
                                       EnvironmentFailureReason reason,
                                       String message,
                                       Throwable cause) {
        super(makeMsg(envImpl, reason, message, cause), cause);
        this.reason = reason;

        if (reason.invalidatesEnvironment()) {
            /*
             * If the environment exists, invalidate it. Note that there are
             * cases, such as if an exception occurred during recovery, that
             * the environment will not exist.
             */
            if (envImpl == null) {
                assert !reason.envShouldExist();
            } else {
                envImpl.invalidate(this);
            }
        }
    }

    private static String makeMsg(EnvironmentImpl envImpl,
                                  EnvironmentFailureReason reason,
                                  String message,
                                  Throwable cause) {
        StringBuilder s = new StringBuilder(300);
        if (envImpl != null) {
            s.append(envImpl.getName()).append(" ");
        }

        if (message != null) {
            s.append(message);
            s.append(' ');
        } else if (cause != null) {
            s.append(cause.toString());
            s.append(' ');
        }
        assert reason != null;
        s.append(reason);

        /*
         * Use the current environment status for reporting in the exception
         * message.  This is more information than simply whether this
         * exception caused an invalidate, since previous exceptions may have
         * occurred.
         */
        if (reason.invalidatesEnvironment() ||
            (envImpl != null && !envImpl.isValid())) {
            s.append(" Environment is invalid and must be closed.");
        }

        s.append(" Suggestion: ").append(reason.getSuggestion().toString());
        return s.toString();
    }

    /**
     * For internal use only.
     * @hidden
     * Only for use by wrapSelf methods.
     */
    protected EnvironmentFailureException(String message,
                                          EnvironmentFailureException cause) {
        super(message, cause);
        assert cause != null;
        reason = cause.reason;
    }

    /**
     * For internal use only.
     * @hidden
     * Use to re-throw an exception that originally caused the Environment to
     * be invalidated, wrapped in a new exception with the same class and
     * properties.
     *
     * <p>Providing both stack traces is important because the new exception
     * may thrown in a different context than the original exception. The same
     * class and properties are used so that when the new exception is caught,
     * it can be handled in the same way as the original exception.</p>
     *
     * <p>Returns a new exception that has the same class and properties as
     * this exception, other than the message (the given param should be
     * used), the stack trace (should be the current stack trace), and the
     * cause exception (should be null).</p>*
     *
     * <p>The caller must clone the original exception and pass it as the
     * clonedCause exception. Cloning is necessary to avoid a CIRCULAR
     * REFERENCE when a saved cause exception appears in the cause
     * chain for multiple thrown exceptions (see SavedExceptionTest).
     * {@link FormatUtil#cloneBySerialization} is used as a practical way to
     * clone all cause and suppressed exceptions.</p>
     *
     * <p>This method must be overridden by every concrete subclass to return
     * a new instance of the same class.</p>
     * <ul>
     *     <li>The given msg must be used and all other properties must
     *     be copied.</li>
     *
     *     <li>The Environment must not be invalidated by the ctor, since
     *     this will have already been done (if appropriate) by the original
     *     exception's ctor.</li>
     * </ul>
     *
     * <p>A simple implementation can use a ctor with message and cause params
     * that invokes super(msg, cause) and eventually invokes the (msg, cause)
     * ctor in this class. The ctors should to copy the non-msg properties.
     * All such ctors should only be used when invalidation of the Environment
     * is unnecessary and should have the minimum possible visibility,
     * preferably private.</p>
     */
    public EnvironmentFailureException wrapSelf(
        final String msg,
        final EnvironmentFailureException clonedCause) {

        assert EnvironmentFailureException.class == this.getClass() :
               "Missing overriding " + this.getClass().getName() +
               ".wrapSelf() method";

        return new EnvironmentFailureException(msg, clonedCause);
    }

    @Override
    public String getMessage() {
        return (javaErrorMsg != null) ? javaErrorMsg : super.getMessage();
    }

    /**
     * For internal use only.
     * @hidden
     */
    public EnvironmentFailureReason getReason() {
        return reason;
    }

    /**
     * Added to explain what would be an optimal action to take when encounter
     * a certain EnvironmentFailureException
     * @return EnvironmentFailureReason.Suggestion
     */
    public EnvironmentFailureReason.Suggestion getSuggestion() {
        return reason.getSuggestion();
    }

    /**
     * Whether the EnvironmentFailureException indicates that the log is
     * corrupt, meaning that a network restore (or restore from backup) should
     * be performed.
     * <p>
     * This method currently returns true only when corruption has been
     * detected by verifying checksums in the disk data log, and in this case
     * the corruption indicates a media/disk failure. This method will return
     * true in this case.
     * <p>
     * Additionally, when a persistent corruption is detected, the {@link
     * EnvironmentConfig#LOG_CHECKSUM_FATAL} param is true (the default) and
     * the Environment is open for read-write access,  a marker file named
     * 7fffffff.jdb is created in the Environment directory that will
     * prevent re-opening the environment. If an attempt is made to
     * re-open the Environment, the original EnvironmentFailureException
     * will be thrown. This is meant to safeguard against using a corrupt
     * environment when the original exception is accidentally overlooked.
     * While the marker file can be deleted to allow re-opening the
     * environment, this is normally unsafe and is not recommended.
     *
     * @return true if the environment is corrupt.
     *
     * @since 7.3
     */
    public boolean isCorrupted() {
        return reason == EnvironmentFailureReason.LOG_CHECKSUM;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * May ONLY be used for EnvironmentImpl.preallocatedEFE.
     */
    public static EnvironmentFailureException makeJavaErrorException(
        String msg) {
        return new EnvironmentFailureException(msg);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * May ONLY be used for EnvironmentImpl.preallocatedEFE.
     */
    public void setJavaErrorMessage(String msg) {
        assert isJavaError();
        javaErrorMsg = msg;
    }

    @Override
    boolean isJavaError() {
        return reason == EnvironmentFailureReason.JAVA_ERROR;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public EnvironmentFailureException clone() {
        return FormatUtil.cloneBySerialization(this);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Promotes the given cause exception and message to an
     * EnvironmentFailureException.
     *
     * If the cause is not an EnvironmentFailureException, wraps the cause
     * exception in an EnvironmentFailureException along with the message.  If
     * the cause is an EnvironmentFailureException, adds the message to it.
     *
     * @return the resulting EnvironmentFailureException.
     */
    public static EnvironmentFailureException
        promote(EnvironmentImpl envImpl,
                EnvironmentFailureReason reason,
                String message,
                Throwable cause) {
        if (cause instanceof EnvironmentFailureException) {
            EnvironmentFailureException e =
                (EnvironmentFailureException) cause;
            e.addErrorMessage(message);
            return e;
        }

        return new EnvironmentFailureException
            (envImpl, reason, message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates an exception indicating that an unexpected exception was caught
     * internally.  Used in place of an assert, when an exception is preferred.
     * Used when the Environment should *not* be invalidated.
     */
    public static EnvironmentFailureException
        unexpectedException(Exception cause) {
        return new EnvironmentFailureException
            (EnvironmentFailureReason.UNEXPECTED_EXCEPTION,
             null /*message*/, cause);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates an exception indicating that an unexpected exception was caught
     * internally.  Used in place of an assert, when an exception is preferred.
     * Used when the Environment *should* be invalidated.
     */
    public static EnvironmentFailureException
        unexpectedException(EnvironmentImpl envImpl, Exception cause) {
        return new EnvironmentFailureException
            (envImpl, EnvironmentFailureReason.UNEXPECTED_EXCEPTION_FATAL,
             null /*message*/, cause);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates an exception indicating that an unexpected exception was caught
     * internally.  Used in place of an assert, when an exception is preferred.
     * Used when the Environment should *not* be invalidated.
     */
    public static EnvironmentFailureException
        unexpectedException(String message, Exception cause) {
        return new EnvironmentFailureException
            (EnvironmentFailureReason.UNEXPECTED_EXCEPTION, message,
             cause);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates an exception indicating that an unexpected exception was caught
     * internally.  Used in place of an assert, when an exception is preferred.
     * Used when the Environment *should* be invalidated.
     */
    public static EnvironmentFailureException
        unexpectedException(EnvironmentImpl envImpl,
                            String message,
                            Exception cause) {
        return new EnvironmentFailureException
            (envImpl, EnvironmentFailureReason.UNEXPECTED_EXCEPTION_FATAL,
             message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates an exception indicating that unexpected internal state was
     * detected.  Used in place of an assert, when an exception is preferred.
     * Used when the Environment should *not* be invalidated.
     */
    public static EnvironmentFailureException unexpectedState() {
        return new EnvironmentFailureException
            (EnvironmentFailureReason.UNEXPECTED_STATE,
             null /*message*/, null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates an exception indicating that unexpected internal state was
     * detected.  Used in place of an assert, when an exception is preferred.
     * Used when the Environment *should* be invalidated.
     */
    public static EnvironmentFailureException
        unexpectedState(EnvironmentImpl envImpl) {
        return new EnvironmentFailureException
            (envImpl, EnvironmentFailureReason.UNEXPECTED_STATE_FATAL,
             null /*message*/, null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates an exception indicating that unexpected internal state was
     * detected.  Used in place of an assert, when an exception is preferred.
     * Used when the Environment should *not* be invalidated.
     */
    public static EnvironmentFailureException unexpectedState(String message) {
        return new EnvironmentFailureException
            (EnvironmentFailureReason.UNEXPECTED_STATE, message,
             null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates an exception indicating that unexpected internal state was
     * detected.  Used in place of an assert, when an exception is preferred.
     * Used when the Environment *should* be invalidated.
     */
    public static EnvironmentFailureException
        unexpectedState(EnvironmentImpl envImpl, String message) {
        return new EnvironmentFailureException
            (envImpl, EnvironmentFailureReason.UNEXPECTED_STATE_FATAL,
             message, null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates an exception indicating that unexpected internal state was
     * detected.  Used in place of an assert, when an exception is preferred.
     * Used when the Environment *should* be invalidated. use only for 
     * cases where errors in VLSNIndex causes the environment invalidation and 
     * if we need to dump the VLSNIndex into the log.
     */
    public static EnvironmentFailureException
        unexpectedState(EnvironmentImpl envImpl, String message,
                        VLSNIndex vlsnIndex) {
        if (vlsnIndex != null) {
            vlsnIndex.dumpToLog();
        }
        return new EnvironmentFailureException
            (envImpl, EnvironmentFailureReason.UNEXPECTED_STATE_FATAL,
             message, null /*cause*/);
    }


    /**
     * For internal use only.
     * @hidden
     *
     * Convenience method that throws an UNEXPECTED_STATE exception (non-fatal)
     * if the given condition is false.
     */
    public static void assertState(boolean cond) {

        /*
         * Implementation in assertState(boolean,String) is repeated to reduce
         * assertion overhead.
         */
        if (!cond) {
            throw unexpectedState((String) null);
        }
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Convenience method that throws an UNEXPECTED_STATE exception (non-fatal)
     * if the given condition is false.
     * <p>
     * Note: If the message must be computed (it is not a constant or already
     * available), then it is more efficient to use {@link
     * #assertState(boolean, Supplier)}.
     */
    public static void assertState(boolean cond, String message) {

        /*
         * Implementation in assertState(boolean) is repeated to reduce
         * assertion overhead.
         */
        if (!cond) {
            throw unexpectedState(message);
        }
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Convenience method that throws an UNEXPECTED_STATE exception (non-fatal)
     * if the given condition is false.
     * <p>
     * A Supplier is used to make this behave like a Java assert, where the
     * message construction does not occur unless cond is false. Note that the
     * Supplier object is still created unconditionally, so there is a small
     * additional cost compared to an 'if' statement or Java assert.
     */
    public static void assertState(boolean cond, Supplier<String> message) {
        if (!cond) {
            throw unexpectedState(message.get());
        }
    }
}
