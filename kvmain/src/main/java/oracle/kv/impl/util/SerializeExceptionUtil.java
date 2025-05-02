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

package oracle.kv.impl.util;

import static oracle.kv.impl.util.CommonLoggerUtils.appendCurrentStack;
import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import oracle.kv.FastExternalizableException;

/**
 * Utilities for reading and writing exceptions without using Java
 * serialization, to support non-Java clients.  This class provides the {@link
 * #writeException} and {@link #readException} methods to serialize and
 * deserialize Java, JE, and KVS exceptions in a portable format.
 *
 * <p>For exceptions defined in the KVS codebase, any exception that needs to
 * be serialized should extend {@link FastExternalizable} and provide a
 * standard public constructor with {@link DataInput} and {@code short}
 * parameters.  The {@link FastExternalizableException} base class is provided
 * to make this convenient.  Exceptions that are only used on the server, only
 * used on one side of the network, or otherwise not serialized, are not
 * required to implement {@code FastExternalizable}, but will cause exceptions
 * to be thrown if an attempt is made to serialize them.  To make maintenance
 * easier, all exception classes that extend {@code FastExternalizable} should
 * explicitly define a {@link FastExternalizable#writeFastExternal} method,
 * even if the method does nothing more than delegate to the super method.
 * That way, the method provides an obvious place to update when and if the
 * class is modified to add new fields.
 *
 * <p>For exceptions not defined in the KVS codebase, there is a second
 * mechanism that serializes the exception message and cause, and reconstructs
 * the exception through a reflective call to a public constructor either with
 * {@link String} and {@link Throwable} parameters, or with a {@code String}
 * parameter followed by a call to {@link Throwable#initCause} to set a
 * non-null cause.  This scheme is able to reconstruct most, but not all, Java
 * and JE exceptions.  In particular, the {@link AssertionError} class gets
 * special handling.  If a suitable constructor is not available, or some other
 * problem occurs, a {@link RuntimeException} is returned instead.  If this
 * lack of fidelity causes trouble, we could support more exception classes by
 * either using additional constructors (say no-argument constructors for
 * exceptions that never have a cause or a message) or by adding class-specific
 * handlers that serialize custom information and use class-specific
 * constructors to reconstruct the exception.  We can consider these approaches
 * in the future as needed.
 *
 * @see #writeException Serialization format
 */
public class SerializeExceptionUtil {

    /**
     * Represents the format used to serialize an exception.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public enum Format implements FastExternalizable {

        /** An exception that implements {@link FastExternalizable}. */
        FAST_EXTERNALIZABLE(0),

        /** A standard exception with just a detail message and cause. */
        STANDARD(1),

        /** Use Java serialization. Not currently used. */
        JAVA_SERIAL(2);

        private static final Format[] VALUES = values();

        private Format(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        /**
         * Reads an instance from the input stream.
         */
        public static Format readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException(
                    "Wrong ordinal for SerializeExceptionUtil.Format: " +
                    ordinal, e);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>value</i> // {@link #FAST_EXTERNALIZABLE}=0,
         *      {@link #STANDARD}=1, {@link #JAVA_SERIAL}=2
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
            out.writeByte(ordinal());
        }
    }

    /**
     * Writes a exception to an output stream in one of two formats.
     *
     * <p>If the exception implements {@link FastExternalizable}, then writes:
     * <ol>
     * <li> ({@link Format}) {@link Format#FAST_EXTERNALIZABLE}
     * <li> ({@link SerializationUtil#writeNonNullString non-null String})
     *      <i>fully qualified class name</i>
     * <li> ({@link FastExternalizable}) <i>fast externalizable data</i>
     * </ol>
     *
     * <p>Otherwise, writes:
     * <ol>
     * <li> ({@link Format}) {@link Format#STANDARD}
     * <li> ({@link SerializationUtil#writeNonNullString non-null String})
     *      <i>fully qualified class name</i>
     * <li> ({@link SerializationUtil#writeString String}) <i>exception detail
     *      message</i>
     * <li> ({@link DataOutput#writeBoolean}) <i>whether cause is present</i>
     * <li> <i>[Optional]</i> ({@link #writeException exception}) {@code cause}
     * <li> ({@link #writeStackTrace}) <i>stack trace</i>
     * </ol>
     *
     * @param exception the exception
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code exception} is defined in the
     * KVS codebase and does not implement {@code FastExternalizable}, or if it
     * is not defined in the KVS codebase and does not have either a String or
     * String and Throwable constructor
     */
    public static void writeException(Throwable exception,
                                      DataOutput out,
                                      short serialVersion)
        throws IOException {

        checkNull("exception", exception);
        if (exception instanceof FastExternalizable) {
            writeFastExternal(
                (FastExternalizable) exception, out, serialVersion);
        } else {
            writeStandard(exception, out, serialVersion);
        }
    }

    /**
     * Reads an exception from the input stream in FAST_EXTERNALIZABLE or
     * STANDARD format.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @throws IOException if an I/O error occurs, if the input format is
     * invalid, or if a class is not found when deserializing an object in Java
     * serialized form
     */
    public static Throwable readException(DataInput in, short serialVersion)
        throws IOException {

        final Format format = Format.readFastExternal(in, serialVersion);
        switch (format) {
        case FAST_EXTERNALIZABLE:
            return readFastExternal(in, serialVersion);
        case STANDARD:
            return readStandard(in, serialVersion);
        default:
            throw new IOException(
                "Exception format is not permitted: " + format);
        }
    }

    /** Writes a FastExternalizable exception. */
    private static void writeFastExternal(FastExternalizable exception,
                                          DataOutput out,
                                          short serialVersion)
        throws IOException {

        Format.FAST_EXTERNALIZABLE.writeFastExternal(out, serialVersion);
        writeNonNullString(out, serialVersion, exception.getClass().getName());
        exception.writeFastExternal(out, serialVersion);
    }

    /**
     * Reads a FastExternalizable exception, creating the instance with the
     * standard FastExternalizable constructor and throwing an IOException if
     * there is an I/O error, format error, or a problem calling the
     * constructor.
     */
    private static Throwable readFastExternal(DataInput in,
                                              short serialVersion)
        throws IOException {

        final String className = readNonNullString(in, serialVersion);
        try {
            final Class<?> cl = Class.forName(className);
            if (!Throwable.class.isAssignableFrom(cl)) {
                throw new IllegalStateException(
                    "Attempt to deserialize an instance of a non-exception" +
                    " class as an exception: " + className);
            }
            final Class<? extends Throwable> exceptClass =
                cl.asSubclass(Throwable.class);
            if (!FastExternalizable.class.isAssignableFrom(exceptClass)) {
                throw new IllegalStateException(
                    "Attempt to deserialize an exception in" +
                    " FAST_EXTERNALIZABLE format whose class does not" +
                    " implement FastExternalizable: " + className);
            }
            final Constructor<? extends Throwable> cons =
                exceptClass.getConstructor(DataInput.class, Short.TYPE);
            return cons.newInstance(in, serialVersion);
        } catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw readExceptionFailed(className, null, e);
        } catch (Exception e) {
            throw readExceptionFailed(className, null, e);
        }
    }

    private static IOException readExceptionFailed(String className,
                                                   String message,
                                                   Throwable exception) {
        return new IOException(
            "Problem reading exception of type " + className +
            ((message != null) ? " with message '" + message + "': " : ": ") +
            exception,
            exception);
    }

    /**
     * Writes a standard exception, throwing IllegalArgumentException for a KVS
     * exception.
     */
    private static void writeStandard(Throwable exception,
                                      DataOutput out,
                                      short serialVersion)
        throws IOException {

        if (isKVSClass(exception.getClass())) {
            throw new IllegalArgumentException(
                "Attempt to write a KVS exception that does not implement" +
                " FastExternalizable: " + exception,
                exception);
        }
        Format.STANDARD.writeFastExternal(out, serialVersion);
        writeNonNullString(out, serialVersion, exception.getClass().getName());
        writeExceptionContents(out, serialVersion, exception.getMessage(),
                               exception);
    }

    /** Returns whether a class is defined in the KVS codebase. */
    private static final boolean isKVSClass(Class<?> cl) {
        return cl.getName().startsWith("oracle.kv.");
    }

    /**
     * Writes an exception message, optional stack trace (depending on the
     * serial version), and cause to the output stream.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param message the exception message or null
     * @param exception the exception
     * @throws IOException if an I/O error occurs
     */
    public static void writeExceptionContents(DataOutput out,
                                              short serialVersion,
                                              String message,
                                              Throwable exception)
        throws IOException {

        Throwable cause = exception.getCause();
        if (cause != null) {
            if (!isSerializableException(cause)) {

                /*
                 * If the cause isn't serializable, then skip it, but include
                 * information about it in the main exception's message
                 */
                message += "\n\tCaused by: " + cause;
                cause = null;
            }
        }

        writeString(out, serialVersion, message);
        if (cause != null) {
            out.writeBoolean(true);
            writeException(cause, out, serialVersion);
        } else {
            out.writeBoolean(false);
        }
        writeStackTrace(out, serialVersion, exception);
    }

    /**
     * Returns whether the exception can be serialized using these utilities,
     * either because it implements FastExternalizable or because it is a
     * standard exception that is not part of KVStore.
     */
    private static boolean isSerializableException(Throwable e) {
        return (e instanceof FastExternalizable) || !isKVSClass(e.getClass());
    }

    /**
     * Reads a standard exception, using one of the standard constructors and
     * the initCause methods as appropriate, and throwing an IOException if
     * there is an I/O error or format problem.  Returns a RuntimeException
     * with the specified message and cause if there is a problem constructing
     * an exception of the specified type.
     */
    private static Throwable readStandard(DataInput in, short serialVersion)
        throws IOException {

        final String className = readNonNullString(in, serialVersion);
        final String message = readString(in, serialVersion);
        final Throwable cause =
            in.readBoolean() ? readException(in, serialVersion) : null;
        final StackTraceElement[] stackTrace =
            readStackTrace(in, serialVersion);
        Throwable result = null;
        try {
            final Class<? extends Throwable> cl =
                Class.forName(className).asSubclass(Throwable.class);
            try {
                final Constructor<? extends Throwable> cons =
                    cl.getConstructor(String.class, Throwable.class);
                result = cons.newInstance(message, cause);
            } catch (Exception e) {
                final Constructor<? extends Throwable> cons =
                    cl.getConstructor(String.class);
                final Throwable t = cons.newInstance(message);
                if (cause != null) {
                    t.initCause(cause);
                }
                result = t;
            }
        } catch (Exception e) {
        }
        /* Return a replacement exception */
        if (result == null) {
            result = new RuntimeException(
                "Problem reading exception of type " + className +
                ((message != null) ? " with message '" + message + "'" : ""),
                cause);
        }
        if (stackTrace != null) {
            result.setStackTrace(stackTrace);
            appendCurrentStack(result);
        }
        return result;
    }

    /**
     * Writes an exception's stack trace to the output stream. Writes:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength} <i>number of
     * stack trace elements</i>
     * <li>For each {@link StackTraceElement}:
     *   <ol>
     *   <li> ({@link SerializationUtil#writeNonNullString}) <i>class name</i>
     *   <li> ({@link SerializationUtil#writeNonNullString}) <i>method name</i>
     *   <li> ({@link SerializationUtil#writeString}) <i>file name</i>
     *   <li> ({@link SerializationUtil#writePackedInt}) <i>line number</i>
     *   </ol>
     * </ol>
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param exception the exception
     * @throws IOException if an I/O error occurs
     */
    public static void writeStackTrace(DataOutput out,
                                       short serialVersion,
                                       Throwable exception)
        throws IOException {

        final StackTraceElement[] stackTrace = exception.getStackTrace();
        writeNonNullSequenceLength(out, stackTrace.length);
        for (StackTraceElement elem : stackTrace) {
            writeNonNullString(out, serialVersion, elem.getClassName());
            writeNonNullString(out, serialVersion, elem.getMethodName());
            writeString(out, serialVersion, elem.getFileName());
            writePackedInt(out, elem.getLineNumber());
        }
    }

    /**
     * Reads exception stack trace elements from the input stream.
     */
    public static StackTraceElement[] readStackTrace(DataInput in,
                                                     short serialVersion)
        throws IOException {

        final int count = readNonNullSequenceLength(in);
        final StackTraceElement[] stackTrace = new StackTraceElement[count];
        for (int i = 0; i < count; i++) {
            final String className = readNonNullString(in, serialVersion);
            final String methodName = readNonNullString(in, serialVersion);
            final String fileName = readString(in, serialVersion);
            final int lineNumber = readPackedInt(in);
            /*
             * TODO: Java 9 added support for class loader name, module name,
             * and module version. Java 9 sets the class loader name to "app"
             * for classes defined by the system class loader. We should
             * consider doing something similar when we require Java 9.
             */
            stackTrace[i] = new StackTraceElement(
                className, methodName, fileName, lineNumber);
        }
        return stackTrace;
    }
}
