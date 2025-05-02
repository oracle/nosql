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

package oracle.kv.impl.async;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.synchronizedMap;
import static oracle.kv.impl.security.ProxyUtils.findInterfaceMethods;
import static oracle.kv.impl.util.AbstractInvocationHandler.invokeMethod;
import static oracle.kv.impl.util.SerializationUtil.readJavaSerial;
import static oracle.kv.impl.util.SerializationUtil.writeJavaSerial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.rmi.Remote;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.util.registry.VersionedRemote;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Maps {@link VersionedRemote} methods to {@link MethodOp} and {@link
 * MethodCall}, for use in implementing versioned remote interfaces using
 * the async network protocol.
 */
public class JavaSerialMethodTable {

    /** Maps remote interfaces to Method tables. */
    private static final
        Map<Class<? extends VersionedRemote>, JavaSerialMethodTable> tables =
        synchronizedMap(new HashMap<>());

    /*
     * The values of both idMap and methodMap are thread safe because the
     * fields are final, the values are initialized before the constructor
     * exits, they are not modified after that, and they are only referenced
     * after the constructor exits.
     */

    /** Maps method IDs to MethodOps. */
    private final Map<Integer, MethodOpImpl> idMap = new HashMap<>();

    /** Maps Methods to MethodOps. */
    private final Map<Method, MethodOpImpl> methodMap = new HashMap<>();

    /** Creates an instance and initializes the ID and method maps. */
    private JavaSerialMethodTable(Class<? extends VersionedRemote> remoteType) {
        initMaps(remoteType, idMap, methodMap);
    }

    /**
     * Returns the table for the specified remote interface.
     *
     * @param remoteType the type of the remote object
     * @return the table
     */
    public static JavaSerialMethodTable
        getTable(Class<? extends VersionedRemote> remoteType)
    {
        return tables.computeIfAbsent(remoteType, JavaSerialMethodTable::new);
    }

    /**
     * Returns the {@link MethodCall} for the specified method.
     *
     * @param method the method for the method call
     * @param args the arguments for the method call
     * @return the method call
     * @throws IllegalArgumentException if the method is not found
     */
    JavaSerialMethodCall getMethodCall(Method method,
                                       @Nullable Object @Nullable[] args) {
        return new JavaSerialMethodCall(getMethodOp(method), args);
    }

    /**
     * Returns the {@link MethodOp} associated with the specified integer
     * method op value.
     *
     * @param methodOpValue the integer method op value
     * @return the method op
     * @throws IllegalArgumentException if the method op value is not found
     */
    MethodOpImpl getMethodOp(int methodOpValue) {
        final MethodOpImpl op = getMethodOpOrNull(methodOpValue);
        if (op == null) {
            throw new IllegalArgumentException(
                "No MethodOp found for method op value: " + methodOpValue);
        }
        return op;
    }


    /**
     * Returns the {@link MethodOp} associated with the specified integer
     * method op value, or {@code null} if not found.
     *
     * @param methodOpValue the integer method op value
     * @return the method op or {@code null}
     */
    public @Nullable MethodOpImpl getMethodOpOrNull(int methodOpValue) {
        return idMap.get(methodOpValue);
    }

    /**
     * An implementation of MethodCall that uses reflection and Java
     * serialization to serialize requests and responses. Note that exceptions
     * continue to be serialized using FAST_EXTERNALIZABLE and STANDARD
     * formats.
     */
    static class JavaSerialMethodCall implements MethodCall<Object> {
        private final MethodOpImpl methodOp;
        final @Nullable Object @Nullable[] args;

        private JavaSerialMethodCall(MethodOpImpl methodOp,
                                     @Nullable Object @Nullable[] args) {
            this.methodOp = methodOp;
            this.args = args;
        }

        /**
         * Makes the associated method call on the service using the specified
         * serial version.
         */
        Object callService(short serialVersion, VersionedRemote service)
            throws Exception
        {
            /*
             * The Eclipse null checker can't figure out that args.length is
             * safe
             */
            @SuppressWarnings("null")
            final int argsLength = (args == null) ? 0 : args.length;
            final Method method = methodOp.method;
            final @Nullable Object @Nullable[] newArgs;
            if ("getSerialVersion".equals(method.getName())) {
                if (argsLength > 0) {
                    throw new IllegalArgumentException(
                        "Arguments not permitted in call to" +
                        " getSerialVersion: " + Arrays.toString(args));
                }
                newArgs = null;
            } else {
                /* Add serialVersion to the end */
                if (args == null) {
                    newArgs = new Object[] { serialVersion };
                } else {
                    final @Nullable Object[] newArray =
                        Arrays.copyOf(args, argsLength + 1);
                    newArray[argsLength] = serialVersion;
                    newArgs = newArray;
                }
            }
            return invokeMethod(service, method, newArgs);
        }

        @Override
        public MethodOp getMethodOp() {
            return methodOp;
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writeJavaSerial(out, args);
        }

        @Override
        public void writeResponse(@Nullable Object response,
                                  DataOutput out,
                                  short sv)
            throws IOException
        {
            writeJavaSerial(out, response);
        }

        @Override
        public Object readResponse(DataInput in, short serialVersion)
            throws IOException
        {
            return readJavaSerial(in, Object.class);
        }

        @Override
        public String describeCall() {
            final Method method = methodOp.method;
            return method.getDeclaringClass().getSimpleName() + "." +
                method.getName() +
                (args != null ? Arrays.toString(args) : "[]");
        }
    }

    /**
     * A method op that records the method op value and the associated method.
     */
    static class MethodOpImpl implements MethodOp {
        final Method method;
        final int value;
        MethodOpImpl(Method method, int value) {
            this.method = method;
            this.value = value;
        }
        @Override
        public int getValue() {
            return value;
        }
        @Override
        public JavaSerialMethodCall readRequest(DataInput in,
                                                short serialVersion)
            throws IOException
        {
            return new JavaSerialMethodCall(
                this, readJavaSerial(in, Object[].class));
        }
        @Override
        public String toString() {
            return "MethodOpImpl[" +

                /*
                 * Print just the method class name, name, and parameters class
                 * names -- printing the method object will include the thrown
                 * exceptions, which makes it harder to look for thrown
                 * exceptions in the log.
                 */
                "method=" + method.getDeclaringClass().getName() + "." +
                method.getName() +
                Arrays.stream(method.getParameterTypes())
                .map(Class::getName)
                .collect(Collectors.joining(", ", "(", ")")) +
                " value=" + value + "]";
        }
    }

    /** Returns a SHA-256 message digest for computing method hashes. */
    static MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not found: " + e, e);
        }
    }

    /**
     * Computes an integer hash to represent the specified method, using an
     * approach inspired by Java RMI. The hash only depends on the method name,
     * the types of the method parameters, and the method return type. A string
     * is computed containing the method name and method descriptor, the string
     * is converted to UTF8, the bytes are used to compute a message digest,
     * and the highest order 4 bytes of the digest are converted to an integer.
     *
     * Using the standard rule of thumb, a random 32-bit value has 2^32
     * possible values, and that should be sufficient to represent 2^16 unique
     * values -- far more than the number of methods we expect for a given
     * remote interface. To be safe, unit tests should confirm that there are
     * no method collisions for classes that we use. (See
     * JavaSerialMethodTableTest.testAllTables.) We should plan to explicitly
     * assign non-colliding values, chosen randomly, for specific methods if
     * needed in the future.
     */
    static int computeMethodHash(Method method, MessageDigest md) {
        md.reset();
        final String desc = getMethodNameAndDescriptor(method);
        final byte[] bytes = desc.getBytes(UTF_8);
        final byte[] digest = md.digest(bytes);
        return ((0xff & digest[0]) << 24) +
            ((0xff & digest[1]) << 16) +
            ((0xff & digest[2]) << 8) +
            (0xff & digest[3]);
    }

    /**
     * Returns the method name and descriptor string for the specified method.
     * The method descriptor uses a similar format to the one documented in the
     * Java Virtual Machine Specification -- see section 4.3.2. Field
     * Descriptors -- but uses dots instead of slashes for package separators.
     */
    static String getMethodNameAndDescriptor(Method method) {
        final StringBuilder sb = new StringBuilder();
        sb.append(method.getName());
        sb.append('(');
        for (final Class<?> parameterType : method.getParameterTypes()) {
            sb.append(getTypeDescriptor(parameterType));
        }
        sb.append(')');
        sb.append(getTypeDescriptor(method.getReturnType()));
        return sb.toString();
    }

    /**
     * Returns the type descriptor for the specified type, similar to the one
     * documented in the Java Virtual Machine Specification, but using dots for
     * package separators rather than slashes.
     *
     * Arrays have a '[' for each array nesting, followed by the element type.
     * Primitives have a single uppercase letter for each of the 9 primitive
     * types. Classes have 'L', followed by the fully qualified class name,
     * followed by a semicolon.
     */
    static String getTypeDescriptor(Class<?> type) {
        if (type.isArray()) {
            /*
             * getName returns a '[' for each array nesting and supplies the
             * appropriate primitive and class type values
             */
            return type.getName();
        } else if (!type.isPrimitive()) {
            return 'L' + type.getName() + ';';
        } else if (type == Boolean.TYPE) {
            return "Z";
        } else if (type == Byte.TYPE) {
            return "B";
        } else if (type == Character.TYPE) {
            return "C";
        } else if (type == Double.TYPE) {
            return "D";
        } else if (type == Float.TYPE) {
            return "F";
        } else if (type == Integer.TYPE) {
            return "I";
        } else if (type == Long.TYPE) {
            return "J";
        } else if (type == Short.TYPE) {
            return "S";
        } else if (type == Void.TYPE) {
            return "V";
        } else {
            throw new AssertionError("Type not recognized: " + type);
        }
    }

    /** Returns the {@link MethodOp} associated with the specified method.
     *
     * @param method the method
     * @return the method op
     * @throws IllegalArgumentException if the method is not found
     */
    MethodOpImpl getMethodOp(Method method) {
        final MethodOpImpl op = methodMap.get(method);
        if (op == null) {
            throw new IllegalArgumentException(
                "No MethodOp found for method: " + method);
        }
        return op;
    }

    private static void initMaps(Class<? extends VersionedRemote> remoteType,
                                 Map<Integer, MethodOpImpl> idMap,
                                 Map<Method, MethodOpImpl> methodMap) {
        final MessageDigest md = getMessageDigest();
        for (final Method method :
                 findInterfaceMethods(remoteType, Remote.class)) {
            final int id = computeMethodHash(method, md);
            final MethodOpImpl newMethodOp = new MethodOpImpl(method, id);
            MethodOpImpl oldMethodOp = idMap.putIfAbsent(id, newMethodOp);
            if (oldMethodOp != null) {
                throw new IllegalStateException(
                    "ID " + id + " found for two methods: " +
                    "'" + method + "' and '" + oldMethodOp.method + "'");
            }
            methodMap.put(method, newMethodOp);
        }
    }
}
