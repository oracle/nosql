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

import static java.util.stream.Collectors.joining;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.stream.Stream;

import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A class for creating dynamic proxies that provide initiator (client-side)
 * implementations of AsyncVersionedRemote interfaces based on {@link
 * MethodCall} classes. See {@link #createProxy}.
 */
public class AsyncInitiatorProxy extends AbstractAsyncInitiatorProxy {

    private AsyncInitiatorProxy(Class<? extends AsyncVersionedRemote>
                                serviceInterface,
                                CreatorEndpoint endpoint,
                                DialogType dialogType,
                                Logger logger) {
        super(serviceInterface, computeGetSerialVersionCall(serviceInterface),
              endpoint, dialogType, logger);
    }

    /**
     * Creates a dynamic proxy that implements the specified service interface
     * by sending requests to the specified endpoint. Each remote method of the
     * service interface should have a {@link MethodCallClass} annotation that
     * specifies the class that implements {@link MethodCall} for the method.
     * The appropriate constructor for that method call class will be called
     * reflectively. Both the method call class and the constructor should be
     * public so that they can be called reflectively using utility methods in
     * other packages.
     *
     * @param <T> the type of the async versioned remote interface
     * @param serviceInterface the async versioned remote interface
     * @param expectedDialogTypeFamily the dialog type family that the dialog
     * type should belong to
     * @param endpoint the associated endpoint
     * @param dialogType the dialog type
     * @param logger the logger
     * @return the proxy
     * @throws IllegalArgumentException if the dialog type has the wrong type
     * family
     */
    public static <T extends AsyncVersionedRemote>
        T createProxy(Class<T> serviceInterface,
                      DialogTypeFamily expectedDialogTypeFamily,
                      CreatorEndpoint endpoint,
                      DialogType dialogType,
                      Logger logger)
    {
        return new AsyncInitiatorProxy(serviceInterface, endpoint, dialogType,
                                       logger)
            .createProxy(serviceInterface, expectedDialogTypeFamily,
                         dialogType);
    }

    private static MethodCall<Short> computeGetSerialVersionCall(
        Class<? extends AsyncVersionedRemote> serviceInterface)
    {
        try {
            final Method method =
                serviceInterface.getMethod("getSerialVersion", Short.TYPE,
                                           Long.TYPE);
            /*
             * There is no good way to check the return type because these
             * methods return a CompletableFuture and we can't check its type
             * parameter. Just cast and expect we'll get a runtime exception if
             * something is wrong.
             */
            @SuppressWarnings("unchecked")
            final MethodCall<Short> result =
                (MethodCall<Short>) staticGetMethodCall(method, null);
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Unexpected exception: " + e, e);
        }
    }

    /**
     * An annotation to put on methods of an {@link AsyncVersionedRemote}
     * subinterface to record what {@link MethodCall} class should be used to
     * serialize the method arguments.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface MethodCallClass {

        /**
         * Returns the {@link MethodCall} class that should be used to
         * serialize method arguments for the annotated method.
         */
        Class<? extends MethodCall<?>> value();
    }

    /**
     * Invokes an async versioned remote method, with arguments: <ul>
     * <li> short serialVersion
     * <li> method-specific arguments
     * <li> long timeoutMillis
     * </ul>
     */
    @Override
    protected Object invokeNonObject(Method method,
                                     Object @Nullable[] args)
        throws Exception
    {
        if (args == null) {
            throw new IllegalArgumentException(
                "Null arguments not permitted in call to " + method);
        }
        if (args.length < 2) {
            throw new IllegalArgumentException(
                "At least two arguments are required in call to " + method);
        }
        if (!(args[0] instanceof Short)) {
            throw new IllegalArgumentException(
                "First argument must be Short in call to " + method +
                ": " + Arrays.toString(args));
        }
        final short serialVersion = (Short) args[0];
        final Object[] methodCallArgs = (args.length > 2) ?
            Arrays.copyOfRange(args, 1, args.length - 1) :
            null;
        final MethodCall<?> methodCall = getMethodCall(method, methodCallArgs);
        final long timeoutMillis = (Long) args[args.length - 1];
        return initiator.startDialog(serialVersion, methodCall, timeoutMillis);
    }

    @Override
    MethodCall<?> getMethodCall(Method method,
                                Object @Nullable[] methodCallArgs)
        throws Exception
    {
        return staticGetMethodCall(method, methodCallArgs);
    }

    private static
        MethodCall<?> staticGetMethodCall(Method method,
                                          Object @Nullable[] methodCallArgs)
        throws Exception
    {
        final Class<?> methodClass = method.getDeclaringClass();
        if (!AsyncVersionedRemote.class.isAssignableFrom(methodClass)) {
            throw new IllegalArgumentException(
                "Method class does not implement AsyncVersionedRemote: " +
                methodClass.getName());
        }

        final MethodCallClass annotation = getAnnotation(method);
        final Class<? extends MethodCall<?>> methodCallClass =
            annotation.value();

        /* Strip off serialVersion and timeoutMillis parameters */
        final Class<?>[] methodParamTypes = method.getParameterTypes();
        final Class<?>[] consParamTypes = (methodParamTypes.length > 2) ?
            Arrays.copyOfRange(methodParamTypes, 1,
                               methodParamTypes.length - 1) :
            EMPTY_CLASSES;

        final Constructor<? extends MethodCall<?>> cons;
        try {
            cons =  methodCallClass.getDeclaredConstructor(consParamTypes);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                "Unable to find constructor for MethodCall class " +
                methodCallClass.getName() +
                " with constructor parameter types " +
                Stream.of(consParamTypes).map(Object::toString)
                .collect(joining(", ", "(", ")")) +
                " for use with remote method " + method,
                e);
        }
        return cons.newInstance(methodCallArgs);
    }

    /*
     * Eclipse isn't paying attention to the @Nullable annotation here, so
     * suppress "unused" to avoid complaint about unneeded null check.
     */
    @SuppressWarnings("unused")
    private static MethodCallClass getAnnotation(Method method) {
        final @Nullable MethodCallClass annotation =
            method.getAnnotation(MethodCallClass.class);
        if (annotation == null) {
            throw new IllegalArgumentException(
                "Attempt to call remote method that has no" +
                " MethodCallClass annotation: " + method);
        }
        return annotation;
    }
}
