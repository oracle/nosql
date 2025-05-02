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
package oracle.kv.impl.security;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.rmi.Remote;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import oracle.kv.KVSecurityException;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.ProcessFaultHandler.Operation;
import oracle.kv.impl.security.annotations.PublicAPI;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureInternalMethod;
import oracle.kv.impl.util.AbstractInvocationHandler;

/**
 * Provide a proxy for an object that implements one or more Remote interfaces.
 * The proxy examines annotation information on the object class to determine
 * what security-checking steps are required before calling methods.
 */
public final class SecureProxy extends AbstractInvocationHandler {

    /* An immutable empty list of KVStorePrivilege */
    private static final List<KVStorePrivilege> emptyPrivilegeList =
        Collections.emptyList();

    /* The security-enabled object that be will be proxying for */
    private final Remote proxyTo;

    /* Map of Method to the MethodHandlers that will be used */
    private final Map<Method, MethodHandler> methodMap;

    /* Security checking interface */
    private final AccessChecker checker;

    /*
     * The fault handler with which the security evaluation should
     * be executed.
     */
    private final ProcessFaultHandler faultHandler;

    /*
     * A set of the known, security annotation classes
     */
    private static final Set<Class<?>> methodAnnotationClasses =
        new HashSet<Class<?>>();

    /*
     * Initialize methodAnnotationClasses
     */
    static {
        methodAnnotationClasses.add(PublicMethod.class);
        methodAnnotationClasses.add(SecureAutoMethod.class);
        methodAnnotationClasses.add(SecureInternalMethod.class);
    }

    /* A map of method to string describing the invocation of the method. */
    private final ConcurrentHashMap<Method, String> describeMap =
        new ConcurrentHashMap<>();

    @Override
    @SuppressWarnings("null")
    protected Object invokeNonObject(final Method method,
                                     final Object[] args)
        throws Exception {

        final MethodHandler handler = methodMap.get(method);
        if (handler == null) {

            /*
             * This is a major configuration error, and shouldn't be able
             * to occur, but wrap the exception in the fault handler
             * to allow it to be processed correctly.
             */
            faultHandler.execute(
                (Runnable) () -> {
                    throw new OperationFaultException(
                        "MethodHandler for method " +
                        qualifiedMethodName(method) + " was not found");
                });
        }
        return handler.invoke(method, args);
    }

    /**
     * Create a proxy object for the input object that performs all of the
     * security checks indicated through annotations on the Object's
     * implementation class.
     *
     * @param proxyTo the object for which a secure proxy is to be created.
     *        The object must implement one or more Remote interfaces and
     *        must be annotated with instructions on how security should be
     *        applied.
     * @param checker an object that validates the access roles based on
     *        annotation information.  If the instance is null, no checking
     *        is performed, though configuration is checked for validity.
     * @param faultHandler a process fault handler in which security checking
     *        operations are performed.
     * @return a proxy instance
     *
     * @throws ConfigurationException if the class annotation is incomplete or
     *        inconsistent.
     */
    public static <T extends Remote> T create(T proxyTo,
                                              AccessChecker checker,
                                              ProcessFaultHandler faultHandler)
        throws ConfigurationException {

        final SecureProxy proxyHandler =
            new SecureProxy(proxyTo, checker, faultHandler);

        /*
         * Create a dynamic proxy instance that implements all of the
         * supplied interfaces by calling invoke on a SecureProxy instance.
         */
        final Class<?>[] remoteInterfaces =
            ProxyUtils.findInterfaces(proxyTo.getClass(), Remote.class)
            .toArray(new Class<?>[0]);

        /* No compile-time type checking for Proxy.newProxyInstance */
        @SuppressWarnings("unchecked")
        final T proxy = (T) Proxy.newProxyInstance(
            proxyTo.getClass().getClassLoader(), remoteInterfaces,
            proxyHandler);
        return proxy;
    }

    /*
     * Only for use by create()
     */
    private SecureProxy(Remote proxyTo,
                        AccessChecker checker,
                        ProcessFaultHandler faultHandler)
        throws ConfigurationException {

        this.proxyTo = proxyTo;
        this.checker = checker;
        this.methodMap = new HashMap<Method, MethodHandler>();
        this.faultHandler = faultHandler;

        final Class<? extends Remote> proxyToClass = proxyTo.getClass();

        /* Find the methods declared by the remote interfaces of the class */
        final Set<Method> interfaceMethods =
            ProxyUtils.findInterfaceMethods(proxyToClass, Remote.class);

        /*
         * Build an alternate version keyed by method signature rather than
         * by identity, in order to allow matching across classes and
         * interfaces.  Populate it initially with nulls, to allow for it
         * to serve as a complete set of valid interface signatures.
         */
        final Map<String, MethodHandler> methodKeyMap =
            new HashMap<String, MethodHandler>();
        for (Method m : interfaceMethods) {
            final String mKey = methodKey(m);
            methodKeyMap.put(mKey, null);
        }

        /*
         * Collect method handler implementations, and augment the
         * interfaceMethods by adding the implementation methods.
         */
        collectMethodInfo(proxyToClass, methodKeyMap, interfaceMethods);

        /*
         * Now that we have a map of method signature to handler, populate
         * the methodMap with mapping of interface method to method handler.
         */
        for (Method m : interfaceMethods) {
            final String mKey = methodKey(m);
            final MethodHandler handler = methodKeyMap.get(mKey);
            if (handler != null) {
                methodMap.put(m, handler);
            } else {
                /*
                 * We have an interface method that does not have a
                 * method handler defined, so it cannot be called.
                 */
                throw new ConfigurationException(
                    "Interface method " + qualifiedMethodName(m) +
                    " has no method handler defined.");
            }
        }

        if (methodMap.isEmpty()) {
            throw new ConfigurationException(
                "Class " + proxyToClass +
                " has no proxyable interface methods.");
        }
    }

    /*
     * Recursively visit classes in derived-to-superclass order, checking
     * security annotations on the class and methods.  For each remote
     * interface method encountered that does not yet have a MethodHandler
     * defined, create a MethodHandler and mark the method as handled.
     */
    private void collectMethodInfo(
        Class<?> implClass,
        Map<String, MethodHandler> methodKeyMap,
        Set<Method> implMethods)
        throws ConfigurationException {

        if (implClass == Object.class) {
            return;
        }

        /*
         * Look to see how the specified class is annotated.  If it implements
         * any Remote interface methods, it must be marked as either SecureAPI
         * or PublicAPI.  If it does not implement any Remote methods then no
         * security annotation is required.
         */
        final Class<?> classAnnType = getClassSecureAnnotation(implClass);

        /*
         * Next, look at the annotations in individual methods.
         */
        for (Method m : implClass.getDeclaredMethods()) {
            final Annotation methodAnnotation = getMethodSecureAnnotation(m);
            final String mKey = methodKey(m);

            if (!methodKeyMap.containsKey(mKey)) {
                /* Not an interface method */
                if (methodAnnotation != null) {
                    throw new ConfigurationException(
                        "SecureMethod and PublicMethod annotations may not " +
                        "be applied to methods that are not Remote " +
                        "interface methods.  Method " + methodName(m) +
                        " of Class " + implClass + " is marked as " +
                        methodAnnotation.annotationType().getSimpleName());
                }
            } else {
                /* An interface method */

                if (null == classAnnType) {
                    throw new ConfigurationException(
                        "Class " + implClass +
                        " is not marked as either SecureAPI or PublicAPI.");
                }

                if (methodAnnotation == null &&
                    classAnnType == SecureAPI.class) {
                    throw new ConfigurationException(
                        "All Remote interface methods implemented by a " +
                        "class marked as SecureAPI must be annotated " +
                        "with a security decoration. " +
                        "Method " + methodName(m) + " of Class " +
                        implClass.getName() + " is not annotated.");
                }

                /* Record the method as invokable */
                implMethods.add(m);

                final MethodHandler handler =
                    makeMethodHandler(m, methodAnnotation);
                assert (handler != null);

                /*
                 * If this is not the first matching interface method in the
                 * traversal, set this MethodHandler as the one to use for
                 * the interace method.
                 */
                final MethodHandler currentHandler = methodKeyMap.get(mKey);
                if (currentHandler == null) {
                    methodKeyMap.put(mKey, handler);
                }
            }
        }

        /* Recursively visit super class implementation */
        final Class<?> implSuperclass = implClass.getSuperclass();
        collectMethodInfo(implSuperclass, methodKeyMap, implMethods);
    }

    /**
     * For the specified class, find what secure annotation, if any has been
     * applied.  The class may have at most one annotation.
     * @param implClass a class to examine
     * @return the the annotation type that has been applied, if any
     * @throws ConfigurationException if the class has more than one security
     * annotation.
     */
    private Class<?> getClassSecureAnnotation(Class<?> implClass)
        throws ConfigurationException {

        Class<?> classAnnType = null;

        for (Annotation a : implClass.getDeclaredAnnotations()) {
            if (a.annotationType() == SecureAPI.class ||
                a.annotationType() == PublicAPI.class) {
                if (classAnnType != null) {
                    throw new ConfigurationException(
                        "Class " + implClass.getName() +
                        " is marked as both " +
                        a.annotationType().getSimpleName() + " and " +
                        classAnnType.getSimpleName());
                }
                classAnnType = a.annotationType();
            }
        }

        return classAnnType;
    }

    /**
     * For the specified method, find what secure annotation, if any has been
     * applied.  The method may have at most one annotation.
     * @param method a Method to examine
     * @return the the annotation type that has been applied, if any
     * @throws ConfigurationException if the class has more than one security
     * annotation.
     */
    private Annotation getMethodSecureAnnotation(Method method)
        throws ConfigurationException {

        Annotation methodAnnotation = null;
        for (Annotation a : method.getDeclaredAnnotations()) {
            if (methodAnnotationClasses.contains(a.annotationType())) {
                if (methodAnnotation != null) {
                    throw new ConfigurationException(
                        "Method " + qualifiedMethodName(method) +
                        " is marked as both " +
                        methodAnnotation.annotationType().getSimpleName() +
                        " and " +
                        a.annotationType().getSimpleName());
                }
                methodAnnotation = a;
            }
        }
        return methodAnnotation;
    }

    /*
     * Create a method handler that should be used to invoke the
     * method.  If methodAnnotation is null, PublicMethod is assumed.
     */
    @SuppressWarnings("null")
    private MethodHandler makeMethodHandler(Method meth,
                                            Annotation methodAnnotation)
        throws ConfigurationException {

        final Class<?> methodAnnType = methodAnnotation == null ?
            PublicMethod.class : methodAnnotation.annotationType();
        MethodHandler handler = null;

        if (methodAnnType == PublicMethod.class) {
            /* No annotation, or, the method is not secured */
            handler = new MethodHandlerUtils.DirectHandler(proxyTo);
        } else if (methodAnnType == SecureAutoMethod.class) {
            final SecureAutoMethod secureMethod =
                (SecureAutoMethod) methodAnnotation;

            ensureAuthContext(meth);
            final KVStorePrivilegeLabel[] privilegeLabels =
                secureMethod.privileges();
            if (privilegeLabels == null || privilegeLabels.length == 0) {
                throw new ConfigurationException(
                    "SecureAutoMethod requires a non-empty privilege list: " +
                    qualifiedMethodName(meth));
            }
            final KVStorePrivilege[] userPrivileges =
                lookupPrivileges(privilegeLabels);
            handler = new CheckingHandler(userPrivileges);
        } else if (methodAnnType == SecureInternalMethod.class) {
            ensureAuthContext(meth);
            handler = new CheckingHandler(null /* empty privilege list */);
        } else {
            /* shouldn't occur */
            throw new IllegalStateException(
                "missing case for " + methodAnnType.getSimpleName());
        }

        return handler;
    }

    /*
     * Check that secureMethod has a AuthContext argument as the
     * next-to-last in the arg list.
     */
    private static void ensureAuthContext(Method secureMethod)
        throws ConfigurationException {

        final Class<?>[] args = secureMethod.getParameterTypes();
        if (args.length < 2 ||
            AuthContext.class != args[args.length - 2]) {

            throw new ConfigurationException(
                "Method " + qualifiedMethodName(secureMethod) +
                " does not have an AuthContext " +
                "argument in the next to last position.");
        }
    }

    /**
     * Constructs an array of canonical system privileges denoted by the labels
     *
     * @param labels labels of system privileges to look up
     * @return an array of canonical system privileges
     * @throws ConfigurationException if a label is not of system type, or
     * there is no system privilege for the label.
     */
    private static KVStorePrivilege[]
        lookupPrivileges(KVStorePrivilegeLabel[] labels)
        throws ConfigurationException {

        final KVStorePrivilege[] privileges =
            new KVStorePrivilege[labels.length];

        for (int i = 0; i < labels.length; i++) {
            final KVStorePrivilegeLabel label = labels[i];
            KVStorePrivilege privilege = null;
            try {
                privilege = SystemPrivilege.get(label);
            } catch (IllegalArgumentException iae) {
                throw new ConfigurationException(iae.getMessage(), iae);
            }
            if (privilege == null) {
                throw new ConfigurationException(
                    "The privilege label " + label +
                    " has no corresponding privilege");
            }
            privileges[i] = privilege;
        }
        return privileges;
    }

    /**
     * Construct a string that canonically encodes the relevant
     * type information to allow an interface method and an implementation
     * method to be recognized as the same.
     */
    private static String methodKey(Method m) {
        final StringBuffer sb = new StringBuffer();
        if ((m.getModifiers() & Modifier.PRIVATE) != 0) {

            /*
             * flag this as private to prevent a match against an interface
             * method.
             */
            sb.append("private ");
        }

        sb.append(m.getName());
        sb.append("(");
        final Class<?>[] paramTypes = m.getParameterTypes();
        for (Class<?> pt : paramTypes) {
            sb.append(pt.getName());
            sb.append(",");
        }
        sb.append(")");

        return sb.toString();
    }

    /**
     * Construct a string that concisely encodes the relevant type information
     * to allow a developer to recognize the method signature when reported in
     * an exception message.
     */
    private static String methodName(Method m) {
        final StringBuffer sb = new StringBuffer();
        sb.append(m.getName());
        sb.append("(");
        final Class<?>[] paramTypes = m.getParameterTypes();
        boolean first = true;
        for (Class<?> pt : paramTypes) {
            if (!first) {
                sb.append(",");
            }
            sb.append(pt.getSimpleName());
            first = false;
        }
        sb.append(")");

        return sb.toString();
    }

    /**
     * Construct a string that concisely encodes the relevant type information
     * to allow a developer to recognize the method signature when reported in
     * an exception message.
     */
    private static String qualifiedMethodName(Method m) {
        final StringBuffer sb = new StringBuffer();
        sb.append(m.getDeclaringClass().getSimpleName());
        sb.append(".");
        sb.append(methodName(m));
        return sb.toString();
    }

    /**
     * Checking invocation
     */
    final class CheckingHandler implements MethodHandler {

        private final List<KVStorePrivilege> requiredPrivileges;

        CheckingHandler(KVStorePrivilege[] requiredPrivileges) {
            this.requiredPrivileges =
                (requiredPrivileges == null) ?
                emptyPrivilegeList :
                Collections.unmodifiableList(
                    Arrays.asList(requiredPrivileges));
        }

        @Override
        public Object invoke(final Method method, final Object[] args)
            throws Exception {

            if (checker == null) {
                /* No security checker installed, so simply call directly */
                return invokeMethod(proxyTo, method, args);
            }

            /*
             * Apply our access checker to both evaluate the caller identity
             * and make sure they have sufficient access rights.  Execute
             * in the context of the process fault handler.
             */
            final ExecutionContext execCtx =
                faultHandler.execute(
                    (Supplier<ExecutionContext>) () -> {
                        final AuthContext authCtx =
                            (AuthContext) args[args.length - 2];

                        try {
                            return ExecutionContext.create(
                                checker, authCtx,
                                new MethodInvokeContext(
                                    method, requiredPrivileges));
                        } catch (SessionAccessException sae) {
                            throw sae;
                        } catch (KVSecurityException kvse) {
                            throw new ClientAccessException(kvse);
                        }
                    });

            return ExecutionContext.runWithContext(
                (Operation<Object, Exception>)
                () -> invokeMethod(proxyTo, method, args),
                execCtx);
        }
    }

    /**
     * Provides an OperationContext implementation for use with method
     * invocations audited by this module.
     */
    private final class MethodInvokeContext implements OperationContext {
        private final Method m;
        private final List<KVStorePrivilege> reqPrivileges;

        private MethodInvokeContext(Method m,
                                    List<KVStorePrivilege> reqPrivileges) {
            this.m = m;
            this.reqPrivileges = reqPrivileges;
        }

        @Override
        public String describe() {
            if (!describeMap.contains(m)) {
                describeMap.putIfAbsent(m, qualifiedMethodName(m));
            }
            return "attempt to call " + describeMap.get(m);
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            return reqPrivileges;
        }
    }
}
