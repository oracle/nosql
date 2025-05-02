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

package oracle.kv.impl.security.kerberos;

import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import oracle.kv.impl.security.kerberos.KerberosConfig.StoreKrbConfiguration;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * KerberosContext contains the logic to manage the JAAS subject generated via
 * Kerberos login module and operation description. It provides methods for
 * recording the login subject for static access at later point in a
 * Kerberos-related operation. This class only performs JAAS login for
 * server-side.
 */
public class KerberosContext {

    private static KerberosContext currentContext;

    private static StoreKrbConfiguration loginConfig;

    private final Subject subject;

    KerberosContext(Subject subject) {
        this.subject = subject;
    }

    /**
     * Find out current Kerberos context. This method will try to locate a
     * valid subject from calling context that contains Kerberos Ticket to
     * construct current Kerbeors context. If no valid subject found, call
     * {@link #getContext()}} to get a new context via JAAS login.
     * @throws LoginException JAAS login errors while trying to acquire tickets.
     */
    public synchronized static KerberosContext getCurrentContext()
        throws LoginException {

        final Subject subject = getCurrentSubject();

        if (subject == null ||
            subject.getPrivateCredentials(KerberosTicket.class).isEmpty()) {
            return getContext();
        }
        return new KerberosContext(subject);
    }

    /**
     * TODO: Replace with Subject::current directly when we switch to a Java 18
     * or later compilation target.
     */
    static Subject getCurrentSubject() {
        return SubjectWrapper.current();
    }

    /**
     * Get a new Kerberos context via JAAS login. If current context has not
     * been initialized, call {@link #createContext()}}} to create a new one.
     * @throws LoginException JAAS login errors while trying to acquire tickets.
     */
    public synchronized static KerberosContext getContext()
        throws LoginException {

        if (currentContext == null) {
            createContext();
        }
        return currentContext;
    }

    /**
     * Create a new context via JAAS login. This method will use underlying
     * Kerberos login module to acquire initial ticket for server node services.
     *
     * @throws LoginException
     */
    public synchronized static void createContext()
        throws LoginException {

        if (loginConfig == null) {
            throw new IllegalStateException("No Kerberos configuration found");
        }
        final String existingConfig =
            System.getProperty(KerberosConfig.getKrb5ConfigName());
        if (existingConfig != null &&
            !existingConfig.equals(loginConfig.getKrbConfig())) {
            throw new IllegalStateException(String.format(
                "Error specifying the location of the Kerberos " +
                "configuration file.  The service security parameters " +
                "specify the file %s, but the %s system property specifies %s",
                loginConfig.getKrbConfig(),
                KerberosConfig.getKrb5ConfigName(),
                existingConfig));
        }
        System.setProperty(KerberosConfig.getKrb5ConfigName(),
                           loginConfig.getKrbConfig());
        Subject subject = new Subject();
        final LoginContext login = new LoginContext(
             StoreKrbConfiguration.STORE_KERBEROS_CONFIG,
             subject, null /* callback handler */, loginConfig);
         login.login();
         currentContext = new KerberosContext(subject);
    }

    public static void setConfiguration(StoreKrbConfiguration conf) {
        loginConfig = conf;
    }

    public synchronized static void resetContext() {
        currentContext = null;
        loginConfig = null;
    }

    /**
     * Perform an action with the subject from this context as the current
     * subject. Returns the result of calling the action, and throws any
     * Exception or Error thrown by the action.
     *
     * @param <T> the return type
     * @param action the action to perform
     * @return the result of calling the action
     * @throws IllegalStateException if the action throws an exception that is
     * not an Exception or an Error
     * @throws Exception if the action throws an Exception
     */
    <T> T runWithContext(Callable<T> action) throws Exception {
        return runWithContext(subject, action);
    }

    /**
     * Perform an action with the specified subject as the current subject.
     * Returns the result of calling the action, and throws any Exception or
     * Error thrown by the action.
     *
     * @param <T> the return type
     * @param subject the subject for performing the action
     * @param action the action to perform
     * @return the result of calling the action
     * @throws IllegalStateException if the action throws an exception that is
     * not an Exception or an Error
     * @throws Exception if the action throws an Exception
     *
     * TODO: Replace with Subject::callAs directly after we switch to a Java 18
     * or later compilation target.
     */
    static <T> T runWithContext(Subject subject, Callable<T> action)
        throws Exception {

        return SubjectWrapper.callAs(subject, action);
    }

    /* For testing */
    Subject getSubject() {
        return subject;
    }

    /**
     * The wrapper class to access new Subject APIs. Since Java 23, tow Subject
     * APIs Subject::getSubject and Subject::doAs are deprecated for removal,
     * which require Java security manager to be allowed. They should be
     * migrated to Subject::current and Subject::callAs respectively, but both
     * are introduced in Java 18.
     *
     * When running with Java 18 or higher, this wrapper class use reflection
     * to call new APIs so that we don't have to compile this class with Java
     * 18. If they are not available, fall back to the old APIs.
     *
     * TODO: Update to use Subject API directly when our compilation target
     * changes to Java 18 or later.
     */
    private static class SubjectWrapper {
        private static final int SUBJECT_NEW_API_JAVA_VERSION = 18;
        private static volatile @Nullable Method current;
        private static volatile @Nullable Method callAs;

        /**
         * Make this method deprecated so that Eclipse does not warn about it
         * calling deprecated methods.
         */
        @Deprecated
        static Subject current() {
            if (isSupported()) {
                try {
                    final Method method = getCurrent();
                    return (Subject) method.invoke(null);
                } catch (InvocationTargetException e) {
                    throw handleException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Unexpected exception: " + e, e);
                }
            }

            return Subject.getSubject(
                java.security.AccessController.getContext());
        }

        /**
         * Make this method deprecated so that Eclipse does not warn about it
         * calling deprecated methods.
         */
        @Deprecated
        static <T> T callAs(Subject subject, Callable<T> action)
            throws Exception {

            if (isSupported()) {
                try {

                    final Method method = getCallAs();

                    @SuppressWarnings("unchecked")
                    final T result = (T) method.invoke(null, subject, action);
                    return result;
                } catch (InvocationTargetException e) {
                    if (e.getCause() instanceof CompletionException) {
                        final Throwable cause = e.getCause().getCause();
                        if (cause instanceof Error) {
                            throw (Error) cause;
                        } else if (cause instanceof Exception) {
                            throw (Exception) cause;
                        } else {
                            throw new IllegalStateException(
                                "Unexpected exception: " + cause, cause);
                        }
                    }
                    throw handleException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Unexpected exception: " + e, e);
                }
            }

            try {
                final PrivilegedExceptionAction<T> pa = action::call;
                return Subject.doAs(subject, pa);
            } catch (PrivilegedActionException pae) {
                final Throwable cause = pae.getCause();
                if (cause instanceof Error) {
                    throw (Error) cause;
                } else if (cause instanceof Exception) {
                    throw (Exception) cause;
                } else {
                    throw new IllegalStateException(
                        "Unexpected exception: " + cause, cause);
                }
            }
        }

        private static boolean isSupported() {
            return getJavaMajorVersion() >= SUBJECT_NEW_API_JAVA_VERSION;
        }

        private static Method getCurrent() {
            final @Nullable Method method = current;
            if (current != null) {
                return method;
            }
            try {
                final Method nonNullMethod =
                    Subject.class.getMethod("current");
                current = nonNullMethod;
                return nonNullMethod;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        private static Method getCallAs() {
            final @Nullable Method method = callAs;
            if (callAs != null) {
                return method;
            }

            try {
                final Method nonNullMethod =
                    Subject.class.getMethod(
                        "callAs", Subject.class, Callable.class);
                callAs = nonNullMethod;
                return nonNullMethod;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        private static
            RuntimeException handleException(InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof Error) {
                throw (Error) cause;
            } else if (cause instanceof RuntimeException) {
                return (RuntimeException) cause;
            } else {
                throw new IllegalStateException(
                    "Unexpected exception: " + cause, e);
            }
        }
    }
}
