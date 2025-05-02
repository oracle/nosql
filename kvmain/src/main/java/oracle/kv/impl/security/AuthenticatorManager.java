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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;

/**
 * The class manages all implemented authenticator managers, which provides
 * ability to validate and locate authenticator factory of given authentication
 * method.
 */
public class AuthenticatorManager {

    /**
     * The name of the Kerberos authenticator factory. This is available only
     * in the NoSQL DB EE version.
     */
    public static final String KERBEROS_AUTHENTICATOR_FACTORY_CLASS =
        "oracle.kv.impl.security.kerberos.KerberosAuthFactory";

    /**
     * The name of the IDCS OAuth authenticator factory.
     */
    public static final String IDCS_OAUTH_AUTHENTICATOR_FACTORY_CLASS =
        "oracle.kv.impl.security.oauth.IDCSOAuthAuthFactory";

    /**
     * List of system implemented authenticator factories.
     */
    private static final Map<String, String> systemImplementations =
        new HashMap<>();

    static {
        systemImplementations.put(SystemAuthMethod.KERBEROS.name(),
                                  KERBEROS_AUTHENTICATOR_FACTORY_CLASS);
        systemImplementations.put(SystemAuthMethod.IDCSOAUTH.name(),
                                  IDCS_OAUTH_AUTHENTICATOR_FACTORY_CLASS);
    }

    /* not instantiable */
    private AuthenticatorManager() {
    }

    /**
     * Attempt to load an AuthenticatorFactory instance of the specified
     * authentication method name. Note that given authentication name is
     * case insensitive and currently only accept values of SystemAuthMethod.
     *
     * @param authMethod the name of the authentication method
     * @param secParams security parameters used to instantiate authenticator
     * @param globalParams global parameters used to instantiate authenticator
     * @return an instance of AuthenticatorManager
     * @throws ClassCastException if the class does not implement
     *         AuthenticationFactory
     * @throws ClassNotFoundException if the corresponding class of
     *         authentication method cannot be found
     * @throws ExceptionInInitializerError if an exception is thrown by class
     *         initialization
     * @throws IllegalAccessException if the class or default constructor
     *         are inaccessible
     * @throws InstantiationException if the class has no default constructor
     *         or is not an instantiable class.
     * @throws InvocationTargetException if the constructor throws an exception
     * @throws NoSuchMethodException if the constructor is not found
     */
    public static Authenticator getAuthenticator(String authMethod,
                                                 SecurityParams secParams,
                                                 GlobalParams globalParams)
        throws ClassCastException, ClassNotFoundException,
               IllegalAccessException, InstantiationException,
               InvocationTargetException, NoSuchMethodException {

        final Class<? extends AuthenticatorFactory> authClass =
            findAuthenticatorFactory(authMethod);
        final AuthenticatorFactory factory =
            authClass.getDeclaredConstructor().newInstance();
        return factory.getAuthenticator(secParams, globalParams);
    }

    /**
     * Find class name from system implemented authenticator factory list with
     * specified authentication method and load corresponding class.
     *
     * @param authMethod authentication method that is case-insensitive.
     * @return a class of authenticator factory
     *
     * @throws ClassNotFoundException if the corresponding class cannot be found
     * @throws IllegalArgumentException if specified authentication method
     *         cannot be found
     * @throws ClassCastException if found Class object does not represent a
     *         subclass of AuthenticatorFactory
     */
    private static Class<? extends AuthenticatorFactory>
        findAuthenticatorFactory(String authMethod)
        throws ClassNotFoundException, IllegalArgumentException,
               ClassCastException {

        final String className = systemImplementations.get(
            authMethod.toUpperCase(Locale.ENGLISH));

        if (className == null) {
            throw new IllegalArgumentException(
                "The authentication method " + authMethod + " is not found.");
        }

        return Class.forName(className).asSubclass(AuthenticatorFactory.class);
    }

    /**
     * Check if specified authentication method is supported.
     *
     * @param authMethod authentication method that is case-insensitive.
     * @return true if authentication method is supported.
     */
    public static boolean isSupported(String authMethod) {
        if (noneAuthMethod(authMethod)) {
            return true;
        }
        try {
            AuthenticatorManager.findAuthenticatorFactory(authMethod);
        } catch (ClassNotFoundException cnfe) {
            return false;
        }
        return true;
    }

    /**
     * Check if specified authentication method name is valid.
     *
     * @param authMethod authentication method name that is case-insensitive.
     * @return true if the method name is valid
     */
    public static boolean isValidAuthMethod(String authMethod) {
        try {
            Enum.valueOf(SystemAuthMethod.class,
                         authMethod.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException iae) {
            return false;
        }
        return true;
    }

    /**
     * Case-insensitive check if specified authentication method is NONE.
     *
     * @return true if given value is 'NONE'
     */
    public static boolean noneAuthMethod(String authMethod) {
        return "NONE".equals(authMethod.toUpperCase(Locale.ENGLISH));
    }

    /**
     * System-defined authenticator.
     */
    public static enum SystemAuthMethod {

        KERBEROS,

        IDCSOAUTH;
    }
}
