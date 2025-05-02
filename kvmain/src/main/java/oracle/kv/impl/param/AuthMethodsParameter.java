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
package oracle.kv.impl.param;

import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.security.AuthenticatorManager;

/**
 * Authentication method parameters that can accept multiple values.
 *
 * Qualified string format is "authMethod1,authMethod2".
 */
public class AuthMethodsParameter extends Parameter {

    private static final long serialVersionUID = 1L;

    private static final String DELIMITER = ",";

    private static final String NONE = "NONE";

    private final String[] value;

    public AuthMethodsParameter(String name, String val) {
        super(name);
        value = parseAuthMethods(val);
    }

    public AuthMethodsParameter(String name, String[] value) {
        super(name);
        this.value = value;
    }

    public String[] asAuthMethods() {
        return value;
    }

    private String[] parseAuthMethods(String val) {
        String[] splitVal = val.split(DELIMITER);
        Set<String> authMethods = new HashSet<>();
        for (String v : splitVal) {
            final String inputValue = v.trim();
            if (!AuthenticatorManager.isSupported(inputValue) &&
                !NONE.equalsIgnoreCase(inputValue)) {
                throw new IllegalArgumentException(
                    "Unsupported value of authentication method");
            }
            authMethods.add(inputValue.toUpperCase());
        }
        if (authMethods.size() > 1) {
            for (String authMethod : authMethods) {
                if (authMethod.equalsIgnoreCase("NONE")) {
                    throw new IllegalArgumentException(
                        "Cannot set NONE with other auth method");
                }
            }
        }
        return authMethods.toArray(new String[authMethods.size()]);
    }

    @Override
    public String asString() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length; i++) {
            if (i != 0) {
                sb.append(DELIMITER);
            }
            sb.append(value[i]);
        }
        return sb.toString();
    }

    @Override
    public ParameterState.Type getType() {
        return ParameterState.Type.AUTHMETHODS;
    }
}
