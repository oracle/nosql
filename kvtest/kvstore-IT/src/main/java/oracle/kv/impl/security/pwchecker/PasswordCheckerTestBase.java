/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.pwchecker;

import java.security.SecureRandom;
import java.util.EnumSet;

import oracle.kv.TestBase;
import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.PasswordHash;
import oracle.kv.impl.security.metadata.PasswordHashDigest;

public class PasswordCheckerTestBase extends TestBase {

    private static final SecureRandom random = new SecureRandom();

    public PasswordHashDigest makeDefaultHashDigest(char[] plainPassword) {
        final byte[] saltValue =
            PasswordHash.generateSalt(random, PasswordHash.SUGG_SALT_BYTES);
        return PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                                PasswordHash.SUGG_HASH_ITERS,
                                                PasswordHash.SUGG_SALT_BYTES,
                                                saltValue, plainPassword);
    }

    public ParameterMap getDefaultParamMap() {
        ParameterMap map = new ParameterMap();
        final EnumSet<ParameterState.Info> set =
            EnumSet.of(ParameterState.Info.POLICY,
                       ParameterState.Info.SECURITY);
        for (ParameterState ps : ParameterState.getMap().values()) {
            if (ps.containsAll(set)) {
                map.put(DefaultParameter.getDefaultParameter(ps));
            }
        }
        return map;
    }

    public ParameterMap getClearRequirementMap() {
        final ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.SEC_PASSWORD_COMPLEXITY_CHECK, "true");
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LENGTH, "1");
        map.setParameter(ParameterState.SEC_PASSWORD_MAX_LENGTH, "256");
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_UPPER, "0");
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LOWER, "0");
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_DIGIT, "0");
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_SPECIAL, "0");
        map.setParameter(ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL,
            "'~@#$%~&*()_-+= {}[]/<>,.;?:|");
        map.setParameter(ParameterState.SEC_PASSWORD_PROHIBITED, "");
        map.setParameter(ParameterState.SEC_PASSWORD_REMEMBER, "0");
        map.setParameter(ParameterState.SEC_PASSWORD_NOT_USER_NAME, "false");
        map.setParameter(ParameterState.SEC_PASSWORD_NOT_STORE_NAME, "false");
        return map;
    }
}
