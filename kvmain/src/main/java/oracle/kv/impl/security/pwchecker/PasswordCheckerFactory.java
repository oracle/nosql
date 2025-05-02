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

package oracle.kv.impl.security.pwchecker;

import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.pwchecker.PasswordCheckerRule.*;

/**
 * Factory class to control the creation of instance of PassChecker interface.
 * The static implemented methods in the factory will in charge of:
 *
 * Input conversion
 * - Take the arbitrary inputs from the caller, convert to the specific rules
 * required.
 *
 * Build checker implementation
 * - Initialize the checker implementation, assembling rules into the checker.
 */
public class PasswordCheckerFactory {

    /**
     * Create checker for create user command.
     * @param map Inputs from policy parameters.
     * @param userName User name input.
     * @param storeName Store name input.
     * @return Implementation of password checker.
     */
    public static PasswordChecker
        createCreateUserPassChecker(ParameterMap map,
                                    String userName,
                                    String storeName) {
        return createCommonCheckerImpl(map, userName, storeName);
    }

    /**
     * Create checker for alter user command.
     * @param map Inputs from policy parameters.
     * @param storeName Store name input.
     * @param userMetadata User meta data which store the previous passwords
     * hash digests.
     * @return Implementation of password checker.
     */
    public static PasswordChecker
        createAlterUserPassChecker(ParameterMap map,
                                   String storeName,
                                   KVStoreUser userMetadata) {
        final PasswordCheckerImpl checkImpl =
            createCommonCheckerImpl(map, userMetadata.getName(), storeName);
        final int prevRemember =
            map.getOrDefault(ParameterState.SEC_PASSWORD_REMEMBER).asInt();
        if (prevRemember > 0) {
            checkImpl.addCheckerRule(new PasswordRemember(prevRemember,
                userMetadata.getRememberedPasswords(prevRemember)));
        }
        return checkImpl;
    }

    /*
     * Create common configurations for create user checker and
     * alter user checker.
     */
    private static PasswordCheckerImpl
        createCommonCheckerImpl(ParameterMap map,
                                String userName,
                                String storeName) {
        final PasswordCheckerImpl checkerImpl = new PasswordCheckerImpl();
        checkerImpl.addCheckerRule(new BasicCheckRule(map));

        if (map.getOrDefault(
            ParameterState.SEC_PASSWORD_NOT_USER_NAME).asBoolean()) {
            checkerImpl.addCheckerRule(new PasswordNotUserName(userName));
        }
        if (map.getOrDefault(
            ParameterState.SEC_PASSWORD_NOT_STORE_NAME).asBoolean()) {
            checkerImpl.addCheckerRule(new PasswordNotStoreName(storeName));
        }
        return checkerImpl;
    }
}
