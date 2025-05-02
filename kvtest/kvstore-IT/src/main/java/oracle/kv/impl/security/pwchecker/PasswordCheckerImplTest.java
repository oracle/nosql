/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.pwchecker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.metadata.PasswordHashDigest;
import oracle.kv.impl.security.pwchecker.PasswordCheckerRule.*;

import org.junit.Test;

public class PasswordCheckerImplTest extends PasswordCheckerTestBase {

    @Test
    public void testPasswordCheckerImpl() {
        /*
         * No rules
         */
        PasswordCheckerImpl checker = new PasswordCheckerImpl();
        PasswordCheckerResult result =
            checker.checkPassword("123456".toCharArray());
        assertTrue(result.isPassed());
        assertEquals(result.getMessage(), "");

        /*
         * No null password
         */
        checker = new PasswordCheckerImpl();
        try {
            result = checker.checkPassword(null);
            fail("Should throw IAE");
        } catch (IllegalArgumentException iae) {
            /* Expected */
            assertEquals(iae.getMessage(),
                "The password must be non-null and not empty");
        }

        /*
         * No empty password
         */
        checker = new PasswordCheckerImpl();
        try {
            result = checker.checkPassword("".toCharArray());
            fail("Should throw IAE");
        } catch (IllegalArgumentException iae) {
            /* Expected */
            assertEquals(iae.getMessage(),
                "The password must be non-null and not empty");
        }

        final int maxLenConfigValue = 1;
        ParameterMap map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MAX_LENGTH,
            "" + maxLenConfigValue);
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new BasicCheckRule(map));
        result = checker.checkPassword("123456".toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must have at most " +
            maxLenConfigValue + " characters");

        final int minLenConfigValue = 2;
        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LENGTH,
            "" + minLenConfigValue);
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new BasicCheckRule(map));
        result = checker.checkPassword("1".toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must have at least " +
            minLenConfigValue + " characters");

        final int minUpperConfigValue = 2;
        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_UPPER,
            "" + minUpperConfigValue);
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new BasicCheckRule(map));
        result = checker.checkPassword("U".toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must have at least " +
            minUpperConfigValue + " upper case letters");

        final int minLowerConfigValue = 2;
        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LOWER,
            "" + minLowerConfigValue);
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new BasicCheckRule(map));
        result = checker.checkPassword("l".toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must have at least " +
            minLowerConfigValue + " lower case letters");

        final int minDigitConfigValue = 2;
        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_DIGIT,
            "" + minDigitConfigValue);
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new BasicCheckRule(map));
        result = checker.checkPassword("1".toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must have at least " +
            minDigitConfigValue + " digit numbers");

        final int minSpecialConfigValue = 4;
        final String allowedSpecial = "_-+=";
        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_SPECIAL,
            "" + minSpecialConfigValue);
        map.setParameter(ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL,
            allowedSpecial);
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new BasicCheckRule(map));
        result = checker.checkPassword("-_-".toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must have at least " +
            minSpecialConfigValue + " special characters");

        final String prohibited = "a,b,c";
        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_PROHIBITED, prohibited);
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new BasicCheckRule(map));
        result = checker.checkPassword("a".toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must not be the word " +
            "in the prohibited list");

        final int remember = 3;
        final PasswordHashDigest[] phdArray = {
            makeDefaultHashDigest("prev1".toCharArray()),
            makeDefaultHashDigest("prev2".toCharArray()),
            makeDefaultHashDigest("prev3".toCharArray())
        };
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new PasswordRemember(remember, phdArray));
        result = checker.checkPassword("prev1".toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must not be one of the " +
            "previous " + remember + " remembered passwords");
        result = checker.checkPassword("prev4".toCharArray());
        assertTrue(result.isPassed());
        assertEquals(result.getMessage(), "");

        final String userName = "user";
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new PasswordNotUserName(userName));
        result = checker.checkPassword(userName.toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must not be the " +
            "same as the user name, the user name reversed, or the user " +
            "name with the numbers 1-100 appended.");

        final String storeName = "store";
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new PasswordNotStoreName(storeName));
        result = checker.checkPassword(storeName.toCharArray());
        assertFalse(result.isPassed());
        assertEquals(result.getMessage(), "\n  Password must not be the " +
            "same as the store name, the store name reversed, or the store " +
            "name with the numbers 1-100 appended.");

        /*
         * Test multiple rules
         */
        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LENGTH,
            "" + minLenConfigValue);
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_UPPER,
            "" + minUpperConfigValue);
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LOWER,
            "" + minLowerConfigValue);
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_DIGIT,
            "" + minDigitConfigValue);
        checker = new PasswordCheckerImpl();
        checker.addCheckerRule(new BasicCheckRule(map));
        result = checker.checkPassword("_".toCharArray());
        assertFalse(result.isPassed());
        String finalMessage = "\n  Password must have at least " +
            minLenConfigValue + " characters\n  Password must have at least " +
            minUpperConfigValue + " upper case letters\n  Password must have " +
            "at least " + minLowerConfigValue + " lower case letters" +
            "\n  Password must have at least " + minDigitConfigValue +
            " digit numbers";
        assertEquals(result.getMessage(), finalMessage);
    }
}
