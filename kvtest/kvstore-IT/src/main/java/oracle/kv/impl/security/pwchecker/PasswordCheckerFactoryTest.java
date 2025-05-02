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
import oracle.kv.impl.security.metadata.KVStoreUser;

import org.junit.Test;

public class PasswordCheckerFactoryTest extends PasswordCheckerTestBase {

    private static final String userName = "testUser";
    private static final String userNameReverse = "resUtset";
    private static final String storeName = "testStore";
    private static final String storeNameReverse = "erotStset";

    @Test
    public void testCreateCreateUserPwChecker() {
        ParameterMap map = getDefaultParamMap();
        PasswordChecker checker =
            PasswordCheckerFactory.createCreateUserPassChecker(map, userName,
                storeName);
        commonCheckerCheck(checker);
    }

    @Test
    public void testCreateAlterUserPwChecker() {
        ParameterMap map = getDefaultParamMap();
        final KVStoreUser newUser = KVStoreUser.newInstance(userName);
        newUser.setPassword(makeDefaultHashDigest("NoSql00__1".toCharArray()));
        newUser.setPassword(makeDefaultHashDigest("NoSql00__2".toCharArray()));
        newUser.setPassword(makeDefaultHashDigest("NoSql00__3".toCharArray()));
        PasswordChecker checker =
            PasswordCheckerFactory.createAlterUserPassChecker(map,
                                                              storeName,
                                                              newUser);
        commonCheckerCheck(checker);
        PasswordCheckerResult result =
            checker.checkPassword("NoSql00__1".toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be one of the previous"));
        result = checker.checkPassword("NoSql00__2".toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be one of the previous"));
        result = checker.checkPassword("NoSql00__3".toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be one of the previous"));
        result = checker.checkPassword("NoSql00__4".toCharArray());
        assertTrue(result.isPassed());
        assertEquals(result.getMessage(), "");
    }

    private void commonCheckerCheck(PasswordChecker checker) {
        PasswordCheckerResult result =
            checker.checkPassword("NoSql00__123".toCharArray());
        assertTrue(result.isPassed());
        assertEquals(result.getMessage(), "");

        try {
            result = checker.checkPassword(null);
            fail("Should throw IAE");
        } catch (IllegalArgumentException iae) {
            /* Expected */
            assertEquals(iae.getMessage(),
                "The password must be non-null and not empty");
        }

        try {
            result = checker.checkPassword("".toCharArray());
            fail("Should throw IAE");
        } catch (IllegalArgumentException iae) {
            /* Expected */
            assertEquals(iae.getMessage(),
                "The password must be non-null and not empty");
        }

        result = checker.checkPassword(
            ("Password_Length_Exceeds_The_Default_Max_256_" +
             "----------------------------------------------------" +
             "----------------------------------------------------" +
             "----------------------------------------------------" +
             "----------------------------------------------------" +
             "-----------------------").toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains("Password must have at most"));

        result = checker.checkPassword(userName.toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be the same as"));

        result = checker.checkPassword(userName.toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be the same as"));

        result = checker.checkPassword(userNameReverse.toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be the same as"));

        result = checker.checkPassword((userName + "99").toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be the same as"));

        result = checker.checkPassword(storeName.toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be the same as"));

        result = checker.checkPassword(storeNameReverse.toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be the same as"));

        result = checker.checkPassword((storeName + "99").toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be the same as"));

        result = checker.checkPassword("nosql".toCharArray());
        assertFalse(result.isPassed());
        assertTrue(result.getMessage().contains(
            "Password must not be the word in the prohibited list"));
    }
}
