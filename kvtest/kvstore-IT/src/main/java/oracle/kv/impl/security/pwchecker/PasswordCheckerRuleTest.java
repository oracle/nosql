/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.pwchecker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.metadata.PasswordHashDigest;
import oracle.kv.impl.security.pwchecker.PasswordCheckerRule.*;

import org.junit.Test;

public class PasswordCheckerRuleTest extends PasswordCheckerTestBase {

    @Test
    public void testBasicCheckRule() {
        ParameterMap map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LENGTH, "1");
        PasswordCheckerRule pcr = new BasicCheckRule(map);
        assertTrue(pcr.checkPassword("123".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LENGTH, "100");
        assertFalse(pcr.checkPassword("123".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LENGTH, "10");
        assertFalse(pcr.checkPassword("aa".toCharArray()).isPassed());
        assertTrue(pcr.checkPassword(
            "aaaaaaaaaaa".toCharArray()).isPassed());

        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MAX_LENGTH, "1");
        pcr = new BasicCheckRule(map);
        assertFalse(pcr.checkPassword("123".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MAX_LENGTH, "100");
        assertTrue(pcr.checkPassword("123".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MAX_LENGTH, "10");
        assertTrue(pcr.checkPassword("123".toCharArray()).isPassed());
        assertFalse(pcr.checkPassword(
            "aaaaaaaaaaaa".toCharArray()).isPassed());

        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_UPPER, "0");
        pcr = new BasicCheckRule(map);
        assertTrue(pcr.checkPassword("12__dsds".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_UPPER, "100");
        assertFalse(pcr.checkPassword("12__dsds".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_UPPER, "10");
        assertTrue(pcr.checkPassword(
            "AUASDFGSDD1213_+_+dsdsa".toCharArray()).isPassed());
        assertFalse(pcr.checkPassword(
            "AUAS1213_+_+dsdsa".toCharArray()).isPassed());

        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LOWER, "0");
        pcr = new BasicCheckRule(map);
        assertTrue(pcr.checkPassword("12__DSDS".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LOWER, "100");
        assertFalse(pcr.checkPassword("sdsdavas".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_LOWER, "10");
        assertTrue(pcr.checkPassword(
            "aaaasdsdsds___TTDDD".toCharArray()).isPassed());
        assertFalse(pcr.checkPassword(
            "aaa___TTDDSDSDDD".toCharArray()).isPassed());

        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_DIGIT, "0");
        pcr = new BasicCheckRule(map);
        assertTrue(pcr.checkPassword("aaa__DSDS".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_DIGIT, "100");
        assertFalse(pcr.checkPassword(
            "123124235425534".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_DIGIT, "10");
        assertTrue(pcr.checkPassword(
            "12345678111__DSDSdsd".toCharArray()).isPassed());
        assertFalse(pcr.checkPassword(
            "123456789__dsdsdDDD".toCharArray()).isPassed());

        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_SPECIAL, "1");
        map.setParameter(ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL, "");
        pcr = new BasicCheckRule(map);
        assertFalse(pcr.checkPassword("".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_SPECIAL, "3");
        map.setParameter(ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL, "__++==");
        assertTrue(pcr.checkPassword("===".toCharArray()).isPassed());
        assertFalse(pcr.checkPassword("_+".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_SPECIAL, "0");
        map.setParameter(ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL,
            "'~@#$%~&*()_-+={}[]/<>,.;?:| ");
        assertTrue(pcr.checkPassword(" ".toCharArray()).isPassed());
        map.setParameter(ParameterState.SEC_PASSWORD_MIN_SPECIAL, "5");
        map.setParameter(ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL,
            "!'~@#$%~&*()_-+={}[]/<>,.;?:| ");
        assertTrue(pcr.checkPassword(
            "!'~@#$%~&*()_-+={}[]/<>,.;?:| ".toCharArray()).isPassed());
        assertFalse(pcr.checkPassword(
            "    ".toCharArray()).isPassed());
        assertTrue(pcr.checkPassword(
            "     ".toCharArray()).isPassed());
        assertTrue(pcr.checkPassword(
            "'~@#$%".toCharArray()).isPassed());

        String words = "nosql,oracle,123456";
        map = getClearRequirementMap();
        map.setParameter(ParameterState.SEC_PASSWORD_PROHIBITED, words);
        pcr = new BasicCheckRule(map);
        assertTrue(pcr.checkPassword("nosql1".toCharArray()).isPassed());
        assertTrue(pcr.checkPassword("oracle1".toCharArray()).isPassed());
        assertTrue(pcr.checkPassword("1123456".toCharArray()).isPassed());
        assertTrue(pcr.checkPassword(" ".toCharArray()).isPassed());
        assertFalse(pcr.checkPassword("nosql".toCharArray()).isPassed());
        assertFalse(pcr.checkPassword("oracle".toCharArray()).isPassed());
        assertFalse(pcr.checkPassword("123456".toCharArray()).isPassed());
        words = "oracle 123456";
        map.setParameter(ParameterState.SEC_PASSWORD_PROHIBITED, words);
        pcr = new BasicCheckRule(map);
        assertTrue(pcr.checkPassword(" ".toCharArray()).isPassed());
        assertTrue(pcr.checkPassword("nosql".toCharArray()).isPassed());
        words = "";
        map.setParameter(ParameterState.SEC_PASSWORD_PROHIBITED, words);
        pcr = new BasicCheckRule(map);
        assertTrue(pcr.checkPassword("nosql".toCharArray()).isPassed());
        assertTrue(pcr.checkPassword("oracle".toCharArray()).isPassed());
        assertTrue(pcr.checkPassword("123456".toCharArray()).isPassed());
    }

    @Test
    public void testPasswordRememberRule() {
        PasswordRemember prr =
            new PasswordRemember(0, new PasswordHashDigest[0]);
        assertTrue(prr.checkPassword("".toCharArray()).isPassed());
        assertTrue(prr.checkPassword("abc123".toCharArray()).isPassed());
        /*
         * No check apply
         */
        prr = new PasswordRemember(3, null);
        assertTrue(prr.checkPassword("abc123".toCharArray()).isPassed());

        PasswordHashDigest[] phdArray = {
            makeDefaultHashDigest("1".toCharArray()),
            makeDefaultHashDigest("2".toCharArray()),
            makeDefaultHashDigest("3".toCharArray())
        };
        prr = new PasswordRemember(3, phdArray);
        assertTrue(prr.checkPassword("4".toCharArray()).isPassed());
        assertFalse(prr.checkPassword("1".toCharArray()).isPassed());
        assertFalse(prr.checkPassword("2".toCharArray()).isPassed());
        assertFalse(prr.checkPassword("3".toCharArray()).isPassed());
    }

    @Test
    public void testPasswordNotUserNameRule() {
        PasswordNotUserName rule = new PasswordNotUserName("user");
        assertTrue(rule.checkPassword(
            "NotUserName".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "user".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "resu".toCharArray()).isPassed());
        assertTrue(rule.checkPassword(
            "user111".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "user100".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "user1".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "user99".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "user001".toCharArray()).isPassed());
        assertTrue(rule.checkPassword(
            "user000".toCharArray()).isPassed());
        assertTrue(rule.checkPassword(
            "user00".toCharArray()).isPassed());
        assertTrue(rule.checkPassword(
            "user0".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "user010".toCharArray()).isPassed());
    }

    @Test
    public void testPasswordNotStoreNameRule() {
        PasswordNotStoreName rule = new PasswordNotStoreName("store");
        assertTrue(rule.checkPassword(
            "NotStoreName".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "store".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "erots".toCharArray()).isPassed());
        assertTrue(rule.checkPassword(
            "store111".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "store100".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "store1".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "store99".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "store001".toCharArray()).isPassed());
        assertTrue(rule.checkPassword(
            "store000".toCharArray()).isPassed());
        assertTrue(rule.checkPassword(
            "store00".toCharArray()).isPassed());
        assertTrue(rule.checkPassword(
            "store0".toCharArray()).isPassed());
        assertFalse(rule.checkPassword(
            "store010".toCharArray()).isPassed());
    }
}
