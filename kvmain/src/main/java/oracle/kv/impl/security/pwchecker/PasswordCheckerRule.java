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

import java.util.Arrays;

import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.metadata.PasswordHashDigest;

/**
 * This class hosts all the password checker rule implementations.
 *
 * Each rule defines the logic of the check, the required input parameters,
 * and the result message.
 */
public abstract class PasswordCheckerRule {

    /**
     * Basic function for each rule, check the password according to the rule.
     */
    public abstract PasswordCheckerResult checkPassword(char[] password);

    /**
     * Check the passwords should meet all the common requirements from the
     * parameter map.
     */
    static class BasicCheckRule extends PasswordCheckerRule {

        protected final ParameterMap map;

        public BasicCheckRule(ParameterMap map) {
            this.map = map;
        }

        @Override
        public PasswordCheckerResult checkPassword(char[] password) {

            final StringBuilder sb = new StringBuilder();
            boolean isCheckPass = true;
            int minValue = 0;
            int maxValue = 0;
            int counter = 0;

            /*
             * Check the minimum length requirement.
             */
            minValue = map.getOrDefault(
                ParameterState.SEC_PASSWORD_MIN_LENGTH).asInt();
            if (password.length < minValue) {
                isCheckPass = false;
                sb.append("\n  Password must have at least " +
                    minValue + " characters");
            }

            /*
             * Check the maximum length requirement.
             */
            maxValue = map.getOrDefault(
                ParameterState.SEC_PASSWORD_MAX_LENGTH).asInt();
            if(password.length > maxValue) {
                isCheckPass = false;
                sb.append("\n  Password must have at most " +
                    maxValue + " characters");
            }

            /*
             * Check the minimum upper case letters requirement.
             */
            minValue = map.getOrDefault(
                ParameterState.SEC_PASSWORD_MIN_UPPER).asInt();
            counter = 0;
            for (char c : password) {
                if (Character.isUpperCase(c)) {
                    counter ++;
                }
            }
            if(counter < minValue) {
                isCheckPass = false;
                sb.append("\n  Password must have at least " +
                    minValue + " upper case letters");
            }

            /*
             * Check the minimum lower case letters requirement.
             */
            minValue = map.getOrDefault(
                ParameterState.SEC_PASSWORD_MIN_LOWER).asInt();
            counter = 0;
            for (char c : password) {
                if (Character.isLowerCase(c)) {
                    counter ++;
                }
            }
            if(counter < minValue) {
                isCheckPass = false;
                sb.append("\n  Password must have at least " +
                    minValue + " lower case letters");
            }

            /*
             * Check the minimum digit number requirement.
             */
            minValue = map.getOrDefault(
                ParameterState.SEC_PASSWORD_MIN_DIGIT).asInt();
            counter = 0;
            for (char c : password) {
                if (Character.isDigit(c)) {
                    counter ++;
                }
            }
            if(counter < minValue) {
                isCheckPass = false;
                sb.append("\n  Password must have at least " +
                    minValue + " digit numbers");
            }

            /*
             * Check the minimum special letters requirement.
             */
            minValue = map.getOrDefault(
                ParameterState.SEC_PASSWORD_MIN_SPECIAL).asInt();
            final String allowedSpecial = map.getOrDefault(
                ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL).asString();
            counter = 0;
            for (char c : password) {
                if (allowedSpecial.indexOf(c) != -1) {
                    counter++;
                }
            }
            if(counter < minValue) {
                isCheckPass = false;
                sb.append("\n  Password must have at least " +
                    minValue + " special characters");
            }

            /*
             * Check the prohibited words requirement.
             */
            final String prohibited = map.getOrDefault(
                ParameterState.SEC_PASSWORD_PROHIBITED).asString();

            if (prohibited != null && !prohibited.isEmpty()) {
                for (String word: prohibited.split(",")) {
                    if (Arrays.equals(word.toCharArray(), password)) {
                        isCheckPass = false;
                        sb.append("\n  Password must not be the word in the " +
                            "prohibited list");
                    }
                }
            }

            return new PasswordCheckerResult(isCheckPass, sb.toString());
        }
    }

    /**
     * Check whether the password is one of the previous recorded password.
     * The comparison is based on password hash digest.
     */
    static class PasswordRemember extends PasswordCheckerRule {

        /**
         * How many previous used password will be remembered in this check.
         */
        private final int preRemember;

        /**
         * Array contains the previous password's hash digests.
         */
        private final PasswordHashDigest[] phdArray;

        public PasswordRemember(int remember, PasswordHashDigest[] phdArray) {
            this.preRemember = remember;
            this.phdArray = phdArray;
        }

        @Override
        public PasswordCheckerResult checkPassword(char[] password) {

            if (phdArray == null) {
                return new PasswordCheckerResult(true, null);
            }

            for (PasswordHashDigest phd : phdArray) {
                if (phd.verifyPassword(password)) {
                    return new PasswordCheckerResult(
                        false,
                        "\n  Password must not be one of the previous " +
                        preRemember + " remembered passwords");
                }
            }
            return new PasswordCheckerResult(true, null);
        }
    }

    /**
     * Check that the password should not be the same as specific name, nor it
     * is the name spelled backward or with the numbers 1-100 appended.
     */
    static abstract class PasswordNameCheck extends PasswordCheckerRule {

        private final char[] name;

        public PasswordNameCheck(String stringValue) {
            name = stringValue.toCharArray();
        }

        @Override
        public PasswordCheckerResult checkPassword(char[] password) {

            if (password.length == name.length) {
                final char[] backward = new char[name.length];
                for (int i = 0; i < name.length; i++) {
                    backward[i] = name[name.length - i - 1];
                }
                if (Arrays.equals(name, password) ||
                    Arrays.equals(backward, password)) {
                    return new PasswordCheckerResult(false, getViolation());
                }
            }

            final int suffixLength = password.length - name.length;
            if ((suffixLength >= 1) && (suffixLength <= 3)) {
                boolean isPrefix = true;
                for (int i = 0; i < name.length; i++) {
                    if (password[i] != name[i]) {
                        isPrefix= false;
                        break;
                    }
                }
                if (isPrefix) {
                    int suffix = 0;
                    for (int i = 0; i < suffixLength; i++) {
                        final char c = password[name.length + i];
                        if (!Character.isDigit(c)) {
                            suffix = -1;
                            break;
                        }
                        suffix = (suffix * 10) + Character.digit(c, 10);
                    }
                    if (suffix >= 1 && suffix <= 100) {
                        return new PasswordCheckerResult(
                            false, getViolation());
                    }
                }
            }

            return new PasswordCheckerResult(true, null);
        }

        abstract public String getViolation();
    }

    /**
     * Check that the password should not be the same as user name, nor it
     * is the name spelled backward or with the numbers 1-100 appended.
     *
     * In this checker rule first we check that the password is not set to be
     * the same as user name. The next, the rule check that the password is not
     * the same as the user name spelled backward. At last, if the user name is
     * a prefix of the password. This rule will check that whether the
     * password is a user name with number appended. The range of the number
     * is from 001 to 100.
     */
    public static class PasswordNotUserName extends PasswordNameCheck {
        
        public PasswordNotUserName(String userName) {
            super(userName);
        }

        @Override
        public String getViolation() {
            return "\n  Password must not be the same as the user name, the" +
                " user name reversed, or the user name with the numbers" +
                " 1-100 appended.";
        }
    }

    /**
     * Check that the password should not be the same as store name, nor it
     * is the name spelled backward or with the numbers 1-100 appended.
     *
     * In this checker rule first we check that the password is not set to be
     * the same as store name. The next, the rule check that the password is
     * not the same as the store name spelled backward. At last, if the store
     * name is a prefix of the password. This rule will check that whether the
     * password is a store name with number appended. The range of the number
     * is from 001 to 100.
     */
    public static class PasswordNotStoreName extends PasswordNameCheck {

        public PasswordNotStoreName(String storeName) {
            super(storeName);
        }

        @Override
        public String getViolation() {
            return "\n  Password must not be the same as the store name, the" +
                " store name reversed, or the store name with the numbers" +
                " 1-100 appended.";
        }
    }
}
