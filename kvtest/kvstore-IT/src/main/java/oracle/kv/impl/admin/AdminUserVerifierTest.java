/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.admin;

import static org.junit.Assert.assertNull;

import java.io.DataInput;
import java.io.DataOutput;

import javax.security.auth.Subject;

import oracle.kv.LoginCredentials;
import oracle.kv.LoginCredentials.LoginCredentialsType;
import oracle.kv.LoginCredentialsTypeFinders;
import oracle.kv.TestBase;

import org.junit.Test;

public class AdminUserVerifierTest extends TestBase {

    @Test
    public void testVerifyUserWithOddCredentials() {
        AdminUserVerifier auv = new AdminUserVerifier(null);
        Subject result = auv.verifyUser(new OddCredentials(), null);
        assertNull(result);
    }

    //TODO: Adding cases covering more login cases

    private static class OddCredentialsType implements LoginCredentialsType {
        private static final int INT_VALUE = 123;
        private static final OddCredentialsType TYPE =
            new OddCredentialsType();
        static {
            LoginCredentialsTypeFinders.addFinder(
                OddCredentialsType::getType);
        }
        static OddCredentialsType getType(int intValue) {
            return (intValue == INT_VALUE) ? TYPE : null;
        }
        @Override
        public int getIntValue() {
            return INT_VALUE;
        }
        @Override
        public LoginCredentials readLoginCredentials(DataInput in, short sv) {
            return new OddCredentials();
        }
    }

    private static class OddCredentials implements LoginCredentials {
        @Override
        public String getUsername() {
            return "Odd";
        }
        @Override
        public void writeFastExternal(DataOutput out, short sv) { }
        @Override
        public LoginCredentialsType getLoginCredentialsType() {
            return OddCredentialsType.TYPE;
        }
    }
}
