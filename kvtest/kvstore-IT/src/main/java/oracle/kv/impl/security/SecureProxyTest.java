/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.rmi.Remote;
import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.TestBase;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.TestProcessFaultHandler;
import oracle.kv.impl.security.annotations.PublicAPI;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureInternalMethod;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Test the SecureProxy API.
 */
public class SecureProxyTest extends TestBase {

    static SPFH spfh;

    static {
        spfh = new SPFH(Logger.getLogger("SecureProxyTest"));
    }

    /**
     * Test that error is thrown by proxy creation when the proxied object
     * has class-level annotation, but no proxyable methods.
     */
    @Test
    public void testNoProxyableMethods()
        throws Exception {

        final NoProxyableMethods obj = new NoProxyableMethods();

        try {
            SecureProxy.create(obj, null, spfh);
            fail("expected exception");
        } catch (ConfigurationException e) {
            if (TestUtils.testDebugEnabled()) {
                /* allow the mesages to be reviewed for understandability */
                System.out.println("testNoProxyableMethods: " + e.getMessage());
            }
        }
    }

    /**
     * Test that error is thrown by proxy creation when the proxied object
     * has no class-level security annotation.
     */
    @Test
    public void testNoClassAnnotation()
        throws Exception {

        final NoClassAnnotation obj = new NoClassAnnotation();

        try {
            SecureProxy.create(obj, null, spfh);
            fail("expected exception");
        } catch (ConfigurationException e) {
            if (TestUtils.testDebugEnabled()) {
                /* allow the mesages to be reviewed for understandability */
                System.out.println("testNoClassAnnotation: " + e.getMessage());
            }
        }
    }

    /**
     * Test that error is thrown by proxy creation when the proxied object
     * has interface methods that are not marked with method-level annotation.
     */
    @Test
    public void testNoMethodAnnotation()
        throws Exception {

        final NoMethodAnnotation obj = new NoMethodAnnotation();

        try {
            SecureProxy.create(obj, null, spfh);
            fail("expected exception");
        } catch (ConfigurationException e) {
            if (TestUtils.testDebugEnabled()) {
                /* allow the mesages to be reviewed for understandability */
                System.out.println("testNoMethodAnnotation: " + e.getMessage());
            }
        }
    }

    /**
     * Test that error is thrown by proxy creation when the proxied object
     * has an annotated method that is missing a required AuthContext argument.
     */
    @Test
    public void testNoAuthContext()
        throws Exception {

        final NoAuthContext obj = new NoAuthContext();

        try {
            SecureProxy.create(obj, null, spfh);
            fail("expected exception");
        } catch (ConfigurationException e) {
            if (TestUtils.testDebugEnabled()) {
                /* allow the mesages to be reviewed for understandability */
                System.out.println("testAuthContext: " + e.getMessage());
            }
        }
    }

    /**
     * Test that error is thrown by proxy creation when the proxied object
     * has multiple class-level annotations.
     */
    @Test
    public void testMultipleClassAnnotations()
        throws Exception {

        final MultipleClassAnnotations obj = new MultipleClassAnnotations();

        try {
            SecureProxy.create(obj, null, spfh);
            fail("expected exception");
        } catch (ConfigurationException e) {
            if (TestUtils.testDebugEnabled()) {
            /* allow the mesages to be reviewed for understandability */
                System.out.println("testMultipleClassAnnotations: " +
                                   e.getMessage());
            }
        }
    }

    /**
     * Test that error is thrown by proxy creation when the proxied object
     * has multiple method-level annotations.
     */
    @Test
    public void testMultipleMethodAnnotations()
        throws Exception {

        final MultipleMethodAnnotations obj = new MultipleMethodAnnotations();

        try {
            SecureProxy.create(obj, null, spfh);
            fail("expected exception");
        } catch (ConfigurationException e) {
            if (TestUtils.testDebugEnabled()) {
                /* allow the mesages to be reviewed for understandability */
                System.out.println("testMultipleMethodAnnotations: " +
                                   e.getMessage());
            }
        }
    }

    /**
     * Test that error is thrown by proxy creation when the proxied object
     * has method-level annotations on non-interface methods.
     */
    @Test
    public void testNonInterfaceMethodAnnotations()
        throws Exception {

        final NonInterfaceMethodAnnotation obj =
            new NonInterfaceMethodAnnotation();

        try {
            SecureProxy.create(obj, null, spfh);
            fail("expected exception");
        } catch (ConfigurationException e) {
            if (TestUtils.testDebugEnabled()) {
                /* allow the mesages to be reviewed for understandability */
                System.out.println("testNonInterfaceMethodAnnotations: " +
                                   e.getMessage());
            }
        }
    }

    /**
     * Test that error is thrown by proxy creation when the proxied object
     * has a method marked as SecureAutoMethod but has an empty roles list.
     */
    @Test
    public void testEmptyRoleList()
        throws Exception {

        final EmptyRoleList obj = new EmptyRoleList();

        try {
            SecureProxy.create(obj, null, spfh);
            fail("expected exception");
        } catch (ConfigurationException e) {
            if (TestUtils.testDebugEnabled()) {
                /* allow the mesages to be reviewed for understandability */
                System.out.println("testEmptyRoleList: " + e.getMessage());
            }
        }
    }

    /**
     * Helper method for testing multiple invocation cases for methods marked
     * as public methods.
     */
    private void publicMethodHelper(String info, IF1 obj)
        throws Exception {

        /* With security disabled */
        final Object noCheckProxy = SecureProxy.create(obj, null, spfh);
        final int value1 = ((IF1) noCheckProxy).f(null, 17);
        assertEquals(info, 17, value1);

        /* With security enabled */
        final AccessChecker checker = new MySecurityChecker();
        final Object checkingProxy = SecureProxy.create(obj, checker, spfh);

        /* Not authenticated */
        final int value2 = ((IF1) checkingProxy).f(null, 17);
        assertEquals(info, 17, value2);

        /* Authenticated */
        final AuthContext context = new AuthContext(makeLoginToken());
        final int value3 = ((IF1) checkingProxy).f(context, 17);
        assertEquals(info, 18, value3);
    }

    /**
     * Test methods on a PublicAPI class with unannotated methods.
     * PublicAPI establishes public access as the default behavior.
     */
    @Test
    public void testPublicAPINoMethodAnnotation()
        throws Exception {

        publicMethodHelper("PublicAPINoMethodAnnotation",
                           new PublicAPINoMethodAnnotation());
    }

    /**
     * Test methods on a PublicAPI class with methods annotated redundantly
     * as public.
     */
    @Test
    public void testPublicAPIPublicMethodAnnotation()
        throws Exception {

        publicMethodHelper("PublicAPIPublicMethodAnnotation",
                           new PublicAPINoMethodAnnotation());
    }

    /**
     * Helper method for testing multiple invocation cases for methods marked
     * as secure auto methods.
     */
    private void secureAutoMethodHelper(String info, IF1 obj)
        throws Exception {

        /* With security disabled */
        final Object noCheckProxy = SecureProxy.create(obj, null, spfh);
        final int value1 = ((IF1) noCheckProxy).f(null, 17);
        assertEquals(17, value1);

        /* With security enabled */
        final AccessChecker checker = new MySecurityChecker();
        final Object checkingProxy = SecureProxy.create(obj, checker, spfh);

        /* Not authenticated */
        try {
            ((IF1) checkingProxy).f(null, 17);
            fail(info + " - expected exception");
        } catch (Exception e) {
            assertSame(AuthenticationRequiredException.class, e.getClass());
        }

        /* Authenticated */
        final AuthContext context = new AuthContext(makeLoginToken());
        final int value2 = ((IF1) checkingProxy).f(context, 17);
        assertEquals(18, value2);
    }

    /**
     * Test methods on a PublicAPI class with methods marked as secure,
     * overriding the default.
     */
    @Test
    public void testPublicAPISecureAutoMethodAnnotation()
        throws Exception {

        secureAutoMethodHelper("PublicAPISecureMethodAnnotation",
                               new PublicAPISecureAutoMethodAnnotation());
    }

    /**
     * Test methods on a SecureAPI class with methods marked as secure.
     */
    @Test
    public void testSecureAPISecureAutoMethodAnnotation()
        throws Exception {

        secureAutoMethodHelper("SecureAPISecureAutoMethodAnnotation",
                               new SecureAPISecureAutoMethodAnnotation());
    }

    /**
     * Test methods on a SecureAPI class with methods marked as public.
     */
    @Test
    public void testSecureAPIPublicMethodAnnotation()
        throws Exception {

        publicMethodHelper("SecureAPIPublicMethodAnnotation",
                           new SecureAPIPublicMethodAnnotation());
    }

    /**
     * Test methods on a SecureAPI class with methods marked as internally
     * authenticated.
     */
    @Test
    public void testSecureAPISecureInternalMethodAnnotation()
        throws Exception {

        final SecureAPISecureInternalMethodAnnotation obj =
            new SecureAPISecureInternalMethodAnnotation();

        /* With security disabled */
        final Object noCheckProxy = SecureProxy.create(obj, null, spfh);
        final int value1 = ((IF1) noCheckProxy).f(null, 17);
        assertEquals(17, value1);

        /* With security enabled */
        final AccessChecker checker = new MySecurityChecker();
        final Object checkingProxy = SecureProxy.create(obj, checker, spfh);

        /* authenticated */
        final AuthContext context = new AuthContext(makeLoginToken());
        final int value2 = ((IF1) checkingProxy).f(context, 17);
        assertEquals(17, value2);
    }

    /*
     * Set of interfaces and classes to enable testing
     */
    public interface IF1 extends Remote {
        int f(AuthContext context, int version);
    }

    public interface R2IF1 extends Remote {
         int f(int version);
    }

    /*
     * Classes that result in exceptions
     */

    /*
     * Doesn't implement Remote
     */
    class NotRemote {
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * Implements Remote, but no proxyable methods
     */
    @SecureAPI
    class NoProxyableMethods implements Remote {
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * No class annotation
     */
    class NoClassAnnotation implements IF1 {
        @Override
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * No method annotation for SecureAPI class
     */
    @SecureAPI
    class NoMethodAnnotation implements R2IF1 {
        @Override
        public int f(int version) {
            return version;
        }
    }

    /*
     * No authentication context for SecureMethod method
     */
    @SecureAPI
    class NoAuthContext implements R2IF1 {
        @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
        @Override
        public int f(int version) {
            return version;
        }
    }

    /*
     * Multiple class-level annotations
     */
    @SecureAPI
    @PublicAPI
    class MultipleClassAnnotations implements IF1 {
        @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
        @Override
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * Multiple method-level annotations
     */
    @SecureAPI
    class MultipleMethodAnnotations implements IF1 {
        @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
        @PublicMethod
        @Override
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * Annotation on a non-interface method
     */
    @SecureAPI
    class NonInterfaceMethodAnnotation implements Remote {
        @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * Empty privilege list
     */
    @SecureAPI
    class EmptyRoleList implements IF1 {
        @SecureAutoMethod(privileges = { })
        @Override
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * Classes that should allow proxy creation
     */

    /*
     * PublicAPI - ok to have no method annotation
     */
    @PublicAPI
    class PublicAPINoMethodAnnotation implements IF1 {
        @Override
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * PublicAPI - ok to include PublicMethod annotation
     */
    @PublicAPI
    class PublicAPIPublicMethodAnnotation implements IF1 {
        @PublicMethod
        @Override
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * PublicAPI - ok to include SecureMethod annotation
     */
    @PublicAPI
    class PublicAPISecureAutoMethodAnnotation implements IF1 {
        @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
        @Override
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * SecureAPI - ok to include PublicMethod annotation
     */
    @SecureAPI
    class SecureAPIPublicMethodAnnotation implements IF1 {
        @PublicMethod
        @Override
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * SecureAPI - ok to include SecureMethod(AUTO) annotation
     */
    @SecureAPI
    class SecureAPISecureAutoMethodAnnotation implements IF1 {
        @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
        @Override
        public int f(AuthContext context, int version) {
            return version + (context == null ? 0 : 1);
        }
    }

    /*
     * SecureAPI - ok to include SecureMethod(INTERNAL) annotation
     */
    @SecureAPI
    class SecureAPISecureInternalMethodAnnotation implements IF1 {
        @SecureInternalMethod
        @Override
        public int f(AuthContext context, int version) {
            return version;
        }
    }

    private LoginToken makeLoginToken() {
        return new LoginToken(new SessionId(new byte[4]), 0L);
    }

    class MySecurityChecker implements AccessChecker {
        private final Subject authenticatedSubject;
        private final RoleInstance defaultRole = RoleInstance.PUBLIC;

        MySecurityChecker() {
            authenticatedSubject = makeAuthenticatedSubject();
        }

        @Override
        public Subject identifyRequestor(AuthContext context) {
            if (context == null || context.getLoginToken() == null) {
                throw new AuthenticationRequiredException(
                    "Not authenticated", false /* isReturnSignal */);
            }

            return authenticatedSubject;
        }

        @Override
        public void checkAccess(ExecutionContext execCtx,
                                OperationContext opCtx)
            throws AuthenticationRequiredException, UnauthorizedException {

            final Subject subject = execCtx.requestorSubject();
            if (subject == null) {
                throw new AuthenticationRequiredException(
                    "Not authenticated", false /* isReturnSignal */);
            }

            final List<? extends KVStorePrivilege> reqPrivis =
                opCtx.getRequiredPrivileges();

            if (!execCtx.hasAllPrivileges(reqPrivis)) {
                throw new UnauthorizedException(
                    "Insufficient access rights granted");
            }
        }

        private Subject makeAuthenticatedSubject() {
            final Set<Principal> subjPrincs = new HashSet<Principal>();
            subjPrincs.add(KVStoreRolePrincipal.get(defaultRole.name()));
            final Set<Object> publicCreds = new HashSet<Object>();
            final Set<Object> privateCreds = new HashSet<Object>();
            return new Subject(true, subjPrincs, publicCreds, privateCreds);
        }

        @Override
        public Set<KVStorePrivilege> identifyPrivileges(Subject subj) {
            if (subj == null) {
                return null;
            }
            return defaultRole.getPrivileges();
        }
    }

    static class SPFH extends TestProcessFaultHandler {
        SPFH(Logger logger) {
            super(logger, ProcessExitCode.RESTART);
        }

        @Override
        public void queueShutdownInternal(Throwable th, ProcessExitCode pec) {
            fail("queueShutdownInternal called");
        }
    }
}
