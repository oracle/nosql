/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.security.MessageDigest;
import java.util.Arrays;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.arb.admin.ArbNodeAdmin;
import oracle.kv.impl.async.JavaSerialMethodTable.JavaSerialMethodCall;
import oracle.kv.impl.async.JavaSerialMethodTable.MethodOpImpl;
import oracle.kv.impl.monitor.MonitorAgent;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.security.login.TrustedLogin;
import oracle.kv.impl.sna.StorageNodeAgentInterface;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.VersionedRemote;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.junit.Test;

public class JavaSerialMethodTableTest extends TestBase {

    private static final MessageDigest md =
        JavaSerialMethodTable.getMessageDigest();
    static final JavaSerialMethodTable noOtherMethodsTable =
        JavaSerialMethodTable.getTable(NoOtherMethods.class);
    static final JavaSerialMethodTable secondRemoteMethodTable =
        JavaSerialMethodTable.getTable(SecondRemoteMethod.class);

    /* Tests */

    @Test
    public void testGetTypeDescriptor() {
        testGetTypeDescriptor("[I", new int[0].getClass());
        testGetTypeDescriptor("[[D", new double[0][0].getClass());
        testGetTypeDescriptor("[Loracle.kv.impl.util.SerialVersion;",
                              new SerialVersion[0].getClass());
        testGetTypeDescriptor("Loracle.kv.impl.util.SerialVersion;",
                              SerialVersion.class);
        testGetTypeDescriptor("Ljava.lang.String;", String.class);
        testGetTypeDescriptor("Z", Boolean.TYPE);
        testGetTypeDescriptor("B", Byte.TYPE);
        testGetTypeDescriptor("C", Character.TYPE);
        testGetTypeDescriptor("D", Double.TYPE);
        testGetTypeDescriptor("F", Float.TYPE);
        testGetTypeDescriptor("I", Integer.TYPE);
        testGetTypeDescriptor("J", Long.TYPE);
        testGetTypeDescriptor("S", Short.TYPE);
        testGetTypeDescriptor("V", Void.TYPE);
    }

    private void testGetTypeDescriptor(String desc, Class<?> type) {
        assertEquals(desc, JavaSerialMethodTable.getTypeDescriptor(type));
    }

    @Test
    public void testGetMethodNameAndDescriptor() throws Exception {
        testGetMethodNameAndDescriptor("foo()V",
                                       FooRemote.class.getMethod("foo"));
        testGetMethodNameAndDescriptor("bar(IZLjava.lang.String;)J",
                                       FooRemote.class.getMethod(
                                           "bar", Integer.TYPE, Boolean.TYPE,
                                           String.class));
        testGetMethodNameAndDescriptor(
            "baz([Ljava.lang.Long;)Ljava.lang.String;",
            FooRemote.class.getMethod("baz", new Long[0].getClass()));
    }

    private void testGetMethodNameAndDescriptor(String desc, Method method) {
        assertEquals(desc,
                     JavaSerialMethodTable.getMethodNameAndDescriptor(method));
    }

    @Test
    public void testComputeMethodHash() throws Exception {
        testComputeMethodHash(-1390960742,
                              VersionedRemote.class.getMethod(
                                  "getSerialVersion"));
        testComputeMethodHash(-454096463, FooRemote.class.getMethod("foo"));
        testComputeMethodHash(2088650632,
                              FooRemote.class.getMethod(
                                  "bar", Integer.TYPE, Boolean.TYPE,
                                  String.class));
        testComputeMethodHash(874747446,
                              FooRemote.class.getMethod(
                                  "baz", new Long[0].getClass()));
    }

    private void testComputeMethodHash(int value, Method method) {
        assertEquals(value,
                     JavaSerialMethodTable.computeMethodHash(method, md));
        assertEquals(value,
                     JavaSerialMethodTable.computeMethodHash(method, md));
    }

    @Test
    public void testGetTable() throws Exception {
        final JavaSerialMethodTable table = testGetTable(FooRemote.class);
        testGetTableMethod(table, -454096463,
                           FooRemote.class.getMethod("foo"));
        testGetTableMethod(table, 2088650632,
                           FooRemote.class.getMethod(
                               "bar", Integer.TYPE, Boolean.TYPE,
                               String.class));
        testGetTableMethod(table, 874747446,
                           FooRemote.class.getMethod(
                               "baz", new Long[0].getClass()));
    }

    private JavaSerialMethodTable
        testGetTable(Class<? extends VersionedRemote> type)
        throws Exception
    {
        final JavaSerialMethodTable table =
            JavaSerialMethodTable.getTable(type);
        final Method method =
            VersionedRemote.class.getMethod("getSerialVersion");
        assertEquals(method, table.getMethodOp(-1390960742).method);
        assertEquals(method, table.getMethodOp(method).method);
        return table;
    }

    void testGetTableMethod(JavaSerialMethodTable table,
                            int id,
                            Method method) {
        assertEquals(method, table.getMethodOp(id).method);
    }

    /**
     * Test all remote types that use JavaSerialMethodTable to confirm there
     * are no collisions
     */
    @Test
    public void testAllTables() throws Exception {
        testGetTable(ArbNodeAdmin.class);
        testGetTable(CommandService.class);
        testGetTable(MonitorAgent.class);
        testGetTable(RemoteTestInterface.class);
        testGetTable(RepNodeAdmin.class);
        testGetTable(StorageNodeAgentInterface.class);
        testGetTable(TrustedLogin.class);
    }

    @Test
    public void testGetMethodCall() throws Exception {
        assertEquals(-1390960742,
                     noOtherMethodsTable.getMethodCall(
                         NoOtherMethods.class.getMethod("getSerialVersion"),
                         null)
                     .getMethodOp()
                     .getValue());

        checkException(() -> noOtherMethodsTable.getMethodCall(
                           Object.class.getMethod("hashCode"), null),
                       IllegalArgumentException.class,
                       "No MethodOp found");
    }

    @Test
    public void testGetMethodOp() {

        assertEquals(-1390960742,
                     noOtherMethodsTable.getMethodOp(-1390960742).getValue());
        checkException(() -> noOtherMethodsTable.getMethodOp(1),
                       IllegalArgumentException.class,
                       "No MethodOp found");
    }

    @Test
    public void testCallService() throws Exception {
        final JavaSerialMethodCall noOtherMethodsGetSerialVersion =
            noOtherMethodsTable.getMethodCall(
                VersionedRemote.class.getMethod("getSerialVersion"), null);
        assertEquals((short) 3,
                     noOtherMethodsGetSerialVersion.callService(
                         SerialVersion.CURRENT, () -> (short) 3));

        final JavaSerialMethodCall secondRemoteMethodRemoteMethod =
            secondRemoteMethodTable.getMethodCall(
                SecondRemoteMethod.class.getMethod("remoteMethod",
                                                   Integer.TYPE, Short.TYPE),
                new Object[] { 4 });
        assertEquals("Five",
                     secondRemoteMethodRemoteMethod.callService(
                         SerialVersion.CURRENT,
                         new SecondRemoteMethod() {
                             @Override
                             public short getSerialVersion() {
                                 throw new RuntimeException("Unexpected call");
                             }
                             @Override
                             public String remoteMethod(int x, short sv) {
                                 assertEquals(4, x);
                                 assertEquals(SerialVersion.CURRENT, sv);
                                 return "Five";
                             }
                         }));
    }

    /** Test serialization of call arguments */
    @Test
    public void testArgsSerialVersion() throws Exception {
        checkAll(serialVersionChecker(testArgs(null),
                                      0xf2447af16ba6d841L),
                 serialVersionChecker(testArgs(new Object[0]),
                                      0xa04b2ac8b3740273L),
                 serialVersionChecker(testArgs(new Object[] { null }),
                                      0x1a04c26289b388e0L),
                 serialVersionChecker(testArgs(new Object[] { "a", 1L }),
                                      0xcae5c536fe799de8L));
    }

    /** A remote interface for testing argument serialization */
    public interface TestInterface extends VersionedRemote {
        void testMethod(Object[] args) throws RemoteException;
    }

    /** Create an object to test serializing the specified arguments */
    static Object testArgs(@Nullable Object @Nullable[] args) {
        return new TestArgsSerialVersion(args);
    }

    /**
     * Tests a JavaSerialMethodTable method called with a specific set of
     * arguments.
     */
    static class TestArgsSerialVersion implements FastExternalizable {
        private static final JavaSerialMethodTable methodTable =
            JavaSerialMethodTable.getTable(TestInterface.class);
        private static final Method method;
        static {
            try {
                method = TestInterface.class.getMethod("testMethod",
                                                       Object[].class);
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Unexpected exception: " + e, e);
            }
        }
        private static final MethodOpImpl methodOp =
            methodTable.getMethodOp(method);
        final @Nullable Object @Nullable [] args;
        TestArgsSerialVersion(@Nullable Object @Nullable[] args) {
            this.args = args;
        }
        public TestArgsSerialVersion(DataInput in, short serialVersion)
            throws IOException
        {
            /* Read the arguments */
            args = methodOp.readRequest(in, serialVersion).args;
        }
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            /* Write the arguments */
            methodTable.getMethodCall(method, args)
                .writeFastExternal(out, serialVersion);
        }
        @Override
        public boolean equals(@Nullable Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof TestArgsSerialVersion)) {
                return false;
            }
            final TestArgsSerialVersion other = (TestArgsSerialVersion) obj;
            return Arrays.equals(args, other.args);
        }
        @Override
        public int hashCode() {
            return Arrays.hashCode(args);
        }
    }

    public interface FooRemote extends VersionedRemote {
        void foo() throws RemoteException;
        long bar(int i, boolean b, String d) throws RemoteException;
        String baz(Long[] array) throws RemoteException;
    }

    public interface NoOtherMethods extends VersionedRemote { }

    public interface SecondRemoteMethod extends VersionedRemote {
        String remoteMethod(int x, short serialVersion) throws RemoteException;
    }

}
