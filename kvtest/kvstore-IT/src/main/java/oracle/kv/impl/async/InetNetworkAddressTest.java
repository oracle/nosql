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
import static org.junit.Assert.assertNotEquals;

import java.net.InetAddress;

import oracle.kv.TestBase;

import org.junit.Test;

/** Test the {@link InetNetworkAddress} class. */
public class InetNetworkAddressTest extends TestBase {

    @SuppressWarnings("null")
    @Test
    public void testIllegalArguments() throws Exception {
        checkException(() -> new InetNetworkAddress((String) null, 42),
                       IllegalArgumentException.class);
        checkException(() -> new InetNetworkAddress("unix_domain:b", 42),
                       IllegalArgumentException.class,
                       "Unexpected Unix domain hostname");
        checkException(() -> new InetNetworkAddress("h", -1),
                       IllegalArgumentException.class);
        checkException(() -> new InetNetworkAddress("h", 65536),
                       IllegalArgumentException.class);
        checkException(() ->
                       InetNetworkAddress.create((InetAddress) null, 42).get(),
                       IllegalArgumentException.class);
        checkException(() ->
                       InetNetworkAddress.create(
                           InetAddress.getLocalHost(),
                           Integer.MIN_VALUE)
                       .get(),
                       IllegalArgumentException.class);
        checkException(() ->
                       InetNetworkAddress.create(
                           InetAddress.getLocalHost(),
                           Integer.MAX_VALUE)
                       .get(),
                       IllegalArgumentException.class);
    }

    @Test
    public void testEquals() throws Exception {
        assertEquals(new InetNetworkAddress("foo", 0),
                     new InetNetworkAddress("foo", 0));
        assertEquals(new InetNetworkAddress("foo", 0).hashCode(),
                     new InetNetworkAddress("foo", 0).hashCode());
        InetNetworkAddress addr = new InetNetworkAddress("foo", 42);
        assertEquals(addr, addr);
        assertNotEquals(addr, null);
        assertNotEquals(addr, "nope");
        assertNotEquals(new InetNetworkAddress("foo", 1),
                        new InetNetworkAddress("foo", 2));
        assertNotEquals(new InetNetworkAddress("foo", 1).hashCode(),
                        new InetNetworkAddress("foo", 2).hashCode());
        assertNotEquals(new InetNetworkAddress("foo", 1),
                        new InetNetworkAddress("bar", 1));
        assertNotEquals(new InetNetworkAddress("foo", 1).hashCode(),
                        new InetNetworkAddress("bar", 1).hashCode());
    }

    @Test
    public void testSerialVersion() throws Exception {
        checkAll(serialVersionChecker(new InetNetworkAddress("Foo", 42),
                                      0x6a44fbae69645a3eL),
                 serialVersionChecker(new InetNetworkAddress("Foo", 0),
                                      0x7ce299da81c3e0b5L),
                 serialVersionChecker(new InetNetworkAddress("Foo", 65535),
                                      0x1de6e2d8ecbb054fL));
    }
}
