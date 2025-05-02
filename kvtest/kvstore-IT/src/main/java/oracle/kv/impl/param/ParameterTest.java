/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.param;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashSet;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Tests Parameter-related classes.
 * TODO: test ParameterState more fully.
 */
public class ParameterTest extends TestBase {

    private static final File testDir = TestUtils.getTestDir();
    private static final String configFile = "config.xml";

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Exercise the basic interfaces, setting defaults, globals, and
     * per-node parameters.
     */
    @Test
    public void testBasic()  {
        final ParameterMap map = new ParameterMap();
        final String key = "key";
        final int value = 67;
        map.put(new IntParameter(key, value));
        assertTrue(value == map.get(key).asInt());
        map.put(new StringParameter(key, Integer.toString(value)));
        try {
            map.get(key).asInt();
            fail("Operation should have failed");
        } catch (IllegalStateException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        final Parameter p = map.get("notfound");
        assertTrue(p != null);
        assertTrue(p.asString() == null);

        assertTrue(0 == map.getOrZeroInt("alsonotfound"));
        assertTrue(0L == map.getOrZeroLong("alsonotfound"));
        assertTrue(map.exists(key));
        map.remove(key);
        assertFalse(map.exists(key));
    }

    /**
     * Create a few maps and test write/load.
     */
    @Test
    public void testLoadParameters()  {
        final ParameterMap map1 = new ParameterMap("c1", "int");
        final ParameterMap map2 = new ParameterMap("c2", "string");
        final ParameterMap map3 = new ParameterMap("c3", "long");
        final LoadParameters lp = new LoadParameters();

        for (int i = 0; i < 10; i++) {
            final String s = Integer.toString(i);
            map1.put(new IntParameter(s, i));
            map2.put(new StringParameter(s, s));
            map3.put(new LongParameter(s, i));
        }
        final ParameterMap map4 = map3.copy();
        map4.setName("c3copy");

        lp.addMap(map1);
        lp.addMap(map2);
        lp.addMap(map3);
        lp.addMap(map4);

        assertTrue(map1 == lp.getMap("c1", "int"));
        assertTrue(map2 == lp.getMapByType("string"));
        assertTrue(map3 == lp.getMap("c3", "long"));

        /* There should be 2 "long" maps */
        List<ParameterMap> maps = lp.getAllMaps("long");
        assertTrue(maps.size() == 2);

        /*
         * Test adding a duplicate map. The duplicate map should replace the
         * existing map with the same name.
         */
        final String existingName = map2.getName();
        final ParameterMap dup = new ParameterMap(existingName, "dup");

        final int beforeSize = lp.getMaps().size();
        lp.addMap(dup);
        assertTrue(dup == lp.getMap(existingName));
        assertTrue(lp.getMaps().size() == beforeSize);

        /**
         * Dump/load the maps and assert that the loaded ones equal the
         * originals.
         */
        final File config = new File(testDir, configFile);
        lp.saveParameters(config);
        final LoadParameters newlp =
            LoadParameters.getParametersByType(config);
        maps = newlp.getMaps();
        assertTrue(maps.size() == 4);
        for (ParameterMap newmap : maps) {
            assertTrue(newmap.equals(lp.getMap(newmap.getName())));
        }
    }

    @Test
    public void testMerge() {
        final String svalue = "2 is a string";
        final ParameterMap map = new ParameterMap("c1", "int");
        for (int i = 0; i < 10; i++) {
            final String s = Integer.toString(i);
            map.put(new IntParameter(s, i));
        }
        assertTrue(map.size() == 10);
        final ParameterMap mergeMap = new ParameterMap();
        /* add a parameter and change one */
        mergeMap.put(new IntParameter("11", 11));
        mergeMap.put(new StringParameter("2", svalue));

        /* merge */
        map.merge(mergeMap, false);
        assertTrue(map.size() == 11);
        assertTrue(map.get("2").asString().equals(svalue));
    }

    @Test
    public void testDuration() {

        DurationParameter dp = new DurationParameter("foo", "1   SECONDS");
        final DurationParameter dp1 = new DurationParameter("foo", "1-SECONDS");
        final DurationParameter dp2 = new DurationParameter("foo", "1_SECONDS");
        final DurationParameter dp3 = new DurationParameter("foo", "1 seconds");
        final DurationParameter dp4 = new DurationParameter("foo", "1      s");
        final DurationParameter dp5 = new DurationParameter("foo", "1000 MS");
        assertTrue(dp.equals(dp1));
        assertTrue(dp.equals(dp2));
        assertTrue(dp.equals(dp3));
        assertTrue(dp.equals(dp4));
        assertTrue(dp.equals(dp5));
        assertTrue("1 SECONDS".equals(dp.asString()));
        assertTrue("1-SECONDS".equals(dp.asString('-')));

        /* some failures */
        try {
            dp = new DurationParameter("foo", "1 SEC");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        try {
            dp = new DurationParameter("foo", "1S");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    @Test
    public void testSize() {

        /* Size parameters should compare the size not the strings */
        SizeParameter dp = new SizeParameter("foo", "1 GB");
        final SizeParameter dp1 = new SizeParameter("foo", "1-GB");
        final SizeParameter dp2 = new SizeParameter("foo", "1_GB");
        final SizeParameter dp3 = new SizeParameter("foo", "1 gb");
        final SizeParameter dp4 = new SizeParameter("foo", "1024 mb");
        final SizeParameter dp5 = new SizeParameter("foo", "1073741824");
        assertTrue(dp.equals(dp1));
        assertTrue(dp.equals(dp2));
        assertTrue(dp.equals(dp3));
        assertTrue(dp.equals(dp4));
        assertTrue(dp.equals(dp5));
        assertTrue("1 GB".equals(dp.asString()));

        /* check greater than int */
        dp = new SizeParameter("foo", "9663676416");

        /* some failures */
        try {
            dp = new SizeParameter("foo", "1 G");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        try {
            dp = new SizeParameter("foo", "1GB");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    /**
     * Test that upper and lower case work.
     */
    @Test
    public void testCacheMode() {

        CacheModeParameter cmp =
            new CacheModeParameter("foo", "EVICT_LN");
        final CacheModeParameter cmp1 =
            new CacheModeParameter("foo", "EVICT_ln");
        assertTrue(cmp.equals(cmp1));
        try {
            cmp = new CacheModeParameter("foo", "notvalid");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    /**
     * Test dealing with parameters that are not known.  This can happen when
     * parameters are removed from ParameterState between releases.
     */
    @Test
    public void testUnknown() {
        final ParameterMap map = new ParameterMap();

        /* Add a couple of normal parameters */
        map.setParameter(ParameterState.COMMON_SN_ID, "1");
        map.setParameter(ParameterState.COMMON_HOSTNAME, "myhost");

        /* Add one that is not in ParameterState */
        try {
            map.setParameter("unknownParam", "unknown");
            fail("setParameter should have failed");
        } catch (IllegalStateException ise) /* CHECKSTYLE:OFF */ {
            /* success */
        } /* CHECKSTYLE:ON */

        /* Add it again, bypassing validation */
        map.put(new StringParameter("unknownParam", "unknown"));

        /* Test that filters don't fail because of the unknown parameter */
        final HashSet<String> set = new HashSet<String>();
        set.add(ParameterState.COMMON_SN_ID);
        set.add("unknownParam");
        map.filter(set, true);
        map.readOnlyFilter();
    }

    /*
     * Test parameter time duration translation.
     */
    @Test
    public void testDurationTranslation() {

        final ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.MP_POLL_PERIOD, "111 MILLISECONDS");
        assertEquals(111,
                     ParameterUtils.getDurationMillis(
                         map, ParameterState.MP_POLL_PERIOD));
        try {
            map.setParameter(ParameterState.MP_POLL_PERIOD, "111");
            fail("Bad time unit, should be rejected");
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    @Test
    public void testAuthMethod() {
        AuthMethodsParameter cmp = new AuthMethodsParameter("foo", "kerberos");
        final AuthMethodsParameter cmp1 =
            new AuthMethodsParameter("foo", "KerbeRos");
        assertTrue(cmp.equals(cmp1));
        try {
            cmp = new AuthMethodsParameter("foo", "notvalid");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        cmp = new AuthMethodsParameter("foo", "none");
        cmp.asString().equals("NONE");
    }

    @Test
    public void testSpecialChars() {
        SpecialCharsParameter scp =
            new SpecialCharsParameter("foo", "!#$%&'()*+,-./:; <=>?@[]^_`{|}~");
        assertEquals(scp.getName(), "foo");
        assertEquals(scp.asString(), "!#$%&'()*+,-./:; <=>?@[]^_`{|}~");

        try {
            scp = new SpecialCharsParameter("foo", "abc");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            scp = new SpecialCharsParameter("foo", "123");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            scp = new SpecialCharsParameter("foo", "abc 123 !@#$%");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    /*
     * Test AuthMethod parameter parsing.
     */
    @Test
    public void testAuthMethodParsing() {
        final ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.GP_USER_EXTERNAL_AUTH, "kerberos ");
        final Parameter param = map.get(ParameterState.GP_USER_EXTERNAL_AUTH);
        String[] methods = ((AuthMethodsParameter) param).asAuthMethods();
        assertEquals(1, methods.length);
        assertEquals("KERBEROS", methods[0]);

        try {
             map.setParameter(ParameterState.GP_USER_EXTERNAL_AUTH,
                              "kerberos,ldap");
            fail("ldap does not supported");
        } catch (IllegalArgumentException expected) {
            assertThat("ldap in the exception message",
                       expected.getMessage(), containsString("ldap"));
        }

        try {
            map.setParameter(ParameterState.GP_USER_EXTERNAL_AUTH,
                             "kerberos,   none");
            fail("cannot set none and other auth method at the same time");
        } catch (IllegalArgumentException expected) {
            assertThat("None in the exception message",
                       expected.getMessage(), containsString("NONE"));
        }

        try {
            map.setParameter(ParameterState.GP_USER_EXTERNAL_AUTH, "keberos;");
            fail("only accept format authMethod1,authMethod2");
        } catch (IllegalArgumentException expected) {
            assertThat("Reported ';' is invalid authMethod",
                       expected.getMessage(), containsString(";"));
        }
        map.setParameter(ParameterState.GP_USER_EXTERNAL_AUTH, "kerberos,");
        methods = ((AuthMethodsParameter) param).asAuthMethods();
        assertEquals(1, methods.length);
        assertEquals("KERBEROS", methods[0]);
    }

    /** Test boundary conditions for COMMON_NUMCPUS. */
    @Test
    public void testCommonNumCPUs() {
        assertFalse(ParameterState.validate(
                        ParameterState.COMMON_NUMCPUS, -42));
        assertFalse(ParameterState.validate(
                        ParameterState.COMMON_NUMCPUS, 0));
        assertTrue(ParameterState.validate(
                       ParameterState.COMMON_NUMCPUS, 1));
        assertTrue(ParameterState.validate(
                       ParameterState.COMMON_NUMCPUS, 256));
        assertTrue(ParameterState.validate(
                       ParameterState.COMMON_NUMCPUS, 65536));
        assertFalse(ParameterState.validate(
                       ParameterState.COMMON_NUMCPUS, 65537));
    }

    /** Test boundary conditions for COMMON_MEMORY_MB. */
    @Test
    public void testCommonMemoryMB() {
        assertFalse(ParameterState.validate(
                        ParameterState.COMMON_MEMORY_MB, -42));
        assertFalse(ParameterState.validate(
                        ParameterState.COMMON_MEMORY_MB, -1));
        assertTrue(ParameterState.validate(
                       ParameterState.COMMON_MEMORY_MB, 0));
        assertTrue(ParameterState.validate(
                       ParameterState.COMMON_MEMORY_MB, 1024*1024));
        assertTrue(ParameterState.validate(
                       ParameterState.COMMON_MEMORY_MB, 128*1024*1024));
        assertFalse(ParameterState.validate(
                        ParameterState.COMMON_MEMORY_MB, 128*1024*1024 + 1));
    }
    
    @Test
    public void testTimeToLive() {
        TimeToLiveParameter ttlp;
        ttlp = new TimeToLiveParameter("foo", "0 Days");
        assertEquals(0, ttlp.toTimeToLive().toDays());
        ttlp = new TimeToLiveParameter("foo", "123 Days");
        assertEquals(123, ttlp.toTimeToLive().toDays());
        ttlp = new TimeToLiveParameter("foo", "456 Hours");
        assertEquals(456, ttlp.toTimeToLive().toHours());
        
        /* some failures */
        try {
            ttlp = new TimeToLiveParameter("foo", "1 SEC");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        try {
            ttlp = new TimeToLiveParameter("foo", "-1 days");
            fail("should not get here");
        } catch (IllegalArgumentException ignored) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }
}
