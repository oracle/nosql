/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;


import oracle.kv.TestBase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Tests PortConflictValidator.
 */
public class PortConflictValidatorTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    @Test
    public void testSinglePort() {
        /*
         * Test the return of check(String, int) method of PortConflictValidator 
         */
        PortConflictValidator validator = new PortConflictValidator(); 
        assertNull(validator.check("port1", 5001));
        assertNull(validator.check("port2", 5002));
        assertEquals(validator.check("port3", 5002), 
                "Specified port3 5002 is already assigned as port2");
        

    }
    
    @Test
    public void testRangePort() {
        /*
         * Test the return of check(String, String) method of 
         * PortConflictValidator 
         */
        PortConflictValidator validator = new PortConflictValidator(); 
        assertNull(validator.check("port1", "5001,5010"));
        assertNull(validator.check("port2", "6001,6010"));
        assertEquals(validator.check("port3", "6005,6015"), 
                "Specified port3 6005 is already assigned as port2");
    }
    
    @Test
    public void testMixedPort() {
        /*
         * Test the return of check(String, String) and check(String, int) of 
         * PortConflictValidator 
         */
        PortConflictValidator validator = new PortConflictValidator(); 
        assertNull(validator.check("port1", 5001));
        assertNull(validator.check("port2", "6001,6010"));
        assertEquals(validator.check("port3", 6005), 
                "Specified port3 6005 is already assigned as port2");
        

    }
}
