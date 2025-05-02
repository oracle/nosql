/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import oracle.kv.TestBase;
import oracle.kv.TestClassTimeoutMillis;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Wrapper class for testing StorageNodeAgentImpl.  This is the only class that
 * should be run for these tests. It uses Suite runner to run all the tests in
 * the suite classes. New test case classes should add in the
 * list of @SuiteClasses.
 */
@TestClassTimeoutMillis(40*60*1000)
@RunWith(Suite.class)
@SuiteClasses({StorageNodeBasic.class,
               StorageNodeAdmin.class,
               StorageNodeRepNode.class,
               StorageNodeProcess.class,
               StorageNodeParameters.class,
               StorageNodeFailure.class,
               /*
                * Do this last for now so that residual SSL effects don't impact
                * non-SSL tests.
                */
               StorageNodeLogin.class})
public class StorageNodeAgentTest extends TestBase {
}
