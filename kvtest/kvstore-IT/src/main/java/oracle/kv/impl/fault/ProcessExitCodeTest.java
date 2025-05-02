/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.fault;

import static org.junit.Assert.assertEquals;

import java.util.function.BiConsumer;

import oracle.kv.TestBase;

import org.junit.Test;

public class ProcessExitCodeTest extends TestBase {

    @Test
    public void testGetProcessExitCode() {

        final BiConsumer<ProcessExitCode, Integer> test =
            (v, i) -> assertEquals(v, ProcessExitCode.getProcessExitCode(i));

        test.accept(null, 0);
        test.accept(null, 199);
        test.accept(ProcessExitCode.RESTART, 200);
        test.accept(ProcessExitCode.NO_RESTART, 201);
        test.accept(ProcessExitCode.RESTART_OOME, 202);
        test.accept(ProcessExitCode.INJECTED_FAULT_RESTART, 203);
        test.accept(ProcessExitCode.INJECTED_FAULT_NO_RESTART, 204);
        test.accept(null, 205);
        test.accept(null, 255);
    }

    @Test
    public void testInjectedFault() {

        final BiConsumer<Boolean, Integer> test =
            (b, i) -> assertEquals(b, ProcessExitCode.injectedFault(i));

        test.accept(false, 0);
        test.accept(false, 199);
        test.accept(false, 200);
        test.accept(false, 201);
        test.accept(false, 202);
        test.accept(true, 203);
        test.accept(true, 204);
        test.accept(false, 205);
        test.accept(false, 255);
    }

    @Test
    public void testDescription() {

        final BiConsumer<String, Integer> test =
            (d, i) -> assertEquals(d, ProcessExitCode.description(i));

        test.accept("0/normal", 0);
        test.accept("1/unknown", 1);
        test.accept("128/unknown", 128);
        test.accept("129/kill-1", 129);
        test.accept("137/kill-9", 137);
        test.accept("192/kill-64", 192);
        test.accept("193/unknown", 193);
        test.accept("199/unknown", 199);
        test.accept("200/RESTART", 200);
        test.accept("201/NO_RESTART", 201);
        test.accept("202/RESTART_OOME", 202);
        test.accept("203/INJECTED_FAULT_RESTART", 203);
        test.accept("204/INJECTED_FAULT_NO_RESTART", 204);
        test.accept("205/unknown", 205);
        test.accept("255/unknown", 255);
    }
}
